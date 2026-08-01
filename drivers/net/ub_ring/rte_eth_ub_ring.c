/* SPDX-License-Identifier: BSD-3-Clause
 *
 * ub_ring ethdev PMD (Layer 3) — mbuf adapter over lib/ub_ring (Layer 2).
 */

#include <stdlib.h>
#include <string.h>

#include <rte_eal.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_errno.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ring.h>
#include <rte_ethdev.h>
#include <ethdev_driver.h>

#include <rte_ub_ring.h>
#include <rte_eth_ub_ring.h>

RTE_LOG_REGISTER_DEFAULT(ub_ring_eth_logtype, NOTICE);
#define RTE_LOGTYPE_UB_RING_ETH ub_ring_eth_logtype
#define UB_RING_ETH_LOG(level, ...) \
	RTE_LOG_LINE_PREFIX(level, UB_RING_ETH, "%s(): ", __func__, __VA_ARGS__)

#define UB_RING_ETH_MAX_BURST 128

/* ---- internals ---- */

struct ub_ring_eth_internals;

struct ub_ring_eth_queue {
	uint32_t queue_id;
	uint32_t provider_index;  /* which provider shm this queue maps to */
	struct ub_ring_eth_internals *internals;
};

struct ub_ring_eth_internals {
	struct ub_ring_eth_queue rx_queues[RTE_MAX_QUEUES_PER_PORT];
	struct ub_ring_eth_queue tx_queues[RTE_MAX_QUEUES_PER_PORT];

	/* dummy pools: one per provider, backed by data ring buffer area */
	struct rte_mempool *dummy_pool;  /* single pool covering all data ring areas */

	struct rte_ether_addr address;
};

/* ---- helpers ---- */

/**
 * Encode an mbuf into a ub_buf for TX.
 * Returns the ub_buf pointer (already allocated from data ring).
 */
static void *
encode_mbuf_to_buf(struct rte_mbuf *m, uint32_t provider_index)
{
	uint32_t elt_size = rte_ub_ring_get_data_elt_size(provider_index);

	/*
	 * Full mbuf layout (see rte_pktmbuf_init):
	 *   [header: sizeof(rte_mbuf)] [priv: m->priv_size] [buf: m->buf_len]
	 *   m->buf_addr = m + sizeof(rte_mbuf) + m->priv_size
	 */
	uint32_t mbuf_hdr  = (uint32_t)sizeof(struct rte_mbuf);
	uint32_t priv_size = m->priv_size;
	uint32_t buf_len   = m->buf_len;
	uint32_t need      = mbuf_hdr + priv_size + buf_len;

	if (need > elt_size) {
		UB_RING_ETH_LOG(ERR,
			"mbuf too large: hdr=%u priv=%u buf=%u = %u > elt=%u",
			mbuf_hdr, priv_size, buf_len, need, elt_size);
		return NULL;
	}

	void *buf = rte_ub_ring_alloc_buf(provider_index);
	if (!buf)
		return NULL;

	struct rte_mbuf *dst = buf;

	/* full deep copy: header + priv + entire data buffer */
	memcpy(buf, m, need);

	/*
	 * Fix pointer fields:
	 *   buf_addr — no encoding needed, RX reconstructs from:
	 *       m->buf_addr = (char*)m + sizeof(rte_mbuf) + m->priv_size
	 *   next     — encode as {provider_index, data_ring_offset}
	 *   pool     — NULL (RX restores to dummy pool)
	 */
	if (m->next) {
		void *next_buf = encode_mbuf_to_buf(m->next, provider_index);
		if (next_buf) {
			uint64_t off = (uint64_t)((char *)next_buf -
				(char *)rte_ub_ring_get_data_buf_area(provider_index));
			dst->next = (struct rte_mbuf *)(uintptr_t)
				(((uint64_t)provider_index << 56) | off);
		} else {
			UB_RING_ETH_LOG(ERR, "encode chained next failed");
			rte_ub_ring_free_buf(buf);
			return NULL;
		}
	} else {
		dst->next = (struct rte_mbuf *)(uintptr_t)UINT64_MAX; /* all-F = NULL */
	}

	/* pool → NULL (RX restores to dummy pool) */
	dst->pool = NULL;

	return buf;
}

/**
 * Decode a ub_buf into an mbuf (RX path).
 * Returns the head of the mbuf chain (may be chained via next).
 * The returned mbuf has m->pool set to the dummy pool for this provider.
 */
static struct rte_mbuf *
decode_buf_to_mbuf(void *buf, struct rte_mempool *dummy_pool)
{
	struct rte_mbuf *m = buf; /* The buf IS the mbuf (zero-copy) */

	/* reconstruct buf_addr from invariant layout */
	m->buf_addr = (char *)m + sizeof(struct rte_mbuf) + m->priv_size;
	m->pool     = dummy_pool;

	/* decode next chain */
	uint64_t next_val = (uint64_t)(uintptr_t)m->next;
	if (next_val != UINT64_MAX) {
		uint8_t  ni  = (uint8_t)(next_val >> 56);
		uint64_t off = next_val & 0x00FFFFFFFFFFFFFFULL;
		void *next_buf = (char *)rte_ub_ring_get_data_buf_area(ni) + off;
		struct rte_mbuf *next_m = decode_buf_to_mbuf(next_buf, dummy_pool);
		m->next = next_m;
	} else {
		m->next = NULL;
	}

	return m;
}

/**
 * Drain the dummy pool: dequeue all freed mbufs and return
 * each to its data ring (identified by address range).
 */
static void
drain_dummy_pool(struct rte_mempool *dummy)
{
	if (!dummy)
		return;

	/* dequeue all available mbufs from the dummy pool and
	 * return each to its data ring via rte_ub_ring_free_buf.
	 * Since the dummy pool uses rte_mempool internally, we drain
	 * by repeatedly calling rte_mempool_get until empty, then
	 * rte_ub_ring_free_buf on each.
	 */
	void *objs[32];
	int n = rte_mempool_get_bulk(dummy, objs, 32);
	while (n > 0) {
		for (int i = 0; i < n; i++)
			rte_ub_ring_free_buf(objs[i]);
		n = rte_mempool_get_bulk(dummy, objs, 32);
	}
}

/* ---- RX / TX fast paths ---- */

static uint16_t
ub_ring_eth_rx(void *q, struct rte_mbuf **bufs, uint16_t nb_bufs)
{
	struct ub_ring_eth_queue *rq = q;
	struct ub_ring_eth_internals *in = rq->internals;

	/* drain dummy pool: return previously-freed mbufs to data rings */
	drain_dummy_pool(in->dummy_pool);

	/* bulk recv from Layer 2 */
	void *raw[UB_RING_ETH_MAX_BURST];
	uint16_t n = rte_ub_ring_recv_burst(rq->queue_id, raw,
		RTE_MIN(nb_bufs, UB_RING_ETH_MAX_BURST));

	for (uint16_t i = 0; i < n; i++) {
		bufs[i] = decode_buf_to_mbuf(raw[i], in->dummy_pool);
	}

	return n;
}

static uint16_t
ub_ring_eth_tx(void *q, struct rte_mbuf **mbufs, uint16_t nb_bufs)
{
	struct ub_ring_eth_queue *rq = q;
	void *raw[nb_bufs];
	uint16_t count = 0;

	for (uint16_t i = 0; i < nb_bufs; i++) {
		void *buf = encode_mbuf_to_buf(mbufs[i], rq->provider_index);
		if (!buf)
			break;
		raw[count++] = buf;
	}

	if (count == 0)
		return 0;

	uint16_t sent = rte_ub_ring_send_burst(rq->queue_id, raw, count);

	/* free unsent buffers back to data ring */
	for (uint16_t i = sent; i < count; i++)
		rte_ub_ring_free_buf(raw[i]);

	return sent;
}

/* ---- eth_dev_ops ---- */

static int
ub_ring_eth_dev_configure(struct rte_eth_dev *dev __rte_unused)
{ return 0; }

static int
ub_ring_eth_dev_start(struct rte_eth_dev *dev)
{
	dev->data->dev_link.link_status = RTE_ETH_LINK_UP;
	return 0;
}

static int
ub_ring_eth_dev_stop(struct rte_eth_dev *dev)
{
	dev->data->dev_started = 0;
	dev->data->dev_link.link_status = RTE_ETH_LINK_DOWN;
	return 0;
}

static int
ub_ring_eth_dev_close(struct rte_eth_dev *dev)
{
	struct ub_ring_eth_internals *in = dev->data->dev_private;
	ub_ring_eth_dev_stop(dev);

	/* drain and free dummy pools */
	if (in->dummy_pool) {
		drain_dummy_pool(in->dummy_pool);
		rte_mempool_free(in->dummy_pool);
	}

	dev->data->mac_addrs = NULL;
	rte_free(in);
	dev->data->dev_private = NULL;
	rte_ub_ring_fini();
	return 0;
}

static int
ub_ring_eth_dev_info(struct rte_eth_dev *dev, struct rte_eth_dev_info *info)
{
	uint32_t total = rte_ub_ring_get_total_queues();
	info->max_mac_addrs   = 1;
	info->max_rx_pktlen   = (uint32_t)-1;
	info->max_rx_queues    = (uint16_t)total;
	info->max_tx_queues    = (uint16_t)total;
	info->min_rx_bufsize   = 0;
	info->rx_offload_capa  = RTE_ETH_RX_OFFLOAD_SCATTER;
	info->tx_offload_capa  = RTE_ETH_TX_OFFLOAD_MULTI_SEGS;
	(void)dev;
	return 0;
}

static int
ub_ring_eth_rx_queue_setup(struct rte_eth_dev *dev, uint16_t rx_queue_id,
	uint16_t nb_rx_desc __rte_unused, unsigned int socket_id __rte_unused,
	const struct rte_eth_rxconf *rx_conf __rte_unused,
	struct rte_mempool *mb_pool __rte_unused)
{
	struct ub_ring_eth_internals *in = dev->data->dev_private;
	uint32_t total = rte_ub_ring_get_total_queues();
	if (rx_queue_id >= total)
		return -EINVAL;
	in->rx_queues[rx_queue_id].queue_id = rx_queue_id;
	in->rx_queues[rx_queue_id].provider_index =
		rte_ub_ring_queue_to_provider(rx_queue_id);
	in->rx_queues[rx_queue_id].internals = in;
	dev->data->rx_queues[rx_queue_id] = &in->rx_queues[rx_queue_id];
	return 0;
}

static int
ub_ring_eth_tx_queue_setup(struct rte_eth_dev *dev, uint16_t tx_queue_id,
	uint16_t nb_tx_desc __rte_unused, unsigned int socket_id __rte_unused,
	const struct rte_eth_txconf *tx_conf __rte_unused)
{
	struct ub_ring_eth_internals *in = dev->data->dev_private;
	uint32_t total = rte_ub_ring_get_total_queues();
	if (tx_queue_id >= total)
		return -EINVAL;
	in->tx_queues[tx_queue_id].queue_id = tx_queue_id;
	in->tx_queues[tx_queue_id].provider_index =
		rte_ub_ring_queue_to_provider(tx_queue_id);
	in->tx_queues[tx_queue_id].internals = in;
	dev->data->tx_queues[tx_queue_id] = &in->tx_queues[tx_queue_id];
	return 0;
}

static int ub_ring_eth_link_update(struct rte_eth_dev *d __rte_unused,
				   int w __rte_unused)
{ return 0; }

static int ub_ring_eth_stats_get(struct rte_eth_dev *d, struct rte_eth_stats *s)
{ (void)d; memset(s, 0, sizeof(*s)); return 0; }

static int ub_ring_eth_stats_reset(struct rte_eth_dev *d)
{ (void)d; return 0; }

static int noop_ok(struct rte_eth_dev *d __rte_unused) { return 0; }

static int mac_addr_add(struct rte_eth_dev *d __rte_unused,
	struct rte_ether_addr *a __rte_unused, uint32_t i __rte_unused,
	uint32_t p __rte_unused) { return 0; }

static void mac_addr_remove(struct rte_eth_dev *d __rte_unused,
			    uint32_t i __rte_unused) {}

static int ub_ring_eth_set_link_down(struct rte_eth_dev *dev)
{ dev->data->dev_link.link_status = RTE_ETH_LINK_DOWN; return 0; }

static int ub_ring_eth_set_link_up(struct rte_eth_dev *dev)
{ dev->data->dev_link.link_status = RTE_ETH_LINK_UP; return 0; }

static const struct eth_dev_ops ub_ring_eth_ops = {
	.dev_configure       = ub_ring_eth_dev_configure,
	.dev_start           = ub_ring_eth_dev_start,
	.dev_stop            = ub_ring_eth_dev_stop,
	.dev_close           = ub_ring_eth_dev_close,
	.dev_infos_get       = ub_ring_eth_dev_info,
	.rx_queue_setup      = ub_ring_eth_rx_queue_setup,
	.tx_queue_setup      = ub_ring_eth_tx_queue_setup,
	.link_update         = ub_ring_eth_link_update,
	.stats_get           = ub_ring_eth_stats_get,
	.stats_reset         = ub_ring_eth_stats_reset,
	.mac_addr_add        = mac_addr_add,
	.mac_addr_remove     = mac_addr_remove,
	.promiscuous_enable  = noop_ok,
	.promiscuous_disable = noop_ok,
	.allmulticast_enable  = noop_ok,
	.allmulticast_disable = noop_ok,
	.dev_set_link_up     = ub_ring_eth_set_link_up,
	.dev_set_link_down   = ub_ring_eth_set_link_down,
};

/* ---- internal: create ethdev ---- */

static int
do_eth_dev_create(const char *name, uint32_t nb_queues,
		  struct ub_ring_eth_internals *in)
{
	struct rte_eth_dev *eth_dev;
	struct rte_eth_dev_data *data;
	void **rx_queues, **tx_queues;

	rx_queues = rte_calloc_socket(NULL, nb_queues, sizeof(void *), 0,
				      rte_socket_id());
	tx_queues = rte_calloc_socket(NULL, nb_queues, sizeof(void *), 0,
				      rte_socket_id());
	if (!rx_queues || !tx_queues) {
		rte_free(rx_queues);
		rte_free(tx_queues);
		return -ENOMEM;
	}

	for (uint32_t i = 0; i < nb_queues; i++) {
		in->rx_queues[i].queue_id = i;
		in->tx_queues[i].queue_id = i;
	}

	eth_dev = rte_eth_dev_allocate(name);
	if (!eth_dev) {
		UB_RING_ETH_LOG(ERR, "[%s] rte_eth_dev_allocate failed", name);
		rte_free(rx_queues);
		rte_free(tx_queues);
		return -ENODEV;
	}

	data = eth_dev->data;
	data->rx_queues    = rx_queues;
	data->tx_queues    = tx_queues;
	data->nb_rx_queues = (uint16_t)nb_queues;
	data->nb_tx_queues = (uint16_t)nb_queues;
	data->dev_private  = in;
	data->numa_node     = rte_socket_id();
	data->promiscuous   = 1;
	data->all_multicast = 1;
	data->dev_flags    |= RTE_ETH_DEV_AUTOFILL_QUEUE_XSTATS;

	for (uint32_t i = 0; i < nb_queues; i++) {
		data->rx_queues[i] = &in->rx_queues[i];
		data->tx_queues[i] = &in->tx_queues[i];
	}

	in->address = (struct rte_ether_addr){
		.addr_bytes = { 0x02, 0x00, 0x00, 0x00, 0x00, 0x00 }
	};
	data->mac_addrs = &in->address;
	data->dev_link = (struct rte_eth_link){
		.link_speed  = RTE_ETH_SPEED_NUM_10G,
		.link_duplex = RTE_ETH_LINK_FULL_DUPLEX,
		.link_status = RTE_ETH_LINK_DOWN,
		.link_autoneg = RTE_ETH_LINK_FIXED,
	};

	eth_dev->dev_ops      = &ub_ring_eth_ops;
	eth_dev->rx_pkt_burst = ub_ring_eth_rx;
	eth_dev->tx_pkt_burst = ub_ring_eth_tx;

	rte_eth_dev_probing_finish(eth_dev);
	return data->port_id;
}

/**
 * Create dummy pools for all provider data ring areas.
 * Each dummy pool is backed by the provider's data ring buffer area
 * so that rte_pktmbuf_free(m) works correctly.
 */
static int
create_dummy_pool(struct ub_ring_eth_internals *in)
{
	struct rte_mempool *mp = rte_mempool_create_empty("ub_eth_dummy",
		1024, 1, 0, 0, rte_socket_id(),
		RTE_MEMPOOL_F_SP_PUT | RTE_MEMPOOL_F_SC_GET);
	if (!mp)
		return -1;

	in->dummy_pool = mp;
	UB_RING_ETH_LOG(INFO, "dummy pool created (empty, no populate)");
	return 0;
}

/* ---- public API ---- */

int
rte_eth_ub_ring_create_provider(const char *name,
	const char **ub_nodes, uint32_t nb_nodes,
	const char *region, const char *shm_prefix,
	size_t shm_size, uint32_t nb_queues, uint32_t queue_depth)
{
	int ret = rte_ub_ring_provider_init(ub_nodes, nb_nodes, region,
					    shm_prefix, shm_size,
					    nb_queues, queue_depth);
	if (ret < 0)
		return ret;

	struct ub_ring_eth_internals *in = rte_zmalloc_socket(NULL, sizeof(*in),
		0, rte_socket_id());
	if (!in)
		return -ENOMEM;

	create_dummy_pool(in);

	return do_eth_dev_create(name, nb_queues, in);
}

int
rte_eth_ub_ring_create_user(const char *name,
	const char **ub_nodes, uint32_t nb_nodes,
	const char *region, const char *shm_prefix,
	uint32_t data_elt_size)
{
	int ret = rte_ub_ring_user_init(ub_nodes, nb_nodes, region,
					shm_prefix, data_elt_size);
	if (ret < 0)
		return ret;

	uint32_t total = rte_ub_ring_get_total_queues();

	struct ub_ring_eth_internals *in = rte_zmalloc_socket(NULL, sizeof(*in),
		0, rte_socket_id());
	if (!in)
		return -ENOMEM;

	create_dummy_pool(in);

	return do_eth_dev_create(name, total, in);
}

int
rte_eth_ub_ring_get_queue_mapping(uint32_t provider_node_index,
				   uint32_t sub_queue,
				   uint32_t *tx_queue_id,
				   uint32_t *rx_queue_id)
{
	return rte_ub_ring_get_queue_mapping(provider_node_index,
					      sub_queue, tx_queue_id,
					      rx_queue_id);
}
