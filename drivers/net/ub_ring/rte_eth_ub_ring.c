/* SPDX-License-Identifier: BSD-3-Clause
 *
 * ub_ring ethdev PMD (Layer 3) — thin mbuf wrapper over ub_ring handle.
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
	uint32_t provider_index;
	struct ub_ring_eth_internals *internals;
};

struct ub_ring_eth_internals {
	struct ub_ring_eth_queue rx_queues[RTE_MAX_QUEUES_PER_PORT];
	struct ub_ring_eth_queue tx_queues[RTE_MAX_QUEUES_PER_PORT];
	struct rte_ub_ring_handle *ub_ring_h;
	struct rte_mempool *dummy_pool;
	struct rte_ether_addr address;
};

/* ---- encode / decode (helpers take handle directly) ---- */

static void *
encode_mbuf_to_buf(struct rte_mbuf *m, uint32_t provider_index,
		   struct rte_ub_ring_handle *h)
{
	uint32_t elt_size = rte_ub_ring_get_data_elt_size(h, provider_index);

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

	void *buf = rte_ub_ring_alloc_buf(h, provider_index);
	if (!buf)
		return NULL;

	struct rte_mbuf *dst = buf;
	memcpy(buf, m, need);

	if (m->next) {
		void *next_buf = encode_mbuf_to_buf(m->next, provider_index, h);
		if (next_buf) {
			uint64_t off = (uint64_t)((char *)next_buf -
				(char *)rte_ub_ring_get_data_buf_area(h, provider_index));
			dst->next = (struct rte_mbuf *)(uintptr_t)
				(((uint64_t)provider_index << 56) | off);
		} else {
			UB_RING_ETH_LOG(ERR, "encode chained next failed");
			rte_ub_ring_free_buf(h, buf);
			return NULL;
		}
	} else {
		dst->next = (struct rte_mbuf *)(uintptr_t)UINT64_MAX;
	}

	dst->pool = NULL;
	return buf;
}

static struct rte_mbuf *
decode_buf_to_mbuf(void *buf, struct rte_mempool *dummy_pool,
		   struct rte_ub_ring_handle *h)
{
	struct rte_mbuf *m = buf;

	m->buf_addr = (char *)m + sizeof(struct rte_mbuf) + m->priv_size;
	m->pool     = dummy_pool;

	uint64_t next_val = (uint64_t)(uintptr_t)m->next;
	if (next_val != UINT64_MAX) {
		uint8_t  ni  = (uint8_t)(next_val >> 56);
		uint64_t off = next_val & 0x00FFFFFFFFFFFFFFULL;
		void *next_buf = (char *)rte_ub_ring_get_data_buf_area(h, ni) + off;
		m->next = decode_buf_to_mbuf(next_buf, dummy_pool, h);
	} else {
		m->next = NULL;
	}

	return m;
}

static void
drain_dummy_pool(struct rte_mempool *dummy, struct rte_ub_ring_handle *h)
{
	if (!dummy)
		return;

	void *objs[32];
	int n = rte_mempool_get_bulk(dummy, objs, 32);
	while (n > 0) {
		for (int i = 0; i < n; i++)
			rte_ub_ring_free_buf(h, objs[i]);
		n = rte_mempool_get_bulk(dummy, objs, 32);
	}
}

static int
create_dummy_pool(struct ub_ring_eth_internals *in)
{
	struct rte_mempool *mp = rte_mempool_create("ub_eth_dummy",
		1024, 64, 0, 0, NULL, NULL, NULL, NULL,
		rte_socket_id(), RTE_MEMPOOL_F_SP_PUT | RTE_MEMPOOL_F_SC_GET);
	if (!mp)
		return -1;

	void *objs[64];
	int n;
	while ((n = rte_mempool_get_bulk(mp, objs, 64)) > 0)
		;

	in->dummy_pool = mp;
	return 0;
}

/* ---- RX / TX ---- */

static uint16_t
ub_ring_eth_rx(void *q, struct rte_mbuf **bufs, uint16_t nb_bufs)
{
	struct ub_ring_eth_queue *rq = q;
	struct ub_ring_eth_internals *in = rq->internals;
	struct rte_ub_ring_handle *h = in->ub_ring_h;

	drain_dummy_pool(in->dummy_pool, h);

	void *raw[UB_RING_ETH_MAX_BURST];
	uint16_t n;

	if (h->mpmc_enabled)
		n = rte_ub_ring_mc_recv_burst(h, rq->queue_id, raw,
			RTE_MIN(nb_bufs, UB_RING_ETH_MAX_BURST));
	else
		n = rte_ub_ring_recv_burst(h, rq->queue_id, raw,
			RTE_MIN(nb_bufs, UB_RING_ETH_MAX_BURST));

	for (uint16_t i = 0; i < n; i++)
		bufs[i] = decode_buf_to_mbuf(raw[i], in->dummy_pool, h);

	return n;
}

static uint16_t
ub_ring_eth_tx(void *q, struct rte_mbuf **mbufs, uint16_t nb_bufs)
{
	struct ub_ring_eth_queue *rq = q;
	struct ub_ring_eth_internals *in = rq->internals;
	struct rte_ub_ring_handle *h = in->ub_ring_h;
	void *raw[nb_bufs];
	uint16_t count = 0;

	for (uint16_t i = 0; i < nb_bufs; i++) {
		void *buf = encode_mbuf_to_buf(mbufs[i], rq->provider_index, h);
		if (!buf) break;
		raw[count++] = buf;
	}

	if (count == 0)
		return 0;

	uint16_t sent;
	if (h->mpmc_enabled)
		sent = rte_ub_ring_mp_send_burst(h, rq->queue_id, raw, count);
	else
		sent = rte_ub_ring_send_burst(h, rq->queue_id, raw, count);

	for (uint16_t i = sent; i < count; i++)
		rte_ub_ring_free_buf(h, raw[i]);

	return sent;
}

/* ---- eth_dev_ops ---- */

static int ub_ring_eth_dev_configure(struct rte_eth_dev *d __rte_unused) { return 0; }
static int ub_ring_eth_dev_start(struct rte_eth_dev *dev)
{ dev->data->dev_link.link_status = RTE_ETH_LINK_UP; return 0; }
static int ub_ring_eth_dev_stop(struct rte_eth_dev *dev)
{ dev->data->dev_started = 0; dev->data->dev_link.link_status = RTE_ETH_LINK_DOWN; return 0; }

static int ub_ring_eth_dev_close(struct rte_eth_dev *dev)
{
	struct ub_ring_eth_internals *in = dev->data->dev_private;
	ub_ring_eth_dev_stop(dev);
	if (in->dummy_pool) {
		drain_dummy_pool(in->dummy_pool, in->ub_ring_h);
		rte_mempool_free(in->dummy_pool);
	}
	dev->data->mac_addrs = NULL;
	rte_free(in);
	dev->data->dev_private = NULL;
	/* note: caller is responsible for rte_ub_ring_fini(h) */
	return 0;
}

static int ub_ring_eth_dev_info(struct rte_eth_dev *dev, struct rte_eth_dev_info *info)
{
	struct ub_ring_eth_internals *in = dev->data->dev_private;
	uint32_t total = rte_ub_ring_get_total_queues(in->ub_ring_h);
	info->max_mac_addrs  = 1;
	info->max_rx_pktlen  = (uint32_t)-1;
	info->max_rx_queues  = (uint16_t)total;
	info->max_tx_queues  = (uint16_t)total;
	info->min_rx_bufsize = 0;
	info->rx_offload_capa = RTE_ETH_RX_OFFLOAD_SCATTER;
	info->tx_offload_capa = RTE_ETH_TX_OFFLOAD_MULTI_SEGS;
	return 0;
}

static int ub_ring_eth_rx_queue_setup(struct rte_eth_dev *dev, uint16_t rx_queue_id,
	uint16_t nb_rx_desc __rte_unused, unsigned int socket_id __rte_unused,
	const struct rte_eth_rxconf *rx_conf __rte_unused,
	struct rte_mempool *mb_pool __rte_unused)
{
	struct ub_ring_eth_internals *in = dev->data->dev_private;
	struct rte_ub_ring_handle *h = in->ub_ring_h;
	uint32_t total = rte_ub_ring_get_total_queues(h);
	if (rx_queue_id >= total) return -EINVAL;
	in->rx_queues[rx_queue_id].queue_id = rx_queue_id;
	in->rx_queues[rx_queue_id].provider_index =
		rte_ub_ring_queue_to_provider(h, rx_queue_id);
	in->rx_queues[rx_queue_id].internals = in;
	dev->data->rx_queues[rx_queue_id] = &in->rx_queues[rx_queue_id];
	return 0;
}

static int ub_ring_eth_tx_queue_setup(struct rte_eth_dev *dev, uint16_t tx_queue_id,
	uint16_t nb_tx_desc __rte_unused, unsigned int socket_id __rte_unused,
	const struct rte_eth_txconf *tx_conf __rte_unused)
{
	struct ub_ring_eth_internals *in = dev->data->dev_private;
	struct rte_ub_ring_handle *h = in->ub_ring_h;
	uint32_t total = rte_ub_ring_get_total_queues(h);
	if (tx_queue_id >= total) return -EINVAL;
	in->tx_queues[tx_queue_id].queue_id = tx_queue_id;
	in->tx_queues[tx_queue_id].provider_index =
		rte_ub_ring_queue_to_provider(h, tx_queue_id);
	in->tx_queues[tx_queue_id].internals = in;
	dev->data->tx_queues[tx_queue_id] = &in->tx_queues[tx_queue_id];
	return 0;
}

static int ub_ring_eth_link_update(struct rte_eth_dev *d __rte_unused, int w __rte_unused) { return 0; }
static int ub_ring_eth_stats_get(struct rte_eth_dev *d, struct rte_eth_stats *s)
{ (void)d; memset(s, 0, sizeof(*s)); return 0; }
static int ub_ring_eth_stats_reset(struct rte_eth_dev *d) { (void)d; return 0; }
static int noop_ok(struct rte_eth_dev *d __rte_unused) { return 0; }
static int mac_addr_add(struct rte_eth_dev *d __rte_unused,
	struct rte_ether_addr *a __rte_unused, uint32_t i __rte_unused,
	uint32_t p __rte_unused) { return 0; }
static void mac_addr_remove(struct rte_eth_dev *d __rte_unused, uint32_t i __rte_unused) {}
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

/* ---- ethdev create ---- */

static int
do_eth_dev_create(const char *name, uint32_t nb_queues,
		  struct ub_ring_eth_internals *in)
{
	struct rte_eth_dev *eth_dev;
	struct rte_eth_dev_data *data;
	void **rx_queues, **tx_queues;

	rx_queues = rte_calloc_socket(NULL, nb_queues, sizeof(void *), 0, rte_socket_id());
	tx_queues = rte_calloc_socket(NULL, nb_queues, sizeof(void *), 0, rte_socket_id());
	if (!rx_queues || !tx_queues) { rte_free(rx_queues); rte_free(tx_queues); return -ENOMEM; }

	for (uint32_t i = 0; i < nb_queues; i++) {
		in->rx_queues[i].queue_id  = i;
		in->rx_queues[i].provider_index = rte_ub_ring_queue_to_provider(in->ub_ring_h, i);
		in->rx_queues[i].internals = in;
		in->tx_queues[i].queue_id  = i;
		in->tx_queues[i].provider_index = rte_ub_ring_queue_to_provider(in->ub_ring_h, i);
		in->tx_queues[i].internals = in;
	}

	eth_dev = rte_eth_dev_allocate(name);
	if (!eth_dev) { UB_RING_ETH_LOG(ERR, "[%s] alloc failed", name); rte_free(rx_queues); rte_free(tx_queues); return -ENODEV; }

	data = eth_dev->data;
	data->rx_queues    = rx_queues;
	data->tx_queues    = tx_queues;
	data->nb_rx_queues = (uint16_t)nb_queues;
	data->nb_tx_queues = (uint16_t)nb_queues;
	data->dev_private  = in;
	data->numa_node    = rte_socket_id();
	data->promiscuous  = 1;
	data->all_multicast = 1;
	data->dev_flags    |= RTE_ETH_DEV_AUTOFILL_QUEUE_XSTATS;

	for (uint32_t i = 0; i < nb_queues; i++) {
		data->rx_queues[i] = &in->rx_queues[i];
		data->tx_queues[i] = &in->tx_queues[i];
	}

	in->address = (struct rte_ether_addr){ .addr_bytes = {0x02} };
	data->mac_addrs = &in->address;
	data->dev_link = (struct rte_eth_link){
		.link_speed = RTE_ETH_SPEED_NUM_10G,
		.link_duplex = RTE_ETH_LINK_FULL_DUPLEX,
		.link_status = RTE_ETH_LINK_DOWN,
		.link_autoneg = RTE_ETH_LINK_FIXED,
	};

	eth_dev->device       = NULL;
	eth_dev->dev_ops      = &ub_ring_eth_ops;
	eth_dev->rx_pkt_burst = ub_ring_eth_rx;
	eth_dev->tx_pkt_burst = ub_ring_eth_tx;

	rte_eth_dev_probing_finish(eth_dev);
	return data->port_id;
}

/* ---- public API ---- */

int
rte_eth_ub_ring_attach(const char *name, struct rte_ub_ring_handle *h)
{
	if (!name || !h)
		return -EINVAL;

	uint32_t nb_q = rte_ub_ring_get_total_queues(h);

	struct ub_ring_eth_internals *in = rte_zmalloc_socket(NULL, sizeof(*in),
		0, rte_socket_id());
	if (!in) return -ENOMEM;
	in->ub_ring_h = h;
	create_dummy_pool(in);

	int ret = do_eth_dev_create(name, nb_q, in);
	if (ret < 0) {
		if (in->dummy_pool) rte_mempool_free(in->dummy_pool);
		rte_free(in);
	}
	return ret;
}
