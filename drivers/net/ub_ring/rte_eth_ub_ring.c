/* SPDX-License-Identifier: BSD-3-Clause
 *
 * ub_ring ethdev PMD (Layer 3) — mbuf adapter over lib/ub_ring (Layer 2).
 * Registered as a vdev: loaded via -d librte_net_ub_ring.so
 */

#include <stdlib.h>
#include <string.h>

#include <rte_eal.h>
#include <rte_kvargs.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_errno.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ring.h>
#include <rte_ethdev.h>
#include <bus_vdev_driver.h>
#include <ethdev_driver.h>

#include <rte_ub_ring.h>
#include <rte_eth_ub_ring.h>

RTE_LOG_REGISTER_DEFAULT(ub_ring_eth_logtype, NOTICE);
#define RTE_LOGTYPE_UB_RING_ETH ub_ring_eth_logtype
#define UB_RING_ETH_LOG(level, ...) \
	RTE_LOG_LINE_PREFIX(level, UB_RING_ETH, "%s(): ", __func__, __VA_ARGS__)

#define UB_RING_ETH_MAX_BURST 128

/* ---- devargs keys ---- */
#define UB_RING_ARG_ROLE       "r"
#define UB_RING_ARG_UB_NODES   "ns"
#define UB_RING_ARG_NB_NODES   "n"
#define UB_RING_ARG_REGION     "reg"
#define UB_RING_ARG_SHM_PREFIX "pre"
#define UB_RING_ARG_SHM_SIZE   "sz"
#define UB_RING_ARG_NB_QUEUES  "nq"
#define UB_RING_ARG_QUEUE_DEPTH "qd"
#define UB_RING_ARG_DATA_ELT   "de"

/* ---- devargs struct ---- */

struct ub_ring_eth_devargs {
	char     **ub_nodes;
	uint32_t   nb_nodes;
	char       region[64];
	char       shm_prefix[64];
	size_t     shm_size;
	uint32_t   nb_queues;
	uint32_t   queue_depth;
	uint32_t   data_elt_size;
	bool       is_provider;
};

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
	struct rte_mempool *dummy_pool;
	struct rte_ether_addr address;
};

/* ---- encode / decode ---- */

static void *
encode_mbuf_to_buf(struct rte_mbuf *m, uint32_t provider_index)
{
	uint32_t elt_size = rte_ub_ring_get_data_elt_size(provider_index);

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
	memcpy(buf, m, need);

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
		dst->next = (struct rte_mbuf *)(uintptr_t)UINT64_MAX;
	}

	dst->pool = NULL;
	return buf;
}

static struct rte_mbuf *
decode_buf_to_mbuf(void *buf, struct rte_mempool *dummy_pool)
{
	struct rte_mbuf *m = buf;

	m->buf_addr = (char *)m + sizeof(struct rte_mbuf) + m->priv_size;
	m->pool     = dummy_pool;

	uint64_t next_val = (uint64_t)(uintptr_t)m->next;
	if (next_val != UINT64_MAX) {
		uint8_t  ni  = (uint8_t)(next_val >> 56);
		uint64_t off = next_val & 0x00FFFFFFFFFFFFFFULL;
		void *next_buf = (char *)rte_ub_ring_get_data_buf_area(ni) + off;
		m->next = decode_buf_to_mbuf(next_buf, dummy_pool);
	} else {
		m->next = NULL;
	}

	return m;
}

/* ---- drain ---- */

static void
drain_dummy_pool(struct rte_mempool *dummy)
{
	if (!dummy)
		return;

	void *objs[32];
	int n = rte_mempool_get_bulk(dummy, objs, 32);
	while (n > 0) {
		for (int i = 0; i < n; i++)
			rte_ub_ring_free_buf(objs[i]);
		n = rte_mempool_get_bulk(dummy, objs, 32);
	}
}

/* ---- dummy pool ---- */

static int
create_dummy_pool(struct ub_ring_eth_internals *in)
{
	struct rte_mempool *mp = rte_mempool_create("ub_eth_dummy",
		1024, 64, 0, 0, NULL, NULL, NULL, NULL,
		rte_socket_id(), RTE_MEMPOOL_F_SP_PUT | RTE_MEMPOOL_F_SC_GET);
	if (!mp)
		return -1;

	/* drain immediately — pool ring is valid, but we want it empty */
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

	drain_dummy_pool(in->dummy_pool);

	void *raw[UB_RING_ETH_MAX_BURST];
	uint16_t n = rte_ub_ring_recv_burst(rq->queue_id, raw,
		RTE_MIN(nb_bufs, UB_RING_ETH_MAX_BURST));

	for (uint16_t i = 0; i < n; i++)
		bufs[i] = decode_buf_to_mbuf(raw[i], in->dummy_pool);

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
	for (uint16_t i = sent; i < count; i++)
		rte_ub_ring_free_buf(raw[i]);

	return sent;
}

/* ---- eth_dev_ops ---- */

static int ub_ring_eth_dev_configure(struct rte_eth_dev *d __rte_unused) { return 0; }

static int ub_ring_eth_dev_start(struct rte_eth_dev *dev)
{ dev->data->dev_link.link_status = RTE_ETH_LINK_UP; return 0; }

static int ub_ring_eth_dev_stop(struct rte_eth_dev *dev)
{
	dev->data->dev_started = 0;
	dev->data->dev_link.link_status = RTE_ETH_LINK_DOWN;
	return 0;
}

static int ub_ring_eth_dev_close(struct rte_eth_dev *dev)
{
	struct ub_ring_eth_internals *in = dev->data->dev_private;
	ub_ring_eth_dev_stop(dev);
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

static int ub_ring_eth_dev_info(struct rte_eth_dev *dev, struct rte_eth_dev_info *info)
{
	uint32_t total = rte_ub_ring_get_total_queues();
	info->max_mac_addrs  = 1;
	info->max_rx_pktlen  = (uint32_t)-1;
	info->max_rx_queues  = (uint16_t)total;
	info->max_tx_queues  = (uint16_t)total;
	info->min_rx_bufsize = 0;
	info->rx_offload_capa = RTE_ETH_RX_OFFLOAD_SCATTER;
	info->tx_offload_capa = RTE_ETH_TX_OFFLOAD_MULTI_SEGS;
	(void)dev;
	return 0;
}

static int ub_ring_eth_rx_queue_setup(struct rte_eth_dev *dev, uint16_t rx_queue_id,
	uint16_t nb_rx_desc __rte_unused, unsigned int socket_id __rte_unused,
	const struct rte_eth_rxconf *rx_conf __rte_unused,
	struct rte_mempool *mb_pool __rte_unused)
{
	struct ub_ring_eth_internals *in = dev->data->dev_private;
	uint32_t total = rte_ub_ring_get_total_queues();
	if (rx_queue_id >= total) return -EINVAL;
	in->rx_queues[rx_queue_id].queue_id = rx_queue_id;
	in->rx_queues[rx_queue_id].provider_index = rte_ub_ring_queue_to_provider(rx_queue_id);
	in->rx_queues[rx_queue_id].internals = in;
	dev->data->rx_queues[rx_queue_id] = &in->rx_queues[rx_queue_id];
	return 0;
}

static int ub_ring_eth_tx_queue_setup(struct rte_eth_dev *dev, uint16_t tx_queue_id,
	uint16_t nb_tx_desc __rte_unused, unsigned int socket_id __rte_unused,
	const struct rte_eth_txconf *tx_conf __rte_unused)
{
	struct ub_ring_eth_internals *in = dev->data->dev_private;
	uint32_t total = rte_ub_ring_get_total_queues();
	if (tx_queue_id >= total) return -EINVAL;
	in->tx_queues[tx_queue_id].queue_id = tx_queue_id;
	in->tx_queues[tx_queue_id].provider_index = rte_ub_ring_queue_to_provider(tx_queue_id);
	in->tx_queues[tx_queue_id].internals = in;
	dev->data->tx_queues[tx_queue_id] = &in->tx_queues[tx_queue_id];
	return 0;
}

static int ub_ring_eth_link_update(struct rte_eth_dev *d __rte_unused, int w __rte_unused)
{ return 0; }
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
		  struct ub_ring_eth_internals *in,
		  struct rte_device *device)
{
	struct rte_eth_dev *eth_dev;
	struct rte_eth_dev_data *data;
	void **rx_queues, **tx_queues;

	rx_queues = rte_calloc_socket(NULL, nb_queues, sizeof(void *), 0, rte_socket_id());
	tx_queues = rte_calloc_socket(NULL, nb_queues, sizeof(void *), 0, rte_socket_id());
	if (!rx_queues || !tx_queues) {
		rte_free(rx_queues); rte_free(tx_queues);
		return -ENOMEM;
	}

	for (uint32_t i = 0; i < nb_queues; i++) {
		in->rx_queues[i].queue_id  = i;
		in->rx_queues[i].provider_index = rte_ub_ring_queue_to_provider(i);
		in->rx_queues[i].internals = in;
		in->tx_queues[i].queue_id  = i;
		in->tx_queues[i].provider_index = rte_ub_ring_queue_to_provider(i);
		in->tx_queues[i].internals = in;
	}

	eth_dev = rte_eth_dev_allocate(name);
	if (!eth_dev) {
		rte_free(rx_queues); rte_free(tx_queues);
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

	eth_dev->device       = device;
	eth_dev->dev_ops      = &ub_ring_eth_ops;
	eth_dev->rx_pkt_burst = ub_ring_eth_rx;
	eth_dev->tx_pkt_burst = ub_ring_eth_tx;

	rte_eth_dev_probing_finish(eth_dev);
	return data->port_id;
}

/* ---- devargs parsing ---- */

static int
parse_nodes(const char *key __rte_unused, const char *value, void *extra)
{
	struct ub_ring_eth_devargs *da = extra;
	char *s = strdup(value);
	if (!s) return -ENOMEM;

	da->nb_nodes = 0;
	char *tok = strtok(s, "|");
	while (tok && da->nb_nodes < 64) {
		da->ub_nodes[da->nb_nodes++] = strdup(tok);
		tok = strtok(NULL, "|");
	}
	free(s);
	return 0;
}

static int
parse_uint32(const char *key __rte_unused, const char *value, void *extra)
{
	*(uint32_t *)extra = (uint32_t)strtoul(value, NULL, 0);
	return 0;
}

static int
parse_size(const char *key __rte_unused, const char *value, void *extra)
{
	*(size_t *)extra = strtoull(value, NULL, 0);
	return 0;
}

static int
parse_str(const char *key __rte_unused, const char *value, void *extra)
{
	strncpy((char *)extra, value, 63);
	return 0;
}

static int
parse_role(const char *key __rte_unused, const char *value, void *extra)
{
	bool *is_provider = extra;
	*is_provider = (strcmp(value, "pv") == 0);
	return 0;
}

/* ---- vdev probe / remove ---- */

static int
ub_ring_eth_vdev_probe(struct rte_vdev_device *vdev)
{
	const char *devname = rte_vdev_device_name(vdev);
	struct ub_ring_eth_devargs da;
	struct rte_kvargs *kv;
	int ret;

	memset(&da, 0, sizeof(da));
	da.ub_nodes = calloc(64, sizeof(char *));

	kv = rte_kvargs_parse(rte_vdev_device_args(vdev), NULL);
	if (!kv) { ret = -EINVAL; goto out; }

	rte_kvargs_process(kv, UB_RING_ARG_ROLE,       parse_role,  &da.is_provider);
	rte_kvargs_process(kv, UB_RING_ARG_UB_NODES,   parse_nodes, &da);
	rte_kvargs_process(kv, UB_RING_ARG_NB_NODES,   parse_uint32, &da.nb_nodes);
	rte_kvargs_process(kv, UB_RING_ARG_REGION,     parse_str,   da.region);
	rte_kvargs_process(kv, UB_RING_ARG_SHM_PREFIX, parse_str,   da.shm_prefix);
	rte_kvargs_process(kv, UB_RING_ARG_SHM_SIZE,   parse_size,  &da.shm_size);
	rte_kvargs_process(kv, UB_RING_ARG_NB_QUEUES,  parse_uint32, &da.nb_queues);
	rte_kvargs_process(kv, UB_RING_ARG_QUEUE_DEPTH,parse_uint32, &da.queue_depth);
	rte_kvargs_process(kv, UB_RING_ARG_DATA_ELT,   parse_uint32, &da.data_elt_size);
	rte_kvargs_free(kv);

	/* strip "net_ub_ring_" prefix */
	const char *prefix = "net_ub_ring_";
	const char *short_name = devname;
	if (strncmp(devname, prefix, strlen(prefix)) == 0)
		short_name = devname + strlen(prefix);

	if (da.is_provider) {
		ret = rte_ub_ring_provider_init((const char **)da.ub_nodes,
			da.nb_nodes, da.region, da.shm_prefix, da.shm_size,
			da.nb_queues, da.queue_depth);
	} else {
		ret = rte_ub_ring_user_init((const char **)da.ub_nodes,
			da.nb_nodes, da.region, da.shm_prefix, da.data_elt_size);
	}
	if (ret < 0) goto out;

	struct ub_ring_eth_internals *in = rte_zmalloc_socket(NULL, sizeof(*in),
		0, rte_socket_id());
	if (!in) { ret = -ENOMEM; goto out; }

	create_dummy_pool(in);

	uint32_t nb_q = da.is_provider ? da.nb_queues : rte_ub_ring_get_total_queues();
	ret = do_eth_dev_create(short_name, nb_q, in, &vdev->device);

out:
	if (da.ub_nodes) {
		for (uint32_t i = 0; i < da.nb_nodes; i++)
			free((void *)da.ub_nodes[i]);
		free(da.ub_nodes);
	}
	return ret < 0 ? ret : 0;
}

static int
ub_ring_eth_vdev_remove(struct rte_vdev_device *vdev)
{
	const char *name = rte_vdev_device_name(vdev);
	struct rte_eth_dev *eth_dev = rte_eth_dev_allocated(name);
	if (!eth_dev) return 0;
	ub_ring_eth_dev_close(eth_dev);
	rte_eth_dev_release_port(eth_dev);
	return 0;
}

static struct rte_vdev_driver ub_ring_eth_vdev_drv = {
	.probe  = ub_ring_eth_vdev_probe,
	.remove = ub_ring_eth_vdev_remove,
};

RTE_PMD_REGISTER_VDEV(net_ub_ring, ub_ring_eth_vdev_drv);

/* ---- public API ---- */

int
rte_eth_ub_ring_get_queue_mapping(uint32_t provider_node_index,
	uint32_t sub_queue, uint32_t *tx_queue_id, uint32_t *rx_queue_id)
{
	return rte_ub_ring_get_queue_mapping(provider_node_index,
		sub_queue, tx_queue_id, rx_queue_id);
}
