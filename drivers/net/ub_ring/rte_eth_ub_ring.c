/* SPDX-License-Identifier: BSD-3-Clause
 *
 * ub_ring ethdev PMD — follows rte_eth_from_rings() vdev pattern.
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
#include <rte_kvargs.h>
#include <rte_cycles.h>
#include <rte_ethdev.h>
#include <bus_vdev_driver.h>
#include <ethdev_driver.h>

#include <rte_ub_ring.h>
#include <rte_eth_ub_ring.h>

RTE_LOG_REGISTER_DEFAULT(ub_ring_eth_logtype, INFO);
#define RTE_LOGTYPE_UB_RING_ETH ub_ring_eth_logtype
#define UB_RING_ETH_LOG(level, ...) \
	RTE_LOG_LINE_PREFIX(level, UB_RING_ETH, "%s(): ", __func__, __VA_ARGS__)

/* flow-trace helpers: at most one line per second.
 * Only compiled in when -DUB_RING_FLOW_TRACE is set (debug builds —
 * see drivers/net/ub_ring/meson.build); expands to nothing otherwise. */
#ifdef UB_RING_FLOW_TRACE
#define UB_RING_ETH_FLOW_N(site, n) do { \
	static uint64_t cnt, prev, last; \
	uint64_t now = rte_rdtsc(); \
	cnt += (n); \
	if (now - last >= rte_get_tsc_hz()) { \
		UB_RING_ETH_LOG(INFO, "[flow] %s: total=%" PRIu64 \
			" (+%" PRIu64 "/s)", site, cnt, cnt - prev); \
		prev = cnt; \
		last = now; \
	} \
} while (0)

#define UB_RING_ETH_FLOW_WARN(fmt, ...) do { \
	static uint64_t last; \
	uint64_t now = rte_rdtsc(); \
	if (now - last >= rte_get_tsc_hz()) { \
		last = now; \
		UB_RING_ETH_LOG(WARNING, "[flow] " fmt, ##__VA_ARGS__); \
	} \
} while (0)
#else
#define UB_RING_ETH_FLOW_N(site, n) do { } while (0)
#define UB_RING_ETH_FLOW_WARN(fmt, ...) do { } while (0)
#endif

#define UB_RING_ETH_MAX_BURST 128

struct ub_ring_eth_internals;
struct ub_ring_eth_queue {
	uint32_t queue_id, provider_index;
	struct ub_ring_eth_internals *internals;
};
struct ub_ring_eth_internals {
	struct ub_ring_eth_queue rx_queues[RTE_MAX_QUEUES_PER_PORT];
	struct ub_ring_eth_queue tx_queues[RTE_MAX_QUEUES_PER_PORT];
	struct rte_ub_ring_handle *ub_ring_h;
	struct rte_mempool *dummy_pool;
	struct rte_ether_addr address;
};

static void *enc(struct rte_mbuf *m, uint32_t pi_pref,
		 struct rte_ub_ring_handle *h, uint32_t *pi_out)
{
	/* copy only header + priv + headroom + data (no tailroom) */
	uint32_t need = sizeof(struct rte_mbuf) + m->priv_size +
			m->data_off + m->data_len;

	/*
	 * Try the target provider's data ring first; if exhausted, fall
	 * back to any other node's data ring. The actual provider index
	 * is returned in *pi_out so the caller can pack ring elements
	 * correctly.
	 */
	uint32_t pi = pi_pref;
	void *buf = NULL;
	if (rte_ub_ring_get_data_elt_size(h, pi) >= need)
		buf = rte_ub_ring_alloc_buf(h, pi);
	if (!buf) {
		for (uint32_t c = 0; c < UB_RING_MAX_NODES; c++) {
			if (c == pi_pref)
				continue;
			if (rte_ub_ring_get_data_elt_size(h, c) < need)
				continue;
			buf = rte_ub_ring_alloc_buf(h, c);
			if (buf) { pi = c; break; }
		}
	}
	if (!buf) return NULL;

	/* encode chain first (recursive) */
	void *nb = NULL;
	uint32_t nb_pi = 0;
	if (m->next) {
		nb = enc(m->next, pi, h, &nb_pi);
		if (!nb) { rte_ub_ring_free_buf(h, buf); return NULL; }
	}

	/*
	 * Optimisation: mutate the LOCAL mbuf's next/pool/buf_len,
	 * memcpy once, then restore. Avoids separate slow writes to
	 * shared memory.
	 */
	struct rte_mbuf *save_next = m->next;
	struct rte_mempool *save_pool = m->pool;
	uint16_t save_buf_len = m->buf_len;

	if (nb) {
		/* pack the child pointer as {pi, element-base offset} so
		 * dec() can rebuild it */
		uint64_t off = ub_ring_buf_off(
			rte_ub_ring_get_data_buf_area(h, nb_pi), nb);
		m->next = (struct rte_mbuf *)(uintptr_t)(((uint64_t)nb_pi << 56) | off);
	} else {
		m->next = (struct rte_mbuf *)(uintptr_t)UINT64_MAX;
	}
	m->pool = NULL;
	m->buf_len = m->data_off + m->data_len; /* no tailroom in shm */

	memcpy(buf, m, need);

	m->next = save_next;
	m->pool = save_pool;
	m->buf_len = save_buf_len;
	*pi_out = pi;
	return buf;
}

/* free an ENCODED buffer chain (next = packed {ni, off} or UINT64_MAX) */
static void
free_enc_chain(struct rte_ub_ring_handle *h, void *buf)
{
	while (buf) {
		struct rte_mbuf *m = buf;
		uint64_t nv = (uint64_t)(uintptr_t)m->next;
		void *next = NULL;
		if (nv != UINT64_MAX) {
			uint8_t ni = (uint8_t)(nv >> 56);
			uint64_t off = nv & 0x00FFFFFFFFFFFFFFULL;
			next = ub_ring_buf_ptr(
				rte_ub_ring_get_data_buf_area(h, ni), off);
		}
		rte_ub_ring_free_buf(h, buf);
		buf = next;
	}
}

static struct rte_mbuf *dec(void *buf, struct rte_mempool *dp, struct rte_ub_ring_handle *h, int depth)
{
	struct rte_mbuf *m = buf;
	uint64_t nv = (uint64_t)(uintptr_t)m->next;

	if (depth > 8) {
		UB_RING_ETH_LOG(ERR, "decode depth exceeded: buf=%p depth=%d next=0x%" PRIx64,
				buf, depth, nv);
		m->next = NULL;
		return m;
	}

	m->buf_addr = (char *)m + sizeof(struct rte_mbuf) + m->priv_size; m->pool = dp;
	if (nv != UINT64_MAX) {
		uint8_t ni = (uint8_t)(nv >> 56); uint64_t off = nv & 0x00FFFFFFFFFFFFFFULL;
		void *nb = ub_ring_buf_ptr(rte_ub_ring_get_data_buf_area(h, ni), off);
		if (nb == buf) {
			UB_RING_ETH_LOG(ERR, "self-referencing next: buf=%p ni=%u off=0x%" PRIx64,
					buf, ni, off);
			m->next = NULL;
			return m;
		}
		m->next = dec(nb, dp, h, depth + 1);
	} else m->next = NULL;
	return m;
}

static void drn(struct rte_mempool *d, struct rte_ub_ring_handle *h)
{
	if (!d) return;
	void *o[32];
	/* fixed-size all-or-nothing batches, no dependence on the pool
	 * internals: freed mbufs sit in the workers' per-lcore caches
	 * until they overflow into the ring; bypass the cache here
	 * (get_bulk would strand up to cache_size objects in this
	 * lcore's cache). A partial tail below one batch stays in the
	 * ring and is picked up once the next flush tops it up — the
	 * drain lcore loops this anyway */
	for (;;) {
		/* mempool ops contract: 0 = got n objects, <0 = empty */
		if (rte_mempool_ops_dequeue_bulk(d, o, 32) < 0)
			break;
		/* one bulk SP enqueue per drain pass instead of one CAS
		 * per buffer (workers would serialize on the return ring) */
		rte_ub_ring_return_burst(h, o, 32);
		UB_RING_ETH_FLOW_N("drn", 32);
	}
}

static struct rte_mempool *mkp(void)
{ /* MPSC: put side is MP (each worker's cache overflow flushes into the
   * ring), get side is SC (only the dedicated drain lcore — so the
   * ring backend dequeue is lock-free). cache_size=256 batches the
   * workers' frees so the hot pktmbuf_free path never takes the ring's
   * MP enqueue lock.
   * 64K objects: more than every in-flight buffer can be freed at once */
	struct rte_mempool *mp=rte_mempool_create("ub_eth_dummy",64*1024,0,256,0,NULL,NULL,NULL,NULL,rte_socket_id(),MEMPOOL_F_SC_GET);
	if (mp) {
		/* drain all placeholder objects so only freed mbufs are
		 * ever retrieved. Dequeue straight from the ring (get_bulk
		 * would refill this lcore's cache with placeholders):
		 * fixed-size batches, then one-by-one for the tail.
		 * mempool ops contract: 0 = got n objects, <0 = empty */
		void *o[64];
		for (;;) {
			if (rte_mempool_ops_dequeue_bulk(mp, o, 64) < 0)
				break;
		}
		for (;;) {
			if (rte_mempool_ops_dequeue_bulk(mp, o, 1) < 0)
				break;
		}
	}
	return mp; }

static uint16_t eth_rx(void *q, struct rte_mbuf **b, uint16_t nb) {
	struct ub_ring_eth_queue *rq=q; struct ub_ring_eth_internals *in=rq->internals;
	struct rte_ub_ring_handle *h=in->ub_ring_h;
	/* freed mbufs are drained back to the providers by the app's own
	 * lcore calling rte_eth_ub_ring_drain() — not on the RX path */
	void *raw[UB_RING_ETH_MAX_BURST];
	uint16_t n=rte_ub_ring_is_mpmc_enabled(h)?rte_ub_ring_mc_recv_burst(h,rq->queue_id,raw,RTE_MIN(nb,UB_RING_ETH_MAX_BURST)):rte_ub_ring_recv_burst(h,rq->queue_id,raw,RTE_MIN(nb,UB_RING_ETH_MAX_BURST));
	/* prefetch the next buffer while decoding the current one: the
	 * header lines are cold (the peer's remote write invalidated them
	 * last round trip), so one-mbuf look-ahead hides the refill latency */
	for(uint16_t i=0;i<n;i++){
		if (i + 1 < n) {
			rte_prefetch0(raw[i + 1]);
			rte_prefetch0((char *)raw[i + 1] + 64);
		}
		b[i]=dec(raw[i],in->dummy_pool,h,0);
	}
	return n; }

static uint16_t eth_tx(void *q, struct rte_mbuf **m, uint16_t nb) {
	struct ub_ring_eth_queue *rq=q; struct ub_ring_eth_internals *in=rq->internals;
	struct rte_ub_ring_handle *h=in->ub_ring_h;
	void *raw[nb]; uint32_t raw_pi[nb]; uint16_t c=0;
	for(uint16_t i=0;i<nb;i++){
		uint32_t pi_used;
		void*buf=enc(m[i],rq->provider_index,h,&pi_used);
		if(!buf)break;
		raw[c]=buf; raw_pi[c]=pi_used; c++;
	}
	if(!c){
		UB_RING_ETH_FLOW_WARN("eth_tx: enc failed (no free buffer anywhere)");
		return 0;
	}
	/*
	 * Ownership model:
	 *  - sent buffers: ownership transferred to peer; peer returns
	 *    them via pktmbuf_free → drain → return ring → collect.
	 *  - unsent buffers (s < c): ownership still local, return to
	 *    local data ring. fail-fast send makes s ∈ {0, c}.
	 */
	uint16_t s = rte_ub_ring_is_mpmc_enabled(h)
		? rte_ub_ring_mp_send_burst_pi(h, rq->queue_id, raw_pi, raw, c)
		: rte_ub_ring_send_burst_pi(h, rq->queue_id, raw_pi, raw, c);
	for (uint16_t i = s; i < c; i++)
		free_enc_chain(h, raw[i]);
	if (s)
		UB_RING_ETH_FLOW_N("eth_tx", s);
	return s; }

static int eth_cfg(struct rte_eth_dev*d __rte_unused){return 0;}
static int eth_start(struct rte_eth_dev*d){d->data->dev_link.link_status=RTE_ETH_LINK_UP;return 0;}
static int eth_stop(struct rte_eth_dev*d){d->data->dev_started=0;d->data->dev_link.link_status=RTE_ETH_LINK_DOWN;return 0;}
static int eth_close(struct rte_eth_dev*d){struct ub_ring_eth_internals*in=d->data->dev_private;eth_stop(d);if(in->dummy_pool){drn(in->dummy_pool,in->ub_ring_h);rte_mempool_free(in->dummy_pool);}d->data->mac_addrs=NULL;rte_free(in);d->data->dev_private=NULL;return 0;}
static int eth_info(struct rte_eth_dev*d,struct rte_eth_dev_info*i){struct ub_ring_eth_internals*in=d->data->dev_private;uint32_t t=rte_ub_ring_get_total_queues(in->ub_ring_h);i->max_mac_addrs=1;i->max_rx_pktlen=(uint32_t)-1;i->max_rx_queues=(uint16_t)t;i->max_tx_queues=(uint16_t)t;i->min_rx_bufsize=0;i->rx_offload_capa=RTE_ETH_RX_OFFLOAD_SCATTER;i->tx_offload_capa=RTE_ETH_TX_OFFLOAD_MULTI_SEGS;return 0;}
static int eth_rxq(struct rte_eth_dev*d,uint16_t q,uint16_t nb __rte_unused,unsigned int s __rte_unused,const struct rte_eth_rxconf*c __rte_unused,struct rte_mempool*p __rte_unused){struct ub_ring_eth_internals*in=d->data->dev_private;uint32_t t=rte_ub_ring_get_total_queues(in->ub_ring_h);if(q>=t)return -EINVAL;in->rx_queues[q].queue_id=q;in->rx_queues[q].provider_index=rte_ub_ring_queue_to_provider(in->ub_ring_h,q);in->rx_queues[q].internals=in;d->data->rx_queues[q]=&in->rx_queues[q];return 0;}
static int eth_txq(struct rte_eth_dev*d,uint16_t q,uint16_t nb __rte_unused,unsigned int s __rte_unused,const struct rte_eth_txconf*c __rte_unused){struct ub_ring_eth_internals*in=d->data->dev_private;uint32_t t=rte_ub_ring_get_total_queues(in->ub_ring_h);if(q>=t)return -EINVAL;in->tx_queues[q].queue_id=q;in->tx_queues[q].provider_index=rte_ub_ring_queue_to_provider(in->ub_ring_h,q);in->tx_queues[q].internals=in;d->data->tx_queues[q]=&in->tx_queues[q];return 0;}
static int eth_link(struct rte_eth_dev*d __rte_unused,int w __rte_unused){return 0;}
static int eth_stats(struct rte_eth_dev*d,struct rte_eth_stats*s){(void)d;memset(s,0,sizeof(*s));return 0;}
static int eth_sreset(struct rte_eth_dev*d){(void)d;return 0;}
static int eth_ok(struct rte_eth_dev*d __rte_unused){return 0;}
static int eth_ma(struct rte_eth_dev*d __rte_unused,struct rte_ether_addr*a __rte_unused,uint32_t i __rte_unused,uint32_t p __rte_unused){return 0;}
static void eth_mr(struct rte_eth_dev*d __rte_unused,uint32_t i __rte_unused){}
static int eth_ld(struct rte_eth_dev*d){d->data->dev_link.link_status=RTE_ETH_LINK_DOWN;return 0;}
static int eth_lu(struct rte_eth_dev*d){d->data->dev_link.link_status=RTE_ETH_LINK_UP;return 0;}

static const struct eth_dev_ops eth_ops={
	.dev_configure=eth_cfg,.dev_start=eth_start,.dev_stop=eth_stop,.dev_close=eth_close,
	.dev_infos_get=eth_info,.rx_queue_setup=eth_rxq,.tx_queue_setup=eth_txq,
	.link_update=eth_link,.stats_get=eth_stats,.stats_reset=eth_sreset,
	.mac_addr_add=eth_ma,.mac_addr_remove=eth_mr,
	.promiscuous_enable=eth_ok,.promiscuous_disable=eth_ok,
	.allmulticast_enable=eth_ok,.allmulticast_disable=eth_ok,
	.dev_set_link_up=eth_lu,.dev_set_link_down=eth_ld,
};

static int parse_addr(const char *k __rte_unused, const char *v, void *extra)
{ *(void **)extra = (void *)(uintptr_t)strtoull(v, NULL, 0); return 0; }

static int
vdev_probe(struct rte_vdev_device *vdev)
{
	struct rte_kvargs *kv = rte_kvargs_parse(rte_vdev_device_args(vdev), NULL);
	void *hp = NULL;
	if (kv) { rte_kvargs_process(kv, "addr", parse_addr, &hp); rte_kvargs_free(kv); }
	if (!hp) return -EINVAL;

	const char *dn = rte_vdev_device_name(vdev);

	struct rte_ub_ring_handle *h = hp;
	uint32_t nq = rte_ub_ring_get_total_queues(h);
	struct ub_ring_eth_internals *in = rte_zmalloc_socket(NULL, sizeof(*in), 0, rte_socket_id());
	if (!in) return -ENOMEM; in->ub_ring_h = h; in->dummy_pool = mkp();

	void **rxq = rte_calloc_socket(NULL, nq, sizeof(void *), 0, rte_socket_id());
	void **txq = rte_calloc_socket(NULL, nq, sizeof(void *), 0, rte_socket_id());
	if (!rxq || !txq) { rte_free(rxq); rte_free(txq); rte_free(in); return -ENOMEM; }
	for (uint32_t i = 0; i < nq; i++) {
		in->rx_queues[i].queue_id = i;
		in->rx_queues[i].provider_index = rte_ub_ring_queue_to_provider(h, i);
		in->rx_queues[i].internals = in;
		in->tx_queues[i].queue_id = i;
		in->tx_queues[i].provider_index = rte_ub_ring_queue_to_provider(h, i);
		in->tx_queues[i].internals = in;
	}
	struct rte_eth_dev *ed = rte_eth_dev_allocate(dn);
	if (!ed) { rte_free(rxq); rte_free(txq); rte_free(in); return -ENODEV; }
	ed->data->rx_queues = rxq; ed->data->tx_queues = txq;
	ed->data->nb_rx_queues = (uint16_t)nq; ed->data->nb_tx_queues = (uint16_t)nq;
	ed->data->dev_private = in; ed->data->numa_node = rte_socket_id();
	ed->data->promiscuous = 1; ed->data->all_multicast = 1;
	ed->data->dev_flags |= RTE_ETH_DEV_AUTOFILL_QUEUE_XSTATS;
	for (uint32_t i = 0; i < nq; i++) {
		ed->data->rx_queues[i] = &in->rx_queues[i];
		ed->data->tx_queues[i] = &in->tx_queues[i];
	}
	in->address = (struct rte_ether_addr){ .addr_bytes = {0x02} };
	ed->data->mac_addrs = &in->address;
	ed->data->dev_link = (struct rte_eth_link){
		.link_speed = RTE_ETH_SPEED_NUM_10G, .link_duplex = RTE_ETH_LINK_FULL_DUPLEX,
		.link_status = RTE_ETH_LINK_DOWN, .link_autoneg = RTE_ETH_LINK_FIXED,
	};
	ed->device = &vdev->device;
	ed->dev_ops = &eth_ops;
	ed->rx_pkt_burst = eth_rx;
	ed->tx_pkt_burst = eth_tx;
	rte_eth_dev_probing_finish(ed);
	return 0;
}

static int vdev_remove(struct rte_vdev_device *vdev)
{
	const char *n = rte_vdev_device_name(vdev);
	struct rte_eth_dev *ed = rte_eth_dev_allocated(n);
	if (!ed) return 0;
	eth_close(ed);
	rte_eth_dev_release_port(ed);
	return 0;
}

static struct rte_vdev_driver vdrv = { .probe = vdev_probe, .remove = vdev_remove };
RTE_PMD_REGISTER_VDEV(net_ub_ring, vdrv);

int
rte_eth_ub_ring_attach(const char *name, struct rte_ub_ring_handle *h)
{
	char vname[64], args[32];
	int ret;

	ret = snprintf(vname, sizeof(vname), "net_ub_ring_%s", name);
	if (ret >= (int)sizeof(vname)) { rte_errno = ENAMETOOLONG; return -1; }

	snprintf(args, sizeof(args), "addr=%p", (void *)h);

	ret = rte_vdev_init(vname, args);
	if (ret) return -1;

	uint16_t pid;
	ret = rte_eth_dev_get_port_by_name(vname, &pid);
	return ret ? -1 : (int)pid;
}

void
rte_eth_ub_ring_drain(int port_id)
{
	if (port_id < 0 || port_id >= RTE_MAX_ETHPORTS)
		return;
	struct rte_eth_dev *d = &rte_eth_devices[port_id];
	if (!d->data || !d->data->dev_private)
		return;
	struct ub_ring_eth_internals *in = d->data->dev_private;
	drn(in->dummy_pool, in->ub_ring_h);
}
