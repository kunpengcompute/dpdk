/* SPDX-License-Identifier: BSD-3-Clause */

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <sys/mman.h>

#include <ubs_mem.h>

#include <rte_eal.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_errno.h>
#include <rte_cycles.h>
#include <rte_ring.h>

#include <rte_ub_ring.h>

RTE_LOG_REGISTER_DEFAULT(ub_ring_logtype, INFO);
#define RTE_LOGTYPE_UB_RING ub_ring_logtype
#define UB_RING_LOG(level, ...) \
	RTE_LOG_LINE_PREFIX(level, UB_RING, "%s(): ", __func__, __VA_ARGS__)

#define UB_RING_MD_ALIGN 64
#define UB_RING_POLL_US  50000
#define UB_RING_POLL_S   60

/* flow-trace helpers: per-site counters with at most one log line per
 * second, so the hot path never floods the console. Steady state should
 * show roughly equal +N/s on every site.
 * Only compiled in when -DUB_RING_FLOW_TRACE is set (debug builds —
 * see lib/ub_ring/meson.build); expands to nothing otherwise. */
#ifdef UB_RING_FLOW_TRACE
#define UB_RING_FLOW_N(site, n) do { \
	static uint64_t cnt, prev, last; \
	uint64_t now = rte_rdtsc(); \
	cnt += (n); \
	if (now - last >= rte_get_tsc_hz()) { \
		UB_RING_LOG(INFO, "[flow] %s: total=%" PRIu64 \
			" (+%" PRIu64 " pkts/s)", site, cnt, cnt - prev); \
		prev = cnt; \
		last = now; \
	} \
} while (0)

#define UB_RING_FLOW_WARN(fmt, ...) do { \
	static uint64_t last; \
	uint64_t now = rte_rdtsc(); \
	if (now - last >= rte_get_tsc_hz()) { \
		last = now; \
		UB_RING_LOG(WARNING, "[flow] " fmt, ##__VA_ARGS__); \
	} \
} while (0)
#else
#define UB_RING_FLOW_N(site, n) do { } while (0)
#define UB_RING_FLOW_WARN(fmt, ...) do { } while (0)
#endif

/* loss counters: ALWAYS compiled (independent of UB_RING_FLOW_TRACE) so
 * every drop site on the buffer path shows up even in release builds.
 * Each site prints at most one ERR line per second with the delta. */
#define UB_RING_LOSS_ADD(site, n, why) do { \
	static uint64_t cnt, prev, last; \
	uint64_t now = rte_rdtsc(); \
	cnt += (n); \
	if (now - last >= rte_get_tsc_hz()) { \
		UB_RING_LOG(ERR, "[loss] %s: +%" PRIu64 \
			" pkts/s dropped (%s)", site, cnt - prev, why); \
		prev = cnt; \
		last = now; \
	} \
} while (0)

static const char *ubs_strerror(int rc)
{
	switch (rc) {
	case UBSM_OK: return "OK";
	case UBSM_ERR_PARAM_INVALID: return "PARAM_INVALID";
	case UBSM_ERR_NOPERM: return "NOPERM";
	case UBSM_ERR_MEMORY: return "MEMORY";
	case UBSM_ERR_UNIMPL: return "UNIMPL";
	case UBSM_CHECK_RESOURCE_ERROR: return "CHECK_RESOURCE_ERROR";
	case UBSM_ERR_MEMLIB: return "MEMLIB";
	case UBSM_ERR_NO_NEEDED: return "NO_NEEDED";
	case UBSM_ERR_BUSY: return "BUSY";
	case UBSM_ERR_NOT_FOUND: return "NOT_FOUND";
	case UBSM_ERR_ALREADY_EXIST: return "ALREADY_EXIST";
	case UBSM_ERR_MALLOC_FAIL: return "MALLOC_FAIL";
	case UBSM_ERR_RECORD: return "RECORD";
	case UBSM_ERR_IN_USING: return "IN_USING";
	case UBSM_ERR_NOT_SUPPORTED: return "NOT_SUPPORTED";
	case UBSM_ERR_NET: return "NET";
	case UBSM_ERR_UBSE: return "UBSE";
	case UBSM_ERR_OBMM: return "OBMM";
	case UBSM_ERR_LOCK_NOT_SUPPORTED: return "LOCK_NOT_SUPPORTED";
	case UBSM_ERR_LOCK_ALREADY_LOCKED: return "LOCK_ALREADY_LOCKED";
	case UBSM_ERR_DLOCK: return "DLOCK";
	case UBSM_ERR_BUFF: return "BUFF";
	default: return "UNKNOWN";
	}
}

/* per-lcore alloc cache: each lcore's cache is touched by exactly one
 * thread — no locks needed. The shared MP/MC data ring is accessed
 * only for bulk refills via MC dequeue (CAS), the single
 * cross-thread synchronization point — same pattern as a per-lcore
 * object cache.
 * Capacity is 2x the refill size: a full refill (512) plus one TX
 * burst's failed returns (≤512 by burst contract) always fit, so the
 * TX failure path can return elements to the cache without ever
 * overflowing — no data-ring round trip on backpressure. */
#define UB_RING_ALLOC_CACHE_SIZE 1024 /* total per-lcore slots */
#define UB_RING_ALLOC_REFILL      512  /* bulk MC pull per refill */
struct alloc_cache {
	uint32_t len;
	void *offs[UB_RING_ALLOC_CACHE_SIZE];
};

struct rte_ub_ring_handle {
	uint32_t nb_nodes, my_node_index;
	enum ub_ring_role role;

	struct {
		void            *shm_base;
		struct rte_ring *return_ring;   /* in shm, SP/SC: provider drain
						   * lcore enqueues, user collect
						   * lcore dequeues */
		void            *data_buf_area;
		uint32_t         data_elt_size;
		uint32_t         data_nb_bufs;   /* data ring depth (mx) of this peer */
		/* metadata cached locally at init — no shm reads afterwards */
		uint64_t         total_size;
		uint32_t         nb_queues;
		char             node_name[64];
	} peers[UB_RING_MAX_NODES];

	/* local data rings (user only, one per provider shm) */
	struct rte_ring **data_rings;

	/* per-lcore alloc caches (user only, one per provider) */
	struct alloc_cache *alloc_caches[UB_RING_MAX_NODES];

	struct rte_ring **rx_rings, **tx_rings, **shadow_tx, **shadow_rx;
	/* per-queue flush leftovers (user, mpmc only): elements that could
	 * not enter the remote tx_ring are stashed here — NOT re-enqueued
	 * to shadow_tx — and retried on the next flush round. Keeps the
	 * flush lcore from becoming a second MP producer on shadow_tx and
	 * avoids 512 single-element re-enqueues on a full remote ring */
	void **tx_stash;
	uint16_t *tx_stash_len;
	/* collect pipeline mid ring (user only, SPSC): the fetch thread
	 * SP-enqueues from the remote return rings, the dispatch thread
	 * SC-dequeues and routes into the local data rings */
	struct rte_ring *collect_mid;
	bool mpmc_enabled;
	uint32_t total_queues;
	uint32_t queue_to_provider[UB_RING_MAX_QUEUES];
	uint32_t provider_base_qid[UB_RING_MAX_NODES];
	char region_name[64], shm_prefix[64];
};

static inline struct ub_ring_metadata *
peer_meta(struct rte_ub_ring_handle *h, uint32_t i)
{ return (struct ub_ring_metadata *)h->peers[i].shm_base; }

static int cmp_node(const void *a, const void *b)
{
	const char *sa = *(const char **)a, *sb = *(const char **)b;
	if (!sa) return sb ? -1 : 0;
	if (!sb) return 1;
	return strcmp(sa, sb);
}

/* init-time data ring layout resolution for one peer: same math as
 * user_init, reads the meta once. Called from provider_init only —
 * no hot-path use; the fields are read-only afterwards */
static void
init_peer_data_ring(struct rte_ub_ring_handle *h, uint32_t i)
{
	struct ub_ring_metadata *m = peer_meta(h, i);
	if (!m || !(m->state & UB_RING_STATE_DATA_RING_READY)) return;
	h->peers[i].data_elt_size = m->data_elt_size;
	h->peers[i].data_nb_bufs = m->data_nb_bufs;
	h->peers[i].return_ring = (struct rte_ring *)((char *)h->peers[i].shm_base + m->data_ring_offset);
	size_t rsz = rte_ring_get_memsize(m->data_nb_bufs);
	/* same 128B alignment as user_init: both sides must agree */
	h->peers[i].data_buf_area = (void *)RTE_ALIGN(
		(uintptr_t)((char *)h->peers[i].return_ring + rsz), 128);
}

/* buffer -> packed {pi, offset} element: both come from the 128B
 * ownership header written by user_init (O(1), no node scan) */
static void *
buf_elem(void *buf)
{
	struct ub_ring_buf_hdr *hd = ub_ring_buf_hdr(buf);
	return ub_ring_elem_pack((uint8_t)hd->node_index, hd->offset);
}

/* the return ring used for a return action: the CALLING provider's own
 * ring, not the one of the buffer's owning provider. Resolved once at
 * provider_init — read-only afterwards */
static struct rte_ring *
own_return_ring(struct rte_ub_ring_handle *h)
{
	return h->peers[h->my_node_index].return_ring;
}

static size_t crs(uint32_t qd) { return rte_ring_get_memsize(qd); }
static size_t cms(uint32_t nq)
{ return RTE_ALIGN(sizeof(struct ub_ring_metadata) + nq*2*sizeof(uint64_t), UB_RING_MD_ALIGN); }
static size_t cras(uint32_t nq, size_t rs) { return nq*2*rs; }

static void
write_meta(struct ub_ring_metadata *m, uint32_t nb, uint32_t nq, uint32_t ni,
	uint32_t qd, uint64_t ts, uint64_t ra_off, uint64_t ra_sz, const char *hn)
{
	memset(m, 0, sizeof(*m));
	m->magic=UB_RING_MAGIC; m->version=UB_RING_VERSION;
	m->nb_nodes=nb; m->nb_queues=nq; m->node_index=ni; m->queue_depth=qd; m->state=0;
	m->total_size=ts; m->ring_area_offset=ra_off; m->ring_area_size=ra_sz;
	strncpy(m->node_name, hn, 63);
}

static int wait_meta(struct ub_ring_metadata *m)
{
	for (int i=0; i<UB_RING_POLL_S*20; i++) {
		if (m->magic==UB_RING_MAGIC && m->version==UB_RING_VERSION && (m->state&UB_RING_STATE_META_READY)) return 0;
		usleep(UB_RING_POLL_US);
	}
	return -1;
}

static int
resolve_index(const char **pn, uint32_t np, const char **un, uint32_t nu,
	uint32_t *mi, uint32_t *tot, char (*names)[64])
{
	char my[MAX_HOST_NAME_DESC_LENGTH];
	if (gethostname(my,sizeof(my))) return -1;
	*tot=np+nu;
	const char **all=rte_malloc(NULL,*tot*sizeof(char*),0); if(!all) return -1;
	uint32_t k=0;
	for (uint32_t i=0;i<np;i++)all[k++]=pn[i];
	for (uint32_t i=0;i<nu;i++)all[k++]=un[i];
	qsort(all,*tot,sizeof(char*),cmp_node);
	for (uint32_t i=0;i<*tot;i++){
		if (names) snprintf(names[i],64,"%s",all[i]);
		if(!strcmp(all[i],my)){*mi=i;rte_free(all);return 0;}}
	rte_free(all); return -1;
}

static int
wait_shms(const char *pre, const char **pn, uint32_t np)
{
	ubsmem_shmem_desc_t l[64];
	for (int t=0;t<UB_RING_POLL_S*20;t++){
		uint32_t c=64; if(ubsmem_shmem_list_lookup(pre,l,&c)!=UBSM_OK){usleep(UB_RING_POLL_US);continue;}
		uint32_t f=0;
		for(uint32_t i=0;i<np;i++){char e[128];snprintf(e,128,"%s_%s",pre,pn[i]);
			for(uint32_t k=0;k<c;k++)if(!strcmp(l[k].name,e)){f++;break;}}
		if(f==np)return 0;
		usleep(UB_RING_POLL_US);
	}
	return -1;
}

/* ---- Provider ---- */

struct rte_ub_ring_handle *
rte_ub_ring_provider_init(const char **pn, uint32_t np, const char **un, uint32_t nu,
	const char *reg, const char *pre, size_t sz, uint32_t nq, uint32_t qd)
{
	char my[MAX_HOST_NAME_DESC_LENGTH]; ubsmem_options_t o; ubsmem_region_attributes_t a;
	uint32_t mi, tot; char nm[128]; void *sh=NULL; size_t ms, rs, ras; int rc;

	if (!pn||!np||np>UB_RING_MAX_NODES||nu>UB_RING_MAX_NODES||!reg||!pre||!sz||!nq||nq>UB_RING_MAX_QUEUES||!qd||!rte_is_power_of_2(qd)){rte_errno=EINVAL;return NULL;}
	struct rte_ub_ring_handle *h=rte_zmalloc(NULL,sizeof(*h),0); if(!h)return NULL;
	h->role=UB_RING_ROLE_PROVIDER; snprintf(h->region_name,64,"%s",reg); snprintf(h->shm_prefix,64,"%s",pre);
	char names[UB_RING_MAX_NODES][64];
	if (resolve_index(pn,np,un,nu,&mi,&tot,names)<0)goto fail;
	h->nb_nodes=tot; h->my_node_index=mi;
	for (uint32_t j=0;j<tot;j++) snprintf(h->peers[j].node_name,64,"%s",names[j]);
	if (gethostname(my,sizeof(my)))goto fail;
	UB_RING_LOG(INFO,"provider: %s idx=%u/%u nq=%u qd=%u sz=%zu",my,mi,tot,nq,qd,sz);
	if ((rc=ubsmem_init_attributes(&o))||(rc=ubsmem_initialize(&o))){UB_RING_LOG(ERR,"ubsmem:%s(%d)",ubs_strerror(rc),rc);goto fail_ub;}
	memset(&a,0,sizeof(a));a.host_num=tot;
	for(uint32_t i=0;i<np;i++){strncpy(a.hosts[i].host_name,pn[i],MAX_HOST_NAME_DESC_LENGTH-1);a.hosts[i].affinity=true;}
	for(uint32_t i=0;i<nu;i++){strncpy(a.hosts[np+i].host_name,un[i],MAX_HOST_NAME_DESC_LENGTH-1);a.hosts[np+i].affinity=true;}
	rc=ubsmem_create_region(reg,0,&a);if(rc!=UBSM_OK&&rc!=UBSM_ERR_ALREADY_EXIST){UB_RING_LOG(ERR,"region:%s(%d)",ubs_strerror(rc),rc);goto fail_ub;}
	snprintf(nm,128,"%s_%s",pre,my); ubsmem_shmem_deallocate(nm);
	ubs_mem_provider_t src_loc;
	strncpy(src_loc.host_name, my, MAX_HOST_NAME_DESC_LENGTH - 1);
	src_loc.socket_id = 0;
	src_loc.numa_id   = 0;
	src_loc.port_id   = UINT32_MAX;
	if((rc=ubsmem_shmem_allocate_with_provider(&src_loc,nm,sz,0600,UBSM_FLAG_ONLY_IMPORT_NONCACHE|UBSM_FLAG_WR_DELAY_COMP|UBSM_FLAG_MMAP_HUGETLB_PMD))){UB_RING_LOG(ERR,"alloc:%s(%d)",ubs_strerror(rc),rc);goto fail_reg;}
	if((rc=ubsmem_shmem_map(NULL,sz,PROT_READ|PROT_WRITE,MAP_SHARED,nm,0,&sh))){UB_RING_LOG(ERR,"map:%s(%d)",ubs_strerror(rc),rc);goto fail_reg;}
	memset(sh,0,sz); h->peers[mi].shm_base=sh;
	h->peers[mi].total_size=(uint64_t)sz; h->peers[mi].nb_queues=nq;
	ms=cms(nq); rs=crs(qd); ras=cras(nq,rs);
	for(uint32_t q=0;q<nq;q++){char rn[32];void*rx,*tx;
		rx=(char*)sh+ms+q*rs;tx=(char*)sh+ms+(nq+q)*rs;
		snprintf(rn,32,"rx_q%u",q);if((rc=rte_ring_init((struct rte_ring*)rx,rn,qd,RING_F_SP_ENQ|RING_F_SC_DEQ))<0)goto fail_reg;
		snprintf(rn,32,"tx_q%u",q);if((rc=rte_ring_init((struct rte_ring*)tx,rn,qd,RING_F_SP_ENQ|RING_F_SC_DEQ))<0)goto fail_reg;
	}
	struct ub_ring_metadata*om=(struct ub_ring_metadata*)sh;
	write_meta(om,tot,nq,mi,qd,sz,ms,ras,my);
	for(uint32_t q=0;q<nq;q++){om->queue_offsets[q]=ms+q*rs;om->queue_offsets[nq+q]=ms+(nq+q)*rs;}
	om->state|=UB_RING_STATE_META_READY; h->peers[mi].return_ring=NULL; h->peers[mi].data_buf_area=NULL;
	/* Wait for the USER to initialize OUR shm (data rings) BEFORE
	 * mapping any peer. A peer may still be recreating its own shm
	 * (dealloc+alloc) and an early map pins it, so its recreate
	 * fails with IN_USING. The user only initializes us after it
	 * saw ALL provider shms, so mapping peers afterwards is safe. */
	UB_RING_LOG(INFO,"waiting for user to init my shm...");
	{
		struct ub_ring_metadata *m=(struct ub_ring_metadata*)sh;
		int wi; for(wi=0;wi<UB_RING_POLL_S*20;wi++){
			if(m->state&UB_RING_STATE_DATA_RING_READY)break;
			usleep(UB_RING_POLL_US);
		}
		if(wi>=UB_RING_POLL_S*20){UB_RING_LOG(ERR,"user did not init my shm in %ds",UB_RING_POLL_S);goto fail_reg;}
	}
	for(uint32_t i=0;i<np;i++){if(!strcmp(pn[i],my))continue;
		char pn2[128];snprintf(pn2,128,"%s_%s",pre,pn[i]);ubsmem_shmem_desc_t dl[32];uint32_t dc=32;uint64_t ps=0;
		ubsmem_shmem_list_lookup(pre,dl,&dc);for(uint32_t k=0;k<dc;k++)if(!strcmp(dl[k].name,pn2)){ps=dl[k].size;break;}if(!ps)continue;
		void*psh=NULL;if(ubsmem_shmem_map(NULL,ps,PROT_READ|PROT_WRITE,MAP_SHARED,pn2,0,&psh))continue;
		uint32_t pi=UINT32_MAX;for(uint32_t j=0;j<tot;j++){const char*n=j<np?pn[j]:un[j-np];if(!strcmp(n,pn[i])){pi=j;break;}}if(pi>=UB_RING_MAX_NODES)continue;
		h->peers[pi].shm_base=psh;if(wait_meta((struct ub_ring_metadata*)psh)<0)goto fail_reg;
		h->peers[pi].total_size=((struct ub_ring_metadata*)psh)->total_size;
		h->peers[pi].nb_queues=((struct ub_ring_metadata*)psh)->nb_queues;
		h->peers[pi].return_ring=NULL; h->peers[pi].data_buf_area=NULL;
		UB_RING_LOG(INFO,"peer %s (pi=%u)",pn2,pi);
	}
	h->total_queues=nq; h->rx_rings=rte_malloc(NULL,nq*sizeof(void*),0); h->tx_rings=rte_malloc(NULL,nq*sizeof(void*),0);
	if(!h->rx_rings||!h->tx_rings){UB_RING_LOG(ERR,"ring arrays NULL (nq=%u)",nq);goto fail_reg;}
	for(uint32_t q=0;q<nq;q++){h->rx_rings[q]=(void*)((char*)sh+om->queue_offsets[q]);h->tx_rings[q]=(void*)((char*)sh+om->queue_offsets[nq+q]);h->queue_to_provider[q]=mi;}
	h->provider_base_qid[mi]=0;
	/* cross-node support: the user lays out the data rings on EVERY
	 * provider shm. Wait for all of them, then resolve every mapped
	 * peer once — after this, each provider's data_buf_area,
	 * return_ring and elt_size are directly available, so buffers
	 * from any provider can be received and the hot paths
	 * never look anything up again. On timeout keep going: unresolved
	 * peers stay NULL (their buffers cannot be received) until the
	 * next restart */
	for (int i = 0; i < UB_RING_POLL_S * 20; i++) {
		uint32_t nb_shm = 0, nb_ready = 0;
		for (uint32_t j = 0; j < h->nb_nodes; j++) {
			struct ub_ring_metadata *m = peer_meta(h, j);
			if (!m)
				continue;
			nb_shm++;
			if (m->state & UB_RING_STATE_DATA_RING_READY)
				nb_ready++;
		}
		if (nb_ready == nb_shm)
			break;
		usleep(UB_RING_POLL_US);
	}
	for (uint32_t j = 0; j < h->nb_nodes; j++)
		init_peer_data_ring(h, j);
	UB_RING_LOG(INFO,"provider done"); return h;
fail_reg: ubsmem_destroy_region(reg);
fail_ub: ubsmem_finalize();
fail: rte_free(h); return NULL;
}

/* ---- User ---- */

struct rte_ub_ring_handle *
rte_ub_ring_user_init(const char **pn, uint32_t np, const char **un, uint32_t nu,
	const char *reg, const char *pre, uint32_t elt)
{
	char my[MAX_HOST_NAME_DESC_LENGTH]; ubsmem_options_t o; ubsmem_region_attributes_t a;
	uint32_t mi, tot; int rc;
	if (!pn||!np||np>UB_RING_MAX_NODES||nu>UB_RING_MAX_NODES||!reg||!pre||!elt){rte_errno=EINVAL;return NULL;}
	/* element = 128B ownership header + caller payload, as a 128B
	 * multiple: every buffer starts on a 128B boundary (given the
	 * 128B-aligned buffer area start) */
	elt = RTE_ALIGN(elt + UB_RING_BUF_HDR_SIZE, 128);
	struct rte_ub_ring_handle *h=rte_zmalloc(NULL,sizeof(*h),0); if(!h)return NULL;
	h->role=UB_RING_ROLE_USER; snprintf(h->region_name,64,"%s",reg); snprintf(h->shm_prefix,64,"%s",pre);
	char names[UB_RING_MAX_NODES][64];
	if (resolve_index(pn,np,un,nu,&mi,&tot,names)<0)goto fail;
	h->nb_nodes=tot; h->my_node_index=mi;
	for (uint32_t j=0;j<tot;j++) snprintf(h->peers[j].node_name,64,"%s",names[j]);
	if (gethostname(my,sizeof(my)))goto fail;
	UB_RING_LOG(INFO,"user: %s idx=%u/%u elt=%u",my,mi,tot,elt);
	if ((rc=ubsmem_init_attributes(&o))||(rc=ubsmem_initialize(&o))){UB_RING_LOG(ERR,"ubsmem:%s(%d)",ubs_strerror(rc),rc);goto fail_ub;}
	memset(&a,0,sizeof(a));a.host_num=tot;
	for(uint32_t i=0;i<np;i++){strncpy(a.hosts[i].host_name,pn[i],MAX_HOST_NAME_DESC_LENGTH-1);a.hosts[i].affinity=true;}
	for(uint32_t i=0;i<nu;i++){strncpy(a.hosts[np+i].host_name,un[i],MAX_HOST_NAME_DESC_LENGTH-1);a.hosts[np+i].affinity=true;}
	rc=ubsmem_create_region(reg,0,&a);if(rc!=UBSM_OK&&rc!=UBSM_ERR_ALREADY_EXIST){UB_RING_LOG(ERR,"region:%s(%d)",ubs_strerror(rc),rc);goto fail_ub;}
	UB_RING_LOG(INFO,"waiting for %u provider shms...",np);
	if (wait_shms(pre,pn,np)<0){UB_RING_LOG(ERR,"timeout");goto fail_reg;}

	/* create local data rings (one per provider) */
	h->data_rings = rte_zmalloc(NULL, UB_RING_MAX_NODES*sizeof(void*), 0);
	if (!h->data_rings) goto fail_reg;

	uint32_t nb_prov=0, pi_arr[UB_RING_MAX_NODES], pq_arr[UB_RING_MAX_NODES];
	for (uint32_t i=0;i<np;i++){
		if (!strcmp(pn[i],my)) continue;
		char pn2[128]; snprintf(pn2,128,"%s_%s",pre,pn[i]);
		ubsmem_shmem_desc_t dl[32];uint32_t dc=32;uint64_t ps=0;
		ubsmem_shmem_list_lookup(pre,dl,&dc); for(uint32_t k=0;k<dc;k++)if(!strcmp(dl[k].name,pn2)){ps=dl[k].size;break;}if(!ps)continue;
		void*psh=NULL;if(ubsmem_shmem_map(NULL,ps,PROT_READ|PROT_WRITE,MAP_SHARED,pn2,0,&psh))continue;
		uint32_t pi=UINT32_MAX;for(uint32_t j=0;j<tot;j++){const char*n=j<np?pn[j]:un[j-np];if(!strcmp(n,pn[i])){pi=j;break;}}if(pi>=UB_RING_MAX_NODES)continue;
		h->peers[pi].shm_base=psh;
		struct ub_ring_metadata*pm=(struct ub_ring_metadata*)psh;
		if (wait_meta(pm)<0){UB_RING_LOG(ERR,"meta timeout %s",pn2);goto fail_reg;}
		/* cache metadata locally — no shm reads after init */
		h->peers[pi].total_size=pm->total_size;
		h->peers[pi].nb_queues=pm->nb_queues;
		h->peers[pi].data_elt_size=elt;

		/* compute data ring layout: return ring + buffer area.
		 * buffer area starts 128B-aligned and each element is a
		 * 128B multiple, so every buffer begins on a 128B boundary
		 * (reserve up to 127B for the area alignment) */
		uint64_t dr_off=pm->ring_area_offset+pm->ring_area_size;
		uint64_t dr_sz=pm->total_size-dr_off;
		uint32_t mx=(uint32_t)((dr_sz-sizeof(struct rte_ring)-128)/(8+elt));
		mx=rte_align32prevpow2(mx);
		while(rte_ring_get_memsize(mx)+128+(size_t)mx*elt>dr_sz)mx/=2;
		if(mx<2){UB_RING_LOG(ERR,"data ring too small");goto fail_reg;}
		size_t rrs=rte_ring_get_memsize(mx);
		void*drb=(char*)psh+dr_off;
		struct rte_ring*rr=(struct rte_ring*)drb;
		char rn[64];snprintf(rn,64,"ret_%s",pn2);
		/* return ring: SPSC — the provider's drain lcore enqueues,
		 * the user's collect lcore dequeues */
		rc=rte_ring_init(rr,rn,mx,RING_F_SP_ENQ|RING_F_SC_DEQ);
		if(rc<0){UB_RING_LOG(ERR,"return ring init fail:%d",rc);goto fail_reg;}
		void*ba=(void*)RTE_ALIGN((uintptr_t)((char*)drb+rrs),128);
		h->peers[pi].return_ring=rr;
		h->peers[pi].data_buf_area=ba;
		h->peers[pi].data_nb_bufs=mx;

		/* create LOCAL data ring
		 * MP enqueue (collect dispatch, free_buf and TX failure-path
		 * returns all enqueue concurrently)
		 * MC dequeue (multiple alloc_buf consumers).
		 * Depth = mx (the shm buffer count) so every buffer can
		 * be in flight at once */
		char dn[64];snprintf(dn,64,"data_%s",pn2);
		h->data_rings[pi]=rte_ring_create(dn,mx,rte_socket_id(),0);
		if(!h->data_rings[pi]){UB_RING_LOG(ERR,"local data ring fail");goto fail_reg;}
		/* pre-allocate the cache array; the entries stay empty and
		 * are bulk-filled only on the first allocation that finds
		 * them empty (see alloc_buf) */
		h->alloc_caches[pi]=rte_zmalloc(NULL,
			RTE_MAX_LCORE*sizeof(struct alloc_cache),0);
		if(!h->alloc_caches[pi]){UB_RING_LOG(ERR,"alloc cache fail");goto fail_reg;}

		/* populate: write each buffer's ownership header (once, then
		 * read-only), then enqueue all free offsets to the local
		 * data ring. Element 0 is reserved: the packed chain value
		 * 0 doubles as the NULL terminator, so no buffer may live
		 * at offset 0 */
		/* elements packed {pi, off} like every other ring — no
		 * format conversion anywhere on the data path */
		for(uint32_t idx=1;idx<mx;idx+=64){void*e[64];uint32_t n=RTE_MIN(64u,mx-idx);
			for(uint32_t k=0;k<n;k++){
				struct ub_ring_buf_hdr *hd=(struct ub_ring_buf_hdr*)((char*)ba+(size_t)(idx+k)*elt);
				hd->node_index=pi;
				/* 64-bit multiply first: (idx+k)*elt can exceed 32 bits
				 * (16G shm / elt=2304 → max offset ≈ 9.4G), a uint32
				 * product wraps before the (uint64_t) cast */
				hd->offset=(uint64_t)(idx+k)*(uint64_t)elt;
				hd->elt_size=elt;
				hd->magic=UB_RING_BUF_MAGIC;
				e[k]=ub_ring_elem_pack((uint8_t)pi,
					(uint64_t)(idx+k)*(uint64_t)elt);
			}
			rte_ring_enqueue_burst(h->data_rings[pi],e,n,NULL);}

		pm->data_ring_offset=dr_off;
		pm->data_elt_size=elt; pm->data_nb_bufs=mx;
		pm->state|=UB_RING_STATE_DATA_RING_READY;
		pi_arr[nb_prov]=pi; pq_arr[nb_prov]=h->peers[pi].nb_queues; nb_prov++;
		UB_RING_LOG(INFO,"data ring %s: %u bufs, local+return",pn2,mx);
	}
	h->peers[mi].return_ring=NULL; h->peers[mi].data_buf_area=NULL;

	uint32_t tq=0;for(uint32_t p=0;p<nb_prov;p++)tq+=pq_arr[p];
	h->total_queues=tq;
	h->rx_rings=rte_malloc(NULL,tq*sizeof(void*),0);h->tx_rings=rte_malloc(NULL,tq*sizeof(void*),0);
	if(!h->rx_rings||!h->tx_rings){UB_RING_LOG(ERR,"ring arrays NULL (tq=%u)",tq);goto fail_reg;}
	uint32_t idx=0;
	for(uint32_t p=0;p<nb_prov;p++){uint32_t pi=pi_arr[p],nq=pq_arr[p];h->provider_base_qid[pi]=idx;
		struct ub_ring_metadata*m=peer_meta(h,pi);void*sh=h->peers[pi].shm_base;
		for(uint32_t q=0;q<nq;q++){h->tx_rings[idx]=(void*)((char*)sh+m->queue_offsets[q]);h->rx_rings[idx]=(void*)((char*)sh+m->queue_offsets[nq+q]);h->queue_to_provider[idx]=pi;idx++;}
	}
	UB_RING_LOG(INFO,"user done: %u queues",tq); return h;
fail_reg: ubsmem_destroy_region(reg);
fail_ub: ubsmem_finalize();
fail: rte_free(h); return NULL;
}

/* ---- Alloc / Return / Collect ---- */

void *
rte_ub_ring_alloc_buf(struct rte_ub_ring_handle *h, uint32_t pi)
{
	if (!h || pi>=UB_RING_MAX_NODES||!h->data_rings||!h->data_rings[pi]) return NULL;
	struct alloc_cache *c = &h->alloc_caches[pi][rte_lcore_id()];
	if (c->len == 0) {
		/* lazy fill: bulk-refill only when this lcore's cache runs
		 * dry — one MC dequeue per 512 allocations instead of one
		 * CAS per buffer. Variable burst — an all-or-nothing bulk
		 * would fail on a partial ring. Refill size (512) stays
		 * below the cache capacity (1024) so a following TX-failure
		 * return can never overflow the cache. */
		uint32_t n = rte_ring_mc_dequeue_burst(h->data_rings[pi],
			c->offs, UB_RING_ALLOC_REFILL, NULL);
		if (n == 0) {
			UB_RING_LOSS_ADD("alloc_buf_fail", 1, "data ring empty");
			return NULL;
		}
		c->len = n;
		/* count actual pkts pulled from the data ring on refill */
		UB_RING_FLOW_N("alloc_buf", n);
	}
	void *e = c->offs[--c->len];
	/* elements are packed {pi, off}; this ring only holds pi's
	 * buffers, so just strip the node byte and use the caller's pi */
	return ub_ring_buf_ptr(h->peers[pi].data_buf_area,
		ub_ring_elem_get_offset(e));
}

void
rte_ub_ring_return_buf(struct rte_ub_ring_handle *h, void *buf)
{
	if (!h || !buf) return;
	/* the provider performing the return uses its OWN return ring;
	 * the element carries {pi, offset} from the buffer's header, so
	 * ownership is independent of which ring the buffer passes through */
	struct rte_ring *rr = own_return_ring(h);
	if (!rr) return;
	void *e = buf_elem(buf);
	rte_ring_sp_enqueue_bulk(rr, &e, 1, NULL);
}

void
rte_ub_ring_return_burst(struct rte_ub_ring_handle *h, void **bufs, uint16_t nb)
{
	if (!h || !bufs || !nb) return;
	struct rte_ring *rr = own_return_ring(h);
	if (!rr) {
		UB_RING_FLOW_WARN("return_burst: no own return ring (data ring not synced?)");
		return;
	}
	void *e[nb];
	for (uint16_t i = 0; i < nb; i++)
		e[i] = buf_elem(bufs[i]);
	/* one bulk SP enqueue per drain pass instead of one CAS
	 * per buffer (workers would serialize on the return ring) */
	uint16_t s = rte_ring_sp_enqueue_bulk(rr, e, nb, NULL);
	if (s < nb)
		UB_RING_LOSS_ADD("return_burst", nb - s, "own return ring full");
	UB_RING_FLOW_N("return_burst", s);
}

void
rte_ub_ring_free_buf(struct rte_ub_ring_handle *h, void *buf)
{
	if (!h || !buf) return;
	/* single buffer only — chain segments are freed separately by the
	 * caller. Ownership comes from the 128B header (O(1), no node
	 * scan) */
	struct ub_ring_buf_hdr *hd = ub_ring_buf_hdr(buf);
	uint32_t ni = hd->node_index;
	void *e = ub_ring_elem_pack((uint8_t)ni, hd->offset);
	/* user: return to the owning provider's local data ring (MP —
	 * concurrent with collect); provider: return via its OWN return
	 * ring as a packed {pi, offset} element */
	if (h->data_rings && h->data_rings[ni]) {
		if (rte_ring_mp_enqueue_bulk(h->data_rings[ni], &e, 1, NULL) != 1)
			UB_RING_LOSS_ADD("free_buf", 1, "own data ring full");
		return;
	}
	struct rte_ring *rr = own_return_ring(h);
	if (rr) {
		void *pe = buf_elem(buf);
		if (rte_ring_sp_enqueue_bulk(rr, &pe, 1, NULL) != 1)
			UB_RING_LOSS_ADD("free_buf", 1, "own return ring full");
	} else {
		UB_RING_LOG(ERR, "free_buf %p: no return ring (leak!)", buf);
	}
}

/* Return a FINAL packed {pi, offset} element to THIS lcore's alloc
 * cache — no 128B header read, the element already carries pi. Used
 * by failure paths: freshly packed elements go straight back to this
 * lcore's cache, so the next alloc_buf reuses them without a ring
 * round trip. The cache never overflows by design (capacity 1024 >
 * refill 512 + one burst's max failed returns), so nothing is ever
 * flushed back to the data ring here. */
void
rte_ub_ring_free_elem_cached(struct rte_ub_ring_handle *h, void *elem)
{
	if (!h || !elem) return;
	uint32_t pi = ub_ring_elem_get_node(elem);
	if (!h->data_rings || !h->data_rings[pi] ||
			!h->alloc_caches || !h->alloc_caches[pi]) {
		/* no local data ring/cache (e.g. provider role): fall back
		 * to the header-based free path */
		void *buf = ub_ring_buf_ptr(
			rte_ub_ring_get_data_buf_area(h, pi),
			ub_ring_elem_get_offset(elem));
		rte_ub_ring_free_buf(h, buf);
		return;
	}
	struct alloc_cache *c = &h->alloc_caches[pi][rte_lcore_id()];
	if (c->len >= UB_RING_ALLOC_CACHE_SIZE) {
		/* unreachable by design (see capacity note above) — guard
		 * kept so an invariant break surfaces loudly instead of
		 * writing out of bounds */
		UB_RING_LOG(ERR, "alloc cache overflow pi=%u len=%u",
			pi, c->len);
		return;
	}
	c->offs[c->len++] = elem;
}

uint64_t
rte_ub_ring_collect_fetch(struct rte_ub_ring_handle *h)
{
	if (!h || !h->collect_mid) return 0;
	uint64_t total = 0;
	/* stage 1: one 512-batch per remote return ring, one pass over
	 * the actual nodes — bounded work per call so the caller's poll
	 * loop keeps ticking. The remote (non-cacheable) reads stay in
	 * this thread; everything else is a local SP enqueue */
	for (uint32_t i = 0; i < h->nb_nodes; i++) {
		struct rte_ring *rr = h->peers[i].return_ring;
		if (!rr) continue;
		void *elems[512];
		uint16_t n = (uint16_t)rte_ring_dequeue_burst(rr, elems, 512, NULL);
		if (n == 0)
			continue;
		uint16_t s = rte_ring_sp_enqueue_bulk(h->collect_mid, elems, n, NULL);
		if (s < n)
			UB_RING_LOSS_ADD("collect_fetch", n - s, "collect_mid full (PERM LOSS)");
		total += s;
		UB_RING_FLOW_N("collect_fetch", s);
	}
	return total;
}

uint64_t
rte_ub_ring_collect_dispatch(struct rte_ub_ring_handle *h)
{
	if (!h || !h->collect_mid || !h->data_rings) return 0;
	/* stage 2: pure local work — SC-dequeue one batch from the mid
	 * ring and route the packed {pi, offset} elements into pi's
	 * local data ring, AS-IS (all rings share one element format) */
	void *elems[512];
	uint16_t n = (uint16_t)rte_ring_dequeue_burst(
		h->collect_mid, elems, 512, NULL);
	if (n == 0)
		return 0;
	uint16_t k = 0;
	while (k < n) {
		uint32_t pi = ub_ring_elem_get_node(elems[k]);
		/* group consecutive same-owner elements into one bulk
		 * MP enqueue */
		uint16_t run = 1;
		while (k + run < n &&
		       ub_ring_elem_get_node(elems[k + run]) == pi)
			run++;
		if (!h->data_rings[pi]) {
			UB_RING_LOG(ERR,
				"collect_dispatch: no data ring for pi=%u (leak!)",
				pi);
			k += run;
			continue;
		}
		uint16_t s = rte_ring_mp_enqueue_bulk(h->data_rings[pi],
			&elems[k], run, NULL);
		if (s < run)
			UB_RING_LOSS_ADD("collect_dispatch", run - s, "data ring full (PERM LOSS)");
		k += run;
	}
	UB_RING_FLOW_N("collect_dispatch", n);
	return n;
}

uint64_t
rte_ub_ring_collect(struct rte_ub_ring_handle *h)
{
	/* serial combined version (e.g. teardown): requires
	 * rte_ub_ring_enable_mpmc() for the mid ring */
	uint64_t n = rte_ub_ring_collect_fetch(h);
	return n + rte_ub_ring_collect_dispatch(h);
}

uint32_t rte_ub_ring_get_data_elt_size(struct rte_ub_ring_handle *h, uint32_t pi)
{ if(!h||pi>=UB_RING_MAX_NODES)return 0;return h->peers[pi].data_elt_size; }
void *rte_ub_ring_get_data_buf_area(struct rte_ub_ring_handle *h, uint32_t pi)
{ if(!h||pi>=UB_RING_MAX_NODES)return NULL;return h->peers[pi].data_buf_area; }
uint32_t rte_ub_ring_get_total_bufs(struct rte_ub_ring_handle *h)
{ if(!h)return 0;uint32_t t=0;for(uint32_t i=0;i<UB_RING_MAX_NODES;i++)t+=h->peers[i].data_nb_bufs;return t; }

/* ---- Send / Recv / Flush / Fini ---- */
uint16_t rte_ub_ring_send_burst(struct rte_ub_ring_handle *h, uint32_t q,
		void **e, uint16_t n)
{ if(!h||q>=h->total_queues||!e||!n)return 0;
  /* elements are already in the FINAL packed {pi, offset} form
   * (ub_ring_elem_pack) — enqueue as-is, no re-encoding */
  uint16_t s=(uint16_t)rte_ring_enqueue_burst(h->tx_rings[q],e,n,NULL);
  if(s<n)UB_RING_LOSS_ADD("send_burst",n-s,"tx_ring full (backpressure)");
  return s; }
uint16_t rte_ub_ring_recv_burst(struct rte_ub_ring_handle *h, uint32_t q, void **b, uint16_t n)
{ if(!h||q>=h->total_queues||!b||!n)return 0;
  /* raw ring elements: packed {pi, offset}. Rings only ever carry 8B
   * values — decoding is the caller's job */
  uint16_t r=(uint16_t)rte_ring_dequeue_burst(h->rx_rings[q],b,n,NULL);
  if (r) UB_RING_FLOW_N("recv_burst", r);
  return r; }
int rte_ub_ring_enable_mpmc(struct rte_ub_ring_handle *h) {
	if (!h || h->mpmc_enabled) return -EINVAL;
	h->shadow_tx = rte_zmalloc(NULL, h->total_queues * sizeof(void*), 0);
	h->shadow_rx = rte_zmalloc(NULL, h->total_queues * sizeof(void*), 0);
	/* one 512-slot stash per queue for flush_tx leftovers */
	h->tx_stash = rte_zmalloc(NULL,
		(size_t)h->total_queues * 512 * sizeof(void*), 0);
	h->tx_stash_len = rte_zmalloc(NULL,
		h->total_queues * sizeof(uint16_t), 0);
	if (!h->shadow_tx || !h->shadow_rx || !h->tx_stash || !h->tx_stash_len) {
		UB_RING_LOG(ERR, "enable_mpmc: shadow array alloc failed "
			"(rte_errno=%d %s)", rte_errno, rte_strerror(rte_errno));
		return -ENOMEM;
	}
	for (uint32_t q = 0; q < h->total_queues; q++) {
		/* shadow_tx depth = 2x the owning provider's data ring, so the
		 * producers can run up to a full buffer pool ahead of the
		 * flusher. shadow_rx keeps the old fixed depth (spec).
		 * stx: MP/SC (multiple send-side lcores enqueue, flush lcore
		 * dequeues)
		 * srx: SP/MC (flush lcore enqueues, rx-side lcores dequeue) */
		uint32_t stx_depth = 2u * h->peers[h->queue_to_provider[q]].data_nb_bufs;
		char n[64]; snprintf(n,64,"stx%u",q);
		h->shadow_tx[q]=rte_ring_create(n,stx_depth,rte_socket_id(),RING_F_SC_DEQ);
		if (!h->shadow_tx[q]) {
			UB_RING_LOG(ERR, "enable_mpmc: stx%u create failed "
				"(rte_errno=%d %s)", q, rte_errno, rte_strerror(rte_errno));
			return -ENOMEM;
		}
		snprintf(n,64,"srx%u",q);
		h->shadow_rx[q]=rte_ring_create(n,1024*1024,rte_socket_id(),RING_F_SP_ENQ);
		if (!h->shadow_rx[q]) {
			UB_RING_LOG(ERR, "enable_mpmc: srx%u create failed "
				"(rte_errno=%d %s)", q, rte_errno, rte_strerror(rte_errno));
			return -ENOMEM;
		}
	}
	/* collect pipeline mid ring: SPSC, 1M deep — may hold the whole
	 * in-flight set between the fetch and dispatch stages */
	h->collect_mid = rte_ring_create("cmid", 1024*1024,
		rte_socket_id(), RING_F_SP_ENQ | RING_F_SC_DEQ);
	if (!h->collect_mid) {
		UB_RING_LOG(ERR, "enable_mpmc: cmid create failed "
			"(rte_errno=%d %s)", rte_errno, rte_strerror(rte_errno));
		return -ENOMEM;
	}
	h->mpmc_enabled = true; return 0;
}
bool rte_ub_ring_is_mpmc_enabled(struct rte_ub_ring_handle *h) { return h&&h->mpmc_enabled; }
/* enqueue FINAL packed {pi, offset} elements to shadow_tx — already
 * in ring element form, nothing is re-encoded here */
uint16_t rte_ub_ring_mp_send_burst(struct rte_ub_ring_handle *h, uint32_t q,
		void **e, uint16_t n)
{
	if (!h || !h->mpmc_enabled || q >= h->total_queues || !e || !n)
		return 0;
	if (rte_ring_free_count(h->shadow_tx[q]) < n) {
		UB_RING_LOSS_ADD("mp_send_burst", n, "shadow_tx full (backpressure)");
		return 0; /* fail-fast */
	}
	return (uint16_t)rte_ring_enqueue_burst(h->shadow_tx[q], e, n, NULL);
}
uint16_t rte_ub_ring_mc_recv_burst(struct rte_ub_ring_handle *h, uint32_t q, void **b, uint16_t n)
{ if(!h||!h->mpmc_enabled||q>=h->total_queues)return 0;return (uint16_t)rte_ring_dequeue_burst(h->shadow_rx[q],(void**)b,n,NULL); }
uint64_t rte_ub_ring_flush_tx(struct rte_ub_ring_handle *h, uint32_t q) {
	if (!h || !h->mpmc_enabled || q >= h->total_queues) return 0;
	uint64_t moved = 0;
	void *elems[512];

	/* round 1: if the previous round left elements behind (remote ring
	 * was full), retry the stash first — FIFO, and it keeps the flush
	 * lcore from ever re-enqueueing to shadow_tx (no second producer
	 * racing the send-side lcores) */
	if (h->tx_stash_len[q] > 0) {
		uint16_t s = (uint16_t)rte_ring_enqueue_burst(h->tx_rings[q],
			h->tx_stash[q], h->tx_stash_len[q], NULL);
		if (s == h->tx_stash_len[q]) {
			h->tx_stash_len[q] = 0;
		} else {
			memmove(h->tx_stash[q], h->tx_stash[q] + s,
				(size_t)(h->tx_stash_len[q] - s) * sizeof(void *));
			h->tx_stash_len[q] -= s;
		}
		if (s)
			UB_RING_FLOW_N("flush_tx", s);
		/* remote ring still backed up — don't pull fresh work from
		 * the shadow, it could only fill the stash */
		return moved + s;
	}

	/* round 2: stash drained, dequeue a fresh batch and push it */
	uint16_t n = (uint16_t)rte_ring_dequeue_burst(h->shadow_tx[q],
		(void**)elems, 512, NULL);
	if (n == 0)
		return moved;
	uint16_t s = (uint16_t)rte_ring_enqueue_burst(h->tx_rings[q],
		elems, n, NULL);
	if (s < n) {
		/* real ring full — hold the unsent tail in the per-queue
		 * stash and retry it next round (no shadow rollback) */
		memcpy(h->tx_stash[q], elems + s,
			(size_t)(n - s) * sizeof(void *));
		h->tx_stash_len[q] = (uint16_t)(n - s);
	}
	if (s)
		UB_RING_FLOW_N("flush_tx", s);
	return moved + s;
}

void rte_ub_ring_flush_tx_all(struct rte_ub_ring_handle *h)
{
	if (!h || !h->mpmc_enabled) return;
	/* the peer keeps consuming until its workers finish, so the tx
	 * rings keep draining and this loop makes progress; give up after
	 * 1s without progress (peer may be done and rings full) */
	uint32_t prev = UINT32_MAX, stalled = 0;
	for (;;) {
		uint32_t pending = 0;
		for (uint32_t q = 0; q < h->total_queues; q++) {
			rte_ub_ring_flush_tx(h, q);
			pending += rte_ring_count(h->shadow_tx[q]) +
				h->tx_stash_len[q];
		}
		if (pending == 0)
			return;
		if (pending < prev) {
			prev = pending;
			stalled = 0;
		} else if (++stalled > 1000) {
			UB_RING_LOG(WARNING,
				"flush_tx_all: %u elems stuck (peer done?)",
				pending);
			return;
		}
		rte_delay_us(1000);
	}
}
void rte_ub_ring_flush_rx(struct rte_ub_ring_handle *h, uint32_t q) {
	if (!h || !h->mpmc_enabled || q >= h->total_queues) return;
	void *bufs[64]; uint16_t n;
	while ((n = rte_ub_ring_recv_burst(h, q, bufs, 64)) > 0) {
		uint16_t s = (uint16_t)rte_ring_enqueue_burst(h->shadow_rx[q],
			(void **)bufs, n, NULL);
		if (s < n)
			UB_RING_LOSS_ADD("flush_rx", n - s, "shadow_rx full");
		if (s == 0)
			break;
	}
}
int rte_ub_ring_get_queue_mapping(struct rte_ub_ring_handle *h, uint32_t pi, uint32_t sq, uint32_t *tx, uint32_t *rx)
{ if(!h||!tx||!rx)return -EINVAL;if(pi>=UB_RING_MAX_NODES||!h->peers[pi].shm_base||sq>=h->peers[pi].nb_queues)return -EINVAL;uint32_t q=h->provider_base_qid[pi]+sq;*tx=q;*rx=q;return 0; }
uint32_t rte_ub_ring_queue_to_provider(struct rte_ub_ring_handle *h, uint32_t q)
{ return (!h||q>=h->total_queues)?0:h->queue_to_provider[q]; }
uint32_t rte_ub_ring_get_total_queues(struct rte_ub_ring_handle *h) { return h?h->total_queues:0; }
int rte_ub_ring_get_node_index(struct rte_ub_ring_handle *h, uint32_t *i) { if(!h||!i)return -EINVAL;*i=h->my_node_index;return 0; }
int rte_ub_ring_get_peer_index(struct rte_ub_ring_handle *h, const char *hn, uint32_t *i)
{ if(!h||!hn||!i)return -EINVAL;for(uint32_t j=0;j<h->nb_nodes;j++){if(!strcmp(h->peers[j].node_name,hn)){*i=j;return 0;}}return -ENOENT; }
void rte_ub_ring_fini(struct rte_ub_ring_handle *h) {
	if (!h) return;
	for (uint32_t i=0;i<UB_RING_MAX_NODES;i++){if(i==h->my_node_index)continue;if(h->peers[i].shm_base)ubsmem_shmem_unmap(h->peers[i].shm_base,h->peers[i].total_size);}
	if(h->role==UB_RING_ROLE_PROVIDER&&h->peers[h->my_node_index].shm_base){ubsmem_shmem_unmap(h->peers[h->my_node_index].shm_base,h->peers[h->my_node_index].total_size);char nm[128],my[MAX_HOST_NAME_DESC_LENGTH];if(!gethostname(my,sizeof(my))){snprintf(nm,128,"%s_%s",h->shm_prefix,my);ubsmem_shmem_deallocate(nm);}}
	if(h->data_rings){for(uint32_t i=0;i<UB_RING_MAX_NODES;i++)if(h->data_rings[i])rte_ring_free(h->data_rings[i]);rte_free(h->data_rings);}
	for(uint32_t i=0;i<UB_RING_MAX_NODES;i++)rte_free(h->alloc_caches[i]);
	if(h->shadow_tx){for(uint32_t q=0;q<h->total_queues;q++)rte_ring_free(h->shadow_tx[q]);rte_free(h->shadow_tx);}
	if(h->shadow_rx){for(uint32_t q=0;q<h->total_queues;q++)rte_ring_free(h->shadow_rx[q]);rte_free(h->shadow_rx);}
	rte_free(h->tx_stash); rte_free(h->tx_stash_len);
	if(h->collect_mid)rte_ring_free(h->collect_mid);
	ubsmem_destroy_region(h->region_name);ubsmem_finalize();rte_free(h->rx_rings);rte_free(h->tx_rings);rte_free(h);
}
