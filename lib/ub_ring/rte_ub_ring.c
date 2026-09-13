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
			" (+%" PRIu64 "/s)", site, cnt, cnt - prev); \
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

/* per-lcore alloc cache: senders are pinned 1:1 to lcores, so each
 * lcore's cache is touched by exactly one thread — no locks needed.
 * The shared MP/MC data ring is accessed only for bulk refills via
 * MC dequeue (CAS), the single cross-thread synchronization point —
 * same pattern as a mempool per-lcore cache */
#define UB_RING_ALLOC_CACHE_SIZE 512
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
		/* metadata cached locally at init 鈥?no shm reads afterwards */
		uint64_t         total_size;
		uint32_t         nb_queues;
		char             node_name[64];
	} peers[UB_RING_MAX_NODES];

	/* local data rings (user only, one per provider shm) */
	struct rte_ring **data_rings;

	/* per-lcore alloc caches (user only, one per provider) */
	struct alloc_cache *alloc_caches[UB_RING_MAX_NODES];

	struct rte_ring **rx_rings, **tx_rings, **shadow_tx, **shadow_rx;
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
	 * from any provider can be received/decoded and the hot paths
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
		/* cache metadata locally 鈥?no shm reads after init */
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

		/* create LOCAL data ring
		 * MP enqueue (flush thread collect/free_buf AND sender
		 * threads' eth_tx failure path all enqueue concurrently)
		 * MC dequeue (multiple sender threads: alloc_buf).
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
				hd->offset=(uint32_t)((idx+k)*elt);
				hd->elt_size=elt;
				hd->magic=UB_RING_BUF_MAGIC;
				e[k]=ub_ring_elem_pack((uint8_t)pi,(idx+k)*elt);
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
		 * would fail on a partial ring */
		uint32_t n = rte_ring_mc_dequeue_burst(h->data_rings[pi],
			c->offs, UB_RING_ALLOC_CACHE_SIZE, NULL);
		if (n == 0) {
			UB_RING_FLOW_WARN("alloc_buf FAIL pi=%u (data ring empty)", pi);
			return NULL;
		}
		c->len = n;
	}
	void *e = c->offs[--c->len];
	UB_RING_FLOW_N("alloc_buf", 1);
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
	rte_ring_sp_enqueue_bulk(rr, e, nb, NULL);
	UB_RING_FLOW_N("return_burst", nb);
}

void
rte_ub_ring_free_buf(struct rte_ub_ring_handle *h, void *buf)
{
	if (!h || !buf) return;
	/* single buffer only — chains are split by rte_pktmbuf_free on the
	 * peer side, each segment entering the dummy pool separately.
	 * Ownership comes from the 128B header (O(1), no node scan) */
	struct ub_ring_buf_hdr *hd = ub_ring_buf_hdr(buf);
	uint32_t ni = hd->node_index;
	void *e = ub_ring_elem_pack((uint8_t)ni, hd->offset);
	/* user: return to the owning provider's local data ring (MP —
	 * concurrent with collect); provider: return via its OWN return
	 * ring as a packed {pi, offset} element */
	if (h->data_rings && h->data_rings[ni]) {
		rte_ring_mp_enqueue_bulk(h->data_rings[ni], &e, 1, NULL);
		return;
	}
	struct rte_ring *rr = own_return_ring(h);
	if (rr) {
		void *pe = buf_elem(buf);
		rte_ring_sp_enqueue_bulk(rr, &pe, 1, NULL);
	} else {
		UB_RING_LOG(ERR, "free_buf %p: no return ring (leak!)", buf);
	}
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
		rte_ring_sp_enqueue_bulk(h->collect_mid, elems, n, NULL);
		total += n;
		UB_RING_FLOW_N("collect_fetch", n);
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
		rte_ring_mp_enqueue_bulk(h->data_rings[pi],
			&elems[k], run, NULL);
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

/* ---- Send / Recv / Flush / Fini (unchanged) ---- */
uint16_t rte_ub_ring_send_burst(struct rte_ub_ring_handle *h, uint32_t q, void **b, uint16_t n)
{ if(!h||q>=h->total_queues||!b||!n)return 0; void *e[n];uint16_t c=0;
  /* fail-fast: any invalid buffer → send nothing (caller frees all) */
  for(uint16_t i=0;i<n;i++){if(!b[i])return 0;e[c++]=buf_elem(b[i]);}
  return (uint16_t)rte_ring_enqueue_burst(h->tx_rings[q],e,c,NULL); }

uint16_t rte_ub_ring_send_burst_pi(struct rte_ub_ring_handle *h, uint32_t q,
		const uint32_t *pi, void **b, uint16_t n)
{ if(!h||q>=h->total_queues||!b||!pi||!n)return 0; void *e[n];uint16_t c=0;
  /* caller-provided pi; offset by LOCAL pointer arithmetic — the
   * header lives in remote shm (non-cacheable), never read it on
   * the hot path */
  for(uint16_t i=0;i<n;i++){if(!b[i])return 0;
  e[c++]=ub_ring_elem_pack((uint8_t)pi[i],
	  ub_ring_buf_off(h->peers[pi[i]].data_buf_area,b[i]));}
  return (uint16_t)rte_ring_enqueue_burst(h->tx_rings[q],e,c,NULL); }
uint16_t rte_ub_ring_recv_burst(struct rte_ub_ring_handle *h, uint32_t q, void **b, uint16_t n)
{ if(!h||q>=h->total_queues||!b||!n)return 0; void *e[n];
  uint16_t r=(uint16_t)rte_ring_dequeue_burst(h->rx_rings[q],e,n,NULL);
  for(uint16_t i=0;i<r;i++){uint8_t ni=ub_ring_elem_get_node(e[i]);uint64_t o=ub_ring_elem_get_offset(e[i]);b[i]=ub_ring_buf_ptr(h->peers[ni].data_buf_area,o);}
  if (r) UB_RING_FLOW_N("recv_burst", r);
  return r; }
int rte_ub_ring_enable_mpmc(struct rte_ub_ring_handle *h) {
	if (!h || h->mpmc_enabled) return -EINVAL;
	h->shadow_tx = rte_zmalloc(NULL, h->total_queues * sizeof(void*), 0);
	h->shadow_rx = rte_zmalloc(NULL, h->total_queues * sizeof(void*), 0);
	if (!h->shadow_tx || !h->shadow_rx) return -ENOMEM;
	for (uint32_t q = 0; q < h->total_queues; q++) {
		/* 1M-deep shadow rings: senders may run far ahead of flush.
		 * stx: MP/SC (n senders enqueue, flush lcore dequeues)
		 * srx: SP/MC (flush lcore enqueues, rx lcores dequeue) */
		char n[64]; snprintf(n,64,"stx%u",q); h->shadow_tx[q]=rte_ring_create(n,1024*1024,rte_socket_id(),RING_F_SC_DEQ);
		snprintf(n,64,"srx%u",q); h->shadow_rx[q]=rte_ring_create(n,1024*1024,rte_socket_id(),RING_F_SP_ENQ);
		if (!h->shadow_tx[q] || !h->shadow_rx[q]) return -ENOMEM;
	}
	/* collect pipeline mid ring: SPSC, 1M deep — may hold the whole
	 * in-flight set between the fetch and dispatch stages */
	h->collect_mid = rte_ring_create("cmid", 1024*1024,
		rte_socket_id(), RING_F_SP_ENQ | RING_F_SC_DEQ);
	if (!h->collect_mid) return -ENOMEM;
	h->mpmc_enabled = true; return 0;
}
bool rte_ub_ring_is_mpmc_enabled(struct rte_ub_ring_handle *h) { return h&&h->mpmc_enabled; }
/* pack buffers into encoded {pi, offset} elements and enqueue to shadow */
static uint16_t
mp_send_pack(struct rte_ub_ring_handle *h, uint32_t q, const uint32_t *pi,
	     void **b, uint16_t n, bool use_pi)
{
	if (!h || !h->mpmc_enabled || q >= h->total_queues || !b || !n)
		return 0;
	if (rte_ring_free_count(h->shadow_tx[q]) < n)
		return 0; /* fail-fast */

	void *e[n];
	uint16_t c = 0;
	for (uint16_t i = 0; i < n; i++) {
		if (!b[i])
			return 0;
		uint32_t ni;
		if (use_pi)
			ni = pi[i];
		else {
			/* non-pi variant needs the owner from the header —
			 * cold path in the demo */
			struct ub_ring_buf_hdr *hd = ub_ring_buf_hdr(b[i]);
			ni = hd->node_index;
		}
		/* offset by LOCAL pointer arithmetic — the header lives in
		 * remote shm (non-cacheable), never read it on the hot
		 * path */
		e[c++] = ub_ring_elem_pack((uint8_t)ni,
			ub_ring_buf_off(h->peers[ni].data_buf_area, b[i]));
	}
	return (uint16_t)rte_ring_enqueue_burst(h->shadow_tx[q], e, c, NULL);
}

uint16_t rte_ub_ring_mp_send_burst(struct rte_ub_ring_handle *h, uint32_t q, void **b, uint16_t n)
{ return mp_send_pack(h, q, NULL, b, n, false); }

uint16_t rte_ub_ring_mp_send_burst_pi(struct rte_ub_ring_handle *h, uint32_t q,
		const uint32_t *pi, void **b, uint16_t n)
{ return mp_send_pack(h, q, pi, b, n, true); }
uint16_t rte_ub_ring_mc_recv_burst(struct rte_ub_ring_handle *h, uint32_t q, void **b, uint16_t n)
{ if(!h||!h->mpmc_enabled||q>=h->total_queues)return 0;return (uint16_t)rte_ring_dequeue_burst(h->shadow_rx[q],(void**)b,n,NULL); }
uint64_t rte_ub_ring_flush_tx(struct rte_ub_ring_handle *h, uint32_t q) {
	if (!h || !h->mpmc_enabled || q >= h->total_queues) return 0;
	/* one burst per call: the flush lcore sweeps every queue once
	 * per round — same pattern as collect's one-batch-per-ring.
	 * Bounded work keeps its per-second poll accounting alive */
	void *elems[512];
	uint16_t n = (uint16_t)rte_ring_dequeue_burst(
		h->shadow_tx[q], (void**)elems, 512, NULL);
	if (n == 0)
		return 0;
	uint16_t s = (uint16_t)rte_ring_enqueue_burst(h->tx_rings[q],
		elems, n, NULL);
	if (s < n) {
		/* real ring full — put unsent back to shadow for retry */
		for (uint16_t i = s; i < n; i++)
			rte_ring_enqueue_burst(h->shadow_tx[q], &elems[i], 1, NULL);
	}
	if (s)
		UB_RING_FLOW_N("flush_tx", s);
	return s;
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
			pending += rte_ring_count(h->shadow_tx[q]);
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
	while ((n = rte_ub_ring_recv_burst(h, q, bufs, 64)) > 0)
		rte_ring_enqueue_burst(h->shadow_rx[q], (void**)bufs, n, NULL);
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
	if(h->collect_mid)rte_ring_free(h->collect_mid);
	ubsmem_destroy_region(h->region_name);ubsmem_finalize();rte_free(h->rx_rings);rte_free(h->tx_rings);rte_free(h);
}
