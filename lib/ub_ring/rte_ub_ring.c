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
#include <rte_ring.h>

#include <rte_ub_ring.h>

RTE_LOG_REGISTER_DEFAULT(ub_ring_logtype, NOTICE);
#define RTE_LOGTYPE_UB_RING ub_ring_logtype
#define UB_RING_LOG(level, ...) \
	RTE_LOG_LINE_PREFIX(level, UB_RING, "%s(): ", __func__, __VA_ARGS__)

#define UB_RING_MD_ALIGN 64
#define UB_RING_POLL_US  50000
#define UB_RING_POLL_S   60

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

struct rte_ub_ring_handle {
	uint32_t nb_nodes, my_node_index;
	enum ub_ring_role role;

	struct {
		void            *shm_base;
		struct rte_ring *return_ring;   /* in shm: providers return bufs */
		void            *data_buf_area, *data_buf_end;
		uint32_t         data_elt_size;
	} peers[UB_RING_MAX_NODES];

	/* local data rings (user only, one per provider shm) */
	struct rte_ring **data_rings;

	struct rte_ring **rx_rings, **tx_rings, **shadow_tx, **shadow_rx;
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

static void
sync_peer_data_ring(struct rte_ub_ring_handle *h, uint32_t i)
{
	if (h->peers[i].data_buf_area) return;
	struct ub_ring_metadata *m = peer_meta(h, i);
	if (!m || !(m->state & UB_RING_STATE_DATA_RING_READY)) return;
	h->peers[i].data_elt_size = m->data_elt_size;
	h->peers[i].return_ring = (struct rte_ring *)((char *)h->peers[i].shm_base + m->data_ring_offset);
	size_t rsz = rte_ring_get_memsize(m->data_nb_bufs);
	h->peers[i].data_buf_area = (char *)h->peers[i].return_ring + rsz;
	h->peers[i].data_buf_end   = (char *)h->peers[i].data_buf_area + (size_t)m->data_nb_bufs * m->data_elt_size;
}

static int find_peer_by_buf(struct rte_ub_ring_handle *h, const void *buf)
{
	for (uint32_t i = 0; i < UB_RING_MAX_NODES; i++) {
		sync_peer_data_ring(h, i);
		if (!h->peers[i].data_buf_area || !h->peers[i].data_buf_end) continue;
		if ((const char *)buf >= (const char *)h->peers[i].data_buf_area &&
		    (const char *)buf < (const char *)h->peers[i].data_buf_end) return (int)i;
	}
	UB_RING_LOG(ERR, "buf %p not in any data ring", buf);
	return -1;
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
resolve_index(const char **pn, uint32_t np, const char **un, uint32_t nu, uint32_t *mi, uint32_t *tot)
{
	char my[MAX_HOST_NAME_DESC_LENGTH];
	if (gethostname(my,sizeof(my))) return -1;
	*tot=np+nu;
	const char **all=rte_malloc(NULL,*tot*sizeof(char*),0); if(!all) return -1;
	uint32_t k=0;
	for (uint32_t i=0;i<np;i++)all[k++]=pn[i];
	for (uint32_t i=0;i<nu;i++)all[k++]=un[i];
	qsort(all,*tot,sizeof(char*),cmp_node);
	for (uint32_t i=0;i<*tot;i++){if(!strcmp(all[i],my)){*mi=i;rte_free(all);return 0;}}
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
	if (resolve_index(pn,np,un,nu,&mi,&tot)<0)goto fail;
	h->nb_nodes=tot; h->my_node_index=mi;
	if (gethostname(my,sizeof(my)))goto fail;
	UB_RING_LOG(INFO,"provider: %s idx=%u/%u nq=%u qd=%u sz=%zu",my,mi,tot,nq,qd,sz);
	if ((rc=ubsmem_init_attributes(&o))||(rc=ubsmem_initialize(&o))){UB_RING_LOG(ERR,"ubsmem:%s(%d)",ubs_strerror(rc),rc);goto fail_ub;}
	memset(&a,0,sizeof(a));a.host_num=tot;
	for(uint32_t i=0;i<np;i++){strncpy(a.hosts[i].host_name,pn[i],MAX_HOST_NAME_DESC_LENGTH-1);a.hosts[i].affinity=true;}
	for(uint32_t i=0;i<nu;i++){strncpy(a.hosts[np+i].host_name,un[i],MAX_HOST_NAME_DESC_LENGTH-1);a.hosts[np+i].affinity=true;}
	ubsmem_destroy_region(reg);if((rc=ubsmem_create_region(reg,0,&a))){UB_RING_LOG(ERR,"region:%s(%d)",ubs_strerror(rc),rc);goto fail_ub;}
	snprintf(nm,128,"%s_%s",pre,my); ubsmem_shmem_deallocate(nm);
	if((rc=ubsmem_shmem_allocate(reg,nm,sz,0600,UBSM_FLAG_NONCACHE|UBSM_FLAG_WR_DELAY_COMP))){UB_RING_LOG(ERR,"alloc:%s(%d)",ubs_strerror(rc),rc);goto fail_reg;}
	if((rc=ubsmem_shmem_map(NULL,sz,PROT_READ|PROT_WRITE,MAP_SHARED,nm,0,&sh))){UB_RING_LOG(ERR,"map:%s(%d)",ubs_strerror(rc),rc);goto fail_reg;}
	memset(sh,0,sz); h->peers[mi].shm_base=sh;
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
	UB_RING_LOG(INFO,"waiting for %u provider shms...",np);
	if (wait_shms(pre,pn,np)<0){UB_RING_LOG(ERR,"timeout");goto fail_reg;}
	for(uint32_t i=0;i<np;i++){if(!strcmp(pn[i],my))continue;
		char pn2[128];snprintf(pn2,128,"%s_%s",pre,pn[i]);ubsmem_shmem_desc_t dl[32];uint32_t dc=32;uint64_t ps=0;
		ubsmem_shmem_list_lookup(pre,dl,&dc);for(uint32_t k=0;k<dc;k++)if(!strcmp(dl[k].name,pn2)){ps=dl[k].size;break;}if(!ps)continue;
		void*psh=NULL;if(ubsmem_shmem_map(NULL,ps,PROT_READ|PROT_WRITE,MAP_SHARED,pn2,0,&psh))continue;
		uint32_t pi=UINT32_MAX;for(uint32_t j=0;j<tot;j++){const char*n=j<np?pn[j]:un[j-np];if(!strcmp(n,pn[i])){pi=j;break;}}if(pi>=UB_RING_MAX_NODES)continue;
		h->peers[pi].shm_base=psh;if(wait_meta((struct ub_ring_metadata*)psh)<0)goto fail_reg;
		h->peers[pi].return_ring=NULL; h->peers[pi].data_buf_area=NULL;
		UB_RING_LOG(INFO,"peer %s (pi=%u)",pn2,pi);
	}
	h->total_queues=nq; h->rx_rings=rte_malloc(NULL,nq*sizeof(void*),0); h->tx_rings=rte_malloc(NULL,nq*sizeof(void*),0);
	if(!h->rx_rings||!h->tx_rings)goto fail_reg;
	for(uint32_t q=0;q<nq;q++){h->rx_rings[q]=(void*)((char*)sh+om->queue_offsets[q]);h->tx_rings[q]=(void*)((char*)sh+om->queue_offsets[nq+q]);h->queue_to_provider[q]=mi;}
	h->provider_base_qid[mi]=0; UB_RING_LOG(INFO,"provider done"); return h;
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
	struct rte_ub_ring_handle *h=rte_zmalloc(NULL,sizeof(*h),0); if(!h)return NULL;
	h->role=UB_RING_ROLE_USER; snprintf(h->region_name,64,"%s",reg); snprintf(h->shm_prefix,64,"%s",pre);
	if (resolve_index(pn,np,un,nu,&mi,&tot)<0)goto fail;
	h->nb_nodes=tot; h->my_node_index=mi;
	if (gethostname(my,sizeof(my)))goto fail;
	UB_RING_LOG(INFO,"user: %s idx=%u/%u elt=%u",my,mi,tot,elt);
	if ((rc=ubsmem_init_attributes(&o))||(rc=ubsmem_initialize(&o))){UB_RING_LOG(ERR,"ubsmem:%s(%d)",ubs_strerror(rc),rc);goto fail_ub;}
	memset(&a,0,sizeof(a));a.host_num=tot;
	for(uint32_t i=0;i<np;i++){strncpy(a.hosts[i].host_name,pn[i],MAX_HOST_NAME_DESC_LENGTH-1);a.hosts[i].affinity=true;}
	for(uint32_t i=0;i<nu;i++){strncpy(a.hosts[np+i].host_name,un[i],MAX_HOST_NAME_DESC_LENGTH-1);a.hosts[np+i].affinity=true;}
	ubsmem_destroy_region(reg);if((rc=ubsmem_create_region(reg,0,&a))){UB_RING_LOG(ERR,"region:%s(%d)",ubs_strerror(rc),rc);goto fail_ub;}
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
		h->peers[pi].data_elt_size=elt;

		/* compute data ring layout: return ring + buffer area */
		uint64_t dr_off=pm->ring_area_offset+pm->ring_area_size;
		uint64_t dr_sz=pm->total_size-dr_off;
		uint32_t mx=(uint32_t)((dr_sz-sizeof(struct rte_ring))/(8+elt));
		mx=rte_align32prevpow2(mx);
		while(rte_ring_get_memsize(mx)+(size_t)mx*elt>dr_sz)mx/=2;
		if(mx<2){UB_RING_LOG(ERR,"data ring too small");goto fail_reg;}
		size_t rrs=rte_ring_get_memsize(mx);
		void*drb=(char*)psh+dr_off;
		struct rte_ring*rr=(struct rte_ring*)drb;
		char rn[64];snprintf(rn,64,"ret_%s",pn2);
		/* return ring: MP (providers enqueue), SC (user drain) */
		rc=rte_ring_init(rr,rn,mx,RING_F_SC_DEQ);
		if(rc<0){UB_RING_LOG(ERR,"return ring init fail:%d",rc);goto fail_reg;}
		void*ba=(char*)drb+rrs;
		h->peers[pi].return_ring=rr;
		h->peers[pi].data_buf_area=ba;
		h->peers[pi].data_buf_end=(char*)ba+(size_t)mx*elt;

		/* create LOCAL data ring */
		char dn[64];snprintf(dn,64,"data_%s",pn2);
		h->data_rings[pi]=rte_ring_create(dn,1024,rte_socket_id(),RING_F_SP_ENQ|RING_F_SC_DEQ);
		if(!h->data_rings[pi]){UB_RING_LOG(ERR,"local data ring fail");goto fail_reg;}

		/* populate: enqueue all free offsets to local data ring */
		for(uint32_t idx=0;idx<mx;idx++){void*e=(void*)(uintptr_t)(idx*elt);rte_ring_enqueue_burst(h->data_rings[pi],&e,1,NULL);}

		pm->data_ring_offset=dr_off;
		pm->data_elt_size=elt; pm->data_nb_bufs=mx;
		pm->state|=UB_RING_STATE_DATA_RING_READY;
		pi_arr[nb_prov]=pi; pq_arr[nb_prov]=pm->nb_queues; nb_prov++;
		UB_RING_LOG(INFO,"data ring %s: %u bufs, local+return",pn2,mx);
	}
	h->peers[mi].return_ring=NULL; h->peers[mi].data_buf_area=NULL;

	uint32_t tq=0;for(uint32_t p=0;p<nb_prov;p++)tq+=pq_arr[p];
	h->total_queues=tq;
	h->rx_rings=rte_malloc(NULL,tq*sizeof(void*),0);h->tx_rings=rte_malloc(NULL,tq*sizeof(void*),0);
	if(!h->rx_rings||!h->tx_rings)goto fail_reg;
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
	void *e;
	if (rte_ring_sc_dequeue_bulk(h->data_rings[pi], &e, 1, NULL) == 0) return NULL;
	sync_peer_data_ring(h, pi);
	return (char *)h->peers[pi].data_buf_area + (size_t)(uintptr_t)e;
}

void
rte_ub_ring_return_buf(struct rte_ub_ring_handle *h, void *buf)
{
	if (!h || !buf) return;
	int ni = find_peer_by_buf(h, buf);
	if (ni < 0) return;
	struct rte_ring *rr = h->peers[ni].return_ring;
	if (!rr) return;
	void *e = (void *)(uintptr_t)((char *)buf - (char *)h->peers[ni].data_buf_area);
	rte_ring_mp_enqueue_bulk(rr, &e, 1, NULL);
}

void
rte_ub_ring_free_buf(struct rte_ub_ring_handle *h, void *buf)
{
	if (!h || !buf) return;
	int ni = find_peer_by_buf(h, buf);
	if (ni < 0) return;
	void *e = (void *)(uintptr_t)((char *)buf - (char *)h->peers[ni].data_buf_area);
	/* user: return to local data ring; provider: return to shm return ring */
	if (h->data_rings && h->data_rings[ni])
		rte_ring_sp_enqueue_bulk(h->data_rings[ni], &e, 1, NULL);
	else if (h->peers[ni].return_ring)
		rte_ring_mp_enqueue_bulk(h->peers[ni].return_ring, &e, 1, NULL);
}

void
rte_ub_ring_collect(struct rte_ub_ring_handle *h)
{
	if (!h || !h->data_rings) return;
	for (uint32_t i = 0; i < UB_RING_MAX_NODES; i++) {
		struct rte_ring *rr = h->peers[i].return_ring;
		if (!rr || !h->data_rings[i]) continue;
		void *bufs[64];
		uint16_t n;
		while ((n = (uint16_t)rte_ring_dequeue_burst(rr, bufs, 64, NULL)) > 0)
			rte_ring_enqueue_burst(h->data_rings[i], bufs, n, NULL);
	}
}

uint32_t rte_ub_ring_get_data_elt_size(struct rte_ub_ring_handle *h, uint32_t pi)
{ if(!h||pi>=UB_RING_MAX_NODES)return 0;sync_peer_data_ring(h,pi);return h->peers[pi].data_elt_size; }
void *rte_ub_ring_get_data_buf_area(struct rte_ub_ring_handle *h, uint32_t pi)
{ if(!h||pi>=UB_RING_MAX_NODES)return NULL;return h->peers[pi].data_buf_area; }

/* ---- Send / Recv / Flush / Fini (unchanged) ---- */
uint16_t rte_ub_ring_send_burst(struct rte_ub_ring_handle *h, uint32_t q, void **b, uint16_t n)
{ if(!h||q>=h->total_queues||!b||!n)return 0; void *e[n];uint16_t c=0;
  for(uint16_t i=0;i<n;i++){if(!b[i])continue;int ni=find_peer_by_buf(h,b[i]);if(ni<0)continue;
  uint64_t o=(uint64_t)((char*)b[i]-(char*)h->peers[ni].data_buf_area);e[c++]=ub_ring_elem_pack((uint8_t)ni,o);}
  return (uint16_t)rte_ring_enqueue_burst(h->tx_rings[q],e,c,NULL); }
uint16_t rte_ub_ring_recv_burst(struct rte_ub_ring_handle *h, uint32_t q, void **b, uint16_t n)
{ if(!h||q>=h->total_queues||!b||!n)return 0; void *e[n];
  uint16_t r=(uint16_t)rte_ring_dequeue_burst(h->rx_rings[q],e,n,NULL);
  for(uint16_t i=0;i<r;i++){uint8_t ni=ub_ring_elem_get_node(e[i]);uint64_t o=ub_ring_elem_get_offset(e[i]);sync_peer_data_ring(h,ni);b[i]=(char*)h->peers[ni].data_buf_area+o;}
  return r; }
int rte_ub_ring_enable_mpmc(struct rte_ub_ring_handle *h) {
	if (!h || h->mpmc_enabled) return -EINVAL;
	h->shadow_tx = rte_zmalloc(NULL, h->total_queues * sizeof(void*), 0);
	h->shadow_rx = rte_zmalloc(NULL, h->total_queues * sizeof(void*), 0);
	if (!h->shadow_tx || !h->shadow_rx) return -ENOMEM;
	for (uint32_t q = 0; q < h->total_queues; q++) {
		char n[64]; snprintf(n,64,"stx%u",q); h->shadow_tx[q]=rte_ring_create(n,1024,rte_socket_id(),RING_F_SC_DEQ);
		snprintf(n,64,"srx%u",q); h->shadow_rx[q]=rte_ring_create(n,1024,rte_socket_id(),RING_F_SC_DEQ);
		if (!h->shadow_tx[q] || !h->shadow_rx[q]) return -ENOMEM;
	}
	h->mpmc_enabled = true; return 0;
}
bool rte_ub_ring_is_mpmc_enabled(struct rte_ub_ring_handle *h) { return h&&h->mpmc_enabled; }
uint16_t rte_ub_ring_mp_send_burst(struct rte_ub_ring_handle *h, uint32_t q, void **b, uint16_t n)
{ if(!h||!h->mpmc_enabled||q>=h->total_queues)return 0;return (uint16_t)rte_ring_enqueue_burst(h->shadow_tx[q],(void**)b,n,NULL); }
uint16_t rte_ub_ring_mc_recv_burst(struct rte_ub_ring_handle *h, uint32_t q, void **b, uint16_t n)
{ if(!h||!h->mpmc_enabled||q>=h->total_queues)return 0;return (uint16_t)rte_ring_dequeue_burst(h->shadow_rx[q],(void**)b,n,NULL); }
void rte_ub_ring_flush_tx(struct rte_ub_ring_handle *h, uint32_t q) {
	if (!h || !h->mpmc_enabled || q >= h->total_queues) return;
	void *bufs[64]; uint16_t n;
	while ((n = (uint16_t)rte_ring_dequeue_burst(h->shadow_tx[q], (void**)bufs, 64, NULL)) > 0)
		rte_ub_ring_send_burst(h, q, bufs, n);
}
void rte_ub_ring_flush_rx(struct rte_ub_ring_handle *h, uint32_t q) {
	if (!h || !h->mpmc_enabled || q >= h->total_queues) return;
	void *bufs[64]; uint16_t n;
	while ((n = rte_ub_ring_recv_burst(h, q, bufs, 64)) > 0)
		rte_ring_enqueue_burst(h->shadow_rx[q], (void**)bufs, n, NULL);
}
int rte_ub_ring_get_queue_mapping(struct rte_ub_ring_handle *h, uint32_t pi, uint32_t sq, uint32_t *tx, uint32_t *rx)
{ if(!h||!tx||!rx)return -EINVAL;struct ub_ring_metadata*m=peer_meta(h,pi);if(!m||sq>=m->nb_queues)return -EINVAL;uint32_t q=h->provider_base_qid[pi]+sq;*tx=q;*rx=q;return 0; }
uint32_t rte_ub_ring_queue_to_provider(struct rte_ub_ring_handle *h, uint32_t q)
{ return (!h||q>=h->total_queues)?0:h->queue_to_provider[q]; }
uint32_t rte_ub_ring_get_total_queues(struct rte_ub_ring_handle *h) { return h?h->total_queues:0; }
int rte_ub_ring_get_node_index(struct rte_ub_ring_handle *h, uint32_t *i) { if(!h||!i)return -EINVAL;*i=h->my_node_index;return 0; }
int rte_ub_ring_get_peer_index(struct rte_ub_ring_handle *h, const char *hn, uint32_t *i)
{ if(!h||!hn||!i)return -EINVAL;for(uint32_t j=0;j<h->nb_nodes;j++){if(!peer_meta(h,j))continue;if(!strcmp(peer_meta(h,j)->node_name,hn)){*i=j;return 0;}}return -ENOENT; }
void rte_ub_ring_fini(struct rte_ub_ring_handle *h) {
	if (!h) return;
	for (uint32_t i=0;i<UB_RING_MAX_NODES;i++){if(i==h->my_node_index)continue;if(h->peers[i].shm_base){size_t sz=peer_meta(h,i)?peer_meta(h,i)->total_size:0;ubsmem_shmem_unmap(h->peers[i].shm_base,sz);}}
	if(h->role==UB_RING_ROLE_PROVIDER&&h->peers[h->my_node_index].shm_base){ubsmem_shmem_unmap(h->peers[h->my_node_index].shm_base,peer_meta(h,h->my_node_index)->total_size);char nm[128],my[MAX_HOST_NAME_DESC_LENGTH];if(!gethostname(my,sizeof(my))){snprintf(nm,128,"%s_%s",h->shm_prefix,my);ubsmem_shmem_deallocate(nm);}}
	if(h->data_rings){for(uint32_t i=0;i<UB_RING_MAX_NODES;i++)if(h->data_rings[i])rte_ring_free(h->data_rings[i]);rte_free(h->data_rings);}
	if(h->shadow_tx){for(uint32_t q=0;q<h->total_queues;q++)rte_ring_free(h->shadow_tx[q]);rte_free(h->shadow_tx);}
	if(h->shadow_rx){for(uint32_t q=0;q<h->total_queues;q++)rte_ring_free(h->shadow_rx[q]);rte_free(h->shadow_rx);}
	ubsmem_destroy_region(h->region_name);ubsmem_finalize();rte_free(h->rx_rings);rte_free(h->tx_rings);rte_free(h);
}
