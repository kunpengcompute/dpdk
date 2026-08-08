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

/* ---- logging ---- */
RTE_LOG_REGISTER_DEFAULT(ub_ring_logtype, NOTICE);
#define RTE_LOGTYPE_UB_RING ub_ring_logtype
#define UB_RING_LOG(level, ...) \
	RTE_LOG_LINE_PREFIX(level, UB_RING, "%s(): ", __func__, __VA_ARGS__)

/* ---- internal constants ---- */
#define UB_RING_METADATA_ALIGN    64
#define UB_RING_POLL_INTERVAL_US  50000
#define UB_RING_POLL_TIMEOUT_S    60

/* ---- ubsmem error string ---- */
static const char *
ubs_strerror(int rc)
{
	switch (rc) {
	case UBSM_OK:                return "OK";
	case UBSM_ERR_PARAM_INVALID: return "PARAM_INVALID";
	case UBSM_ERR_NOPERM:        return "NOPERM";
	case UBSM_ERR_MEMORY:        return "MEMORY";
	case UBSM_ERR_UNIMPL:        return "UNIMPL";
	case UBSM_CHECK_RESOURCE_ERROR: return "CHECK_RESOURCE_ERROR";
	case UBSM_ERR_MEMLIB:        return "MEMLIB";
	case UBSM_ERR_NO_NEEDED:     return "NO_NEEDED";
	case UBSM_ERR_BUSY:          return "BUSY";
	case UBSM_ERR_NOT_FOUND:     return "NOT_FOUND";
	case UBSM_ERR_ALREADY_EXIST: return "ALREADY_EXIST";
	case UBSM_ERR_MALLOC_FAIL:   return "MALLOC_FAIL";
	case UBSM_ERR_RECORD:        return "RECORD";
	case UBSM_ERR_IN_USING:      return "IN_USING";
	case UBSM_ERR_NOT_SUPPORTED: return "NOT_SUPPORTED";
	case UBSM_ERR_NET:           return "NET";
	case UBSM_ERR_UBSE:          return "UBSE";
	case UBSM_ERR_OBMM:          return "OBMM";
	case UBSM_ERR_LOCK_NOT_SUPPORTED:   return "LOCK_NOT_SUPPORTED";
	case UBSM_ERR_LOCK_ALREADY_LOCKED:  return "LOCK_ALREADY_LOCKED";
	case UBSM_ERR_DLOCK:         return "DLOCK";
	case UBSM_ERR_BUFF:          return "BUFF";
	default:                     return "UNKNOWN";
	}
}

/* ---- handle (opaque) ---- */

struct rte_ub_ring_handle {
	uint32_t nb_nodes;
	uint32_t my_node_index;
	enum ub_ring_role role;

	struct {
		void            *shm_base;
		struct rte_ring *data_ring;
		void            *data_buf_area;
		void            *data_buf_end;
		uint32_t         data_elt_size;
	} peers[UB_RING_MAX_NODES];

	struct rte_ring **rx_rings;
	struct rte_ring **tx_rings;
	uint32_t total_queues;

	uint32_t queue_to_provider[UB_RING_MAX_QUEUES];
	uint32_t provider_base_qid[UB_RING_MAX_NODES];

	char     region_name[64];
	char     shm_prefix[64];
};

static inline struct ub_ring_metadata *
peer_meta(struct rte_ub_ring_handle *h, uint32_t i)
{
	return (struct ub_ring_metadata *)h->peers[i].shm_base;
}

/* ---- forward declarations ---- */

static int cmp_node(const void *a, const void *b);
static int find_peer_by_buf(struct rte_ub_ring_handle *h, const void *buf);

/* ---- helpers ---- */

static int
cmp_node(const void *a, const void *b)
{
	const char *sa = *(const char **)a;
	const char *sb = *(const char **)b;
	if (!sa) return sb ? -1 : 0;
	if (!sb) return 1;
	return strcmp(sa, sb);
}

static void
sync_peer_data_ring(struct rte_ub_ring_handle *h, uint32_t i)
{
	if (h->peers[i].data_buf_area)
		return;
	struct ub_ring_metadata *meta = peer_meta(h, i);
	if (!meta || !(meta->state & UB_RING_STATE_DATA_RING_READY))
		return;
	h->peers[i].data_elt_size = meta->data_elt_size;
	h->peers[i].data_ring = (struct rte_ring *)(
		(char *)h->peers[i].shm_base +
		meta->ring_area_offset + meta->ring_area_size);
	size_t isz = rte_ring_get_memsize(meta->data_nb_bufs);
	h->peers[i].data_buf_area = (char *)h->peers[i].data_ring + isz;
	h->peers[i].data_buf_end  = (char *)h->peers[i].data_buf_area +
		(size_t)meta->data_nb_bufs * meta->data_elt_size;
}

static int
find_peer_by_buf(struct rte_ub_ring_handle *h, const void *buf)
{
	for (uint32_t i = 0; i < UB_RING_MAX_NODES; i++) {
		sync_peer_data_ring(h, i);
		if (!h->peers[i].data_buf_area || !h->peers[i].data_buf_end)
			continue;
		if ((const char *)buf >= (const char *)h->peers[i].data_buf_area &&
		    (const char *)buf < (const char *)h->peers[i].data_buf_end)
			return (int)i;
	}
	UB_RING_LOG(ERR, "buf %p not in any data ring", buf);
	return -1;
}

/* ---- metadata helpers ---- */

static size_t calc_rsz(uint32_t qd) { return rte_ring_get_memsize(qd); }
static size_t calc_msz(uint32_t nq)
{ return RTE_ALIGN(sizeof(struct ub_ring_metadata) + nq*2*sizeof(uint64_t), UB_RING_METADATA_ALIGN); }
static size_t calc_rasz(uint32_t nq, size_t rs) { return nq*2*rs; }

static void
write_meta(struct ub_ring_metadata *m, uint32_t nbn, uint32_t nq,
	uint32_t ni, uint32_t qd, uint64_t ts, uint64_t ro, uint64_t rs,
	const char *hostname)
{
	memset(m, 0, sizeof(*m));
	m->magic = UB_RING_MAGIC; m->version = UB_RING_VERSION;
	m->nb_nodes = nbn; m->nb_queues = nq; m->node_index = ni;
	m->queue_depth = qd; m->state = 0;
	m->total_size = ts; m->ring_area_offset = ro; m->ring_area_size = rs;
	strncpy(m->node_name, hostname, sizeof(m->node_name) - 1);
}

static int wait_meta(struct ub_ring_metadata *m)
{
	for (int i = 0; i < UB_RING_POLL_TIMEOUT_S * 20; i++) {
		if (m->magic == UB_RING_MAGIC && m->version == UB_RING_VERSION &&
		    (m->state & UB_RING_STATE_META_READY)) return 0;
		usleep(UB_RING_POLL_INTERVAL_US);
	}
	return -1;
}

static int
resolve_my_index(const char **p_nodes, uint32_t np,
		 const char **u_nodes, uint32_t nu,
		 uint32_t *my_idx, uint32_t *total)
{
	char my_host[MAX_HOST_NAME_DESC_LENGTH];
	if (gethostname(my_host, sizeof(my_host)) != 0) { UB_RING_LOG(ERR, "gethostname"); return -1; }
	*total = np + nu;
	const char **all = rte_malloc(NULL, *total * sizeof(char *), 0);
	if (!all) return -1;
	uint32_t k = 0;
	for (uint32_t i = 0; i < np; i++) all[k++] = p_nodes[i];
	for (uint32_t i = 0; i < nu; i++) all[k++] = u_nodes[i];
	qsort(all, *total, sizeof(char *), cmp_node);
	for (uint32_t i = 0; i < *total; i++) {
		if (strcmp(all[i], my_host) == 0) { *my_idx = i; rte_free(all); return 0; }
	}
	rte_free(all);
	UB_RING_LOG(ERR, "hostname '%s' not in node lists", my_host);
	return -1;
}

/*
 * Poll ubsmem_shmem_list_lookup until all provider shms are created.
 */
static int
wait_all_provider_shms(const char *prefix, const char **p_nodes, uint32_t np)
{
	ubsmem_shmem_desc_t list[64];
	for (int t = 0; t < UB_RING_POLL_TIMEOUT_S * 20; t++) {
		uint32_t cnt = 64;
		int rc = ubsmem_shmem_list_lookup(prefix, list, &cnt);
		if (rc != UBSM_OK) { usleep(UB_RING_POLL_INTERVAL_US); continue; }
		uint32_t found = 0;
		for (uint32_t i = 0; i < np; i++) {
			char expected[128];
			snprintf(expected, sizeof(expected), "%s_%s", prefix, p_nodes[i]);
			for (uint32_t k = 0; k < cnt; k++) {
				if (strcmp(list[k].name, expected) == 0) { found++; break; }
			}
		}
		if (found == np) return 0;
		usleep(UB_RING_POLL_INTERVAL_US);
	}
	return -1;
}

/* ================================================================
 *  Provider Init
 * ================================================================ */

struct rte_ub_ring_handle *
rte_ub_ring_provider_init(const char **p_nodes, uint32_t np,
			  const char **u_nodes, uint32_t nu,
			  const char *region, const char *shm_prefix,
			  size_t shm_size, uint32_t nb_queues,
			  uint32_t queue_depth)
{
	char my_hostname[MAX_HOST_NAME_DESC_LENGTH];
	ubsmem_options_t opts;
	ubsmem_region_attributes_t attr;
	uint32_t my_index, total;
	char shm_name[128];
	void *own_shm = NULL;
	size_t meta_size, ring_memsize, ring_area_sz;
	int rc;

	if (!p_nodes || np == 0 || np > UB_RING_MAX_NODES ||
	    nu > UB_RING_MAX_NODES || !region || !shm_prefix || shm_size == 0 ||
	    nb_queues == 0 || nb_queues > UB_RING_MAX_QUEUES ||
	    queue_depth == 0 || !rte_is_power_of_2(queue_depth))
		{ rte_errno = EINVAL; return NULL; }

	struct rte_ub_ring_handle *h = rte_zmalloc(NULL, sizeof(*h), 0);
	if (!h) { UB_RING_LOG(ERR, "alloc handle"); return NULL; }
	h->role = UB_RING_ROLE_PROVIDER;
	snprintf(h->region_name, sizeof(h->region_name), "%s", region);
	snprintf(h->shm_prefix, sizeof(h->shm_prefix), "%s", shm_prefix);

	if (resolve_my_index(p_nodes, np, u_nodes, nu, &my_index, &total) < 0)
		goto fail;
	h->nb_nodes = total; h->my_node_index = my_index;

	if (gethostname(my_hostname, sizeof(my_hostname)) != 0) { UB_RING_LOG(ERR, "gethostname"); goto fail; }

	UB_RING_LOG(INFO, "provider: hostname='%s' idx=%u/%u nq=%u qd=%u sz=%zu",
		    my_hostname, my_index, total, nb_queues, queue_depth, shm_size);

	rc = ubsmem_init_attributes(&opts);
	if (rc) { UB_RING_LOG(ERR, "init_attr: %s(%d)", ubs_strerror(rc), rc); goto fail; }
	rc = ubsmem_initialize(&opts);
	if (rc) { UB_RING_LOG(ERR, "init: %s(%d)", ubs_strerror(rc), rc); goto fail_ub; }

	memset(&attr, 0, sizeof(attr)); attr.host_num = total;
	for (uint32_t i = 0; i < np; i++) {
		strncpy(attr.hosts[i].host_name, p_nodes[i], MAX_HOST_NAME_DESC_LENGTH - 1);
		attr.hosts[i].affinity = true;
	}
	for (uint32_t i = 0; i < nu; i++) {
		strncpy(attr.hosts[np + i].host_name, u_nodes[i], MAX_HOST_NAME_DESC_LENGTH - 1);
		attr.hosts[np + i].affinity = true;
	}
	ubsmem_destroy_region(region);
	rc = ubsmem_create_region(region, 0, &attr);
	if (rc) { UB_RING_LOG(ERR, "region: %s(%d)", ubs_strerror(rc), rc); goto fail_ub; }

	/* create own shm */
	snprintf(shm_name, sizeof(shm_name), "%s_%s", shm_prefix, my_hostname);
	ubsmem_shmem_deallocate(shm_name);
	rc = ubsmem_shmem_allocate(region, shm_name, shm_size, 0600,
		UBSM_FLAG_NONCACHE | UBSM_FLAG_WR_DELAY_COMP);
	if (rc) { UB_RING_LOG(ERR, "alloc shm: %s(%d)", ubs_strerror(rc), rc); goto fail_reg; }
	rc = ubsmem_shmem_map(NULL, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_name, 0, &own_shm);
	if (rc) { UB_RING_LOG(ERR, "map shm: %s(%d)", ubs_strerror(rc), rc); goto fail_reg; }
	memset(own_shm, 0, shm_size);
	h->peers[my_index].shm_base = own_shm;

	/* init tx/rx rings */
	meta_size = calc_msz(nb_queues); ring_memsize = calc_rsz(queue_depth);
	ring_area_sz = calc_rasz(nb_queues, ring_memsize);
	for (uint32_t q = 0; q < nb_queues; q++) {
		char rn[32]; void *rx, *tx;
		rx = (char *)own_shm + meta_size + q * ring_memsize;
		tx = (char *)own_shm + meta_size + (nb_queues + q) * ring_memsize;
		snprintf(rn, 32, "rx_q%u", q);
		rc = rte_ring_init((struct rte_ring *)rx, rn, queue_depth, RING_F_SP_ENQ | RING_F_SC_DEQ);
		if (rc < 0) { UB_RING_LOG(ERR, "rx_q%u: %d", q, rc); goto fail_reg; }
		snprintf(rn, 32, "tx_q%u", q);
		rc = rte_ring_init((struct rte_ring *)tx, rn, queue_depth, RING_F_SP_ENQ | RING_F_SC_DEQ);
		if (rc < 0) { UB_RING_LOG(ERR, "tx_q%u: %d", q, rc); goto fail_reg; }
	}

	/* write metadata */
	struct ub_ring_metadata *om = (struct ub_ring_metadata *)own_shm;
	write_meta(om, total, nb_queues, my_index, queue_depth, shm_size, meta_size, ring_area_sz, my_hostname);
	for (uint32_t q = 0; q < nb_queues; q++) {
		om->queue_offsets[q] = meta_size + q * ring_memsize;
		om->queue_offsets[nb_queues + q] = meta_size + (nb_queues + q) * ring_memsize;
	}
	om->state |= UB_RING_STATE_META_READY;
	h->peers[my_index].data_ring = NULL;
	h->peers[my_index].data_buf_area = NULL;

	/* wait for all provider shms to appear */
	UB_RING_LOG(INFO, "waiting for %u provider shms...", np);
	if (wait_all_provider_shms(shm_prefix, p_nodes, np) < 0) {
		UB_RING_LOG(ERR, "timeout waiting for provider shms");
		goto fail_reg;
	}

	/* discover + map peer shms */
	for (uint32_t i = 0; i < np; i++) {
		if (strcmp(p_nodes[i], my_hostname) == 0) continue;

		char peer_name[128];
		snprintf(peer_name, sizeof(peer_name), "%s_%s", shm_prefix, p_nodes[i]);

		ubsmem_shmem_desc_t list[32]; uint32_t cnt = 32;
		uint64_t psz = 0;
		rc = ubsmem_shmem_list_lookup(shm_prefix, list, &cnt);
		if (rc != UBSM_OK) continue;
		for (uint32_t k = 0; k < cnt; k++)
			if (strcmp(list[k].name, peer_name) == 0) { psz = list[k].size; break; }
		if (!psz) continue;

		void *ps = NULL;
		rc = ubsmem_shmem_map(NULL, psz, PROT_READ | PROT_WRITE, MAP_SHARED, peer_name, 0, &ps);
		if (rc) { UB_RING_LOG(ERR, "map %s: %s(%d)", peer_name, ubs_strerror(rc), rc); goto fail_reg; }

		uint32_t pi = UINT32_MAX;
		for (uint32_t j = 0; j < total; j++) {
			const char *n = j < np ? p_nodes[j] : u_nodes[j - np];
			if (strcmp(n, p_nodes[i]) == 0) { pi = j; break; }
		}
		if (pi >= UB_RING_MAX_NODES) continue;

		h->peers[pi].shm_base = ps;
		if (wait_meta((struct ub_ring_metadata *)ps) < 0) {
			UB_RING_LOG(ERR, "meta timeout %s", peer_name); goto fail_reg; }
		h->peers[pi].data_ring = NULL;
		h->peers[pi].data_buf_area = NULL;
		UB_RING_LOG(INFO, "mapped peer %s (pi=%u)", peer_name, pi);
	}

	/* ring pointers */
	h->total_queues = nb_queues;
	h->rx_rings = rte_malloc(NULL, nb_queues * sizeof(struct rte_ring *), 0);
	h->tx_rings = rte_malloc(NULL, nb_queues * sizeof(struct rte_ring *), 0);
	if (!h->rx_rings || !h->tx_rings) goto fail_reg;
	for (uint32_t q = 0; q < nb_queues; q++) {
		h->rx_rings[q] = (struct rte_ring *)((char *)own_shm + om->queue_offsets[q]);
		h->tx_rings[q] = (struct rte_ring *)((char *)own_shm + om->queue_offsets[nb_queues + q]);
		h->queue_to_provider[q] = my_index;
	}
	h->provider_base_qid[my_index] = 0;

	UB_RING_LOG(INFO, "provider init done"); return h;

fail_reg: ubsmem_destroy_region(region);
fail_ub: ubsmem_finalize();
fail: rte_free(h); return NULL;
}

/* ================================================================
 *  User Init
 * ================================================================ */

struct rte_ub_ring_handle *
rte_ub_ring_user_init(const char **p_nodes, uint32_t np,
		      const char **u_nodes, uint32_t nu,
		      const char *region, const char *shm_prefix,
		      uint32_t data_elt_size)
{
	char my_hostname[MAX_HOST_NAME_DESC_LENGTH];
	ubsmem_options_t opts;
	ubsmem_region_attributes_t attr;
	uint32_t my_index, total;
	int rc;

	if (!p_nodes || np == 0 || np > UB_RING_MAX_NODES ||
	    nu > UB_RING_MAX_NODES || !region || !shm_prefix || data_elt_size == 0)
		{ rte_errno = EINVAL; return NULL; }

	struct rte_ub_ring_handle *h = rte_zmalloc(NULL, sizeof(*h), 0);
	if (!h) { UB_RING_LOG(ERR, "alloc handle"); return NULL; }
	h->role = UB_RING_ROLE_USER;
	snprintf(h->region_name, sizeof(h->region_name), "%s", region);
	snprintf(h->shm_prefix, sizeof(h->shm_prefix), "%s", shm_prefix);

	if (resolve_my_index(p_nodes, np, u_nodes, nu, &my_index, &total) < 0)
		goto fail;
	h->nb_nodes = total; h->my_node_index = my_index;

	if (gethostname(my_hostname, sizeof(my_hostname)) != 0) { UB_RING_LOG(ERR, "gethostname"); goto fail; }

	UB_RING_LOG(INFO, "user: hostname='%s' idx=%u/%u elt=%u",
		    my_hostname, my_index, total, data_elt_size);

	rc = ubsmem_init_attributes(&opts);
	if (rc) { UB_RING_LOG(ERR, "init_attr: %s(%d)", ubs_strerror(rc), rc); goto fail; }
	rc = ubsmem_initialize(&opts);
	if (rc) { UB_RING_LOG(ERR, "init: %s(%d)", ubs_strerror(rc), rc); goto fail_ub; }

	memset(&attr, 0, sizeof(attr)); attr.host_num = total;
	for (uint32_t i = 0; i < np; i++) {
		strncpy(attr.hosts[i].host_name, p_nodes[i], MAX_HOST_NAME_DESC_LENGTH - 1);
		attr.hosts[i].affinity = true;
	}
	for (uint32_t i = 0; i < nu; i++) {
		strncpy(attr.hosts[np + i].host_name, u_nodes[i], MAX_HOST_NAME_DESC_LENGTH - 1);
		attr.hosts[np + i].affinity = true;
	}
	ubsmem_destroy_region(region);
	rc = ubsmem_create_region(region, 0, &attr);
	if (rc) { UB_RING_LOG(ERR, "region: %s(%d)", ubs_strerror(rc), rc); goto fail_ub; }

	/* wait for all provider shms */
	UB_RING_LOG(INFO, "waiting for %u provider shms...", np);
	if (wait_all_provider_shms(shm_prefix, p_nodes, np) < 0) {
		UB_RING_LOG(ERR, "timeout waiting for provider shms");
		goto fail_reg;
	}

	/* discover + map provider shms + init data rings */
	uint32_t nb_prov = 0, pi_arr[UB_RING_MAX_NODES], pq_arr[UB_RING_MAX_NODES];

	for (uint32_t i = 0; i < np; i++) {
		if (strcmp(p_nodes[i], my_hostname) == 0) continue;

		char peer_name[128];
		snprintf(peer_name, sizeof(peer_name), "%s_%s", shm_prefix, p_nodes[i]);

		ubsmem_shmem_desc_t list[32]; uint32_t cnt = 32;
		uint64_t psz = 0;
		rc = ubsmem_shmem_list_lookup(shm_prefix, list, &cnt);
		if (rc != UBSM_OK) continue;
		for (uint32_t k = 0; k < cnt; k++)
			if (strcmp(list[k].name, peer_name) == 0) { psz = list[k].size; break; }
		if (!psz) continue;

		void *ps = NULL;
		rc = ubsmem_shmem_map(NULL, psz, PROT_READ | PROT_WRITE, MAP_SHARED, peer_name, 0, &ps);
		if (rc) { UB_RING_LOG(ERR, "map %s: %s(%d)", peer_name, ubs_strerror(rc), rc); goto fail_reg; }

		uint32_t pi = UINT32_MAX;
		for (uint32_t j = 0; j < total; j++) {
			const char *n = j < np ? p_nodes[j] : u_nodes[j - np];
			if (strcmp(n, p_nodes[i]) == 0) { pi = j; break; }
		}
		if (pi >= UB_RING_MAX_NODES) continue;

		h->peers[pi].shm_base = ps;
		struct ub_ring_metadata *pm = (struct ub_ring_metadata *)ps;
		if (wait_meta(pm) < 0) { UB_RING_LOG(ERR, "meta timeout %s", peer_name); goto fail_reg; }
		h->peers[pi].data_elt_size = data_elt_size;

		uint64_t dr_off = pm->ring_area_offset + pm->ring_area_size;
		uint64_t dr_sz  = pm->total_size - dr_off;
		uint32_t mx = (uint32_t)((dr_sz - sizeof(struct rte_ring)) / (8 + data_elt_size));
		mx = rte_align32prevpow2(mx);
		while (rte_ring_get_memsize(mx) + (size_t)mx * data_elt_size > dr_sz) mx /= 2;
		if (mx < 2) { UB_RING_LOG(ERR, "data ring too small"); goto fail_reg; }

		size_t irs = rte_ring_get_memsize(mx);
		void *dr = (char *)ps + dr_off;
		struct rte_ring *ir = (struct rte_ring *)dr;
		char rn[64]; snprintf(rn, 64, "data_%s", peer_name);
		rc = rte_ring_init(ir, rn, mx, RING_F_SC_DEQ);
		if (rc < 0) { UB_RING_LOG(ERR, "data_ring: %d", rc); goto fail_reg; }
		void *ba = (char *)dr + irs;

		for (uint32_t idx = 0; idx < mx; idx++) {
			void *e = (void *)(uintptr_t)(idx * data_elt_size);
			rte_ring_enqueue_burst(ir, &e, 1, NULL);
		}
		h->peers[pi].data_ring = ir;
		h->peers[pi].data_buf_area = ba;
		h->peers[pi].data_buf_end  = (char *)ba + (size_t)mx * data_elt_size;
		pm->data_elt_size = data_elt_size; pm->data_nb_bufs = mx;
		pm->state |= UB_RING_STATE_DATA_RING_READY;
		pi_arr[nb_prov] = pi; pq_arr[nb_prov] = pm->nb_queues; nb_prov++;
		UB_RING_LOG(INFO, "data ring %s: %u bufs", peer_name, mx);
	}

	h->peers[my_index].data_ring = NULL;
	h->peers[my_index].data_buf_area = NULL;

	uint32_t tq = 0;
	for (uint32_t p = 0; p < nb_prov; p++) tq += pq_arr[p];
	h->total_queues = tq;
	h->rx_rings = rte_malloc(NULL, tq * sizeof(struct rte_ring *), 0);
	h->tx_rings = rte_malloc(NULL, tq * sizeof(struct rte_ring *), 0);
	if (!h->rx_rings || !h->tx_rings) goto fail_reg;

	uint32_t idx = 0;
	for (uint32_t p = 0; p < nb_prov; p++) {
		uint32_t pi = pi_arr[p], nq = pq_arr[p];
		h->provider_base_qid[pi] = idx;
		struct ub_ring_metadata *m = peer_meta(h, pi);
		void *sh = h->peers[pi].shm_base;
		for (uint32_t q = 0; q < nq; q++) {
			h->tx_rings[idx] = (struct rte_ring *)((char *)sh + m->queue_offsets[q]);
			h->rx_rings[idx] = (struct rte_ring *)((char *)sh + m->queue_offsets[nq + q]);
			h->queue_to_provider[idx] = pi; idx++;
		}
	}
	UB_RING_LOG(INFO, "user init done: %u queues", tq); return h;

fail_reg: ubsmem_destroy_region(region);
fail_ub: ubsmem_finalize();
fail: rte_free(h); return NULL;
}

/* ---- Alloc/Free/Send/Recv/Queue/Fini unchanged ---- */
void *rte_ub_ring_alloc_buf(struct rte_ub_ring_handle *h, uint32_t pi) {
	if (!h || pi >= UB_RING_MAX_NODES) return NULL;
	sync_peer_data_ring(h, pi);
	struct rte_ring *dr = h->peers[pi].data_ring; if (!dr) return NULL;
	void *e; if (rte_ring_sc_dequeue_bulk(dr, &e, 1, NULL) == 0) return NULL;
	return (char *)h->peers[pi].data_buf_area + (size_t)(uintptr_t)e;
}
void rte_ub_ring_free_buf(struct rte_ub_ring_handle *h, void *buf) {
	if (!h || !buf) return;
	int ni = find_peer_by_buf(h, buf); if (ni < 0) return;
	struct rte_ring *dr = h->peers[ni].data_ring; if (!dr) return;
	void *e = (void *)(uintptr_t)((char *)buf - (char *)h->peers[ni].data_buf_area);
	rte_ring_mp_enqueue_bulk(dr, &e, 1, NULL);
}
uint32_t rte_ub_ring_get_data_elt_size(struct rte_ub_ring_handle *h, uint32_t pi) {
	if (!h || pi >= UB_RING_MAX_NODES) return 0;
	sync_peer_data_ring(h, pi); return h->peers[pi].data_elt_size;
}
void *rte_ub_ring_get_data_buf_area(struct rte_ub_ring_handle *h, uint32_t pi) {
	if (!h || pi >= UB_RING_MAX_NODES) return NULL;
	return h->peers[pi].data_buf_area;
}
uint16_t rte_ub_ring_send_burst(struct rte_ub_ring_handle *h, uint32_t qid, void **bufs, uint16_t nb) {
	if (!h || qid >= h->total_queues || !bufs || nb == 0) return 0;
	void *elems[nb]; uint16_t c = 0;
	for (uint16_t i = 0; i < nb; i++) {
		if (!bufs[i]) continue;
		int ni = find_peer_by_buf(h, bufs[i]); if (ni < 0) continue;
		uint64_t off = (uint64_t)((char *)bufs[i] - (char *)h->peers[ni].data_buf_area);
		elems[c++] = ub_ring_elem_pack((uint8_t)ni, off);
	}
	return (uint16_t)rte_ring_enqueue_burst(h->tx_rings[qid], elems, c, NULL);
}
uint16_t rte_ub_ring_recv_burst(struct rte_ub_ring_handle *h, uint32_t qid, void **bufs, uint16_t nb) {
	if (!h || qid >= h->total_queues || !bufs || nb == 0) return 0;
	void *elems[nb];
	uint16_t n = (uint16_t)rte_ring_dequeue_burst(h->rx_rings[qid], elems, nb, NULL);
	for (uint16_t i = 0; i < n; i++) {
		uint8_t ni = ub_ring_elem_get_node(elems[i]);
		uint64_t off = ub_ring_elem_get_offset(elems[i]);
		sync_peer_data_ring(h, ni);
		bufs[i] = (char *)h->peers[ni].data_buf_area + off;
	}
	return n;
}
int rte_ub_ring_get_queue_mapping(struct rte_ub_ring_handle *h, uint32_t pi, uint32_t sq, uint32_t *tx, uint32_t *rx) {
	if (!h || !tx || !rx) return -EINVAL;
	struct ub_ring_metadata *m = peer_meta(h, pi);
	if (!m || sq >= m->nb_queues) return -EINVAL;
	uint32_t q = h->provider_base_qid[pi] + sq; *tx = q; *rx = q; return 0;
}
uint32_t rte_ub_ring_queue_to_provider(struct rte_ub_ring_handle *h, uint32_t qid)
{ return (!h || qid >= h->total_queues) ? 0 : h->queue_to_provider[qid]; }
uint32_t rte_ub_ring_get_total_queues(struct rte_ub_ring_handle *h) { return h ? h->total_queues : 0; }
int rte_ub_ring_get_node_index(struct rte_ub_ring_handle *h, uint32_t *idx)
{ if (!h || !idx) return -EINVAL; *idx = h->my_node_index; return 0; }
int rte_ub_ring_get_peer_index(struct rte_ub_ring_handle *h, const char *hostname, uint32_t *idx) {
	if (!h || !hostname || !idx) return -EINVAL;
	for (uint32_t i = 0; i < h->nb_nodes; i++) {
		if (!peer_meta(h, i)) continue;
		if (strcmp(peer_meta(h, i)->node_name, hostname) == 0) { *idx = i; return 0; }
	}
	return -ENOENT;
}
void rte_ub_ring_fini(struct rte_ub_ring_handle *h) {
	if (!h) return;
	for (uint32_t i = 0; i < UB_RING_MAX_NODES; i++) {
		if (i == h->my_node_index) continue;
		if (h->peers[i].shm_base) {
			size_t sz = peer_meta(h, i) ? peer_meta(h, i)->total_size : 0;
			ubsmem_shmem_unmap(h->peers[i].shm_base, sz);
			h->peers[i].shm_base = NULL;
		}
	}
	if (h->role == UB_RING_ROLE_PROVIDER && h->peers[h->my_node_index].shm_base) {
		ubsmem_shmem_unmap(h->peers[h->my_node_index].shm_base,
			peer_meta(h, h->my_node_index)->total_size);
		char nm[128], my_host[MAX_HOST_NAME_DESC_LENGTH];
		if (gethostname(my_host, sizeof(my_host)) == 0) {
			snprintf(nm, 128, "%s_%s", h->shm_prefix, my_host);
			ubsmem_shmem_deallocate(nm);
		}
	}
	ubsmem_destroy_region(h->region_name);
	ubsmem_finalize();
	rte_free(h->rx_rings); rte_free(h->tx_rings); rte_free(h);
}
