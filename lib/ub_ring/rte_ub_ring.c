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
#include "rte_ub_bootstrap.h"

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

/* ---- internal context (== rte_ub_ring_handle) ---- */

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
	if (!sa)
		return sb ? -1 : 0;
	if (!sb)
		return 1;
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

	size_t idx_ring_sz = rte_ring_get_memsize(meta->data_nb_bufs);
	h->peers[i].data_buf_area =
		(char *)h->peers[i].data_ring + idx_ring_sz;
	h->peers[i].data_buf_end =
		(char *)h->peers[i].data_buf_area +
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
	UB_RING_LOG(ERR, "buf %p does not belong to any shm data ring", buf);
	return -1;
}

/* ---- metadata helpers ---- */

static size_t
calc_ring_memsize(uint32_t queue_depth)
{
	return rte_ring_get_memsize(queue_depth);
}

static size_t
calc_metadata_size(uint32_t nb_queues)
{
	size_t base = sizeof(struct ub_ring_metadata) +
		      nb_queues * 2 * sizeof(uint64_t);
	return RTE_ALIGN(base, UB_RING_METADATA_ALIGN);
}

static size_t
calc_ring_area_size(uint32_t nb_queues, size_t ring_memsize)
{
	return nb_queues * 2 * ring_memsize;
}

static void
write_metadata_provider(struct ub_ring_metadata *meta, uint32_t nb_nodes,
	uint32_t nb_queues, uint32_t node_index, uint32_t queue_depth,
	uint64_t total_size, uint64_t ring_area_off, uint64_t ring_area_sz,
	const char *hostname)
{
	memset(meta, 0, sizeof(*meta));
	meta->magic       = UB_RING_MAGIC;
	meta->version     = UB_RING_VERSION;
	meta->nb_nodes    = nb_nodes;
	meta->nb_queues   = nb_queues;
	meta->node_index  = node_index;
	meta->queue_depth = queue_depth;
	meta->state       = 0;
	meta->data_elt_size    = 0;
	meta->data_nb_bufs     = 0;
	meta->total_size       = total_size;
	meta->ring_area_offset = ring_area_off;
	meta->ring_area_size   = ring_area_sz;
	strncpy(meta->node_name, hostname, sizeof(meta->node_name) - 1);
}

static int
wait_meta_ready(struct ub_ring_metadata *meta)
{
	for (int i = 0; i < UB_RING_POLL_TIMEOUT_S * 20; i++) {
		if (meta->magic == UB_RING_MAGIC &&
		    meta->version == UB_RING_VERSION &&
		    (meta->state & UB_RING_STATE_META_READY))
			return 0;
		usleep(UB_RING_POLL_INTERVAL_US);
	}
	return -1;
}

/*
 * Merge provider and user node lists, sort alphabetically, find my index.
 */
static int
resolve_my_index(const char **p_nodes, uint32_t np,
		 const char **u_nodes, uint32_t nu,
		 uint32_t *my_idx, uint32_t *total)
{
	char my_hostname[MAX_HOST_NAME_DESC_LENGTH];
	const char **all;
	uint32_t k;

	if (gethostname(my_hostname, sizeof(my_hostname)) != 0) {
		UB_RING_LOG(ERR, "gethostname failed");
		return -1;
	}

	*total = np + nu;
	all = rte_malloc(NULL, *total * sizeof(char *), 0);
	if (!all) {
		UB_RING_LOG(ERR, "malloc for node list failed");
		return -1;
	}

	k = 0;
	for (uint32_t i = 0; i < np; i++)
		all[k++] = p_nodes[i];
	for (uint32_t i = 0; i < nu; i++)
		all[k++] = u_nodes[i];

	qsort(all, *total, sizeof(char *), cmp_node);

	for (uint32_t i = 0; i < *total; i++) {
		if (strcmp(all[i], my_hostname) == 0) {
			*my_idx = i;
			rte_free(all);
			return 0;
		}
	}

	rte_free(all);
	UB_RING_LOG(ERR, "hostname '%s' not found in node lists", my_hostname);
	return -1;
}

/*
 * Wait for bootstrap to reach BS_READY state.
 */
static int
wait_bootstrap_ready(void)
{
	for (int t = 0; t < UB_RING_POLL_TIMEOUT_S * 20; t++) {
		struct bootstrap *bs = ub_bootstrap_get();

		ubsmem_shmem_read_lock(BOOTSTRAP_SHM_NAME);
		int ready = (bs->hdr.magic == BOOTSTRAP_MAGIC &&
			     bs->hdr.state == BS_READY);
		ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);
		if (ready)
			return 0;
		usleep(UB_RING_POLL_INTERVAL_US);
	}
	UB_RING_LOG(ERR, "timeout waiting for bootstrap READY");
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
	    nu > UB_RING_MAX_NODES ||
	    !region || !shm_prefix || shm_size == 0 ||
	    nb_queues == 0 || nb_queues > UB_RING_MAX_QUEUES ||
	    queue_depth == 0 || !rte_is_power_of_2(queue_depth)) {
		rte_errno = EINVAL;
		return NULL;
	}

	struct rte_ub_ring_handle *h = rte_zmalloc(NULL, sizeof(*h), 0);
	if (!h) {
		UB_RING_LOG(ERR, "failed to allocate handle");
		return NULL;
	}
	h->role = UB_RING_ROLE_PROVIDER;
	snprintf(h->region_name, sizeof(h->region_name), "%s", region);
	snprintf(h->shm_prefix, sizeof(h->shm_prefix), "%s", shm_prefix);

	/* resolve my sorted index */
	if (resolve_my_index(p_nodes, np, u_nodes, nu,
			     &my_index, &total) < 0) {
		UB_RING_LOG(ERR, "failed to resolve node index");
		goto fail;
	}
	h->nb_nodes      = total;
	h->my_node_index = my_index;

	if (gethostname(my_hostname, sizeof(my_hostname)) != 0) {
		UB_RING_LOG(ERR, "gethostname failed");
		goto fail;
	}

	UB_RING_LOG(INFO, "provider init: hostname='%s', idx=%u/%u, "
		    "nq=%u, qd=%u, shm_sz=%zu",
		    my_hostname, my_index, total,
		    nb_queues, queue_depth, shm_size);

	/* ubsmem init */
	rc = ubsmem_init_attributes(&opts);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "ubsmem_init_attributes: %s(%d)",
			    ubs_strerror(rc), rc);
		goto fail;
	}
	rc = ubsmem_initialize(&opts);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "ubsmem_initialize: %s(%d)",
			    ubs_strerror(rc), rc);
		goto fail_ubsmem;
	}

	/* create region */
	memset(&attr, 0, sizeof(attr));
	attr.host_num = total;
	for (uint32_t i = 0; i < np; i++) {
		strncpy(attr.hosts[i].host_name, p_nodes[i],
			MAX_HOST_NAME_DESC_LENGTH - 1);
		attr.hosts[i].affinity = true;
	}
	for (uint32_t i = 0; i < nu; i++) {
		strncpy(attr.hosts[np + i].host_name, u_nodes[i],
			MAX_HOST_NAME_DESC_LENGTH - 1);
		attr.hosts[np + i].affinity = true;
	}
	ubsmem_destroy_region(region);
	rc = ubsmem_create_region(region, 0, &attr);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "create_region(%s): %s(%d)",
			    region, ubs_strerror(rc), rc);
		goto fail_ubsmem;
	}

	/* bootstrap */
	rc = ub_bootstrap_init_or_map(region);
	if (rc < 0) {
		UB_RING_LOG(ERR, "bootstrap init/map failed");
		goto fail_region;
	}

	/* create own shm */
	snprintf(shm_name, sizeof(shm_name), "%s_%s",
		 shm_prefix, my_hostname);

	ubsmem_shmem_deallocate(shm_name);
	uint64_t flags = UBSM_FLAG_NONCACHE | UBSM_FLAG_WR_DELAY_COMP;
	rc = ubsmem_shmem_allocate(region, shm_name, shm_size, 0600, flags);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "shmem_allocate(%s, %zu): %s(%d)",
			    shm_name, shm_size, ubs_strerror(rc), rc);
		goto fail_region;
	}
	rc = ubsmem_shmem_map(NULL, shm_size, PROT_READ | PROT_WRITE,
			      MAP_SHARED, shm_name, 0, &own_shm);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "shmem_map(%s): %s(%d)",
			    shm_name, ubs_strerror(rc), rc);
		goto fail_region;
	}
	memset(own_shm, 0, shm_size);
	h->peers[my_index].shm_base = own_shm;

	/* init tx/rx rings */
	meta_size    = calc_metadata_size(nb_queues);
	ring_memsize = calc_ring_memsize(queue_depth);
	ring_area_sz = calc_ring_area_size(nb_queues, ring_memsize);

	UB_RING_LOG(INFO, "layout: meta=%zu ring_area=%zu "
		    "data_ring at 0x%zx size=0x%zx",
		    meta_size, ring_area_sz,
		    meta_size + ring_area_sz,
		    shm_size - meta_size - ring_area_sz);

	for (uint32_t q = 0; q < nb_queues; q++) {
		char rname[32];
		void *rx_addr = (char *)own_shm + meta_size +
				q * ring_memsize;
		void *tx_addr = (char *)own_shm + meta_size +
				(nb_queues + q) * ring_memsize;

		snprintf(rname, sizeof(rname), "rx_q%u", q);
		rc = rte_ring_init((struct rte_ring *)rx_addr, rname,
				   queue_depth,
				   RING_F_SP_ENQ | RING_F_SC_DEQ);
		if (rc < 0) {
			UB_RING_LOG(ERR, "rte_ring_init rx_q%u failed: %d",
				    q, rc);
			goto fail_region;
		}

		snprintf(rname, sizeof(rname), "tx_q%u", q);
		rc = rte_ring_init((struct rte_ring *)tx_addr, rname,
				   queue_depth,
				   RING_F_SP_ENQ | RING_F_SC_DEQ);
		if (rc < 0) {
			UB_RING_LOG(ERR, "rte_ring_init tx_q%u failed: %d",
				    q, rc);
			goto fail_region;
		}
	}

	/* write metadata */
	struct ub_ring_metadata *own_meta =
		(struct ub_ring_metadata *)own_shm;
	write_metadata_provider(own_meta, total, nb_queues, my_index,
				queue_depth, shm_size, meta_size,
				ring_area_sz, my_hostname);

	for (uint32_t q = 0; q < nb_queues; q++) {
		own_meta->queue_offsets[q] =
			meta_size + q * ring_memsize;
		own_meta->queue_offsets[nb_queues + q] =
			meta_size + (nb_queues + q) * ring_memsize;
	}
	own_meta->state |= UB_RING_STATE_META_READY;
	h->peers[my_index].data_ring = NULL;
	h->peers[my_index].data_buf_area = NULL;

	/* register in bootstrap (shm first, then provider) */
	ub_bootstrap_register_shm(shm_name, my_hostname, shm_size);
	ub_bootstrap_register_provider(my_hostname, np);

	/* ★ wait for all providers to register → bootstrap READY */
	UB_RING_LOG(INFO, "waiting for bootstrap READY...");
	if (wait_bootstrap_ready() < 0)
		goto fail_region;
	UB_RING_LOG(INFO, "bootstrap READY, discovering peers");

	/* discover peer shms via bootstrap */
	struct bootstrap *bs = ub_bootstrap_get();

	ubsmem_shmem_read_lock(BOOTSTRAP_SHM_NAME);
	uint32_t nb_shms = bs->hdr.nb_shms;
	ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);

	for (uint32_t s = 0; s < nb_shms; s++) {
		ubsmem_shmem_read_lock(BOOTSTRAP_SHM_NAME);
		int ready = (bs->shms[s].state == SHM_READY);
		char owner[64], name[64];
		uint64_t sz;
		if (ready) {
			strncpy(owner, bs->shms[s].owner_name, 63);
			strncpy(name, bs->shms[s].name, 63);
			sz = bs->shms[s].size;
		}
		ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);
		if (!ready || strcmp(owner, my_hostname) == 0)
			continue;

		/* find sorted index for owner */
		uint32_t pi = UINT32_MAX;
		for (uint32_t j = 0; j < total; j++) {
			const char *n = j < np ? p_nodes[j] : u_nodes[j - np];
			if (strcmp(n, owner) == 0) { pi = j; break; }
		}
		if (pi >= UB_RING_MAX_NODES)
			continue;

		void *peer_shm = NULL;
		rc = ubsmem_shmem_map(NULL, sz, PROT_READ | PROT_WRITE,
				      MAP_SHARED, name, 0, &peer_shm);
		if (rc != UBSM_OK) {
			UB_RING_LOG(ERR, "shmem_map(%s): %s(%d)",
				    name, ubs_strerror(rc), rc);
			goto fail_region;
		}
		h->peers[pi].shm_base = peer_shm;

		if (wait_meta_ready((struct ub_ring_metadata *)peer_shm) < 0) {
			UB_RING_LOG(ERR, "timeout meta_ready on '%s'", name);
			goto fail_region;
		}
		h->peers[pi].data_ring = NULL;
		h->peers[pi].data_buf_area = NULL;

		UB_RING_LOG(INFO, "mapped peer shm '%s' (owner=%s, pi=%u)",
			    name, owner, pi);
	}

	/* set up ring pointers */
	h->total_queues = nb_queues;
	h->rx_rings = rte_malloc(NULL, nb_queues * sizeof(struct rte_ring *), 0);
	h->tx_rings = rte_malloc(NULL, nb_queues * sizeof(struct rte_ring *), 0);
	if (!h->rx_rings || !h->tx_rings) {
		UB_RING_LOG(ERR, "ring pointer array alloc failed");
		goto fail_region;
	}
	for (uint32_t q = 0; q < nb_queues; q++) {
		h->rx_rings[q] = (struct rte_ring *)((char *)own_shm +
			own_meta->queue_offsets[q]);
		h->tx_rings[q] = (struct rte_ring *)((char *)own_shm +
			own_meta->queue_offsets[nb_queues + q]);
		h->queue_to_provider[q] = my_index;
	}
	h->provider_base_qid[my_index] = 0;

	UB_RING_LOG(INFO, "provider init complete");
	return h;

fail_region:
	ubsmem_destroy_region(region);
fail_ubsmem:
	ubsmem_finalize();
fail:
	rte_free(h);
	return NULL;
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
	    nu > UB_RING_MAX_NODES ||
	    !region || !shm_prefix || data_elt_size == 0) {
		rte_errno = EINVAL;
		return NULL;
	}

	struct rte_ub_ring_handle *h = rte_zmalloc(NULL, sizeof(*h), 0);
	if (!h) {
		UB_RING_LOG(ERR, "failed to allocate handle");
		return NULL;
	}
	h->role = UB_RING_ROLE_USER;
	snprintf(h->region_name, sizeof(h->region_name), "%s", region);
	snprintf(h->shm_prefix, sizeof(h->shm_prefix), "%s", shm_prefix);

	if (resolve_my_index(p_nodes, np, u_nodes, nu,
			     &my_index, &total) < 0) {
		UB_RING_LOG(ERR, "failed to resolve node index");
		goto fail;
	}
	h->nb_nodes      = total;
	h->my_node_index = my_index;

	if (gethostname(my_hostname, sizeof(my_hostname)) != 0) {
		UB_RING_LOG(ERR, "gethostname failed");
		goto fail;
	}

	UB_RING_LOG(INFO, "user init: hostname='%s', idx=%u/%u, "
		    "data_elt_size=%u",
		    my_hostname, my_index, total, data_elt_size);

	rc = ubsmem_init_attributes(&opts);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "ubsmem_init_attributes: %s(%d)",
			    ubs_strerror(rc), rc);
		goto fail;
	}
	rc = ubsmem_initialize(&opts);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "ubsmem_initialize: %s(%d)",
			    ubs_strerror(rc), rc);
		goto fail_ubsmem;
	}

	memset(&attr, 0, sizeof(attr));
	attr.host_num = total;
	for (uint32_t i = 0; i < np; i++) {
		strncpy(attr.hosts[i].host_name, p_nodes[i],
			MAX_HOST_NAME_DESC_LENGTH - 1);
		attr.hosts[i].affinity = true;
	}
	for (uint32_t i = 0; i < nu; i++) {
		strncpy(attr.hosts[np + i].host_name, u_nodes[i],
			MAX_HOST_NAME_DESC_LENGTH - 1);
		attr.hosts[np + i].affinity = true;
	}
	ubsmem_destroy_region(region);
	rc = ubsmem_create_region(region, 0, &attr);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "create_region(%s): %s(%d)",
			    region, ubs_strerror(rc), rc);
		goto fail_ubsmem;
	}

	/* map bootstrap (user does not register) */
	rc = ub_bootstrap_init_or_map(region);
	if (rc < 0) {
		UB_RING_LOG(ERR, "bootstrap init/map failed");
		goto fail_region;
	}

	/* wait for all providers to register → bootstrap READY */
	UB_RING_LOG(INFO, "waiting for bootstrap READY...");
	if (wait_bootstrap_ready() < 0)
		goto fail_region;
	UB_RING_LOG(INFO, "bootstrap READY, discovering providers");

	/* discover + map all provider shms + init data rings */
	struct bootstrap *bs = ub_bootstrap_get();
	uint32_t nb_providers = 0;
	uint32_t provider_indices[UB_RING_MAX_NODES];
	uint32_t provider_nb_queues[UB_RING_MAX_NODES];

	ubsmem_shmem_read_lock(BOOTSTRAP_SHM_NAME);
	uint32_t nb_shms = bs->hdr.nb_shms;
	ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);

	for (uint32_t s = 0; s < nb_shms; s++) {
		ubsmem_shmem_read_lock(BOOTSTRAP_SHM_NAME);
		int rdy = (bs->shms[s].state == SHM_READY);
		char owner[64], name[64];
		uint64_t sz;
		if (rdy) {
			strncpy(owner, bs->shms[s].owner_name, 63);
			strncpy(name, bs->shms[s].name, 63);
			sz = bs->shms[s].size;
		}
		ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);
		if (!rdy || strcmp(owner, my_hostname) == 0)
			continue;

		uint32_t pi = UINT32_MAX;
		for (uint32_t j = 0; j < total; j++) {
			const char *n = j < np ? p_nodes[j] : u_nodes[j - np];
			if (strcmp(n, owner) == 0) { pi = j; break; }
		}
		if (pi >= UB_RING_MAX_NODES)
			continue;

		void *peer_shm = NULL;
		rc = ubsmem_shmem_map(NULL, sz, PROT_READ | PROT_WRITE,
				      MAP_SHARED, name, 0, &peer_shm);
		if (rc != UBSM_OK) {
			UB_RING_LOG(ERR, "shmem_map(%s): %s(%d)",
				    name, ubs_strerror(rc), rc);
			goto fail_region;
		}
		h->peers[pi].shm_base = peer_shm;

		struct ub_ring_metadata *peer_meta =
			(struct ub_ring_metadata *)peer_shm;
		if (wait_meta_ready(peer_meta) < 0) {
			UB_RING_LOG(ERR, "timeout meta_ready on '%s'", name);
			goto fail_region;
		}
		h->peers[pi].data_elt_size = data_elt_size;

		uint64_t dr_off = peer_meta->ring_area_offset +
				  peer_meta->ring_area_size;
		uint64_t dr_sz  = peer_meta->total_size - dr_off;
		uint32_t max_idx = (uint32_t)((dr_sz - sizeof(struct rte_ring)) /
					      (8 + data_elt_size));
		max_idx = rte_align32prevpow2(max_idx);
		while (rte_ring_get_memsize(max_idx) +
		       (size_t)max_idx * data_elt_size > dr_sz)
			max_idx /= 2;
		if (max_idx < 2) {
			UB_RING_LOG(ERR, "data ring area too small "
				    "for elt_size=%u", data_elt_size);
			goto fail_region;
		}

		size_t idx_ring_memsize = rte_ring_get_memsize(max_idx);
		void *dr_base = (char *)peer_shm + dr_off;
		struct rte_ring *idx_ring = (struct rte_ring *)dr_base;
		char rname[64];
		snprintf(rname, sizeof(rname), "data_ring_%s", name);

		rc = rte_ring_init(idx_ring, rname, max_idx, RING_F_SC_DEQ);
		if (rc < 0) {
			UB_RING_LOG(ERR, "rte_ring_init data_ring failed: %d",
				    rc);
			goto fail_region;
		}

		void *buf_area = (char *)dr_base + idx_ring_memsize;

		for (uint32_t idx = 0; idx < max_idx; idx++) {
			void *elem = (void *)(uintptr_t)(idx * data_elt_size);
			rte_ring_enqueue_burst(idx_ring, &elem, 1, NULL);
		}

		h->peers[pi].data_ring     = idx_ring;
		h->peers[pi].data_buf_area = buf_area;
		h->peers[pi].data_buf_end  = (char *)buf_area +
			(size_t)max_idx * data_elt_size;

		peer_meta->data_elt_size = data_elt_size;
		peer_meta->data_nb_bufs  = max_idx;
		peer_meta->state |= UB_RING_STATE_DATA_RING_READY;

		provider_indices[nb_providers]  = pi;
		provider_nb_queues[nb_providers] = peer_meta->nb_queues;
		nb_providers++;

		UB_RING_LOG(INFO, "data ring '%s': nb_bufs=%u, elt_size=%u, "
			    "idx_ring=%zu bytes",
			    name, max_idx, data_elt_size, idx_ring_memsize);
	}

	h->peers[my_index].data_ring = NULL;
	h->peers[my_index].data_buf_area = NULL;

	/* collect ring pointers */
	uint32_t total_q = 0;
	for (uint32_t p = 0; p < nb_providers; p++)
		total_q += provider_nb_queues[p];
	h->total_queues = total_q;
	h->rx_rings = rte_malloc(NULL, total_q * sizeof(struct rte_ring *), 0);
	h->tx_rings = rte_malloc(NULL, total_q * sizeof(struct rte_ring *), 0);
	if (!h->rx_rings || !h->tx_rings) {
		UB_RING_LOG(ERR, "ring pointer array alloc failed");
		goto fail_region;
	}

	uint32_t idx = 0;
	for (uint32_t p = 0; p < nb_providers; p++) {
		uint32_t pi = provider_indices[p];
		uint32_t nq = provider_nb_queues[p];
		h->provider_base_qid[pi] = idx;
		struct ub_ring_metadata *m = peer_meta(h, pi);
		void *sh = h->peers[pi].shm_base;
		for (uint32_t q = 0; q < nq; q++) {
			/* crossed: provider rx → user tx, provider tx → user rx */
			h->tx_rings[idx] = (struct rte_ring *)((char *)sh +
				m->queue_offsets[q]);
			h->rx_rings[idx] = (struct rte_ring *)((char *)sh +
				m->queue_offsets[nq + q]);
			h->queue_to_provider[idx] = pi;
			idx++;
		}
	}

	UB_RING_LOG(INFO, "user init complete: total_queues=%u, "
		    "nb_providers=%u", total_q, nb_providers);
	return h;

fail_region:
	ubsmem_destroy_region(region);
fail_ubsmem:
	ubsmem_finalize();
fail:
	rte_free(h);
	return NULL;
}

/* ================================================================
 *  Buffer Alloc / Free
 * ================================================================ */

void *
rte_ub_ring_alloc_buf(struct rte_ub_ring_handle *h,
		      uint32_t provider_node_index)
{
	if (!h || provider_node_index >= UB_RING_MAX_NODES)
		return NULL;

	sync_peer_data_ring(h, provider_node_index);
	struct rte_ring *dr = h->peers[provider_node_index].data_ring;
	if (!dr)
		return NULL;

	void *elem;
	if (rte_ring_sc_dequeue_bulk(dr, &elem, 1, NULL) == 0)
		return NULL;

	return (char *)h->peers[provider_node_index].data_buf_area +
	       (size_t)(uintptr_t)elem;
}

void
rte_ub_ring_free_buf(struct rte_ub_ring_handle *h, void *buf)
{
	if (!h || !buf)
		return;

	int ni = find_peer_by_buf(h, buf);
	if (ni < 0)
		return;
	struct rte_ring *dr = h->peers[ni].data_ring;
	if (!dr)
		return;

	void *elem = (void *)(uintptr_t)((char *)buf -
		(char *)h->peers[ni].data_buf_area);

	rte_ring_mp_enqueue_bulk(dr, &elem, 1, NULL);
}

uint32_t
rte_ub_ring_get_data_elt_size(struct rte_ub_ring_handle *h,
			      uint32_t provider_node_index)
{
	if (!h || provider_node_index >= UB_RING_MAX_NODES)
		return 0;
	sync_peer_data_ring(h, provider_node_index);
	return h->peers[provider_node_index].data_elt_size;
}

void *
rte_ub_ring_get_data_buf_area(struct rte_ub_ring_handle *h,
			      uint32_t provider_node_index)
{
	if (!h || provider_node_index >= UB_RING_MAX_NODES)
		return NULL;
	return h->peers[provider_node_index].data_buf_area;
}

/* ================================================================
 *  Send / Recv Burst
 * ================================================================ */

uint16_t
rte_ub_ring_send_burst(struct rte_ub_ring_handle *h,
		       uint32_t queue_id, void **bufs, uint16_t nb)
{
	if (!h || queue_id >= h->total_queues || !bufs || nb == 0)
		return 0;

	void *elems[nb];
	uint16_t count = 0;

	for (uint16_t i = 0; i < nb; i++) {
		if (!bufs[i])
			continue;
		int ni = find_peer_by_buf(h, bufs[i]);
		if (ni < 0)
			continue;
		uint64_t off = (uint64_t)((char *)bufs[i] -
			(char *)h->peers[ni].data_buf_area);
		elems[count++] = ub_ring_elem_pack((uint8_t)ni, off);
	}

	return (uint16_t)rte_ring_enqueue_burst(h->tx_rings[queue_id],
						 elems, count, NULL);
}

uint16_t
rte_ub_ring_recv_burst(struct rte_ub_ring_handle *h,
		       uint32_t queue_id, void **bufs, uint16_t nb)
{
	if (!h || queue_id >= h->total_queues || !bufs || nb == 0)
		return 0;

	void *elems[nb];
	uint16_t n = (uint16_t)rte_ring_dequeue_burst(
		h->rx_rings[queue_id], elems, nb, NULL);

	for (uint16_t i = 0; i < n; i++) {
		uint8_t  ni  = ub_ring_elem_get_node(elems[i]);
		uint64_t off = ub_ring_elem_get_offset(elems[i]);
		sync_peer_data_ring(h, ni);
		bufs[i] = (char *)h->peers[ni].data_buf_area + off;
	}

	return n;
}

/* ================================================================
 *  Queue Mapping
 * ================================================================ */

int
rte_ub_ring_get_queue_mapping(struct rte_ub_ring_handle *h,
			      uint32_t provider_node_index,
			      uint32_t sub_queue,
			      uint32_t *tx_queue_id, uint32_t *rx_queue_id)
{
	if (!h || !tx_queue_id || !rx_queue_id)
		return -EINVAL;
	struct ub_ring_metadata *m = peer_meta(h, provider_node_index);
	if (!m || sub_queue >= m->nb_queues)
		return -EINVAL;

	uint32_t qid = h->provider_base_qid[provider_node_index] + sub_queue;
	*tx_queue_id = qid;
	*rx_queue_id = qid;
	return 0;
}

uint32_t
rte_ub_ring_queue_to_provider(struct rte_ub_ring_handle *h,
			      uint32_t queue_id)
{
	if (!h || queue_id >= h->total_queues)
		return 0;
	return h->queue_to_provider[queue_id];
}

uint32_t
rte_ub_ring_get_total_queues(struct rte_ub_ring_handle *h)
{
	return h ? h->total_queues : 0;
}

int
rte_ub_ring_get_node_index(struct rte_ub_ring_handle *h, uint32_t *idx)
{
	if (!h || !idx)
		return -EINVAL;
	*idx = h->my_node_index;
	return 0;
}

int
rte_ub_ring_get_peer_index(struct rte_ub_ring_handle *h,
			   const char *hostname, uint32_t *idx)
{
	if (!h || !hostname || !idx)
		return -EINVAL;
	for (uint32_t i = 0; i < h->nb_nodes; i++) {
		if (!peer_meta(h, i))
			continue;
		if (strcmp(peer_meta(h, i)->node_name, hostname) == 0) {
			*idx = i;
			return 0;
		}
	}
	return -ENOENT;
}

/* ================================================================
 *  Fini
 * ================================================================ */

void
rte_ub_ring_fini(struct rte_ub_ring_handle *h)
{
	if (!h)
		return;

	UB_RING_LOG(INFO, "cleaning up...");

	for (uint32_t i = 0; i < UB_RING_MAX_NODES; i++) {
		if (i == h->my_node_index)
			continue;
		if (h->peers[i].shm_base) {
			size_t sz = peer_meta(h, i) ?
				peer_meta(h, i)->total_size : 0;
			ubsmem_shmem_unmap(h->peers[i].shm_base, sz);
			h->peers[i].shm_base = NULL;
		}
	}

	if (h->role == UB_RING_ROLE_PROVIDER &&
	    h->peers[h->my_node_index].shm_base) {
		ubsmem_shmem_unmap(h->peers[h->my_node_index].shm_base,
				   peer_meta(h, h->my_node_index)->total_size);
		h->peers[h->my_node_index].shm_base = NULL;

		char shm_name[128];
		char my_hostname[MAX_HOST_NAME_DESC_LENGTH];
		if (gethostname(my_hostname, sizeof(my_hostname)) == 0) {
			snprintf(shm_name, sizeof(shm_name), "%s_%s",
				 h->shm_prefix, my_hostname);
			ubsmem_shmem_deallocate(shm_name);
		}
	}

	struct bootstrap *bs = ub_bootstrap_get();
	if (bs)
		ubsmem_shmem_unmap(bs, sizeof(*bs));

	ubsmem_destroy_region(h->region_name);
	ubsmem_finalize();

	rte_free(h->rx_rings);
	rte_free(h->tx_rings);
	rte_free(h);
	UB_RING_LOG(INFO, "cleanup complete");
}
