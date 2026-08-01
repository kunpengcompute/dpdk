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

/* ---- global context ---- */

struct ub_ring_ctx {
	uint32_t nb_nodes;
	uint32_t my_node_index;
	enum ub_ring_role role;

	struct {
		void            *shm_base;    /* → (struct ub_ring_metadata *) */
		struct rte_ring *data_ring;
		void            *data_buf_area;
		void            *data_buf_end;
		uint32_t         data_elt_size;
	} peers[UB_RING_MAX_NODES];

	struct rte_ring **rx_rings;
	struct rte_ring **tx_rings;
	uint32_t total_queues;

	/* precomputed queue mappings (O(1) lookup) */
	uint32_t queue_to_provider[UB_RING_MAX_QUEUES];  /* global qid → node_index */
	uint32_t provider_base_qid[UB_RING_MAX_NODES];    /* node_index → first global qid */

	char     region_name[64];
	char     shm_prefix[64];
};

static struct ub_ring_ctx *g_ctx;

static inline struct ub_ring_metadata *
peer_meta(uint32_t i)
{
	return (struct ub_ring_metadata *)g_ctx->peers[i].shm_base;
}

/* ---- forward declarations ---- */

static int cmp_node(const void *a, const void *b);
static int find_peer_by_buf(const void *buf);

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

/*
 * Sync data ring fields from metadata (for provider: user wrote them to shm).
 */
static void
sync_peer_data_ring(uint32_t i)
{
	if (g_ctx->peers[i].data_buf_area)
		return; /* already synced */

	struct ub_ring_metadata *meta = peer_meta(i);
	if (!meta || !(meta->state & UB_RING_STATE_DATA_RING_READY))
		return;

	g_ctx->peers[i].data_elt_size = meta->data_elt_size;
	g_ctx->peers[i].data_ring = (struct rte_ring *)(
		(char *)g_ctx->peers[i].shm_base +
		meta->ring_area_offset + meta->ring_area_size);

	size_t idx_ring_sz = rte_ring_get_memsize(meta->data_nb_bufs);
	g_ctx->peers[i].data_buf_area =
		(char *)g_ctx->peers[i].data_ring + idx_ring_sz;
	g_ctx->peers[i].data_buf_end =
		(char *)g_ctx->peers[i].data_buf_area +
		(size_t)meta->data_nb_bufs * meta->data_elt_size;
}

static int
find_peer_by_buf(const void *buf)
{
	for (uint32_t i = 0; i < g_ctx->nb_nodes; i++) {
		sync_peer_data_ring(i);
		if (!g_ctx->peers[i].data_buf_area || !g_ctx->peers[i].data_buf_end)
			continue;
		if ((const char *)buf >= (const char *)g_ctx->peers[i].data_buf_area &&
		    (const char *)buf < (const char *)g_ctx->peers[i].data_buf_end)
			return (uint8_t)i;
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
	meta->state       = 0;               /* neither meta nor data ring ready yet */
	meta->data_elt_size    = 0;          /* user fills this */
	meta->data_nb_bufs     = 0;          /* user fills this */
	meta->total_size       = total_size;
	meta->ring_area_offset = ring_area_off;
	meta->ring_area_size   = ring_area_sz;

	/* store self hostname for peer validation */
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

/* ================================================================
 *  Provider Init
 * ================================================================ */

int
rte_ub_ring_provider_init(const char **ub_nodes, uint32_t nb_nodes,
			  const char *region, const char *shm_prefix,
			  size_t shm_size, uint32_t nb_queues,
			  uint32_t queue_depth)
{
	char my_hostname[MAX_HOST_NAME_DESC_LENGTH];
	const char **sorted;
	ubsmem_options_t opts;
	ubsmem_region_attributes_t attr;
	uint32_t my_index = UINT32_MAX;
	char shm_name[128];
	void *own_shm = NULL;
	size_t meta_size, ring_memsize, ring_area_sz;
	int rc;

	if (!ub_nodes || nb_nodes == 0 || nb_nodes > UB_RING_MAX_NODES ||
	    !region || !shm_prefix || shm_size == 0 ||
	    nb_queues == 0 || nb_queues > UB_RING_MAX_QUEUES ||
	    queue_depth == 0 || !rte_is_power_of_2(queue_depth)) {
		rte_errno = EINVAL;
		return -EINVAL;
	}

	g_ctx = rte_zmalloc(NULL, sizeof(*g_ctx), 0);
	if (!g_ctx)
		return -ENOMEM;
	g_ctx->role = UB_RING_ROLE_PROVIDER;
	snprintf(g_ctx->region_name, sizeof(g_ctx->region_name), "%s", region);
	snprintf(g_ctx->shm_prefix, sizeof(g_ctx->shm_prefix), "%s", shm_prefix);

	/* step 1: sort nodes + find self */
	sorted = rte_malloc(NULL, nb_nodes * sizeof(char *), 0);
	if (!sorted)
		goto fail;
	for (uint32_t i = 0; i < nb_nodes; i++) {
		if (!ub_nodes[i]) {
			UB_RING_LOG(ERR, "ub_nodes[%u] is NULL", i);
			rte_free(sorted);
			goto fail;
		}
		sorted[i] = ub_nodes[i];
	}
	qsort(sorted, nb_nodes, sizeof(char *), cmp_node);

	if (gethostname(my_hostname, sizeof(my_hostname)) != 0) {
		UB_RING_LOG(ERR, "gethostname failed");
		rte_free(sorted);
		goto fail;
	}
	for (uint32_t i = 0; i < nb_nodes; i++) {
		if (strcmp(sorted[i], my_hostname) == 0) {
			my_index = i;
			break;
		}
	}
	if (my_index == UINT32_MAX) {
		UB_RING_LOG(ERR, "hostname '%s' not in ub_nodes list", my_hostname);
		rte_free(sorted);
		goto fail;
	}

	g_ctx->nb_nodes     = nb_nodes;
	g_ctx->my_node_index = my_index;

	UB_RING_LOG(INFO, "hostname='%s' → sorted index=%u (nb_nodes=%u)",
		    my_hostname, my_index, nb_nodes);

	UB_RING_LOG(INFO, "provider init: my_index=%u, nb_nodes=%u, "
		    "nb_queues=%u, queue_depth=%u",
		    my_index, nb_nodes, nb_queues, queue_depth);

	/* step 2: ubsmem init */
	rc = ubsmem_init_attributes(&opts);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "ubsmem_init_attributes: %s(%d)", ubs_strerror(rc), rc);
		rte_free(sorted);
		goto fail;
	}
	rc = ubsmem_initialize(&opts);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "ubsmem_initialize: %s(%d)", ubs_strerror(rc), rc);
		rte_free(sorted);
		goto fail_ubsmem;
	}

	/* step 3: create region */
	memset(&attr, 0, sizeof(attr));
	attr.host_num = nb_nodes;
	for (uint32_t i = 0; i < nb_nodes; i++) {
		strncpy(attr.hosts[i].host_name, sorted[i],
			MAX_HOST_NAME_DESC_LENGTH - 1);
		attr.hosts[i].affinity = true;
	}
	ubsmem_destroy_region(region);
	rc = ubsmem_create_region(region, 0, &attr);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "create_region(%s): %s(%d)", region, ubs_strerror(rc), rc);
		rte_free(sorted);
		goto fail_ubsmem;
	}

	/* step 4: create own shm */
	snprintf(shm_name, sizeof(shm_name), "%s_%s", shm_prefix, my_hostname);
	ubsmem_shmem_deallocate(shm_name);
	uint64_t flags = UBSM_FLAG_NONCACHE | UBSM_FLAG_WR_DELAY_COMP;
	rc = ubsmem_shmem_allocate(region, shm_name, shm_size, 0600, flags);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "shmem_allocate(%s): %s(%d)", shm_name, ubs_strerror(rc), rc);
		rte_free(sorted);
		goto fail_region;
	}
	rc = ubsmem_shmem_map(NULL, shm_size, PROT_READ | PROT_WRITE,
			      MAP_SHARED, shm_name, 0, &own_shm);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "shmem_map(%s): %s(%d)", shm_name, ubs_strerror(rc), rc);
		rte_free(sorted);
		goto fail_region;
	}
	memset(own_shm, 0, shm_size);
	g_ctx->peers[my_index].shm_base = own_shm;

	/* step 5: init tx/rx rings */
	meta_size    = calc_metadata_size(nb_queues);
	ring_memsize = calc_ring_memsize(queue_depth);
	ring_area_sz = calc_ring_area_size(nb_queues, ring_memsize);
	UB_RING_LOG(INFO, "layout: meta=%zu ring_area=%zu data_ring at 0x%zx size=0x%zx",
		    meta_size, ring_area_sz,
		    meta_size + ring_area_sz, shm_size - meta_size - ring_area_sz);

	for (uint32_t q = 0; q < nb_queues; q++) {
		char rname[32];
		void *rx_addr = (char *)own_shm + meta_size + q * ring_memsize;
		void *tx_addr = (char *)own_shm + meta_size +
				(nb_queues + q) * ring_memsize;

		snprintf(rname, sizeof(rname), "rx_q%u", q);
		rc = rte_ring_init((struct rte_ring *)rx_addr, rname,
				   queue_depth, RING_F_SP_ENQ | RING_F_SC_DEQ);
		if (rc < 0) {
			UB_RING_LOG(ERR, "rte_ring_init rx_q%u failed: %d", q, rc);
			rte_free(sorted);
			goto fail_region;
		}
		snprintf(rname, sizeof(rname), "tx_q%u", q);
		rc = rte_ring_init((struct rte_ring *)tx_addr, rname,
				   queue_depth, RING_F_SP_ENQ | RING_F_SC_DEQ);
		if (rc < 0) {
			UB_RING_LOG(ERR, "rte_ring_init tx_q%u failed: %d", q, rc);
			rte_free(sorted);
			goto fail_region;
		}
	}

	/* step 6: write metadata */
	struct ub_ring_metadata *own_meta = (struct ub_ring_metadata *)own_shm;
	write_metadata_provider(own_meta, nb_nodes, nb_queues, my_index,
		queue_depth, shm_size, meta_size, ring_area_sz, my_hostname);
	for (uint32_t q = 0; q < nb_queues; q++) {
		own_meta->queue_offsets[q] = meta_size + q * ring_memsize;
		own_meta->queue_offsets[nb_queues + q] =
			meta_size + (nb_queues + q) * ring_memsize;
	}
	own_meta->state |= UB_RING_STATE_META_READY;
	/* data ring is not initialized yet — user will do it */
	g_ctx->peers[my_index].data_ring = NULL;
	g_ctx->peers[my_index].data_buf_area = NULL;

	/* step 7: discover + map other provider shms */
	for (uint32_t i = 0; i < nb_nodes; i++) {
		if (i == my_index)
			continue;

		snprintf(shm_name, sizeof(shm_name), "%s_%s", shm_prefix, sorted[i]);

		ubsmem_shmem_desc_t list[32];
		uint32_t cnt = 32;
		void *peer_shm = NULL;
		size_t peer_size = 0;
		int found = 0;

		for (int timeout = 0; timeout < UB_RING_POLL_TIMEOUT_S * 20; timeout++) {
			rc = ubsmem_shmem_list_lookup(shm_prefix, list, &cnt);
			if (rc == UBSM_OK) {
				for (uint32_t k = 0; k < cnt; k++) {
					if (strcmp(list[k].name, shm_name) == 0) {
						peer_size = list[k].size;
						found = 1;
						break;
					}
				}
			}
			if (found) break;
			cnt = 32;
			usleep(UB_RING_POLL_INTERVAL_US);
		}
		if (!found) {
			UB_RING_LOG(ERR, "timeout waiting for peer shm '%s'", shm_name);
			goto fail_region;
		}
		rc = ubsmem_shmem_map(NULL, peer_size, PROT_READ | PROT_WRITE,
				      MAP_SHARED, shm_name, 0, &peer_shm);
		if (rc != UBSM_OK) {
			UB_RING_LOG(ERR, "shmem_map(%s): %s(%d)", shm_name, ubs_strerror(rc), rc);
			goto fail_region;
		}
		g_ctx->peers[i].shm_base = peer_shm;

		struct ub_ring_metadata *peer_meta = (struct ub_ring_metadata *)peer_shm;
		if (wait_meta_ready(peer_meta) < 0) {
			UB_RING_LOG(ERR, "timeout waiting for meta_ready on '%s'", shm_name);
			goto fail_region;
		}
		g_ctx->peers[i].data_ring = NULL;
		g_ctx->peers[i].data_buf_area = NULL;
	}

	rte_free(sorted);

	/* step 8: set up local ring pointers */
	g_ctx->total_queues = nb_queues;
	g_ctx->rx_rings = rte_malloc(NULL, nb_queues * sizeof(struct rte_ring *), 0);
	g_ctx->tx_rings = rte_malloc(NULL, nb_queues * sizeof(struct rte_ring *), 0);
	if (!g_ctx->rx_rings || !g_ctx->tx_rings)
		goto fail_region;
	for (uint32_t q = 0; q < nb_queues; q++) {
		g_ctx->rx_rings[q] = (struct rte_ring *)((char *)own_shm +
			own_meta->queue_offsets[q]);
		g_ctx->queue_to_provider[q] = my_index;
		g_ctx->tx_rings[q] = (struct rte_ring *)((char *)own_shm +
			own_meta->queue_offsets[nb_queues + q]);
		g_ctx->queue_to_provider[q] = my_index;
	}
	g_ctx->provider_base_qid[my_index] = 0;

	UB_RING_LOG(INFO, "provider init complete");
	return 0;

fail_region:
	ubsmem_destroy_region(region);
fail_ubsmem:
	ubsmem_finalize();
fail:
	rte_free(g_ctx);
	g_ctx = NULL;
	return -1;
}

/* ================================================================
 *  User Init
 * ================================================================ */

int
rte_ub_ring_user_init(const char **ub_nodes, uint32_t nb_nodes,
		      const char *region, const char *shm_prefix,
		      uint32_t data_elt_size)
{
	char my_hostname[MAX_HOST_NAME_DESC_LENGTH];
	const char **sorted;
	ubsmem_options_t opts;
	ubsmem_region_attributes_t attr;
	uint32_t my_index = UINT32_MAX;
	char shm_name[128];
	int rc;

	if (!ub_nodes || nb_nodes == 0 || nb_nodes > UB_RING_MAX_NODES ||
	    !region || !shm_prefix || data_elt_size == 0) {
		rte_errno = EINVAL;
		return -EINVAL;
	}

	g_ctx = rte_zmalloc(NULL, sizeof(*g_ctx), 0);
	if (!g_ctx)
		return -ENOMEM;
	g_ctx->role = UB_RING_ROLE_USER;
	snprintf(g_ctx->region_name, sizeof(g_ctx->region_name), "%s", region);
	snprintf(g_ctx->shm_prefix, sizeof(g_ctx->shm_prefix), "%s", shm_prefix);

	/* step 1: sort nodes + find self */
	sorted = rte_malloc(NULL, nb_nodes * sizeof(char *), 0);
	if (!sorted)
		goto fail;
	for (uint32_t i = 0; i < nb_nodes; i++) {
		if (!ub_nodes[i]) {
			UB_RING_LOG(ERR, "ub_nodes[%u] is NULL", i);
			rte_free(sorted);
			goto fail;
		}
		sorted[i] = ub_nodes[i];
	}
	qsort(sorted, nb_nodes, sizeof(char *), cmp_node);

	if (gethostname(my_hostname, sizeof(my_hostname)) != 0) {
		UB_RING_LOG(ERR, "gethostname failed");
		rte_free(sorted);
		goto fail;
	}
	for (uint32_t i = 0; i < nb_nodes; i++) {
		if (strcmp(sorted[i], my_hostname) == 0) {
			my_index = i;
			break;
		}
	}
	if (my_index == UINT32_MAX) {
		UB_RING_LOG(ERR, "hostname '%s' not in ub_nodes list", my_hostname);
		rte_free(sorted);
		goto fail;
	}
	g_ctx->nb_nodes     = nb_nodes;
	g_ctx->my_node_index = my_index;

	UB_RING_LOG(INFO, "hostname='%s' → sorted index=%u (nb_nodes=%u)",
		    my_hostname, my_index, nb_nodes);

	UB_RING_LOG(INFO, "user init: my_index=%u, nb_nodes=%u, data_elt_size=%u",
		    my_index, nb_nodes, data_elt_size);

	/* step 2: ubsmem init */
	rc = ubsmem_init_attributes(&opts);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "ubsmem_init_attributes: %s(%d)", ubs_strerror(rc), rc);
		goto fail;
	}
	rc = ubsmem_initialize(&opts);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "ubsmem_initialize: %s(%d)", ubs_strerror(rc), rc);
		goto fail_ubsmem;
	}

	/* step 3: create region */
	memset(&attr, 0, sizeof(attr));
	attr.host_num = nb_nodes;
	for (uint32_t i = 0; i < nb_nodes; i++) {
		strncpy(attr.hosts[i].host_name, sorted[i],
			MAX_HOST_NAME_DESC_LENGTH - 1);
		attr.hosts[i].affinity = true;
	}
	ubsmem_destroy_region(region);
	rc = ubsmem_create_region(region, 0, &attr);
	if (rc != UBSM_OK) {
		UB_RING_LOG(ERR, "create_region(%s): %s(%d)", region, ubs_strerror(rc), rc);
		goto fail_ubsmem;
	}

	/* step 4: discover + map all provider shms */
	uint32_t nb_providers = 0;
	uint32_t provider_indices[UB_RING_MAX_NODES];
	uint32_t provider_nb_queues[UB_RING_MAX_NODES];

	for (uint32_t i = 0; i < nb_nodes; i++) {
		if (i == my_index)
			continue;

		snprintf(shm_name, sizeof(shm_name), "%s_%s", shm_prefix, sorted[i]);

		ubsmem_shmem_desc_t list[32];
		uint32_t cnt = 32;
		void *peer_shm = NULL;
		size_t peer_size = 0;
		int found = 0;

		for (int timeout = 0; timeout < UB_RING_POLL_TIMEOUT_S * 20; timeout++) {
			rc = ubsmem_shmem_list_lookup(shm_prefix, list, &cnt);
			if (rc == UBSM_OK) {
				for (uint32_t k = 0; k < cnt; k++) {
					if (strcmp(list[k].name, shm_name) == 0) {
						peer_size = list[k].size;
						found = 1;
						break;
					}
				}
			}
			if (found) break;
			cnt = 32;
			usleep(UB_RING_POLL_INTERVAL_US);
		}
		if (!found) {
			UB_RING_LOG(ERR, "timeout waiting for provider shm '%s'", shm_name);
			goto fail_region;
		}
		rc = ubsmem_shmem_map(NULL, peer_size, PROT_READ | PROT_WRITE,
				      MAP_SHARED, shm_name, 0, &peer_shm);
		if (rc != UBSM_OK) {
			UB_RING_LOG(ERR, "shmem_map(%s): %s(%d)", shm_name, ubs_strerror(rc), rc);
			goto fail_region;
		}
		g_ctx->peers[i].shm_base = peer_shm;

		struct ub_ring_metadata *peer_meta = (struct ub_ring_metadata *)peer_shm;
		if (wait_meta_ready(peer_meta) < 0) {
			UB_RING_LOG(ERR, "timeout waiting for meta_ready on '%s'", shm_name);
			goto fail_region;
		}
		g_ctx->peers[i].data_elt_size = data_elt_size;

		/* step 5: init data ring on provider shm */
		/* data ring is right after tx/rx ring area */
		uint64_t dr_off = peer_meta->ring_area_offset +
				  peer_meta->ring_area_size;
		uint64_t dr_sz  = peer_meta->total_size - dr_off;
		/* rte_ring_get_memsize(N) ≈ sizeof(rte_ring) + N*8 (aligned).
		 * Estimate N algebraically, then adjust down until it fits. */
		uint32_t max_idx = (uint32_t)((dr_sz - sizeof(struct rte_ring)) /
					      (8 + data_elt_size));
		/* round down to power of 2 (ring requirement) */
		max_idx = rte_align32prevpow2(max_idx);
		while (rte_ring_get_memsize(max_idx) +
		       (size_t)max_idx * data_elt_size > dr_sz)
			max_idx /= 2;
		size_t idx_ring_memsize = rte_ring_get_memsize(max_idx);

		if (max_idx < 2) {
			UB_RING_LOG(ERR, "data ring area too small for elt_size=%u",
				    data_elt_size);
			goto fail_region;
		}

		void *dr_base = (char *)peer_shm + dr_off;
		struct rte_ring *idx_ring = (struct rte_ring *)dr_base;
		char rname[64];
		snprintf(rname, sizeof(rname), "data_ring_%s", sorted[i]);

		/* MPSC: user dequeues (SC), providers enqueue (MP) */
		rc = rte_ring_init(idx_ring, rname, max_idx,
				   RING_F_SC_DEQ);
		if (rc < 0) {
			UB_RING_LOG(ERR, "rte_ring_init data_ring failed: %d", rc);
			goto fail_region;
		}

		void *buf_area = (char *)dr_base + idx_ring_memsize;

		/* populate: enqueue all free buffer indices */
		for (uint32_t idx = 0; idx < max_idx; idx++) {
			void *elem = (void *)(uintptr_t)(idx * data_elt_size);
			rte_ring_enqueue_burst(idx_ring, &elem, 1, NULL);
		}

		g_ctx->peers[i].data_ring     = idx_ring;
		g_ctx->peers[i].data_buf_area = buf_area;
		g_ctx->peers[i].data_buf_end  = (char *)buf_area +
			(size_t)max_idx * data_elt_size;

		/* write data ring info back to metadata */
		peer_meta->data_elt_size = data_elt_size;
		peer_meta->data_nb_bufs  = max_idx;
		peer_meta->state |= UB_RING_STATE_DATA_RING_READY;

		provider_indices[nb_providers] = i;
		provider_nb_queues[nb_providers] = peer_meta->nb_queues;
		nb_providers++;

		UB_RING_LOG(INFO, "data ring '%s': nb_bufs=%u, elt_size=%u, "
			    "idx_ring=%zu bytes",
			    sorted[i], max_idx, data_elt_size, idx_ring_memsize);
	}

	/* set own peer (no shm) */
	g_ctx->peers[my_index].data_ring = NULL;
	g_ctx->peers[my_index].data_buf_area = NULL;

	/* step 6: collect all provider ring pointers */
	uint32_t total_q = 0;
	for (uint32_t p = 0; p < nb_providers; p++)
		total_q += provider_nb_queues[p];
	g_ctx->total_queues = total_q;
	g_ctx->rx_rings = rte_malloc(NULL, total_q * sizeof(struct rte_ring *), 0);
	g_ctx->tx_rings = rte_malloc(NULL, total_q * sizeof(struct rte_ring *), 0);
	if (!g_ctx->rx_rings || !g_ctx->tx_rings)
		goto fail_region;

	uint32_t idx = 0;
	for (uint32_t p = 0; p < nb_providers; p++) {
		uint32_t pi = provider_indices[p];
		uint32_t nq = provider_nb_queues[p];
		g_ctx->provider_base_qid[pi] = idx;
		struct ub_ring_metadata *meta = peer_meta(pi);
		void *shm = g_ctx->peers[pi].shm_base;
		for (uint32_t q = 0; q < nq; q++) {
			/* crossed: provider rx → user tx, provider tx → user rx */
			g_ctx->tx_rings[idx] = (struct rte_ring *)((char *)shm +
				meta->queue_offsets[q]);
			g_ctx->rx_rings[idx] = (struct rte_ring *)((char *)shm +
				meta->queue_offsets[nq + q]);
			g_ctx->queue_to_provider[idx] = pi;
			idx++;
		}
	}

	rte_free(sorted);

	UB_RING_LOG(INFO, "user init complete: total_queues=%u, nb_providers=%u",
		    total_q, nb_providers);
	return 0;

fail_region:
	ubsmem_destroy_region(region);
fail_ubsmem:
	ubsmem_finalize();
fail:
	rte_free(g_ctx);
	g_ctx = NULL;
	return -1;
}

/* ================================================================
 *  Buffer Alloc / Free
 * ================================================================ */

void *
rte_ub_ring_alloc_buf(uint32_t provider_node_index)
{
	if (!g_ctx || provider_node_index >= g_ctx->nb_nodes)
		return NULL;

	sync_peer_data_ring(provider_node_index);
	struct rte_ring *dr = g_ctx->peers[provider_node_index].data_ring;
	if (!dr)
		return NULL;

	void *elem;
	if (rte_ring_sc_dequeue_bulk(dr, &elem, 1, NULL) == 0)
		return NULL;

	return (char *)g_ctx->peers[provider_node_index].data_buf_area +
	       (size_t)(uintptr_t)elem;
}

void
rte_ub_ring_free_buf(void *buf)
{
	if (!g_ctx || !buf)
		return;

	int ni = find_peer_by_buf(buf);
	if (ni < 0)
		return;
	struct rte_ring *dr = g_ctx->peers[ni].data_ring;
	if (!dr)
		return;

	void *elem = (void *)(uintptr_t)((char *)buf -
		(char *)g_ctx->peers[ni].data_buf_area);

	/* MPSC: providers (multiple) enqueue, user (single) dequeues */
	rte_ring_mp_enqueue_bulk(dr, &elem, 1, NULL);
}

uint32_t
rte_ub_ring_get_data_elt_size(uint32_t provider_node_index)
{
	if (!g_ctx || provider_node_index >= g_ctx->nb_nodes)
		return 0;
	sync_peer_data_ring(provider_node_index);
	uint32_t sz = g_ctx->peers[provider_node_index].data_elt_size;
	if (sz == 0) {
		struct ub_ring_metadata *m = peer_meta(provider_node_index);
		UB_RING_LOG(ERR, "data_elt_size is 0 for peer %u: "
			    "ctx=%p meta=%p meta->data_elt=%u "
			    "ctx->buf_area=%p ctx->buf_end=%p ctx->data_ring=%p",
			    provider_node_index, (void *)g_ctx, (void *)m,
			    m ? m->data_elt_size : 0,
			    g_ctx->peers[provider_node_index].data_buf_area,
			    g_ctx->peers[provider_node_index].data_buf_end,
			    (void *)g_ctx->peers[provider_node_index].data_ring);
	}
	return sz;
}

void *
rte_ub_ring_get_data_buf_area(uint32_t provider_node_index)
{
	if (!g_ctx || provider_node_index >= g_ctx->nb_nodes)
		return NULL;
	return g_ctx->peers[provider_node_index].data_buf_area;
}

/* ================================================================
 *  Send / Recv Burst
 * ================================================================ */

uint16_t
rte_ub_ring_send_burst(uint32_t queue_id, void **bufs, uint16_t nb)
{
	if (!g_ctx || queue_id >= g_ctx->total_queues || !bufs || nb == 0)
		return 0;

	void *elems[nb];
	uint16_t count = 0;

	for (uint16_t i = 0; i < nb; i++) {
		if (!bufs[i])
			continue;
		int ni = find_peer_by_buf(bufs[i]);
		if (ni < 0)
			continue;
		uint64_t off = (uint64_t)((char *)bufs[i] -
			(char *)g_ctx->peers[ni].data_buf_area);
		elems[count++] = ub_ring_elem_pack((uint8_t)ni, off);
	}

	return (uint16_t)rte_ring_enqueue_burst(g_ctx->tx_rings[queue_id],
						 elems, count, NULL);
}

uint16_t
rte_ub_ring_recv_burst(uint32_t queue_id, void **bufs, uint16_t nb)
{
	if (!g_ctx || queue_id >= g_ctx->total_queues || !bufs || nb == 0)
		return 0;

	void *elems[nb];
	uint16_t n = (uint16_t)rte_ring_dequeue_burst(
		g_ctx->rx_rings[queue_id], elems, nb, NULL);

	for (uint16_t i = 0; i < n; i++) {
		uint8_t  ni  = ub_ring_elem_get_node(elems[i]);
		uint64_t off = ub_ring_elem_get_offset(elems[i]);
		sync_peer_data_ring(ni);
		bufs[i] = (char *)g_ctx->peers[ni].data_buf_area + off;
	}

	return n;
}

/* ================================================================
 *  Queue Mapping
 * ================================================================ */

int
rte_ub_ring_get_queue_mapping(uint32_t provider_node_index,
			       uint32_t sub_queue,
			       uint32_t *tx_queue_id,
			       uint32_t *rx_queue_id)
{
	if (!g_ctx || !tx_queue_id || !rx_queue_id)
		return -EINVAL;
	struct ub_ring_metadata *m = peer_meta(provider_node_index);
	if (!m || sub_queue >= m->nb_queues)
		return -EINVAL;

	uint32_t qid = g_ctx->provider_base_qid[provider_node_index] + sub_queue;
	*tx_queue_id = qid;
	*rx_queue_id = qid;
	return 0;
}

uint32_t
rte_ub_ring_queue_to_provider(uint32_t queue_id)
{
	if (!g_ctx || queue_id >= g_ctx->total_queues)
		return 0;
	return g_ctx->queue_to_provider[queue_id];
}

uint32_t
rte_ub_ring_get_total_queues(void)
{
	return g_ctx ? g_ctx->total_queues : 0;
}

int
rte_ub_ring_get_node_index(uint32_t *idx)
{
	if (!g_ctx || !idx)
		return -EINVAL;
	*idx = g_ctx->my_node_index;
	return 0;
}

int
rte_ub_ring_get_peer_index(const char *hostname, uint32_t *idx)
{
	if (!g_ctx || !hostname || !idx)
		return -EINVAL;
	for (uint32_t i = 0; i < g_ctx->nb_nodes; i++) {
		if (!peer_meta(i))
			continue;
		if (strcmp(peer_meta(i)->node_name, hostname) == 0) {
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
rte_ub_ring_fini(void)
{
	if (!g_ctx)
		return;

	UB_RING_LOG(INFO, "cleaning up...");

	/* unmap peer shms */
	for (uint32_t i = 0; i < g_ctx->nb_nodes; i++) {
		if (i == g_ctx->my_node_index)
			continue;
		if (g_ctx->peers[i].shm_base) {
			size_t sz = peer_meta(i) ?
				peer_meta(i)->total_size : 0;
			ubsmem_shmem_unmap(g_ctx->peers[i].shm_base, sz);
			g_ctx->peers[i].shm_base = NULL;
		}
	}

	/* unmap + deallocate own shm (provider only) */
	if (g_ctx->role == UB_RING_ROLE_PROVIDER &&
	    g_ctx->peers[g_ctx->my_node_index].shm_base) {
		ubsmem_shmem_unmap(g_ctx->peers[g_ctx->my_node_index].shm_base,
				   peer_meta(g_ctx->my_node_index)->total_size);
		g_ctx->peers[g_ctx->my_node_index].shm_base = NULL;

		char shm_name[128];
		char my_hostname[MAX_HOST_NAME_DESC_LENGTH];
		if (gethostname(my_hostname, sizeof(my_hostname)) == 0) {
			snprintf(shm_name, sizeof(shm_name), "%s_%s",
				 g_ctx->shm_prefix, my_hostname);
			ubsmem_shmem_deallocate(shm_name);
		}
	}

	ubsmem_destroy_region(g_ctx->region_name);
	ubsmem_finalize();

	rte_free(g_ctx->rx_rings);
	rte_free(g_ctx->tx_rings);
	rte_free(g_ctx);
	g_ctx = NULL;

	UB_RING_LOG(INFO, "cleanup complete");
}
