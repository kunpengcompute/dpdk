/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef RTE_UB_RING_H
#define RTE_UB_RING_H

/**
 * @file
 *
 * ub_ring: Pure data buffer transport over ubsmem shared memory (Layer 2).
 *
 * Provides provider/user init, buffer alloc/free from data rings,
 * and void* send/recv with {node_index, offset} conversion.
 * No mbuf or mempool — that is Layer 3's responsibility.
 */

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include <rte_ring.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---- constants ---- */

#define UB_RING_MAX_NODES    256
#define UB_RING_MAX_QUEUES   512
#define UB_RING_MAGIC        0x5542524E4752494EULL  /* "UBRINGRN" */
#define UB_RING_VERSION      0x00020000

/* ---- metadata structure (in shared memory) ---- */

#define UB_RING_STATE_META_READY       (1U << 0)
#define UB_RING_STATE_DATA_RING_READY  (1U << 1)

struct ub_ring_metadata {
	uint64_t magic;
	uint32_t version;
	uint32_t nb_nodes;
	uint32_t nb_queues;
	uint32_t node_index;
	uint32_t queue_depth;
	uint32_t state;                    /* bitmask: meta_ready | data_ring_ready */
	uint32_t data_elt_size;            /* size of each buffer element */
	uint32_t data_nb_bufs;             /* number of buffers */
	uint64_t data_ring_offset;         /* return ring offset from shm base */
	uint64_t total_size;
	uint64_t ring_area_offset;         /* tx/rx ring area offset from shm base */
	uint64_t ring_area_size;           /* tx/rx ring area total size */
	char     node_name[64];            /* this node's hostname (for validation) */
	uint64_t queue_offsets[];          /* [rx_offsets...][tx_offsets...] */
} __rte_aligned(64);

/* ---- per-buffer ownership header (128B prefix of every element) ---- */

#define UB_RING_BUF_HDR_SIZE 128
#define UB_RING_BUF_MAGIC   0x55425248ULL /* "UBRH" */

/* written once by user_init, read-only afterwards */
struct ub_ring_buf_hdr {
	uint32_t node_index; /* owning provider node index */
	uint32_t offset;     /* element offset from data_buf_area (bytes) */
	uint32_t elt_size;   /* element stride in bytes (sanity check) */
	uint32_t magic;      /* UB_RING_BUF_MAGIC */
};

/* The app-visible buffer pointer sits UB_RING_BUF_HDR_SIZE past the
 * element base; the ownership header is right in front of it. */
static inline struct ub_ring_buf_hdr *
ub_ring_buf_hdr(const void *buf)
{
	return (struct ub_ring_buf_hdr *)((const char *)buf -
			UB_RING_BUF_HDR_SIZE);
}

/* element base area + offset -> app-visible buffer pointer */
static inline void *
ub_ring_buf_ptr(void *area, uint64_t off)
{
	return (char *)area + off + UB_RING_BUF_HDR_SIZE;
}

/* app-visible buffer pointer -> element-base offset */
static inline uint64_t
ub_ring_buf_off(void *area, const void *buf)
{
	return (uint64_t)((const char *)buf - (const char *)area -
			UB_RING_BUF_HDR_SIZE);
}

/* ---- ring element codec (8+56 bit packing) ---- */

static inline void *
ub_ring_elem_pack(uint8_t node_index, uint64_t offset)
{
	uint64_t v = ((uint64_t)node_index << 56) |
		     (offset & 0x00FFFFFFFFFFFFFFULL);
	return (void *)(uintptr_t)v;
}

static inline uint8_t
ub_ring_elem_get_node(void *elem)
{
	return (uint8_t)((uintptr_t)elem >> 56);
}

static inline uint64_t
ub_ring_elem_get_offset(void *elem)
{
	return (uint64_t)((uintptr_t)elem & 0x00FFFFFFFFFFFFFFULL);
}

/* ---- opaque handle ---- */

struct rte_ub_ring_handle;

/* ---- role enum ---- */

enum ub_ring_role {
	UB_RING_ROLE_PROVIDER = 0,
	UB_RING_ROLE_USER     = 1,
};

/* ================================================================
 *  Provider API
 * ================================================================ */

/**
 * Provider initialization.
 *
 * Creates own ubsmem shm, initializes tx/rx rings, writes metadata,
 * registers in bootstrap. Waits for bootstrap READY before mapping
 * peer shms. Data ring area left uninitialized for user.
 *
 * @param provider_nodes  Provider node hostnames (self must be included)
 * @param nb_providers    Number of provider nodes
 * @param user_nodes      User node hostnames (NULL if nb_users==0)
 * @param nb_users        Number of user nodes
 * @param region          ubsmem region name
 * @param shm_prefix      shm name prefix ("{prefix}_{hostname}")
 * @param shm_size        Shared memory size in bytes
 * @param nb_queues       Number of queues per direction
 * @param queue_depth     Ring element count (must be power of 2)
 * @return handle on success, NULL on error
 */
struct rte_ub_ring_handle *
rte_ub_ring_provider_init(const char **provider_nodes, uint32_t nb_providers,
			  const char **user_nodes, uint32_t nb_users,
			  const char *region, const char *shm_prefix,
			  size_t shm_size, uint32_t nb_queues,
			  uint32_t queue_depth);

/* ================================================================
 *  User API
 * ================================================================ */

/**
 * User initialization.
 *
 * Maps all provider shms via bootstrap, initializes data rings on each
 * provider shm, populates free buffers, collects tx/rx ring pointers.
 *
 * @param provider_nodes  Provider node hostnames
 * @param nb_providers    Number of provider nodes
 * @param user_nodes      User node hostnames (self must be included)
 * @param nb_users        Number of user nodes
 * @param region          ubsmem region name
 * @param shm_prefix      shm name prefix
 * @param data_elt_size   Size of each data ring buffer element
 * @return handle on success, NULL on error
 */
struct rte_ub_ring_handle *
rte_ub_ring_user_init(const char **provider_nodes, uint32_t nb_providers,
		      const char **user_nodes, uint32_t nb_users,
		      const char *region, const char *shm_prefix,
		      uint32_t data_elt_size);

/* ================================================================
 *  Common API — all take handle from init
 * ================================================================ */

int rte_ub_ring_get_node_index(struct rte_ub_ring_handle *h, uint32_t *idx);
int rte_ub_ring_get_peer_index(struct rte_ub_ring_handle *h,
			       const char *hostname, uint32_t *idx);

int rte_ub_ring_get_queue_mapping(struct rte_ub_ring_handle *h,
				  uint32_t provider_node_index,
				  uint32_t sub_queue,
				  uint32_t *tx_queue_id,
				  uint32_t *rx_queue_id);

/** O(1) reverse lookup: global queue_id → provider node_index */
uint32_t rte_ub_ring_queue_to_provider(struct rte_ub_ring_handle *h,
				       uint32_t queue_id);

uint32_t rte_ub_ring_get_total_queues(struct rte_ub_ring_handle *h);

/**
 * Allocate a buffer from a provider's local data ring (user only).
 * Returns the app-visible pointer: element base + 128B ownership
 * header (see ub_ring_buf_hdr()).
 */
void *rte_ub_ring_alloc_buf(struct rte_ub_ring_handle *h,
			    uint32_t provider_node_index);

/**
 * Free a buffer back to local data ring (user) or return ring (provider).
 * Ownership is read from the buffer's 128B header (O(1), no node scan).
 */
void rte_ub_ring_free_buf(struct rte_ub_ring_handle *h, void *buf);

/**
 * Return a FINAL packed {pi, offset} element to this lcore's alloc
 * cache (user side). No 128B header read — the element already
 * carries the owning pi. The cache never overflows by design (1024
 * slots > 512 refill + one burst's max failed returns), so nothing is
 * flushed to the data ring. Intended for TX failure paths: freshly
 * encoded buffers go straight back to the sender's cache, so the next
 * alloc_buf on this lcore reuses them without a ring round trip.
 */
void rte_ub_ring_free_elem_cached(struct rte_ub_ring_handle *h,
				  void *elem);

/**
 * Return a used buffer (provider side). The element enqueued to the
 * return ring is packed {node_index, offset} read from the buffer's
 * 128B header, and the ring used is the CALLING provider's own return
 * ring — independent of which provider owns the buffer.
 * Return rings are SPSC: only the provider's drain lcore may call this
 * (any other caller must be serialized with it, e.g. at teardown).
 */
void rte_ub_ring_return_buf(struct rte_ub_ring_handle *h, void *buf);

/**
 * Return a batch of used buffers (provider side). Same rules as
 * rte_ub_ring_return_buf() — packed {node_index, offset} elements into
 * the calling provider's own return ring, one bulk SP enqueue per batch.
 */
void rte_ub_ring_return_burst(struct rte_ub_ring_handle *h, void **bufs,
			      uint16_t nb);

/**
 * Collect stage 1 (fetch): move buffers from the REMOTE return rings
 * into the local mid ring. Run on its own lcore — this is where all
 * the non-cacheable remote reads happen; everything else here is a
 * local SP enqueue. One 512-batch per return ring per call (bounded).
 *
 * @return number of buffers fetched (0 = idle)
 */
uint64_t rte_ub_ring_collect_fetch(struct rte_ub_ring_handle *h);

/**
 * Collect stage 2 (dispatch): SC-dequeue one batch from the local mid
 * ring and route the packed {pi, offset} elements into pi's local
 * data ring (AS-IS — all rings share one element format). Pure local
 * work; run on its own lcore.
 *
 * @return number of buffers dispatched (0 = idle)
 */
uint64_t rte_ub_ring_collect_dispatch(struct rte_ub_ring_handle *h);

/**
 * Serial combined fetch+dispatch (e.g. for teardown). Requires
 * rte_ub_ring_enable_mpmc() — the mid ring lives with the shadow rings.
 *
 * @return total number of buffers collected
 */
uint64_t rte_ub_ring_collect(struct rte_ub_ring_handle *h);

/**
 * Get the data element size for a provider's data ring.
 */
uint32_t rte_ub_ring_get_data_elt_size(struct rte_ub_ring_handle *h,
				       uint32_t provider_node_index);

/**
 * Get a provider's data ring buffer area base address.
 */
void *rte_ub_ring_get_data_buf_area(struct rte_ub_ring_handle *h,
				    uint32_t provider_node_index);

/**
 * Send multiple buffers via a TX ring.
 *
 * @param h         handle from init
 * @param queue_id  global TX queue_id
 * @param bufs      array of buffer pointers
 * @param nb        number of buffers to send
 * @return number of buffers actually sent
 */
uint16_t rte_ub_ring_send_burst(struct rte_ub_ring_handle *h,
				uint32_t queue_id, void **bufs, uint16_t nb);

/**
 * Send burst of FINAL packed elements (non-MPMC path).
 * Elements are already encoded as {node_index, offset} by enc() —
 * they are enqueued to the real TX ring as-is, no re-encoding.
 *
 * @param elems  array of packed {node_index, offset} elements
 */
uint16_t rte_ub_ring_send_elems(struct rte_ub_ring_handle *h,
				uint32_t queue_id, void **elems, uint16_t nb);

/**
 * Receive multiple buffers via an RX ring.
 *
 * @param h         handle from init
 * @param queue_id  global RX queue_id
 * @param bufs      [out] array for received buffer pointers
 * @param nb        maximum number to receive
 * @return number of buffers actually received
 */
uint16_t rte_ub_ring_recv_burst(struct rte_ub_ring_handle *h,
				uint32_t queue_id, void **bufs, uint16_t nb);

/* ================================================================
 *  MPMC support (user side — shadow rings for multi-threaded access)
 * ================================================================ */

/**
 * Enable MPMC shadow rings for this handle.
 * Creates local shadow rings per queue:
 *  - shadow_tx: MP/SC — n sender lcores enqueue, flush lcore dequeues
 *  - shadow_rx: SP/MC — flush lcore enqueues, rx lcores dequeue
 */
int rte_ub_ring_enable_mpmc(struct rte_ub_ring_handle *h);
bool rte_ub_ring_is_mpmc_enabled(struct rte_ub_ring_handle *h);

/**
 * MPMC send of FINAL packed elements: enqueue to shadow_tx[qid] as-is.
 * Elements are already encoded as {node_index, offset} by enc() — no
 * re-encoding on the hot path.
 */
uint16_t rte_ub_ring_mp_send_elems(struct rte_ub_ring_handle *h,
				   uint32_t qid, void **elems, uint16_t nb);

/**
 * MPMC recv (multi-consumer safe): dequeue from shadow_rx[qid].
 */
uint16_t rte_ub_ring_mc_recv_burst(struct rte_ub_ring_handle *h,
				   uint32_t qid, void **bufs, uint16_t nb);

/**
 * Flush TX: move all from shadow_tx[qid] → real tx_ring[qid].
 * Single-threaded caller only.
 *
 * @return number of elements actually moved (0 = shadow empty / idle)
 */
uint64_t rte_ub_ring_flush_tx(struct rte_ub_ring_handle *h, uint32_t qid);

/**
 * Flush TX for all queues, retrying until every shadow_tx ring is
 * empty; gives up with a warning after ~1s without progress (peer
 * done consuming). Use at teardown (after the flush worker stopped).
 * Single-threaded caller only.
 */
void rte_ub_ring_flush_tx_all(struct rte_ub_ring_handle *h);

/**
 * Flush RX: move all from real rx_ring[qid] → shadow_rx[qid].
 * Single-threaded caller only.
 */
void rte_ub_ring_flush_rx(struct rte_ub_ring_handle *h, uint32_t qid);

/**
 * Clean up the entire ub_ring subsystem.
 */
void rte_ub_ring_fini(struct rte_ub_ring_handle *h);

#ifdef __cplusplus
}
#endif

#endif /* RTE_UB_RING_H */
