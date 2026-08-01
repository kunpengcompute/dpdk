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
	uint64_t total_size;
	uint64_t ring_area_offset;         /* tx/rx ring area offset from shm base */
	uint64_t ring_area_size;           /* tx/rx ring area total size */
	char     node_name[64];            /* this node's hostname (for validation) */
	uint64_t queue_offsets[];          /* [rx_offsets...][tx_offsets...] */
} __rte_aligned(64);

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
 * Creates own shm, initializes tx/rx rings, writes metadata.
 * Data ring area is left uninitialized — user will set it up.
 *
 * @param ub_nodes      All participating node hostnames
 * @param nb_nodes      Number of nodes
 * @param region        ubsmem region name
 * @param shm_prefix    Shared memory name prefix ("{prefix}_{hostname}")
 * @param shm_size      Shared memory size (bytes)
 * @param nb_queues     Number of queues per direction
 * @param queue_depth   Ring element count (must be power of 2)
 * @return 0 on success, negative on error
 */
int rte_ub_ring_provider_init(const char **ub_nodes, uint32_t nb_nodes,
			      const char *region, const char *shm_prefix,
			      size_t shm_size, uint32_t nb_queues,
			      uint32_t queue_depth);

/* ================================================================
 *  User API
 * ================================================================ */

/**
 * User initialization.
 *
 * Maps all provider shms, reads metadata, initializes data rings
 * on each provider shm (index ring + buffer array), populates with
 * free buffer indices. Collects all tx/rx ring pointers.
 *
 * @param ub_nodes       All node hostnames
 * @param nb_nodes       Number of nodes
 * @param region         ubsmem region name
 * @param shm_prefix     Shared memory name prefix
 * @param data_elt_size  Size of each buffer in the data ring
 * @return 0 on success, negative on error
 */
int rte_ub_ring_user_init(const char **ub_nodes, uint32_t nb_nodes,
			  const char *region, const char *shm_prefix,
			  uint32_t data_elt_size);

/* ================================================================
 *  Common API
 * ================================================================ */

int rte_ub_ring_get_node_index(uint32_t *idx);
int rte_ub_ring_get_peer_index(const char *hostname, uint32_t *idx);

int rte_ub_ring_get_queue_mapping(uint32_t provider_node_index,
				   uint32_t sub_queue,
				   uint32_t *tx_queue_id,
				   uint32_t *rx_queue_id);

/** O(1) reverse lookup: global queue_id → provider node_index */
uint32_t rte_ub_ring_queue_to_provider(uint32_t queue_id);

uint32_t rte_ub_ring_get_total_queues(void);

/**
 * Allocate a buffer from a provider's data ring.
 * Dequeues an index from the data ring, returns pointer to the buffer.
 *
 * @param provider_node_index  Which provider's data ring
 * @return buffer pointer, or NULL if ring empty
 */
void *rte_ub_ring_alloc_buf(uint32_t provider_node_index);

/**
 * Free a buffer back to its data ring.
 * Computes buffer index and enqueues it to the owning data ring (MPSC).
 *
 * @param buf  Buffer pointer (must be from alloc_buf)
 */
void rte_ub_ring_free_buf(void *buf);

/**
 * Get the data element size for a provider's data ring.
 */
uint32_t rte_ub_ring_get_data_elt_size(uint32_t provider_node_index);

/**
 * Get a provider's data ring buffer area base address.
 */
void *rte_ub_ring_get_data_buf_area(uint32_t provider_node_index);

/**
 * Send multiple buffers via a TX ring.
 *
 * Each buffer pointer is encoded to {node_index, offset} relative
 * to its owning data ring base, then enqueued into the TX ring.
 *
 * @param queue_id  Global TX queue_id
 * @param bufs      Array of buffer pointers
 * @param nb        Number of buffers to send
 * @return number of buffers actually sent
 */
uint16_t rte_ub_ring_send_burst(uint32_t queue_id, void **bufs, uint16_t nb);

/**
 * Receive multiple buffers via an RX ring.
 *
 * Dequeues encoded elements from the RX ring, decodes each
 * {node_index, offset} to a local VA pointer in the owning
 * shm's data ring buffer area.
 *
 * @param queue_id  Global RX queue_id
 * @param bufs      [out] Array for received buffer pointers
 * @param nb        Maximum number to receive
 * @return number of buffers actually received
 */
uint16_t rte_ub_ring_recv_burst(uint32_t queue_id, void **bufs, uint16_t nb);

void rte_ub_ring_fini(void);

#ifdef __cplusplus
}
#endif

#endif /* RTE_UB_RING_H */
