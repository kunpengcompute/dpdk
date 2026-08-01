/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef RTE_ETH_UB_RING_H
#define RTE_ETH_UB_RING_H

/**
 * @file
 *
 * ub_ring ethdev PMD (Layer 3) — mbuf adapter over lib/ub_ring (Layer 2).
 *
 * TX: mbuf → encode to ub_buf → rte_ub_ring_send_buf
 * RX: rte_ub_ring_recv_buf → decode to mbuf (dummy pool + drain)
 */

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ethdev.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Provider ethdev creation.
 */
int rte_eth_ub_ring_create_provider(const char *name,
	const char **ub_nodes, uint32_t nb_nodes,
	const char *region, const char *shm_prefix,
	size_t shm_size, uint32_t nb_queues, uint32_t queue_depth);

/**
 * User ethdev creation.
 *
 * @param data_elt_size  Size of each data ring buffer element
 */
int rte_eth_ub_ring_create_user(const char *name,
	const char **ub_nodes, uint32_t nb_nodes,
	const char *region, const char *shm_prefix,
	uint32_t data_elt_size);

int rte_eth_ub_ring_get_queue_mapping(uint32_t provider_node_index,
	uint32_t sub_queue, uint32_t *tx_queue_id, uint32_t *rx_queue_id);

#ifdef __cplusplus
}
#endif

#endif /* RTE_ETH_UB_RING_H */
