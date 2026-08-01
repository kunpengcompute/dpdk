/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef RTE_ETH_UB_RING_H
#define RTE_ETH_UB_RING_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_dev.h>
#include <rte_ethdev.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Create a provider ethdev (vdev probe wrapper).
 *
 * Builds a devargs string and calls rte_dev_probe().
 * The PMD vdev probe handles the real init.
 * Load PMD with: -d librte_net_ub_ring.so
 */
static inline int
rte_eth_ub_ring_create_provider(const char *name,
	const char **ub_nodes, uint32_t nb_nodes,
	const char *region, const char *shm_prefix,
	size_t shm_size, uint32_t nb_queues, uint32_t queue_depth)
{
	char devargs[1024];
	char vdev_name[64];
	int ret;

	snprintf(vdev_name, sizeof(vdev_name), "net_ub_ring_%s", name);

	/* compact devargs: under 128 bytes for DPDK's limit */
	char nodes_str[64] = "";
	for (uint32_t i = 0; i < nb_nodes && strlen(nodes_str) < 50; i++) {
		if (i > 0) strcat(nodes_str, "|");
		strncat(nodes_str, ub_nodes[i], 15);
	}

	snprintf(devargs, sizeof(devargs),
		"%s,r=pv,ns=%s,n=%u,reg=%s,pre=%s,sz=%zu,nq=%u,qd=%u",
		vdev_name, nodes_str, nb_nodes, region, shm_prefix,
		shm_size, nb_queues, queue_depth);

	ret = rte_dev_probe(devargs);
	if (ret)
		return -1;

	uint16_t port_id;
	ret = rte_eth_dev_get_port_by_name(name, &port_id);
	if (ret)
		return -1;
	return port_id;
}

/**
 * Create a user ethdev (vdev probe wrapper).
 */
static inline int
rte_eth_ub_ring_create_user(const char *name,
	const char **ub_nodes, uint32_t nb_nodes,
	const char *region, const char *shm_prefix,
	uint32_t data_elt_size)
{
	char devargs[1024];
	char vdev_name[64];
	int ret;

	snprintf(vdev_name, sizeof(vdev_name), "net_ub_ring_%s", name);

	char nodes_str[64] = "";
	for (uint32_t i = 0; i < nb_nodes && strlen(nodes_str) < 50; i++) {
		if (i > 0) strcat(nodes_str, "|");
		strncat(nodes_str, ub_nodes[i], 15);
	}

	snprintf(devargs, sizeof(devargs),
		"%s,r=us,ns=%s,n=%u,reg=%s,pre=%s,de=%u",
		vdev_name, nodes_str, nb_nodes, region, shm_prefix, data_elt_size);

	ret = rte_dev_probe(devargs);
	if (ret)
		return -1;

	uint16_t port_id;
	ret = rte_eth_dev_get_port_by_name(name, &port_id);
	if (ret)
		return -1;
	return port_id;
}

int
rte_eth_ub_ring_get_queue_mapping(uint32_t provider_node_index,
	uint32_t sub_queue, uint32_t *tx_queue_id, uint32_t *rx_queue_id);

#ifdef __cplusplus
}
#endif

#endif /* RTE_ETH_UB_RING_H */
