/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef RTE_ETH_UB_RING_H
#define RTE_ETH_UB_RING_H

#include <stdint.h>
#include <stdio.h>
#include <rte_ethdev.h>

struct rte_ub_ring_handle;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Attach ub_ring handle to an ethdev port.
 *
 * Builds a devargs string and calls rte_dev_probe() internally.
 * The PMD vdev probe handles the rest. dev->device is properly set.
 *
 * @param name  ethdev port name
 * @param h     ub_ring handle
 * @return port_id on success, -1 on error
 */
int rte_eth_ub_ring_attach(const char *name, struct rte_ub_ring_handle *h);

#ifdef __cplusplus
}
#endif

#endif
