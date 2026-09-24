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

/**
 * Drain all freed mbufs from the port's dummy mempool back to the
 * providers' return rings (batched, one bulk enqueue per batch).
 *
 * RX workers free mbufs into their per-lcore caches; only the overflow
 * reaches the pool ring, which is what this function consumes. Run it
 * in a loop on a dedicated lcore so buffers keep flowing back to the
 * providers while the workers only receive.
 *
 * @param port_id  port returned by rte_eth_ub_ring_attach()
 * @return number of buffers returned to the return rings
 *         (0 = nothing to drain / idle)
 */
unsigned int rte_eth_ub_ring_drain(int port_id);

#ifdef __cplusplus
}
#endif

#endif
