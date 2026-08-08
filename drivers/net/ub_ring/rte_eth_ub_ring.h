/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef RTE_ETH_UB_RING_H
#define RTE_ETH_UB_RING_H

#include <stdint.h>
#include <rte_mbuf.h>
#include <rte_ethdev.h>
#include <rte_ub_ring.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Attach ub_ring handle to an ethdev port.
 *
 * Uses all queues from the handle. Internally checks h->mpmc_enabled
 * to decide whether to use mp_send_burst/mc_recv_burst or send_burst/recv_burst.
 *
 * @param name  ethdev port name
 * @param h     ub_ring handle (from provider_init or user_init)
 * @return port_id (>=0) on success, negative on error
 */
int rte_eth_ub_ring_attach(const char *name, struct rte_ub_ring_handle *h);

#ifdef __cplusplus
}
#endif

#endif /* RTE_ETH_UB_RING_H */
