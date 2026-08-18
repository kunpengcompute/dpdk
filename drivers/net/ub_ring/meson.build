# SPDX-License-Identifier: BSD-3-Clause

sources = files('rte_eth_ub_ring.c')
headers = files('rte_eth_ub_ring.h')
require_iova_in_mbuf = false
deps += ['ub_ring', 'kvargs', 'bus_vdev']

# data-path flow-trace logging: debug builds only
if get_option('buildtype').startswith('debug')
    cflags += '-DUB_RING_FLOW_TRACE'
endif
