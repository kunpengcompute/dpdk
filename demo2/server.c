/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Server (provider) — ub_ring ethdev PMD.
 * Run: ./server --no-huge -l 0
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <rte_eal.h>
#include <rte_mbuf.h>
#include <rte_ethdev.h>
#include <rte_errno.h>

#include <rte_eth_ub_ring.h>

#define NUM_PACKETS 10
#define BURST_SIZE  32

int
main(int argc, char **argv)
{
	if (rte_eal_init(argc, argv) < 0) {
		fprintf(stderr, "[Server] rte_eal_init failed\n");
		return EXIT_FAILURE;
	}
	printf("\n===== Server (provider) =====\n\n");

	const char *ub_nodes[] = {"master", "worker1"};
	uint32_t nb_nodes = 2;
	uint32_t nb_queues = 1;
	uint32_t queue_depth = 1024;
	size_t shm_size = 1UL * 1024 * 1024 * 1024;

	int port_id = rte_eth_ub_ring_create_provider("server_eth",
		ub_nodes, nb_nodes, "region1", "ubring",
		shm_size, nb_queues, queue_depth);
	if (port_id < 0) {
		fprintf(stderr, "[Server] create_provider failed: %d\n", port_id);
		return EXIT_FAILURE;
	}
	printf("[Server] ethdev port=%d\n", port_id);

	struct rte_eth_conf conf = {
		.rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN },
	};
	if (rte_eth_dev_configure(port_id, nb_queues, nb_queues, &conf) < 0 ||
	    rte_eth_dev_start(port_id) < 0) {
		fprintf(stderr, "[Server] configure/start failed\n");
		rte_eth_dev_close(port_id);
		return EXIT_FAILURE;
	}

	struct rte_mbuf *pkts[BURST_SIZE];
	int received = 0;
	printf("[Server] listening...\n\n");

	while (received < NUM_PACKETS) {
		uint16_t n = rte_eth_rx_burst(port_id, 0, pkts, BURST_SIZE);
		if (n == 0) {
			rte_delay_us(1000);
			continue;
		}
		for (uint16_t i = 0; i < n; i++) {
			uint32_t len = rte_pktmbuf_data_len(pkts[i]);
			printf("[Server] RX (%uB): %.*s\n",
			       len, len, rte_pktmbuf_mtod(pkts[i], char *));
			received++;
			while (rte_eth_tx_burst(port_id, 0, &pkts[i], 1) == 0)
				;
		}
	}

	printf("\n[Server] done (%d packets)\n", received);

	rte_eth_dev_stop(port_id);
	rte_eth_dev_close(port_id);
	rte_eal_cleanup();
	return (received == NUM_PACKETS) ? 0 : EXIT_FAILURE;
}
