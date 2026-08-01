/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Client (user) — ub_ring ethdev PMD.
 * Run: ./client --no-huge -l 0
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <rte_eal.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ethdev.h>
#include <rte_errno.h>

#include <rte_eth_ub_ring.h>

#define NUM_PACKETS      10
#define BURST_SIZE       32
#define SEND_INTERVAL_US 500000

int
main(int argc, char **argv)
{
	if (rte_eal_init(argc, argv) < 0) {
		fprintf(stderr, "[Client] rte_eal_init failed\n");
		return EXIT_FAILURE;
	}
	printf("\n===== Client (user) =====\n\n");

	const char *ub_nodes[] = {"master", "worker1"};
	uint32_t nb_nodes = 2;
	uint32_t data_elt_size = sizeof(struct rte_mbuf) + RTE_MBUF_DEFAULT_BUF_SIZE;

	int port_id = rte_eth_ub_ring_create_user("client_eth",
		ub_nodes, nb_nodes, "region1", "ubring", data_elt_size);
	if (port_id < 0) {
		fprintf(stderr, "[Client] create_user failed: %d\n", port_id);
		return EXIT_FAILURE;
	}
	printf("[Client] ethdev port=%d\n", port_id);

	struct rte_eth_conf conf = {
		.rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN },
	};
	uint32_t total_q = 1;
	if (rte_eth_dev_configure(port_id, total_q, total_q, &conf) < 0 ||
	    rte_eth_dev_start(port_id) < 0) {
		fprintf(stderr, "[Client] configure/start failed\n");
		rte_eth_dev_close(port_id);
		return EXIT_FAILURE;
	}

	/* need a local mempool for alloc on TX side */
	struct rte_mempool *local_pool = rte_pktmbuf_pool_create("local_pool",
		256, 32, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
	if (!local_pool) {
		fprintf(stderr, "[Client] local pool create failed\n");
		rte_eth_dev_close(port_id);
		return EXIT_FAILURE;
	}

	for (int i = 0; i < NUM_PACKETS; i++) {
		struct rte_mbuf *m = rte_pktmbuf_alloc(local_pool);
		if (!m) {
			fprintf(stderr, "[Client] alloc #%d failed\n", i);
			continue;
		}

		char buf[128];
		int len = snprintf(buf, sizeof(buf),
				   "Hello from client! Packet #%d", i);
		if (!rte_pktmbuf_append(m, len)) {
			rte_pktmbuf_free(m);
			continue;
		}
		rte_memcpy(rte_pktmbuf_mtod(m, char *), buf, len);
		printf("[Client] TX #%d: %s\n", i, buf);

		while (!rte_eth_tx_burst(port_id, 0, &m, 1))
			;

		struct rte_mbuf *back = NULL;
		while (!rte_eth_rx_burst(port_id, 0, &back, 1))
			rte_delay_us(1000);

		/* back->pool is dummy pool → rte_pktmbuf_free goes to dummy,
		 * next RX will drain it back to data ring */
		rte_pktmbuf_free(back);
		printf("[Client] #%d freed\n", i);

		rte_delay_us(SEND_INTERVAL_US);
	}

	printf("\n[Client] done (%d packets)\n", NUM_PACKETS);

	rte_eth_dev_stop(port_id);
	rte_eth_dev_close(port_id);
	rte_mempool_free(local_pool);
	rte_eal_cleanup();
	return 0;
}
