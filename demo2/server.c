/* SPDX-License-Identifier: BSD-3-Clause
 * Server (provider) — 4 lcores, 4 queues.
 * Run: ./server --no-huge -l 0-3
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_mbuf.h>
#include <rte_ethdev.h>
#include <rte_ub_ring.h>
#include <rte_eth_ub_ring.h>

#define NQ   4
#define NP   40
#define BS   32

struct thread_arg { int pid; uint32_t qid; uint32_t max_pkts; };

static int
worker(void *a)
{
	struct thread_arg *ta = a;
	struct rte_mbuf *pkts[BS];
	uint32_t rx = 0;

	while (rx < ta->max_pkts) {
		uint16_t n = rte_eth_rx_burst(ta->pid, ta->qid, pkts, BS);
		for (uint16_t i = 0; i < n; i++) {
			uint32_t l = rte_pktmbuf_data_len(pkts[i]);
			printf("[Srv q%u] RX(%uB): %.*s\n", ta->qid, l, l,
			       rte_pktmbuf_mtod(pkts[i], char *));
			rx++;
			while (!rte_eth_tx_burst(ta->pid, ta->qid, &pkts[i], 1));
			rte_pktmbuf_free(pkts[i]);
		}
	}
	return rx;
}

int main(int argc, char **argv)
{
	if (rte_eal_init(argc, argv) < 0) { fprintf(stderr,"eal\n"); return 1; }
	printf("\n===== Server (%u queues) =====\n\n", NQ);

	const char *pn[] = {"master"}, *un[] = {"worker1"};
	struct rte_ub_ring_handle *h = rte_ub_ring_provider_init(
		pn, 1, un, 1, "region1", "ubring", 1UL*1024*1024*1024, NQ, 1024);
	if (!h) { fprintf(stderr,"init\n"); return 1; }

	int pid = rte_eth_ub_ring_attach("server_eth", h);
	if (pid < 0) { fprintf(stderr,"attach %d\n", pid); return 1; }

	struct rte_eth_conf c = { .rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN } };
	if (rte_eth_dev_configure(pid, NQ, NQ, &c) < 0 || rte_eth_dev_start(pid) < 0)
		{ fprintf(stderr,"cfg\n"); rte_eth_dev_close(pid); rte_ub_ring_fini(h); return 1; }

	struct thread_arg ta[NQ];
	for (uint32_t q = 0; q < NQ; q++) {
		ta[q] = (struct thread_arg){ pid, q, NP / NQ };
		rte_eal_remote_launch(worker, &ta[q], q);
	}

	int total = 0;
	for (uint32_t q = 0; q < NQ; q++)
		total += rte_eal_wait_lcore(q);

	printf("\n[Server] done (%d)\n", total);
	rte_eth_dev_stop(pid); rte_eth_dev_close(pid); rte_ub_ring_fini(h);
	rte_eal_cleanup();
	return 0;
}
