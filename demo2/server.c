/* SPDX-License-Identifier: BSD-3-Clause
 * Server — 4 workers, rx + free, auto-exit after NP total.
 * Run: ./server --no-huge -l 1-5
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

#define NQ 4
#define NP (1024*1024)
#define BS 32

static volatile int g_running = 1;
static volatile int g_total;

struct targ { int pid; uint32_t qid; };

static int worker(void *a)
{
	struct targ *ta = a;
	struct rte_mbuf *pkts[BS];
	while (g_running) {
		uint16_t n = rte_eth_rx_burst(ta->pid, ta->qid, pkts, BS);
		for (uint16_t i = 0; i < n; i++) {
			uint32_t l = rte_pktmbuf_data_len(pkts[i]);
			printf("[Srv q%u] RX %.*s (pool=%p)\n", ta->qid, l,
			       rte_pktmbuf_mtod(pkts[i], char *),
			       (void *)pkts[i]->pool);
			fflush(stdout);
			rte_pktmbuf_free(pkts[i]);
			int t = __atomic_add_fetch(&g_total, 1, __ATOMIC_RELAXED);
			printf("[Srv q%u] total=%d/%d\n", ta->qid, t, NP);
			fflush(stdout);
			if (t >= NP) { g_running = 0; return 0; }
		}
	}
	return 0;
}

int main(int argc, char **argv)
{
	rte_eal_init(argc, argv);
	unsigned int ml = rte_get_main_lcore();
	unsigned int lc[8]; unsigned int nl = 0;
	for (unsigned int l = 0; l < RTE_MAX_LCORE; l++)
		if (rte_lcore_is_enabled(l) && l != ml) lc[nl++] = l;

	printf("\n===== Server (%u queues, %u workers) =====\n\n", NQ, nl);

	const char *pn[] = {"master"}, *un[] = {"worker1"};
	struct rte_ub_ring_handle *h = rte_ub_ring_provider_init(
		pn, 1, un, 1, "region1", "ubring", 1UL*1024*1024*1024, NQ, 1024);
	int pid = rte_eth_ub_ring_attach("server_eth", h);
	struct rte_eth_conf c = { .rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN } };
	rte_eth_dev_configure(pid, NQ, NQ, &c);
	rte_eth_dev_start(pid);

	struct targ ta[NQ];
	for (uint32_t q = 0; q < NQ; q++) {
		ta[q] = (struct targ){ pid, q };
		rte_eal_remote_launch(worker, &ta[q], lc[q]);
	}

	printf("[Server] running...\n");
	for (uint32_t q = 0; q < NQ; q++)
		rte_eal_wait_lcore(lc[q]);

	rte_eth_dev_stop(pid); rte_eth_dev_close(pid); rte_ub_ring_fini(h);
	printf("[Server] done (%d total)\n", g_total);
	rte_eal_cleanup();
	return 0;
}
