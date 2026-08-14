/* SPDX-License-Identifier: BSD-3-Clause
 * Server — N workers (one per queue), each counts its own packets and pps.
 * Run: ./server --no-huge -l 1-5 --nq=4   (nq = number of queues/workers)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_cycles.h>
#include <rte_mbuf.h>
#include <rte_ethdev.h>
#include <rte_ub_ring.h>
#include <rte_eth_ub_ring.h>

#define NP (1024*1024)
#define BS 32

static uint32_t g_nq;

/* parse "--opt=N" BEFORE eal_init and strip it from argv, so EAL's
 * getopt never sees it (would fail with "unrecognized option") */
static uint32_t app_opt_uint(int *argc, char **argv, const char *opt,
			     uint32_t def)
{
	for (int i = 1; i < *argc; i++) {
		size_t l = strlen(opt);
		if (strncmp(argv[i], opt, l) == 0 && argv[i][l] == '=') {
			char *end = NULL;
			unsigned long v = strtoul(argv[i] + l + 1, &end, 0);
			for (int j = i; j < *argc - 1; j++)
				argv[j] = argv[j + 1];
			(*argc)--;
			if (end && *end == '\0')
				return (uint32_t)v;
		}
	}
	return def;
}

struct targ { int pid; uint32_t qid; };

static int worker(void *a)
{
	struct targ *ta = a;
	struct rte_mbuf *pkts[BS];
	uint32_t rx = 0;
	uint64_t rx_bytes = 0;
	uint64_t t0 = 0, t1;

	while (rx < NP / g_nq) {
		uint16_t n = rte_eth_rx_burst(ta->pid, ta->qid, pkts, BS);
		if (n == 0)
			continue;
		if (rx == 0)
			t0 = rte_rdtsc();
		for (uint16_t i = 0; i < n; i++) {
			rx_bytes += pkts[i]->pkt_len;
			rte_pktmbuf_free(pkts[i]);
		}
		rx += n;
	}
	t1 = rte_rdtsc();
	double secs = (double)(t1 - t0) / rte_get_tsc_hz();
	double mpps = rx / secs / 1e6;
	double mbps = rx_bytes / secs * 8 / 1e6;
	printf("[Srv q%u] %u pkts (%lu B), %.3fs, %.2f Mpps, %.2f Mbps\n",
		ta->qid, rx, rx_bytes, secs, mpps, mbps);
	return 0;
}

int main(int argc, char **argv)
{
	g_nq = app_opt_uint(&argc, argv, "--nq", 4);
	if (g_nq < 1 || g_nq > UB_RING_MAX_QUEUES) {
		fprintf(stderr, "bad --nq\n");
		return 1;
	}
	rte_eal_init(argc, argv);

	unsigned int ml = rte_get_main_lcore();
	unsigned int lc[RTE_MAX_LCORE]; unsigned int nl = 0;
	for (unsigned int l = 0; l < RTE_MAX_LCORE; l++)
		if (rte_lcore_is_enabled(l) && l != ml) lc[nl++] = l;
	if (g_nq > nl) {
		fprintf(stderr, "need %u worker lcores (have %u)\n", g_nq, nl);
		return 1;
	}

	printf("\n===== Server (%u queues, %u workers) =====\n\n", g_nq, nl);

	const char *pn[] = {"master"}, *un[] = {"worker1"};
	struct rte_ub_ring_handle *h = rte_ub_ring_provider_init(
		pn, 1, un, 1, "region1", "ubring", 1UL*1024*1024*1024, g_nq, 1024);
	int pid = rte_eth_ub_ring_attach("server_eth", h);
	struct rte_eth_conf c = { .rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN } };
	rte_eth_dev_configure(pid, g_nq, g_nq, &c);
	rte_eth_dev_start(pid);

	struct targ ta[UB_RING_MAX_QUEUES];
	for (uint32_t q = 0; q < g_nq; q++) {
		ta[q] = (struct targ){ pid, q };
		rte_eal_remote_launch(worker, &ta[q], lc[q]);
	}
	for (uint32_t q = 0; q < g_nq; q++)
		rte_eal_wait_lcore(lc[q]);

	rte_eth_dev_stop(pid); rte_eth_dev_close(pid); rte_ub_ring_fini(h);
	rte_eal_cleanup();
	return 0;
}
