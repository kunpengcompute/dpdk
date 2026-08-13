/* SPDX-License-Identifier: BSD-3-Clause
 * Client — 2 send lcores + 1 flush lcore, each sender counts its own pps.
 * Run: ./client --no-huge -l 1-4
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_cycles.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ethdev.h>
#include <rte_ub_ring.h>
#include <rte_eth_ub_ring.h>

#define NQ      4
#define NP      (1024*1024)
#define BS      32

static volatile int g_running = 1;
static struct rte_ub_ring_handle *g_h;
static uint32_t g_nb_q;

static int flush_worker(void *arg)
{
	(void)arg;
	while (g_running) {
		for (uint32_t q = 0; q < g_nb_q; q++) {
			rte_ub_ring_flush_tx(g_h, q);
			rte_ub_ring_flush_rx(g_h, q);
		}
		rte_ub_ring_collect(g_h);
		usleep(1);
	}
	return 0;
}

struct sarg { int pid; uint32_t q0, q1; struct rte_mempool *mp; };

static int sender(void *a)
{
	struct sarg *sa = a;
	struct rte_mbuf *burst[BS];
	uint32_t rounds = NP / 2 / BS;
	uint32_t sent_total = 0;

	/* phase 1 (untimed): build one batch of packets, reused every round */
	for (int i = 0; i < BS; i++) {
		struct rte_mbuf *m = rte_pktmbuf_alloc(sa->mp);
		if (!m) break;
		char buf[64]; int l = snprintf(buf, 64, "Hello #%d", i);
		rte_pktmbuf_append(m, l);
		rte_memcpy(rte_pktmbuf_mtod(m, char *), buf, l);
		burst[i] = m;
	}

	/* phase 2 (timed): send the same batch round after round */
	uint64_t t0 = rte_rdtsc();
	for (uint32_t r = 0; r < rounds; r++) {
		uint32_t q = (r & 1) ? sa->q1 : sa->q0;

		uint16_t sent = 0;
		while (sent < BS) {
			uint16_t s = rte_eth_tx_burst(sa->pid, q,
				&burst[sent], BS - sent);
			if (s == 0) {
				rte_delay_us(100);
				continue;
			}
			sent += s;
		}
		sent_total += BS;
	}
	uint64_t t1 = rte_rdtsc();

	/* phase 3 (untimed): free the batch */
	for (int i = 0; i < BS; i++)
		rte_pktmbuf_free(burst[i]);

	double secs = (double)(t1 - t0) / rte_get_tsc_hz();
	printf("[Cli lc%u] %u pkts, %.3fs, %.2f Mpps\n",
		rte_lcore_id(), sent_total, secs, sent_total / secs / 1e6);
	return 0;
}

int main(int argc, char **argv)
{
	rte_eal_init(argc, argv);
	unsigned int ml = rte_get_main_lcore();
	unsigned int lc[8]; unsigned int nl = 0;
	for (unsigned int l = 0; l < RTE_MAX_LCORE; l++)
		if (rte_lcore_is_enabled(l) && l != ml) lc[nl++] = l;
	if (nl < 3) { fprintf(stderr,"need 3 worker lcores\n"); return 1; }

	printf("\n===== Client (%u queues, flush=lc%u, senders=lc%u,lc%u) =====\n\n",
		NQ, lc[0], lc[1], lc[2]);

	const char *pn[] = {"master"}, *un[] = {"worker1"};
	uint32_t elt = sizeof(struct rte_mbuf) + RTE_MBUF_DEFAULT_BUF_SIZE;
	struct rte_ub_ring_handle *h = rte_ub_ring_user_init(
		pn, 1, un, 1, "region1", "ubring", elt);
	rte_ub_ring_enable_mpmc(h);
	int pid = rte_eth_ub_ring_attach("client_eth", h);

	struct rte_eth_conf c = { .rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN } };
	uint32_t tq = rte_ub_ring_get_total_queues(h);
	rte_eth_dev_configure(pid, tq, tq, &c);
	rte_eth_dev_start(pid);

	g_h = h; g_nb_q = tq;
	rte_eal_remote_launch(flush_worker, NULL, lc[0]);

	struct rte_mempool *mp = rte_pktmbuf_pool_create("tx", 256, 32, 0,
		RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());

	struct sarg s0 = { pid, 0, 1, mp }, s1 = { pid, 2, 3, mp };
	rte_eal_remote_launch(sender, &s0, lc[1]);
	rte_eal_remote_launch(sender, &s1, lc[2]);
	rte_eal_wait_lcore(lc[1]); rte_eal_wait_lcore(lc[2]);

	/* final flush: drain any packets still in shadow rings */
	for (uint32_t q = 0; q < g_nb_q; q++) {
		rte_ub_ring_flush_tx(g_h, q);
		rte_ub_ring_flush_rx(g_h, q);
	}
	rte_ub_ring_collect(g_h);

	g_running = 0;
	rte_eal_wait_lcore(lc[0]);
	rte_eth_dev_stop(pid); rte_eth_dev_close(pid); rte_ub_ring_fini(h);
	rte_mempool_free(mp);
	rte_eal_cleanup();
	return 0;
}
