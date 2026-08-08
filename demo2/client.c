/* SPDX-License-Identifier: BSD-3-Clause
 * Client (user) — 2 send lcores + 1 flush lcore, 4 queues.
 * Run: ./client --no-huge -l 0-3
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ethdev.h>
#include <rte_ub_ring.h>
#include <rte_eth_ub_ring.h>

#define NQ      4
#define NP      40
#define BS      32
#define SEND_US 200000

static volatile int g_running = 1;
static struct rte_ub_ring_handle *g_h;
static uint32_t g_nb_q;

static int
flush_worker(void *arg)
{
	(void)arg;
	while (g_running) {
		for (uint32_t q = 0; q < g_nb_q; q++) {
			rte_ub_ring_flush_tx(g_h, q);
			rte_ub_ring_flush_rx(g_h, q);
		}
		usleep(100);
	}
	return 0;
}

struct send_arg { int pid; uint32_t q0, q1; int count; struct rte_mempool *mp; };

static int
sender(void *a)
{
	struct send_arg *sa = a;
	for (int i = 0; i < sa->count; i++) {
		uint32_t q = (i & 1) ? sa->q1 : sa->q0;
		struct rte_mbuf *m = rte_pktmbuf_alloc(sa->mp);
		if (!m) continue;
		char buf[128]; int l = snprintf(buf, 128, "Hello #%d q%u", i, q);
		if (!rte_pktmbuf_append(m, l)) { rte_pktmbuf_free(m); continue; }
		rte_memcpy(rte_pktmbuf_mtod(m, char *), buf, l);
		while (!rte_eth_tx_burst(sa->pid, q, &m, 1));
		struct rte_mbuf *back = NULL;
		while (!rte_eth_rx_burst(sa->pid, q, &back, 1)) rte_delay_us(1000);
		rte_pktmbuf_free(back); rte_pktmbuf_free(m);
		printf("[Cli lc%u q%u] #%d done\n", rte_lcore_id(), q, i);
		rte_delay_us(SEND_US);
	}
	return 0;
}

int main(int argc, char **argv)
{
	if (rte_eal_init(argc, argv) < 0) { fprintf(stderr,"eal\n"); return 1; }

	unsigned int main_lcore = rte_get_main_lcore();
	unsigned int lcores[RTE_MAX_LCORE];
	unsigned int nb_lc = 0;
	for (unsigned int l = 0; l < RTE_MAX_LCORE; l++)
		if (rte_lcore_is_enabled(l) && l != main_lcore)
			lcores[nb_lc++] = l;

	if (nb_lc < 3) { fprintf(stderr,"need >=3 worker lcores\n"); return 1; }
	printf("\n===== Client (%u queues, lcores %u %u %u) =====\n\n",
		NQ, lcores[0], lcores[1], lcores[2]);

	const char *pn[] = {"master"}, *un[] = {"worker1"};
	uint32_t elt = sizeof(struct rte_mbuf) + RTE_MBUF_DEFAULT_BUF_SIZE;

	struct rte_ub_ring_handle *h = rte_ub_ring_user_init(
		pn, 1, un, 1, "region1", "ubring", elt);
	if (!h) { fprintf(stderr,"init\n"); return 1; }

	rte_ub_ring_enable_mpmc(h);

	int pid = rte_eth_ub_ring_attach("client_eth", h);
	if (pid < 0) { fprintf(stderr,"attach %d\n", pid); return 1; }

	struct rte_eth_conf c = { .rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN } };
	uint32_t tq = rte_ub_ring_get_total_queues(h);
	if (rte_eth_dev_configure(pid, tq, tq, &c) < 0 || rte_eth_dev_start(pid) < 0)
		{ fprintf(stderr,"cfg\n"); rte_eth_dev_close(pid); rte_ub_ring_fini(h); return 1; }

	/* flush worker on lcores[2] */
	g_h = h; g_nb_q = tq;
	rte_eal_remote_launch(flush_worker, NULL, lcores[2]);

	struct rte_mempool *mp = rte_pktmbuf_pool_create("local", 512, 32, 0,
		RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
	if (!mp) { g_running = 0; rte_eal_wait_lcore(lcores[2]);
		rte_eth_dev_close(pid); rte_ub_ring_fini(h); return 1; }

	/* 2 send workers: lcores[0] → q[0,1], lcores[1] → q[2,3] */
	int per = NP / 2;
	struct send_arg sa0 = { pid, 0, 1, per, mp };
	struct send_arg sa1 = { pid, 2, 3, per, mp };
	rte_eal_remote_launch(sender, &sa0, lcores[0]);
	rte_eal_remote_launch(sender, &sa1, lcores[1]);

	rte_eal_wait_lcore(lcores[0]);
	rte_eal_wait_lcore(lcores[1]);

	printf("\n[Client] done\n");
	g_running = 0; rte_eal_wait_lcore(lcores[2]);
	rte_eth_dev_stop(pid); rte_eth_dev_close(pid); rte_ub_ring_fini(h);
	rte_mempool_free(mp); rte_eal_cleanup();
	return 0;
}
