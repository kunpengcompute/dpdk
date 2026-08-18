/* SPDX-License-Identifier: BSD-3-Clause
 * Client — N send lcores + 1 flush lcore + 1 collect lcore, continuous
 * TX with per-second rate output on every lcore.
 * Run: ./client --no-huge -l 1-5 --ns=2 --pktlen=500 [--time=10]
 *      ns     = number of sender lcores (any value vs server queues:
 *               ns <= nq → contiguous queue blocks, RR within block;
 *               ns >  nq → sender s shares queue s % nq)
 *               (needs ns+2 lcores: flush + collect + senders)
 *      pktlen = packet payload size (<= RTE_MBUF_DEFAULT_BUF_SIZE - headroom)
 *      time   = run duration in seconds (0 = run until Ctrl+C)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_cycles.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ethdev.h>
#include <rte_ub_ring.h>
#include <rte_eth_ub_ring.h>

#define BS 32

static volatile sig_atomic_t g_running = 1;
static struct rte_ub_ring_handle *g_h;
static uint32_t g_nb_q;

static void on_sigint(int sig)
{
	(void)sig;
	g_running = 0;
}

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

static int flush_worker(void *arg)
{
	(void)arg;
	/* TX flush only: this demo never receives, so rx flush is skipped
	 * on the hot path (a one-shot final flush_rx still runs at exit) */
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc();
	uint64_t loops = 0, work = 0;
	while (g_running) {
		uint64_t moved = 0;
		for (uint32_t q = 0; q < g_nb_q; q++)
			moved += rte_ub_ring_flush_tx(g_h, q);
		loops++;
		work += moved > 0;

		uint64_t now = rte_rdtsc();
		if (now - last >= hz) {
			printf("[poll flush] loops=%llu work=%llu "
				"idle=%llu busy=%.1f%%\n",
				(unsigned long long)loops,
				(unsigned long long)work,
				(unsigned long long)(loops - work),
				loops ? 100.0 * (double)work /
					(double)loops : 0);
			last = now;
			loops = 0;
			work = 0;
		}
	}
	return 0;
}

/* move buffers the server returned back to the local data rings on its
 * own lcore, so the senders' alloc_buf refills don't wait for a slow
 * flush loop that also moves tx/rx traffic */
static int collect_worker(void *arg)
{
	(void)arg;
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc();
	uint64_t loops = 0, work = 0;
	while (g_running) {
		loops++;
		work += rte_ub_ring_collect(g_h) > 0;

		uint64_t now = rte_rdtsc();
		if (now - last >= hz) {
			printf("[poll collect] loops=%llu work=%llu "
				"idle=%llu busy=%.1f%%\n",
				(unsigned long long)loops,
				(unsigned long long)work,
				(unsigned long long)(loops - work),
				loops ? 100.0 * (double)work /
					(double)loops : 0);
			last = now;
			loops = 0;
			work = 0;
		}
	}
	return 0;
}

struct sarg { int pid; uint32_t q0, nq_own, pkt_len;
	struct rte_mempool *mp; };

static int sender(void *a)
{
	struct sarg *sa = a;
	struct rte_mbuf *burst[BS];
	int nb_built = 0;

	/* build one batch of packets, reused for the whole run */
	for (int i = 0; i < BS; i++) {
		struct rte_mbuf *m = rte_pktmbuf_alloc(sa->mp);
		if (!m) break;
		rte_pktmbuf_append(m, sa->pkt_len);
		memset(rte_pktmbuf_mtod(m, char *), 0x55, sa->pkt_len);
		burst[i] = m;
		nb_built++;
	}
	if (nb_built == 0) {
		printf("[Cli lc%u] pool exhausted, nothing to send\n",
			rte_lcore_id());
		return 0;
	}

	/* send round-robin over this sender's queues forever,
	 * print the rate once per second */
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc(), window = 0;
	uint64_t loops = 0, work = 0;
	while (g_running) {
		for (uint32_t i = 0; i < sa->nq_own && g_running; i++) {
			uint32_t q = sa->q0 + i;
			uint16_t sent = 0;
			while (g_running && sent < nb_built) {
				uint16_t s = rte_eth_tx_burst(sa->pid, q,
					&burst[sent], nb_built - sent);
				sent += s;
				window += s;
				loops++;
				work += s > 0;

				uint64_t now = rte_rdtsc();
				if (now - last >= hz) {
					double secs = (double)(now - last) / hz;
					double mpps = window / secs / 1e6;
					printf("[Cli lc%u] %.2f Mpps, %.2f Mbps | "
						"loops=%llu work=%llu busy=%.1f%%\n",
						rte_lcore_id(), mpps,
						mpps * sa->pkt_len * 8,
						(unsigned long long)loops,
						(unsigned long long)work,
						loops ? 100.0 * (double)work /
							(double)loops : 0);
					last = now;
					window = 0;
					loops = 0;
					work = 0;
				}
			}
		}
	}

	for (int i = 0; i < nb_built; i++)
		rte_pktmbuf_free(burst[i]);
	return 0;
}

int main(int argc, char **argv)
{
	uint32_t ns = app_opt_uint(&argc, argv, "--ns", 2);
	if (ns < 1) { fprintf(stderr, "bad --ns\n"); return 1; }
	uint32_t pkt_len = app_opt_uint(&argc, argv, "--pktlen", 500);
	if (pkt_len < 1 ||
	    pkt_len > RTE_MBUF_DEFAULT_BUF_SIZE - RTE_PKTMBUF_HEADROOM) {
		fprintf(stderr, "bad --pktlen (1..%u)\n",
			RTE_MBUF_DEFAULT_BUF_SIZE - RTE_PKTMBUF_HEADROOM);
		return 1;
	}
	uint32_t duration = app_opt_uint(&argc, argv, "--time", 0);
	rte_eal_init(argc, argv);
	signal(SIGINT, on_sigint);
	signal(SIGTERM, on_sigint);

	unsigned int ml = rte_get_main_lcore();
	unsigned int lc[RTE_MAX_LCORE]; unsigned int nl = 0;
	for (unsigned int l = 0; l < RTE_MAX_LCORE; l++)
		if (rte_lcore_is_enabled(l) && l != ml) lc[nl++] = l;
	if (nl < ns + 2) {
		fprintf(stderr,
			"need %u worker lcores (flush + collect + %u senders)\n",
			ns + 2, ns);
		return 1;
	}

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

	printf("\n===== Client (%u queues, %u sender lcores, %uB pkts, %s) =====\n\n",
		tq, ns, pkt_len, duration ? "timed" : "Ctrl+C to stop");

	g_h = h; g_nb_q = tq;
	rte_eal_remote_launch(flush_worker, NULL, lc[0]);
	rte_eal_remote_launch(collect_worker, NULL, lc[1]);

	/* each sender holds BS mbufs for the whole run (TX ownership
	 * transferred, local mbufs never returned) → pool = ns * BS.
	 * cache_size=0: senders allocate the whole batch once, and a
	 * per-lcore cache would refill cache_size+1 objects and strand
	 * them, starving the other senders */
	uint32_t pool_n = ns * BS;
	struct rte_mempool *mp = rte_pktmbuf_pool_create("tx", pool_n,
		0, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
	if (!mp) { fprintf(stderr, "pool create fail\n"); return 1; }

	struct sarg sa[RTE_MAX_LCORE];
	if (ns <= tq) {
		/* contiguous even split: first `rem` senders take one
		 * extra queue, RR over the block */
		uint32_t base = tq / ns, rem = tq % ns;
		for (uint32_t s = 0; s < ns; s++) {
			sa[s].pid = pid;
			sa[s].q0 = s * base + RTE_MIN(s, rem);
			sa[s].nq_own = base + (s < rem);
			sa[s].pkt_len = pkt_len;
			sa[s].mp = mp;
			rte_eal_remote_launch(sender, &sa[s], lc[2 + s]);
		}
	} else {
		/* more senders than queues: sender s shares queue s % tq */
		for (uint32_t s = 0; s < ns; s++) {
			sa[s].pid = pid;
			sa[s].q0 = s % tq;
			sa[s].nq_own = 1;
			sa[s].pkt_len = pkt_len;
			sa[s].mp = mp;
			rte_eal_remote_launch(sender, &sa[s], lc[2 + s]);
		}
	}

	/* run until the timer expires or Ctrl+C sets g_running = 0 */
	if (duration > 0) {
		uint64_t end = rte_rdtsc() + (uint64_t)duration * rte_get_tsc_hz();
		while (g_running && rte_rdtsc() < end)
			rte_delay_us_sleep(200000);
		g_running = 0;
	}
	for (uint32_t s = 0; s < ns; s++)
		rte_eal_wait_lcore(lc[2 + s]);

	/* stop the flush/collect workers FIRST: shadow rings are SC_DEQ and
	 * shm rings are SP_ENQ — a concurrent final flush from main would
	 * corrupt the rings and lose packets */
	g_running = 0;
	rte_eal_wait_lcore(lc[0]);
	rte_eal_wait_lcore(lc[1]);

	/* final flush alone (bounded: gives up if the peer stopped) */
	rte_ub_ring_flush_tx_all(g_h);
	for (uint32_t q = 0; q < g_nb_q; q++)
		rte_ub_ring_flush_rx(g_h, q);
	rte_ub_ring_collect(g_h);

	rte_eth_dev_stop(pid); rte_eth_dev_close(pid); rte_ub_ring_fini(h);
	rte_mempool_free(mp);
	rte_eal_cleanup();
	return 0;
}
