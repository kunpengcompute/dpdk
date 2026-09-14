/* SPDX-License-Identifier: BSD-3-Clause
 * Client — N send lcores + 1 flush lcore + collect 2-stage pipeline
 * (fetch + dispatch), continuous TX with per-second rate output on
 * every lcore.
 * Run: ./client --no-huge -l 1-6 --ns=2 --pktlen=500 [--pn=a,b] [--un=c,d] [--time=10]
 *      ns     = number of sender lcores (any value vs server queues:
 *               ns <= nq → contiguous queue blocks, RR within block;
 *               ns >  nq → sender s shares queue s % nq)
 *               (needs ns+3 lcores: flush + fetch + dispatch + senders)
 *      pktlen = packet payload size (<= RTE_MBUF_DEFAULT_BUF_SIZE - headroom)
 *      time   = run duration in seconds (0 = run until Ctrl+C)
 *      pn/un  = UB provider/user node names: comma-separated list or
 *               repeated --pn/--un (must match the server)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_cycles.h>
#include <rte_errno.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ethdev.h>
#include <rte_ub_ring.h>
#include <rte_eth_ub_ring.h>

#define BS 32

static volatile sig_atomic_t g_running = 1;
static struct rte_ub_ring_handle *g_h;
static uint32_t g_nb_q;
static const char **g_pn;  /* provider node names, argv order */
static int g_nb_pn;        /* number of providers */

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

/* parse "--opt=STR" BEFORE eal_init and strip it from argv (see above) */
static const char *app_opt_str(int *argc, char **argv, const char *opt,
			     const char *def)
{
	for (int i = 1; i < *argc; i++) {
		size_t l = strlen(opt);
		if (strncmp(argv[i], opt, l) == 0 && argv[i][l] == '=') {
			const char *v = argv[i] + l + 1;
			for (int j = i; j < *argc - 1; j++)
				argv[j] = argv[j + 1];
			(*argc)--;
			return v;	/* points into argv (never freed) */
		}
	}
	return def;
}

/* parse "--opt=a,b,c" (comma-separated and/or repeated --opt=) BEFORE
 * eal_init; strips EVERY occurrence from argv so EAL never sees them.
 * Fills out[] with up to maxn names (strdup'd, never freed — demo).
 * Falls back to a single default. Returns the count. */
static int app_opt_strs(int *argc, char **argv, const char *opt,
			const char **out, int maxn, const char *def)
{
	int n = 0;
	for (int i = 1; i < *argc; i++) {
		size_t l = strlen(opt);
		if (strncmp(argv[i], opt, l) == 0 && argv[i][l] == '=') {
			char *copy = strdup(argv[i] + l + 1);
			char *save = copy;
			for (char *t = strtok_r(copy, ",", &save);
			     t && n < maxn; t = strtok_r(NULL, ",", &save))
				out[n++] = strdup(t);
			free(copy);
			for (int j = i; j < *argc - 1; j++)
				argv[j] = argv[j + 1];
			(*argc)--;
			i--;	/* another --opt may sit in this slot now */
		}
	}
	if (n == 0) {
		out[0] = def;
		return 1;
	}
	return n;
}

/* join names into a comma-separated string for the startup banner */
static void join_names(char *buf, size_t sz, const char **v, int n)
{
	buf[0] = '\0';
	for (int i = 0; i < n; i++) {
		if (i)
			strncat(buf, ",", sz - strlen(buf) - 1);
		strncat(buf, v[i], sz - strlen(buf) - 1);
	}
}

/* provider node name serving global queue q ("?" if out of range) */
static const char *q_provider_name(uint32_t q)
{
	uint32_t pi = rte_ub_ring_queue_to_provider(g_h, q);
	return (pi < (uint32_t)g_nb_pn) ? g_pn[pi] : "?";
}

/* reverse-map global queue q to its index WITHIN the owning provider
 * (the order the provider exported its queues = its server worker id) */
static uint32_t q_local_index(uint32_t q)
{
	uint32_t pi = rte_ub_ring_queue_to_provider(g_h, q);
	for (uint32_t sq = 0; ; sq++) {
		uint32_t tx, rx;
		if (rte_ub_ring_get_queue_mapping(g_h, pi, sq, &tx, &rx) < 0)
			break;
		if (tx == q)
			return sq;
	}
	return (uint32_t)-1;
}

/* print this worker's TX assignment: every queue it sends into, each
 * with the queue's GLOBAL index, the owning provider node name, and
 * the queue's LOCAL index within that provider (= the server worker
 * thread serving it). all = 1 -> the worker touches every queue. */
static void log_assign(const char *role, uint32_t q0, uint32_t nq, int all)
{
	printf("[%s] lcore %u: sends into", role, rte_lcore_id());
	if (all) {
		for (uint32_t q = 0; q < g_nb_q; q++)
			printf(" queue %u (provider %s local %u)",
			       q, q_provider_name(q), q_local_index(q));
	} else {
		for (uint32_t i = 0; i < nq; i++) {
			uint32_t q = q0 + i;
			printf(" queue %u (provider %s local %u)",
			       q, q_provider_name(q), q_local_index(q));
		}
	}
	printf("\n");
}

static int flush_worker(void *arg)
{
	(void)arg;
	/* TX flush only: this demo never receives, so rx flush is skipped
	 * on the hot path (a one-shot final flush_rx still runs at exit) */
	log_assign("flush", 0, 0, 1);
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

/* collect stage 1: move buffers from the REMOTE return rings into the
 * local mid ring — all the non-cacheable remote reads stay here,
 * everything else is a local SP enqueue */
static int collect_fetch_worker(void *arg)
{
	(void)arg;
	printf("[collect_fetch] lcore %u: returns only, sends to no queue\n",
		rte_lcore_id());
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc();
	uint64_t loops = 0, work = 0, fetched = 0;
	while (g_running) {
		loops++;
		uint64_t f = rte_ub_ring_collect_fetch(g_h);
		work += f > 0;
		fetched += f;

		uint64_t now = rte_rdtsc();
		if (now - last >= hz) {
			printf("[poll collect_fetch] fetched %llu/s from provider "
				"return rings -> mid ring | loops=%llu work=%llu "
				"idle=%llu busy=%.1f%%\n",
				(unsigned long long)fetched,
				(unsigned long long)loops,
				(unsigned long long)work,
				(unsigned long long)(loops - work),
				loops ? 100.0 * (double)work /
					(double)loops : 0);
			last = now;
			loops = 0;
			work = 0;
			fetched = 0;
		}
	}
	return 0;
}

/* collect stage 2: route the mid ring's elements into the per-provider
 * local data rings — pure local work */
static int collect_dispatch_worker(void *arg)
{
	(void)arg;
	printf("[collect_dispatch] lcore %u: returns only, sends to no queue\n",
		rte_lcore_id());
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc();
	uint64_t loops = 0, work = 0, dispatched = 0;
	while (g_running) {
		loops++;
		uint64_t f = rte_ub_ring_collect_dispatch(g_h);
		work += f > 0;
		dispatched += f;

		uint64_t now = rte_rdtsc();
		if (now - last >= hz) {
			printf("[poll collect_dispatch] dispatched %llu/s from mid "
				"-> provider data rings | loops=%llu work=%llu "
				"idle=%llu busy=%.1f%%\n",
				(unsigned long long)dispatched,
				(unsigned long long)loops,
				(unsigned long long)work,
				(unsigned long long)(loops - work),
				loops ? 100.0 * (double)work /
					(double)loops : 0);
			last = now;
			loops = 0;
			work = 0;
			dispatched = 0;
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
	log_assign("sender", sa->q0, sa->nq_own, 0);

	/* send round-robin over this sender's queues forever,
	 * print the rate once per second */
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc(), window = 0;
	uint64_t loops = 0, work = 0, fails = 0;
	while (g_running) {
		for (uint32_t i = 0; i < sa->nq_own && g_running; i++) {
			uint32_t q = sa->q0 + i;
			uint16_t sent = 0;
			while (g_running && sent < nb_built) {
				uint16_t s = rte_eth_tx_burst(sa->pid, q,
					&burst[sent], nb_built - sent);
				sent += s;
				window += s;
				fails += s == 0;
				loops++;
				work += s > 0;

				uint64_t now = rte_rdtsc();
				if (now - last >= hz) {
					double secs = (double)(now - last) / hz;
					double mpps = window / secs / 1e6;
					printf("[Cli lc%u] %.2f Mpps, %.2f Mbps | "
						"fail=%llu loops=%llu work=%llu busy=%.1f%%\n",
						rte_lcore_id(), mpps,
						mpps * sa->pkt_len * 8,
						(unsigned long long)fails,
						(unsigned long long)loops,
						(unsigned long long)work,
						loops ? 100.0 * (double)work /
							(double)loops : 0);
					last = now;
					window = 0;
					loops = 0;
					work = 0;
					fails = 0;
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
	const char *pn[UB_RING_MAX_NODES], *un[UB_RING_MAX_NODES];
	int nb_pn = app_opt_strs(&argc, argv, "--pn", pn, UB_RING_MAX_NODES,
				      "master");
	int nb_un = app_opt_strs(&argc, argv, "--un", un, UB_RING_MAX_NODES,
				      "worker1");
	rte_eal_init(argc, argv);
	signal(SIGINT, on_sigint);
	signal(SIGTERM, on_sigint);

	unsigned int ml = rte_get_main_lcore();
	unsigned int lc[RTE_MAX_LCORE]; unsigned int nl = 0;
	for (unsigned int l = 0; l < RTE_MAX_LCORE; l++)
		if (rte_lcore_is_enabled(l) && l != ml) lc[nl++] = l;
	if (nl < ns + 3) {
		fprintf(stderr,
			"need %u worker lcores (flush + collect_fetch + "
			"collect_dispatch + %u senders)\n",
			ns + 3, ns);
		return 1;
	}

	uint32_t elt = sizeof(struct rte_mbuf) + RTE_MBUF_DEFAULT_BUF_SIZE;
	struct rte_ub_ring_handle *h = rte_ub_ring_user_init(
		pn, nb_pn, un, nb_un, "region1", "ubring", elt);
	int mrc = rte_ub_ring_enable_mpmc(h);
	if (mrc) {
		fprintf(stderr,
			"rte_ub_ring_enable_mpmc FAILED: rc=%d rte_errno=%d (%s)\n",
			mrc, rte_errno, rte_strerror(rte_errno));
		return 1;
	}
	printf("MPMC pipeline OK (shadow_tx/shadow_rx/collect_mid created)\n");
	int pid = rte_eth_ub_ring_attach("client_eth", h);

	struct rte_eth_conf c = { .rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN } };
	uint32_t tq = rte_ub_ring_get_total_queues(h);
	rte_eth_dev_configure(pid, tq, tq, &c);
	rte_eth_dev_start(pid);

	char pns[512] = "", uns[512] = "";
	join_names(pns, sizeof pns, pn, nb_pn);
	join_names(uns, sizeof uns, un, nb_un);
	printf("\n===== Client (%u queues, %u sender lcores, %uB pkts, %s, pn=[%s] un=[%s]) =====\n\n",
		tq, ns, pkt_len, duration ? "timed" : "Ctrl+C to stop", pns, uns);

	g_h = h; g_nb_q = tq; g_pn = pn; g_nb_pn = nb_pn;
	rte_eal_remote_launch(flush_worker, NULL, lc[0]);
	rte_eal_remote_launch(collect_fetch_worker, NULL, lc[1]);
	rte_eal_remote_launch(collect_dispatch_worker, NULL, lc[2]);

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
			rte_eal_remote_launch(sender, &sa[s], lc[3 + s]);
		}
	} else {
		/* more senders than queues: sender s shares queue s % tq */
		for (uint32_t s = 0; s < ns; s++) {
			sa[s].pid = pid;
			sa[s].q0 = s % tq;
			sa[s].nq_own = 1;
			sa[s].pkt_len = pkt_len;
			sa[s].mp = mp;
			rte_eal_remote_launch(sender, &sa[s], lc[3 + s]);
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
		rte_eal_wait_lcore(lc[3 + s]);

	/* stop the flush/collect workers FIRST: shadow rings are SC_DEQ and
	 * shm rings are SP_ENQ — a concurrent final flush from main would
	 * corrupt the rings and lose packets */
	g_running = 0;
	rte_eal_wait_lcore(lc[0]);
	rte_eal_wait_lcore(lc[1]);
	rte_eal_wait_lcore(lc[2]);

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
