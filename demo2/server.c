/* SPDX-License-Identifier: BSD-3-Clause
 * Server — N workers (one per queue) + 1 drain lcore, continuous RX
 * with per-second rate output on every worker.
 * Run: ./server --no-huge -l 1-6 --nq=4 [--pn=a,b] [--un=c,d] [--time=10]
 *      nq   = number of queues/workers (needs nq+1 lcores: workers+drain)
 *      pn/un = UB provider/user node names: comma-separated list or
 *              repeated --pn/--un (must match the client)
 *      time = run duration in seconds (0 = run until Ctrl+C)
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
#include <rte_ethdev.h>
#include <rte_ub_ring.h>
#include <rte_eth_ub_ring.h>

#define BS 32

static volatile sig_atomic_t g_running = 1;
static uint32_t g_nq;

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

struct targ { int pid; uint32_t qid; };

/* drain the dummy pool back to the providers' return rings on its own
 * lcore: workers free into per-lcore caches and only the overflow
 * reaches the ring, so this loop is what keeps buffers flowing back
 * to the client */
static int drain_worker(void *a)
{
	int pid = *(int *)a;
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc();
	uint64_t loops = 0, work = 0, returned = 0;
	while (g_running) {
		uint64_t r = rte_eth_ub_ring_drain(pid);
		loops++;
		work += r > 0;
		returned += r;

		uint64_t now = rte_rdtsc();
		if (now - last >= hz) {
			printf("[poll drain] returned %llu/s to own return ring "
				"(client collect pulls it) | loops=%llu work=%llu "
				"idle=%llu busy=%.1f%%\n",
				(unsigned long long)returned,
				(unsigned long long)loops,
				(unsigned long long)work,
				(unsigned long long)(loops - work),
				loops ? 100.0 * (double)work /
					(double)loops : 0);
			last = now;
			loops = 0;
			work = 0;
			returned = 0;
		}
	}
	return 0;
}

static int worker(void *a)
{
	struct targ *ta = a;
	struct rte_mbuf *pkts[BS];
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc();
	uint64_t window = 0, win_bytes = 0;
	uint64_t loops = 0, work = 0;

	/* receive forever, print the rate once per second
	 * (bandwidth uses the actual pkt_len of each packet) */
	while (g_running) {
		uint16_t n = rte_eth_rx_burst(ta->pid, ta->qid, pkts, BS);
		loops++;
		if (n == 0) {
			continue;
		}
		++work;
		for (uint16_t i = 0; i < n; i++) {
			win_bytes += pkts[i]->pkt_len;
			rte_pktmbuf_free(pkts[i]);
			// rte_mbuf_raw_free(pkts[i]);
		}
		window += n;

		uint64_t now = rte_rdtsc();
		if (now - last >= hz) {
			double secs = (double)(now - last) / hz;
			double mpps = window / secs / 1e6;
			printf("[Srv q%u] %.2f Mpps (freed %llu/s to dummy_pool), "
				"%.2f Mbps | loops=%llu work=%llu busy=%.1f%%\n",
				ta->qid, mpps, (unsigned long long)window,
				win_bytes / secs * 8 / 1e6,
				(unsigned long long)loops,
				(unsigned long long)work,
				loops ? 100.0 * (double)work /
					(double)loops : 0);
			last = now;
			window = 0;
			win_bytes = 0;
			loops = 0;
			work = 0;
		}
	}
	return 0;
}

int main(int argc, char **argv)
{
	g_nq = app_opt_uint(&argc, argv, "--nq", 4);
	if (g_nq < 1 || g_nq > UB_RING_MAX_QUEUES) {
		fprintf(stderr, "bad --nq\n");
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
	if (g_nq + 1 > nl) {
		fprintf(stderr, "need %u worker lcores (have %u)\n",
			g_nq + 1, nl);
		return 1;
	}

	char pns[512] = "", uns[512] = "";
	join_names(pns, sizeof pns, pn, nb_pn);
	join_names(uns, sizeof uns, un, nb_un);
	printf("\n===== Server (%u queues, %u workers + 1 drain, %s, pn=[%s] un=[%s]) =====\n\n",
		g_nq, g_nq, duration ? "timed" : "Ctrl+C to stop", pns, uns);

	struct rte_ub_ring_handle *h = rte_ub_ring_provider_init(
		pn, nb_pn, un, nb_un, "region1", "ubring", 4UL*1024*1024*1024, g_nq, 1024*1024);
	int pid = rte_eth_ub_ring_attach("server_eth", h);
	struct rte_eth_conf c = { .rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN } };
	rte_eth_dev_configure(pid, g_nq, g_nq, &c);
	rte_eth_dev_start(pid);

	struct targ ta[UB_RING_MAX_QUEUES];
	int dpid = pid;
	rte_eal_remote_launch(drain_worker, &dpid, lc[0]);
	for (uint32_t q = 0; q < g_nq; q++) {
		ta[q] = (struct targ){ pid, q };
		rte_eal_remote_launch(worker, &ta[q], lc[1 + q]);
	}

	/* run until the timer expires or Ctrl+C sets g_running = 0 */
	if (duration > 0) {
		uint64_t end = rte_rdtsc() + (uint64_t)duration * rte_get_tsc_hz();
		while (g_running && rte_rdtsc() < end)
			rte_delay_us_sleep(200000);
		g_running = 0;
	}
	for (uint32_t q = 0; q < g_nq; q++)
		rte_eal_wait_lcore(lc[1 + q]);
	rte_eal_wait_lcore(lc[0]);

	rte_eth_dev_stop(pid); rte_eth_dev_close(pid); rte_ub_ring_fini(h);
	rte_eal_cleanup();
	return 0;
}
