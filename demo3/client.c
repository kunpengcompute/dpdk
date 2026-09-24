/* SPDX-License-Identifier: BSD-3-Clause
 * Client — demo3: pure ring-index transport, no data payload, no mbuf.
 *
 * The only thing that crosses the wire is the 8B ring element
 * {pi<<56 | offset}: a sender pulls a free buffer offset off its local
 * data ring (alloc_buf), packs it, and pushes the element through
 * shadow_tx → shm tx_ring. The server decodes the element back to the
 * buffer pointer (nothing was ever written into the buffer) and
 * returns it; the element flows back through return ring → mid ring →
 * local data ring and is reused. Zero data-region writes, zero memcpy.
 *
 * Run: ./client --no-huge -l 1-6 --ns=2 [--f=1] [--elt=128] [--pn=a,b] [--un=c,d] [--time=10]
 *      ns    = sender lcore count (queue split identical to demo2:
 *              ns <= tq → contiguous blocks + RR; ns > tq → share q = s % tq)
 *      f     = flush lcore count (each queue owned by exactly one flush
 *              thread, so the shm tx ring keeps its SP/SC contract)
 *      elt   = data element stride handed to user_init. Only the 128B
 *              ownership header is ever touched, so any small value
 *              works. elt=128 → aligned to 256B → a 4G shm holds ~8M
 *              buffers (mx) → shadow_tx per queue = 2×mx×8B ≈ 128MB
 *              local memory; lower it (or pass a bigger --elt) if the
 *              local hugepages budget is tight.
 *      pn/un = UB provider/user node names (must match the server)
 *      time  = run duration in seconds (0 = run until Ctrl+C)
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
#include <rte_ub_ring.h>

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

/* parse "--opt=N" (and "-opt=N") BEFORE eal_init and strip it from argv,
 * so EAL's getopt never sees it — EAL rejects unknown single-dash
 * options ("invalid option -- 'f'") before it even reaches the app */
static uint32_t app_opt_uint(int *argc, char **argv, const char *opt,
			     uint32_t def)
{
	const char *name = opt + 2;	/* "--f" -> "f" */
	size_t nl = strlen(name);
	for (int i = 1; i < *argc; i++) {
		const char *p = argv[i];
		if (p[0] != '-' || p[1] == '\0')
			continue;
		const char *v = p + ((p[1] == '-') ? 2 : 1);
		if (strncmp(v, name, nl) != 0 || v[nl] != '=')
			continue;
		char *end = NULL;
		unsigned long val = strtoul(v + nl + 1, &end, 0);
		for (int j = i; j < *argc - 1; j++)
			argv[j] = argv[j + 1];
		(*argc)--;
		if (end && *end == '\0')
			return (uint32_t)val;
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

/* flush thread assignment: owns queues [q0, q0+nq_own). Each queue is
 * owned by EXACTLY ONE flush thread — shadow_tx[q] stays SC_DEQ and the
 * shm tx_ring[q] stays SP_ENQ, so the per-queue SP/SC contract holds */
struct farg { uint32_t q0, nq_own; };

static int flush_worker(void *arg)
{
	struct farg *fa = arg;
	log_assign("flush", fa->q0, fa->nq_own, 0);
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc();
	uint64_t loops = 0, work = 0;
	while (g_running) {
		uint64_t moved = 0;
		for (uint32_t i = 0; i < fa->nq_own; i++)
			moved += rte_ub_ring_flush_tx(g_h, fa->q0 + i);
		loops++;
		work += moved > 0;

		uint64_t now = rte_rdtsc();
		if (now - last >= hz) {
			printf("[poll flush %u] loops=%llu work=%llu "
				"idle=%llu busy=%.1f%%\n",
				rte_lcore_id(),
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

/* collect stage 1: move elements from the REMOTE return rings into the
 * local mid ring — all the non-cacheable remote reads stay here */
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

struct sarg { uint32_t q0, nq_own; };

static int sender(void *a)
{
	struct sarg *sa = a;
	log_assign("sender", sa->q0, sa->nq_own, 0);

	/* send round-robin over this sender's queues forever. Per elem:
	 * alloc_buf pops a free {pi,off} from the local data ring (via the
	 * per-lcore cache) → pack back to the 8B element → mp_send_burst
	 * into shadow_tx. On shadow-full (fail-fast returns 0) the element
	 * goes back to this lcore's alloc cache — natural backpressure,
	 * nothing is lost. The shm data region is NEVER touched. */
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc(), window = 0;
	uint64_t loops = 0, work = 0, alloc_fail = 0, send_fail = 0;
	while (g_running) {
		uint64_t sent = 0;
		for (uint32_t i = 0; i < sa->nq_own && g_running; i++) {
			uint32_t q = sa->q0 + i;
			uint32_t pi = rte_ub_ring_queue_to_provider(g_h, q);
			void *buf = rte_ub_ring_alloc_buf(g_h, pi);
			if (!buf) {
				alloc_fail++;
				continue;
			}
			void *area = rte_ub_ring_get_data_buf_area(g_h, pi);
			void *elem = ub_ring_elem_pack((uint8_t)pi,
				ub_ring_buf_off(area, buf));
			uint16_t s = rte_ub_ring_mp_send_burst(g_h, q, &elem, 1);
			if (s == 0) {
				send_fail++;
				rte_ub_ring_free_elem_cached(g_h, elem);
				continue;
			}
			sent++;
		}
		loops++;
		work += sent > 0;
		window += sent;

		uint64_t now = rte_rdtsc();
		if (now - last >= hz) {
			double secs = (double)(now - last) / hz;
			double mpps = window / secs / 1e6;
			printf("[Cli lc%u] %.2f Mpps (elems) | "
				"alloc_fail=%llu send_fail=%llu loops=%llu "
				"work=%llu busy=%.1f%%\n",
				rte_lcore_id(), mpps,
				(unsigned long long)alloc_fail,
				(unsigned long long)send_fail,
				(unsigned long long)loops,
				(unsigned long long)work,
				loops ? 100.0 * (double)work /
					(double)loops : 0);
			last = now;
			window = 0;
			loops = 0;
			work = 0;
			alloc_fail = 0;
			send_fail = 0;
		}
	}
	return 0;
}

int main(int argc, char **argv)
{
	uint32_t ns = app_opt_uint(&argc, argv, "--ns", 2);
	if (ns < 1) { fprintf(stderr, "bad --ns\n"); return 1; }
	uint32_t nf = app_opt_uint(&argc, argv, "--f", 1);
	if (nf < 1) { fprintf(stderr, "bad --f\n"); return 1; }
	uint32_t elt = app_opt_uint(&argc, argv, "--elt", 128);
	if (elt < 1) { fprintf(stderr, "bad --elt\n"); return 1; }
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
	if (nl < ns + 2 + nf) {
		fprintf(stderr,
			"need %u worker lcores (%u flush + collect_fetch + "
			"collect_dispatch + %u senders)\n",
			ns + 2 + nf, nf, ns);
		return 1;
	}

	struct rte_ub_ring_handle *h = rte_ub_ring_user_init(
		pn, nb_pn, un, nb_un, "region1", "ubring", elt);
	if (!h) {
		fprintf(stderr, "rte_ub_ring_user_init FAILED: rte_errno=%d (%s)\n",
			rte_errno, rte_strerror(rte_errno));
		return 1;
	}
	int mrc = rte_ub_ring_enable_mpmc(h);
	if (mrc) {
		fprintf(stderr,
			"rte_ub_ring_enable_mpmc FAILED: rc=%d rte_errno=%d (%s)\n",
			mrc, rte_errno, rte_strerror(rte_errno));
		return 1;
	}
	printf("MPMC pipeline OK (shadow_tx/shadow_rx/collect_mid created)\n");

	uint32_t tq = rte_ub_ring_get_total_queues(h);
	if (tq == 0) {
		fprintf(stderr, "no provider queues resolved (server up?)\n");
		rte_ub_ring_fini(h);
		return 1;
	}
	char pns[512] = "", uns[512] = "";
	join_names(pns, sizeof pns, pn, nb_pn);
	join_names(uns, sizeof uns, un, nb_un);
	printf("\n===== Client (demo3: %u queues, %u sender lcores, %u flush "
		"lcores, elt=%u (aligned %uB), %u bufs, %s, pn=[%s] un=[%s]) "
		"=====\n\n",
		tq, ns, nf, elt,
		rte_ub_ring_get_data_elt_size(h, rte_ub_ring_queue_to_provider(h, 0)),
		rte_ub_ring_get_total_bufs(h),
		duration ? "timed" : "Ctrl+C to stop", pns, uns);

	g_h = h; g_nb_q = tq; g_pn = pn; g_nb_pn = nb_pn;

	/* flush threads: queues split evenly, each queue owned by exactly
	 * one flush thread (per-queue SP/SC preserved — see farg above) */
	static struct farg fg[RTE_MAX_LCORE];
	uint32_t fbase = tq / nf, frem = tq % nf;
	for (uint32_t i = 0; i < nf; i++) {
		fg[i].q0 = (i < tq) ? i * fbase + RTE_MIN(i, frem) : 0;
		fg[i].nq_own = (i < tq) ? fbase + (i < frem) : 0;
		rte_eal_remote_launch(flush_worker, &fg[i], lc[i]);
	}
	rte_eal_remote_launch(collect_fetch_worker, NULL, lc[nf]);
	rte_eal_remote_launch(collect_dispatch_worker, NULL, lc[nf + 1]);

	struct sarg sa[RTE_MAX_LCORE];
	if (ns <= tq) {
		/* contiguous even split: first `rem` senders take one
		 * extra queue, RR over the block */
		uint32_t base = tq / ns, rem = tq % ns;
		for (uint32_t s = 0; s < ns; s++) {
			sa[s].q0 = s * base + RTE_MIN(s, rem);
			sa[s].nq_own = base + (s < rem);
			rte_eal_remote_launch(sender, &sa[s], lc[nf + 2 + s]);
		}
	} else {
		/* more senders than queues: sender s shares queue s % tq */
		for (uint32_t s = 0; s < ns; s++) {
			sa[s].q0 = s % tq;
			sa[s].nq_own = 1;
			rte_eal_remote_launch(sender, &sa[s], lc[nf + 2 + s]);
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
		rte_eal_wait_lcore(lc[nf + 2 + s]);

	/* stop the flush/collect workers FIRST: shadow rings are SC_DEQ and
	 * shm rings are SP_ENQ — a concurrent final flush from main would
	 * corrupt the rings and lose elements */
	g_running = 0;
	for (uint32_t i = 0; i < nf + 2; i++)
		rte_eal_wait_lcore(lc[i]);

	/* final flush alone (bounded: gives up if the peer stopped) */
	rte_ub_ring_flush_tx_all(g_h);
	for (uint32_t q = 0; q < g_nb_q; q++)
		rte_ub_ring_flush_rx(g_h, q);
	rte_ub_ring_collect(g_h);

	rte_ub_ring_fini(h);
	rte_eal_cleanup();
	return 0;
}
