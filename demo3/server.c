/* SPDX-License-Identifier: BSD-3-Clause
 * Server — demo3: pure ring-index transport, no data payload, no mbuf.
 *
 * Counterpart of client.c. Each worker SC-dequeues raw 8B elements
 * {pi<<56 | offset} off its shm rx ring, decodes the offset back to the
 * buffer pointer (ub_ring_buf_ptr — the data region was never written,
 * only the 128B ownership header exists), and hands the pointer to a
 * local "to-return" ring (ret_stage, MP/SC). The drain lcore is the
 * ONLY thread that calls return_burst — the shm return ring is SPSC, so
 * all worker returns funnel through ret_stage to keep a single SP
 * producer. No ethdev, no mbuf, no mempool.
 *
 * Run: ./server --no-huge -l 1-6 --nq=4 [--pn=a,b] [--un=c,d] [--time=10]
 *      nq   = number of queues/workers (needs nq+1 lcores: workers+drain)
 *      pn/un = UB provider/user node names (must match the client)
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
#include <rte_errno.h>
#include <rte_pause.h>
#include <rte_ring.h>
#include <rte_ub_ring.h>

#define BS 32

static volatile sig_atomic_t g_running = 1;
static struct rte_ub_ring_handle *g_h;
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

/* worker w owns queue w (SC dequeue on the shm rx ring). It decodes each
 * raw element back to the owning buffer pointer and MP-enqueues it into
 * the shared ret_stage ring; the drain lcore is the single return_burst
 * producer (the shm return ring is SPSC). Enqueue returns are always
 * consumed — a full ret_stage just backs the worker off, nothing drops. */
struct targ { uint32_t qid; struct rte_ring *ret_stage; };

static int worker(void *a)
{
	struct targ *ta = a;
	void *elems[BS], *bufs[BS];
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc();
	uint64_t window = 0, drops = 0;
	uint64_t loops = 0, work = 0;

	while (g_running) {
		uint16_t n = rte_ub_ring_recv_burst(g_h, ta->qid, elems, BS);
		loops++;
		if (n == 0)
			continue;
		++work;

		uint16_t nb = 0;
		for (uint16_t i = 0; i < n; i++) {
			uint32_t pi = ub_ring_elem_get_node(elems[i]);
			uint64_t off = ub_ring_elem_get_offset(elems[i]);
			void *area = rte_ub_ring_get_data_buf_area(g_h, pi);
			if (!area) {	/* unresolved peer — cannot happen in demo */
				drops++;
				continue;
			}
			bufs[nb++] = ub_ring_buf_ptr(area, off);
		}
		/* backpressure loop: ret_stage is drained by the drain lcore,
		 * so a full ring clears on its own — keep retrying, never drop */
		uint16_t en = 0;
		while (en < nb && g_running) {
			uint16_t s = rte_ring_mp_enqueue_burst(ta->ret_stage,
				&bufs[en], nb - en, NULL);
			en += s;
			if (s == 0)
				rte_pause();
		}
		window += en;

		uint64_t now = rte_rdtsc();
		if (now - last >= hz) {
			double secs = (double)(now - last) / hz;
			double mpps = window / secs / 1e6;
			printf("[Srv q%u] %.2f Mpps (recv) | drops=%llu "
				"loops=%llu work=%llu busy=%.1f%%\n",
				ta->qid, mpps, (unsigned long long)drops,
				(unsigned long long)loops,
				(unsigned long long)work,
				loops ? 100.0 * (double)work /
					(double)loops : 0);
			last = now;
			window = 0;
			drops = 0;
			loops = 0;
			work = 0;
		}
	}
	return 0;
}

/* the ONLY return_burst caller: SC-dequeues a batch of returned buffer
 * pointers off ret_stage and hands them to the shm return ring with one
 * bulk SP enqueue. return_burst reads each buffer's 128B ownership
 * header (written by the client's user_init) and packs the element —
 * the data region itself is never touched. */
struct darg { struct rte_ring *ret_stage; };

static int drain_worker(void *a)
{
	struct darg *da = a;
	void *bufs[512];
	uint64_t hz = rte_get_tsc_hz(), last = rte_rdtsc();
	uint64_t loops = 0, work = 0, returned = 0;
	while (g_running) {
		uint16_t n = rte_ring_sc_dequeue_burst(da->ret_stage,
			bufs, 512, NULL);
		loops++;
		if (n == 0)
			continue;
		++work;
		returned += n;
		rte_ub_ring_return_burst(g_h, bufs, n);

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
	printf("\n===== Server (demo3: %u queues, %u workers + 1 drain, %s, "
		"pn=[%s] un=[%s]) =====\n\n",
		g_nq, g_nq, duration ? "timed" : "Ctrl+C to stop", pns, uns);

	g_h = rte_ub_ring_provider_init(
		pn, nb_pn, un, nb_un, "region1", "ubring",
		4UL * 1024 * 1024 * 1024, g_nq, 1024 * 1024);
	if (!g_h) {
		fprintf(stderr, "rte_ub_ring_provider_init FAILED\n");
		return 1;
	}

	/* local to-return ring: every worker MP-enqueues, the drain lcore
	 * SC-dequeues. Depth = the provider's buffer count, so the whole
	 * in-flight set fits (mx is a power of 2 — valid ring depth). */
	uint32_t mx = rte_ub_ring_get_total_bufs(g_h);
	if (!mx) {
		fprintf(stderr, "no data ring resolved (client not up?)\n");
		return 1;
	}
	struct rte_ring *ret_stage = rte_ring_create("ret_stage", mx,
		rte_socket_id(), RING_F_SC_DEQ);
	if (!ret_stage) {
		fprintf(stderr, "ret_stage create failed: rte_errno=%d (%s)\n",
			rte_errno, rte_strerror(rte_errno));
		return 1;
	}
	printf("ret_stage ring OK (%u bufs, elt=%uB)\n", mx,
		rte_ub_ring_get_data_elt_size(g_h, rte_ub_ring_queue_to_provider(g_h, 0)));

	struct targ ta[UB_RING_MAX_QUEUES];
	struct darg dg = { ret_stage };
	rte_eal_remote_launch(drain_worker, &dg, lc[0]);
	for (uint32_t q = 0; q < g_nq; q++) {
		ta[q] = (struct targ){ q, ret_stage };
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

	/* drain whatever the workers left in ret_stage; the client may have
	 * stopped collecting, in which case the shm return ring tail is lost
	 * at fini — same residual semantics as demo2, run-time unaffected */
	void *bufs[512];
	uint64_t leftover = 0;
	uint16_t n;
	while ((n = rte_ring_sc_dequeue_burst(ret_stage, bufs, 512, NULL)) > 0) {
		leftover += n;
		rte_ub_ring_return_burst(g_h, bufs, n);
	}
	if (leftover)
		printf("teardown: %llu buffers returned from ret_stage tail\n",
			(unsigned long long)leftover);

	rte_ub_ring_fini(g_h);
	rte_eal_cleanup();
	return 0;
}
