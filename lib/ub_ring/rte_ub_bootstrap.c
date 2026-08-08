/* SPDX-License-Identifier: BSD-3-Clause */

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/mman.h>

#include <ubs_mem.h>

#include <rte_log.h>

#include "rte_ub_bootstrap.h"

RTE_LOG_REGISTER_DEFAULT(ub_bootstrap_logtype, NOTICE);
#define RTE_LOGTYPE_UB_BOOTSTRAP ub_bootstrap_logtype
#define BS_LOG(level, ...) \
	RTE_LOG_LINE_PREFIX(level, UB_BOOTSTRAP, "%s(): ", __func__, __VA_ARGS__)

#define BS_POLL_INTERVAL_US  50000
#define BS_POLL_TIMEOUT_S    60

static struct bootstrap *g_bs;

static const char *
bs_strerr(int rc)
{
	switch (rc) {
	case UBSM_OK: return "OK";
	case UBSM_ERR_ALREADY_EXIST: return "ALREADY_EXIST";
	case UBSM_ERR_PARAM_INVALID: return "PARAM_INVALID";
	case UBSM_ERR_NOPERM: return "NOPERM";
	case UBSM_ERR_MEMORY: return "MEMORY";
	default: return "UNKNOWN";
	}
}

int
ub_bootstrap_init_or_map(const char *region)
{
	int rc;
	void *p;

	size_t bs_sz = RTE_ALIGN(sizeof(struct bootstrap), 4UL * 1024 * 1024);
	uint64_t flags = UBSM_FLAG_MEM_ANONYMOUS | UBSM_FLAG_WITH_LOCK;
	rc = ubsmem_shmem_allocate(region, BOOTSTRAP_SHM_NAME,
				   bs_sz, 0600, flags);
	if (rc == UBSM_OK) {
		BS_LOG(INFO, "allocating bootstrap (first node)...");
		p = NULL;
		rc = ubsmem_shmem_map(NULL, bs_sz,
				      PROT_READ | PROT_WRITE, MAP_SHARED,
				      BOOTSTRAP_SHM_NAME, 0, &p);
		if (rc != UBSM_OK || !p) {
			BS_LOG(ERR, "map after allocate failed: %s(%d)",
			       bs_strerr(rc), rc);
			return -1;
		}
		g_bs = p;
		memset(g_bs, 0, sizeof(*g_bs));
		g_bs->hdr.magic      = BOOTSTRAP_MAGIC;
		g_bs->hdr.generation = 1;
		BS_LOG(INFO, "bootstrap created, waiting for peers");
		return 0;
	}

	if (rc == UBSM_ERR_ALREADY_EXIST) {
		BS_LOG(INFO, "bootstrap exists, mapping...");
		p = NULL;
		rc = ubsmem_shmem_map(NULL, bs_sz,
				      PROT_READ | PROT_WRITE, MAP_SHARED,
				      BOOTSTRAP_SHM_NAME, 0, &p);
		if (rc != UBSM_OK || !p) {
			BS_LOG(ERR, "map existing bootstrap failed: %s(%d)",
			       bs_strerr(rc), rc);
			return -1;
		}
		g_bs = p;

		/* wait for the first node to set BS_READY */
		for (int t = 0; t < BS_POLL_TIMEOUT_S * 20; t++) {
			ubsmem_shmem_read_lock(BOOTSTRAP_SHM_NAME);
			int ready = (g_bs->hdr.magic == BOOTSTRAP_MAGIC &&
				     g_bs->hdr.state == BS_READY);
			ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);
			if (ready)
				break;
			usleep(BS_POLL_INTERVAL_US);
		}

		ubsmem_shmem_read_lock(BOOTSTRAP_SHM_NAME);
		int st = g_bs->hdr.state;
		ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);
		if (st != BS_READY) {
			BS_LOG(ERR, "timeout waiting for bootstrap READY "
			       "(state=%d)", st);
			return -1;
		}

		BS_LOG(INFO, "bootstrap mapped, generation=%" PRIu64,
		       g_bs->hdr.generation);
		return 0;
	}

	BS_LOG(ERR, "shmem_allocate bootstrap failed: %s(%d)",
	       bs_strerr(rc), rc);
	return -1;
}

int
ub_bootstrap_register_shm(const char *name, const char *owner_name,
			  uint64_t size)
{
	if (!g_bs)
		return -1;

	ubsmem_shmem_write_lock(BOOTSTRAP_SHM_NAME);

	for (uint32_t i = 0; i < g_bs->hdr.nb_shms; i++) {
		if (strcmp(g_bs->shms[i].name, name) == 0) {
			g_bs->shms[i].state = SHM_READY;
			ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);
			BS_LOG(INFO, "shm '%s' state updated to READY", name);
			return 0;
		}
	}

	if (g_bs->hdr.nb_shms >= BOOTSTRAP_MAX_SHMS) {
		BS_LOG(ERR, "shm registry full (%u max)", BOOTSTRAP_MAX_SHMS);
		ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);
		return -1;
	}

	uint32_t n = g_bs->hdr.nb_shms++;
	strncpy(g_bs->shms[n].name, name,
		sizeof(g_bs->shms[n].name) - 1);
	strncpy(g_bs->shms[n].owner_name, owner_name,
		sizeof(g_bs->shms[n].owner_name) - 1);
	g_bs->shms[n].size  = size;
	g_bs->shms[n].state = SHM_READY;
	g_bs->hdr.version++;

	ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);

	BS_LOG(INFO, "shm registered: %s (owner=%s, size=0x%" PRIx64 ")",
	       name, owner_name, size);
	return 0;
}

int
ub_bootstrap_register_provider(const char *node_name, uint32_t nb_expected)
{
	if (!g_bs)
		return -1;

	ubsmem_shmem_write_lock(BOOTSTRAP_SHM_NAME);

	for (uint32_t i = 0; i < g_bs->hdr.nb_providers; i++) {
		if (strcmp(g_bs->providers[i].node_name, node_name) == 0) {
			g_bs->providers[i].state = PR_ONLINE;
			ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);
			BS_LOG(INFO, "provider '%s' already registered, "
			       "state → ONLINE", node_name);
			return 0;
		}
	}

	if (g_bs->hdr.nb_providers >= BOOTSTRAP_MAX_PROV) {
		BS_LOG(ERR, "provider registry full (%u max)",
		       BOOTSTRAP_MAX_PROV);
		ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);
		return -1;
	}

	uint32_t n = g_bs->hdr.nb_providers++;
	strncpy(g_bs->providers[n].node_name, node_name,
		sizeof(g_bs->providers[n].node_name) - 1);
	g_bs->providers[n].start_time = 0;
	g_bs->providers[n].state      = PR_ONLINE;

	uint32_t cnt = g_bs->hdr.nb_providers;
	BS_LOG(INFO, "provider registered: %s (%u/%u)",
	       node_name, cnt, nb_expected);

	if (cnt == nb_expected) {
		g_bs->hdr.state = BS_READY;
		BS_LOG(INFO, "all %u providers registered → bootstrap READY",
		       cnt);
	}

	g_bs->hdr.version++;
	ubsmem_shmem_unlock(BOOTSTRAP_SHM_NAME);
	return 0;
}

struct bootstrap *
ub_bootstrap_get(void)
{
	return g_bs;
}
