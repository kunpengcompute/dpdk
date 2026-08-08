/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef RTE_UB_BOOTSTRAP_H
#define RTE_UB_BOOTSTRAP_H

/**
 * @file
 *
 * ub_ring bootstrap — provider/shm registry on ubsmem shared memory.
 */

#include <stdint.h>

#define BOOTSTRAP_SHM_NAME   "system_bootstrap"
#define BOOTSTRAP_MAGIC      0x5542535452500001ULL
#define BOOTSTRAP_MAX_PROV   32
#define BOOTSTRAP_MAX_SHMS   32

enum bootstrap_state {
	BS_INIT  = 0,
	BS_READY = 1,
};

enum provider_state {
	PR_REGISTERING = 0,
	PR_ONLINE      = 1,
};

enum shm_state {
	SHM_CREATING = 0,
	SHM_READY    = 1,
};

struct bootstrap_hdr {
	uint64_t magic;
	uint64_t generation;
	uint32_t state;
	uint32_t nb_providers;
	uint32_t nb_shms;
	uint32_t padding;
	uint64_t version;
};

struct provider_rec {
	char     node_name[64];
	uint64_t start_time;
	uint32_t state;
	uint32_t padding;
};

struct shm_rec {
	char     name[64];
	char     owner_name[64];
	uint64_t size;
	uint32_t state;
	uint32_t padding;
};

struct bootstrap {
	struct bootstrap_hdr hdr;
	struct provider_rec  providers[BOOTSTRAP_MAX_PROV];
	struct shm_rec       shms[BOOTSTRAP_MAX_SHMS];
};

/**
 * Allocate or map the bootstrap shared memory.
 *
 * First caller allocates and initialises the bootstrap.
 * Subsequent callers map the existing bootstrap and wait for BS_READY.
 *
 * @param region  ubsmem region name
 * @return 0 on success, -1 on error
 */
int ub_bootstrap_init_or_map(const char *region);

/**
 * Register a shared memory in the bootstrap (write-locked).
 *
 * @param name        shm name
 * @param owner_name  hostname of the owning node
 * @param size        shm size in bytes
 * @return 0 on success, -1 on error
 */
int ub_bootstrap_register_shm(const char *name, const char *owner_name,
			      uint64_t size);

/**
 * Register a provider in the bootstrap (write-locked).
 *
 * When the last expected provider registers, state is set to BS_READY.
 *
 * @param node_name    hostname of this provider
 * @param nb_expected  total number of provider nodes
 * @return 0 on success, -1 on error
 */
int ub_bootstrap_register_provider(const char *node_name,
				   uint32_t nb_expected);

/**
 * Get the bootstrap pointer (for read-only access by callers).
 */
struct bootstrap *ub_bootstrap_get(void);

#endif /* RTE_UB_BOOTSTRAP_H */
