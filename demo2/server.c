/* SPDX-License-Identifier: BSD-3-Clause
 *
 * 服务端 —— 使用 rte_eth_ub_ring PMD
 *
 * 运行: ./server --no-huge -l 0
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <ubs_mem.h>

#include <rte_eal.h>
#include <rte_mbuf.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_errno.h>

#include <rte_eth_ub_ring.h>

/* ---- 共享内存布局 ---- */
#define SZ_4G              (4UL * 1024 * 1024 * 1024)

#define HANDSHAKE_SIZE     (4 * 1024)
#define RING_AREA_SIZE     (256 * 1024)
#define C2S_RING_OFFSET    HANDSHAKE_SIZE
#define S2C_RING_OFFSET    (HANDSHAKE_SIZE + RING_AREA_SIZE)

#define NUM_PACKETS        10
#define BURST_SIZE         32

#define POLL_INTERVAL_US   50000
#define POLL_TIMEOUT_S     60


static const char *ubs_strerror(int rc)
{
    switch (rc) {
    case UBSM_OK:                return "OK";
    case UBSM_ERR_PARAM_INVALID: return "PARAM_INVALID";
    case UBSM_ERR_NOPERM:        return "NOPERM";
    case UBSM_ERR_MEMORY:        return "MEMORY";
    case UBSM_ERR_UNIMPL:        return "UNIMPL";
    case UBSM_CHECK_RESOURCE_ERROR: return "CHECK_RESOURCE_ERROR";
    case UBSM_ERR_MEMLIB:        return "MEMLIB";
    case UBSM_ERR_NO_NEEDED:     return "NO_NEEDED";
    case UBSM_ERR_BUSY:          return "BUSY";
    case UBSM_ERR_NOT_FOUND:     return "NOT_FOUND";
    case UBSM_ERR_ALREADY_EXIST: return "ALREADY_EXIST";
    case UBSM_ERR_MALLOC_FAIL:   return "MALLOC_FAIL";
    case UBSM_ERR_RECORD:        return "RECORD";
    case UBSM_ERR_IN_USING:      return "IN_USING";
    case UBSM_ERR_NOT_SUPPORTED: return "NOT_SUPPORTED";
    case UBSM_ERR_NET:           return "NET";
    case UBSM_ERR_UBSE:          return "UBSE";
    case UBSM_ERR_OBMM:          return "OBMM";
    case UBSM_ERR_LOCK_NOT_SUPPORTED:   return "LOCK_NOT_SUPPORTED";
    case UBSM_ERR_LOCK_ALREADY_LOCKED:  return "LOCK_ALREADY_LOCKED";
    case UBSM_ERR_DLOCK:         return "DLOCK";
    case UBSM_ERR_BUFF:          return "BUFF";
    default:                     return "UNKNOWN";
    }
}

static int wait_for(int *p, int expect)
{
    for (int i = 0; i < POLL_TIMEOUT_S * 20; i++) {
        if (*p == expect) return 0;
        usleep(POLL_INTERVAL_US);
    }
    return -1;
}

static void fail(const char *msg, int rc)
{
    fprintf(stderr, "[Server] %s: %s(%d)  rte_errno=%d\n",
            msg, ubs_strerror(rc), rc, rte_errno);
}


int main(int argc, char **argv)
{
    if (rte_eal_init(argc, argv) < 0) {
        fprintf(stderr, "[Server] rte_eal_init failed (rte_errno=%d)\n",
                rte_errno); return EXIT_FAILURE;
    }
    printf("\n===== Server =====\n");

    /* ---- 1. ubsmem init + region + allocate + map ---- */
    ubsmem_options_t opts;
    int rc = ubsmem_init_attributes(&opts);
    if (rc) { fail("init_attributes", rc); return EXIT_FAILURE; }
    rc = ubsmem_initialize(&opts);
    if (rc) { fail("initialize", rc); return EXIT_FAILURE; }

    ubsmem_region_attributes_t attr;
    memset(&attr, 0, sizeof(attr));
    attr.host_num = 2;
    strncpy(attr.hosts[0].host_name, "master",  MAX_HOST_NAME_DESC_LENGTH - 1);
    attr.hosts[0].affinity = true;
    strncpy(attr.hosts[1].host_name, "worker1", MAX_HOST_NAME_DESC_LENGTH - 1);
    attr.hosts[1].affinity = true;

    ubsmem_destroy_region("region1");
    rc = ubsmem_create_region("region1", 0, &attr);
    if (rc) { fail("create_region", rc); ubsmem_finalize(); return EXIT_FAILURE; }

    ubsmem_shmem_deallocate("shm1");
    uint64_t flags = UBSM_FLAG_NONCACHE | UBSM_FLAG_WR_DELAY_COMP;
    rc = ubsmem_shmem_allocate("region1", "shm1", SZ_4G, 0600, flags);
    if (rc) { fail("shmem_allocate", rc);
        ubsmem_destroy_region("region1"); ubsmem_finalize(); return EXIT_FAILURE; }

    void *ptr = NULL;
    rc = ubsmem_shmem_map(NULL, SZ_4G, PROT_READ | PROT_WRITE, MAP_SHARED,
                           "shm1", 0, &ptr);
    if (rc) { fail("shmem_map", rc);
        ubsmem_destroy_region("region1"); ubsmem_finalize(); return EXIT_FAILURE; }

    memset(ptr, 0, SZ_4G);
    printf("[Server] ubsmem_base = %p\n", ptr);

    /* ---- 2. 握手 ---- */
    int *val = (int *)ptr;
    printf("[Server] waiting for 1 ...\n");
    if (wait_for(val, 1)) {
        fprintf(stderr, "[Server] timeout (val=%d)\n", *val); goto cleanup; }
    *val = 2;
    printf("[Server] waiting for 3 ...\n");
    if (wait_for(val, 3)) {
        fprintf(stderr, "[Server] timeout (val=%d)\n", *val); goto cleanup; }
    printf("[Server] handshake done\n");
    sleep(1);

    /* ---- 3. 创建 ub_ring ethdev (PMD 内部完成 ring init, 无 pool) ---- */
    int port_id = rte_eth_ub_ring_init("server_eth",
                        (char *)ptr + C2S_RING_OFFSET,   /* shm_ring_rx */
                        (char *)ptr + S2C_RING_OFFSET,   /* shm_ring_tx */
                        NULL, 0,                         /* pool=NULL */
                        ptr);
    if (port_id < 0) {
        fprintf(stderr, "[Server] init failed (ret=%d, rte_errno=%d)\n",
                port_id, rte_errno); goto cleanup;
    }
    printf("[Server] ub_ring ethdev port=%d\n", port_id);

    struct rte_eth_conf conf = {
        .rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN },
    };
    if (rte_eth_dev_configure(port_id, 1, 1, &conf) ||
        rte_eth_dev_start(port_id)) {
        fprintf(stderr, "[Server] configure/start failed (rte_errno=%d)\n",
                rte_errno); goto cleanup;
    }

    /* ---- 4. 收包循环 ---- */
    struct rte_mbuf *pkts[BURST_SIZE];
    int received = 0;
    printf("[Server] listening on port %d\n\n", port_id);

    while (received < NUM_PACKETS) {
        uint16_t n = rte_eth_rx_burst(port_id, 0, pkts, BURST_SIZE);
        if (!n) { rte_delay_us(1000); continue; }
        for (uint16_t i = 0; i < n; i++) {
            uint32_t len = rte_pktmbuf_data_len(pkts[i]);
            printf("[Server] RX (%uB): %.*s\n",
                   len, len, rte_pktmbuf_mtod(pkts[i], char *));
            received++;
            while (!rte_eth_tx_burst(port_id, 0, &pkts[i], 1))
                ;
        }
    }
    printf("\n[Server] done (%d packets)\n", received);

    rte_eth_dev_stop(port_id);
    rte_eth_dev_close(port_id);

cleanup:
    ubsmem_shmem_unmap(ptr, SZ_4G);
    ubsmem_shmem_deallocate("shm1");
    ubsmem_destroy_region("region1");
    ubsmem_finalize();
    rte_eal_cleanup();
    printf("===== Server exit =====\n");
    return (received == NUM_PACKETS) ? 0 : EXIT_FAILURE;
}
