/* SPDX-License-Identifier: BSD-3-Clause
 *
 * 客户端 —— 使用 rte_eth_ub_ring PMD
 *
 * 运行: ./client --no-huge -l 0
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
#include <rte_mempool.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_errno.h>

#include <rte_eth_ub_ring.h>

/* ---- 共享内存布局 (与服务端一致) ---- */
#define SZ_4G              (4UL * 1024 * 1024 * 1024)
#define SZ_1G              (1UL * 1024 * 1024 * 1024)

#define HANDSHAKE_SIZE     (4 * 1024)
#define RING_AREA_SIZE     (256 * 1024)
#define C2S_RING_OFFSET    HANDSHAKE_SIZE
#define S2C_RING_OFFSET    (HANDSHAKE_SIZE + RING_AREA_SIZE)
#define POOL_DATA_OFFSET   (HANDSHAKE_SIZE + 2 * RING_AREA_SIZE)

#define NUM_PACKETS        10
#define BURST_SIZE         32
#define SEND_INTERVAL_US   500000

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
    fprintf(stderr, "[Client] %s: %s(%d)  rte_errno=%d\n",
            msg, ubs_strerror(rc), rc, rte_errno);
}


int main(int argc, char **argv)
{
    if (rte_eal_init(argc, argv) < 0) {
        fprintf(stderr, "[Client] rte_eal_init failed (rte_errno=%d)\n",
                rte_errno); return EXIT_FAILURE;
    }
    printf("\n===== Client =====\n");

    /* ---- 1. ubsmem init + region + map (不 allocate) ---- */
    ubsmem_options_t opts;
    int rc = ubsmem_init_attributes(&opts);
    if (rc) { fail("init_attributes", rc); return EXIT_FAILURE; }
    rc = ubsmem_initialize(&opts);
    if (rc) { fail("initialize", rc); return EXIT_FAILURE; }

    ubsmem_region_attributes_t attr;
    memset(&attr, 0, sizeof(attr));
    attr.host_num = 2;
    strncpy(attr.hosts[0].host_name, "worker1", MAX_HOST_NAME_DESC_LENGTH - 1);
    attr.hosts[0].affinity = true;
    strncpy(attr.hosts[1].host_name, "master",  MAX_HOST_NAME_DESC_LENGTH - 1);
    attr.hosts[1].affinity = true;

    ubsmem_destroy_region("region1");
    rc = ubsmem_create_region("region1", 0, &attr);
    if (rc) { fail("create_region", rc); ubsmem_finalize(); return EXIT_FAILURE; }

    void *ptr = NULL;
    rc = ubsmem_shmem_map(NULL, SZ_4G, PROT_READ | PROT_WRITE, MAP_SHARED,
                           "shm1", 0, &ptr);
    if (rc) { fail("shmem_map (server started?)", rc);
        ubsmem_destroy_region("region1"); ubsmem_finalize(); return EXIT_FAILURE; }
    printf("[Client] ubsmem_base = %p\n", ptr);

    /* ---- 2. 握手 ---- */
    int *val = (int *)ptr;
    *val = 1;
    printf("[Client] waiting for 2 ...\n");
    if (wait_for(val, 2)) {
        fprintf(stderr, "[Client] timeout (val=%d)\n", *val); goto cleanup; }
    *val = 3;
    printf("[Client] handshake done\n");
    sleep(1);

    /* ---- 3. 创建 ub_ring ethdev (PMD 内部完成 ring init + pool 创建) ---- */
    int port_id = rte_eth_ub_ring_init("client_eth",
                        (char *)ptr + S2C_RING_OFFSET,    /* shm_ring_rx */
                        (char *)ptr + C2S_RING_OFFSET,    /* shm_ring_tx */
                        (char *)ptr + POOL_DATA_OFFSET,   /* pool data */
                        SZ_1G,                              /* pool data 大小 */
                        ptr);
    if (port_id < 0) {
        fprintf(stderr, "[Client] init failed (ret=%d, rte_errno=%d)\n",
                port_id, rte_errno); goto cleanup;
    }
    printf("[Client] ub_ring ethdev port=%d\n", port_id);

    struct rte_eth_conf conf = {
        .rxmode = { .max_lro_pkt_size = RTE_ETHER_MAX_LEN },
    };
    if (rte_eth_dev_configure(port_id, 1, 1, &conf) ||
        rte_eth_dev_start(port_id)) {
        fprintf(stderr, "[Client] configure/start failed (rte_errno=%d)\n",
                rte_errno); goto cleanup;
    }

    /* ---- 4. 获取 pool (PMD 内部创建的) ---- */
    /* pool 指针已保存在 PMD internals 中。通过 dev_private 获取或直接 alloc/free。
     * 简便做法: 使用 rte_pktmbuf_alloc 需要一个 pool 引用。
     * client 不需要拿到 pool 指针——在 rx_burst 的内部已完成 offset→指针转换,
     * 释放时直接调用 rte_pktmbuf_free 即可 (m->pool 指向 PMD 内创建的 pool)。 */
    struct rte_mempool *pool = rte_mempool_lookup("client_eth");

    /* ---- 5. 发包循环 ---- */
    for (int i = 0; i < NUM_PACKETS; i++) {
        struct rte_mbuf *m = rte_pktmbuf_alloc(pool);
        if (!m) {
            fprintf(stderr, "[Client] alloc #%d failed (rte_errno=%d)\n",
                    i, rte_errno); continue;
        }

        char buf[128];
        int len = snprintf(buf, sizeof(buf),
                           "Hello from client! Packet #%d", i);
        if (!rte_pktmbuf_append(m, len)) {
            fprintf(stderr, "[Client] append #%d failed\n", i);
            rte_pktmbuf_free(m); continue;
        }
        rte_memcpy(rte_pktmbuf_mtod(m, char *), buf, len);
        printf("[Client] TX #%d: %s\n", i, buf);

        while (!rte_eth_tx_burst(port_id, 0, &m, 1))
            ;

        struct rte_mbuf *back = NULL;
        while (!rte_eth_rx_burst(port_id, 0, &back, 1))
            rte_delay_us(1000);

        rte_pktmbuf_free(back);
        printf("[Client] #%d freed\n", i);
        rte_delay_us(SEND_INTERVAL_US);
    }
    printf("\n[Client] done (%d packets)\n", NUM_PACKETS);

    rte_eth_dev_stop(port_id);
    rte_eth_dev_close(port_id);

cleanup:
    ubsmem_shmem_unmap(ptr, SZ_4G);
    ubsmem_destroy_region("region1");
    ubsmem_finalize();
    rte_eal_cleanup();
    printf("===== Client exit =====\n");
    return 0;
}
