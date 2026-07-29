/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef RTE_ETH_UB_RING_H
#define RTE_ETH_UB_RING_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <rte_dev.h>
#include <rte_ethdev.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * 初始化 ub_ring ethdev 设备。
 *
 * 这是一个 static inline 封装，内部只调用 rte_dev_probe()。
 * 实际 PMD 逻辑在 drivers/net/ub_ring/rte_eth_ub_ring.c 中，
 * 运行时通过 EAL -d 参数加载:
 *   ./app -d /path/to/librte_net_ub_ring.so --no-huge -l 0
 *
 * @param name            设备名称
 * @param shm_ring_rx     接收方向 ring 内存地址
 * @param shm_ring_tx     发送方向 ring 内存地址
 * @param shm_pool_data   mbuf pool 数据区地址 (NULL 表示不创建 pool)
 * @param pool_data_sz    pool 数据区大小
 * @param ubsmem_base     本端共享内存基地址
 *
 * @return  port_id (>=0) 成功，负值失败
 */
static inline int
rte_eth_ub_ring_init(const char *name,
                     void *shm_ring_rx,
                     void *shm_ring_tx,
                     void *shm_pool_data,
                     size_t pool_data_sz,
                     void *ubsmem_base)
{
    char devargs[320];
    char vdev_name[64];
    int ret;

    snprintf(vdev_name, sizeof(vdev_name), "net_ub_ring_%s", name);
    snprintf(devargs, sizeof(devargs),
             "%s,rx=%p,tx=%p,pool=%p,pool_sz=%zu,base=%p",
             vdev_name,
             shm_ring_rx, shm_ring_tx,
             shm_pool_data, pool_data_sz, ubsmem_base);

    ret = rte_dev_probe(devargs);
    if (ret) {
        fprintf(stderr, "[%s] rte_dev_probe failed: %d (rte_errno=%d)\n",
                name, ret, rte_errno);
        return -1;
    }

    uint16_t port_id;
    ret = rte_eth_dev_get_port_by_name(name, &port_id);
    if (ret) {
        fprintf(stderr, "[%s] get_port_by_name failed (rte_errno=%d)\n",
                name, rte_errno);
        return -1;
    }
    return port_id;
}

#ifdef __cplusplus
}
#endif

#endif /* RTE_ETH_UB_RING_H */
