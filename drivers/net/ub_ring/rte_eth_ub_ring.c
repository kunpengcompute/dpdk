/* SPDX-License-Identifier: BSD-3-Clause
 *
 * rte_eth_ub_ring PMD — vdev 驱动, 运行时通过 -d librte_net_ub_ring.so 加载
 */

#include <stdlib.h>
#include <rte_eal.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ring.h>
#include <rte_ethdev.h>
#include <rte_malloc.h>
#include <rte_kvargs.h>
#include <rte_errno.h>
#include <rte_log.h>
#include <bus_vdev_driver.h>
#include <ethdev_driver.h>

/* ---- 日志 ---- */
RTE_LOG_REGISTER_DEFAULT(ub_ring_logtype, NOTICE);
#define RTE_LOGTYPE_UB_RING ub_ring_logtype
#define UB_RING_LOG(level, ...) \
    RTE_LOG_LINE_PREFIX(level, UB_RING, "%s(): ", __func__, __VA_ARGS__)

/* ---- 固定参数 ---- */
#define RING_COUNT    1024
#define NB_MBUFS      512
#define POOL_CACHE    32

/* ---- vdev 参数 key ---- */
#define UB_RING_ARG_RX       "rx"
#define UB_RING_ARG_TX       "tx"
#define UB_RING_ARG_POOL     "pool"
#define UB_RING_ARG_POOL_SZ  "pool_sz"
#define UB_RING_ARG_BASE     "base"

/* ---- 数据结构 ---- */

struct ub_ring_internals;

struct ub_ring_queue {
    struct rte_ring          *rng;
    struct ub_ring_internals *internals;
};

struct ub_ring_internals {
    void               *ubsmem_base;
    struct rte_mempool *pool;
    unsigned int        max_rx_queues;
    unsigned int        max_tx_queues;

    struct ub_ring_queue rx_queues[RTE_MAX_QUEUES_PER_PORT];
    struct ub_ring_queue tx_queues[RTE_MAX_QUEUES_PER_PORT];

    struct rte_ether_addr address;
};

struct ub_ring_devargs {
    void  *rx_addr;
    void  *tx_addr;
    void  *pool_addr;
    size_t pool_sz;
    void  *base_addr;
};


/* ---- RX / TX 快速路径 ---- */

static uint16_t
ub_ring_rx(void *q, struct rte_mbuf **bufs, uint16_t nb_bufs)
{
    struct ub_ring_queue     *rq = q;
    struct ub_ring_internals *in = rq->internals;
    void *vals[nb_bufs];

    uint16_t n = (uint16_t)rte_ring_dequeue_burst(rq->rng, vals, nb_bufs, NULL);
    if (n == 0)
        return 0;

    for (uint16_t i = 0; i < n; i++) {
        uint64_t       off = (uint64_t)(uintptr_t)vals[i];
        struct rte_mbuf *m = (struct rte_mbuf *)((char *)in->ubsmem_base + off);
        m->buf_addr = (void *)((char *)m + sizeof(struct rte_mbuf));
        bufs[i]     = m;
    }
    return n;
}


static uint16_t
ub_ring_tx(void *q, struct rte_mbuf **bufs, uint16_t nb_bufs)
{
    struct ub_ring_queue     *rq = q;
    struct ub_ring_internals *in = rq->internals;
    void *vals[nb_bufs];

    for (uint16_t i = 0; i < nb_bufs; i++) {
        uint64_t off = (uint64_t)((char *)bufs[i] - (char *)in->ubsmem_base);
        vals[i] = (void *)(uintptr_t)off;
    }
    return (uint16_t)rte_ring_enqueue_burst(rq->rng, vals, nb_bufs, NULL);
}


/* ---- eth_dev_ops 回调 ---- */

static int ub_ring_dev_configure(struct rte_eth_dev *d __rte_unused)
{ return 0; }

static int ub_ring_dev_start(struct rte_eth_dev *dev)
{ dev->data->dev_link.link_status = RTE_ETH_LINK_UP; return 0; }

static int ub_ring_dev_stop(struct rte_eth_dev *dev)
{
    dev->data->dev_started = 0;
    dev->data->dev_link.link_status = RTE_ETH_LINK_DOWN;
    for (uint16_t i = 0; i < dev->data->nb_rx_queues; i++)
        dev->data->rx_queue_state[i] = RTE_ETH_QUEUE_STATE_STARTED;
    for (uint16_t i = 0; i < dev->data->nb_tx_queues; i++)
        dev->data->tx_queue_state[i] = RTE_ETH_QUEUE_STATE_STARTED;
    return 0;
}

static int ub_ring_dev_set_link_down(struct rte_eth_dev *dev)
{
    dev->data->dev_link.link_status = RTE_ETH_LINK_DOWN;
    for (uint16_t i = 0; i < dev->data->nb_rx_queues; i++)
        dev->data->rx_queue_state[i] = RTE_ETH_QUEUE_STATE_STOPPED;
    for (uint16_t i = 0; i < dev->data->nb_tx_queues; i++)
        dev->data->tx_queue_state[i] = RTE_ETH_QUEUE_STATE_STOPPED;
    return 0;
}

static int ub_ring_dev_set_link_up(struct rte_eth_dev *dev)
{
    dev->data->dev_link.link_status = RTE_ETH_LINK_UP;
    return 0;
}

static int ub_ring_dev_close(struct rte_eth_dev *dev)
{
    struct ub_ring_internals *in = dev->data->dev_private;
    ub_ring_dev_stop(dev);
    dev->data->mac_addrs = NULL;
    rte_free(in);
    dev->data->dev_private = NULL;
    return 0;
}

static int ub_ring_dev_info(struct rte_eth_dev *dev, struct rte_eth_dev_info *info)
{
    struct ub_ring_internals *in = dev->data->dev_private;
    info->max_mac_addrs  = 1;
    info->max_rx_pktlen  = (uint32_t)-1;
    info->max_rx_queues   = (uint16_t)in->max_rx_queues;
    info->max_tx_queues   = (uint16_t)in->max_tx_queues;
    info->min_rx_bufsize  = 0;
    info->rx_offload_capa = RTE_ETH_RX_OFFLOAD_SCATTER;
    info->tx_offload_capa = RTE_ETH_TX_OFFLOAD_MULTI_SEGS;
    return 0;
}

static int ub_ring_rx_queue_setup(struct rte_eth_dev *dev, uint16_t rx_queue_id,
        uint16_t nb_rx_desc __rte_unused, unsigned int socket_id __rte_unused,
        const struct rte_eth_rxconf *rx_conf __rte_unused,
        struct rte_mempool *mb_pool __rte_unused)
{
    struct ub_ring_internals *in = dev->data->dev_private;
    dev->data->rx_queues[rx_queue_id] = &in->rx_queues[rx_queue_id];
    return 0;
}

static int ub_ring_tx_queue_setup(struct rte_eth_dev *dev, uint16_t tx_queue_id,
        uint16_t nb_tx_desc __rte_unused, unsigned int socket_id __rte_unused,
        const struct rte_eth_txconf *tx_conf __rte_unused)
{
    struct ub_ring_internals *in = dev->data->dev_private;
    dev->data->tx_queues[tx_queue_id] = &in->tx_queues[tx_queue_id];
    return 0;
}

static int ub_ring_link_update(struct rte_eth_dev *d __rte_unused,
                               int w __rte_unused)
{ return 0; }

static int ub_ring_stats_get(struct rte_eth_dev *d, struct rte_eth_stats *s)
{ (void)d; memset(s, 0, sizeof(*s)); return 0; }

static int ub_ring_stats_reset(struct rte_eth_dev *d)
{ (void)d; return 0; }

static int noop_ok(struct rte_eth_dev *d __rte_unused) { return 0; }
static int mac_addr_add(struct rte_eth_dev *d __rte_unused,
        struct rte_ether_addr *a __rte_unused, uint32_t i __rte_unused,
        uint32_t p __rte_unused) { return 0; }
static void mac_addr_remove(struct rte_eth_dev *d __rte_unused,
                            uint32_t i __rte_unused) {}

static const struct eth_dev_ops ub_ring_ops = {
    .dev_configure       = ub_ring_dev_configure,
    .dev_start           = ub_ring_dev_start,
    .dev_stop            = ub_ring_dev_stop,
    .dev_close           = ub_ring_dev_close,
    .dev_infos_get       = ub_ring_dev_info,
    .rx_queue_setup      = ub_ring_rx_queue_setup,
    .tx_queue_setup      = ub_ring_tx_queue_setup,
    .link_update         = ub_ring_link_update,
    .stats_get           = ub_ring_stats_get,
    .stats_reset         = ub_ring_stats_reset,
    .mac_addr_add        = mac_addr_add,
    .mac_addr_remove     = mac_addr_remove,
    .promiscuous_enable  = noop_ok,
    .promiscuous_disable = noop_ok,
    .allmulticast_enable  = noop_ok,
    .allmulticast_disable = noop_ok,
    .dev_set_link_up     = ub_ring_dev_set_link_up,
    .dev_set_link_down   = ub_ring_dev_set_link_down,
};


/* ---- 内部: 创建 mbuf pool ---- */
static struct rte_mempool *
create_pool_internal(const char *name, void *addr, size_t sz)
{
    unsigned int elt_sz = sizeof(struct rte_mbuf) + RTE_MBUF_DEFAULT_BUF_SIZE;
    struct rte_pktmbuf_pool_private priv;
    struct rte_mempool *mp;
    size_t per, need;
    int ret;

    mp = rte_mempool_create_empty(name, NB_MBUFS, elt_sz, POOL_CACHE,
            sizeof(priv), rte_socket_id(),
            RTE_MEMPOOL_F_SP_PUT | RTE_MEMPOOL_F_SC_GET);
    if (!mp) {
        UB_RING_LOG(ERR, "[%s] create_empty failed (rte_errno=%d)",
                    name, rte_errno);
        return NULL;
    }

    priv.mbuf_data_room_size = RTE_MBUF_DEFAULT_BUF_SIZE;
    priv.mbuf_priv_size = 0;
    rte_pktmbuf_pool_init(mp, &priv);

    per  = mp->header_size + mp->elt_size + mp->trailer_size;
    need = per * NB_MBUFS;
    if (need > sz) {
        UB_RING_LOG(ERR, "[%s] pool data too small: need=%zu have=%zu",
                    name, need, sz);
        rte_mempool_free(mp);
        return NULL;
    }

    ret = rte_mempool_populate_iova(mp, addr, RTE_BAD_IOVA, need, NULL, NULL);
    if (ret <= 0) {
        UB_RING_LOG(ERR, "[%s] populate failed: %d (rte_errno=%d)",
                    name, ret, rte_errno);
        rte_mempool_free(mp);
        return NULL;
    }

    rte_mempool_obj_iter(mp, rte_pktmbuf_init, NULL);
    UB_RING_LOG(INFO, "[%s] pool ready (%d objects, total=%zu)",
                name, ret, need);
    return mp;
}


/* ---- 内部: ring 初始化 ---- */
static int
init_ring_if_needed(const char *name, struct rte_ring *r, const char *which)
{
    if (rte_ring_get_size(r) != 0) {
        UB_RING_LOG(INFO, "[%s] %s ring already initialized (size=%u)",
                    name, which, rte_ring_get_size(r));
        return 0;
    }
    int ret = rte_ring_init(r, which, RING_COUNT,
                            RING_F_SP_ENQ | RING_F_SC_DEQ);
    if (ret < 0) {
        UB_RING_LOG(ERR, "[%s] rte_ring_init %s failed (rte_errno=%d)",
                    name, which, rte_errno);
        return ret;
    }
    UB_RING_LOG(INFO, "[%s] %s ring initialized", name, which);
    return 0;
}


/* ---- 内部: ethdev 创建 ---- */
static int
do_ub_ring_create(const char *name, struct rte_vdev_device *vdev,
                  struct ub_ring_devargs *da)
{
    struct ub_ring_internals *in;
    struct rte_eth_dev *eth_dev;
    struct rte_eth_dev_data *data;
    struct rte_ring *rx_ring, *tx_ring;
    void **rx_queues, **tx_queues;
    unsigned int nb_q = 1;

    rx_ring = (struct rte_ring *)da->rx_addr;
    tx_ring = (struct rte_ring *)da->tx_addr;

    if (init_ring_if_needed(name, rx_ring, "rx") < 0 ||
        init_ring_if_needed(name, tx_ring, "tx") < 0)
        return -1;

    struct rte_mempool *pool = NULL;
    if (da->pool_addr) {
        pool = create_pool_internal(name, da->pool_addr, da->pool_sz);
        if (!pool)
            return -1;
    }

    in = rte_zmalloc_socket(NULL, sizeof(*in), 0, rte_socket_id());
    if (!in) {
        if (pool) rte_mempool_free(pool);
        return -ENOMEM;
    }
    in->ubsmem_base   = da->base_addr;
    in->pool          = pool;
    in->max_rx_queues = nb_q;
    in->max_tx_queues = nb_q;

    rx_queues = rte_calloc_socket(NULL, nb_q, sizeof(void *), 0,
                                  rte_socket_id());
    tx_queues = rte_calloc_socket(NULL, nb_q, sizeof(void *), 0,
                                  rte_socket_id());
    if (!rx_queues || !tx_queues) {
        rte_free(rx_queues);
        rte_free(tx_queues);
        if (pool) rte_mempool_free(pool);
        rte_free(in);
        return -ENOMEM;
    }

    for (unsigned int i = 0; i < nb_q; i++) {
        in->rx_queues[i].rng = rx_ring;
        in->rx_queues[i].internals = in;
        in->tx_queues[i].rng = tx_ring;
        in->tx_queues[i].internals = in;
    }

    eth_dev = rte_eth_dev_allocate(name);
    if (!eth_dev) {
        UB_RING_LOG(ERR, "[%s] rte_eth_dev_allocate failed", name);
        rte_free(rx_queues);
        rte_free(tx_queues);
        if (pool) rte_mempool_free(pool);
        rte_free(in);
        return -ENODEV;
    }

    eth_dev->device = &vdev->device;
    data = eth_dev->data;
    data->rx_queues    = rx_queues;
    data->tx_queues    = tx_queues;
    data->nb_rx_queues = (uint16_t)nb_q;
    data->nb_tx_queues = (uint16_t)nb_q;
    data->dev_private  = in;
    data->numa_node     = rte_socket_id();
    data->promiscuous   = 1;
    data->all_multicast = 1;
    data->dev_flags    |= RTE_ETH_DEV_AUTOFILL_QUEUE_XSTATS;

    /* 直接赋值队列指针 (ring PMD 在 create 中直接赋值, 不依赖 queue_setup) */
    for (unsigned int i = 0; i < nb_q; i++) {
        data->rx_queues[i] = &in->rx_queues[i];
        data->tx_queues[i] = &in->tx_queues[i];
    }

    in->address = (struct rte_ether_addr){
        .addr_bytes = { 0x02, 0x00, 0x00, 0x00, 0x00, 0x00 }
    };
    data->mac_addrs = &in->address;
    data->dev_link = (struct rte_eth_link){
        .link_speed  = RTE_ETH_SPEED_NUM_10G,
        .link_duplex = RTE_ETH_LINK_FULL_DUPLEX,
        .link_status = RTE_ETH_LINK_DOWN,
        .link_autoneg = RTE_ETH_LINK_FIXED,
    };

    eth_dev->dev_ops      = &ub_ring_ops;
    eth_dev->rx_pkt_burst = ub_ring_rx;
    eth_dev->tx_pkt_burst = ub_ring_tx;

    rte_eth_dev_probing_finish(eth_dev);
    return data->port_id;
}


/* ---- devargs 解析 ---- */

static int
parse_hex_addr(const char *key __rte_unused, const char *value,
               void *extra_args)
{
    void **dst = extra_args;
    char *end;
    uintptr_t addr = strtoull(value, &end, 0);
    if (*end != '\0')
        return -EINVAL;
    *dst = (void *)addr;
    return 0;
}

static int
parse_size(const char *key __rte_unused, const char *value,
           void *extra_args)
{
    size_t *dst = extra_args;
    char *end;
    *dst = strtoull(value, &end, 0);
    if (*end != '\0')
        return -EINVAL;
    return 0;
}


/* ---- vdev probe / remove ---- */

static int
ub_ring_vdev_probe(struct rte_vdev_device *vdev)
{
    const char *devname = rte_vdev_device_name(vdev);
    struct ub_ring_devargs da;
    struct rte_kvargs *kv;

    if (!devname)
        return -EINVAL;

    /* 跳过 "net_ub_ring_" 前缀获取 short name */
    const char *short_name = devname;
    const char *prefix = "net_ub_ring_";
    size_t plen = strlen(prefix);
    if (strncmp(devname, prefix, plen) == 0)
        short_name = devname + plen;

    memset(&da, 0, sizeof(da));

    kv = rte_kvargs_parse(rte_vdev_device_args(vdev), NULL);
    if (!kv)
        return -EINVAL;

    if (rte_kvargs_process(kv, UB_RING_ARG_RX,   parse_hex_addr, &da.rx_addr) < 0 ||
        rte_kvargs_process(kv, UB_RING_ARG_TX,   parse_hex_addr, &da.tx_addr) < 0 ||
        rte_kvargs_process(kv, UB_RING_ARG_BASE, parse_hex_addr, &da.base_addr) < 0) {
        UB_RING_LOG(ERR, "[%s] failed to parse rx/tx/base args", devname);
        rte_kvargs_free(kv);
        return -EINVAL;
    }

    /* pool 和 pool_sz 可选 */
    rte_kvargs_process(kv, UB_RING_ARG_POOL,    parse_hex_addr, &da.pool_addr);
    rte_kvargs_process(kv, UB_RING_ARG_POOL_SZ, parse_size,    &da.pool_sz);

    rte_kvargs_free(kv);

    return do_ub_ring_create(short_name, vdev, &da);
}

static int
ub_ring_vdev_remove(struct rte_vdev_device *vdev)
{
    const char *name = rte_vdev_device_name(vdev);
    struct rte_eth_dev *eth_dev;

    if (!name)
        return -EINVAL;

    eth_dev = rte_eth_dev_allocated(name);
    if (!eth_dev)
        return 0;

    ub_ring_dev_close(eth_dev);
    rte_eth_dev_release_port(eth_dev);
    return 0;
}

static struct rte_vdev_driver ub_ring_vdev_drv = {
    .probe  = ub_ring_vdev_probe,
    .remove = ub_ring_vdev_remove,
};

RTE_PMD_REGISTER_VDEV(net_ub_ring, ub_ring_vdev_drv);
