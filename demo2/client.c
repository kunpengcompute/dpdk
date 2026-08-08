/* SPDX-License-Identifier: BSD-3-Clause
 * Client (user) — ub_ring ethdev PMD.
 * Run: ./client --no-huge -l 0 -d librte_net_ub_ring.so
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rte_eal.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ethdev.h>
#include <rte_errno.h>
#include <rte_eth_ub_ring.h>

#define NUM_PACKETS 10
#define BURST_SIZE  32
#define SEND_US     500000

int main(int argc, char **argv)
{
	if (rte_eal_init(argc, argv) < 0) { fprintf(stderr,"[Client] eal fail\n"); return 1; }
	printf("\n===== Client (user) =====\n\n");

	const char *p_nodes[] = {"master"}; /* providers */
	const char *u_nodes[] = {"worker1"}; /* users (self included) */
	uint32_t elt = sizeof(struct rte_mbuf)+RTE_MBUF_DEFAULT_BUF_SIZE;

	int pid = rte_eth_ub_ring_create_user("client_eth",
		p_nodes, 1, u_nodes, 1, "region1", "ubring", elt);
	if (pid < 0) { fprintf(stderr,"[Client] create fail: %d\n",pid); return 1; }
	printf("[Client] port=%d\n", pid);

	struct rte_eth_conf conf = { .rxmode={.max_lro_pkt_size=RTE_ETHER_MAX_LEN} };
	if (rte_eth_dev_configure(pid,1,1,&conf)<0 || rte_eth_dev_start(pid)<0)
		{ fprintf(stderr,"[Client] config/start fail\n"); rte_eth_dev_close(pid); return 1; }

	struct rte_mempool *mp = rte_pktmbuf_pool_create("local",256,32,0,
		RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
	if (!mp) { fprintf(stderr,"[Client] pool fail\n"); rte_eth_dev_close(pid); return 1; }

	for (int i=0; i<NUM_PACKETS; i++) {
		struct rte_mbuf *m=rte_pktmbuf_alloc(mp);
		if (!m) { fprintf(stderr,"[Client] alloc#%d fail\n",i); continue; }
		char buf[128]; int len=snprintf(buf,128,"Hello from client! Packet #%d",i);
		if (!rte_pktmbuf_append(m,len)) { rte_pktmbuf_free(m); continue; }
		rte_memcpy(rte_pktmbuf_mtod(m,char*),buf,len);
		printf("[Client] TX #%d: %s\n",i,buf);
		while (!rte_eth_tx_burst(pid,0,&m,1));
		struct rte_mbuf *back=NULL;
		while (!rte_eth_rx_burst(pid,0,&back,1)) rte_delay_us(1000);
		rte_pktmbuf_free(back); rte_pktmbuf_free(m);
		printf("[Client] #%d freed\n",i);
		rte_delay_us(SEND_US);
	}
	printf("\n[Client] done (%d)\n",NUM_PACKETS);
	rte_eth_dev_stop(pid); rte_eth_dev_close(pid); rte_mempool_free(mp); rte_eal_cleanup();
	return 0;
}
