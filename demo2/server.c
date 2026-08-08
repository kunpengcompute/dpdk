/* SPDX-License-Identifier: BSD-3-Clause
 * Server (provider) — ub_ring ethdev PMD.
 * Run: ./server --no-huge -l 0 -d librte_net_ub_ring.so
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rte_eal.h>
#include <rte_mbuf.h>
#include <rte_ethdev.h>
#include <rte_errno.h>
#include <rte_eth_ub_ring.h>

#define NUM_PACKETS 10
#define BURST_SIZE  32

int main(int argc, char **argv)
{
	if (rte_eal_init(argc, argv) < 0) { fprintf(stderr,"[Server] eal fail\n"); return 1; }
	printf("\n===== Server (provider) =====\n\n");

	const char *p_nodes[] = {"master"}; /* provider nodes */
	const char *u_nodes[] = {"worker1"}; /* user nodes */

	int pid = rte_eth_ub_ring_create_provider("server_eth",
		p_nodes, 1, u_nodes, 1, "region1", "ubring",
		1UL*1024*1024*1024, 1, 1024);
	if (pid < 0) { fprintf(stderr,"[Server] create fail: %d\n",pid); return 1; }
	printf("[Server] port=%d\n", pid);

	struct rte_eth_conf conf = { .rxmode={.max_lro_pkt_size=RTE_ETHER_MAX_LEN} };
	if (rte_eth_dev_configure(pid,1,1,&conf)<0 || rte_eth_dev_start(pid)<0)
		{ fprintf(stderr,"[Server] config/start fail\n"); rte_eth_dev_close(pid); return 1; }

	struct rte_mbuf *pkts[BURST_SIZE]; int rx=0;
	printf("[Server] listening...\n\n");
	while (rx < NUM_PACKETS) {
		uint16_t n=rte_eth_rx_burst(pid,0,pkts,BURST_SIZE);
		if (!n) { rte_delay_us(1000); continue; }
		for (uint16_t i=0; i<n; i++) {
			uint32_t len=rte_pktmbuf_data_len(pkts[i]);
			printf("[Server] RX(%uB): %.*s\n",len,len,rte_pktmbuf_mtod(pkts[i],char*));
			rx++;
			while (!rte_eth_tx_burst(pid,0,&pkts[i],1));
			rte_pktmbuf_free(pkts[i]);
		}
	}
	printf("\n[Server] done (%d)\n",rx);
	rte_eth_dev_stop(pid); rte_eth_dev_close(pid); rte_eal_cleanup();
	return (rx==NUM_PACKETS)?0:1;
}
