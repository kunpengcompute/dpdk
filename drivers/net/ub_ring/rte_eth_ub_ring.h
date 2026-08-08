/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef RTE_ETH_UB_RING_H
#define RTE_ETH_UB_RING_H

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_dev.h>
#include <rte_ethdev.h>

#ifdef __cplusplus
extern "C" {
#endif

static inline int
rte_eth_ub_ring_create_provider(const char *name,
	const char **p_nodes, uint32_t np, const char **u_nodes, uint32_t nu,
	const char *region, const char *pre,
	size_t shm_sz, uint32_t nq, uint32_t qd)
{
	char da[1024], vn[64], pns[64]="", uns[64]="";
	snprintf(vn,64,"net_ub_ring_%s",name);
	for (uint32_t i=0; i<np && strlen(pns)<50; i++)
		{ if(i) strcat(pns,"|"); strncat(pns,p_nodes[i],15); }
	for (uint32_t i=0; i<nu && strlen(uns)<50; i++)
		{ if(i) strcat(uns,"|"); strncat(uns,u_nodes[i],15); }
	snprintf(da,sizeof(da),"%s,r=pv,pn=%s,un=%s,np=%u,nu=%u,reg=%s,pre=%s,sz=%zu,nq=%u,qd=%u",
		vn,pns,uns,np,nu,region,pre,shm_sz,nq,qd);
	int ret=rte_dev_probe(da);
	if (ret) return -1;
	uint16_t pid; ret=rte_eth_dev_get_port_by_name(name,&pid);
	return ret?-1:(int)pid;
}

static inline int
rte_eth_ub_ring_create_user(const char *name,
	const char **p_nodes, uint32_t np, const char **u_nodes, uint32_t nu,
	const char *region, const char *pre, uint32_t elt)
{
	char da[1024], vn[64], pns[64]="", uns[64]="";
	snprintf(vn,64,"net_ub_ring_%s",name);
	for (uint32_t i=0; i<np && strlen(pns)<50; i++)
		{ if(i) strcat(pns,"|"); strncat(pns,p_nodes[i],15); }
	for (uint32_t i=0; i<nu && strlen(uns)<50; i++)
		{ if(i) strcat(uns,"|"); strncat(uns,u_nodes[i],15); }
	snprintf(da,sizeof(da),"%s,r=us,pn=%s,un=%s,np=%u,nu=%u,reg=%s,pre=%s,de=%u",
		vn,pns,uns,np,nu,region,pre,elt);
	int ret=rte_dev_probe(da);
	if (ret) return -1;
	uint16_t pid; ret=rte_eth_dev_get_port_by_name(name,&pid);
	return ret?-1:(int)pid;
}

int rte_eth_ub_ring_get_queue_mapping(uint32_t pi, uint32_t sq,
	uint32_t *tx, uint32_t *rx);

#ifdef __cplusplus
}
#endif
#endif
