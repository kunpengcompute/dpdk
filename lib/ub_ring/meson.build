# SPDX-License-Identifier: BSD-3-Clause

sources = files('rte_ub_ring.c')
headers = files('rte_ub_ring.h')

# External ubsmem dependency
ubsmem_inc = include_directories('/usr/local/ubs_mem/include')
includes += ubsmem_inc

ubsmem_lib = cc.find_library('ubsm_sdk', dirs: ['/usr/local/ubs_mem/lib'],
                              required: false)
if ubsmem_lib.found()
    ext_deps += ubsmem_lib
else
    ext_deps += declare_dependency(
        link_args: ['-L/usr/local/ubs_mem/lib', '-lubsm_sdk'])
endif
