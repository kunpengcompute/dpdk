# SPDX-License-Identifier: BSD-3-Clause

sources = files('rte_ub_ring.c')
headers = files('rte_ub_ring.h')
deps += 'ring'

# data-path flow-trace logging: debug builds only
if get_option('buildtype').startswith('debug')
    cflags += '-DUB_RING_FLOW_TRACE'
endif

# External ubsmem dependency.
# On the target machine ubs_mem.h ships in the default include path
# (/usr/include) and libubsm_sdk.so in the default lib path (/usr/lib64).
# The legacy /usr/local/ubs_mem staging layout is still probed as a
# fallback so both layouts build without further configuration.
ubsmem_inc_dir = '/usr/local/ubs_mem/include'
if cc.has_header('ubs_mem.h')
    ubsmem_inc_dir = ''   # found on the default include path
endif
if ubsmem_inc_dir != ''
    includes += include_directories(ubsmem_inc_dir)
endif

ubsmem_lib = cc.find_library('ubsm_sdk',
                             dirs: ['/usr/lib64', '/usr/lib',
                                    '/usr/local/ubs_mem/lib'],
                             required: false)
if not ubsmem_lib.found()
    error('libubsm_sdk.so not found: checked /usr/lib64, /usr/lib and ' +
          '/usr/local/ubs_mem/lib')
endif
ext_deps += ubsmem_lib
