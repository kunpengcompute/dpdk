cmake_minimum_required(VERSION 3.16)
project(dpdk-ub-ring-demo C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_EXTENSIONS ON)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_C_FLAGS_RELEASE "-O2 -g")

# ---- DPDK (custom prefix; export PKG_CONFIG_PATH=<prefix>/lib/pkgconfig) ----
find_package(PkgConfig REQUIRED)
pkg_check_modules(DPDK REQUIRED IMPORTED_TARGET libdpdk)

# ---- ubsmem: headers/libs are on the system default paths (/usr/include,
# /usr/lib64); keep the legacy /usr/local/ubs_mem staging dir as fallback ----
if(EXISTS "/usr/local/ubs_mem/include")
    include_directories("/usr/local/ubs_mem/include")
endif()
if(EXISTS "/usr/local/ubs_mem/lib")
    link_directories("/usr/local/ubs_mem/lib")
endif()

# ---- ub_ring PMD is installed under <libdir>/dpdk/pmds-<ver>/; libdpdk.pc
# only exposes <libdir>, so add the pmds dir(s) explicitly.  Bake the custom
# prefix into the binaries' RPATH so they never pick up the system DPDK
# (23.11 in /usr/lib64). ----
set(_ub_lib_dirs ${DPDK_LIBRARY_DIRS})
foreach(_d IN LISTS DPDK_LIBRARY_DIRS)
    file(GLOB _ub_pmd_dirs "${_d}/dpdk/pmds-*")
    list(APPEND _ub_lib_dirs ${_ub_pmd_dirs})
endforeach()
list(REMOVE_DUPLICATES _ub_lib_dirs)
foreach(_d IN LISTS _ub_lib_dirs)
    link_directories(${_d})
endforeach()
set(CMAKE_BUILD_RPATH "${_ub_lib_dirs}")

# ---- Server ----
add_executable(server server.c)
target_include_directories(server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(server PRIVATE PkgConfig::DPDK rte_net_ub_ring pthread dl numa)

# ---- Client ----
add_executable(client client.c)
target_include_directories(client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(client PRIVATE PkgConfig::DPDK rte_net_ub_ring pthread dl numa)
