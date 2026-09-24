cmake_minimum_required(VERSION 3.16)
project(dpdk-ub-ring-demo3 C)

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

# ---- demo3 不走 ethdev PMD：只链 DPDK 本体（含 lib/ub_ring），
# 不链 rte_net_ub_ring，也不做任何 mbuf/mempool 操作。
# 但仍需要 RPATH 指向自定义 DPDK prefix，否则运行时会去 /usr/lib64
# 找系统 DPDK 23.11（无 rte_ub_ring 符号）。 ----
set(CMAKE_BUILD_RPATH "${DPDK_LIBRARY_DIRS}")

# ---- Server ----
add_executable(server server.c)
target_include_directories(server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(server PRIVATE PkgConfig::DPDK pthread dl numa)

# ---- Client ----
add_executable(client client.c)
target_include_directories(client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(client PRIVATE PkgConfig::DPDK pthread dl numa)
