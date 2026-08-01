cmake_minimum_required(VERSION 3.16)
project(dpdk-ub-ring-demo C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_EXTENSIONS ON)
set(CMAKE_C_FLAGS_RELEASE "-O2")

# ---- DPDK ----
find_package(PkgConfig REQUIRED)
pkg_check_modules(DPDK REQUIRED IMPORTED_TARGET libdpdk)

# ---- ubsmem (needed because DPDK's lib/ub_ring links it) ----
set(UBSM_DIR "/usr/local/ubs_mem")
include_directories(${UBSM_DIR}/include)
link_directories(${UBSM_DIR}/lib)

# ---- Server ----
add_executable(server server.c)
target_include_directories(server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(server PRIVATE PkgConfig::DPDK pthread dl numa)

# ---- Client ----
add_executable(client client.c)
target_include_directories(client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(client PRIVATE PkgConfig::DPDK pthread dl numa)
