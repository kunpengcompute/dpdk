cmake_minimum_required(VERSION 3.16)
project(dpdk-ub-ring-demo C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_EXTENSIONS ON)
set(CMAKE_C_FLAGS_RELEASE "-O2")

# ---- ubsmem ----
set(UBSM_DIR "/usr/local/ubs_mem")
include_directories(${UBSM_DIR}/include)
link_directories(${UBSM_DIR}/lib)
find_library(UBSM_LIB NAMES ubsm_sdk
    PATHS ${UBSM_DIR}/lib NO_DEFAULT_PATH)
if(NOT UBSM_LIB)
    message(FATAL_ERROR "ubsm_sdk not found in ${UBSM_DIR}/lib")
endif()

# ---- DPDK (从源码构建) ----
find_package(PkgConfig REQUIRED)
pkg_check_modules(DPDK REQUIRED IMPORTED_TARGET libdpdk)

# ---- 服务端 ----
add_executable(server server.c)
target_include_directories(server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(server PRIVATE PkgConfig::DPDK ${UBSM_LIB} pthread dl numa)

# ---- 客户端 ----
add_executable(client client.c)
target_include_directories(client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(client PRIVATE PkgConfig::DPDK ${UBSM_LIB} pthread dl numa)
