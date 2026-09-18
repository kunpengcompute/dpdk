#!/usr/bin/env bash
# ============================================================================
#  DEBUG build for DPDK_ub on 90.91.177.156
#
#  Same one-click flow as build_release.sh, but everything is compiled
#  with -g -O0 so gdb produces useful line info and un-optimized code:
#    - DPDK meson:        --buildtype=debug   (= -O0 -g; also auto-enables
#                         UB_RING_FLOW_TRACE flow tracing in
#                         drivers/net/ub_ring/meson.build)
#    - demo2/demo3 cmake: -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS_DEBUG="-g -O0"
#
#  Note: release and debug use SEPARATE build dirs (build-release /
#  build-debug), so you can switch freely; both install into the same
#  PREFIX (default <repo>/install) — the last build you ran is the one
#  demo2 links against.
#
#  Usage:  [PREFIX=/some/non-system/dir] ./scripts/build_debug.sh
#  Then debug with:  gdb --args $SRC_DIR/demo2/build-debug/server ...
# ============================================================================
set -euo pipefail

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PREFIX="${PREFIX:-$SRC_DIR/install}"          # non-system install prefix
BUILD_TYPE="debug"
BUILD_DIR="${BUILD_DIR:-$SRC_DIR/build-debug}"        # DPDK meson build dir
DEMO_BUILD_DIR="$SRC_DIR/demo2/build-debug"           # demo2 cmake build dir
DEMO3_BUILD_DIR="$SRC_DIR/demo3/build-debug"          # demo3 cmake build dir
JOBS="$(nproc)"
LIBDIR="lib"                                  # deterministic layout: $PREFIX/lib

export PKG_CONFIG_PATH="$PREFIX/$LIBDIR/pkgconfig"

say() { printf '\033[1;32m[build:%s] %s\033[0m\n' "$BUILD_TYPE" "$*"; }
die() { printf '\033[1;31m[build:%s][ERROR] %s\033[0m\n' "$BUILD_TYPE" "$*" >&2; exit 1; }

# ------------------------------------------------ [0/5] prerequisites -------
say "==> [0/5] checking build tools"
for t in meson ninja cmake gcc pkg-config python3 gdb; do
    command -v "$t" >/dev/null 2>&1 ||
        die "'$t' not found.  The server is offline: run scripts/fetch_deps.sh on an online machine, copy deps/ here, then scripts/install_deps.sh"
done
python3 -c 'import elftools' 2>/dev/null ||
    die "python3 module 'pyelftools' is required for the DPDK build (scripts/install_deps.sh)"
MESON_VER="$(meson --version)"
printf '%s\n' "$MESON_VER" | awk -F. 'NR==1{ if($1>1 || ($1==1 && $2>=1)) exit 0; exit 1 }' ||
    die "meson >= 1.1.0 required, got $MESON_VER"
say "    meson=$MESON_VER  ninja=$(ninja --version)  cmake=$(cmake --version | head -1 | awk '{print $3}')  gdb=$(gdb --version | head -1 | awk '{print $4}')"

# ------------------------------------------------ [1/5] configure DPDK ------
say "==> [1/5] meson setup (buildtype=$BUILD_TYPE => -O0 -g, prefix=$PREFIX, libdir=$LIBDIR)"
MESON_OPTS=(--prefix="$PREFIX" --libdir="$LIBDIR" --buildtype=$BUILD_TYPE -Dplatform=generic)
if [ -d "$BUILD_DIR" ]; then
    meson setup --reconfigure "$BUILD_DIR" "$SRC_DIR" "${MESON_OPTS[@]}"
else
    meson setup "$BUILD_DIR" "$SRC_DIR" "${MESON_OPTS[@]}"
fi

# ------------------------------------------------ [2/5] build DPDK ----------
say "==> [2/5] ninja build (-j$JOBS)"
ninja -C "$BUILD_DIR" -j"$JOBS"

# ------------------------------------------------ [3/5] install DPDK --------
say "==> [3/5] install into $PREFIX"
ninja -C "$BUILD_DIR" install

ls -1 "$PREFIX/$LIBDIR"/librte_eal.so.* >/dev/null 2>&1 ||
    die "install failed: librte_eal not found in $PREFIX/$LIBDIR"
PMD_FILE="$(ls "$PREFIX/$LIBDIR/dpdk"/pmds-*/librte_net_ub_ring.so* 2>/dev/null | head -1)"
[ -n "$PMD_FILE" ] ||
    die "install failed: librte_net_ub_ring not found under $PREFIX/$LIBDIR/dpdk/pmds-*"
PMD_DIR="$(dirname "$PMD_FILE")"
say "    PMD installed: $PMD_DIR"

# ------------------------------------------------ [4/5] build demos ---------
say "==> [4/5] cmake build demo2 + demo3 (Debug, -g -O0; fresh build dirs)"
rm -rf "$DEMO_BUILD_DIR" "$DEMO3_BUILD_DIR"
cmake -S "$SRC_DIR/demo2" -B "$DEMO_BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS_DEBUG="-g -O0"
cmake --build "$DEMO_BUILD_DIR" -j"$JOBS"
cmake -S "$SRC_DIR/demo3" -B "$DEMO3_BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS_DEBUG="-g -O0"
cmake --build "$DEMO3_BUILD_DIR" -j"$JOBS"

# ------------------------------------------------ [5/5] verify linkage ------
say "==> [5/5] verify demo2 links OUR DPDK (24.11.7-rc1), not the system one (23.11)"
PKGCONF_VER="$(pkg-config --modversion libdpdk)"
say "    pkg-config libdpdk = $PKGCONF_VER   (system RPM is 23.11)"
echo "    --- ldd (librte/ubsm lines): demo2 client ---"
ldd "$DEMO_BUILD_DIR/client" | grep -E 'librte|ubsm' || true
echo "    --- ldd (librte/ubsm lines): demo3 client (no rte_net_ub_ring expected) ---"
ldd "$DEMO3_BUILD_DIR/client" | grep -E 'librte|ubsm' || true
echo "    --- RPATH/RUNPATH ---"
readelf -d "$DEMO_BUILD_DIR/client" | grep -E 'RPATH|RUNPATH' || true

printf '%s\n' \
    "" \
    "Build OK.  ($BUILD_TYPE: -O0 -g, UB_RING_FLOW_TRACE on)" \
    "  DPDK prefix   : $PREFIX   (system DPDK 23.11 in /usr/lib64 untouched)" \
    "  demo2 binaries: $DEMO_BUILD_DIR/client  $DEMO_BUILD_DIR/server" \
    "  demo3 binaries: $DEMO3_BUILD_DIR/client  $DEMO3_BUILD_DIR/server  (ring index only, no mbuf/PMD)" \
    "" \
    "Run (as root; UB devices already loaded, no hugepages needed):" \
    "  demo2: cd $DEMO_BUILD_DIR; ./server --no-huge -l 1-6 --nq=4 [--pn=master] [--un=worker1] [--time=10]" \
    "         ./client --no-huge -l 1-6 --ns=2 --pktlen=500 [--pn=master] [--un=worker1] [--time=10]" \
    "  demo3: cd $DEMO3_BUILD_DIR; ./server --no-huge -l 1-6 --nq=4 [--pn=master] [--un=worker1] [--time=10]" \
    "         ./client --no-huge -l 1-6 --ns=2 --elt=128 [--pn=master] [--un=worker1] [--time=10]" \
    "" \
    "Debug (break e.g. vdev_probe_all_drivers / rte_eth_ub_ring_attach):" \
    "  gdb --args $DEMO_BUILD_DIR/server --no-huge -l 1-6 --nq=4 --pn=master --un=worker1 --time=5" \
    "  (gdb) set pagination off" \
    "  (gdb) break vdev_probe_all_drivers" \
    "  (gdb) run" \
    "" \
    "client/server carry an RPATH to $PREFIX, so they resolve librte_*.so.25 from" \
    "$PREFIX (demo2 additionally pulls librte_net_ub_ring.so) and never from /usr/lib64."
