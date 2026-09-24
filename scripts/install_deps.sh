#!/usr/bin/env bash
# ============================================================================
#  Run on the SERVER (offline) after copying the deps/ dir from an online
#  machine.  Usage:  bash scripts/install_deps.sh [path-to-deps-dir]
#  Installs: meson, ninja, pyelftools  (build tools only — nothing DPDK-related,
#  and the system DPDK 23.11 is not touched).
#
#  After this, build with either:
#    bash scripts/build_release.sh   # optimized, for running / testing
#    bash scripts/build_debug.sh     # -g -O0 for DPDK + demo2 (gdb-ready)
#
#  gdb is only VERIFIED here (needed to debug the debug build).  It ships
#  with the OS, not pip, so on a fresh server install it via the OS repos
#  (dnf install gdb) — on this offline box it is already at /usr/bin/gdb.
# ============================================================================
set -euo pipefail
DEPS="${1:-$(cd "$(dirname "$0")/.." && pwd)/deps}"
[ -d "$DEPS" ] || { echo "deps dir not found: $DEPS"; exit 1; }

echo "==> installing meson + pyelftools"
pip install --no-index --find-links="$DEPS" meson pyelftools

echo "==> installing ninja"
if ls "$DEPS"/ninja-*.whl >/dev/null 2>&1; then
    pip install --no-index --find-links="$DEPS" ninja
elif [ -f "$DEPS/ninja" ]; then
    install -m 0755 "$DEPS/ninja" /usr/local/bin/ninja
else
    echo "ERROR: no ninja wheel or raw binary found in $DEPS"; exit 1
fi

echo
echo "==> verify:"
command -v meson && meson --version
command -v ninja && ninja --version
python3 -c 'import elftools; print("pyelftools", elftools.__version__)'
if command -v gdb >/dev/null 2>&1; then
    gdb --version | head -1
else
    echo "WARNING: gdb not found — debug builds need it (install via OS repos: dnf install gdb)"
fi
