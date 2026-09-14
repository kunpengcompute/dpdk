#!/usr/bin/env bash
# ============================================================================
#  Run this on ANY machine that has internet + python3/pip (e.g. your PC or
#  any dev box).  It downloads the 3 offline wheels needed to build DPDK_ub:
#    meson, ninja (aarch64 linux), pyelftools
#  Then copy the whole deps/ directory to the server and run:
#    bash scripts/install_deps.sh
# ============================================================================
set -euo pipefail
OUT="${1:-deps}"
mkdir -p "$OUT"

echo "==> meson + pyelftools (pure python, any platform)"
pip download meson pyelftools -d "$OUT"

echo "==> ninja (aarch64 linux)"
if uname -m | grep -qi aarch64; then
    pip download ninja -d "$OUT"     # online machine is already aarch64 linux
else
    pip download ninja \
        --platform manylinux_2_17_aarch64 \
        --only-binary=:all: \
        --python-version 3 --implementation py --abi none -d "$OUT" || {
        echo "pip could not fetch an aarch64 ninja wheel; falling back to GitHub binary"
        (cd "$OUT" && curl -fLO https://github.com/ninja-build/ninja/releases/download/v1.12.1/ninja-linux-aarch64.zip \
            && unzip -o ninja-linux-aarch64.zip)
    }
fi

echo
echo "==> done. Copy '$OUT' to the server (e.g. /tmp/deps), then on the server run:"
echo "    bash scripts/install_deps.sh /tmp/deps"
ls -la "$OUT"
