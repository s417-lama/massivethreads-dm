#!/bin/bash
set -euo pipefail
export LC_ALL=C
export LANG=C

CFLAGS=""
# CFLAGS="-O0 -g"

CFLAGS=$CFLAGS ./configure
make -j
