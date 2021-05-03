#!/bin/bash
set -euo pipefail
export LC_ALL=C
export LANG=C

CFLAGS=""
# CFLAGS="-g"
CFLAGS="-O0 -g"

# CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure
CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure --with-comm-layer=mpi3

make clean
make -j
