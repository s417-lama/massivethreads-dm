#!/bin/bash
set -euo pipefail
export LC_ALL=C
export LANG=C

source scripts/envs.bash

ISOLA_HOME=${ISOLA_HOME:-${HOME}}
INSTALL_DIR=${INSTALL_DIR:-${ISOLA_HOME}/opt/massivethreads-dm}

CFLAGS=""
# CFLAGS="-g"
# CFLAGS="$CFLAGS -DMADM_LOGGER_ENABLE=1"

# CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure
CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure --with-comm-layer=mpi3 --prefix=${INSTALL_DIR}

# debug
# DEBUG_LEVEL=0
# CFLAGS="$CFLAGS -O0 -g"
# CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure --with-comm-layer=mpi3 --with-madi-debug-level=$DEBUG_LEVEL --prefix=${INSTALL_DIR}

make clean
make -j
make install
