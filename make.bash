#!/bin/bash
set -euo pipefail
export LC_ALL=C
export LANG=C

ISOLA_HOME=${ISOLA_HOME:-${HOME}}
INSTALL_DIR=${INSTALL_DIR:-${ISOLA_HOME}/opt/massivethreads-dm}

CFLAGS=""
# CFLAGS="-g"
# CFLAGS="-O0 -g"

# CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure
CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure --with-comm-layer=mpi3 --prefix=${INSTALL_DIR}

make clean
make -j
make install
