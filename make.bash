#!/bin/bash
set -euo pipefail
export LC_ALL=C
export LANG=C

MPI_BUILD=${MPI_BUILD:-ompi5}
# MPI_BUILD=${MPI_BUILD:-ompi4}
# MPI_BUILD=${MPI_BUILD:-impi}

MPI_BUILD=$MPI_BUILD source scripts/envs.bash

ISOLA_HOME=${ISOLA_HOME:-${HOME}}
INSTALL_DIR=${INSTALL_DIR:-${ISOLA_HOME}/opt/massivethreads-dm}

CFLAGS=""
CFLAGS="-O3 -g $CFLAGS"

enabled_kinds=""
# disabled_kinds="SCHED THREAD"

disabled_kinds=""
# disabled_kinds="DIST_SPINLOCK_LOCK DIST_SPINLOCK_UNLOCK"

# CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure
CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure --with-comm-layer=mpi3 --prefix=${INSTALL_DIR}
# CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure --with-comm-layer=mpi3 --enable-logger --with-logger-enabled-kinds="$enabled_kinds" --with-logger-disabled-kinds="$disabled_kinds" --prefix=${INSTALL_DIR}

# debug
# DEBUG_LEVEL=3
# CFLAGS="$CFLAGS -O0 -g"
# CCFLAGS=$CFLAGS CXXFLAGS=$CFLAGS ./configure --with-comm-layer=mpi3 --with-madi-debug-level=$DEBUG_LEVEL --prefix=${INSTALL_DIR}

make clean
make -j
make install
