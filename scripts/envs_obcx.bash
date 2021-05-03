#!/bin/bash

ISOLA_ROOT=${ISOLA_ROOT:-${HOME}/.isola}
export PATH=${ISOLA_ROOT}/bin:$PATH
export PATH=/work/gc64/c64050/.local/bin:$PATH
export MACHINE_NAME=obcx

module unload intel
module load gcc/7.5.0
