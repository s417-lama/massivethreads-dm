#!/bin/bash

if ${MY_ENVS_LOADED:-false}; then
  return
fi

if [[ $(hostname) =~ "obcx" ]]; then
  export MACHINE_NAME=obcx

  export PATH=/work/gc64/c64050/.isola/bin:$PATH
  export PATH=/work/gc64/c64050/.local/bin:$PATH

  module unload intel
  module load gcc/7.5.0

  # Open MPI
  module unload impi
  export PATH=/work/gc64/c64050/opt/openmpi/bin:$PATH
else
  export MACHINE_NAME=local
fi

export MY_ENVS_LOADED=true
