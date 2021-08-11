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

elif [[ $(hostname) =~ "ito" ]] || [[ $(hostname) =~ "sca" ]]; then
  export MACHINE_NAME=ito

  module load gcc/10.2.0
  # export PATH=$HOME/opt/openmpi/bin:$PATH
  export PATH=$HOME/opt/openmpi5/bin:$PATH

else
  export MACHINE_NAME=local

  export PATH=$HOME/opt/openmpi5/bin:$PATH
fi

export MY_ENVS_LOADED=true
