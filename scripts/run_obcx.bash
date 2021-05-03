#!/bin/bash
set -euo pipefail
export LC_ALL=C
export LANG=C

cd -P $(dirname $0)/..

COMMANDS_FILE=results/commands.ignore

COMMANDS=()
whitespace="[[:space:]]"
for i in "${@:2}"; do
  if [[ $i =~ $whitespace ]]; then
    i=\\\"$i\\\"
  fi
  COMMANDS+=("$i")
done

JOB_COMMANDS="
(
  ISOLA_ROOT=/work/gc64/c64050/.isola source ./scripts/envs_obcx.bash
  isola create -o test bash -c \""${COMMANDS[@]}"\"
)
"

echo -e "$JOB_COMMANDS" >> $COMMANDS_FILE
