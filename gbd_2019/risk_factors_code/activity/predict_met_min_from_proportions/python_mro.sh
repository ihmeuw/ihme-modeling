#!/bin/sh
set -x
PYTHONSCRIPT="$1"
ENV=FILEPATH/crosswalk_env
if [[ ! -e "$ENV" ]]
then
  echo Cannot find environment "$ENV"
  exit 7
fi

if [[ ! -e "$PYTHONSCRIPT" ]]
then
  echo Cannot find python script $PYTHONSCRIPT
  exit 8
fi
env | sort
echo `which conda`

conda activate "$ENV"
echo `which python`

python "$@"