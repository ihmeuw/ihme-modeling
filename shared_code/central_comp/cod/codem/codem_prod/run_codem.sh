#!/bin/sh
#$ -S /bin/sh

kick_off="KICK_OFF.PY_PATH"

if PYTHON_PATH $kick_off $1 $2 $3 $4; then
    exit 0
else
    exit 1
fi
