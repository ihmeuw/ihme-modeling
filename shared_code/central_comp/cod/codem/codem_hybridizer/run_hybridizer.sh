#!/bin/sh

# Absolute path to this script
SCRIPT=$(readlink -f "$0")
# Absolute path to the directory this script is in
SCRIPTPATH=$(dirname "$SCRIPT")

kick_off=$SCRIPTPATH/submit_hybrid.py

# Input format: user, global model number, data rich model number, server

PYTHON_PATH $kick_off $1 $2 $3 $4
