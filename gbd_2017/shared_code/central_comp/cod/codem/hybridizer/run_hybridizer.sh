#!/bin/sh

# Absolute path to this script
SCRIPT=$(readlink -f "$0")
# Absolute path to the directory this script is in
SCRIPTPATH=$(dirname "$SCRIPT")

kick_off=$FILEPATH.py

# Input format: user, global model number, data rich model number, server

FILEPATH $kick_off $1 $2 $3 $4
