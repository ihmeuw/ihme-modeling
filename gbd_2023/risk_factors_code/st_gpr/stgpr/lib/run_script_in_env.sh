#!/bin/sh

# ${1##*/} is the environment name. ## says to keep everything after the last (*) delimiter / in $1
# ${@:2} passes through all of the arguments passed to this script except the first. This removes
# the path to the Python interpreter from the subsequent call.
source $1/../../bin/activate ${1##*/} && $1/bin/python "${@:2}"