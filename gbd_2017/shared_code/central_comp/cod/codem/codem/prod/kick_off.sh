#!/bin/sh

kick_off=FILEPATH

if FILEPATH $kick_off $1; then
    exit 0
else
    exit 1
fi
