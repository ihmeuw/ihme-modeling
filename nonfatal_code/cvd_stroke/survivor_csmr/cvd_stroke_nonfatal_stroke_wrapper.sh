#!/usr/bin/env bash

#$ -N stroke_intermediary
#$ -cwd
#$ -P proj_custom_models
#$ -l mem_free=32G
#$ -pe multi_slot 16
#$ -o FILEPATH
#$ -e FILEPATH

echo "Step currently being run is $1"
source FILEPATH/activate gbd_env
python FILEPATH/00_master.py "$@"
