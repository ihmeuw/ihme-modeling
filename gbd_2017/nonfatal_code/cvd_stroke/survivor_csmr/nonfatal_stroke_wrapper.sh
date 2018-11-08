#!FILEPATH bash

#$ -N stroke_intermediary
#$ -cwd
#$ -P proj_custom_models
#$ -l mem_free=32G
#$ -pe multi_slot 16
#$ -o FILEPATH/output
#$ -e FILEPATH/errors

echo "Step currently being run is $1"
source FILEPATH gbd_env
python FILEPATH/00_master.py "$@"
