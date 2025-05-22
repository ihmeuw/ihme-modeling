#!/bin/bash
source FILEPATH/activate gbd_env
conda activate winnower
run_extract --no-save-config --topic "$@"
conda deactivate
conda deactivate
