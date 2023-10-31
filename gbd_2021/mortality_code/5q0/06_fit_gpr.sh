#! /bin/bash

code_path=$1
conda_path=$2
conda_env=$3

version_id=$4
location_id=$5
ihme_loc_id=$6
code_dir=$7

source ${conda_path}/bin/activate ${conda_env}

eval "python ${code_path} --version_id ${version_id} --location_id ${location_id} --ihme_loc_id ${ihme_loc_id} --code_dir ${code_dir}"