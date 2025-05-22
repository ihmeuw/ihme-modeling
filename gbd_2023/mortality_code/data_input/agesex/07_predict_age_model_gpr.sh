#! /bin/bash

code_path=$1
conda_path=$2
conda_env=$3

version_id=$4
ihme_loc_id=$5
code_dir=$6

source ${conda_path}"FILEPATH" ${conda_env}

eval "python ${code_path} --version_id ${version_id} --ihme_loc_id ${ihme_loc_id} --code_dir ${code_dir}"
