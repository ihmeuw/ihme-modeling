#! /bin/bash

code_path=$1
conda_path=$2
conda_env=$3

location_id=$4
version_id=$5

source ${conda_path}"FILEPATH" ${conda_env}

eval "python ${code_path} --location_id ${location_id} --version_id ${version_id} --conda_env ${conda_env}"
