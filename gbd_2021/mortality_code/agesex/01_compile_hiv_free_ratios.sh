#! /bin/bash

code_path=$1
conda_path=$2
conda_env=$3

version_id=$4
hiv_version=$5

source ${conda_path}/bin/activate ${conda_env}

eval "python ${code_path} --version_id ${version_id} --hiv_version ${hiv_version}"