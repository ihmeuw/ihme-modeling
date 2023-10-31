#! /bin/bash

code_path=$1
conda_path=$2
conda_env=$3

version_id=$4
ddm_estimate_version=$5
code_dir=$6

source ${conda_path}/bin/activate ${conda_env}

eval "python ${code_path} --version_id ${version_id} --ddm_estimate_version ${ddm_estimate_version} --code_dir ${code_dir} --conda_env ${conda_env}"