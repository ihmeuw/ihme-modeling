#! /bin/bash -x

code_path=$1
conda_path=$2
conda_env=$3

version_id=$4
year_start=$5
year_end=$6
loop=$7
model_age=$8
loc=$9

source ${conda_path}/bin/activate ${conda_env}

eval "python ${code_path} --version_id ${version_id} --year_start ${year_start} --year_end ${year_end} --loop ${loop} --model_age ${model_age} --loc ${loc}"