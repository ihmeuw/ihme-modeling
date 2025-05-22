#! /bin/bash

code_path=$1
conda_path=$2
conda_env=$3

version_id=$4
gbd_year=$5
emp_death_version=$6
code_dir=$7

source ${conda_path}"FILEPATH" ${conda_env}

eval "python ${code_path} --version_id ${version_id} --gbd_year ${gbd_year} --emp_death_version ${emp_death_version} --code_dir ${code_dir} --conda_env ${conda_env}"
