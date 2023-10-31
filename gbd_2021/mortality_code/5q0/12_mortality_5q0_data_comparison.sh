#! /bin/bash

code_path=$1
conda_path=$2
conda_env=$3

version_id=$4
gbd_year=$5
end_year=$6
code_dir=$7

source ${conda_path}/bin/activate ${conda_env}

eval "python ${code_path} --version_id ${version_id} --gbd_year ${gbd_year} --end_year ${end_year} --code_dir ${code_dir} --conda_env ${conda_env}"