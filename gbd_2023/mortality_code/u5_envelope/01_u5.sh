#! /bin/bash

code_path=$1
conda_path=$2
conda_env=$3

end_year=$4
location_id=$5
ihme_loc_id=$6
version_id=$7
version_age_sex_id=$8
births_draws_version=$9
with_shocks=${10}

source ${conda_path}"FILEPATH" ${conda_env}


eval "python ${code_path} --end_year ${end_year} --location_id ${location_id} --ihme_loc_id ${ihme_loc_id} --version_id ${version_id} --version_age_sex_id ${version_age_sex_id} --births_draws_version ${births_draws_version} --with_shocks ${with_shocks} --conda_env ${conda_env}"
