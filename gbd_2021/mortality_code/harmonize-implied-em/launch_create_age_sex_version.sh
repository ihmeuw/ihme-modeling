#!/bin/bash

#SBATCH -J create_age_sex_version
#SBATCH -t 03:00:00
#SBATCH --mem=112G
#SBATCH -c 16
#SBATCH -o /ihme/temp/slurmoutput/%u/output/%x.o%j
#SBATCH -e /ihme/temp/slurmoutput/%u/errors/%x.e%j
#SBATCH -A proj_mortenvelope
#SBATCH -p all.q

CODE_DIR="/ihme/code/mortality/$USER/requests/shocks/gbd_2020/harmonize_implied_em"
SHELL="/ihme/singularity-images/rstudio/shells/execR.sh"

${SHELL} -s ${CODE_DIR}/create_age_sex_version.R
