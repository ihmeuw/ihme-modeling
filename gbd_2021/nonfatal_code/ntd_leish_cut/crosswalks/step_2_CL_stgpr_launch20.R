# NTDS: Cutaneous leishmaniasis 
# Purpose: CL ST-GPR runs
# Description: runs settings from config file for modelling in ST-GPR

central_root <- "FILEPATH"
setwd(central_root)

source('FILEPATH/register.R')
source('FILEPATH/sendoff.R')

##FINAL MODEL : zeta = 160154
run_id <- register_stgpr_model("FILEPATH.csv", model_index_id = "ADDRESS")
stgpr_sendoff(run_id, 'ADDRESS')
stgpr_sendoff(run_id, 'ADDRESS', log_path = 'FILEPATH')
