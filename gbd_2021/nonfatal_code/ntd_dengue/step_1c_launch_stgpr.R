
### NTDs Dengue
#Description: launch ST-GPR models

central_root <- "FILEPATH"
setwd(central_root)

source("FILEPATH/register.R")
source("FILEPATH/sendoff.R")


#Updates for UR model, covars

##FINAL MODEL
run_id <- register_stgpr_model("FILEPATH.csv", model_index_id = "ADDRESS")
stgpr_sendoff(run_id, "ADDRESS")
#"ADDRESS" run id
