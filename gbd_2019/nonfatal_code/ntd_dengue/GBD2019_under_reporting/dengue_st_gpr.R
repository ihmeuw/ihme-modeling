central_root <- 'FILEPATH'
setwd(central_root)

source('FILEPATH')
source('FILEPATH')



#24 -testing logit raking
run_id <- register_stgpr_model("FILEPATH", model_index_id = 24)
stgpr_sendoff(run_id, 'ADDRESS')
run_id <- register_stgpr_model("FILEPATH", model_index_id = 25)
stgpr_sendoff(run_id, 'ADDRESS')

run_id <- register_stgpr_model("FILEPATH", model_index_id = 26)
stgpr_sendoff(run_id, 'ADDRESS')
