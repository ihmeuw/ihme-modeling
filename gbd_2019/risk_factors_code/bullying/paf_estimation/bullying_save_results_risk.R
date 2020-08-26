
source("/FILEPATH/save_results_risk.R")

me_id <- 20015
description <- "Decomp 3: DisMod exp #422534 ST-GPR prop in school #87263 Annual estimates."
input_dir <- "/FILEPATH/"
input_file_pattern <- "paf_{measure}_{location_id}_{year_id}_{sex_id}.csv"
risk_type <- "paf"
mark_best <- T
measure_id <- 3

save_results_risk(modelable_entity_id = me_id, description=description, input_dir=input_dir, input_file_pattern=input_file_pattern, risk_type = risk_type, mark_best = mark_best, year_id = c(1990:2019), measure_id=measure_id, decomp = 'step4')

