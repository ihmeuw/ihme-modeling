# .libPaths("FILEPATH")
# source("FILEPATH")

suppressMessages(library(R.utils))

date <- gsub("-", "_", Sys.Date())

args <- commandArgs(trailingOnly = TRUE)
decomp_step <- args[1]

#decomp_step <- 'step1'
# logs <- "/FILEPATH/"

suppressMessages(sourceDirectory(paste0("/FILEPATH/")))

models <- unlist(subset(data.frame(get_best_model_versions("modelable_entity", c(15755, 9567), decomp_step=decomp_step, gbd_round_id=7)), select="model_version_id"), use.names=F)

save_results_epi(modelable_entity_id=3233, 
	description=paste0("upload after angina, HF adjustment of post-MI modelest submission models (", models[1], ", ", models[2],")"),
	measure_id="5", mark_best=TRUE, 
	gbd_round_id = 7, decomp_step = decomp_step, 
	input_file_pattern="{location_id}.csv",
	input_dir=paste0("/FILEPATH/", decomp_step, "_", date),
	bundle_id = 583, crosswalk_version_id=34925)
