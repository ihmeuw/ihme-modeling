.libPaths(FILEPATH)
suppressMessages(library(R.utils))

date <- gsub("-", "_", Sys.Date())
logs <- "FILEPATH"

suppressMessages(sourceDirectory(paste0(FILEPATH)))

models <- unlist(subset(data.frame(get_best_model_versions("modelable_entity", c(9575))), select="model_version_id"), use.names=F)

save_results_epi(modelable_entity_id=2908, description=paste0("upload after MEPS ratio adjustment of HF due to other CVD (", models, ")"),
				 measure_id="5", mark_best=TRUE, 
				 input_file_pattern="{measure_id}_{location_id}.csv",
				 input_dir="FILEPATH")


