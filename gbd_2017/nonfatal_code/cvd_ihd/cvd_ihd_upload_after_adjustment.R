.libPaths("FILEPATH")
suppressMessages(library(R.utils))

date <- gsub("-", "_", Sys.Date())
logs <- "FILEPATH"

suppressMessages(sourceDirectory(paste0(jpath, "FILEPATH")))

models <- unlist(subset(data.frame(get_best_model_versions("modelable_entity", c(15755, 9567))), select="model_version_id"), use.names=F)

save_results_epi(modelable_entity_id=3233, 
	description=paste0("upload after angina, HF adjustment of post-MI modeles submission models (", models[1], ", ", models[2],")"),
	measure_id="5", mark_best=TRUE, 
	input_file_pattern="{location_id}.csv",
	input_dir="FILEPATH")
