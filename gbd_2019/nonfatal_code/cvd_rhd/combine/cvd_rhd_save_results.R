################### SAVE RESULTS #########################################
######################################################
.libPaths("FILEPATH")
suppressMessages(library(R.utils))

sourceDirectory("FILEPATH")

save_results_epi(modelable_entity_id="VALUE", input_dir = "FILEPATH", 
				 input_file_pattern="{measure_id}_{location_id}.csv", description="Decomp 4 upload", mark_best = T, decomp_step="step4")