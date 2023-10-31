source("/FILEPATH/save_results_epi.R")

draw_files <- '/FILEPATH/'

draws <- 1000

description <- paste0("")

measures_to_save <- c(5, 6)

best <- T

years_to_save <- c(1990:2022)

# Major depressive disorder -----------------------------------------------
save_results_epi(modelable_entity_id = 26756, year_id = years_to_save, description=description, input_dir=paste0(draw_files, "1981/"),
                 input_file_pattern="prev_{location_id}_{year_id}.csv", mark_best = best, measure_id=measures_to_save, decomp_step = "iterative", bundle_id = 159, crosswalk_version_id = 22682)

# Anxiety disorders -------------------------------------------------------
save_results_epi(modelable_entity_id = 26759, year_id = years_to_save, description=description, input_dir=paste0(draw_files, "1989/"),
                 input_file_pattern="prev_{location_id}_{year_id}.csv", mark_best = best, measure_id=measures_to_save, decomp_step = "iterative", bundle_id = 162, crosswalk_version_id = 29924)




