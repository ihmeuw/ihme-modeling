# prep LRI ratio for NF upload
rm(list=ls())
invisible(sapply(list.files("filepath", full.names = T), source))
in_dir <- "filepath/2021_06_22_lri.csv"
date <- gsub("-", "_", Sys.Date())
out_dir <- paste0("/filepath", date, "/")
dir.create(out_dir, showWarnings = F)
all_wide <- fread(in_dir)
all_wide$cause_id <- NULL
age_meta <- get_age_metadata(age_group_set_id = 19, gbd_round_id = 7)
hierarchy <- get_location_metadata(location_set_id = 35, gbd_round_id = 7, decomp_step = "iterative")
# Multiply these ratio draws by all measures in get_model_results and re-upload
lri_draws <- get_draws(gbd_id_type = "modelable_entity_id",
                       gbd_id = 1258,
                       source = "epi",
                       status = "best",
                       gbd_round_id = 7,
                       measure_id = c(5,6), # incidence and prevalence
                       decomp_step = "iterative",
                       location_id = hierarchy[most_detailed == 1]$location_id,
                       age_group_id = age_meta$age_group_id,
                       sex_id = c(1,2),
                       year_id = 1990:2022,
                       num_workers = 10)
merged <- merge(lri_draws, all_wide, by = c("location_id", "year_id", "sex_id", "age_group_id"))
end <- 999
merged <- merged[, paste0("draw_",0:end) := lapply(0:end, function(x) {
  get(paste0("draw_", x)) * get(paste0("ratio_draw_",x))
  })]
merged <- merged[,paste0("ratio_draw_",0:end):=NULL]
lri_draws_edited <- rbind(lri_draws[year_id < 2020], merged, lri_draws[year_id > 2020])
lri_draws_edited[, `:=` (modelable_entity_id = NULL, model_version_id = NULL)]

pblapply(unique(lri_draws_edited$location_id), function(location){
  dt_write <- lri_draws_edited[location_id == location]
  write.csv(dt_write, paste0(out_dir, location, ".csv"), row.names = F)
}, cl = 80)
if(length(list.files(out_dir))!=nrow(hierarchy[most_detailed==1]))stop("locations missing in out_dir")

ids <- get_elmo_ids(gbd_round_id = 7, decomp_step = "iterative", bundle_id = 19)
ids <- ids[is_best==1]

descript <- "COVID impacts on LRI for year 2020, source MEID 1258, source MVID 600542"
save_results_epi(modelable_entity_id=26959, input_dir=out_dir, input_file_pattern="{location_id}.csv",
                 description=descript, measure_id=c(5,6), mark_best=T, gbd_round_id=7, decomp_step="iterative",
                 bundle_id = 19, crosswalk_version_id = ids$crosswalk_version_id)
