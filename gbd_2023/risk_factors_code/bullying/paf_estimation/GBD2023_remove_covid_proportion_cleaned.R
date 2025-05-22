
# STEP 1: Submit jobs to remove covid_adjustment --------------------------------------------------------
submit <- F 
if(submit){
  slots <- 5
  mem <- paste0(1024 * slots*2, "M")
  
  shell <-"/FILEPATH/execRscript.sh -i /FILEPATH/rstudio/latest.img"
  script_covid <- "-s /FILEPATH/GBD2023_remove_covid_proportion.R"
  project <- ''
  sge_output_dir <- '-o /FILEPATH/%x.o%j -e /FILEPATH/errors/%x.o%j '
  
  source("/FILEPATH/get_location_metadata.R")
  location_list <- get_location_metadata(location_set_id=35, release_id = 16)[is_estimate==1,]
  
  files_locations_covid <- paste0("/FILEPATH/paf_yld_", location_list$location_id, "_")
  files_locations_covid <- c(paste0(files_locations_covid, 2020:2024))
  files_locations_covid <- c(paste0(files_locations_covid, "_1.csv"), paste0(files_locations_covid, "_2.csv"))

  filelist_covid <- list.files(path="/FILEPATH/pafs_post_covidadjusted", pattern = "csv", full.names = T)
  missing_covid <- files_locations_covid[!(files_locations_covid %in% filelist_covid)]

  missing_covid <- strsplit(missing_covid, "_")

  missing_locations_covid <- unique(unlist(lapply(missing_covid, function(x){x[5]})))

  for (location in as.integer(missing_locations_covid)){
    job_name<- paste0('-J paf_cov_',location)
    sys_sub<- paste0("sbatch ", job_name, " --mem=", mem, " -c ", slots, " -t 120 -A ", project, " -p all.q -C archive ", sge_output_dir)
    system(paste(sys_sub, shell, script_covid, location))
    print(paste(sys_sub, shell, script_covid, location))
  }

  print("All causes submitted")
}

# STEP 2 (launched by STEP 1): Code run in parallel from above launch code to create PAF files with the pandemic impact removed ------------------

source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_location_metadata.R")

paf_folder <- "/FILEPATH/pafs"

args<-commandArgs(trailingOnly = TRUE)
location <- args[1]

mdd_results <- get_draws(year_id = c(2019:2024), gbd_id_type = "modelable_entity_id", gbd_id = 27665, release_id = 16, source = "epi", status = "best", measure_id = 5, location_id = location)
 
baseline_mdd <- melt.data.table(mdd_results[year_id == 2019,], id.vars = names(mdd_results)[!(names(mdd_results) %like% "draw")], value.name="b_prev", variable.name="draw")

covid_mdd <- melt.data.table(mdd_results[year_id %in% c(2020:2024)], id.vars = names(mdd_results)[!(names(mdd_results) %like% "draw")], value.name="c_prev", variable.name="draw")

mdd <- merge(baseline_mdd, covid_mdd, by = c("age_group_id", "location_id", "measure_id", "sex_id", "metric_id", "draw"))
mdd[, `:=` (cause_id = 568, model_version_id.x = NULL, modelable_entity_id.x = NULL, model_version_id.y = NULL, modelable_entity_id.y = NULL, year_id = year_id.y, year_id.x = NULL, year_id.y = NULL)]

rm(baseline_mdd, covid_mdd, mdd_results)

anx_results <- get_draws(year_id = c(2019:2024), gbd_id_type = "modelable_entity_id", gbd_id = 25077, release_id = 16, source = "epi", status = "best", measure_id = 5, location_id = location)

baseline_anx <- melt.data.table(anx_results[year_id == 2019,], id.vars = names(anx_results)[!(names(anx_results) %like% "draw")], value.name="b_prev", variable.name="draw")

covid_anx <- melt.data.table(anx_results[year_id %in% c(2020:2024)], id.vars = names(anx_results)[!(names(anx_results) %like% "draw")], value.name="c_prev", variable.name="draw")

anx <- merge(baseline_anx, covid_anx, by = c("age_group_id", "location_id", "measure_id", "sex_id", "metric_id", "draw"))
anx[, `:=` (cause_id = 571, model_version_id.x = NULL, modelable_entity_id.x = NULL, model_version_id.y = NULL, modelable_entity_id.y = NULL, year_id = year_id.y, year_id.x = NULL, year_id.y = NULL)]
rm(baseline_anx, covid_anx, anx_results)

epi_results <- rbind(mdd, anx)
rm(mdd, anx)

epi_results[, `:=` (change = b_prev / c_prev)]
epi_results[b_prev == 0 & c_prev == 0, change := 1]
epi_results[, `:=` (b_prev = NULL, c_prev = NULL, paf = gsub("draw", "paf", draw))]

### Adjusted PAF files

files <- c(paste0(paf_folder, "/paf_yld_", location, "_2020_", c(1, 2), ".csv"),
           paste0(paf_folder, "/paf_yld_", location, "_2021_", c(1, 2), ".csv"),
           paste0(paf_folder, "/paf_yld_", location, "_2022_", c(1, 2), ".csv"),
           paste0(paf_folder, "/paf_yld_", location, "_2023_", c(1, 2), ".csv"),
           paste0(paf_folder, "/paf_yld_", location, "_2024_", c(1, 2), ".csv"))

for(p in files){
  paf_estimates <- fread(p)
  paf_estimates <- melt.data.table(paf_estimates, id.vars = names(paf_estimates)[!(names(paf_estimates) %like% "paf")], value.name="val", variable.name="paf")
  paf_estimates[, location_id := as.numeric(location_id)]
  paf_estimates <- merge(paf_estimates, epi_results[,.(age_group_id, location_id = as.numeric(location_id), sex_id, year_id, cause_id, change, paf)], all.x = T, by = c("age_group_id", "location_id", "sex_id", "year_id", "cause_id", "paf"))
  paf_estimates[, val := val * change]
  paf_estimates[val > 1, val := 1]
  paf_estimates[, `:=` (change = NULL)]
  paf_estimates <- dcast(paf_estimates, rei_id + age_group_id + location_id + sex_id + year_id + sex_id + cause_id + modelable_entity_id ~ paf, value.var="val")
  write.csv(paf_estimates, gsub(paf_folder, "/FILEPATH/pafs_post_covidadjusted", p), row.names = F, na = "")
}

# Step 3: Move replace old PAF files with new PAF files  -------------------------------------------------
replace_files <- F
if(replace_files){

  ##### Back up original unadjusted files
  filelist_original <- list.files(path="/FILEPATH/pafs", pattern = "csv", full.names = T)
  filelist_original <- c(filelist_original[grepl("_2020_", filelist_original)],
                        filelist_original[grepl("_2021_", filelist_original)],
                        filelist_original[grepl("_2022_", filelist_original)],
                        filelist_original[grepl("_2023_", filelist_original)],
                        filelist_original[grepl("_2024_", filelist_original)])

  file.copy(filelist_original, "/FILEPATH/pafs_pre_covidadjusted")

  file_names <- gsub("/FILEPATH/pafs/", "", filelist_original)

  # Check all files copied properly
  file_names[!(file_names %in% list.files(path="/FILEPATH/pafs_pre_covidadjusted", pattern = "csv", full.names = F))]


  ##### Remove original files
  i_am_really_sure <- F
  if(i_am_really_sure){
    file.remove(filelist_original)
  }

  ##### Check all covid_adjusted files are present
  file_names[!(file_names %in% list.files(path="/FILEPATH/pafs_post_covidadjusted", pattern = "csv", full.names = F))]

  ##### Move over covid files to paf folder
  filelist_new <- list.files(path="/FILEPATH/pafs_post_covidadjusted", pattern = "csv", full.names = T)

  file.copy(filelist_new, "/FILEPATH/pafs")

  # Check all files copied properly
  gsub("/FILEPATH/pafs/", "", filelist_original)[!(gsub("/FILEPATH/pafs/", "", filelist_original) %in% list.files(path="/FILEPATH/pafs_pre_covidadjusted", pattern = "csv", full.names = F))]

  file_names[!(file_names %in% list.files(path="/FILEPATH/pafs", pattern = "csv", full.names = F))]
}





