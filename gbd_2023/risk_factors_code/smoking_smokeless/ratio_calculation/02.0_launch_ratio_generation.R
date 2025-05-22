# Introduction ------------------------------------------------------------
# Purpose: Generate chewing to non-chewing SLT ratio
# Date Modified: DATE

# Setup -------------------------------------------------------------------
if (TRUE){
rm(list=ls())

# Define parameters
draws <- 100    
make_plots <- T
using_bundles <- T
  
final.ids <- c("slt"      = "ID", 
               "chew"     = "ID", 
               "non_chew" = "ID")
bundle_version_id <- "ID"

gbd_id <- 9
release <- 16
description <- "GBD 2023 bundle data + new data with GBD 2021 tools"
  
# Load packages, and install if missing
library(data.table)
date <- Sys.Date()

# File paths
code_path <- "FILEPATH"
script_ratio <- "FILEPATH"
input_path <- "FILEPATH"
central_root <- "FILEPATH"

# File path where the ratio draws are saved: 
chew_ratio_dir <- "FILEPATH"
input_dir <- "FILEPATH"

# File path where the adjusted dataset is saved after ratios are applied:
adjusted_path <- "FILEPATH"

# For running jobs on the cluster
shell  <-  "FILEPATH"
logs_e <- "FILEPATH"
logs_o <- "FILEPATH"
stgpr_output <- "FILEPATH"

# Load files and source code =========================================================================
source("FILEPATH", "useful_functions.R")
source("FILEPATH", "central_funs.R")
source("FILEPATH", "save_and_upload_bundles.R")
source_functions("FILEPATH")

# Bundle info (unchanging)
bundle_ids <- c("slt" = "ID", "chew" = "ID", "non_chew" = "ID", "final" = "ID")
xwalk_ids <- c("slt" = "ID", "chew" = "ID", "non_chew" = "ID")
final_mei <- 28617

# Cluster resources for launching the ratio jobs
proj_ratio <- "PROJECT"
time_alloc_ratio <- "00:10:00"
mem_alloc_ratio <- 7
fthreads_ratio <- 2
array <- T
queue <- "long.q"

# Cluster resources for launching the ST-GPR run
proj_final <- "PROJECT"
nparallel_final <- 50 # Number of parallelizations! More parallelizations --> faster (if cluster is emtpy). I usually do 50-100.
holdouts <- 0 # Set to 0 unless you want some sort of cross validation. 

message("=== SETUP DONE ===")
}


# Script and Launch ------------------------------------------------------------------
if (TRUE){
for(i in names(final.ids)){
  assign(paste0("run_", i), final.ids[[i]])
  assign(paste0("bundle_", i), bundle_ids[[i]])
  assign(paste0("xwalk_", i), xwalk_ids[[i]])
}

locs <- get_location_metadata(22, release_id = release)[level >= 3, location_id]

# Prep ages datatable
ages <- get_age_spans()[age_group_id %in% 7:21]
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages[,age_end := age_end - 1]
ages[age_start == 80, age_end := 84] #' 80-84 represents 80 plus
ages[, age_group := paste(age_start, "to", age_end)]
ages[age_end == 84, age_group := "80 plus"]

# Folders
if(dir.exists("FILEPATH") == TRUE){
  message("The ratios output folder exists, so the ratio jobs are launching!")
} else {
  dir.create("FILEPATH")
  message(paste("New folder is being created to generate ratios in the following filepath:", "FILEPATH"))
}

# Create param map
if (array == TRUE) {
  n_jobs <- length(locs)
  print(n_jobs)
  
  # Save the parameters as a csv so then you can index the rows to find the appropriate parameters
  param_map <- expand.grid(location_id = locs)
  param_location <- paste0("FILEPATH", "_clean_parameters.csv")
  fwrite(param_map, "FILEPATH")
  Sys.sleep(5)
  name <- paste0("ratio_", array)

  variables <- paste(run_chew, run_non_chew, date, draws, input_dir, array, param_location)
  
  # Launches job
  command   <- paste0("sbatch --mem ", mem_alloc_ratio,
                    "G -C archive",  
                    " -c ", fthreads_ratio, 
                    " -t ", time_alloc_ratio,
                    " -p ", queue, 
                    " -a ", paste0("1-", n_jobs, "%50"),
                    " -e ", paste0(logs_e, "/%x.e%j"),
                    " -o ", paste0(logs_o, "/%x.o%j"),
                    " -A ", proj_ratio, 
                    " -J ", name,
                    " ", shell, 
                    " -s ", script_ratio, 
                    " ", variables)
    
  system(command)
  
}
message("=== JOB LAUNCHED ===")
}


# Prepare Ratio Data ------------------------------------------------------------
if (TRUE){
files <- list.files("FILEPATH", full.names = F)
files <- lapply(paste0("FILEPATH", files), fread)
all_ratios <- rbindlist(files)
all_ratios$V1 <- NULL
setnames(all_ratios, old="se_ratio",new="sd_ratio")

# Load the final estimates and input data:
slt_final <- get_estimates(run_slt, "final")
slt_data <- get_input_data(run_slt, "original")
slt_final$model <- "slt"
slt_data$model <- "slt"

chew_final <- get_estimates(run_chew, "final")
chew_data <- get_input_data(run_chew, "original")
chew_final$model <- "chew"
chew_data$model <- "chew"

non_chew_final <- get_estimates(run_non_chew, "final")
non_chew_data <- get_input_data(run_non_chew, "original")
non_chew_final$model <- "non_chew"
non_chew_data$model <- "non_chew"

non_chew_final <- non_chew_final[location_id %in% locs]
non_chew_data <- non_chew_data[location_id %in% locs]
chew_final <- chew_final[location_id %in% locs]
chew_data <- chew_data[location_id %in% locs]
slt_final <- slt_final[location_id %in% locs]
slt_data <- slt_data[location_id %in% locs]

setnames(chew_data, "val", "data")
setnames(non_chew_data, "val", "data")
setnames(slt_data, "val", "data")

# Within each location, squeeze the data under the slt envelope, then plot the results:
all_slt_final <- rbindlist(list(slt_final,chew_final,non_chew_final), use.names = T, fill = T)
all_slt_data <- rbindlist(list(slt_data,chew_data,non_chew_data), use.names = T, fill = T)

# Then multiply this chew ratio by slt to generate new chewing points:
slt_only <- all_slt_data[model == "slt"]
slt_only <- merge(slt_only,all_ratios,by=c("location_id","year_id","sex_id","age_group_id"))
slt_only[,data := data * mean_ratio]
slt_only[,variance := variance + (sd_ratio*sd_ratio)]
slt_only[,model := "slt_adjusted"]
slt_only[,c("mean_ratio","sd_ratio") := NULL]

all_slt <- rbindlist(list(slt_only,chew_data), use.names = T, fill = T)

# Take out the smokeless tobacco point if a chewing tobacco point is already present
all_slt[,count := .N, by=c("location_id","nid","year_id","age_group_id","sex_id")]
all_slt[count == 2 & model == "slt_adjusted", data := NA]
all_slt <- all_slt[!is.na(data)]
all_slt$count <- NULL

all_slt <- unique(all_slt)

saved <- copy(all_slt)

all_slt$seq <- NULL


if(using_bundles == T){
  # Load the original large dataset bundle
  full_bundle <- get_bundle_data(bundle_id = bundle_ids[["final"]])
  
  full_bundle <- make_bundle_data_changes(full_bundle)
  full_bundle <- full_bundle[var %like% "current_any"]
  to_keep <- c("nid", "location_name", "location_id", "seq", "year_id", "version", "var", "age_start", "age_end", "sex")
  full_bundle_empty <- copy(full_bundle[, ..to_keep])
  age_starts <- seq(5, 84, 5)
  age_ends <- seq(9, 84, 5)
  full_bundle_empty[, count_seq := .N, by = c("nid", "location_id", "year_id", "age_start", "age_end", "sex")]
  full_bundle_empty[age_end == age_start + 4 & age_start %in% age_starts, age_group_id := as.integer((age_start+25)/5)]
  full_bundle_empty[is.na(age_group_id) & age_end > 84, age_end := 84]
  full_bundle_empty[is.na(age_group_id) & age_start > 80, age_start := 80]
  
  full_bundle_empty[, rows := 1:.N]
  age_split_bundle <- copy(full_bundle_empty) %>% .[is.na(age_group_id)]
  count_column <- c()
  for(i in age_split_bundle$rows){
    print(i)
    a <- full_bundle_empty$age_start[i]
    b <- full_bundle_empty$age_end[i]
    a_to_b <- seq(a, b)
    first_starts <- a_to_b[which(a_to_b %in% age_starts)][1]
    first_ends <- a_to_b[which(a_to_b %in% age_ends)]
    first_ends <- first_ends[length(first_ends)]
    
    if(length(first_starts) == 0 | length(first_ends) == 0){
      count <- 1
    } else if (is.na(first_starts)){
      count <- 1
    } else if(first_ends > first_starts){
      count <- (first_ends+1 - first_starts) %/% 5
      outside_lower <- first_starts - a
      outside_higher <- b - first_ends
      if(outside_lower > 0){
        count <- count + 1 
      }
      if(outside_higher > 0){
        count <- count + 1
      }
    }else if(first_starts - 1 == first_ends){
      count <- 2
    }
    count_column <- c(count_column, count)
  }
  
  age_split_bundle[, age.split := count_column]
  young_ages <- data.table("age_group_id" = 6, "age_start" = 5, "age_end" = 9, "age_group" = "5 to 9")
  youngest_ages <- data.table("age_group_id" = 5, "age_start" = 0, "age_end" = 4, "age_group" = "0 to 4")
  ages <- rbindlist(list(ages, young_ages, youngest_ages), use.names = T)
  for(m in 1:nrow(ages)){
    assign(paste0(ages$age_start[m], "_to_", ages$age_end[m]), seq(ages$age_start[m], ages$age_end[m], 1))
  }
  age_group_min <- c()
  age_group_max <- c()
  age_group_range <- c()
  for(i in 1:nrow(age_split_bundle)){
    age_min <- age_split_bundle$age_start[i]
    for(m in 1:nrow(ages)){
      if(age_min %in% get(paste0(ages$age_start[m], "_to_", ages$age_end[m]))){
        age_group_small <- ages$age_group_id[m]
        age_group_min <- c(age_group_min, age_group_small)
      } else if(!(age_min %in% get(paste0(ages$age_start[m], "_to_", ages$age_end[m])))){
        next
      }
    }
    age_max <- age_split_bundle$age_end[i]
    for(m in 1:nrow(ages)){
      if(age_max %in% get(paste0(ages$age_start[m], "_to_", ages$age_end[m]))){
        age_group_large <- ages$age_group_id[m]
        age_group_max <- c(age_group_max, age_group_large)
      } else if(!(age_max %in% get(paste0(ages$age_start[m], "_to_", ages$age_end[m])))){
        next
      }
    }
    age_groups <- seq(age_group_small, age_group_large)
    age_group_range <- c(age_group_range, age_groups)
  }
  age_split_bundle[, `:=` (age_group_max = age_group_max, age_group_min = age_group_min)]
  age_split_bundle <- age_split_bundle[rep(seq_len(nrow(age_split_bundle)), age_split_bundle$age.split), ]
 
  age_split_bundle[, age_group_id := age_group_range]
  
  full_bundle_empty <- full_bundle_empty[!is.na(age_group_id)]
  full_bundle_empty <- rbindlist(list(full_bundle_empty, age_split_bundle), use.names = T, fill = T)
  
  full_bundle_empty[sex == "Both", sex.split := 1]
  
  male_split_bundle <- copy(full_bundle_empty) %>% 
    .[sex == "Both"]
  female_split_bundle <- copy(full_bundle_empty) %>% 
    .[sex == "Both"]
  male_split_bundle$sex <- NULL
  male_split_bundle$sex <- "Male"
  female_split_bundle$sex <- NULL
  female_split_bundle$sex <- "Female"
  
  sex_split_bundle <- rbindlist(list(female_split_bundle, male_split_bundle), use.names = T)
  
  full_bundle_empty <- full_bundle_empty[sex.split != 1 | is.na(sex.split)]
  full_bundle_empty <- rbindlist(list(full_bundle_empty, sex_split_bundle), use.names = T, fill = T)
  full_bundle_empty[, sex_id := ifelse(sex == "Male", 1, 2)]
  try <- merge(full_bundle_empty, all_slt, by = c("location_id", "year_id", "nid", "sex_id", "age_group_id"), all.y = T)
  
  out <- try[!is.na(sex.split) | !is.na(age.split) | model == "slt_adjusted", crosswalk_parent_seq := seq]
  out <- out[!is.na(sex.split) | !is.na(age.split) | model == "slt_adjusted", seq := NA]
  out$age_start <- NULL
  out$age_end <- NULL
  out <- merge(out, ages[, .(age_group_id, age_start, age_end)], by = c("age_group_id"), all.x = T)
  out[age_group_id == 21, age_end := 124]
  setnames(out,"data","val")
  
} else {
  out <- copy(all_slt)
  setnames(out, "data", "val")
  
}

message("=== RATIO DATA PREPARED ===")
}



# Launch ST-GPR -----------------------------------------------------------
if (TRUE){
adjusted_data <- out %>% dplyr::select(-c(any_of(model, age.split, sex.split, age_group_max, rows, age_group_min, age_start))) %>% 
                 mutate(sample_size=NA,
                        measure="proportion",
                        me_name="smokeless_tobacco",
                        measure_id=18)
final <- as.data.table(adjusted_data)

me_name <- "smokeless"
source("FILEPATH")

if(using_bundles == T){
  message("Saving xwalk version")
  xwalk_id <- bundle_save("xwalk", bun_id = bundle_ids[["final"]], gbd_id = gbd_id, release_id = release, raw_data = final, fp_to_bundle = adjusted_path, date = date, me_name = "smokeless_tobacco", bundle_version = bundle_version_id, description = description)
  print(xwalk_id)
} else {
  message("Writing in STGPR data to path")
  fwrite(final, paste0(adjusted_path, "ratio_data_", date, ".csv"), row.names = F)
}

# ST-GPR params
params_chew <- fread("FILEPATH")
params_chew[model_index_id == 5, `:=` (gpr_draws = draws,
                                       holdouts = holdouts,
                                       description = description,
                                       path_to_config = NA,
                                       me_name = "smokeless_tobacco",
                                       modelable_entity_id = final_mei,
                                       release_id = release,
                                       year_end = 2024)]

if(using_bundles == T){
  params_chew[model_index_id == 5, `:=` (crosswalk_version_id = xwalk_id,
                                         bundle_id = bundle_ids[["final"]],
                                         path_to_data = NA)]
  } else {
  params_chew[model_index_id == 5, `:=` (crosswalk_version_id = NA,
                                         bundle_id = NA,
                                         path_to_data = with(params_chew, ifelse(model_index_id==final_config_id, paste0(adjusted_path, "ratio_data_", date, ".csv"), path_to_data)))]
}

new_path <- paste0("FILEPATH", "_final_parameters.csv")
fwrite(params_chew, new_path)

# Submit ST-GPR
final.id <- register_stgpr_model(path_to_config = new_path,
                                 model_index_id = final_config_id)

stgpr_sendoff(run_id=final.id, project=proj_final, nparallel=nparallel_final, log_path=logs_e)

if (make_plots){
  checkmod(final.id)
  tobacco_plot(me_name="chew_final",
               runid=final.id)
}
}



