################################################################################
# Purpose: Get draws of proportion models for India.
# We have data for India so we don't need the Geisinger data to predict etiologies
# for each stage of ckd, instead we have dismod models for that we are getting the draws.

# This step does the following:
# 1. Get draws of proportion models by location for India.
#   (exclude diabetes models for now, we will handle them separately).
# 2. Save draws by location in their respective folder by stage and etiology.
# 3. Get draws of diabetes type 1 and 2 models by location. 
# 4. Split the data into type 1 and type 2.
#     Get draws of ckd due to diabetes proportion models by location for India 
#     generated by ICMR and split in type 1 and type 2 using dm type 1
#     and type 2 models.
# 5. Scale draws to 1
# 6. Save draws by location in their respective folder by stage and etiology 
#     (diabetes type 1 or type 2)

################################################################################

rm(list = ls())

library(data.table)
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_draws.R")

message('Loading args')
args <- commandArgs(trailingOnly = TRUE)
loc_id <- as.numeric(args[1])
cb_path <- args[2]
output_path <- args[3]
release_id <- as.numeric(args[4])
dm_1_me <- as.numeric(args[5])
dm_2_me <- as.numeric(args[6])
functions_path <- args[7]

message(loc_id)
message(cb_path)
message(output_path)
message(release_id)
message(paste0("dm1 me", dm_1_me))
message(paste0("dm2 me", dm_2_me))

source(functions_path)
age_ids <- get_demographics(gbd_team = "epi", release_id = release_id)$age_group_id
sex_ids <- c(1, 2)
cb <- fread(cb_path)

# filter by stage
stages <- unique(cb$dir_stage)
# size of columns to save 
col_save_size <- 1005

for (stage_name in stages) {
  
  message(paste('Processing draws for', stage_name))
  # cb2 <- cb[dir_stage == stage_name, ]
  non_dm_mes <- cb[dir_stage == stage_name & etiology != 'dm', ]$meid
  dm_prop_me <- cb[dir_stage == stage_name & etiology == 'dm', ]$meid

  # 1. Get draws of proportion models (exclude diabetes)------------------------
  
  message("Get ckd proportion draws for mes")
  message(paste(non_dm_mes))
  # measure 18 is proportion
  
  # initialize an empty data table
  dt <- as.data.table(NULL)
  for (me in non_dm_mes) {
    print(me)
    etio_name <- cb[meid == me]$etiology
    dt_me <- get_me_draws(me, age_ids, loc_id, sex_ids, release_id, 18)
    # insert etio_name
    dt_me[, etiology := etio_name]
    # Append the new dataframe
    dt <- rbind(dt, dt_me)
  }
  dt[, modelable_entity_id := NULL]
  rm(dt_me)
  
  # 2. Get draws of diabetes type 1 and 2 models by location--------------------
  
  message("Getting dm draws")
  # Get dm models and reshape to long format
  dm_1 <- get_dm_draws_and_reshape(dm_1_me, 'dm1')
  dm_2 <- get_dm_draws_and_reshape(dm_2_me, 'dm2')
  
  # merge dm_1 and dm_2
  dm_ratios <- merge(dm_1, dm_2, by = c("location_id", "age_group_id", "year_id", "sex_id", "draw"))
  dm_ratios[, dm1_ratio := dm1 / (dm1 + dm2)]
  dm_ratios[, dm2_ratio := dm2 / (dm1 + dm2)]
  
  # drop dm1 and dm2 column
  dm_ratios[, c("dm1", "dm2") := NULL]
  
  # 3. Split diabetes draws-----------------------------------------------------
  
  message(paste("Splitting draws of ", dm_prop_me, stage_name, "into type 1 and 2"))
  
  # Get draws of ckd due to diabetes proportion model by location
  prop_draws <- get_me_draws(dm_prop_me, age_ids, loc_id, sex_ids, release_id, 18)
  # reshape prop draws for splitting
  prop_draws_long <- get_long_draws(prop_draws, 'prop')
  # rm(prop_draws)
  
  # Generate draws by diabetes type
  # merge prop draws with dm types draws
  prop_by_dm <- merge(prop_draws_long, dm_ratios, by = c("location_id", "age_group_id", "year_id", "sex_id", "draw"))
  prop_by_dm[, prop_dm1 := prop * dm1_ratio]
  prop_by_dm[, prop_dm2 := prop * dm2_ratio]
  # drop dm_1_ratio and dm_2_ratio
  prop_by_dm[, c("dm1_ratio", "dm2_ratio", "prop", "modelable_entity_id") := NULL]
  
  rm(prop_draws_long)
  rm(dm_ratios)
  
  # Reshape draws
  cols_to_keep <- c("location_id", "age_group_id", "year_id", "sex_id", "measure_id")
  
  dm_1_prop <- copy(prop_by_dm)
  dm_1_prop[, c("prop_dm2") := NULL]
  dm_1_prop <- get_wide_draws(dm_1_prop, cols_to_keep, 'prop_dm1')
  # there is not a me for type 1 and type 2 dm, so we adding suffix 1 and 2
  # to distinguish them
  # insert a column named dm_1
  dm_1_prop[, etiology := "dm1"]
  
  dm_2_prop <- copy(prop_by_dm)
  dm_2_prop[, c("prop_dm1") := NULL]
  dm_2_prop <- get_wide_draws(dm_2_prop, cols_to_keep, 'prop_dm2')
  # insert a column named dm_1
  dm_2_prop[, etiology := "dm2"]
  rm(prop_by_dm)
  
  # 4. Merge prop draws with the diabetes draws --------------------------------

  dt_prop <- rbind(dm_1_prop, dm_2_prop)
  dt_prop <- rbind(dt_prop, dt)
  
  rm(dm_1_prop)
  rm(dm_2_prop)
  
  # 5. Scale draws to sum to 1 -------------------------------------------------
  
  message('Scaling draws to 1')
  dt_prop_long <- get_long_draws(dt_prop, 'value')
  
  # group data by location_id, age_group, and take the sum
  dt_prop_sum <- dt_prop_long[, .(sum_of_value = sum(value)), by = c("location_id", "age_group_id", "year_id", "sex_id", "draw")]
  
  dt_prop_scaled_long <- merge(dt_prop_long, dt_prop_sum, by = c("location_id", "age_group_id", "year_id", "sex_id", "draw"))
  dt_prop_scaled_long[, scaled_value := value / sum_of_value]
  # drop columns
  dt_prop_scaled_long[, c("value", "sum_of_value") := NULL]
  dt_prop_scaled <- get_wide_draws(dt_prop_scaled_long, c(cols_to_keep, "etiology"), 'scaled_value')
  
  # take sum of first 5 rows of column draw_0
  print(dt_prop_scaled[1:5, sum(draw_0)])
  
  rm(dt_prop_long)
  rm(dt_prop_sum)
  rm(dt_prop_scaled_long)
  
  # 6. Saving draws-------------------------------------------------------------
  
  message(paste("Writing files to", output_path))
  etiologies <- unique(dt_prop_scaled$etiology)
  
  for (etio in etiologies) {
    message(paste("Saving draws for", etio))
    
    dt_by_me <- copy(dt_prop_scaled)
    dt_by_me <- dt_by_me[etiology == etio, ]
    dt_by_me[, etiology := NULL]
    
    check_columns(dt_by_me, col_save_size)
    save_results(dt_by_me, output_path, stage_name, etio, loc_id)
  }
  rm(dt_by_me)
}

message('Done')
