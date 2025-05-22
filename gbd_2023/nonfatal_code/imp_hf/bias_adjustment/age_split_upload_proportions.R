
##
## Author: USERNAME
## Date: DATE
## 
## Purpose: Age-split post-crosswalk HF proportion data, save as crosswalk version
##

rm(list = ls())
date <- gsub("-", "_", Sys.Date())
pacman::p_load(data.table, ggplot2, doBy)


###### Paths, args
#################################################################################

central <- "FILEPATH"

gbd_round_id <- "VALUE"
decomp_step <- "VALUE"

ids <- data.table(name = c("CAD", "Left", "Right", "Valve", "Toxin", "PMD", "Stress"),
                  bundle_id = c("VALUE", "VALUE", "VALUE", "VALUE", "VALUE", "VALUE", "VALUE"), 
                  age_split_mvid = c("VALUE", "VALUE", "VALUE", "VALUE", "VALUE", "VALUE", "VALUE"), 
                  bundle_version = c("VALUE", "VALUE", "VALUE", "VALUE", "VALUE", "VALUE", "VALUE"))

crosswalk_path <- "FILEPATH"

###### Functions
#################################################################################

for (func in paste0(central, list.files(central))) source(paste0(func))
source("/FILEPATH/age_split.R")
source("/FILEPATH/model_helper_functions.R")
source("/FILEPATH/master_mrbrt_crosswalk_func.R")

locs <- get_location_metadata(9)
ssa_locs <- locs[super_region_name == "Sub-Saharan Africa", location_id]

###### Pull in data, age-split
#################################################################################

for (id in unique(ids$bundle_id)) {

   name <- ids[id == bundle_id, name]
   model_version_id <- ids[id == bundle_id, age_split_mvid]
   bundle_version_id <- ids[id == bundle_id, bundle_version]
   age_split_id <- ids[id == bundle_id, age_split_mvid]
  
   print(name)
  
   data <- get_bundle_version(bundle_version_id, fetch = "all")

  if (nrow(data[sex=="Both"])>0){
    lit_data <- rbind(copy(data[sex=="Both"])[, `:=` (sex="Female", note_modeler="Sex-split by duplicating")], 
                      copy(data[sex=="Both"])[, `:=` (sex="Male", note_modeler="Sex-split by duplicating")])
    data <- rbind(data[sex != "Both",], lit_data, fill = T)
  }
  
  data[grepl("Sex-split|sex split with female/male ratio", note_modeler), crosswalk_parent_seq := seq]
  data[!is.na(crosswalk_parent_seq), seq := NA]
  data$age_group_id <- NULL
  data$group_review <- NA
  data$group <- NA
  
  data_agesplit <- age_split(df = data, 
                              model_id = age_split_id,
                              decomp_step = "VALUE", 
                              gbd_round_id = "VALUE", 
                              measure = "proportion", 
                              global_age_pattern = F,
                              age_bin_size = 10, 
                              gbd_age_groups = F,
                              model_version_id = age_split_id)
  
  data <- data_agesplit
  
  data[location_id %in% c("VALUE"), `:=` (location_id="VALUE", location_name="VALUE", ihme_loc_id="VALUE", 
                                                note_modeler="VALUE")]
  data[mean > 1, mean := 1]
  data[standard_error > 1, standard_error := 1]
  data[, c("upper", "lower", "uncertainty_type_value", "underlying_nid") := NA]
  data[, unit_value_as_published := 1]
  data[, specificity := NA]
  data[, group := NA]
  data[, group_review := NA]
  data[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  data[!is.na(crosswalk_parent_seq), seq := NA]
  

  ## Drop SSA data points that aren't literature
  data <- data[!(location_id %in% ssa_locs & note_modeler %like% "Proportion generated from CSMR")]
  
  write.xlsx(data, paste0(crosswalk_path, "post_agesplit_prop_", date, "_", id, ".xlsx"), sheetName = "extraction")
  save_crosswalk_version(bundle_version_id = bundle_version_id, 
                         data_filepath =  paste0(crosswalk_path, "post_agesplit_prop_", date, "_", id, ".xlsx"), 
                         description = paste0(name, ", sex- and age-split with literature data, dropping SSA, ", date))
  
  
  
  
}
