
## 
## Purpose: Age split NRVDs
##

###### Paths, args
#################################################################################

central <- "FILEPATH"

### Age-split

source("age_split.R")

code_root <- paste0("FILEPATH")

# Central comp shared functions
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_population.R"))
source(paste0(central, "get_ids.R"))
source(paste0(central, "upload_bundle_data.R"))
source(paste0(central, "get_bundle_version.R"))
source(paste0(central, "save_bundle_version.R"))
source(paste0(central, "save_crosswalk_version.R"))
source(paste0(central, "validate_input_sheet.R"))
source(paste0(central, "get_draws.R"))
# CVD helper functions
source(paste0(code_root, "get_recent.R"))
source(paste0(code_root, "data_tests.R"))
source(paste0(code_root, "job_hold.R"))
source('bundle_process.R')
locs<-get_location_metadata(location_set_id=9)[, .(location_id, level, region_name, super_region_name, parent_id, region_id)]

for (me in c("aort", "mitral")) {

  data <- as.data.table(readxl::read_excel(paste0("FILEPATH")))
  
  num_cols <- c("mean", "lower", "upper", "standard_error", "cases", "sample_size", "age_start", "age_end",
                "year_start", "year_end", "group_review", "group", "is_outlier", "recall_type_value", "uncertainty_type_value")
  lapply(X = num_cols, FUN = check_class, df=data, class="numeric", coerce=T) 
  
  data[, crosswalk_parent_seq := seq]
  data[, seq := NA]
  
  ## Age split prevalence data
  prev <- copy(data)[measure=="prevalence"]
  
  prev <- age_split(df = prev, 
                    model_id = "VALUE", 
                    decomp_step = "step2", 
                    gbd_round_id = 6, 
                    measure = "prevalence")
  
  
  ## Age split mtwith data
  mtwith <- copy(data)[measure == "mtwith"]
  mtwith <- mtwith[age_end - age_start <= 25,]
  
  ## Age split cfr data
  cfr <- copy(data)[measure == "cfr"]
  cfr <- cfr[age_end - age_start <= 25,]

  ## Age split mtstandard data 
  mtstandard <- copy(data)[measure == "mtstandard"]
  mtstandard <- mtstandard[age_end - age_start <= 25,]
  
  
  ###### Re-append all data types and prep for upload
  ############################################################################################################
  
  data <- do.call("rbind", list(prev, inc, mtwith, cfr, mtstandard, fill=T))
  
  data[location_id %in% c(4622,4624,95), `:=` (location_id=4749, location_name="England", ihme_loc_id="GBR_4749", step2_location_year="UTLA data points reextracted as 'England' because of the location heirarchy.")]
  
  data[, c("underlying_nid", "group_review", "specificity", "group", "upper", "lower", "uncertainty_type_value", "sampling_type") := NA]
  
  locations <- get_location_metadata(9)
  
  data[, grep("cv_*", names(data)) := NULL]

  data[standard_error>1, standard_error := 1]
  
  write.xlsx(data, "FILEPATH", sheetName="extraction")
  
  description <- "Age-split, crosswalked, no MAD filtering"
  save_crosswalk_version_result <- save_crosswalk_version(
    bundle_version_id="VALUE",
    data_filepath='FILEPATH',
    description=description)
  
  print(sprintf('Request status: %s', save_crosswalk_version_result$request_status))
  print(sprintf('Request ID: %s', save_crosswalk_version_result$request_id))
  print(sprintf('Bundle version ID: %s', save_crosswalk_version_result$bundle_version_id))
  print(sprintf('Crosswalk version ID: %s', save_crosswalk_version_result$crosswalk_version_id))
  
  data<-data[age_start<40, is_outlier:=1]
  data[mean==0, is_outlier:=1]
  
  data[location_id %in% c(4622,4624,95), `:=` (location_id=4749, location_name="England", ihme_loc_id="GBR_4749", step2_location_year="UTLA data points reextracted as 'England' because of the location heirarchy.")]
  
  data$underlying_nid <- NA
  data$crosswalk_parent_seq <- data$seq
  data$seq <- NA
  #
  data$group_review <- NA
  data$specificity <- NA
  data$group <- NA
  #
  data$upper <- NA
  data$lower <- NA
  data$uncertainty_type_value <- NA
  #
  data$sampling_type <- NA
  
  parent_locs <- locations[parent_id %in% c(130, 6, 135, 16, 11, 163, 90), c("location_name", "location_id", "parent_id")] 
  data <- data[!(cv_inpatient==1&location_id %in% parent_locs$location_id)]
  data <- data[!(cv_inpatient==1& location_name %in% c("Jordan", "Iran", 
                                                     "Islamic Republic of Iran", 
                                                     "Philippines",
                                                     "Meghalaya", "Karnataka", "Kenya", "Nepal"))]
  
  
  data[, grep("cv_*", names(data)) := NULL]
  data <- data[!(cases==0&sample_size==0)]
  
  write.xlsx(data, "FILEPATH", sheetName="extraction")

  most_recent_bundle_version_id_aort <- "VALUE"
  most_recent_bundle_version_id_mitral <- "VALUE"
  
  most_recent_bundle_version_id <- ifelse(me == "aort", most_recent_bundle_version_id_aort, most_recent_bundle_version_id_mitral)
  
  description <- paste0(me, " age-split and crosswalked, MAD filtered")
  save_crosswalk_version_result <- save_crosswalk_version(
    bundle_version_id=most_recent_bundle_version_id,
    data_filepath="FILEPATH",
    description=description)
  #
  print(sprintf('Request status: %s', save_crosswalk_version_result$request_status))
  print(sprintf('Request ID: %s', save_crosswalk_version_result$request_id))
  print(sprintf('Bundle version ID: %s', save_crosswalk_version_result$bundle_version_id))
  #
                        
}

