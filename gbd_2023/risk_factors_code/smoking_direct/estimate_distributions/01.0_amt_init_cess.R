# Introduction ------------------------------------------------------------
# Purpose: (1) Add newly extracted data to init/cess/amt bundles
#          (2) For each component, run ST-GPR on all data
# 
# Date Modified: 2024-10-16


# Setup -------------------------------------------------------------------
if (TRUE){
  rm(list=ls())
  user <- Sys.getenv("USER")

# GBD cycle-related variables
release <- 16

# GBD 2023 bundle versions
age_init_version <- 47463
age_cess_version <- 47558
amt_smoked_version <- 47559

# Define paths
share_path <- FILEPATH
code_path <- FILEPATH
export_path <- FILEPATH
file_path <- FILEPATH

# Loading packages and files
date <- Sys.Date()
pkgs <- c("data.table", "tidyverse", "parallel")
invisible(lapply(pkgs, library, character.only=TRUE))
source(paste0(FILEPATH, "useful_functions.R"))
source(paste0(FILEPATH, "central_funs.R"))
source(paste0(FILEPATH, "save_and_upload_bundles.R"))
source_functions(FILEPATH)

# Read in relevant demographic data
ages <- get_age_spans()[age_group_id %in% c(7:20, 30:32, 235)]
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages <- ages %>% mutate(age_end=age_end-1, 
                        age_group=paste(age_start, "to", age_end))
ages[age_end==124, age_group := "95 plus"]
suppressWarnings(locs <- get_location_metadata(location_set_id=22, release_id=release)[level > 2])

message("=== SETUP DONE ===")
}


# Data Import -------------------------------------------------------------------
if (TRUE){
files <- list.files(file_path, full.names=T, pattern = ".csv", recursive = T)
df <- lapply(files, fread) %>% rbindlist(use.names = T, fill = T) %>% distinct(nid, ihme_loc_id, sex_id, year_start, age_start, var, round(mean, 1), .keep_all = T)
df$version <- "GBD2023"

# Clean the new data (removing duplicates from extractions, bad sources, etc.)
tobdf <- "new_data"
source(paste0(FILEPATH, "01.1_smoking_mean_data_cleaning.R"))

# Check for duplicates; if present, fix in script above 
checkdups_dist("df")

micro_2023 <- df %>% mutate(age_group_id=case_when(age_start<79               ~ (age_start+25)/5, 
                                                   between(age_start, 80, 94) ~ ((age_start+25)/5)+9, 
                                                   age_start == 95            ~ 235),
                            year_id = floor((year_start+year_end)/2),
                            variance = standard_error^2,
                            variance = case_when(variance<0.0001 ~ standard_deviation/(sqrt(sample_size))^2),
                            seq = "",
                            underlying_nid = "",
                            measure = "continuous",
                            is_outlier = 0,
                            sex = ifelse(sex_id == 1, "Male", "Female")) %>% 
                     filter(age_group_id >= 7) %>% 
                     dplyr::select(everything(), val=mean)
                            
                                    
micro_2023 <- merge(micro_2023, locs[, .(location_id, ihme_loc_id)], by = "ihme_loc_id") %>% 
              dplyr::mutate(nidloc=paste(nid, location_id, sep="-"))

idvars <- c("nid", "year_id", "age_group_id", "sample_size", "val", "me_name", "location_id", "variance", "seq", "is_outlier", "sex", "measure")

# Pull bundle, append new data, check for quality   
for (xvar in c("init", "cess", "amt")){
    varname <- case_when(xvar=="init" ~ "init_age_smoking",
                         xvar=="cess" ~ "cess_age_smoking",
                         xvar=="amt"  ~ "total_amt_smoked")
  
  # Subset new data to init 
    micky <- paste0("micro_2023_", xvar)
    assign(micky, micro_2023 %>% filter(var==varname, age_group_id>8))
    
  # Clean the new data (removing duplicates from extractions, bad sources, etc.)
    tobdf <- xvar
    source(paste0(FILEPATH, "01.1_smoking_mean_data_cleaning.R"))
    
  # Get old bundle, remove NID-locs present in new data
    bunny <- paste0("bundle_data_", xvar)
    
    if(xvar=="amt"){
    assign(bunny, get_bundle_version(get(paste0("amt_smoked_version")), fetch = "all") %>% 
                  dplyr::mutate(crosswalk_parent_seq=NA,
                                nidloc=paste(nid, location_id, sep="-")) %>% 
                  filter(!age_group_id==21,
                         !nidloc %in% get(micky)[["nidloc"]]))
    } else{assign(bunny, get_bundle_version(get(paste0("age_", xvar, "_version")), fetch = "all") %>% 
                  dplyr::mutate(crosswalk_parent_seq=NA,
                                nidloc=paste(nid, location_id, sep="-")) %>% 
                  filter(!age_group_id==21,
                         !nidloc %in% get(micky)[["nidloc"]]))
    }
    
  # Clean the bundle data (removing duplicates from extractions, bad sources, etc.)
    tobdf <- paste0(xvar, "_bun")
    source(paste0(FILEPATH, "01.1_smoking_mean_data_cleaning.R"))
    
  # Check for duplicates; if present, fix in script above 
    checkdups_dist(bunny)

  # Bind together old and new data, check for duplicates
    assign(xvar, rbind(get(micky), get(bunny), fill=T) %>% 
           dplyr::mutate(me_name = varname,
                         crosswalk_parent_seq = NA) %>% 
           drop_na(variance) %>% 
           dplyr::select(all_of(idvars)))
                                                                               
    checkdups_dist(xvar)
}


message("=== DATA IMPORT DONE ===")
}


# Manage Data/Bundles -------------------------------------------------------------------
if (TRUE){
# Init
write.csv(init, paste0(export_path, "initiation_", date, ".csv"), row.names = F)
bundle_id <- 4775
xwalk_final_init_age <- outlier_upload_dist(bundle_id = bundle_id, model="init", path = paste0(export_path, "initiation_", date, ".csv"), date = date)

# Cess
write.csv(cess, paste0(export_path, "cessation_", date, ".csv"), row.names = F)
bundle_id <- 4772
xwalk_final_cess_age <- outlier_upload_dist(bundle_id = bundle_id, model="cess", path = paste0(export_path, "cessation_", date, ".csv"), date = date)

# Amt
    # Temp fix re: empty cell for female 95+
    # Copy over males 95+ for females, adjust by overall ratio of amt smoked
amt2 <- rbind(amt, amt %>% filter(age_group_id==235) %>% 
                           rowwise() %>% 
                           mutate(sex_id=2, 
                                  sex="Female", 
                                  val=val*0.878655))
write.csv(amt2, paste0(FILEPATH, "amt_smoked_", date, ".csv"), row.names = F)
bundle_id <- 4778
xwalk_final_amt_smoked <- outlier_upload_dist(bundle_id = bundle_id, model="amt", path = paste0(FILEPATH, "amt_smoked_", date, ".csv"), date = date)


message("=== DATA MANAGEMENT DONE ===")
}


# ST-GPR -------------------------------------------------------------------
if (TRUE){
# Import ST-GPR configs
config <- fread(paste0(FILEPATH, "smoking_dist_config.csv"))

draws <- 100
holdouts <- 0

# Adjust config if necessary
  # Set xwalk versions for specific components
config[model_index_id==1, `:=`(crosswalk_version_id = xwalk_final_init_age$xwalk_version_id, 
                               description = paste0("init_", xwalk_final_init_age$xwalk_version_id, "_", date))]
config[model_index_id==2, `:=`(crosswalk_version_id = xwalk_final_cess_age$xwalk_version_id, 
                               description = paste0("cess_", xwalk_final_cess_age$xwalk_version_id, "_", date))]
config[model_index_id==3, `:=`(crosswalk_version_id = xwalk_final_amt_smoked$xwalk_version_id, 
                               description = paste0("amt_", xwalk_final_amt_smoked$xwalk_version_id, "_", date))]
 
 # Global params
config[, gpr_draws := draws]
config[, holdouts := holdouts]
config[, year_end := 2024]

# Write config for ST-GPR runs
config_loc <- paste0(FILEPATH, "smoking_dist_config_", date, ".csv")
write.csv(config, FILEPATH, row.names = F)

# Model arguments
cluster_project <- "proj_team"
nparallel <- 50
logs <- paste0(FILEPATH, Sys.getenv("USER"), "/errors")

# Init
id_init <- register_stgpr_model(path_to_config = config_loc, model_index_id = 1)
stgpr_sendoff(run_id=id_init, project=cluster_project, nparallel=nparallel, log_path=logs)

# Cess
id_cess <- register_stgpr_model(path_to_config = config_loc, model_index_id = 2)
stgpr_sendoff(run_id=id_cess, project=cluster_project, nparallel=nparallel, log_path=logs)

# Amt
id_amt <- register_stgpr_model(path_to_config = config_loc, model_index_id = 3)
stgpr_sendoff(run_id=id_amt, project=cluster_project, nparallel=nparallel, log_path=logs)


message("All final ST-GPR models have been submitted.")
message("Init model run_id is ", id_init)
message("Cess model run_id is ", id_cess)
message("Amt model run_id is ", id_amt)

checkmod(id_init)
checkmod(id_cess)
checkmod(id_amt)

message("=== FINAL SMOKING DISTRIBUTION ST-GPRS COMPLETE ===")
}


