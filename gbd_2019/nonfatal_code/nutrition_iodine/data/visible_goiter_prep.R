##########################################################################
### Purpose: 1) Pull raw visible goiter data and run mr-brt to get a global sex split ratio (using functions from a different script), split the data, and then upload as a crosswalk version
###          2) After there is a best model in xx (using the crosswalk version), pull the crosswalk version and age split it using the age pattern from the model in xx. Then upload that as a second crosswalk version (age AND sex split)
##########################################################################
#rm(list = ls())

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"

code_dir <- if (os == "Linux") paste0("/ihme/code/dbd/", user, "/") else if (os == "Windows") ""
source(paste0(code_dir, 'shared/utils/primer.R'))

source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/validate_input_sheet.R")
source("FILEPATH/get_population.R")
source("FILEPATH/sex_split_functions.R") 


#set relevant versions and paths
version <- 2
bundle_id <- 93
bundle_version <- 7793  
iterative_bundle_version <- 8192
crosswalk_version <- 3845    #output from first step of this code (best step2 crosswalk version pre age splitting)
output_dir <- paste0("FILEPATH")

upload_crosswalk_version <- FALSE
crosswalk_version_descript <- "Final ratio for GBD 2019"
upload_iterative_too <- FALSE   #uploads same crosswalk version to iterative and a narrow age range version

############################################################################################################################################

############################################################
# Step 1: do the sex split using sex split mrbrt functions
############################################################


if(run_sex_split){
  all_data <- get_bundle_version(bundle_version)
  
  all_data[nid==43563 & location_id==156, underlying_nid:= 137584]
  all_data[is.na(underlying_nid), underlying_nid:=nid]
  
  all_data <- all_data[is_outlier==0,]
  ratios <- get_sex_split_ratio(all_data, output_dir, version = version)    # run mr-brt!
  
  #make sure we are uploading everything with the nids it expects
  all_data <- get_bundle_version(bundle_version)
  all_data <- all_data[, !c(44:46)]
  all_data[ underlying_nid==137430, underlying_nid:=NA]     

  split_data <- to_split_data(all_data, ratio_mean=ratios$ratio_mean, ratio_se = ratios$ratio_se)
  gplot <- ggplot(split_data[was_sexsplit==1], aes(x=old_mean, y=mean, color=sex))+
    geom_point()+
    theme_bw()+
    labs(title=paste0(model_name, " sex split adjustment; mrbrt ratio(se): ", round(ratios$ratio_mean, 3), "(", round(ratios$ratio_se, 3), ")"))
  print(gplot)
  pdf(paste0(output_dir,"/adjusted_data_scatter.PDF"))
  print(gplot)
  dev.off()
  #just have to check distribution before uploading
  summary(split_data$mean)
  
  
  crosswalk_version_descript <- paste0(crosswalk_version_descript, "; mrbrt F/M ratio(se): ", round(ratios$ratio_mean, 3), "(", round(ratios$ratio_se, 3), ")")
  
  
  save_dir <- paste0(output_dir, "/sex_split_data")
  if(!dir.exists(save_dir)){dir.create(save_dir)}
  openxlsx::write.xlsx(split_data, paste0(save_dir, "/adjusted_data_for_upload_v", version,".xlsx"), sheetName = "extraction")
  
  output_file <- paste0(save_dir, "/adjusted_data_for_upload_v", version,".xlsx")
  
  validate_input_sheet(bundle_id, output_file, error_log_path = "/share/scratch/users/hkl1/")
  
  if(upload_crosswalk_version){
    save_crosswalk_version(bundle_version, output_file, description = crosswalk_version_descript)
    if(upload_iterative_too){
      save_crosswalk_version(iterative_bundle_version, output_file, description = descript)
      #also need to upload a version of the data with <15 year age window
      
      split_data[, age_range:= age_end-age_start]
      narrow_range <- split_data[age_range < 16]
      narrow_range$age_range <- NULL
      output_file <- paste0(save_dir, "/narrow_age_range_adjusted_data_for_upload_v", version,".xlsx")
      openxlsx::write.xlsx(narrow_range, output_file, sheetName = "extraction")
      crosswalk_version_descript <- paste0(crosswalk_version_descript, "; narrow age range data"  )
      save_crosswalk_version(iterative_bundle_version, output_file, description = crosswalk_version_descript)
      
      }
  }
  
}

############################################################
# Step 2: do the age split using age trend from dismod model 
############################################################

if(run_age_split){
  
  location_pattern_id <- 1  #the id to use for the age pattern
  region_pattern <- FALSE
  
  draws <- paste0("draw_", 0:999)
  original <- get_crosswalk_version(crosswalk_version)
  original[!is.na(crosswalk_parent_seq), seq:=NA]
  original[, id := 1:.N]
  original[, was_age_split:=0]
  data_to_split <- original[(age_end-age_start > 15) & is_outlier==0]
  data_to_split[,year_id:=round((year_start + year_end)/2, 0)]
  data_to_split[sex=="Male", sex_id:=1]
  data_to_split[sex=="Female", sex_id:=2]
  data_to_split[age_end==80, age_end:=100] 
  data_to_split[, cases:=mean*effective_sample_size]
  
  ages <- get_age_metadata(12)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  ages[, age_group_weight_value := NULL]
  ages[age_start >= 1, age_end := age_end - 1]
  ages[age_end == 124, age_end := 99]
  
  #expand ages
  split_dt <- expand_age(small_dt=data_to_split, age_dt = ages)
  
  super_region_dt <- get_location_metadata(location_set_id = 22)
  super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
  
  ##GET LOCS AND POPS
  pop_locs <- unique(split_dt$location_id)
  pop_years <- unique(split_dt$year_id)
  
  ## GET PULL LOCATIONS
  if (region_pattern == T){
    split_dt <- merge(split_dt, super_region_dt, by = "location_id")
    super_regions <- unique(split_dt$super_region_id) 
    locations <- super_regions
  } else {
    location_pattern_id <- 1 
    locations <- location_pattern_id   
  }

  ## GET AGE PATTERN
  print("getting age pattern")
  age_pattern <- get_age_pattern(locs = locations, id = 24345, age_groups = ages$age_group_id)  #id for age pattern model 
  age_pattern_model_version <- get_best_model_versions("modelable_entity", 24345, gbd_round_id = 6, decomp_step = "iterative")
  
  if (region_pattern == T) {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "super_region_id"))
  } else {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id"))
  }
  
  ## GET POPULATION INFO
  print("getting pop structure")
  pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = ages$age_group_id)
  split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))
  
  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("splitting data")
  it_split_dt <- age_split_data(split_dt)
  ######################################################################################################
  
  final_dt <- format_data_forfinal(it_split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                   original_dt = original)
  
  
  save_dir <- paste0(output_dir, "/age_split_data")
  if(!dir.exists(save_dir)){dir.create(save_dir)}
  #write.csv(split_data, paste0(save_dir, "/adjusted_data_for_upload_v", version,".csv"), row.names = FALSE)
  openxlsx::write.xlsx(final_dt, paste0(save_dir, "/adjusted_data_for_upload_v", version,".xlsx"), sheetName = "extraction")
  descript <- paste0("Age split data: sex split cv", crosswalk_version, " adjusted using age trend from mv", age_pattern_model_version$model_version_id)
  
  output_file <- paste0(save_dir, "/adjusted_data_for_upload_v", version,".xlsx")
  
  print(validate_input_sheet(bundle_id, output_file, error_log_path = "FILEPATH"))
  
  if(upload_crosswalk_version){
    print(save_crosswalk_version(bundle_version, output_file, description = descript))
    }
  
}




if(FALSE){
  data <- get_crosswalk_version(4316)
  data[underlying_nid==138581, is_outlier:=1]
  data[!is.na(crosswalk_parent_seq), seq:=NA]
  openxlsx::write.xlsx(data, "FILEPATH/age_pattern_crosswalk_version.xlsx", sheetName="extraction" )  
  save_crosswalk_version(iterative_bundle_version, "FILEPATH/age_pattern_crosswalk_version.xlsx", description = "")
  
}








