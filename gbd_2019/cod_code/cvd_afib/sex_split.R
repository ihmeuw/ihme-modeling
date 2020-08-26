
## 
## Purpose: Function for sex-splitting
##

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2)


###### Paths, args
#################################################################################

central <- "FILEPATH"
  

###### Functions
#################################################################################

for (func in paste0(central, list.files(central))) source(paste0(func))
source("master_mrbrt_crosswalk_func.R")


sex_split <- function(data, 
                      xw_measure,
                      model_abbr = NULL, 
                      folder_root = "FILEPATH", 
                      subnat_to_nat = F, 
                      sex_age_overlap = 0, 
                      sex_year_overlap = 0,
                      sex_age_range = 0,
                      sex_year_range = 0,
                      sex_fix_zeros = T,
                      use_lasso = F,
                      id_vars = "id_var",
                      opt_method = "trim_maxL",
                      trim_pct = 0.1,
                      sex_remove_x_intercept = F,
                      sex_covs = "age_scaled",
                      
                      gbd_round_id = 6,
                      decomp_step = "step4",
                      logit_transform = T,
                      upload_crosswalk_version = F) {
  
  if (is.null(model_abbr)) model_abbr <- "sex_split"
  
  sex_split <- master_mrbrt_crosswalk(
                    
                    data = data,
                    model_abbr = model_abbr,
                    xw_measure = xw_measure,
                    folder_root = folder_root,
                    sex_split_only = T,
                    subnat_to_nat = subnat_to_nat,
                    
                    sex_age_overlap = sex_age_overlap,
                    sex_year_overlap = sex_year_overlap,
                    sex_age_range = sex_age_range,
                    sex_year_range = sex_year_range,
                    sex_fix_zeros = sex_fix_zeros,
                    use_lasso = use_lasso,
                    id_vars = id_vars,
                    opt_method = opt_method,
                    trim_pct = trim_pct,
                    sex_remove_x_intercept = sex_remove_x_intercept,
                    sex_covs = sex_covs,
                    
                    gbd_round_id = gbd_round_id,
                    decomp_step = decomp_step,
                    logit_transform = logit_transform,
                    
                    upload_crosswalk_version = upload_crosswalk_version)
  
  sex_split_data <- sex_split$sex_specific_data
  return(list(data = sex_split_data, obj = sex_split))
                  
}

