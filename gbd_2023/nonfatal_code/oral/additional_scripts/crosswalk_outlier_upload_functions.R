####Variables to import in this file,
# bundle_id
# bundle_version
# model_effects
# 
# save_dir
# measure
# decomp_step
# gbd_round_id

### Pull in agesex_split data, for bundle 212, crosswalk betas as well


apply_crosswalk_betas <- function(bun_id, model_effects_filepath, covariate, measure_name){
  #' @description Pulls in agesex split bundle version, applies crosswalk
  #' @param bun_id interger. Bundle to apply outliers to.
  #' @param input_data filepath. 
  #' 
  #' @return a filepath to a flat file which contains the bundle version data with outliers applied
  
  cov_map <- read.xlsx("FILEPATH")
  
  df <- read.xlsx(paste0("FILEPATH", bun_id, "_", measure_name, ".xlsx"))
  
  cvs <- cov_map$covariate[cov_map$bundle_id == bun_id & cov_map]
  
  if (bun_id == 262){
  
    df[cv_atchloss4ormore == 1, covariate := 'cv_atchloss4ormore']
    df[cv_atchloss4ormore == 1, reference := 0]
    df[cv_atchloss5ormore == 1, covariate := 'cv_atchloss5ormore']
    df[cv_atchloss5ormore == 1, reference := 0]
    df[cv_cpiclass3 == 1, covariate := 'cv_cpiclass3']
    df[cv_cpiclass3 == 1, reference := 0]
    
    dat_original[reference == 1, covariate := 'reference']
    
  } else{
    df[, reference := 1]
    df$reference[df$cv == 1] <- 0
    
  }
  
  
  df[reference == 1, covariate := 'reference']
  df <- df[!df$standard_error == 0]
  
  reference_var <- "reference"
  reference_value <- 1
  mean_var <- "mean"
  se_var <- "standard_error"
  covariate <- 'covariate'
  cov_names <- c("age", "covariate")
  
  # create datasets with standardized variable names
  orig_vars <- c(mean_var, se_var, reference_var, cov_names)
  print(orig_vars)
  
  # metareg_vars <- c(ratio_var, ratio_se_var, cov_names) # defined before running the model
  
  sex_names <- fread("FILEPATH")
  df <- merge(df, sex_names, by = c('sex'), all.x = TRUE)
  
  df <- df[, age := round((age_start + age_end)/2)]
  
  tmp_orig <- as.data.frame(dat_original) %>%
    .[, orig_vars] %>%
    setnames(orig_vars, c("mean", "se", "ref", "age", "covariate")) %>%
    mutate(ref = if_else(ref == reference_value, 1, 0, 0))
  
  
  ## *******************************************************************************
  ## Step 3-4. Apply ratios 
  #-- transform original bundle data in logit space
  #-- transform logit to normal space
  #-- apply ratios 
  ## *******************************************************************************
  
  ####Read in previously generated betas
  ratio_dconv_prev <- fread(paste0('FILEPATH', mod_lab, '/', bun_id, '_', cv, '_', measure_name, '_model_effects.csv'))
  
  beta_dconv <- ratio_dconv_prev$ratio
  beta_se_dconv <- exp(ratio_dconv_prev$se)
  sex_id <- ratio_dconv_prev$sex_id
  
  
  
  #Logit transform
  tmp_orig$mean_logit <- logit(tmp_orig$mean)
  tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
    mean_i <- tmp_orig[i, "mean"]
    se_i <- tmp_orig[i, "se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  
  
  
  test_dconv <- as.data.frame(cbind(beta_dconv, beta_se_dconv, sex_id))
  tmp_orig <- join(tmp_orig, test_dconv, by = "sex_id", type = "left", match = "first")
  
  
  tmp_orig2 <- copy(tmp_orig)
  
  if (bun_id == 262){
    tmp_orig2 <- tmp_orig %>%
      mutate(
        mean_logit_tmp = ifelse(tmp_orig2$covariate == 'cv_atchloss4ormore', mean_logit - beta_atch4, ifelse(tmp_orig2$covariate == 'cv_atchloss5ormore', mean_logit - beta_atch5, mean_logit - beta_cpi3)),# adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
        var_logit_tmp = ifelse(tmp_orig2$covariate == 'cv_atchloss4ormore', se_logit^2 + beta_se_atch4^2, ifelse(tmp_orig2$covariate == 'cv_atchloss5ormore', se_logit^2 + beta_se_atch5^2, se_logit^2 + beta_se_cpi3^2)), # adjust the variance
        se_logit_tmp = sqrt(var_logit_tmp)
      )
  } else{
    tmp_orig2 <- tmp_orig %>%
      mutate(
        mean_logit_tmp = mean_logit - beta_dmf, # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
        var_logit_tmp = se_logit^2 + beta_se_dmf^2, # adjust the variance
        se_logit_tmp = sqrt(var_logit_tmp)
      )
  }
  
  
  
  head(tmp_orig2)
  tail(tmp_orig2)
  
  
  # if original data point was a reference data point, leave as-is
  tmp_orig3 <- tmp_orig2 %>%
    mutate(
      mean_logit_adjusted = if_else(ref == 1, mean_logit, mean_logit_tmp),
      se_logit_adjusted = if_else(ref == 1, se_logit, se_logit_tmp),
      lo_logit_adjusted = mean_logit_adjusted - 1.96 * se_logit_adjusted,
      hi_logit_adjusted = mean_logit_adjusted + 1.96 * se_logit_adjusted,
      mean_adjusted = invlogit(mean_logit_adjusted),
      lo_adjusted = invlogit(lo_logit_adjusted),
      hi_adjusted = invlogit(hi_logit_adjusted) )
  
  head(tmp_orig3)
  tail(tmp_orig3)
  
  
  #estimate the adjusted SE using the delta method
  tmp_orig3$se_adjusted <- sapply(1:nrow(tmp_orig3), function(i) {
    ratio_i <- tmp_orig3[i, "mean_logit_adjusted"]
    ratio_se_i <- tmp_orig3[i, "se_logit_adjusted"]
    deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
  })
  
  head(tmp_orig3)
  tail(tmp_orig3)
  
  # Check for NaN SE 
  check <- select(tmp_orig3,  sex_id, ref,  mean, mean_adjusted, se, se_adjusted, lo_adjusted, hi_adjusted, mean_logit, se_logit, mean_logit_adjusted , se_logit_adjusted)
  head(check)
  tail(check)
  
  filter(check,is.na(se_adjusted))
  filter(check, se_adjusted == "Inf")
  
  # Correct NaN SE 
  #tmp_orig4<- tmp_orig3 %>%
  #  mutate(
  #    se_adjusted = if_else(ref == 0 & mean == 0, se, se_adjusted), 
  #    lo_adjusted = if_else(ref == 0 & mean == 0 & lo_adjusted == 0, mean_adjusted - 1.96 * se, lo_adjusted), 
  #    hi_adjusted = if_else(ref == 0 & mean == 0, mean_adjusted + 1.96 * se, hi_adjusted))
  
  # Correct for infinite adjusted SE
  tmp_orig4 <- tmp_orig3 %>%
    mutate(
      se_adjusted = if_else(ref == 0 & se_adjusted == "Inf", se, se_adjusted))
  
  tmp_orig4 <- tmp_orig4 %>%
    mutate(
      se_adjusted = if_else(ref == 1 & se_adjusted == "Inf", se, se_adjusted))
  
  tmp_orig4
  
  check <- select(tmp_orig4,  sex_id, ref,  mean, mean_adjusted, se, se_adjusted, lo_adjusted, hi_adjusted, mean_logit, se_logit, mean_logit_adjusted , se_logit_adjusted)
  
  filter(check,is.na(se_adjusted))
  filter(check, se_adjusted == "Inf")
  filter(check, ref == 0 & mean == 0)
  
  
  # 'final_data' is the original extracted data plus the new variables
  final_data <- cbind(
    dat_original, 
    tmp_orig4[, c("ref", "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
  )
  
  
  final_data[is.na(se_adjusted) & ref == 1, se_adjusted := standard_error]
  
  
  final_data[ref == 1, mean_adjusted:= mean]
  final_data[ref == 1, se_adjusted:= standard_error]
  final_data[ref == 1, hi_adjusted := upper]
  final_data[ref == 1, lo_adjusted := lower]
  final_data[ref == 1, hi_adjusted := upper]
  
  final_data$ref <- NULL
  
  final_data[cv_d_conversion == 0 & mean == 0, mean_adjusted := mean]
  final_data[cv_d_conversion == 0 & mean == 0, se_adjusted := standard_error]
  final_data[cv_d_conversion == 0 & mean == 0, hi_adjusted := upper]
  final_data[cv_d_conversion == 0 & mean == 0, lo_adjusted := lower]
  
  
  
  
  
  write.csv(final_data,
            file = paste0('FILEPATH', bun_id, "_", measure_name, "_crosswalk.csv"),
            row.names = FALSE)
  
  
}


#####Applying outliers

apply_outliers <- function(bun_id, bun_version_id, gbd_round_id, decomp_step){
  #' @description Pulls in agesex split bundle version, applies outliers to data according to 
  #' a prior outlier map
  #' @param bun_id interger. Bundle to apply outliers to.
  #' 
  #' @return a filepath to a flat file which contains the bundle version data with outliers applied
  
  #Pull in final age-sex split data
  df <- get_bundle_version()
  
  outlier_map <- read.xlsx(paste0("FILEPATH", bun_id, "_outlier_map.xlsx"),
                           sheetName = "gbd_2020")
  
  combined <- merge(df, outlier_map, by = c('location_id', "sex_id"))
  
  write.csv(combined, file = paste0("FILEPATH", bun_id, "_", measure, "outliered.csv"))
  
  
}



###Final Transformations, Upload

upload_processed_bundle <- function(bundle_id, gbd_round_id, decomp_step){
  #' @description Pulls in agesex split bundle version, applies outliers to data according to 
  #' a prior outlier map
  #' @param bun_id interger. Bundle to apply outliers to.
  #' 
  #' @return a filepath to a flat file which contains the bundle version data with outliers applied
  
  
  
  #“--plot_note paste0(“bundle: “, bundle, “, bundle_name: “, bundle_name, ” ,crosswalk: “, cv, “, trim: “, trim_pct)”
  final_data <- fread(paste0('FILEPATH', bundle_id, '_prevalence.csv'))
  
  
  
  #Split England locations - in GBD 2019 some England subnational locations would not pass validations.
  #This code reassigns the means to the location to the aggregate mean of the region the location is in.
  
  locs <- get_location_metadata(9, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  locs_35 <- get_location_metadata(35, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  england <- locs_35[!locs_35$location_id %in% locs$location_id]
  eng_data <- final_data[final_data$location_id %in% england$location_id]
  
  final_wo_eng <- final_data[!final_data$location_id %in% unique(eng_data$location_id)]
  
  eng_data <- eng_data[eng_data$location_name == "Yorkshire and the Humber", location_name := "Yorkshire and The Humber"]
  
  uk_pop_weights <- read.xlsx('FILEPATH')
  uk_pop_weights <- uk_pop_weights[, c('location_id', 'location_name', 'parent', 'weight')]
  
  #eng_data_step <- merge(eng_data, uk_pop_weights[, c("location_name", "location_id")], by = 'location_id', allow.cartesian = TRUE)
  
  eng_data_merged <- merge(eng_data, uk_pop_weights, by.x = 'location_name', by.y = "parent", allow.cartesian = TRUE)
  
  
  eng_data_merged$group_review <- ifelse(eng_data_merged$location_name == eng_data_merged$location_name.y, 0, 1)
  eng_data_merged$location_name <- eng_data_merged$location_name.y
  eng_data_merged$location_name.y <- NULL
  eng_data_merged$location_id.x <- eng_data_merged$location_id.y
  colnames(eng_data_merged)[colnames(eng_data_merged)=="location_id.x"] <- "location_id"
  eng_data_merged$location_id.y <- NULL
  eng_data_merged$sample_size <- eng_data_merged$sample_size * eng_data_merged$weight
  eng_data_merged$weight <- NULL
  eng_data_merged$step2_location_year <- "Splitting England subnationals"
  eng_data_merged <- eng_data_merged[eng_data_merged$group_review == 1 | is.na(eng_data_merged$group_review) | eng_data_merged$group_review == '']
  eng_data_merged <- eng_data_merged[is.na(eng_data_merged$group) & eng_data_merged$specificity == '', group_review := NA]
  
  final_data2 <- rbind(final_wo_eng, eng_data_merged, fill=TRUE)
  
  #Remove any group review studies, any data where standard error = 0, and create a parent seq
  
  final_data2 <- final_data2[!is.na(final_data2$standard_error)]
  final_data2 <- final_data2[final_data2$group_review == 1 | is.na(final_data2$group_review) | final_data2$group_review == ' ', ]
  final_data2$crosswalk_parent_seq <- final_data2$parent_seq
  
  #Writing final file out and uploading crosswalk
  
  write.xlsx(final_data2,
             file = paste0("FILEPATH", bundle, "_", measure, ".xlsx"),
             row.names = FALSE, sheetName = "extraction")
  
  result <- save_crosswalk_version(1685, 
                                   data_filepath = paste0('FILEPATH', bundle, "_Permanent_Carries_", cv, "_", cov_names,  ".xlsx"),
                                   description = "261_Permanent_Carries_Allcovariate_Crosswalk")
  
  
  
  
  
  
  
}








