##' ***************************************************************************
##' Title: 1b_registry_cw_functions.R
##' ***************************************************************************
Sys.umask(mode = 002)


os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library(data.table)
library(magrittr)
library(ggplot2)
library(gtools)
library(openxlsx)
library(readxl)
library(dplyr)
library(stringr)
library(readxl)


invisible(lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  }))
'%ni%' <- Negate('%in%')


##' *******************************************************
##' 2. PREP COVARIATES FOR CROSSWALKS
##' *******************************************************
prep_covariates <- function(registry, covariate) {

  uniques <- c("nid", "underlying_nid","location_id", "sex", "year_start", "year_end", "age_start", "age_end", "short_registry_name", "note_SR")
  registry_filepath <- paste0("/ihme/mnch/BornThisWay/Congenital/Data/1_Raw/Registry/raw_", registry, ".xlsx")
  registry_data <- as.data.table(read.xlsx(registry_filepath))
  
  if (registry == "eurocat" & covariate == "cv_livestill"){
    
    print(paste0(registry, " ~ ", covariate))
    registry_data <-registry_data[!is.na(cases_fd)] ## subsetting to only rows where fetal deaths cases are recorded
    

  } else if (registry == "eurocat" & covariate == "cv_excludes_chromos"){
    
    print(paste0(registry, " ~ ", covariate))
    registry_data <- registry_data[!is.na(cases_lb_nonchromo)] # subsetting to only rows where live_birth cases excluding chromosomal disorders are recorded
  } else{
    
    print(paste0(registry, " ~ ", covariate))
    registry_data <- copy(registry_data)
    
  }
  
  validate <- copy(registry_data)
  validate[, occurrence := .N , by = c(uniques, "bundle_id")]
  validate <- validate[, c(uniques, "occurrence", "bundle_id"), with = F]
  unique_rows <- validate[occurrence == 1]
  multiple <- validate[occurrence > 1]
  
  agg <- unique(multiple)
  no_agg <- unique(unique_rows)
  
  ### aggregate
  agg_rows <- copy(registry_data)
  agg_rows[, occurrence := .N , by = c(uniques, "bundle_id")]
  agg_rows_no_agg <- agg_rows[occurrence == 1]
  agg_rows_agg <- agg_rows[occurrence > 1]
  
  agg_rows_agg[, sample_size := mean(sample_size), by = c(uniques, "bundle_id")]
  agg_rows_agg[, cases := sum(cases), by =c(uniques, "bundle_id")]
  agg_rows_agg[, mean := sum(mean), by = c(uniques, "bundle_id")]
  agg_rows_agg[, case_name := lapply(.SD, paste0, collapse=" + "), by = c(uniques, "bundle_id"), .SDcols = "case_name"]
  
  agg_rows_unique <- unique(agg_rows_agg, by = c(uniques, "sample_size", "cases", "case_name", "bundle_id"))
  
  full_registry_data <- rbind(agg_rows_no_agg, agg_rows_unique)
  full_registry_data[, c("cv_livestill", "cv_excludes_chromos") := 0]
  full_registry_data[, standard_error := "" ]
  full_registry_data[, standard_error := as.numeric(standard_error)]
  z <- qnorm(0.975)
  full_registry_data[(is.na(standard_error) | standard_error == 0),
                     standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  
  ### check counts ###
  print(paste0('validate only 1 counts   ~ ', nrow(no_agg)))
  print(paste0('final only 1 counts  ~ ', nrow(agg_rows_no_agg)))
  
  print(paste0('agg counts level 1 ~ ', nrow(agg)))
  print(paste0('final agg counts level 1 ', nrow(agg_rows_unique)))
  
  if (registry == "eurocat"){
    
    cov_validate <- copy(registry_data)
    cov_validate[, occurrence := .N , by = c(uniques, "bundle_id")]
    cov_validate <- cov_validate[, c(uniques, "occurrence", "bundle_id"), with = F]
    cov_unique_rows <- cov_validate[occurrence == 1]
    cov_multiple <- cov_validate[occurrence > 1]
    
    cov_agg <- unique(cov_multiple)
    cov_no_agg <- unique(cov_unique_rows)
    
    ### aggregate
    cov_agg_rows <- copy(registry_data)
    
    if( covariate == "cv_livestill") {
      
      cov_agg_rows[, cv_livestill:= 1]
      cov_agg_rows[, cases := cases_lb + cases_fd]
      cov_agg_rows[, mean := cases/sample_size]
      
      
    } else {
      
      cov_agg_rows[, c("cv_excludes_chromos") := 1]
      cov_agg_rows[, cases:= cases_lb_nonchromo]
      cov_agg_rows[, mean := cases/sample_size]
    }  
    
    ##this is for china  
  } else {
    
    cov_validate <- copy(registry_data)
    cov_validate[, occurrence := .N , by = c(uniques, "bundle_id")]
    cov_validate <- cov_validate[, c(uniques, "occurrence", "bundle_id"), with = F]
    cov_unique_rows <- cov_validate[occurrence == 1]
    cov_multiple <- cov_validate[occurrence > 1]
    
    cov_agg <- unique(cov_multiple)
    cov_no_agg <- unique(cov_unique_rows)
    
    ### aggregate
    cov_agg_rows <- copy(registry_data)
    cov_agg_rows[, c("cv_livestill") := 1]
    cov_agg_rows[, cases := cases + cases_stillbirth] ### check if this includes ToP
    cov_agg_rows[, mean := cases/sample_size]
    
  }
  
  cov_agg_rows[, occurrence := .N , by = c(uniques, "bundle_id")]
  cov_agg_rows_no_agg <- cov_agg_rows[occurrence == 1]
  cov_agg_rows_agg <- cov_agg_rows[occurrence > 1]
  
  cov_agg_rows_agg[, sample_size := mean(sample_size), by = c(uniques, "bundle_id")]
  cov_agg_rows_agg[, cases := sum(cases), by =c(uniques, "bundle_id")]
  cov_agg_rows_agg[, mean := sum(mean), by = c(uniques, "bundle_id")]
  cov_agg_rows_agg[, case_name := lapply(.SD, paste0, collapse=" + "), by = c(uniques, "bundle_id"), .SDcols = "case_name"]
  
  cov_agg_rows_unique <- unique(cov_agg_rows_agg, by = c(uniques, "sample_size", "cases", "case_name", "bundle_id"))
  
  cov_dummy_data <- rbind(cov_agg_rows_no_agg, cov_agg_rows_unique)
  cov_dummy_data[, standard_error := "" ]
  cov_dummy_data[, standard_error := as.numeric(standard_error)]
  z <- qnorm(0.975)
  cov_dummy_data[(is.na(standard_error) | standard_error == 0),
                 standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  
  print(paste0('validate only 1 counts   ~ ', nrow(cov_no_agg)))
  print(paste0('final only 1 counts  ~ ', nrow(cov_agg_rows_no_agg)))
  
  print(paste0('agg counts level 1 ~ ', nrow(cov_agg)))
  print(paste0('final agg counts level 1 ', nrow(cov_agg_rows_unique)))
  
  
  # After merging mean.x and mean.y are flipped alternate: reference so before running MRBRT this must be switched 
  full_dt <- merge(full_registry_data[, c("nid", "underlying_nid", "location_id", "sex", "year_start", "year_end", "age_start",
                                          "age_end", "short_registry_name", "cases", "sample_size", "mean", "standard_error", 
                                          covariate,"bundle_id"), with = F], 
                   cov_dummy_data[, c("nid", "underlying_nid", "location_id", "sex", "year_start", "year_end", "age_start",
                                      "age_end", "short_registry_name", "cases", "sample_size", "mean", "standard_error",
                                      covariate, "bundle_id"), with = F],
                   by = c("nid", "underlying_nid", "location_id", "sex", "year_start", "year_end", "age_start",
                          "age_end", "short_registry_name", "bundle_id", "sample_size"))
  
  full_dt <- full_dt[sex == 'Both', sex_id := 3]
  full_dt <- full_dt[sex == 'Male', sex_id := 1]
  full_dt <- full_dt[sex == 'Female', sex_id := 2]
  #View(full_dt)
  print(paste0("FILEPATH", covariate, "_", registry, ".xlsx"))
  
  write.xlsx(full_dt, paste0("FILEPATH", covariate, "_", registry, ".xlsx"),
             sheetName = "extraction")
  
}


##' *******************************************************
##' 3. MAKE DUMMY REFERENCE DATASET
##' *******************************************************

prep_dummy_reference <- function(source_registries){
  #' @param source_registries character. The source registry datasets from which we create dummy rows.
  #' @output an Excel file of the dummy reference rows. Saved here "/ihme/mnch/BornThisWay/Congenital/Data/1_Raw/Registry/prepped_registries/dummy/"
  
  
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  ## subset out singapore because we process it separately
  reg_cw_map <- reg_cw_map[!(target_registry == 'singapore')]
  
  # Uncomment below to run interactively in a for loop
  #for (registry in source_registries){
  
  print(registry)
  
  reg_map_subset <- reg_cw_map[source_registry == registry]
  
  registry_filepath <- paste0("FILEPATH", registry, ".xlsx")
  registry_data <- as.data.table(read.xlsx(registry_filepath))
  
  reg_map_subset <- reg_cw_map[source_registry == registry]
  bundles <- unique(reg_map_subset$id_input)
  
  for(bun in bundles){
    ### subset out mortality data, write out files which must be appended back in to the bundle prior to uploading
    write.xlsx(registry_data[bundle_id == bun & measure != 'prevalence'], paste0("FILEPATH"))
    registry_data <- registry_data[measure == 'prevalence']
    
    print(bun)
    
    target_registries <- unique(reg_map_subset[id_input == bun & target_registry != 'newzealand']$target_registry)
    
    for (target in target_registries){
      
      print(target)
      ref_case_names <- reg_map_subset[id_input == bun & target_registry == target, list(source_case_name_reference)]
      ref_case_names <- ref_case_names[!is.na(source_case_name_reference)]
      ref_case_names <- ref_case_names[, unique(source_case_name_reference)]
      #print(ref_case_names)
      
      alt_case_names <- reg_map_subset[id_input == bun & target_registry == target, list(target_case_name_alternative)]
      alt_case_names <- alt_case_names[!is.na(target_case_name_alternative)]
      alt_case_names <- alt_case_names[, unique(target_case_name_alternative)]
      #print(alt_case_names)
      
      registry_data_to_use <- copy(registry_data)
      #print(unique(registry_data_to_use$case_name))
      
      registry_data_to_use <- registry_data_to_use[case_name %in% unique(ref_case_names) | case_name %in% unique(alt_case_names)]
      print(unique(registry_data_to_use$case_name))
      
      source_reg_names <- unique(registry_data_to_use$case_name)
      map_names <- c(ref_case_names, alt_case_names)
      check <- setdiff(map_names, source_reg_names)
      check2 <- setdiff(source_reg_names, map_names)
      #' Print a warning for any target/bundle combinations where the unique names in the raw source registry don't match the case names in the reg_cw_map
      if(length(check) != 0L | length(check2) != 0L ) { 
        print(paste0('Warning: The source registry case names do not match the crosswalk registry map case names for ', target, 'target registry, bundle ', bun,
                     '. Please double-check the reg_cw_map contains the correct case names you need.'))
      }
      
      
      ref <- registry_data_to_use[case_name %in% unique(ref_case_names)]
      ref <- ref[!is.na(case_name)]
      ref[, cv_dummy := 0]
      print(unique(ref$case_name))
      print(unique(ref$sex))
      
      alt <- registry_data_to_use[case_name %in% unique(alt_case_names)]
      alt <- alt[!is.na(case_name)]
      alt[, cv_dummy := 1]
      print(unique(alt$case_name))
      print(unique(alt$sex))
      
      combined <- rbind(ref, alt)
      #print(nrow(combined))
      
      output_dir <- paste0("FILEPATH")
      archive_dir <- paste0("FILEPATH")
      
      file.rename(paste0(output_dir, target,'_to_', registry, '_', bun, '.csv'), paste0(archive_dir, target,'_to_', registry, '_', bun, '.csv'))
      
      write.csv(combined, paste0(output_dir, target,'_to_', registry, '_', bun, '.csv'), row.names = F)
    }
  }
}



#' *******************************************************
#' 4. AGGREGATE- collapse by nid, underlying_nid, location, sex, year, age, short_registry_name, cv_dummy
#' *******************************************************

aggregate_dummy_reference <- function(source_registries){
  #' @param source_registries character. The source registry datasets from which we create dummy rows.
  #' @output an Excel file of the aggregated dummy reference rows. 
  
  uniques <- c("nid", "underlying_nid","location_id", "sex", "year_start", "year_end", "age_start", "age_end", "short_registry_name", "cv_dummy")
  
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, paste0("FILEPATH"))))
  ## subset out singapore because we process it separately
  reg_cw_map <- reg_cw_map[!(target_registry == 'singapore')]
  
  # Uncomment below to run interactively in a for loop
  # for(registry in source_registries){
  
  print(paste0('source ', registry))
  reg_map_subset <- reg_cw_map[source_registry == registry]
  bundles <- unique(reg_map_subset$id_input)
  
  for(bun in bundles){
    
    print(bun)
    target_registries <- unique(reg_map_subset[id_input == bun & target_registry != 'newzealand']$target_registry)
    
    for (target in target_registries){
      
      print(paste0('target ', target))
      
      bun_filepath <- paste0("FILEPATH", target,'_to_', registry, "_", bun, ".csv")
      
      dummy_data <- as.data.table(fread(bun_filepath))
      dummy_data[, .N, by= sex]
      dummy_data[, bundle_id := as.character(bundle_id)]
      if (registry == 'newzealand'){
        dummy_data$short_registry_name <- 'newzealand'
      }
      
      ### check what numbers should be aggregated to ###
      validate <- copy(dummy_data)
      validate[, occurrence := .N , by = c(uniques)]
      validate <- validate[, c(uniques, "occurrence"), with = F]
      unique_rows <- validate[occurrence == 1]
      multiple <- validate[occurrence > 1]
      
      agg <- unique(multiple)
      no_agg <- unique(unique_rows)
      
      ### aggregate
      agg_rows <- copy(dummy_data)
      agg_rows[, occurrence := .N , by = c(uniques)]
      agg_rows_no_agg <- agg_rows[occurrence == 1]
      agg_rows_agg <- agg_rows[occurrence > 1]
      
      agg_rows_agg[, sample_size := as.numeric(sample_size)]
      agg_rows_agg[, sample_size := mean(sample_size), by = c(uniques)] #### sample size should be the same for each row so doing mean(SS) is really just SS
      agg_rows_agg[, cases := sum(cases), by =c(uniques)]
      agg_rows_agg[, mean := sum(mean), by = c(uniques)] #### mean can be summed because the sample size is equivalent for each row
      agg_rows_agg[, case_name := lapply(.SD, paste0, collapse=" + "), by = c(uniques), .SDcols = "case_name"]
      agg_rows_agg[, bundle_id := lapply(.SD, paste0, collapse=" + "), by = c(uniques), .SDcols = "bundle_id"] ### To do: check what this is doing
      
      agg_rows_unique <- unique(agg_rows_agg, by = c(uniques, "sample_size", "cases", "case_name", "bundle_id"))
      
      full_dummy_data <- rbind(agg_rows_no_agg, agg_rows_unique)
      
      full_dummy_data[, standard_error := "" ]
      full_dummy_data[, standard_error := as.numeric(standard_error)]
      z <- qnorm(0.975)
      full_dummy_data[(is.na(standard_error) | standard_error == 0),
                      standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
      
      ### check counts ###
      print(paste0('validate only 1 counts   : ', nrow(no_agg)))
      print(paste0('final only 1 counts  : ', nrow(agg_rows_no_agg)))
      
      print(paste0('agg counts level 1 : ', nrow(agg)))
      print(paste0('final agg counts level 1 ', nrow(agg_rows_unique)))
      
      ### divide the data and then merge in order to get separate columns for mean.x and mean.y and SE.x and SE.y (ref, alt)
      cv_dummy_0 <- full_dummy_data[cv_dummy == 0, c("nid", "underlying_nid", "location_name", "sex", "year_start", "year_end", "age_start",
                                                     "age_end", "short_registry_name", "cases", "sample_size", "mean", "standard_error",
                                                     "cv_dummy")]
      #print(unique(cv_dummy_0$sex))
      cv_dummy_1 <- full_dummy_data[cv_dummy == 1, c("nid", "underlying_nid" ,"location_name", "sex", "year_start", "year_end", "age_start",
                                                     "age_end", "short_registry_name", "cases", "sample_size", "mean", "standard_error",
                                                     "cv_dummy")]
      print(unique(cv_dummy_1$sex))
      merge_dummy <- merge(cv_dummy_0, cv_dummy_1, by = c("nid", "underlying_nid", "location_name", "sex", "year_start", "year_end", "age_start",
                                                          "age_end", "short_registry_name", "sample_size"))
      #print(unique(merge_dummy$sex))
      
      offset <- 0.5 * median(full_dummy_data[mean != 0, mean])
      merge_dummy[, `:=` (mean.x = mean.x + offset,
                          mean.y = mean.y + offset)]
      nrow(merge_dummy[mean.x == 0 | mean.y == 0])
      merge_dummy <- merge_dummy[!(mean.x == 0 | mean.y == 0)]
      
      #print(paste0('merged rows : ', nrow(merge_dummy)))
      
      output_dir <- paste0("FILEPATH")
      archive_dir <- paste0("FILEPATH")
      
      file.rename(paste0(output_dir, target,'_to_', registry, '_', bun, '.xlsx'), paste0(archive_dir, target,'_to_', registry, '_', bun, '.xlsx'))
      
      write.xlsx(merge_dummy, paste0(output_dir, target,'_to_', registry, '_', bun, '.xlsx'), sheetName = "extraction")
    }
  }
}



#' ******************************************
#' 5. COMBINE DATASETS FOR CROSSWALKING
#' ******************************************

combine_prepped <- function(target_registries){
  #' @param target_registries character. The target registries to get crossawlked
  #' @output excel file. 1 per target registry bundle. 
  
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  ## subset out singapore because we process it separately
  reg_cw_map <- reg_cw_map[!(target_registry == 'singapore')]
  #target_registries <- unique(reg_cw_map[target_registry != 'newzealand']$target_registry)
  
  for (registry in target_registries){
    map_subset <- reg_cw_map[target_registry == registry]
    print(registry)
    
    
    for(bun in unique(map_subset$id_input)){
      print(bun)
      
      source_registries <- unique(map_subset[id_input == bun & source_registry != 'newzealand']$source_registry)
      
      combined_s_df <- data.table()
      for(source in source_registries){
        
        print(paste0(registry, " to ", source))
        
        s_dir <- paste0("FILEPATH", registry, '_to_', source, "_", bun, ".xlsx")
        
        s_df <- read.xlsx(s_dir) %>% as.data.table()
        detail <- data.table()
        
        combined_s_df <- rbind(s_df, combined_s_df)
        
        print(paste0(source, " ~ ", nrow(s_df) ))
        
      }
      
      #print(paste0("combined ~ ", nrow(combined_s_df)))
      
      output_dir <- paste0("FILEPATH")
      archive_dir <- paste0("FILEPATH")
      
      file.rename(paste0(output_dir, registry, "/", bun, ".csv"), paste0(archive_dir, registry, "/", bun, ".csv"))
      
      write.csv(combined_s_df, paste0(output_dir, registry, "/", bun, ".csv"), row.names = F)
      
    }
  }
}

#' *******************************************************
#' 6. MAKE SINGAPORE DUMMY REFERENCE DATASET
#' *******************************************************

prep_sing_reference <- function(){
  #' @output an Excel file of the Singapore dummy reference rows.
  #' @note   We don't currently crosswalk Turner, Klinefelter, and Congenital genital (437,438, 618) because they have sex specific rows for which there is no
  #'         corresponding source registry row (i.e. Eurocat is not sex specific) 
  #' @note   In the reg_cw_map we switched the source_case_name_reference and target_case_name_alternative in order to be able to maintain the same order of operations
  #'         when applying the crosswalks. This is because the Singapore case names are much more broad than any other case names so we need to parse out into more 
  #'         detailed bundles while other registries must be adjusted when they are not tracking enough case names. 
  
  eurocat_filepath <- paste0("FILEPATH")
  eurocat <- as.data.table(read.xlsx(eurocat_filepath))
  
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  ## subset out all registries except singapore
  reg_cw_map <- reg_cw_map[target_registry == 'singapore']
  
  eurocat_to_use <- eurocat[case_name %in% unique(reg_cw_map$source_case_name_reference) | case_name %in% unique(reg_cw_map$target_case_name_alternative)]
  
  singapore_case_names <- unique(reg_cw_map$id_input)
  
  # first create datasets for each alternate:reference definition
  for(case in unique(singapore_case_names)){
    reg_map_subset <- reg_cw_map[id_input == case]
    
    for(bun in unique(reg_map_subset[bundle_id_output %ni% c(437,438)]$bundle_id_output)){
      print(bun)
      
      #The map is switched for Singapore so that reference case names are actually alternative case names. This is correct!
      ref_case_names <- reg_map_subset[bundle_id_output == bun, list(source_case_name_reference)]
      ref_case_names <- ref_case_names[!is.na(source_case_name_reference)]
      ref_case_names <- ref_case_names[, unique(source_case_name_reference)]
      print(paste0('reference ', ref_case_names))
      
      alt_case_names <- reg_map_subset[id_input == case, list(target_case_name_alternative)]
      alt_case_names <- alt_case_names[!is.na(target_case_name_alternative)]
      alt_case_names <- alt_case_names[, unique(target_case_name_alternative)]
      print(paste0('alt ', alt_case_names))
      
      ref <- eurocat_to_use[case_name %in% unique(ref_case_names)]
      ref <- ref[!is.na(case_name)]
      ref[, cv_dummy := 0]
      
      
      alt <- eurocat_to_use[case_name %in% unique(alt_case_names)]
      alt <- alt[!is.na(case_name)]
      alt[, cv_dummy := 1]
      
      combined <- rbind(ref, alt)
      
      output_dir <- paste0("FILEPATH")
      archive_dir <- paste0("FILEPATH")
      
      file.rename(paste0(output_dir, "singapore_to_eurocat_", bun, ".csv"), paste0(archive_dir, "singapore_to_eurocat_", bun, ".csv"))
      
      write.csv(combined, paste0(output_dir, "singapore_to_eurocat_", bun, ".csv"), row.names = F)
      
    }
    
  }
}

#' **********************************************************************************************************************************
#' 7. AGGREGATE SINGAPORE DUMMY REFERENCE DATASET (by nid, underlying_nid, location, sex, year, age, short_registry_name, cv_dummy)
#' ******************************************************* **************************************************************************

aggregate_sing_reference <- function(){
  #' @output an Excel file of the aggregated Singapore dummy reference rows. 
  #' 
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  reg_cw_map <- reg_cw_map[target_registry == 'singapore']
  
  singapore_case_names <- unique(reg_cw_map$id_input)
  
  uniques <- c("nid", "underlying_nid","location_id", "sex", "year_start", "year_end", "age_start", "age_end", "short_registry_name", "cv_dummy")
  
  for(case in unique(singapore_case_names)){
    print(case)
    
    reg_map_subset <- reg_cw_map[id_input == case]
    
    for(bun in unique(reg_map_subset[bundle_id_output %ni% c(437,438)]$bundle_id_output)){
      print(bun)
      
      bun_filepath <- paste0("FILEPATH", bun, ".csv")
      
      dummy_data <- as.data.table(fread(bun_filepath))
      dummy_data[, bundle_id := as.character(bundle_id)]
      
      ### aggregate
      agg_rows <- copy(dummy_data)
      agg_rows[, occurrence := .N , by = c(uniques)]
      agg_rows_no_agg <- agg_rows[occurrence == 1]
      agg_rows_agg <- agg_rows[occurrence > 1]
      
      agg_rows_agg[, sample_size := mean(sample_size), by = c(uniques)]
      agg_rows_agg[, cases := sum(cases), by =c(uniques)]
      agg_rows_agg[, mean := sum(mean), by = c(uniques)]
      agg_rows_agg[, case_name := lapply(.SD, paste0, collapse=" + "), by = c(uniques), .SDcols = "case_name"]
      agg_rows_agg[, bundle_id := lapply(.SD, paste0, collapse=" + "), by = c(uniques), .SDcols = "bundle_id"]
      
      agg_rows_unique <- unique(agg_rows_agg, by = c(uniques, "sample_size", "cases", "case_name", "bundle_id"))
      
      full_dummy_data <- rbind(agg_rows_no_agg, agg_rows_unique)
      full_dummy_data[, standard_error := "" ]
      full_dummy_data[, standard_error := as.numeric(standard_error)]
      z <- qnorm(0.975)
      full_dummy_data[(is.na(standard_error) | standard_error == 0),
                      standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
      
      cv_dummy_0 <- full_dummy_data[cv_dummy == 0, c("nid", "underlying_nid", "location_name", "sex", "year_start", "year_end", "age_start",
                                                     "age_end", "short_registry_name", "cases", "sample_size", "mean", "standard_error",
                                                     "cv_dummy")]
      cv_dummy_1 <- full_dummy_data[cv_dummy == 1, c("nid", "underlying_nid" ,"location_name", "sex", "year_start", "year_end", "age_start",
                                                     "age_end", "short_registry_name", "cases", "sample_size", "mean", "standard_error",
                                                     "cv_dummy")]
      
      merge_dummy <- merge(cv_dummy_0, cv_dummy_1, by = c("nid", "underlying_nid", "location_name", "sex", "year_start", "year_end", "age_start",
                                                          "age_end", "short_registry_name", "sample_size"))
      
      # add an offset since zeros can't be used in the ratio and SE calculations. Calculate the offset as 50% of the 
      # median of the nonzero values, and add the offset to every matched pair of means.
      offset <- 0.5 * median(full_dummy_data[mean != 0, mean])
      merge_dummy[, `:=` (mean.x = mean.x + offset,
                          mean.y = mean.y + offset)]
      nrow(merge_dummy[mean.x == 0 | mean.y == 0]) 
      
      print(paste0('merged rows ~ ', nrow(merge_dummy)))
      
      # subset to full alternate and full reference
      output_dir <- paste0("FILEPATH")
      archive_dir <- paste0("FILEPATH")
      
      file.rename(paste0(output_dir, "singapore_to_eurocat_", bun, ".xlsx"), paste0(archive_dir, "singapore_to_eurocat_", bun, ".xlsx"))
      
      write.xlsx(merge_dummy, paste0(output_dir, "singapore_to_eurocat_", bun, ".xlsx"), sheetName = "extraction")
      
      
      # also write out to the prepped_for_crosswalk directory. This is the "combine_prepped" function for all other registries but singapore's only source is eurocat
      # so we don't need to do that
      
      output_dir <- paste0("FILEPATH")
      archive_dir <- paste0("FILEPATH")
      
      file.rename(paste0(output_dir, "singapore/", bun, ".csv"), paste0(archive_dir, "singapore/", bun, ".csv"))
      
      write.csv(merge_dummy, paste0(output_dir, "singapore/", bun, ".csv"), row.names = F)
      
      
    }
  }
}


##' *******************************************************
##' 8. CREATE MATCHED DF AND RUN MRBRT (CROSSWALK)
##' *******************************************************

crosswalk_registries <- function(target_registries){
  #' @param character. The target registries we want to adjust.
  #' @output Each target registry outputs 2 files per bundle
  #'         1. CWData and CWModel pickle files
  #'         2. A csv file of the coefficient summary: mean effect, standard error, gamma, random effects
  #' @note   We don't currently crosswalk Turner, Klinefelter, and Congenital genital (437,438, 618) because they have sex specific rows for which there is no
  #'         corresponding source registry row (i.e. Eurocat is not sex specific)
  
  ############# These are the crosswalk packages. Make sure you are running the most updated R singularity image or this will fail to load
  library(reticulate)
  library(crosswalk, lib.loc = "FILEPATH")
  library(dplyr)
  library(data.table)
  ###############################

  # Uncomment below for loop if not launching in a qsub
  #for(registry in target_registries){
  measure <- 'prevalence'
  
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  
  
  print(registry)
  
  map_subset <- reg_cw_map[target_registry == registry]
  
  reg_data <- read.xlsx(paste0("FILEPATH", registry, '.xlsx')) %>% as.data.table()
  reg_data <- reg_data[, group_id := paste0(nid, '_', underlying_nid)]
  
  if (registry == 'singapore'){
    bundles <- unique(map_subset[bundle_id_output %ni% c(437, 438, 618)]$bundle_id_output)
  }else{
    bundles <- unique(map_subset[id_input != 618]$id_input)
  }
  
  for(bun in bundles){
    print(bun)
    
    dir <- paste0("FILEPATH", registry, "/", bun, ".csv")
    df_matched <- as.data.table(fread(dir))
    df_matched <- df_matched[, group_id := paste0(nid, '_', underlying_nid)]
    df_matched <- df_matched[, list(mean.x, standard_error.x, mean.y, standard_error.y, group_id)]
    df_matched <- df_matched[!is.na(mean.x)]
    setnames(df_matched, c("mean.x", "standard_error.x", "mean.y", "standard_error.y"), c("prev_ref", "prev_se_ref", "prev_alt", "prev_se_alt"))
    
    
    #df_matched <<- as.data.frame(df_matched)
    print(length(unique(df_matched$group_id)))
    
    print("transforming data")
    dat_diff <- as.data.frame(cbind(
      delta_transform(
        mean = df_matched$prev_alt,
        sd = df_matched$prev_se_alt,
        transformation = "linear_to_logit"),
      delta_transform(
        mean = df_matched$prev_ref,
        sd = df_matched$prev_se_ref,
        transformation = "linear_to_logit")
    ))
    
    names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")
    
    print("calculating difference")
    df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
      df = dat_diff, 
      alt_mean = "mean_alt", alt_sd = "mean_se_alt",
      ref_mean = "mean_ref", ref_sd = "mean_se_ref" )
    
    df_matched$altvar = "alternate"
    df_matched$refvar = "reference"
    
    df_matched <<- df_matched # The CW functions can only access global variables so I'm assigning these to be global variables
    
    print("Creating CWData")
    
    df1 <- CWData(
      df = df_matched,          # dataset for metaregression
      obs = "logit_diff",       # column name for the observation mean
      obs_se = "logit_diff_se", # column name for the observation standard error
      alt_dorms = "altvar",     # column name of the variable indicating the alternative method
      ref_dorms = "refvar",     # column name of the variable indicating the reference method
      covs = list(),     # names of columns to be used as covariates later
      study_id = "group_id",    # name of the column indicating group membership, usually the matching groups
      add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
    )
    py_save_object(object = df1, filename = paste0("FILEPATH", registry, "/df1", bun, ".pkl"), pickle = "dill")
    
    df1 <<- df1 # The CW functions can only access global variables so I'm assigning these to be global variables
    
    fit1 <- CWModel(
      cwdata = df1,            # object returned by `CWData()`
      obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
      use_random_intercept = TRUE,
      cov_models = list(       # specifying predictors in the model; see help(CovModel)
        CovModel(cov_name = "intercept")),
      gold_dorm = "reference",   # the level of `alt_dorms` that indicates it's the gold standard
      inlier_pct = 0.9
    )
    py_save_object(object = fit1, filename = paste0("FILEPATH", registry, "/fit1", bun, ".pkl"), pickle = "dill")
    
    
    df_result <- fit1$create_result_df()
    
    print(paste0('saving betas FILEPATH', registry, '/', bun, '.csv'))
    write.csv(df_result, paste0('FILEPATH', registry, '/', bun, '.csv'))
    
  }
}




##' *******************************************************
##' 9. MAKE FUNNEL PLOTS
##' *******************************************************

make_funnel_plots <- function(target_registries){
  #' @param character. The target registries we want to adjust.
  #' @output Each target registry outputs 2 more files per bundle 
  #'         1. A funnel plot
  #'         2. A detail csv of the target registry numerator case names and the source registry denominator case names, and the beta.
  #' @note   We don't currently crosswalk Turner, Klinefelter, and Congenital genital (437,438, 618) because they have sex specific rows for which there is no
  #'         corresponding source registry row (i.e. Eurocat is not sex specific) 
  
  map <- fread("FILEPATH")
  
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  
  for(registry in target_registries){
    
    print(registry)
    
    map_subset <- reg_cw_map[target_registry == registry]
    map_subset <- map_subset[!(source_registry == 'newzealand')]
    
    
    if (registry == 'singapore'){
      bundles <- unique(map_subset[bundle_id_output %ni% c(437, 438, 618)]$bundle_id_output)
    }else{
      bundles <- unique(map_subset[id_input != 618]$id_input)
    }
    for(bun in bundles){
      bun_metadata <- map[bundle_id == bun]
      source_registries <- unique(reg_cw_map[target_registry == registry & bundle_id_output == bun]$source_registry)
      
      dir <- paste0("FILEPATH", registry, "/", bun, ".csv")
      df_matched <- as.data.table(fread(dir))
      df_matched <- df_matched[, group_id := paste0(nid, '_', underlying_nid)]
      
      
      #### CREATE A DETAIL CSV ####
      case_name_detail_df <- data.table()
      for(source in source_registries){
        ratio_df <- data.table()
        ratio_df <- ratio_df[, target_registry := registry]
        ratio_df <- ratio_df[, bundle := bun]
        ratio_df <- ratio_df[, source_registry := source]
        numerator <- unique(reg_cw_map[target_registry == registry & bundle_id_output == bun &
                                         target_case_name_alternative != 'NA' &
                                         source_registry == source]$target_case_name_alternative)
        ratio_df <- ratio_df[, numerator := paste(numerator, collapse=" + ")]
        denominator <-  unique(reg_cw_map[target_registry == registry & bundle_id_output == bun  &
                                            source_case_name_reference != 'NA' &
                                            source_registry == source]$source_case_name_reference)
        ratio_df <-ratio_df[, denominator := paste(denominator, collapse=" + ")]
        beta_dir <- paste0("FILEPATH", registry, '/', bun, '.csv')
        ratio <- fread(beta_dir) %>% as.data.table()
        gamma <- ratio[dorms == 'alternate']$gamma
        beta <- ratio[dorms == 'alternate']$beta
        beta_se <- ratio[dorms == 'alternate']$beta_sd
        lower <- beta - 1.96 * beta_se
        upper <- beta + 1.96 * beta_se
        exp_beta <- signif(exp(beta), digits = 4)
        exp_lower <- signif(exp(lower), digits = 4)
        exp_upper <- signif(exp(upper), digits = 4)
        beta <- signif(beta, digits = 4)
        lower <- signif(lower, digits = 4)
        upper <- signif(upper, digits = 4)
        ratio_df <- ratio_df[, beta := paste0(beta, " (", lower, ", ", upper, ")")]
        ratio_df <- ratio_df[, beta_exp := paste0(exp_beta, " (", exp_lower, ", ", exp_upper, ")")]
        ratio_df <- ratio_df[, gamma := gamma]
        ratio_df <- ratio_df[, N_re := (length(unique(df_matched$group_id)))]
        
        ratio_df <- ratio_df[, trim := .1]
        case_name_detail_df <- rbind(case_name_detail_df, ratio_df)
      }
      write.csv(case_name_detail_df, paste0("FILEPATH", registry, '/detail_', bun, '.csv'))
      
      plots <- import("crosswalk.plots")
      
      
      print("funnel plot")
      trim <- "Trim: 10%"
      source_registries <- paste(source_registries, collapse=", ")
      source_registries <- paste0('Source registries: ', source_registries)
      title <- paste0("Target registry ", registry, " : ", bun, ", ", bun_metadata$bundle_name, ', ', trim, ', ', source_registries)
      
      fit1 <- py_load_object(filename = paste0("FILEPATH", registry, "/fit1", bun, ".pkl"), pickle = "dill")
      df1 <- py_load_object(filename = paste0("FILEPATH", registry, "/df1", bun, ".pkl"), pickle = "dill")
      
      plots$funnel_plot(
        cwmodel = fit1,
        cwdata = df1,
        continuous_variables = list(),
        obs_method = 'alternate',
        plot_note = title,
        plots_dir = paste0("FILEPATH", registry, '/'),
        file_name = paste0("funnel_plot_", bun),
        write_file = TRUE
      )
      
    }
    
  }
}


##' *******************************************************
##' 10. COMBINE DETAIL CSV FILES PER TARGET REGISTRY
##' *******************************************************   

combine_cw_detail_csv <- function(target_registries){
  #' @param character. The target registries we adjusted.
  #' @output One csv per target registry of each of its bundles' numerator and denominator case names. 
  
  map <- fread("FILEPATH")
  
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  #target_registries <- unique(reg_cw_map[target_registry != c('newzealand')]$target_registry)
  
  for(registry in target_registries){
    print(registry)
    
    map_subset <- reg_cw_map[target_registry == registry]
    map_subset <- map_subset[!(source_registry == 'newzealand')]
    
    if (registry == 'singapore'){
      bundles <- unique(map_subset[bundle_id_output %ni% c(437, 438)]$bundle_id_output)
    }else{
      bundles <- unique(map_subset[id_input != 618]$id_input)
    }
    
    all_detail <- data.table()
    for(bun in bundles){
      filepath <- paste0(paste0("FILEPATH", registry, '/detail_', bun, '.csv'))
      detail <- fread(filepath)
      all_detail <- rbind(all_detail, detail)
    }
    write.csv(all_detail, paste0("FILEPATH", registry, '/all_bundles_detail.csv'))
  }
}




##' **************************************************************************************************************************************************
##' 11. AGGREGATE ALL RAW REGISTRIES 
##' To do: update the parent child bundle map
##' **************************************************************************************************************************************************

aggregate_raw <- function(congenital_registries){
  #' @output Each congenital registry, collapsed by case name into bundles, and summed into parent (level 2) bundles, that are going to be adjusted. 
  #' @note  New Zealand is done separately (since it didn't get crosswalked) and Singapore is done separately (because raw rows are not yet assigned to a bundle_id)
  
  uniques <- c("nid", "underlying_nid","location_id", "sex", "year_start", "year_end", "age_start", "age_end", "short_registry_name", "note_SR")
  
  #for (registry in congenital_registries){
  
  print(registry)
  registry_filepath <- paste0("FILEPATH", registry, ".xlsx")
  registry_data <- as.data.table(read.xlsx(registry_filepath))
  print(nrow(registry_data))
  
  validate <- copy(registry_data)
  validate[, occurrence := .N, by = c(uniques, "bundle_id")]
  validate[, SS_occurrence := .N, by = c(uniques, "bundle_id", "sample_size")]
  validate[, check := SS_occurrence - occurrence]
  if (any(validate$check > 0)){
    print(paste("Registry",registry,"has more than 1 SS for location, bundle", unique(validate[check >0, .(location_id, bundle_id)]), " and should not be aggregated"))
    stop(paste("Registry",registry,"has unique sample sizes within a single case name/age/sex/loc/year/nid/etc. and should not be aggregated"))
  }
  
  validate <- validate[, c(uniques, "occurrence", "bundle_id"), with = F]
  unique_rows <- validate[occurrence == 1]
  multiple <- validate[occurrence > 1]
  
  
  agg <- unique(multiple)
  no_agg <- unique(unique_rows)
  
  ### aggregate
  agg_rows <- copy(registry_data)
  agg_rows[, occurrence := .N , by = c(uniques, "bundle_id")]
  agg_rows[, SS_occurrence := .N, by = c(uniques, "bundle_id", "sample_size")]
  agg_rows[, check := SS_occurrence - occurrence]
  agg_rows_no_agg <- agg_rows[occurrence == 1]
  agg_rows_agg <- agg_rows[occurrence > 1]
  
  agg_rows_agg[, sample_size := mean(sample_size), by = c(uniques, "bundle_id")]
  agg_rows_agg[, cases := sum(cases), by =c(uniques, "bundle_id")]
  agg_rows_agg[, mean := sum(mean), by = c(uniques, "bundle_id")] ######### we can only do this where sample size is equivalent across unique rows 
  agg_rows_agg[, case_name := lapply(.SD, paste0, collapse=" + "), by = c(uniques, "bundle_id"), .SDcols = "case_name"]
  
  agg_rows_unique <- unique(agg_rows_agg, by = c(uniques, "sample_size", "cases", "case_name", "bundle_id"))
  
  full_registry_data <- rbind(agg_rows_no_agg, agg_rows_unique)
  
  ### check counts ###
  print(paste0('validate only 1 counts   ~ ', nrow(no_agg)))
  print(paste0('final only 1 counts  ~ ', nrow(agg_rows_no_agg)))
  
  print(paste0('agg counts level 1 ~ ', nrow(agg)))
  print(paste0('final agg counts level 1 ', nrow(agg_rows_unique)))
  
  level_2_needed_registries <- c("china", "china_mortality","congmalfworldwide", "icbdms", "nbdpn", "worldatlas") ## this removes eurocat
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  
  bundle_map <- as.data.table(read.xlsx("FILEPATH")) ### this needs to be updated based on bundle mapping child to parent
  
  if (registry %in%  level_2_needed_registries){
    
    print(paste0(registry, " ~ adding level 1 bundles to level 2" ))
    
    level_2_add <- data.table()
    for (bun in  unique(bundle_map$parent_bundle)){
      
      child_buns <- bundle_map[bun == parent_bundle, child_bundle]
      add_level_2 <- copy(full_registry_data)
      add_level_2 <- add_level_2[bundle_id %in% child_buns]
      
      add_level_2[, sample_size := mean(sample_size), by = c(uniques)]
      add_level_2[, cases := sum(cases), by =c(uniques)]
      add_level_2[, mean := sum(mean), by = c(uniques)]
      add_level_2[, case_name := lapply(.SD, paste0, collapse=" + "), by = c(uniques), .SDcols = "case_name"]
      add_level_2[, bundle_id := as.character(bundle_id)]
      add_level_2[, bundle_id := lapply(.SD, paste0, collapse=" + "), by = c(uniques), .SDcols = "bundle_id"]
      
      add_level_2_unique <- unique(add_level_2, by = c(uniques, "sample_size", "cases", "case_name", "bundle_id"))
      add_level_2_unique[, bundle_id := bun]
      print(paste0(bun, ' now has ', nrow(add_level_2_unique), 'rows'))
      
      level_2_add <- rbind(level_2_add, add_level_2_unique)
      print(paste0('level_2_add now has: ', nrow(level_2_add), 'rows'))
      
      
    }
    
    full_registry_data <- rbind(level_2_add, full_registry_data)
    
  }  else {
    
    print(paste0("No add level 2 ~ ", registry))
    full_registry_data <- copy(full_registry_data)
    
  }
  
  
  out_dir <- paste0("FILEPATH", registry, "_agg.xlsx")
  print(out_dir)
  
  write.xlsx(full_registry_data, out_dir, sheetName = "extraction")
  
  #}
}


##' *********************************************************************************************************************************************************************
##' 12. AGGREGATE SINGAPORE LEVEL 2 DATA (and split into bundles, not just case names)
##' To do: - Since we are only reprocessing heart for now just making the singapore parent heart bundle, need to figure out other parent bundles later
##' *********************************************************************************************************************************************************************

aggregate_sing_lvl2 <- function(){
  #' @output Singapore, collapsed by case name into bundles, and summed into parent (level 2) bundles, that are going to be adjusted. 
  #' @note  Singapore is done separately from aggregate_reg_lvl2 because raw rows are not yet assigned to a bundle_id
  
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  registry <- 'singapore'
  reg_data_lvl2_dir <- paste0("FILEPATH")
  reg_data <- read.xlsx(reg_data_lvl2_dir) %>% as.data.table()
  case_names <- unique(reg_cw_map[target_registry == 'singapore']$id_input)
  full_sing_data <- data.table()
  for (case in case_names){
    bundle_ids <- unique(reg_cw_map[target_registry == 'singapore' & id_input == case]$bundle_id_output)
    # Map existing broad case name rows to the level 2 parent bundles
    if (case == 'Heart'){
      reg_data_lvl2 <- reg_data[case_name == case]
      reg_data_lvl2 <- reg_data_lvl2[, bundle_id := 628]
    }
    if (case == 'Syndrome'){
      reg_data_lvl2 <- reg_data[case_name == case]
      reg_data_lvl2 <- reg_data_lvl2[, bundle_id := 3029]
    }
    if (case == 'MSK'){
      reg_data_lvl2 <- reg_data[case_name == case]
      reg_data_lvl2 <- reg_data_lvl2[, bundle_id := 602]
    }
    if (case == 'Neurological'){
      reg_data_lvl2 <- reg_data[case_name == case]
      reg_data_lvl2 <- reg_data_lvl2[, bundle_id := 608]
    }
    if (case == 'Digestive'){
      reg_data_lvl2 <- reg_data[case_name == case]
      reg_data_lvl2 <- reg_data_lvl2[, bundle_id := 620]
    }
    bundles_df <- data.table()
    for (bun in bundle_ids){
      print(bun)
      new_case_names <- unique(reg_cw_map[target_registry == 'singapore' & id_input == case & bundle_id_output == bun & !(is.na(source_case_name_reference))]$source_case_name_reference)

      duplicate <- length(new_case_names)

      reg_data_subset <- reg_data[case_name == case]
      reg_data_subset_1 <- reg_data_subset[rep(1:nrow(reg_data_subset), duplicate)]
      reg_data_subset_1 <- replace(reg_data_subset_1, 'case_name', new_case_names)
      reg_data_subset_1 <- reg_data_subset_1[, bundle_id := bun]
      bundles_df <- rbind(bundles_df, reg_data_subset_1)
    }
    full_sing_data <- rbind(full_sing_data, bundles_df)
    full_sing_data <- rbind(full_sing_data, reg_data_lvl2)
  }
  write.xlsx(full_sing_data, paste0("FILEPATH"))
}  

##' ******************************
##' 13. AGGREGATE NZL LEVEL 2 DATA
##' ******************************

aggregate_nzl_lvl2 <- function(){
  #' @output Newzealand, collapsed by case name into bundles, and summed into parent (level 2) bundles, that are going to be adjusted. 
  #' @note  New Zealand is done separately because it didn't get crosswalked
  
  ###### All level 1 bundles are unique by row, so it just needs to be summed to the parent level 2 bundle in this section
  bundle_map <- as.data.table(read.xlsx("FILEPATH")) ### this needs to be updated based on bundle mapping child to parent

  uniques <- c("nid", "underlying_nid","location_id", "sex", "year_start", "year_end", "age_start", "age_end")
  
  nzl_filepath <- paste0("FILEPATH")
  nzl_data <- as.data.table(read.xlsx(nzl_filepath))
  
  registry <- 'newzealand'
  print(paste0(registry, " ~ adding level 1 bundles to level 2" ))
  
  level_2_add <- data.table()
  for (bun in  unique(bundle_map$parent_bundle)){
    
    print(bun)
    child_buns <- bundle_map[bun == parent_bundle, child_bundle]
    add_level_2 <- copy(nzl_data)
    add_level_2 <- add_level_2[bundle_id %in% child_buns]
    
    add_level_2[, sample_size := mean(sample_size), by = c(uniques)]
    add_level_2[, cases := sum(cases), by =c(uniques)]
    add_level_2[, mean := sum(mean), by = c(uniques)]
    add_level_2[, case_name := lapply(.SD, paste0, collapse=" + "), by = c(uniques), .SDcols = "case_name"]
    add_level_2[, bundle_id := as.character(bundle_id)]
    add_level_2[, bundle_id := lapply(.SD, paste0, collapse=" + "), by = c(uniques), .SDcols = "bundle_id"]
    
    add_level_2_unique <- unique(add_level_2, by = c(uniques, "sample_size", "cases", "case_name", "bundle_id"))
    add_level_2_unique[, bundle_id := bun]
    
    level_2_add <- rbind(level_2_add, add_level_2_unique)
    
    
  }
  
  nzl_registry_data <- rbind(level_2_add, nzl_data)
  nzl_registry_data[, registry_id := 'newzealand']
  nzl_registry_data[, unit_type := 'Person']
  nzl_registry_data[, representative_name := 'Unknown']
  nzl_out_dir <- paste0("FILEPATH")
  write.xlsx(nzl_registry_data, nzl_out_dir, sheetName = "extraction")
  
}


##' ***********************************************************************************************************************************************************
##' 14. Apply betas to raw, aggregated target registries - this loop deals with eurocat and china (source registries) as well but just doesn't apply betas to them
##' ***********************************************************************************************************************************************************

apply_betas <- function(congenital_registries, registry_bundles){
  #' @param congenital_registries character. The congenital registries. Any registry not requiring application of betas will just run through.
  #' @param registry_bundles numeric. The bundles you are creating new bundle data for, from each registry. Bundles that don't need adjusting will just slide through.
  #' @output One excel file per registry with the adjusted prevalence mean and standard errors. 
       
  ## STD ERROR
  get_se <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[(is.na(standard_error) | standard_error == 0) & is.numeric(lower) & is.numeric(upper), standard_error := (upper-lower)/3.92]
    z <- qnorm(0.975)
    dt[(is.na(standard_error) | standard_error == 0) & (measure == "prevalence" | measure == "proportion"),
       standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    return(dt)
  }
  
  ## UPPER/LOWER
  get_upper_lower <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[!is.na(mean) & !is.na(standard_error) & is.na(lower), lower := mean - (1.96 * standard_error)]
    dt[!is.na(mean) & !is.na(standard_error) & is.na(upper), upper := mean + (1.96 * standard_error)]
    return(dt)
  }
  
  library(reticulate)
  library(crosswalk, lib.loc = "FILEPATH")
  reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  reg_cw_map <- reg_cw_map[source_registry != 'newzealand']
  
  #for (registry in congenital_registries){
  print(registry)
  reg_data_lvl2_dir <- paste0("FILEPATH")
  
  reg_data <- read.xlsx(reg_data_lvl2_dir) %>% as.data.table()
  reg_data <- reg_data[, group_id := paste0(nid, '_', underlying_nid)]
  # setting these to NA so the formula works to recalculate and fill them in
  reg_data[, standard_error := NA] 
  reg_data[, lower := NA]
  reg_data[, upper := NA]
  
  
  reg_data <- get_se(reg_data)
  reg_data$standard_error <-  as.numeric(reg_data$standard_error)
  reg_data <- get_upper_lower(reg_data)
  reg_data$lower <-  as.numeric(reg_data$lower)
  reg_data$upper <-  as.numeric(reg_data$upper)
  reg_data$registry_id <- registry
  reg_data$obs_method <- 'alternate'
  
  
  if (registry_bundles == 'default'){
    bundles <- unique(reg_data$bundle_id)
  }else{
    bundles <- unique(reg_data[bundle_id %in% registry_bundles]$bundle_id)
  }
  
  if (registry == 'singapore'){
    bundles_to_adjust <- unique(reg_cw_map[target_registry == registry & bundle_id_output %in% bundles & bundle_id_output %ni% c(437,438,618)]$bundle_id_output)
  }else{
    bundles_to_adjust <- unique(reg_cw_map[target_registry == registry & bundle_id_output %in% bundles]$bundle_id_output)
  }
  
  bundles_to_not_adjust <- setdiff(bundles, bundles_to_adjust)
  
  
  ################# apply betas to data that needs adjusting #############
  combined_adj_reg <- data.table()
  for(bun in bundles_to_adjust){
    print('adjust')
    print(bun)
    
    registry_data_to_adj <- reg_data[bundle_id == bun]
    offset <- 0.5 * median(registry_data_to_adj[mean != 0, mean]) ### Adding the offset to the raw means so that the adjust_orig_vals function will work
    registry_data_to_adj[, `:=` (mean = mean + offset)]
    registry_data_to_adj[, group_id := as.character(group_id)]
    
    
    fit1 <- py_load_object(filename = paste0("FILEPATH"), pickle = "dill")
    
    registry_data_to_adj[, c("mean_adjusted", "standard_error_adjusted", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
      fit_object = fit1,                 # result of CWModel()
      df = registry_data_to_adj,         # original data with obs to be adjusted
      orig_dorms = "obs_method",         # name of column with (all) def/method levels
      orig_vals_mean = "mean",           # original mean
      orig_vals_se = "standard_error",   # standard error of original mean
    )
    # read in beta from csv
    mr_brt_output <- fread(paste0("FILEPATH"))
    beta <- mr_brt_output[dorms == 'alternate']$beta
    
    registry_data_to_adj[cases != 0, mean_adjusted := mean_adjusted - offset/(exp(beta))] ### Subtracting offset to the adjusted means (only to nonzeros)
    registry_data_to_adj[cases == 0, mean_adjusted := 0] ### Setting adjusted mean to anywhere raw mean was zero (cases = 0)
    
    registry_data_to_adj[, mean := mean - offset] ### Subtracting offset to the raw means 

    combined_adj_reg <- rbind(combined_adj_reg, registry_data_to_adj, fill = TRUE)
  }
  
  
  ################## carry over data that doesn't need adjusting ##########
  combined_non_adj_reg <- data.table()
  for (bun in bundles_to_not_adjust){
    print('not adjusting')
    print(bun)
    registry_data_to_not_adj <- reg_data[bundle_id == bun]
    registry_data_to_not_adj <- registry_data_to_not_adj[, mean_adjusted := mean]    
    registry_data_to_not_adj <- registry_data_to_not_adj[, standard_error_adjusted := standard_error] 
    combined_non_adj_reg <- rbind(combined_non_adj_reg, registry_data_to_not_adj, fill = TRUE)
  }
  
  
  registry_data_new <- rbind(combined_adj_reg, combined_non_adj_reg, fill = TRUE)
  registry_data_new <- registry_data_new[, cases := mean*sample_size]  ## adjust cases based on new mean
  
  print(paste0('combined target ', registry, ' has ', nrow(registry_data_new), ' rows'))
  
  file.rename(paste0("FILEPATH"),
              paste0("FILEPATH"))
  
  
  write.xlsx(registry_data_new, paste0("FILEPATH"), sheetName = 'extraction')
  
}


##' *****************************************************************
##' 15. ASSEMBLE ADJUSTED REGISTRIES INTO BUNDLES
##' *****************************************************************

assemble_bundles <- function(congenital_registries, bundles){
  #' @output one excel file per bundle, ready to be cleaned and uploaded
  congenital_registries <- c("eurocat", "china", "congmalfworldwide", "icbdms", "nbdpn", "worldatlas", "singapore")
  
  ########
  ## STD ERROR
  get_se <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[(is.na(standard_error) | standard_error == 0) & is.numeric(lower) & is.numeric(upper), standard_error := (upper-lower)/3.92]
    z <- qnorm(0.975)
    dt[(is.na(standard_error) | standard_error == 0) & (measure == "prevalence" | measure == "proportion"),
       standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    return(dt)
  }
  
  ##############
  
  china_mortality <- read.xlsx("FILEPATH") %>% as.data.table()
  newzealand <- read.xlsx("FILEPATH") %>% as.data.table()
  
  #for (bun in bundles){
  print(paste0('assembling bundle ', bun))
  # read in china mortality data and append to the prevalence data
  china_mortality_bun <- china_mortality[bundle_id == bun]
  china_mortality_bun <- china_mortality_bun[, mean_adjusted := mean] ## doing this so we can do the same renaming of mean adjusted back to mean for all registry data below
  china_mortality_bun <- china_mortality_bun[, standard_error_adjusted := standard_error]
  china_mortality_bun[, registry_id := 'china_mortality']
  print(nrow(china_mortality_bun))
  # read in new zealand data and append to the prevalence data
  newzealand_bun <- newzealand[bundle_id == bun]
  newzealand_bun <- newzealand_bun[, mean_adjusted := mean]
  newzealand_bun[, standard_error := NA]
  newzealand_bun[, lower := NA]
  newzealand_bun[, upper := NA]
  newzealand_bun[, measure := 'prevalence']
  newzealand_bun[, unit_type := 'Person']
  newzealand_bun[, representative_name := 'Unknown']
  newzealand_bun[, registry_id := 'newzealand']
  
  
  newzealand_bun <- get_se(newzealand_bun)
  newzealand_bun$standard_error <-  as.numeric(newzealand_bun$standard_error)
  newzealand_bun <- newzealand_bun[, standard_error_adjusted := standard_error]
  print(nrow(newzealand_bun))
  
  all_reg_bun <- data.table()
  all_reg_bun <- rbind(all_reg_bun, china_mortality_bun)
  all_reg_bun <- rbind(all_reg_bun, newzealand_bun, fill = TRUE)
  
  for (registry in congenital_registries){
    reg <- read.xlsx(paste0("FILEPATH")) %>% as.data.table
    reg_bun <- reg[bundle_id == bun]
    print(paste0(registry, "_", bun, ":", nrow(reg_bun)))
    all_reg_bun <- rbind(all_reg_bun, reg_bun, fill = TRUE)
  }
  ############## VERY IMPORTANT ##################
  ## set mean_adjusted to mean, and standard_error_adjusted to SE
  setnames(all_reg_bun, 'mean', 'pre_cw_mean')
  setnames(all_reg_bun, 'standard_error', 'pre_cw_standard_error')
  ### set lower, upper, to NA, uploader will calculate it for us based on new adjusted SE and means
  all_reg_bun$lower <- NA
  all_reg_bun$upper <- NA
  setnames(all_reg_bun, 'mean_adjusted', 'mean')
  setnames(all_reg_bun, 'standard_error_adjusted', 'standard_error')
  #################################################
  
  print(nrow(all_reg_bun))
  
  file.rename(paste0("FILEPATH"),
              paste0("FILEPATH"))
  
  write.xlsx(all_reg_bun, paste0("FILEPATH"), sheetName = 'extraction')
  #}
}

##' ***********************************************************************************************************************************************************
##' 16. Make diagnostic plots comparing pre and post crosswalked data *** This creates a PDF, you can run the RMarkdown diagnostics for the plotlys
##' ***********************************************************************************************************************************************************

pdf_diagnostics <- function(bundles){
  bun <- pdf_bun
  map <- fread("FILEPATH")
  
  #for (bun in bundles){
  
  pdf(file = paste0("FILEPATH"), width = 20, height = 8)
  
  print(bun)
  bun_metadata <- map[bundle_id == bun]
  
  ### Scatter ###
  new_bun <- read.xlsx(paste0("FILEPATH")) %>% as.data.table()
  new_bun <- new_bun[registry_id != 'china_mortality']
  new_bun$registry_id <- factor(new_bun$registry_id, levels = unique(new_bun$registry_id))
  #new_bun <- new_bun[registry_id %in% c('eurocat', 'china'), mean_adjusted := mean]
  
  scatter_dt <- copy(new_bun)
  
  if (max(scatter_dt$pre_cw_mean) > max(scatter_dt$mean)){
    max <- max(scatter_dt$pre_cw_mean)
  }else{
    max <- max(scatter_dt$mean)
  }
  scatter <-  ggplot() +
    geom_point(data = scatter_dt, aes(x = pre_cw_mean, y = mean, col=registry_id, fill=registry_id,
                                      text = paste('Mean_adj: ', prettyNum(signif(scatter_dt$mean, digits = 3)),
                                                   'Mean_unadj: ', prettyNum(signif(scatter_dt$pre_cw_mean, digits = 3)),
                                                   'Location: ', scatter_dt$location_name,
                                                   'Year: ',  scatter_dt$year_start,
                                                   'Sex: ',  scatter_dt$sex,                                                                                                 
                                                   'Registry: ', scatter_dt$registry_id)), shape = 21) + 
    theme_bw() + xlim(0, max) + ylim(0,  max) + 
    labs(title = paste0('Raw vs. CW,', bun, ', ', bun_metadata$bundle_name), x = "Unadjusted mean", y = "Crosswalked mean") + scale_size_area(max_size = 10)
  
 
  ### Violin ###
  crosswalked <- new_bun[, .(mean, standard_error,  registry_id, location_id, year_start, year_end, sex, bundle_id)]
  crosswalked$version <- 'crosswalked'
  
  
  pre_crosswalk <- new_bun[, .(pre_cw_mean, pre_cw_standard_error, registry_id, location_id, year_start, year_end, sex, bundle_id)]
  pre_crosswalk$version <- 'not crosswalked'
  setnames(pre_crosswalk, 'pre_cw_mean', 'mean')
  setnames(pre_crosswalk, 'pre_cw_standard_error', 'standard_error')
  
  dt <- rbind(pre_crosswalk, crosswalked)
  dt$registry_id <- factor(dt$registry_id, levels = unique(dt$registry_id))
  
  dodge <- position_dodge(width = 1)
  
  violin <- ggplot(data = dt, aes(x = as.factor(registry_id), y = mean, color = as.factor(version), fill = as.factor(version), alpha = .5)) +
    geom_violin(position = dodge) + geom_boxplot(width=.1, position = dodge) +
    scale_x_discrete(breaks=unique(dt$registry_id), labels=unique(as.character(dt$registry_id))) +
    labs(title = paste0('Raw vs. CW,', bun, ', ', bun_metadata$bundle_name),
         x = 'Registry',
         y = 'mean') +
    theme(legend.position = 'bottom') +
    theme(axis.text.x = element_text(angle = 45)) +
    scale_y_log10() 
  

  print(scatter)
  print(violin)
  dev.off()
  
  
}


##' ***********************************
##' 17. Wipe bundle data for reupload
##' ***********************************

wipe_bundle_data <- function(bundles, archive, decomp_step = decomp_step, gbd_round_id = gbd_round_id){
  
  #for (bun in bundles){
  print(bun)
  #write out current bundle
  current_bun <- get_bundle_data(bundle_id = bun, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
  print(nrow(current_bun))
  if (archive == TRUE){
    time <- gsub(' ', '_', Sys.time())
    write.csv(current_bun, paste0('FILEPATH', bun, '_wiped', time, '.csv'))
  }
  
  # get seqs and write out empty file with seqs to delete
  df <- data.table()
  df$seq <- current_bun$seq
  write.xlsx(df, file = paste0("FILEPATH"),sheetName = "extraction", row.names = FALSE)
  
  upload_bundle_data(bundle_id = bun, decomp_step = decomp_step, filepath = paste0("FILEPATH"),
                     gbd_round_id = gbd_round_id)
}

##' ********************
##' SETTING ARGUMENTS
##' ********************

if (sys.nframe() == 0L){
  #Set arguments
  args <- commandArgs(trailingOnly = TRUE)
  functions_to_run <- args[1]
  registry <- args[2]
  pdf_bun <- args[2]
  registry_bundles <- args[3]
  bun <- args[3]
  archive <- args[3]
  decomp_step <- args[4]
  gbd_round_id <- args[5]
  
  
  if (functions_to_run == 'prep_for_cw'){
    
    reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
    reg_cw_map <- reg_cw_map[!(target_registry == 'singapore')]     ## subset out singapore because we process it separately
    
    prep_dummy_reference(source_registries = source_registries)
  }
  
  if (functions_to_run == 'agg_dummy'){
    reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
    reg_cw_map <- reg_cw_map[!(target_registry == 'singapore')]     ## subset out singapore because we process it separately
    
    aggregate_dummy_reference(source_registries = source_registries)
  }
  
  
  if (functions_to_run == 'prep_singapore_for_cw'){
    prep_sing_reference()
  }
  
  
  if (functions_to_run == 'aggregate_singapore'){
    aggregate_sing_reference()
  }
  
  
  if (functions_to_run == 'crosswalk'){
    crosswalk_registries(target_registries = target_registries)
  }
  
  
  if (functions_to_run == 'aggregate_raw'){
    aggregate_raw(congenital_registries = congenital_registries)
  }
  
  
  if (functions_to_run == 'aggregate_sing_lvl2'){
    aggregate_sing_lvl2()
  }
  
  
  if (functions_to_run == 'apply_betas'){
    apply_betas(congenital_registries = congenital_registries, registry_bundles = registry_bundles)
  }
  
  
  if (functions_to_run == 'assemble_bundles'){
    assemble_bundles(congenital_registries = congenital_registries, bundles = bundles)
  }
  
  
  if (functions_to_run == 'pdf_diagnostics'){
    pdf_diagnostics(bundles = bundles)
  }
  
  if (functions_to_run == 'wipe_bundle_data'){
    wipe_bundle_data(bundles = bundles, archive = archive, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
  }
  
}





