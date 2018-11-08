
rm(list = ls())

library(data.table)
library(dplyr)
library(foreign)
library(stringr)
# library(R.utils)
library(haven)
library(RMySQL)


#########################
# define functions
#########################
#shared func
source(paste0(filepath))

# helper function to read a folder of dta files
ms_reader <- function(filepath) {
  files_to_read <- list.files(filepath)
  files_to_read <- files_to_read[files_to_read != "_archive"]
  dat <- lapply(paste0(filepath, files_to_read), read_dta)
  dat <- rbindlist(dat)
  return(dat)
}

# helper function to read in a folder of csv files
csv_reader <- function(filepath) {
  print(filepath)
  files_to_read <- list.files(filepath)
  files_to_read <- files_to_read[files_to_read != "_archive" & files_to_read != 'for_extraction']
  print(files_to_read)
  dat <- lapply(paste0(filepath, files_to_read), fread)
  dat <- rbindlist(dat, fill = TRUE)
  print('made dat')
  return(dat)
}

# marketscan data has single year ages so create gbd age bins
age_binner <- function(dat){
  # age bin
  agebreaks <- c(0,1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,125)
  agelabels = c("0","1","5","10","15","20","25","30","35","40",
                "45","50","55","60","65","70","75","80","85","90", "95")
  setDT(dat)[, age_start := cut(age, breaks = agebreaks, right = FALSE, labels = agelabels)]
  # convert agegroups to numeric
  dat$age_start <- as.numeric(as.character(dat$age_start))
  dat$age_end <- dat$age_start + 4
  dat[age_start == 0, 'age_end'] <- 1
  return(dat)
}


applyRestrictions <- function(df, application_point) {
  # refactor a function in hosp_prep.py which applies restrictions from
  restrict <- fread(paste0(filepath))
  # assert there are no values between 0 and 1
  stopifnot(!0.1 %in% unique(restrict$yld_age_start))
  
  # make age end column
  df$age_end = df$age_start + 4
  df[age_start == 0]$age_end = 1
  df[age_start == 1]$age_end = 4
  
  # merge on restrictions
  pre <- nrow(df)
  df <- merge(df, restrict, all.x = TRUE, by = 'bundle_id')
  stopifnot(pre == nrow(df))
  
  if (application_point == "start") {
    case_cols <- names(df)[grep("_cases$", names(df))]
  } else if (application_point == "end") {
    
    case_cols <- names(df)[grep("value$", names(df))]
    
  } else {break}
  
  # age and sex restrictions
  if (application_point == "start") {
    df[ which(df$male == 0 & df$sex_id == 1), case_cols] <- NA
    df[ which(df$female == 0 & df$sex_id == 2), case_cols] <- NA
    stopifnot(sum(df[bundle_id == 74 & sex_id == 1]$inp_otp_any_claims_cases, na.rm = TRUE) == 0)
    stopifnot(sum(df[bundle_id == 198 & sex_id == 2]$inp_otp_any_claims_cases, na.rm = TRUE) == 0)
  } else if (application_point == "end") {
    df[ which(df$male == 0 & df$sex == 1), case_cols] <- NA
    df[ which(df$female == 0 & df$sex == 2), case_cols] <- NA
    stopifnot(sum(df[bundle_id == 74 & sex == 1]$inp_otp_any_claims_cases, na.rm = TRUE) == 0)
    stopifnot(sum(df[bundle_id == 198 & sex == 2]$inp_otp_any_claims_cases, na.rm = TRUE) == 0)
  }
  
  df[ which(df$age_end < df$yld_age_start), case_cols] <- NA
  df[ which(df$age_start > df$yld_age_end), case_cols] <- NA
  
  # check a few places with hardcoded restrictions
  stopifnot(sum(df[bundle_id == 292 & age_start < 20]$otp_any_indv_cases, na.rm = TRUE) == 0)
  stopifnot(sum(df[bundle_id == 92 & age_start > 10]$inp_otp_any_claims_cases, na.rm = TRUE) == 0)
  
  
  # drop the columns we use to set restrictions
  df[, c('male', 'female', 'yld_age_start', 'yld_age_end', 'age_end') := NULL]
  
  return(df)
}

get_parent_injuries <- function(df) {
  pc_injuries <- fread(paste0(filepath))
  setnames(pc_injuries, 'Level1-Bundle ID', 'bundle_id')
  
  pc_df <- data.table()
  for (parent in pc_injuries[parent == 1]$e_code) {
    children <- pc_injuries[child == 1]$`baby sequela`[grepl(parent, pc_injuries[child == 1]$`baby sequela`)]
    temp_pc_df = pc_injuries[`baby sequela` %in% children]
    temp_pc_df$parent_bid <- pc_injuries$bundle_id[pc_injuries$e_code == parent]
    pc_df = rbind(pc_df, temp_pc_df)
  }
  pc_df <- pc_df[, c('bundle_id', 'parent_bid'), with = FALSE]
  # check number of inj rows in data for an assert later
  inj_rows <- nrow(df[bundle_id %in% pc_df$bundle_id])
  # merge data onto pc_df template
  pc_df = merge(pc_df, df, by = 'bundle_id', all.x = TRUE, all.y = FALSE)
  
  all_children <- unique(pc_df$bundle_id)
  all_parents <- unique(pc_df$parent_bid)
  
  pc_df[, bundle_id := NULL]
  setnames(pc_df, 'parent_bid', 'bundle_id')
  stopifnot(inj_rows == nrow(pc_df))
  
  df = rbind(df, pc_df)
  stopifnot(sum(df[bundle_id %in% all_children, otp_any_claims_cases], na.rm = TRUE) == sum(df[bundle_id %in% all_parents, otp_any_claims_cases], na.rm = TRUE))
  return(df)
}

########################
# get helper data
########################

sex_specific_df <- fread(paste0(filepath))
setnames(sex_specific_df, 'modelable_entity_id', 'me_id')

# get baby seq maternal causes

old_map <- fread(paste0(filepath))
old_map <- old_map[, c('cause_code', 'me_id')][!is.na(me_id), ]
clean_maps <- fread(paste0(filepath)) 
clean_maps <- merge(clean_maps, unique(old_map), by = 'cause_code', all.x = T, allow.cartesian = T)

# just maternal ME IDs
mat_clean_maps <- clean_maps[clean_maps$me_id %in% c(1555, 10484, 1535, 1543, 3086, 1550, 3085, 10504, 1544)]

mat_clean_maps <- mat_clean_maps[level == 1]
mat_clean_maps <- unique(mat_clean_maps[, c('me_id', 'bs_id'), with = FALSE])


cause_map <- loadCauses()


##################
# scalar creator #
##################

create_scalars <- function(source) {
  

  if (length(source) > 1) source_is_list = TRUE
  else source_is_list = FALSE

  for (cf_rank in c("bundle")) {

    counter <- 0
    

    final_df = data.table()
    

    if (cf_rank == "bundle") {rank_name <- "bundle_id"
    } else if (cf_rank == "baby") {rank_name <- "bs_id"
    } else {rank_name <- "me_id"}
    
    df = data.frame()  # df to rbind with data we read in
    
    if (source_is_list) {
      source_name = paste(source, collapse="_")
      for (one_source in source) {
        # read in data for a single source
        if (Sys.info()[1] == "Linux") {
          # cluster data
          new_df <- csv_reader(paste0(filepath))
        }
        
        if (Sys.info()[1] == "Windows") {
          # local data
          new_df <- csv_reader(paste0(filepath))
        }
        df = rbind(df, new_df, fill=TRUE)
      } # for one_source in source loop
    } else{
      # read in data for a single source
      source_name = source
      if (Sys.info()[1] == "Linux") {
        # cluster data
        new_df <- csv_reader(paste0(filepath))
      }
      
      if (Sys.info()[1] == "Windows") {
        # local data
        new_df <- csv_reader(paste0(filepath))
      }
      df = rbind(df, new_df, fill=TRUE)
    }

    df = age_binner(df)
    

    collapser <- function(df) {
      check_inp_pri <- sum(df$inp_pri_claims_cases, na.rm = TRUE)
      df = df[, .(inp_pri_claims_cases = sum(inp_pri_claims_cases, na.rm = TRUE),
                  inp_pri_indv_cases = sum(inp_pri_indv_cases, na.rm = TRUE),
                  inp_any_claims_cases = sum(inp_any_claims_cases, na.rm = TRUE),
                  inp_any_indv_cases = sum(inp_any_indv_cases, na.rm = TRUE)),
              by = .(age_start, sex_id, bundle_id)]
      stopifnot(check_inp_pri == sum(df$inp_pri_claims_cases, na.rm = TRUE))
      return(df)
    }
    
    df <- collapser(df)
    
    df <- applyRestrictions(df, application_point = "start")
    
    
    anence_bundles <- c(610, 612, 614)
    chromo_bundles <- c(436, 437, 438, 439, 638)
    poly_synd_bundles <- c(602, 604, 606, 799)
    cong_bundles <- c(622, 624, 626, 803)
    cong2_bundles <- c(616, 618)
    
    neonate_bundles <- c(80, 81, 82, 500)
    
    anence <- df[bundle_id %in% anence_bundles]
    chromo <- df[bundle_id %in% chromo_bundles]
    poly_synd <- df[bundle_id %in% poly_synd_bundles]
    cong <- df[bundle_id %in% cong_bundles]
    cong2 <- df[bundle_id %in% cong2_bundles]
    neonate <- df[bundle_id %in% neonate_bundles]
    
    anence$bundle_id <- 610
    chromo$bundle_id <- 2000
    poly_synd$bundle_id <- 607
    cong$bundle_id <- 4000
    cong2$bundle_id <- 5000
    neonate$bundle_id <- 6000
    
    anence <- collapser(anence)
    chromo <- collapser(chromo)
    poly_synd <- collapser(poly_synd)
    cong <- collapser(cong)
    cong2 <- collapser(cong2)
    neonate <- collapser(neonate)
    
    # get list of bundle ids with the parent neonatal cause
    child_neo_causes <- unique(loadChildCauses(380))  
    # get map from neonatal causes to bundle ID
    cause_bundle_map <- cause_map[cause_id %in% child_neo_causes$cause_id]
    # vector of neonatal bundles in the data
    neonate_bundles <- unique(df$bundle_id)[unique(df$bundle_id) %in% cause_bundle_map$bundle_id]
    

    chromo_buns <- rep(c(436, 437, 438, 638), 1, each = nrow(chromo))

    cong_buns <- rep(cong_bundles, 1, each = nrow(cong))
    cong2_buns <- rep(cong2_bundles, 1, each = nrow(cong2))
    neonate_buns <- rep(neonate_bundles, 1, each = nrow(neonate))
    

    chromo <- do.call("rbind", replicate(length(unique(chromo_buns)), chromo, simplify = FALSE))

    cong <- do.call("rbind", replicate(length(cong_bundles), cong, simplify = FALSE))
    cong2 <- do.call("rbind", replicate(length(cong2_bundles), cong2, simplify = FALSE))
    neonate <- do.call("rbind", replicate(length(neonate_bundles), neonate, simplify = FALSE))
    
    #anence$bundle_id <- 610
    chromo$bundle_id <- chromo_buns
    
    cong$bundle_id <- cong_buns
    cong2$bundle_id <- cong2_buns
    neonate$bundle_id <- neonate_buns
    
    # remove bundle IDs for pooled causes
    df <- df[!bundle_id %in% c(610, 436, 437, 438, 638, 607, 622, 624, 626, 803, 616, 618, neonate_bundles)]
    
    df <- rbind(df, anence, chromo, poly_synd, cong, cong2, neonate, fill = TRUE)


    
    # output the data with hospital cases
    write.csv(df, paste0(filepath), row.names = FALSE)
    
    # create a vector of sex specific causes at cf_rank level
    sex_specific <- unique(sex_specific_df[[rank_name]])
    sex_specific <- sex_specific[!is.na(sex_specific)]
    # manually add the REI PID to sex specific causes
    if (rank_name == 'bundle_id') {sex_specific = c(sex_specific, 294)}
    
    for (cause in unique(df[[rank_name]])) { 
      # vars to catch unsmoothable cf levels
      all_null  = NULL
      type_null = NULL
      sex_null = NULL

      counter <- counter + 1
      
      print(paste0(" ", round((counter/length(unique(df[[rank_name]])))*100, 2), "% Complete"))
      
      dummy_grid <- expand.grid('age_start' = c(0,1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95),
                                'sex' = c(1,2))
      df_sub <- df[bundle_id == cause]
      setnames(df_sub, "sex_id", "sex")
      
      df_sub <- merge(dummy_grid, df_sub, all.x = TRUE)
      df_sub <- data.table(df_sub)

      
      
      cases_df <- copy(df_sub)

      
      ########################################################
      # process raw ratios
      #######################################################
      

      df_sub <- df_sub[, incidence := get("inp_any_indv_cases") / get("inp_pri_claims_cases")]
      

      df_sub <- df_sub[, injury_cf := get("inp_any_indv_cases") / get("inp_any_claims_cases")]
      
 
      df_sub <- df_sub[, indv_cf := get("inp_pri_indv_cases") / get("inp_pri_claims_cases")]
      

      
      # reshape long
      df_sub <- melt(df_sub[, c("sex", "age_start", "incidence", "injury_cf", "indv_cf"), with = FALSE],
                     id.vars = c("sex", "age_start"),
                     measure.vars = c("incidence", "injury_cf", "indv_cf"),
                     variable.name = "type",
                     value.name = "value")
      
      # create the smoothed value column which will be filled later
      df_sub$smoothed_value <- as.numeric(NA)
      
      # loop over correction level and sex id
      # smooth each one of four correction factors - incidence, injury_cf
      for (corr_level in c("incidence", "injury_cf", "indv_cf")) {
        for (sex_id in c(1,2)) {
          # maternal cause restrictions
          if ( cause %in% c(79, 646, 74, 75, 423, 77, 422, 667, 76) & rank_name == "bundle_id" |
               cause %in% c(1555, 10484, 1535, 1543, 3086, 1550, 3085, 10504, 1544) & rank_name == "me_id" | 
               cause %in% mat_clean_maps$bs_id & rank_name == "bs_id") {
            if (sex_id == 1) {next}
            
            dat <- df_sub[type == corr_level & sex == sex_id]
            dat$value[is.infinite(dat$value)] <- NA

            if (sum(!is.na(dat$value)) == 1) {
              df_sub[type == corr_level & sex == sex_id, smoothed_value := unique(dat$value)[!is.na(unique(dat$value))]]
            }
            else if (sum(!is.na(dat$value)) <= 2) {
              all_null <- cause
              type_null <- c(type_null, corr_level)
              sex_null <- c(sex_null, sex_id)

              next
            }
            else if (sum(!is.na(dat$value)) <= 5) {
              smoothobj <- loess(log(value) ~ age_start, data = dat, span = 1)
              
              df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
            }
            else if (sum(!is.na(dat$value)) <= 10) {
              smoothobj <- loess(log(value) ~ age_start, data = dat, span = .75)
              
              df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
            } else {
              smoothobj <- loess(log(value) ~ age_start,
                                 data = dat,
                                 span = 0.5)
              
              df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
            }
            if (sex_id == 2) {
              

              max_age = max(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$type==corr_level])            
              upper_val <- df_sub$smoothed_value[df_sub$age_start==max_age & df_sub$sex==sex_id & df_sub$type==corr_level]
              
              df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$age_start>max_age & df_sub$type==corr_level] <- upper_val
              
              # again for min age
              min_age <- min(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$type==corr_level])
              lower_val <- df_sub$smoothed_value[df_sub$age_start==min_age & df_sub$sex==sex_id & df_sub$type==corr_level]
              df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$age_start<min_age & df_sub$type==corr_level] <- lower_val
            }

          } else {
            # create loess model for subset of data
            dat <- df_sub[type == corr_level & sex == sex_id]
            dat$value[is.infinite(dat$value)] <- NA
            # loess will break with too few data points
            if (sum(!is.na(dat$value)) == 1) {
              df_sub[type == corr_level & sex == sex_id, smoothed_value := unique(dat$value)[!is.na(unique(dat$value))]]
            }
            else if (sum(!is.na(dat$value)) <= 2) {
              all_null <- cause
              type_null <- c(type_null, corr_level)
              sex_null <- c(sex_null, sex_id)

              next
            }
            else if (sum(!is.na(dat$value)) <= 5) {
              smoothobj <- loess(log(value) ~ age_start, data = dat, span = 1)

              df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
            }
            else if (sum(!is.na(dat$value)) <= 10) {
              smoothobj <- loess(log(value) ~ age_start, data = dat, span = .75)
              # add loess smoothed values to df_sub
              df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
            } else {
              smoothobj <- loess(log(value) ~ age_start,
                                 data = dat,
                                 span = 0.5)
              # add loess smoothed values to df_sub
              df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
            }
            # fix the tails of the predictions

            max_age = max(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex == sex_id & df_sub$type == corr_level])            
            upper_val <- df_sub$smoothed_value[df_sub$age_start == max_age & df_sub$sex == sex_id & df_sub$type == corr_level]

            df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex == sex_id & df_sub$age_start > max_age & df_sub$type == corr_level] <- upper_val
            
            # again for min age
            min_age <- min(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$type==corr_level])
            lower_val <- df_sub$smoothed_value[df_sub$age_start==min_age & df_sub$sex==sex_id & df_sub$type==corr_level]
            df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$age_start<min_age & df_sub$type==corr_level] <- lower_val
            
          } # else statement (after maternal cause smoothing)
        } # sex id
      } # corr level
      
      if (cause %in% all_null) {
       
        for (asex in sex_null) {
          if ("incidence" %in% type_null) {
            df_sub[sex == asex & type == "incidence"]$smoothed_value <- sum(cases_df[sex == asex]$inp_any_indv_cases[!is.na(cases_df[sex==asex]$inp_pri_claims_cases) & cases_df[sex==asex]$inp_pri_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_pri_claims_cases, na.rm=TRUE)
          
          }
          if ("injury_cf" %in% type_null) {
            df_sub[sex == asex & type == "injury_cf"]$smoothed_value <- sum(cases_df[sex == asex]$inp_any_indv_cases[!is.na(cases_df[sex==asex]$inp_any_claims_cases) & cases_df[sex==asex]$inp_any_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_any_claims_cases, na.rm=TRUE)
          
          }
          if ("indv_cf" %in% type_null) {
            df_sub[sex == asex & type == "indv_cf"]$smoothed_value <- sum(cases_df[sex == asex]$inp_pri_indv_cases[!is.na(cases_df[sex==asex]$inp_pri_claims_cases) & cases_df[sex==asex]$inp_pri_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_pri_claims_cases, na.rm=TRUE)
          
          }
          
        } # for asex in sex_null
      } # if cause in all_null
      
      
      df_sub[type == "injury_cf" & smoothed_value > 1]$smoothed_value <- 1
      df_sub[type == "indv_cf" & smoothed_value > 1]$smoothed_value <- 1
      
      
      df_sub <- df_sub[is.infinite(smoothed_value), smoothed_value := NA]

      
      
      df_sub[[rank_name]] = cause
      
      final_df = rbind(final_df, df_sub)
      
    } # end cause loop
    
    # re apply restrictions
    final_df = applyRestrictions(final_df, application_point = "end")
    
    # # Write data
    write.csv(final_df, paste0(filepath)
    
    write.csv(final_df, paste0(filepath), row.names = FALSE)
    
    if (cf_rank == "bundle") {

      wide_df <- data.table::dcast(final_df, sex+age_start+bundle_id~type, value.var = 'smoothed_value')
      wide_df <- wide_df[, c("sex", "age_start", "bundle_id", "indv_cf", "incidence", "injury_cf"), with = FALSE]
      write.csv(wide_df, paste0(filepath), row.names = FALSE)
    }
    print("We're Over")
  }
} 

source_list = c("PHL" , "NZL_NMDS", "USA_HCUP_SID")

#########################
# run everything
#########################

for (source in source_list) {
  create_scalars(source)
}
