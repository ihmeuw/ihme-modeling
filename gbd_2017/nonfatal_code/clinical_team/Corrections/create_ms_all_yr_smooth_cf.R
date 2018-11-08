####
rm(list = ls())

library(data.table)
library(dplyr)
library(foreign)
library(stringr)
# library(R.utils)
library(haven)
library(RMySQL)
library(parallel)


#########################
# define functions
#########################
#shared func
source(paste0("filepath"))

# helper function to read a folder of dta files
ms_reader <- function(filepath) {
  dat <- lapply(paste0(filepath, list.files(filepath)), read_dta)
  dat <- rbindlist(dat)
  return(dat)
}

# helper function to read in a folder of csv files
csv_reader <- function(filepath) {
  dat <- lapply(paste0(filepath, list.files(filepath)[1:50]), fread)
  dat <- rbindlist(dat, fill = TRUE)
  return(dat)
}

para_csv_reader <- function(filepath) {
  dat <- mclapply(paste0(filepath, list.files(filepath)[1:50]), fread, mc.cores = 10)
  dat <- rbindlist(dat, fill = TRUE)
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

old_applyRestrictions <- function(df, manually_fix, application_point) {

  restrict <- fread(paste0("filepath"))

  restrict[restrict$yld_age_start < 1, "yld_age_start"] <- 0
  
  level1 <- restrict[, c('Level1-Bundel ID', 'male',
                         'female', 'yld_age_start', 'yld_age_end'), with = FALSE]
  level2 <- restrict[, c('Level2-Bundel ID', 'male',
                         'female', 'yld_age_start', 'yld_age_end'), with = FALSE]
  level3 <- restrict[, c('Level3-Bundel ID', 'male',
                         'female', 'yld_age_start', 'yld_age_end'), with = FALSE]
  setnames(level1,'Level1-Bundel ID', 'bundle_id')
  setnames(level2,'Level2-Bundel ID', 'bundle_id')
  setnames(level3,'Level3-Bundel ID', 'bundle_id')
  # clean the bundle ids
  level1 <- level1[bundle_id != 0]
  level2 <- level2[bundle_id != 0 & bundle_id != "#N/A"]
  level3 <- level3[bundle_id != 0]
  
  level2$bundle_id <- as.numeric(level2$bundle_id)
  
  if (manually_fix) {
    
    level1[ which(level1$bundle_id == 121), "yld_age_start"] <- 0
    level1[which(level1$bundle_id == 131), c("yld_age_start", "yld_age_end")] <- list(0, 95)
    level1[which(level1$bundle_id == 292), c("male", "female", "yld_age_start", "yld_age_end")] <- list(1, 1, 20, 65)
    level1[which(level1$bundle_id == 409), c("yld_age_start", "yld_age_end")] <- list(15, 95)
    level1[which(level1$bundle_id == 618), c("male", "female")] <- list(1, 1)
    level2[which(level2$bundle_id == 283), "yld_age_start"] <- 0
  }
  
  
  level1 <- unique(level1)
  level2 <- unique(level2)
  level3 <- unique(level3)

  level2 <- level2[bundle_id != 616]
  level3 <- level3[bundle_id != 502]
  
  level_list = list(level1, level2, level3)
  for (i in 1:3) {
    # merge on restrictions
    pre <- nrow(df)
    df <- merge(df, level_list[[i]], all.x = TRUE, by = 'bundle_id')
    stopifnot(pre == nrow(df))
    
    if (application_point == "start") {
      case_cols <- names(df)[grep("_cases$", names(df))]
  
    } else if (application_point == "end") {
  
      case_cols <- names(df)[grep("value$", names(df))]
      
    } else {break}
    
    # age and sex restrictions
    df[ which(df$male == 0 & df$sex_id == 1), case_cols] <- NA
    df[ which(df$female == 0 & df$sex_id == 2), case_cols] <- NA
    df[ which(df$age_end < df$yld_age_start), case_cols] <- NA
    df[ which(df$age_start > df$yld_age_end), case_cols] <- NA
    
    # drop the columns we use to set restrictions
    df[, c('male', 'female', 'yld_age_start', 'yld_age_end') := NULL]
  }
  return(df)
  
}

applyRestrictions <- function(df, application_point) {
   
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
    # case_cols <- c("value", "smoothed_value")
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
  pc_injuries <- fread(paste0("filepath"))
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
  # drop children bundle IDs
  pc_df[, bundle_id := NULL]
  setnames(pc_df, 'parent_bid', 'bundle_id')

  
  df = rbind(df, pc_df)
  
  return(df)
}

newcollapser <- function(df) {
  
  case_cols <- names(df)[grep("_cases$", names(df))]
  
  df = df[, lapply(.SD, sum, na.rm = TRUE), .SDcols = case_cols, by = .(age_start, sex_id, bundle_id)]
  
  
  return(df)
}


clean_twn <- function(twn) {
  # drop the null bundle and cast to int
  twn <- twn[bundle_id != "."]
  twn$bundle_id <- as.integer(twn$bundle_id)
  
  # cast cfs to numeric
  twn$correction1 <- as.numeric(twn$correction1)
  twn$correction2 <- as.numeric(twn$correction2)
  # set the ages properly
  twn$age_start <- twn$age_ihmec - 4
  twn[age_ihmec == 4, 'age_start'] = 1
  twn[age_ihmec == 0, 'age_start'] = 0
  twn[age_ihmec == 95, 'age_start'] = 95
  
  
}

########################
# get helper data
########################


sex_specific_df <- fread(paste0(filepath))
setnames(sex_specific_df, 'modelable_entity_id', 'me_id')

# get baby seq maternal causes
clean_maps <- fread(paste0(filepath))  
mat_clean_maps <- clean_maps[clean_maps$me_id %in% c(1555, 10484, 1535, 1543, 3086, 1550, 3085, 10504, 1544)]

mat_clean_maps <- mat_clean_maps[level == 1]
mat_clean_maps <- unique(mat_clean_maps[, c('me_id', 'bs_id'), with = FALSE])

# map bundles on cause restrictions
cause_map <- loadCauses()


# This is the function which calculates correction factors and writes the corr
create_scalars <- function(gbd2017 = FALSE, fix_213 = FALSE) {

  for (cf_rank in c("bundle")) {
    if (!exists("cf_rank")) {cf_rank = "bundle"}
    # counter for progress report
    counter <- 0
    
    # initiate final df to write to J
    final_df = data.table()
    
    # gives us some flexibility if we want to create corr factors at BS level
    if (cf_rank == "bundle") {rank_name <- "bundle_id"
    } else if (cf_rank == "baby") {rank_name <- "bs_id"
    } else {rank_name <- "me_id"}
        
    df <- fread(paste0(filepath))
    
    
    # remove data from year 2000
    df <- df[year_start != 2000]
    
    # duplicate data for parent injuries
    pre_shape <- nrow(df)
    df <- get_parent_injuries(df)
    stopifnot(pre_shape < nrow(df))
    
    # set otp claims to 0 for outpatient cf except for 3 facility types we want to use
    df[facility_id != 11 & facility_id != 22 & facility_id != 95]$otp_any_claims_cases <- 0
    
    # age bin and sum to 5 year groups
    df = age_binner(df)  
   
    df <- newcollapser(df)
    
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
    
    anence <- newcollapser(anence)
    chromo <- newcollapser(chromo)
    poly_synd <- newcollapser(poly_synd)
    cong <- newcollapser(cong)
    cong2 <- newcollapser(cong2)
    neonate <- newcollapser(neonate)
    
    # get list of bundle ids with the parent neonatal cause
    child_neo_causes <- unique(loadChildCauses(380)) 
    # get map from neonatal causes to bundle ID
    cause_bundle_map <- cause_map[cause_id %in% child_neo_causes$cause_id]
    # vector of neonatal bundles in the data
    neonate_bundles <- unique(df$bundle_id)[unique(df$bundle_id) %in% cause_bundle_map$bundle_id]
    
    #anence_buns <- rep(anence_bundles, 1, each=nrow(anence))
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
    #poly_synd$bundle_id <- poly_synd_buns
    cong$bundle_id <- cong_buns
    cong2$bundle_id <- cong2_buns
    neonate$bundle_id <- neonate_buns
    
    # remove bundle IDs for pooled causes
    df <- df[!bundle_id %in% c(610, 436, 437, 438, 638, 607, 622, 624, 626, 803, 616, 618, neonate_bundles)]
    
    df <- rbind(df, anence, chromo, poly_synd, cong, cong2, neonate, fill = TRUE)
    
   
    pre_otp_cases <- sum(df$inp_otp_any_indv_cases, na.rm = T)
    pre_otp_claims <- sum(df$inp_otp_any_claims_cases, na.rm = T)
    
    if (gbd2017) {
      
      df$source <- "marketscan"
      # ok, so create a new column for ms inp claims
      df$ms_inp_pri_claims_cases <- df$inp_pri_claims_cases
      
   
      # rbind counts from hospital
      hosp_df <- read.csv(paste0(filepath))
      hosp_df$source <- "hospital"
      df <- rbind(df, hosp_df, fill = TRUE)
      # collapse again
      df <- newcollapser(df)
      post_otp_cases <- sum(df$inp_otp_any_indv_cases, na.rm = T)
      post_otp_claims <- sum(df$inp_otp_any_claims_cases, na.rm = T)
      # collapser seems to be generating some zeros in outpatient data
      case_cols <- names(df)[grep("_cases$", names(df))]
    
      for (col in case_cols) df[get(col) == 0, (col) := NA]
   
    }
    
   
    sex_specific <- unique(sex_specific_df[[rank_name]])
    sex_specific <- sex_specific[!is.na(sex_specific)]
    # manually add the REI PID to sex specific causes
    if (rank_name == 'bundle_id') {sex_specific = c(sex_specific, 294)}
    
    for (cause in unique(df[[rank_name]])) { 
      # vars to catch unsmoothable cf levels
      all_null  = NULL
      type_null = NULL
      sex_null = NULL
      # # var to show progress complete and # of remaining cause ids
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
      
      # create cf from inpatient primary admis to any dx inpatient individual
      df_sub <- df_sub[, incidence := get("inp_any_indv_cases") / get("inp_pri_claims_cases")]
      
      # create cf from inpatient primary admis to inp + outpatient all individual
      df_sub <- df_sub[, prevalence := get("inp_otp_any_adjusted_otp_only_indv_cases") / get("inp_pri_claims_cases")]
      
      # create cf for injuries, any inpatient individual / any inpatient admissio
      df_sub <- df_sub[, injury_cf := get("inp_any_indv_cases") / get("inp_any_claims_cases")]
      
      df_sub <- df_sub[, indv_cf := get("inp_pri_indv_cases") / get("inp_pri_claims_cases")]
      
      df_sub <- df_sub[, outpatient := get("otp_any_indv_cases") / get("otp_any_claims_cases")]
      
      # reshape long
      df_sub <- melt(df_sub[, c("sex", "age_start", "incidence", "prevalence", "injury_cf", "indv_cf", "outpatient"), with = FALSE],
                     id.vars = c("sex", "age_start"),
                     measure.vars = c("incidence", "prevalence", "injury_cf", "indv_cf", "outpatient"),
                     variable.name = "type",
                     value.name = "value")
      
      # create the smoothed value column which will be filled later
      df_sub$smoothed_value <- as.numeric(NA)
      
      # loop over correction level and sex id
      # smooth each one of four correction factors - incidence, prevalence, injury_cf and individual
      for (corr_level in c("incidence", "prevalence", "injury_cf", "indv_cf", "outpatient")) {
        for (sex_id in c(1,2)) {
          # maternal cause restrictions
          if ( cause %in% c(79, 3419, 646, 74, 75, 423, 77, 422, 667, 76) & rank_name == "bundle_id" |
               cause %in% c(1555, 10484, 1535, 1543, 3086, 1550, 3085, 10504, 1544) & rank_name == "me_id" | 
               cause %in% mat_clean_maps$bs_id & rank_name == "bs_id") {
            if (sex_id == 1) {next}

            # smooth values
            dat = df_sub[type == corr_level & sex == sex_id]
            dat$value[is.infinite(dat$value)] <- NA
            smoothobj <- loess(log(value) ~ age_start,
                               data = dat,
                               span = 0.6)
            
            # add smoothed values to df_sub
            df_sub[type == corr_level & sex == 2, smoothed_value := exp(predict(smoothobj, dat))]
 
            
            if (sex_id == 2) {
              # fix the tails of the predictions
              # if the loess prediction ends at the last insample datapoint assign that value up to age start 95
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
            dat$value[dat$value == 0] <- NA
            # loess will break with too few data points
            if (sum(!is.na(dat$value)) <= 2) {
              all_null <- cause
              type_null <- c(type_null, corr_level)
              sex_null <- c(sex_null, sex_id)

              next
            }
            else if (sum(!is.na(dat$value)) <= 5) {
              smoothobj <- loess(log(value) ~ age_start, data = dat, span = 1)
              # add loess smoothed values to df_sub
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
          
          
            max_age = max(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex == sex_id & df_sub$type == corr_level])            
            upper_val <- df_sub$smoothed_value[df_sub$age_start == max_age & df_sub$sex == sex_id & df_sub$type == corr_level]
          
            df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex == sex_id & df_sub$age_start > max_age & df_sub$type == corr_level] <- upper_val
            
            # again for min age
            min_age <- min(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$type==corr_level])
            lower_val <- df_sub$smoothed_value[df_sub$age_start==min_age & df_sub$sex==sex_id & df_sub$type==corr_level]
            df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$age_start<min_age & df_sub$type==corr_level] <- lower_val
            
          }
        } # sex id
      } # corr level
      
      if (cause %in% all_null) {
        # take a weighted average if not able to smooth and not a sex specific cause
        # this should be exceedingly rare b/c we're smoothing values with only 2 observations
        for (asex in sex_null) {
          if ("incidence" %in% type_null) {
            df_sub[sex == asex & type == "incidence"]$smoothed_value <- sum(cases_df[sex==asex]$inp_any_indv_cases[!is.na(cases_df[sex==asex]$inp_pri_claims_cases) & cases_df[sex==asex]$inp_pri_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_pri_claims_cases, na.rm=TRUE)

          }
          if ("prevalence" %in% type_null) {
            df_sub[sex==asex & type=="prevalence"]$smoothed_value <- sum(cases_df[sex==asex]$inp_otp_any_adjusted_otp_only_indv_cases[!is.na(cases_df[sex==asex]$inp_pri_claims_cases) & cases_df[sex==asex]$inp_pri_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_pri_claims_cases, na.rm=TRUE)

          }
          if ("injury_cf" %in% type_null) {
            df_sub[sex==asex & type=="injury_cf"]$smoothed_value <- sum(cases_df[sex==asex]$inp_any_indv_cases[!is.na(cases_df[sex==asex]$inp_any_claims_cases) & cases_df[sex==asex]$inp_any_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_any_claims_cases, na.rm=TRUE)

          }
          if ("indv_cf" %in% type_null) {
            df_sub[sex == asex & type == "indv_cf"]$smoothed_value <- sum(cases_df[sex==asex]$inp_pri_indv_cases[!is.na(cases_df[sex==asex]$inp_pri_claims_cases) & cases_df[sex==asex]$inp_pri_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_pri_claims_cases, na.rm=TRUE)

          }
          if ("outpatient" %in% type_null) {
            df_sub[sex == asex & type == "outpatient"]$smoothed_value <- 
              sum(cases_df[sex == asex]$otp_any_indv_cases[!is.na(cases_df[sex == asex]$otp_any_claims_cases) &
                                                             cases_df[sex == asex]$otp_any_claims_cases != 0], na.rm = TRUE) /
              sum(cases_df[sex == asex]$otp_any_claims_cases, na.rm = TRUE)
          }
        } # for asex in sex_null
      } # if cause in all_null
      
      # injury correction factor should never go above 1
      df_sub[type == "injury_cf" & smoothed_value > 1]$smoothed_value <- 1
      df_sub[type == "indv_cf" & smoothed_value > 1]$smoothed_value <- 1
      # outpatient correction factor shouldn't smooth above 1
      df_sub[type == "outpatient" & smoothed_value > 1]$smoothed_value <- 1
      
      # some values are infinite due to dividing a number by zero
      df_sub <- df_sub[is.infinite(smoothed_value), smoothed_value := NA]


      df_sub[[rank_name]] = cause
      # rbind back together
      final_df = rbind(final_df, df_sub)
      
    } # end cause loop
    
    # re apply restrictions
    final_df = applyRestrictions(final_df, application_point = "end")
    
    if (gbd2017) { write_fold = filepath
    } else {write_fold = filepath}
    if (fix_213) {write.csv(final_df, paste0(filepath)
    } else {

      write.csv(final_df, paste0(filepath)

      write.csv(final_df, paste0(filepath), row.names = FALSE)
    }
    
    if (cf_rank == "bundle") {
      if (fix_213) {

        wide_df <- data.table::dcast(final_df, sex+age_start+bundle_id~type, value.var = 'smoothed_value')
        wide_df <- wide_df[, c("sex", "age_start", "bundle_id", "indv_cf", "incidence", "prevalence", "injury_cf", "outpatient"), with = FALSE]
        write.csv(wide_df, paste0(filepath), row.names = FALSE)
      } else {
        # reshape wide to make applying CF in python easier
        wide_df <- data.table::dcast(final_df, sex+age_start+bundle_id~type, value.var = 'smoothed_value')
        wide_df <- wide_df[, c("sex", "age_start", "bundle_id", "indv_cf", "incidence", "prevalence", "injury_cf", "outpatient"), with = FALSE]
        write.csv(wide_df, paste0(filepath), row.names = FALSE)
      }
    }
    
  } # end loop for cf_rank
}
create_scalars(gbd2017 = FALSE)
