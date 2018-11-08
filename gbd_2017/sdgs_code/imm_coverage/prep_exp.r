#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Prep vaccination for ST-GPR
#***********************************************************************************************************************


#----SETUP--------------------------------------------------------------------------------------------------------------
### clear workspace
rm(list=ls())

### set os flexibility
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH/", user)
}

### load packages
source("FILEPATH/load_packages.R")
load_packages(c("data.table", "dplyr", "parallel", "readxl", "ggplot2", "boot", "lme4", "pscl", "purrr", "splines", "binom", "magrittr", "rhdf5"))

### load functions
source(paste0(j, 'FILEPATH/init.r'))
source(db_tools)
source("FILEPATH/cluster_tools.r")
"FILEPATH/read_excel.R" %>% source
file.path(j, "FILEPATH/get_location_metadata.R") %>% source
file.path(j, "FILEPATH/get_covariate_estimates.R") %>% source
locs <- get_location_metadata(location_set_id=22)[level >= 3, ]

### make directory
date         <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%Y-%m-%d")
to_model_dir <- paste0(data_root, "FILEPATH", date); if (!dir.exists(to_model_dir)) dir.create(to_model_dir)

### set options
beta_version <- TRUE # use beta or production version of ST-GPR?
#***********************************************************************************************************************


#----PREP---------------------------------------------------------------------------------------------------------------
### load vaccine introduction and admin coverage paths
vacc.intro     <- paste0(data_root, "FILEPATH/vaccine_intro.rds")
vacc.admin     <- paste0(data_root, "FILEPATH/who_admin.rds")
vacc.whosurvey <- paste0(data_root, "FILEPATH/who_survey.rds")
vacc.survey    <- paste0(data_root, "FILEPATH/vaccination/")
vacc.outliers  <- paste0(code_root, "FILEPATH/vaccination.csv")
vacc.lit       <- paste0(data_root, "FILEPATH/vaccine_lit.rds")
vacc.schedule  <- paste0(data_root, "FILEPATH/vaccine_schedule.rds")

### load other references
me.db <- paste0(code_root, "FILEPATH/me_db.csv") %>% fread

### set objects
year.est.start <- year_start %>% as.numeric
year.est.end   <- year_end %>% as.numeric
#***********************************************************************************************************************


########################################################################################################################
# SECTION 1: Prep tabulated survey data
########################################################################################################################


#----SURVEY FUNCTIONS---------------------------------------------------------------------------------------------------
### get most recent tabulated survey dataset
load.survey <- function(folder) {
  ## Find most recent file in folder
  files <- list.files(folder, full.names=TRUE)
  files <- files[!grepl("failed", files)]
  file <- files[order(file.mtime(files), decreasing=TRUE)][1]
  print(paste0("Opening ", file))
  return(fread(file))
}

### prepare USA NIS Kindergarten report
prep.nisreport <- function() {
  ### NIS coverage data
  length <- year.est.end - 2009
  path <- paste0(data_root, "FILEPATH/dataView2011_3_MMRonly.xls")
  nis <- read_excel(path, sheet="SVV Coverage Trend 2016-17 Data", skip=2, 
                    col_types=c("text", rep(c("text", "skip", "text", "text", "text", "text"), length))) %>% data.table
  # remove unnecessary rows
  nis <- nis[!Names %in% c("HP 2020 Target", "Median")]
  # remove unnecessary columns
  nis <- nis[, colnames(nis)[grep("SURVEY TYPE|TARGET|TOTAL KINDERGARTEN POPULATION|PERCENT SURVEYED", colnames(nis))] := NULL]
  # add years to colnames
  yrs <- 2009:(year.est.end - 1)
  names <- c("name", paste0("x_", yrs))
  colnames(nis) <- names
  # reshape long
  nis[, (paste0("coverage_", yrs)) := lapply(paste0("x_", yrs), function(cols) as.numeric(get(cols)))]
  nis <- melt(nis[, c("name", paste0("coverage_", yrs)), with=FALSE], value.name="data")
  # add in year of fall of academic year
  nis[, year_start := NA_integer_]
  nis[, year_end := NA_integer_]
  nis[, year_start := substring(variable, 10, 13) %>% as.integer]
  nis[, year_end := year_start + 1]
  nis[, year_id := floor((year_start + year_end) / 2)]
  nis[, variable := NULL]
  setnames(nis, "name", "location_name")
  nis <- merge(nis[!grep("Median", location_name)], locs[parent_id==102, .(ihme_loc_id, location_name)], by="location_name", all.x=TRUE) %>%
    .[, location_name := NULL]
  # calculate population included
  nis[, data := data / 100]
  # add vars
  nis[, nid := 334866]
  nis[, file_path := path]
  nis[, variance := data * (1 - data) / 50]
  nis[, cv_survey := 0]
  nis[, age_group_id := 22]
  nis[, sex_id := 3]
  nis[, survey_name := "School Vaccination Assessment Program"]
  nis <- nis[!is.na(data), ]
  nis_mmr <- copy(nis)[, me_name := "vacc_mmr2"]
  nis_mcv <- copy(nis)[, me_name := "vacc_mcv2"]
  nis <- rbind(nis_mmr, nis_mcv)
  
  return(nis)
}

### prep tabulated unit record data from UbCov
prep.survey <- function() {
  df <- load.survey(vacc.survey)
  ## Clean
  old <- c('var', 'mean', 'age_start')
  new <- c('me_name', 'data', 'age_year')
  setnames(df, old, new)
  ## Variance
  df <- df[, variance := standard_error ^ 2]
  ## Age groups
  df <- df[, age_group_id := 22]
  ## Years
  df <- df[, year_id := floor((year_start + year_end) / 2)]
  ## Center around birth year
  df <- df[, year_id := year_id - age_year]
  ## Clean me_name
  df <- df[me_name == "bcg1", me_name := "bcg"]
  df <- df[me_name == "yfv1", me_name := "yfv"]
  df <- df[, me_name := paste0("vacc_", me_name)]
  ## Remap some ihme_loc_ids
  df <- df[ihme_loc_id == "ALG", ihme_loc_id := "DZA"]
  df <- df[ihme_loc_id == "PAL", ihme_loc_id := "PSE"]
  ## Drop age_year == 0
  df <- df[age_year != 0]
  df <- df[, cv_survey := 1]
  ## Drop messed up locations
  drop.locs <- df[!(ihme_loc_id %in% locs$ihme_loc_id), ihme_loc_id] %>% unique
  if (length(drop.locs)>0 ) {
    print(paste0("UNMAPPED LOCATIONS (DROPPING): ", toString(drop.locs)))
    df <- df[!(ihme_loc_id %in% drop.locs)]
  }
  ## If mean==0/1 use Wilson Interval Method (https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Wilson_score_interval)
  if (nrow(df[data %in% c(0,1)]) > 0) {
    df.w <- df[data %in% c(0,1)]
    sample_size <- df.w$sample_size
    n <- ifelse(df.w$data==0, 0, sample_size)
    ci <- binom.confint(n, sample_size, conf.level = 0.95, methods = "wilson")
    se <- (ci$upper - ci$lower)/3.92
    variance <- se^2 * 2.25 ## Inflate by design effect according to DHS official/unocfficial rule of thumb (http://userforum.dhsprogram.com/index.php?t=msg&goto=3450&)
    df[data %in% c(0,1)]$variance <- variance
  }
  ## If design effect is unreasonably small, assume 2.25 and readjust
  df <- df[design_effect < 1, variance := variance *  2.25/design_effect]
  df <- df[design_effect < 1, design_effect := 2.25]
  # add on NIS survey data from SchoolVaxView
  nis <- prep.nisreport()
  df <- rbind(df, nis, fill=TRUE)
  
  return(df)
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 2: Outlier data
########################################################################################################################


#----OUTLIER FUNCTIONS--------------------------------------------------------------------------------------------------
### outlier data from outlier database
outlier.data <- function(df, gbd=TRUE) {
  ## Get outlier frame
  df.o <- fread(vacc.outliers) %>% unique
  if (gbd) df.o <- df.o[gbd==1, ]
  df.o <- df.o[, outlier := 1] %>% .[, .(me_name, nid, batch_outlier, ihme_loc_id, year_id)]
  df.o[, year_id := as.integer(year_id)]
  ## Set conditions
  cond <- list(
              list('me_name==""', c("nid")),
              list('batch_outlier==1 & me_name != ""', c("me_name", "nid")),
              list('batch_outlier==0', c("me_name", "nid", "ihme_loc_id", "year_id"))
          )
  ## Loop through and outlier
  for (i in cond) {
    condition <- i[[1]]
    vars <- i[[2]]
    ## Subset
    o.sub <- df.o[eval(parse(text=condition)), (vars), with=FALSE]
    o.sub <- o.sub[, outlier := 1]
    ## Merge
    df <- merge(df, o.sub, by=vars, all.x=TRUE)
    ## Set outlier
    df <- df[outlier==1 & !is.na(data), cv_outlier := data]
    ## Clean
    df <- df[outlier==1 & !is.na(data), data := NA]
    df <- df[, outlier := NULL]
  }
  return(df)
}

### temp outlier certain surveys and survey series that we don't trust
temp.outlier <- function(df) {
  
  ### systematically outlier WHO_WHS
  df <- df[which(grepl("WHO_WHS", survey_name) & !(nid %in% c(21535)) & !is.na(data)), cv_outlier := data]
  df <- df[which(grepl("WHO_WHS", survey_name) & !(nid %in% c(21535)) & !is.na(data)), data := NA]
  
  ### systematically outlier SUSENAS
  df <- df[which(grepl("SUSENAS", survey_name) & !is.na(data)), cv_outlier := data]
  df <- df[which(grepl("SUSENAS", survey_name) & !is.na(data)), data := NA]
  
  ### outlier IND/HMIS FOR DPT3
  df <- df[which(grepl("IND/HMIS", survey_name) & me_name=="vacc_dpt3" & !is.na(data)), cv_outlier := data]
  df <- df[which(grepl("IND/HMIS", survey_name) & me_name=="vacc_dpt3" & !is.na(data)), data := NA]
  
  ### outlier MEX INEGI
  df <- df[which(grepl("MEX/INEGI", survey_name) & me_name=="vacc_dpt3" & !is.na(data)), cv_outlier := data]
  df <- df[which(grepl("MEX/INEGI", survey_name) & me_name=="vacc_dpt3" & !is.na(data)), data := NA]
  
  ### for RUS/LMS keep only 1 y/o
  df <- df[which(grepl("RUS/LONGITUDINAL", survey_name) & age_year != 2 & !is.na(data)), cv_outlier := data]
  df <- df[which(grepl("RUS/LONGITUDINAL", survey_name) & age_year != 2 & !is.na(data)), data := NA]
  
  ### outlier 1 y/o for MCV from cv_survey==1
  df[cv_survey==1 & age_year==1 & me_name=="vacc_mcv1" & !is.na(data) &
             ihme_loc_id != "HTI", cv_outlier_mcv1 := data]
  df[cv_survey==1 & age_year==1 & me_name=="vacc_mcv1" & !is.na(data) &
             ihme_loc_id != "HTI", data := NA]
  df[cv_survey==1 & age_year==1 & me_name=="vacc_mcv1" & !is.na(cv_outlier) &
             ihme_loc_id != "HTI", cv_outlier_mcv1 := cv_outlier]
  df[cv_survey==1 & age_year==1 & me_name=="vacc_mcv1" & !is.na(cv_outlier) &
             ihme_loc_id != "HTI", cv_outlier := NA]
  
  ### outlier MCV2 in USA, since schedule is 4-6 years
  df[cv_survey==1 & (age_year < 6 | is.na(age_year)) & me_name %in% c("vacc_mcv2", "vacc_mmr2") & !is.na(data) & 
             ihme_loc_id %in% c("USA", locs[parent_id==102, ihme_loc_id]) & survey_name != "School Vaccination Assessment Program", cv_outlier := data]
  df[cv_survey==1 & (age_year < 6 | is.na(age_year)) & me_name %in% c("vacc_mcv2", "vacc_mmr2") & !is.na(data) & 
             ihme_loc_id %in% c("USA", locs[parent_id==102, ihme_loc_id]) & survey_name != "School Vaccination Assessment Program", data := NA]
  
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 3: Clean data
########################################################################################################################


#----BASIC CLEANING FUNCTIONS-------------------------------------------------------------------------------------------
### cleanup dataset
clean.data <- function(df) {
  ## Get list of cvs
  cvs <- grep("cv_", names(df), value=TRUE)
  ## Subset to ones that are binary
  i <- lapply(cvs, function(x) length(setdiff(unique(df[!is.na(get(x))][[x]]), c(0, 1)))==0) %>% unlist
  cvs <- cvs[i]
  ## Harmonize cv_* terms that are binary
  for (var in cvs) df[is.na(get(var)), (var) := 0]
  ## Clean year_id
  df <- df[, year_id := as.numeric(as.character(year_id))]
  ## Clean sample_size
  df <- df[, sample_size := as.numeric(sample_size)]
  ## Offset coverage
  df <- df[data <= 0, data := 0.001]
  df <- df[data >= 1, data := 0.999]
  ## Age group and sex
  df <- df[, `:=` (age_group_id=22, sex_id=3)]
  ## Bundle
  df <- df[, bundle := "vaccination"]
  ## Order
  df <- df[order(me_name, ihme_loc_id, year_id)]
  return(df)
}

### create unique identifier by survey
create.id <- function(df) {
  ## Create id for cleanliness
  cols.id <- c('nid', 'ihme_loc_id', 'year_start', 'year_end', 'year_id', 'survey_name', 'survey_module', 'file_path')
  df <- df[, cv_id := .GRP, by=cols.id]
  ## Look for duplicates
  df <- df[, dupe := lapply(.SD, function(x) length(x) - 1), .SDcols='cv_id', by=c('cv_id', 'me_name')]
  ## Drop duplicates from last year from india
  ndupe <- nrow(df[which(dupe > 0)])
  print(paste0("Dropping duplicates by cv_id : ", ndupe, " rows"))
  df <- df[!which(dupe > 0)]
  ## Check for remaining duplicates
  if (max(df$dupe) > 0) stop("Duplicates in meta")
  df$dupe <- NULL
  
  return(df)
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 4: Calculate out administrative bias
########################################################################################################################


#----ADMIN BIAS CORRECTION FUNCTIONS------------------------------------------------------------------------------------
### function to use ST-GPR to smooth administrative bias using SDI as a predictor
est.time_varying_bias <- function(df, launch=TRUE, NOTES) {
  
  if (launch) {
    
    to_model_bias_dir <- file.path(to_model_dir, "admin_bias")
    if (!dir.exists(to_model_bias_dir)) dir.create(to_model_bias_dir)
    
    ### prep dataset to include in bias adjustment
    print("Prepping dataset")
    df.include <- df[!is.na(data) & me_name %in% vacc_list, ]
    # set variance where missing
    df.include <- df.include[cv_lit==1 & !is.na(sample_size), variance := data * (1 - data) / sample_size]
    df.include <- df.include[cv_lit==1 & is.na(variance), variance := data * (1 - data) / 100]
    df.include[is.na(standard_error) & !is.na(variance), standard_error := sqrt(variance)]
    
    ### calculate difference between admin data and survey
    merge_cols <- c("me_name", "ihme_loc_id", "year_id", "age_group_id", "sex_id")
    df.adjust  <- merge(df.include[cv_admin==1, c(merge_cols, "nid", "data", "standard_error", "variance"), with=FALSE], 
                       df.include[cv_admin==0, c(merge_cols, "data", "standard_error", "variance", "survey_name", "year_start"), with=FALSE], 
                       by=merge_cols, all=TRUE)
    df.adjust  <- df.adjust[!is.na(data.x) & !is.na(data.y), ]
    #df.adjust[, data := data.x - data.y]
    df.adjust[, data := data.y / data.x]
    df.adjust[, variance := data.y ^ 2 / data.x ^ 2 * (variance.y / data.y ^ 2 + 0 / data.x ^ 2)]
    df.adjust <- merge(df.adjust, locs[, .(location_id, ihme_loc_id)], by="ihme_loc_id", all.x=TRUE)
    df.adjust[, sample_size := NA_integer_]
    
    ### outlier
    outlier <- df.adjust[(data > 2) |
                         (ihme_loc_id %in% c("TCD", "CAF", "NGA") & survey_name=="UNICEF_MICS" & year_start==1995) |
                         (me_name=="vacc_dpt3" & ihme_loc_id=="VEN" & data > 1.5) |
                         (ihme_loc_id=="GNB" & data > 1.4) |
                         (ihme_loc_id=="NGA" & survey_name=="MACRO_DHS" & year_start==1990 & me_name=="vacc_mcv1") |
                         (ihme_loc_id=="NGA" & survey_name=="MACRO_DHS" & year_start==1990 & year_id %in% 1986:1987) |
                         (ihme_loc_id=="LSO" & survey_name=="UNICEF_MICS" & year_id < 2000) |
                         (ihme_loc_id=="LSO" & year_id==2002) |
                         (ihme_loc_id=="TCD" & survey_name=="MACRO_DHS" & year_start==1996) |
                         (ihme_loc_id=="COG" & data.x < 0.61 & me_name %in% c("vacc_bcg", "vacc_polio3")) |
                         (ihme_loc_id=="DJI" & survey_name=="ARAB_LEAGUE_PAPFAM" & year_start==2002), ]
    write.csv(outlier, file.path(to_model_bias_dir, "outliers.csv"), row.names=FALSE)
    df.adjust <- fsetdiff(df.adjust, outlier, all = TRUE)
    df.adjust <- df.adjust[, .(me_name, nid, location_id, year_id, age_group_id, sex_id, data, variance, sample_size)]
    
    ### save for upload
    for (i in vacc_list) {
      df.sub <- df.adjust[me_name==i, ] %>% copy
      df.sub[, me_name := paste0("bias_", me_name)]
      write.csv(df.sub, file.path(to_model_bias_dir, paste0(i, ".csv")), row.names=FALSE, na="")
    }
  
    ### model smoothed bias
    # launch ST-GPR models
    print("Calculating administrative bias across vaccinations")
    source(file.path(code_root, "FILEPATH/launch.r"), local=TRUE)
    job_hold(paste0("MASTER_", RUNS)) 
    
  } else {
    
    # load run ids
    RUNS <- fread(file.path(code_root, "FILEPATH/run_log.csv"))[is_best==1, run_id]
    
    # clean workspace and load special functions
    if (beta_version) { model_root <- "FILEPATH" 
    } else { model_root <- "FILEPATH" }
    setwd(model_root)
    source("init.r"); source("register_data.r")
  
  }
  
  # pull model results
  bias <- rbindlist(lapply(RUNS, function(x) model_load(x, obj="raked") %>% data.table %>% .[, run_id := x]))
  
  # attach me names
  logs_path <- file.path(code_root, "FILEPATH/run_log.csv")
  logs_file <- fread(logs_path)
  bias <- merge(bias, logs_file[, .(run_id, me_name)], by="run_id", all.x=TRUE)
  setnames(bias, "gpr_mean", "cv_admin_bias_ratio")
  bias <- merge(bias, locs[, .(location_id, ihme_loc_id)], by="location_id", all.x=TRUE)
  bias <- bias[, .(me_name, ihme_loc_id, year_id, age_group_id, sex_id, cv_admin_bias_ratio)]
  
  ### merge on bias ratio
  bias[, cv_admin := 1]
  df <- merge(df, bias, by=c("me_name", "ihme_loc_id", "year_id", "age_group_id", "sex_id", "cv_admin"), all.x=TRUE)
  
  ### shift administrative data and save preshifted value
  print("Shifting administrative estimates")
  df <- df[cv_admin==1 & !is.na(cv_admin_bias_ratio), cv_admin_orig := data]
  df <- df[cv_admin==1 & !is.na(cv_admin_bias_ratio), data := data * cv_admin_bias_ratio]
  
  return(df)
  
}

### function to calculate uncertainty in administrative data
set.admin_variance <- function(df) {
  ### by country, year, vaccination, count how many non-admin points
  df <- df[, n_survey := length(data[!is.na(data) & cv_survey==1]), by=c("ihme_loc_id", "year_id", "me_name")]
  df <- df[n_survey > 0 & cv_admin==1, variance := data * (1 - data) / 10]
  df <- df[n_survey==0 & cv_admin==1, variance := data * (1 - data) / 50]
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 5: Adjust introduction frame on if data is present prior to official introduction date
########################################################################################################################


#----INTRO YEAR FUNCTIONS-----------------------------------------------------------------------------------------------
adjust.intro_frame <- function(df) {
  df <- df[, cv_intro_years := ifelse((year_id - (cv_intro - 1)) >= 0, year_id - (cv_intro - 1), 0)]
  return(df)
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 6: Set ROTAC Based on Schedule
########################################################################################################################


#----ROTAVIRUS FUNCTIONS------------------------------------------------------------------------------------------------
### make rotavirus c coverage from rota doses
make.rotac <- function(df) {
  sf <- readRDS(vacc.schedule)[, .(ihme_loc_id, doses)]
  ## Cap doses at 3, if doses = 0, set to 2
  sf <- sf[doses > 3, doses := 3]; sf <- sf[doses==0, doses := 2]
  sf <- sf[, me_name := paste0("vacc_rota", doses)]; sf$doses <- NULL
  sf <- sf[, rota_flag := 1]
  ## Merge on
  df <- merge(df, sf, by=c("ihme_loc_id", "me_name"), all.x=TRUE)
  ## Locations with dose information
  locs.dose <- df[!is.na(rota_flag)]$ihme_loc_id %>% unique
  ## Locations that have introduced rota
  df.intro <- readRDS(vacc.intro)
  locs.intro <- df.intro[grepl("rota", me_name) & cv_intro < 9999]$ihme_loc_id %>% unique
  ## For locations that have no dose information but have introduced rota, assume 2 doses for now
  locs.assume <- setdiff(locs.intro, locs.dose)
  ## Pull out new frame that will be the mapped rotac data
  df.rotac <- df[(rota_flag == 1 | (me_name == "vacc_rota2" & ihme_loc_id %in% locs.assume)) & !is.na(data)]
  df.rotac <- df.rotac[, me_name := "vacc_rotac"]
  ## Append back onto df
  df <- rbind(df, df.rotac)
  ## Clean
  df <- df[, rota_flag := NULL]
  return(df)
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 7: Create ratios for modeling
########################################################################################################################


#----RATIO FUNCTIONS----------------------------------------------------------------------------------------------------
### make ratios from dtp3/mcv1 denominators
make.ratios <- function(df, me) {
  ## Setup
  vaccs <- unlist(strsplit(me, "_"))[2:3]
  num <- paste0("vacc_", vaccs[1])
  denom <- paste0("vacc_", vaccs[2])
  ## Combine frames
  cols <- c("ihme_loc_id", "year_id", "age_group_id", "sex_id", "nid", "cv_id", "cv_admin_orig", "data", "variance", "sample_size")
  df.num <- df[me_name==num, cols, with=F]; df.num$x <- "num"
  df.denom <- df[me_name==denom, cols, with=F]; df.denom$x <- "denom"
  ## Make sure that ratio calculated between admin data is based on pre-bias shift
  df.num <- df.num[!is.na(cv_admin_orig), data := cv_admin_orig]
  df.num$cv_admin_orig <- NULL
  df.denom <- df.denom[!is.na(cv_admin_orig), data := cv_admin_orig]
  df.denom$cv_admin_orig <- NULL
  cols <- setdiff(cols, "cv_admin_orig")
  df.r <- rbind(df.num, df.denom) %>% dcast.data.table(., ihme_loc_id + year_id + age_group_id + sex_id + nid + cv_id ~ x, value.var=c("data", "variance", "sample_size"))
  ## Calculate ratios, variance 
  df.r <- df.r[, data := data_num / data_denom]
  #df.r <- df.r[, data := ifelse(data >= 1, 0.999, data)]
  df.r <- df.r[!is.na(data)]
  df.r <- df.r[, variance := data_num ^ 2 / data_denom ^ 2 * (variance_num / data_num ^ 2 + variance_denom / data_denom ^ 2)] ## (http://www.stat.cmu.edu/~hseltman/files/ratio.pdf)
  df.r <- df.r[, sample_size := data * (1 - data) / variance] ## Effective sample size from above variance calculation
  ## Clean
  df.r <- df.r[, cols, with=F]
  df.r <- df.r[, me_name := me]
  return(df.r)
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 8: Split metadata
########################################################################################################################


#----SPLIT AND SAVE FUNCTIONS-------------------------------------------------------------------------------------------
### split dataset 
split.meta <- function(df) {
  
  ### create id for cleanliness
  cols.id <- c('nid', 'ihme_loc_id', 'year_start', 'year_end', 'year_id', 'survey_name', 'survey_module', 'file_path')
  cols.meta <- c(cols.id, grep("cv_", names(df), value=TRUE))
  cols.data <- c('me_name', 'data', 'variance', 'sample_size')
  
  ### split sets
  meta <- df[, cols.meta, with=FALSE] %>% unique
  df.mod <- df[, c("ihme_loc_id", "year_id", "age_group_id", "sex_id", "nid",  "cv_id", "cv_intro", "cv_intro_years",  cols.data), with=FALSE]
  
  return(list(meta=meta, df.mod=df.mod, df=df))
}

### account for border changes
split.borders <- function(df, run=38720) {
  
  ### get LDI
  ldi <- get_covariate_estimates(covariate_id=57, year_id=unique(data_to_split$year_id))
  ldi <- merge(ldi, locations, by="location_id", all.x=TRUE)
  ldi[ihme_loc_id %in% c("IND_4841", "IND_4871"), ap_tel_mean_value := mean(mean_value), by="year_id"]
  ### get coefficient on LDI
  beta <- fread(file.path("FILEPATH", run, "prior_summary.csv"))[covariate=="ldi_pc", betas] %>% as.numeric %>% exp
  
  ### create missing border changes
  vars <- c("me_name", "nid", "year_id", "survey_name", "year_start", "year_end", "survey_module", "file_path", "age_year", "cv_lit", "cv_survey", "cv_admin")
  data_to_split <- df[nid %in% c(19963)] #, 65181
  data_to_add <- data_to_split[, vars, with=FALSE] %>% unique
  data_to_add[, ihme_loc_id := "IND_4871"]
  data_to_add <- rbind(data_to_add, copy(data_to_add)[, ihme_loc_id := "IND_4841"])
  
  ### prep dataset to split
  data_to_split <- data_to_split[ihme_loc_id=="IND_4841", c(vars, "data", "standard_error", "design_effect"), with=FALSE]
  setnames(data_to_split, c("data", "standard_error"), c("original_data", "original_standard_error"))
  
  ### bring together
  data_to_add <- merge(data_to_add, data_to_split, by=vars, all.x=TRUE)
  data_to_add <- merge(data_to_add, ldi[, .(ihme_loc_id, year_id, mean_value, ap_tel_mean_value)], by=c("ihme_loc_id", "year_id"), all.x=TRUE)
  data_to_add[, data := original_data * ((beta * mean_value) / (beta * ap_tel_mean_value))]
  data_to_add[, standard_error := sqrt( (original_standard_error * ((beta * mean_value) / (beta * ap_tel_mean_value))) ^ 2 * design_effect )]

}
#***********************************************************************************************************************


#----RUN---------------------------------------------------------------------------------------------------------------
## 1.) Load data
df.survey    <- prep.survey() ## prep unit record data
df.admin     <- readRDS(vacc.admin)[, year_id := as.numeric(as.character(year_id))] ## WHO administrative data
df.whosurvey <- readRDS(vacc.whosurvey)[, nid := nid %>% as.numeric] ## WHO survey report data
df.lit       <- readRDS(vacc.lit)[, cv_lit := 1] %>% .[, sample_size := gsub(",", "", sample_size) %>% as.numeric] %>% 
                      .[survey_name=="FILEPATH" & nid==5127, nid := 5243] %>% ## literature data 
                      .[!nid %in% unique(df.survey$nid), ] ## remove lit sources where we have microdata
df           <- rbind(df.survey, df.admin, df.whosurvey, df.lit, fill=TRUE) #rbind(df.survey, df.admin, df.lit, fill=TRUE)

## 2.) Outlier data using database
df <- outlier.data(df)

## 3.) Other outliers
df <- temp.outlier(df)

## 4.) Floor sample size at 20
df <- df[-which(sample_size < 20)]

## 5.) Clean data 
df <- clean.data(df)

## 6.) Remove duplicates
df <- create.id(df)

### 7.) Set variance
# Set admin variance based on survey data availability by country-year-vaccine
df <- set.admin_variance(df)
# Set lit variance where we have sample_size
df <- df[!is.na(sample_size), variance := data * (1 - data) / sample_size]
# Set lit variance where we dont have sample_size
df <- df[(cv_lit==1 | cv_whosurvey==1) & is.na(variance), variance := data * (1 - data) / 100]
# Set sample_size based off of variance
df <- df[!is.na(data) & is.na(sample_size), sample_size := data * (1 - data) / variance]

## 8.) Clean and save full set
df <- clean.data(df)

## 8.) Calculate out administrative bias
vacc_list <- c("vacc_mcv1", "vacc_dpt3", "vacc_bcg", "vacc_polio3")
df <- est.time_varying_bias(df, launch=TRUE, NOTES=paste0("outliers for SDGs, beta, zeta_no_data at 0.1 and 0 for everywhere else, ", date))
df <- clean.data(df)

## 10.) Set rotac using the scheduled doses
df <- make.rotac(df)

## 11.) Make ratios to model out
df <- df[me_name=="vacc_full_sub", me_name := "vacc_fullsub"]
df <- df[me_name=="vacc_dpt3_on_time", me_name := "vacc_dpt3time"]

ratios <- c("vacc_hib3_dpt3_ratio", "vacc_pcv3_dpt3_ratio", "vacc_rotac_dpt3_ratio", "vacc_dpt3_dpt1_ratio", 
            "vacc_rcv1_mcv1_ratio",
            "vacc_hepb3_dpt3_ratio",
            "vacc_mcv2_mcv1_ratio")
df.ratios <- lapply(ratios, function(x) make.ratios(df, x)) %>% rbindlist
df <- rbind(df, df.ratios, fill=TRUE, use.names=TRUE)

## 12.) Set introduction frame
df.intro <- readRDS(vacc.intro)
df <- merge(df, df.intro, by=c("ihme_loc_id", "year_id", "me_name"), all=TRUE)
df <- adjust.intro_frame(df)
df <- clean.data(df)

## 13.) Split meta
split <- split.meta(df)
df <- split$df.mod
df.full <- split$df
meta <- split$meta
#***********************************************************************************************************************


#----SAVE---------------------------------------------------------------------------------------------------------------
### save full dataset
saveRDS(df.full, file.path(to_model_dir, "vaccination.rds"))

### save metadata
saveRDS(meta, file.path(to_model_dir, "vaccination_meta.rds"))

### keep model mes
subset <- c("vacc_dpt3", "vacc_mcv1", "vacc_bcg", "vacc_polio3", "vacc_dpt3time", ratios)
df <- df[me_name %in% subset]

### map location_id
df <- merge(df, locs[, .(ihme_loc_id, location_id)], by='ihme_loc_id', all.x=TRUE)
if (nrow(df[is.na(location_id)]) > 0) stop("Unmapped locations")

### save for upload
for (i in subset) {
  df.sub <- df[me_name==i, ] %>% copy
  df.sub[!is.na(variance), sample_size := NA]
  write.csv(df.sub, file.path(to_model_dir, paste0(i, ".csv")), row.names=FALSE, na="")
}
#***********************************************************************************************************************


#----LAUNCH-------------------------------------------------------------------------------------------------------------
### launch ST-GPR models
NOTES <- paste0("outlier fixes for SDGs, add MOZ AID, ", date)
source(file.path(code_root, "FILEPATH/launch_gpr.r"))
#***********************************************************************************************************************