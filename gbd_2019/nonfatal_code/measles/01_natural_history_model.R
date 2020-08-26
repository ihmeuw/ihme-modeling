#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: "USERNAME""USERNAME"
# Path:    "FILEPATH"01_natural_history_model.R
# Purpose: Model measles fatal and nonfatal outcomes for GBD
#          PART ONE -    Incidence natural history model
#          PART TWO -    Rescale case estimates
#          PART THREE -  Age-sex split cases
#          PART FOUR -   Calculate prevalence and incidence from split case estimates
#          PART FIVE -   Format for COMO and save results to the database
#          PART SIX -    Model fatal outcomes with negative binomial model
#          PART SEVEN -  Calculate deaths from CFR
#          PART EIGHT -  Rescale death estimates
#          PART NINE -   Age-sex split deaths
#          PART TEN -    Replace modeled death estimates in select locations with CODEm data-rich feeder model
#          PART ELEVEN - Format for codcorrect and save results to the database
#***********************************************************************************************************************


########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, foreign, stats, MASS, dplyr, plyr, lme4, reshape2, parallel, rhdf5, reticulate, optimx) # "USERNAME": added reticulate package 02.11.19; optimx for lmer optimizer 09.09.19
source(""FILEPATH"load_packages.R")
load_packages(c("data.table", "mvtnorm"))

## Pull in Pandas for HDF5 
pandas <- import("pandas")

### set data.table fread threads to 1
setDTthreads(1)
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "measles"
age_start <- 4              ## age "early neonatal"
age_end   <- 16              ## age 55-59 years
a         <- 3               ## birth cohort years before 1980
cause_id  <- 341
me_id     <- 1436
gbd_round <- 6
year_end  <- if (gbd_round %in% c(3, 4, 5)) gbd_round + 2012 else if (gbd_round==6) 2019

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_gbd    <- paste0("draw_", draw_nums_gbd)
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", draw_cols_gbd)

### make folders on cluster
# mortality
cl.death.dir <- file.path(FILEPATH)
if (!dir.exists(cl.death.dir) & CALCULATE_COD=="yes") dir.create(cl.death.dir, recursive=TRUE)
# nonfatal
cl.version.dir <- file.path(FILEPATH)
if (!dir.exists(cl.version.dir) & CALCULATE_NONFATAL=="yes") dir.create(file.path(cl.version.dir), recursive=TRUE)
# shocks
cl.shocks.dir <- file.path(FILEPATH)
if (!dir.exists(cl.shocks.dir) & run_shocks_adjustment) dir.create(file.path(cl.shocks.dir), recursive=TRUE)

### directories
home <- file.path(FILEPATH,"00_documentation")
j.version.dir <- file.path(FILEPATH)
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
if (!dir.exists(j.version.dir.inputs)) dir.create(j.version.dir.inputs, recursive=TRUE)
if (!dir.exists(j.version.dir.logs)) dir.create(j.version.dir.logs, recursive=TRUE)

### save description of model run
# which model components are being uploaded?
if (CALCULATE_NONFATAL=="yes" & CALCULATE_COD=="no") add_  <- "NF"
if (CALCULATE_COD=="yes" & CALCULATE_NONFATAL=="no") add_  <- "CoD"
if (CALCULATE_NONFATAL=="yes" & CALCULATE_COD=="yes") add_ <- "NF and CoD"
# record CODEm data-rich feeder model versions used in CoD hybridization
if (CALCULATE_COD=="yes") description <- paste0("DR M ", male_CODEm_version, ", DR F ", female_CODEm_version, ", ", description)
# save full description to model folder
description <- paste0(add_, " - ", description)
cat(description, file=file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------
### load shared functions
source(""FILEPATH"get_population.R")
source(""FILEPATH"get_envelope.R") 
source(""FILEPATH"get_location_metadata.R") 
source(""FILEPATH"get_covariate_estimates.R") 
source(""FILEPATH"get_cod_data.R")

### load personal functions
""FILEPATH"read_hdf5_table-copy.R" %>% source 
""FILEPATH"read_excel.R" %>% source
""FILEPATH"rake.R" %>% source
""FILEPATH"collapse_point.R" %>% source
""FILEPATH"sql_query.R" %>% source
file.path(home, "code/functions/age_sex_split.R") %>% source
#***********************************************************************************************************************


########################################################################################################################
##### PART ONE: INCIDENCE NAT HIST MODEL ###############################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22)[,.(location_id, ihme_loc_id, location_name, 
                                                                                  location_ascii_name, region_id, super_region_id, 
                                                                                  super_region_name, level, location_type, 
                                                                                  parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])
standard_locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=101)$location_id %>% unique

### read population file
# get population for birth cohort at average age of notification by year
if (decomp) {
  population <- get_population(location_id=pop_locs, year_id=(1980-a):year_end, age_group_id=c(2:20, 30:32, 235), status="best",
                               sex_id=1:3, gbd_round_id=gbd_round, decomp_step=decomp_step)
} else { population <- get_population(location_id=pop_locs, year_id=(1980-a):year_end, age_group_id=c(2:20, 30:32, 235), status="best",
                                      sex_id=1:3, gbd_round_id=gbd_round)
}
population_young <- population[age_group_id %in% c(2:4) & sex_id==3, ]
population_young <- population_young[, year_id := year_id + a] %>% .[year_id <= year_end, ]
# collapse by location-year
sum_pop_young <- population_young[, pop := sum(population), by=c("location_id", "year_id")] %>% .[, .(location_id, year_id, pop)] %>% unique
# save model version
cat(paste0("Population - model run ", unique(population$run_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

### prep SIA data
prep_sia <- function(...) {

  ### correct SIA coverage
  source(FILEPATH"adjust_sia_data.R")
  SIA <- adjust_sia_data()
  setnames(SIA, "corrected_target_coverage", "supp")

  # cannot have >100%
  SIA[supp > 1, supp := 0.99]

  # apply SIAs in Sudan pre-2011 to South Sudan
  SIA_s_sudan <- subset(SIA, location_id==522 & year_id < 2011)
  SIA_s_sudan$location_id <- 435
  SIA <- rbind(SIA, SIA_s_sudan)

  # apply SIAs to subnational units
  SIA_sub <- copy(SIA)
  # make df of unique subnational locations
  loc_subs <- locations[level > 3, c("location_id", "parent_id")]
  # subset SIAs to only admin0 locations with subnational units
  SIA_sub <- SIA_sub[SIA_sub$location_id %in% unique(loc_subs$parent_id), ]
  setnames(SIA_sub, "location_id", "parent_id")

  # level 1
  subs_level1 <- join(SIA_sub, loc_subs, by="parent_id", type="inner")

  # level 2
  subs_level2 <- subs_level1[, .(location_id, year_id, supp)]
  setnames(subs_level2, "location_id", "parent_id")
  subs_level2 <- join(subs_level2, loc_subs, by="parent_id", type="inner")
  subs_level2 <- subs_level2[!is.na(location_id), ]

  # level 3
  subs_level3 <- subs_level2[, .(location_id, year_id, supp)]
  setnames(subs_level3, "location_id", "parent_id")
  subs_level3 <- join(subs_level3, loc_subs, by="parent_id", type="inner")
  subs_level3 <- subs_level3[!is.na(location_id), ]

  # bring together SIAs for all subnational levels
  SIA_subs <- bind_rows(subs_level1, subs_level2, subs_level3)[, parent_id := NULL]

  # add subnational SIAs to national SIA df
  SIA <- bind_rows(SIA, SIA_subs)
  SIA <- SIA[!duplicated(SIA[, c("location_id", "year_id")]), ]

  # add lag to SIA, 1 to 5 years
  for (i in 1:5) {
    assign(paste("lag", i, sep="_"), copy(SIA))
    get(paste("lag", i, sep="_"))[, year_id := year_id + i]
    setnames(get(paste("lag", i, sep="_")), "supp", paste0("supp_", i))
  }

  # bring lags together
  SIAs <- join_all(list(SIA, lag_1, lag_2, lag_3, lag_4, lag_5), by=c("location_id", "year_id"), type="full") %>% as.data.table

  # make missing SIAs 0
  for (i in 1:5) {
    SIAs[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0]
    SIAs[get(paste0("supp_", i))==0, paste0("supp_", i) := 0.000000001]
  }

  return(SIAs)

}

### prep incidence (case notification) data
prep_case_notifs <- function(add_subnationals=FALSE) {

  # get WHO case notification data, downloaded from "ADDRESS"
  case_notif <- read_excel(""FILEPATH"incidence_series.xls", sheet="Measles") %>% data.table
  case_notif[, c("WHO_REGION", "Cname", "Disease") := NULL]
  setnames(case_notif, "ISO_code", "ihme_loc_id")
  case_notif <- melt(case_notif, variable.name="year_id", value.name="cases")
  case_notif[, year_id := year_id %>% as.character %>% as.integer]
  case_notif[, nid := 83133]

  # get supplement
  case_notif_supp <- fread(FILEPATH)
  case_notif_supp_who <- read_excel(""FILEPATH"measlesreportedcasesbycountry.xls", sheet="WEB", skip=3) %>% data.table
  case_notif_supp_who <- case_notif_supp_who[, c(2:3, 16)]
  colnames(case_notif_supp_who) <- c("location_name", "ihme_loc_id", "cases")
  case_notif_supp_who[, cases := as.numeric(cases)]
  case_notif_supp_who <- case_notif_supp_who[!is.na(ihme_loc_id)]
  case_notif_supp_who[, year_id := 2017]
  case_notif_supp_who[, nid := 83133]
  case_notif_supp <- case_notif_supp[!(year_id==2017 & ihme_loc_id %in% case_notif_supp_who$ihme_loc_id), ]

  # get supplement (preliminary 2019 WHO case notifications)
  case_notif_supp <- read_excel(FILEPATH, sheet="WEB") %>% data.table
  setnames(case_notif_supp, c("ISO3", "Year"), c("ihme_loc_id", "year_id"))
  case_notif_supp <- case_notif_supp[year_id==2019]
  case_notif_supp[, c("Region", "Country", "July", "August", "September", "October", "November", "December") := NULL]
  # sum across first six months of 2019
  case_notif_supp$January <- as.numeric(case_notif_supp$January)
  case_notif_supp$February <- as.numeric(case_notif_supp$February)
  case_notif_supp$March <- as.numeric(case_notif_supp$March)
  case_notif_supp$April <- as.numeric(case_notif_supp$April)
  case_notif_supp$May <- as.numeric(case_notif_supp$May)
  case_notif_supp$June <- as.numeric(case_notif_supp$June)
  case_notif_supp[, six_mo_sum := rowSums(.SD), .SDcols = 3:8]
  case_notif_supp[, cases := six_mo_sum*2][, c("January", "February", "March", "April", "May", "June", "six_mo_sum") := NULL]
  # make nid column
  case_notif_supp[, nid := 419891]  
  # rbind to other case_notifs
  case_notif <- rbind(case_notif, case_notif_supp)

  # include WHO 2018 numbers if missing
  case_notif_supp <- read_excel(FILEPATH, sheet="WEB") %>% data.table
  setnames(case_notif_supp, c("ISO3", "Year"), c("ihme_loc_id", "year_id"))
  case_notif_supp <- case_notif_supp[year_id==2018]
  case_notif_supp$January <- as.numeric(case_notif_supp$January)
  case_notif_supp$February <- as.numeric(case_notif_supp$February)
  case_notif_supp$March <- as.numeric(case_notif_supp$March)
  case_notif_supp$April <- as.numeric(case_notif_supp$April)
  case_notif_supp$May <- as.numeric(case_notif_supp$May)
  case_notif_supp$June <- as.numeric(case_notif_supp$June)
  case_notif_supp$July <- as.numeric(case_notif_supp$July)
  case_notif_supp$August <- as.numeric(case_notif_supp$August)
  case_notif_supp$September <- as.numeric(case_notif_supp$September)
  case_notif_supp$October <- as.numeric(case_notif_supp$October)
  case_notif_supp$November <- as.numeric(case_notif_supp$November)
  case_notif_supp$December <- as.numeric(case_notif_supp$December)
  case_notif_supp[, c("Region", "Country") := NULL]
  case_notif_supp[, cases := rowSums(.SD), .SDcols = 3:14]
  case_notif_supp <- case_notif_supp[, c("ihme_loc_id", "year_id", "cases")]
  setnames(case_notif_supp, "cases", "cases_supp")

  case_notif <- case_notif[year_id==2018 & is.na(cases), add_supp := 1]
  case_notif <- merge(case_notif, case_notif_supp, by=c("ihme_loc_id", "year_id"), all.x=TRUE)
  case_notif <- case_notif[add_supp==1, cases := cases_supp][, c("add_supp", "cases_supp") := NULL]

  # Force 2019 ECU to 0 based on this source: "ADDRESS"
  extra_row <- data.table(ihme_loc_id="ECU",
                          year_id=2019,
                          cases=0,
                          nid=422994)  
  case_notif <- rbind(case_notif, extra_row)

  # bring together and clean
  case_notif <- merge(case_notif, locations[, .(location_id, ihme_loc_id)], by="ihme_loc_id", all.x=TRUE)


  if (add_subnationals) {

    ### add subnational case notifications
    # Japan
    JPN_data_subnational <- list.files(file.path(j_root, ""FILEPATH"/INFECTIOUS_DISEASE_WEEKLY_REPORT"),
                                       full.names=TRUE, pattern=".csv", ignore.case=TRUE)
    JPN <- lapply(JPN_data_subnational, function(x) {
      dt <- read.csv(x, skip=3) %>% as.data.table
      colnums <- grep("Measles", colnames(dt)) + 1
      dt <- dt[-1, c(1, colnums), with=FALSE]
      colnames(dt) <- c("location_ascii_name", "cases")
      dt[, cases := cases %>% as.integer]
      dt[, year_id := substring(x, 98, 101) %>% as.integer]
    }) %>% rbindlist(., fill=TRUE)
    JPN <- JPN[location_ascii_name != "Total No."]
    if (decomp) {JPN <- JPN[year_id %in% c(2012, 2013, 2014, 2015, 2016, 2017, 2018)]}  
    JPN_nids <- data.table(year_id=c(2012, 2013, 2014, 2015, 2016, 2017, 2018), nid=c(205877, 206136, 206141, 257236, 282062, 335319, 373828))
    JPN <- merge(JPN, JPN_nids, by="year_id", all.x=TRUE)
    JPN <- merge(JPN, locations[, .(location_id, location_ascii_name, ihme_loc_id)], by="location_ascii_name", all.x=TRUE) %>% .[, .(nid, ihme_loc_id, location_id, year_id, cases)]
    # United States
    USA <- read_excel(FILEPATH) %>% data.table %>% .[, .(nid, ihme_loc_id, location_id, year_id, cases)]
    # add to dataset
    case_notif <- bind_rows(case_notif, JPN, USA)

  }

  # drop unmapped locations
  drop.locs <- case_notif[!(ihme_loc_id %in% locations$ihme_loc_id)]$ihme_loc_id %>% unique
  if (length(drop.locs) > 0 ) {
    print(paste0("unmapped locations in WHO dataset (dropping all): ", toString(drop.locs)))
    case_notif <- case_notif[!(ihme_loc_id %in% drop.locs)]
  }

  # remove missing rows
  case_notif <- case_notif[!is.na(cases), .(nid, location_id, year_id, cases)]

  return(case_notif)

}

### prepare covariates
prep_covs <- function(...) {

  ### get updated covariates
  ### covariate: measles_vacc_cov_prop,
  if (use_lagged_covs) {
    mcv1 <- get_covariate_estimates(covariate_id=2309, gbd_round_id=gbd_round, decomp_step=decomp_step)
  } else {
    if (decomp) mcv1 <- get_covariate_estimates(covariate_id=75, gbd_round_id=gbd_round, decomp_step=decomp_step) else mcv1 <- get_covariate_estimates(covariate_id=75, gbd_round_id=gbd_round)
  }
  mcv1[, ln_unvacc := log(1 - mean_value)]
  setnames(mcv1, "mean_value", "mean_mcv1")
  # save model version
  cat(paste0("Covariate MCV1 (NF) - model version ", unique(mcv1$model_version_id)), file=file.path(FILEPATH, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  #remove excess columns
  mcv1 <- mcv1[, .(location_id, year_id, ln_unvacc, mean_mcv1)]

  ### covariate: covariate_name_short="measles_vacc_cov_prop_2"
  if (use_lagged_covs) {
    mcv2 <- get_covariate_estimates(covariate_id=2310, gbd_round_id=gbd_round, decomp_step=decomp_step)
  } else {
    if (decomp) mcv2 <- get_covariate_estimates(covariate_id=1108, gbd_round_id=gbd_round, decomp_step=decomp_step) else mcv2 <- get_covariate_estimates(covariate_id=1108, gbd_round_id=gbd_round)
  }
  mcv2[, ln_unvacc_mcv2 := log(1 - mean_value)]
  setnames(mcv2, "mean_value", "mean_mcv2")
  # save model version
  cat(paste0("Covariate MCV2 (NF) - model version ", unique(mcv2$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  #remove excess columns
  mcv2 <- mcv2[, .(location_id, year_id, ln_unvacc_mcv2, mean_mcv2)]

  ### merge together MCV1 and MCV2
  covs_both <- merge(mcv1, mcv2, by=c("location_id", "year_id"), all.x=TRUE)

  if (MCV_lags == "yes") {

    # calculate proportion of people receiving ONLY mcv1 versus mcv2
    covs_both[, ln_unvacc_mcv1_only := log( 1 - (mean_mcv1 - mean_mcv2) )]
    covs_both[, ln_unvacc_mcv2_only := log(1 - mean_mcv2)]

    # lags on MCV coverage
    for (i in 1:5) {
      assign(paste0("mcv_lag_", i), subset(covs_both, select=c("location_id", "year_id", "ln_unvacc", "ln_unvacc_mcv2")))
      setnames(get(paste0("mcv_lag_", i)), "ln_unvacc", paste0("mcv1_lag_", i))
      setnames(get(paste0("mcv_lag_", i)), "ln_unvacc_mcv2", paste0("mcv2_lag_", i))
      get(paste0("mcv_lag_", i))[, year_id := year_id + i]
      assign(paste0("mcv_lag_", i), get(paste0("mcv_lag_", i))[, c("location_id", "year_id", paste0("mcv1_lag_", i), paste0("mcv2_lag_", i)), with=FALSE])
      # fill in missingness
      get(paste0("mcv_lag_", i))[is.na(get(paste0("mcv1_lag_", i))), paste0("mcv1_lag_", i) := log(1)]
      get(paste0("mcv_lag_", i))[is.na(get(paste0("mcv2_lag_", i))), paste0("mcv2_lag_", i) := log(1)]
    }

    covs_both <- merge(covs_both, mcv_lag_1, by=c("location_id", "year_id"), all.x=TRUE)
    covs_both <- merge(covs_both, mcv_lag_2, by=c("location_id", "year_id"), all.x=TRUE)
    covs_both <- merge(covs_both, mcv_lag_3, by=c("location_id", "year_id"), all.x=TRUE)
    covs_both <- merge(covs_both, mcv_lag_4, by=c("location_id", "year_id"), all.x=TRUE)
    covs <- merge(covs_both, mcv_lag_5, by=c("location_id", "year_id"), all.x=TRUE)

  } else {

    covs <- copy(covs_both[!is.na(ln_unvacc_mcv2)])

  }

  return(covs)

}

### combine input data for regression
prep_regression <- function(...) {

  # merge on location_id, region, and super region
  regress <- merge(case_notif, locations[, .(location_id, ihme_loc_id, region_id, super_region_id)], by="location_id", all.x=TRUE)
  # merge on population 
  regress <- join(regress, sum_pop_young, by=c("location_id", "year_id"), type="inner")
  # merge on ihme covariates and WHO SIA data
  regress <- join(regress, covs, by=c("location_id", "year_id"), type="inner")
  regress <- join(regress, SIAs, by=c("location_id", "year_id"), type="left")

  # add small offset to 0 or missing lags
  regress[is.na(supp), supp := 0]
  for (i in 1:5) {
    regress[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0.000000001]
    regress[get(paste0("supp_", i))==0, paste0("supp_", i) := 0.000000001]
  }

  if (ln_sia) {
    # Calculate ln_unvacc equivalent for SIAs  
    for (i in 1:5) {
      regress[, paste0("ln_unvacc_supp_", i) := log(1-get(paste0("supp_", i)))]
    }

  }

  ### set up regression
  # incidence rate generated from the population just for the 1 year birth cohort at average age of notification
  regress[, inc_rate := (cases / pop) * 100000]
  regress <- regress[inc_rate <= 95000, ]
  regress <- regress[!(ihme_loc_id=="PNG" & year_id %in% c(2013:2015)), ]

  ### generate transformed variables
  # proportion of population unvaccinated
  regress[, ln_inc := log(inc_rate)]
  regress <- do.call(data.table, lapply(regress, function(x) replace(x, is.infinite(x), NA)))

  # set up factors for regression
  regress[, super_region_id := as.factor(super_region_id)]
  regress[, region_id := as.factor(region_id)]
  regress[, location_id := as.factor(location_id)]

  ### add herd immunity covariate
  regress$herd_imm_95_mcv1 <- 0
  regress[mean_mcv1 >= 0.95, herd_imm_95_mcv1 := 1]

  regress$herd_imm_95_mcv2 <- 0
  regress[mean_mcv2 >= 0.95, herd_imm_95_mcv2 := 1]

  # relationship between vaccination and incidence
  regress <- regress[year_id >= 1995, ]  

  return(regress)

}

### prepare regression input data
covs <- prep_covs()
SIAs <- prep_sia()
case_notif <- prep_case_notifs()
regress <- prep_regression()
regress <- regress[location_id %in% standard_locations]

# save model input
fwrite(regress, FILEPATH, row.names=FALSE)
#***********************************************************************************************************************


#----MIXED EFFECTS MODEL------------------------------------------------------------------------------------------------
### scale regression covariates
if (scale_to_converge) {
  ## save the means
  mean_u1 <- mean(regress$ln_unvacc)
  mean_u2 <- mean(regress$ln_unvacc_mcv2)
  mean_s1 <- mean(regress$supp_1)
  mean_s2 <- mean(regress$supp_2)
  mean_s3 <- mean(regress$supp_3)
  mean_s4 <- mean(regress$supp_4)
  mean_s5 <- mean(regress$supp_5)

  ## save the standard devs
  sd_u1 <- sd(regress$ln_unvacc)
  sd_u2 <- sd(regress$ln_unvacc_mcv2)
  sd_s1 <- sd(regress$supp_1)
  sd_s2 <- sd(regress$supp_2)
  sd_s3 <- sd(regress$supp_3)
  sd_s4 <- sd(regress$supp_4)
  sd_s5 <- sd(regress$supp_5)

  ## manually scale
  regress[, ln_unvacc := (ln_unvacc - mean_u1)/sd_u1]
  regress[, ln_unvacc_mcv2 := (ln_unvacc_mcv2 - mean_u2)/sd_u2]
  regress[, supp_1 := (supp_1-mean_s1)/sd_s1]
  regress[, supp_2 := (supp_2-mean_s2)/sd_s2]
  regress[, supp_3 := (supp_3-mean_s3)/sd_s3]
  regress[, supp_4 := (supp_4-mean_s4)/sd_s4]
  regress[, supp_5 := (supp_5-mean_s5)/sd_s5]
}

### run mixed effects regression model
if (MCV1_or_MCV2 == "use_MCV2") {

  me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 +
                     (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress, 
                   control = lmerControl(optimizer = "nloptwrap", calc.derivs = FALSE))

  if (ln_sia) {
    me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + ln_unvacc_supp_1 + ln_unvacc_supp_2 + ln_unvacc_supp_3 + ln_unvacc_supp_4 + ln_unvacc_supp_5 +
                       (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress)
  }

} else if (MCV1_or_MCV2 == "MCV1_only") {

  me_model <- lmer(ln_inc ~ ln_unvacc + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 +
                     (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress)

}

### save log
capture.output(summary(me_model), file=FILEPATH, type="output")
#***********************************************************************************************************************


#----DRAWS--------------------------------------------------------------------------------------------------------------
set.seed(0311)  
### prep prediction data
draws_nonfatal <- merge(covs, locations[, .(location_id, ihme_loc_id, region_id, super_region_id)], by="location_id", all.x=TRUE)
draws_nonfatal <- merge(draws_nonfatal, sum_pop_young, by=c("location_id", "year_id"), all.x=TRUE)
draws_nonfatal <- merge(draws_nonfatal, SIAs[, c("location_id", "year_id", paste0("supp_", 1:5))], by=c("location_id", "year_id"), all.x=TRUE)

# fix missing lags
for (i in 1:5) {
  if (ln_sia) {
    draws_nonfatal[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0.000000001]

  } else {
    draws_nonfatal[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0]
  }
}

if (ln_sia) {
  # Calculate ln_unvacc equivalent for SIAs
  for (i in 1:5) {
    draws_nonfatal[, paste0("ln_unvacc_supp_", i) := log(1-get(paste0("supp_", i)))]
  }
}


if (scale_to_converge) {
  ## manually scale
  draws_nonfatal[, ln_unvacc := (ln_unvacc - mean_u1)/sd_u1]
  draws_nonfatal[, ln_unvacc_mcv2 := (ln_unvacc_mcv2 - mean_u2)/sd_u2]
  draws_nonfatal[, supp_1 := (supp_1-mean_s1)/sd_s1]
  draws_nonfatal[, supp_2 := (supp_2-mean_s2)/sd_s2]
  draws_nonfatal[, supp_3 := (supp_3-mean_s3)/sd_s3]
  draws_nonfatal[, supp_4 := (supp_4-mean_s4)/sd_s4]
  draws_nonfatal[, supp_5 := (supp_5-mean_s5)/sd_s5]
}


### set options
if (MCV1_or_MCV2 == "use_MCV2") {
  cols <- c("constant", "b_ln_unvacc", "b_ln_unvacc_mcv2", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5")
} else if (MCV1_or_MCV2 == "MCV1_only") {
  cols <- c("constant", "b_ln_unvacc", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5")
}
### calculate 1000 draws of case counts
# coefficient matrix
coeff <- cbind(fixef(me_model)[[1]] %>% data.table, coef(me_model)[[1]] %>% data.table %>% .[, "(Intercept)" := NULL] %>% .[1, ])
colnames(coeff) <- cols
coefmat <- matrix(unlist(coeff), ncol=length(cols), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(cols))))

# generate a standard random effect - 95% attack rate in unvaccinated population
# given 0% vaccinated, the combination of the RE and the constant will lead to an incidence of 95,000/100,000
standard_RE <- log(95000) - coefmat["coef", "constant"]

# covariance matrix
vcovmat <- vcov(me_model)
vcovlist <- NULL
for (ii in 1:length(cols)) {
  if (MCV1_or_MCV2 == "use_MCV2") { vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2], vcovmat[ii, 3], vcovmat[ii, 4], vcovmat[ii, 5], vcovmat[ii, 6], vcovmat[ii, 7], vcovmat[ii, 8])
  } else if (MCV1_or_MCV2 == "MCV1_only") { vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2], vcovmat[ii, 3], vcovmat[ii, 4], vcovmat[ii, 5], vcovmat[ii, 6], vcovmat[ii, 7]) }
  vcovlist <- c(vcovlist, vcovlist_a)
}
vcovmat2 <- matrix(vcovlist, ncol=length(cols), byrow=TRUE)

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat2)

# transpose coefficient matrix
betas <- t(betadraws)

### calculate cases from model coefficients
draw_nums <- 1:1000
draw_cols <- paste0("case_draw_", draw_nums_gbd)
if (MCV1_or_MCV2 == "use_MCV2") {
  invisible(
    draws_nonfatal[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                                           ( betas[2, x] * ln_unvacc ) +
                                                                           ( betas[3, x] * ln_unvacc_mcv2 ) +
                                                                           ( betas[4, x] * supp_1 ) +
                                                                           ( betas[5, x] * supp_2 ) +
                                                                           ( betas[6, x] * supp_3 ) +
                                                                           ( betas[7, x] * supp_4 ) +
                                                                           ( betas[8, x] * supp_5 ) +
                                                                           standard_RE ) *
        ( pop / 100000 ) } )] )
} else if (MCV1_or_MCV2 == "MCV1_only") {
  invisible(
    draws_nonfatal[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                                           ( betas[2, x] * ln_unvacc ) +
                                                                           ( betas[3, x] * supp_1 ) +
                                                                           ( betas[4, x] * supp_2 ) +
                                                                           ( betas[5, x] * supp_3 ) +
                                                                           ( betas[6, x] * supp_4 ) +
                                                                           ( betas[7, x] * supp_5 ) +
                                                                           standard_RE ) *
        ( pop / 100000 ) } )] )
}

### save results
draws_nonfatal <- draws_nonfatal[, c("location_id", "year_id", draw_cols), with=FALSE]
if (WRITE_FILES == "yes") {
  fwrite(draws_nonfatal, FILEPATH, row.names=FALSE)
}
#***********************************************************************************************************************



#----FIX MODEL ESTIMATES------------------------------------------------------------------------------------------------
### get population
pop_u5 <- population[year_id %in% c(1980:year_end) & age_group_id %in% c(2:5) & sex_id==3, ]
pop_u5 <- pop_u5[, pop := sum(population), by=c("location_id", "year_id")] %>% .[, .(location_id, year_id, pop)] %>% unique

### PART A - trust case notifications from 3 super regions: high income, eastern europe/central asia, latin america/caribbean
sr_64_31_103 <- locations[super_region_id %in% c(64, 31, 103), location_id]
case_notif <- prep_case_notifs(add_subnationals=TRUE)
# ignore GBD locations in these super regions that do not have case notifications (i.e. BMU, GRL, PRI, VIR, and
# other high-come subnationals levels)
trusted_notifications <- sr_64_31_103[sr_64_31_103 %in% unique(case_notif$location_id)]

### model case notifications in ST-GPR to smooth missingness in WHO reporting data
if (run_case_notification_smoothing) {
  source(file.path(FILEPATH"est_shocks_copy.R"))
  case_notification_smoothing()
}

### pull case notifications
adjusted_notifications_q <- lapply(list.files(FILEPATH, full.names=TRUE), fread) %>%
  rbindlist %>% .[, .(location_id, year_id, combined)] %>% setnames(., "combined", "cases")

#include additional data for recent outbreaks not yet reflected in WHO
if (late_2019) {
  annualized_cases <- fread(FILEPATH)
  setnames(annualized_cases, "annualized_report", "cases")
  replacements <- unique(annualized_cases$location_id)
  removed_to_replace <- adjusted_notifications_q[!(location_id %in% replacements & year_id==2019), ]
  adjusted_notifications_q <- rbind(removed_to_replace, annualized_cases[, .(location_id, year_id, cases)])
  
  usa_cases <- fread(FILEPATH)
  setnames(usa_cases, "case_report", "cases")
  usa_replacements <- unique(usa_cases$location_id)
  removed_to_replace <- adjusted_notifications_q[!(location_id %in% usa_replacements & year_id==2019), ]
  adjusted_notifications_q <- rbind(removed_to_replace, usa_cases[, .(location_id, year_id, cases)])
  
  
}

### generate error
# calculate incidence rate from case notification and population data
adjusted_notifications <- merge(adjusted_notifications_q, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
adjusted_notifications <- merge(adjusted_notifications, locations[, .(location_id, region_id, super_region_id)], by="location_id", all.x=TRUE)
invisible(adjusted_notifications[, inc_rate := cases / pop])
wide <- dcast.data.table(adjusted_notifications_q, value.var="cases", location_id ~ year_id)
colnames(wide) <- c("location_id", paste0("x_", 1980:year_end))
if (decomp_step != "step4") {
  colnames(wide) <- c("location_id", paste0("x_", 1980:2017))
} else {
  colnames(wide) <- c("location_id", paste0("x_", 1980:year_end))
}

weird_locs <- wide[x_2019 > x_2018 & x_2019 > x_2017 & x_2019 > x_2016 & x_2019 > x_2015 & x_2019 > 5, location_id]  
weird_locs <- weird_locs[!weird_locs %in% case_notif[year_id==2019, location_id]] 
if (late_2019) weird_locs <- weird_locs[!weird_locs %in% c(replacements, usa_replacements)] 
year_end <- as.integer(year_end)                                                   


if (decomp_step != "step4") {  
  invisible(lapply(weird_locs, function(x) adjusted_notifications[location_id==x & year_id==2017, inc_rate := round(mean(adjusted_notifications[location_id==x & year_id %in% c((2017 - 3):(year_end - 1)), inc_rate]), 0)]))
} else {
  invisible(lapply(weird_locs, function(x) adjusted_notifications[location_id==x & year_id==year_end, inc_rate := round(mean(adjusted_notifications[location_id==x & year_id %in% c((year_end - 3):(year_end - 1)), inc_rate]), 0)]))
}


# calculate standard error
adjusted_notifications[, SE := sqrt( (inc_rate * (1 - inc_rate)) / pop )]  

# calculate region and super-region average error for country-years with 0 cases reported
notif_error_reg <- ddply(adjusted_notifications, c("region_id", "year_id"), summarise, reg_SE=mean(SE, na.rm=TRUE))
notif_error_sr <- ddply(adjusted_notifications, c("super_region_id", "year_id"), summarise, sr_SE=mean(SE, na.rm=TRUE))
# add on region and super-region mean error, replace with these errors if country-level error is zero
adjusted_notifications <- merge(adjusted_notifications, notif_error_reg, by=c("region_id", "year_id"), all.x=TRUE)
adjusted_notifications <- merge(adjusted_notifications, notif_error_sr, by=c("super_region_id", "year_id"), all.x=TRUE)
invisible(adjusted_notifications[SE==0 | is.na(SE), SE := reg_SE])
invisible(adjusted_notifications[SE==0 | is.na(SE), SE := sr_SE])

adjusted_notifications[SE==0, SE := sqrt( ((1 / pop) * inc_rate * (1 - inc_rate)) + ((1 / (4 * (pop ^ 2))) * ((qnorm(0.975)) ^ 2)) ) ]
# generate 1000 draws of incidence from error term
inc_draws <- rnorm(n=1000 * length(adjusted_notifications$inc_rate), mean=adjusted_notifications$inc_rate, sd=adjusted_notifications$SE) %>% matrix(., ncol=1000, byrow=FALSE) %>% as.data.table
colnames(inc_draws) <- paste0("inc_draw_", draw_nums_gbd)
adjusted_notifications <- cbind(adjusted_notifications, inc_draws)

### calculate cases from incidence and population
invisible(adjusted_notifications[, (draw_cols) := lapply(draw_nums_gbd, function(x) get(paste0("inc_draw_", x)) * pop)])
invisible(adjusted_notifications <- adjusted_notifications[, c("location_id", "year_id", draw_cols), with=FALSE])

if (run_shocks_adjustment) {
  ### PART B - countries where case notifications not trusted but want to adjust for shocks (i.e. outbreaks),
  ### trusting the trend in case notifications but not the absolute value
  # get list of countries with case notifications but are not in the 3 trusted super regions
  untrusted_locations <- case_notif[!location_id %in% trusted_notifications, location_id] %>% unique
  # get draws from modeled case notifications where we don't trust case notifications, to adjust for underreporting and shocks
  shocks_cases <- adjusted_notifications[location_id %in% untrusted_locations, ]
  # run ST-GPR shocks adjustment (smoothed difference between model case notifications through time)
  source(file.path(FILEPATH"est_shocks.r"))
  shocks_adjustment()

  # get results
  shocks_difference <- lapply(list.files(cl.shocks.dir, full.names=TRUE), fread) %>% rbindlist %>%
    .[, .(location_id, year_id, mean)] %>% setnames(., "mean", "shock")
  # apply modeled difference to modeled draws
  shocks_cases <- merge(shocks_cases, shocks_difference, by=c("location_id", "year_id"), all.x=TRUE)
  shocks_cases[, (draw_cols) := lapply(draw_nums_gbd, function(x) get(paste0("case_draw_", x)) + shock)]
  shocks_cases <- shocks_cases[, c("location_id", "year_id", draw_cols), with=FALSE]

  ### bring together modeled case draws and notification case draws
  combined_case_draws <- bind_rows(shocks_cases[location_id %in% untrusted_locations, ],
                                   adjusted_notifications[location_id %in% trusted_notifications, ],
                                   ### PART C - use model estimates for countries with no case notification data
                                   draws_nonfatal[!location_id %in% c(untrusted_locations, trusted_notifications), ])
} else {
  combined_case_draws <- bind_rows(adjusted_notifications[location_id %in% trusted_notifications, ],
                                   draws_nonfatal[!location_id %in% trusted_notifications, ])
  
  if (late_2019) {
    
    wsm_2019 <- adjusted_notifications[location_id==27 & year_id==2019]
    combined_case_draws <- combined_case_draws[!(location_id==27 & year_id==2019)]
    combined_case_draws <- rbind(combined_case_draws, wsm_2019)
    
  }

}


### floor draws at 0
invisible( lapply(draw_nums_gbd, function(x) combined_case_draws[get(paste0("case_draw_", x)) < 0, paste0("case_draw_", x) := 0]) )


### save results
if (WRITE_FILES == "yes") {
  fwrite(combined_case_draws, FILEPATH"02_add_case_notifs_and_shocks.csv"), row.names=FALSE)

  if (run_shocks_adjustment) {
    ### save for plotting
    # get mean of results and steps
    modeled_draws_q <- collapse_point(draws_nonfatal) %>% .[, .(location_id, year_id, mean)] %>% setnames(., "mean", "model_cases")
    notifs <- copy(adjusted_notifications_q[location_id %in% unique(case_notif$location_id), ]) %>% setnames(., "cases", "filled_case_notifications")
    final_cases_q <- collapse_point(combined_case_draws) %>% .[, .(location_id, year_id, mean)] %>% setnames(., "mean", "final_cases")
    gbd_2016 <- fread(file.path(home, "models/06.09.17/02_case_predictions_fixed_for_rescaling.csv")) %>% collapse_point %>%
      .[, .(location_id, year_id, mean)] %>% setnames(., "mean", "gbd_2016")
    # bring together
    save_cases_to_plot <- merge(modeled_draws_q, notifs, by=c("location_id", "year_id"), all.x=TRUE)
    save_cases_to_plot <- merge(save_cases_to_plot, final_cases_q, by=c("location_id", "year_id"), all.x=TRUE)
    save_cases_to_plot <- merge(save_cases_to_plot, gbd_2016, by=c("location_id", "year_id"), all.x=TRUE)
    save_cases_to_plot <- merge(save_cases_to_plot, shocks_difference, by=c("location_id", "year_id"), all.x=TRUE)
    # add location tags
    save_cases_to_plot[location_id %in% trusted_notifications, location_source := "final model using filled case notifications"]
    save_cases_to_plot[location_id %in% untrusted_locations, location_source := "final model using shocks adjustment of case notifications"]
    save_cases_to_plot[!location_id %in% c(untrusted_locations, trusted_notifications), location_source := "no case notifications available"]
    write.csv(save_cases_to_plot, file.path(j.version.dir.logs, "diagnostic_case_notifs_and_shocks.csv"), row.names=FALSE)

    ### plot case model diagnostics
    source(FILEPATH"diagnostics.r", echo=TRUE)
  }

}

rm(draws_nonfatal)
gc(TRUE)
#***********************************************************************************************************************


########################################################################################################################
##### PART TWO: RESCALE CASES ##########################################################################################
########################################################################################################################


#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescaling function
rescaled_cases <- rake(combined_case_draws)

gc(T)

### save results
if (WRITE_FILES == "yes") {
  fwrite(rescaled_cases, FILEPATH "03_case_predictions_rescaled.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


if (CALCULATE_NONFATAL == "yes") {
  ########################################################################################################################
  ##### PART THREE: CASES AGE-SEX SPLIT ##################################################################################
  ########################################################################################################################


  #----SPLIT--------------------------------------------------------------------------------------------------------------
  ### split measles cases by age/sex pattern from CoD database
  split_cases <- age_sex_split(acause=acause, input_file=rescaled_cases, measure="case")

  ### save results
  if (WRITE_FILES == "yes") {
    fwrite(split_cases, FILEPATH "04_case_predictions_split.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************

  ########################################################################################################################
  ##### PART FOUR: CASES TO PREVALENCE AND INCIDENCE #####################################################################
  ########################################################################################################################


  #----PREVALENCE AND INCIDENCE-------------------------------------------------------------------------------------------
  ### convert cases to prevalence, assuming duration of 10 days, and incidence
  # prep data
  predictions_prev <- copy(split_cases) %>% .[, measure_id := 5]
  predictions_inc <- copy(split_cases) %>% .[, measure_id := 6]
  rm(split_cases)

  # calculate prevalence
  invisible( predictions_prev[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) (get(paste0("split_case_draw_", x)) * (10 / 365)) / population )] )
  predictions_prev <- predictions_prev[, c(draw_cols_upload, "measure_id"), with=FALSE]
  # calculate incidence
  invisible( predictions_inc[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("split_case_draw_", x)) / population )] )
  predictions_inc <- predictions_inc[, c(draw_cols_upload, "measure_id"), with=FALSE]
  #***********************************************************************************************************************

  ########################################################################################################################
  ##### PART FIVE: FORMAT FOR COMO #######################################################################################
  ########################################################################################################################


  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### format for como, (prevalence measure_id==5, incidence measure_id==6)
  predictions <- rbind(predictions_prev, predictions_inc)
  predictions <- predictions[age_group_id %in% c(age_start:age_end), ]

  rm(predictions_prev)
  rm(predictions_inc)

  # Garbage collect
  gc(TRUE)


  ### save to /share directory
  system.time(lapply(unique(predictions$location_id), function(x) fwrite(predictions[location_id==x, ],
                                                                         FILEPATH, row.names=FALSE)))
  print(paste0("nonfatal estimates saved in ", cl.version.dir))

  rm(predictions)
  gc(T)

  ### save measles nonfatal draws to the database
  job_nf <- paste0("qsub -N s_epi_", acause, " -l m_mem_free=100G -l fthread=5 -l archive -l h_rt=8:00:00 -P ", cluster_proj, " -q all.q -o "FILEPATH"",
                   username, " -e "FILEPATH"", username,
                   " "FILEPATH"save_results_wrapper.r",
                   " --args",
                   " --type epi",
                   " --me_id ", me_id,
                   " --input_directory ", cl.version.dir,
                   " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                   " --best ", mark_model_best)
  if (decomp) job_nf <- paste0(job_nf, " --decomp_step ", decomp_step)
  system(job_nf)
  print(job_nf)
  #***********************************************************************************************************************
}


if (CALCULATE_COD == "yes") {
  ########################################################################################################################
  ##### PART Six: MODEL CFR ##############################################################################################
  ########################################################################################################################


  #----PREP---------------------------------------------------------------------------------------------------------------
  ### function: prep covariates (for use later)
  prep_covariates <- function(...) {

    ### get covariates
    # Age-standardized SEV for Child underweight, covariate id 1230
    if (decomp) {
      mal_cov <- get_covariate_estimates(covariate_id=1230, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=decomp_step)
    } else {
      mal_cov <- get_covariate_estimates(covariate_id=1230, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
    }
    mal_cov[, ln_mal := log(mean_value)] 
    setnames(mal_cov, "mean_value", "mal")
    # save model version
    cat(paste0("Covariate malnutrition, covariate id 1230 (CoD) - model version ", unique(mal_cov$model_version_id)), file=FILEPATH "input_model_version_ids.txt", sep="\n", append=TRUE)
    # remove excess columns
    covariates <- mal_cov[, c("location_id", "year_id", "mal", "ln_mal"), with=FALSE]


    # LDI, covariate_name_short="LDI_pc"
    if (decomp) {
      ldi_cov <- get_covariate_estimates(covariate_id=57, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=decomp_step)
    } else {
      ldi_cov <- get_covariate_estimates(covariate_id=57, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
    }
    ldi_cov[, ln_LDI := log(mean_value)]
    setnames(ldi_cov, "mean_value", "LDI")
    # save model version
    cat(paste0("Covariate LDI (CoD) - model version ", unique(ldi_cov$model_version_id)), file=file.path(FILEPATH, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    ldi_cov <- ldi_cov[, c("location_id", "year_id", "ln_LDI", "LDI"), with=FALSE]
    covariates <- merge(ldi_cov, covariates, by=c("location_id", "year_id"), all.x=TRUE)

    # healthcare access and quality index, covariate_name_short="haqi"
    if (decomp) {
      haqi <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=decomp_step)
    } else {
      haqi <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
    }
    haqi[, HAQI := mean_value / 100] #setnames(haqi, "mean_value", "HAQI")
    haqi[, ln_HAQI := log(HAQI)]
    # save model version
    cat(paste0("Covariate HAQ (CoD) - model version ", unique(haqi$model_version_id)), file=file.path(FILEPATH, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    haqi <- haqi[, c("location_id", "year_id", "HAQI", "ln_HAQI"), with=FALSE]
    covariates <- merge(haqi, covariates, by=c("location_id", "year_id"), all.x=TRUE)

    # socio-demographic index, covariate_name_short="sdi"
    if (decomp) {
      sdi <- get_covariate_estimates(covariate_id=881, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=decomp_step)
    } else {
      sdi <- get_covariate_estimates(covariate_id=881, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
    }
    # save model version
    cat(paste0("Covariate SDI (CoD) - model version ", unique(sdi$model_version_id)), file=file.path(FILEPATH, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    setnames(sdi, "mean_value", "SDI")
    sdi <- sdi[, c("location_id", "year_id", "SDI"), with=FALSE]
    covariates <- merge(sdi, covariates, by=c("location_id", "year_id"), all.x=TRUE)

    return(covariates)

  }

  ### function: prep CoD regression
  prep_cod <- function(add_vital_registration=TRUE) {

    ### get mortality envelope
    if (decomp) {
      mortality_envelope <- get_envelope(location_id=pop_locs, year_id=1980:year_end, age_group_id=22, sex_id=1:2, gbd_round_id=gbd_round, decomp_step=decomp_step)
    } else {
      mortality_envelope <- get_envelope(location_id=pop_locs, year_id=1980:year_end, age_group_id=22, sex_id=1:2, gbd_round_id=gbd_round)
    }
    setnames(mortality_envelope, "mean", "mortality_envelope")

    ### get CoD data
    # download raw (uncorrected) COD data from the database
    if (add_vital_registration) {
      if (decomp) {
        cod <- get_cod_data(cause_id=cause_id, decomp_step=decomp_step)
      } else {
        cod <- get_cod_data(cause_id=cause_id)
      }
      setnames(cod, "year", "year_id")
      setnames(cod, "sex", "sex_id")  
      # save model version
      cat(paste0("CoD data - version ", unique(cod$description)),
          file=file.path(FILEPATH, "input_model_version_ids.txt"), sep="\n", append=TRUE)
      cod <- cod[acause=="measles" & age_group_id %in% 22 & !is.na(cf_corr) & sample_size != 0, ] 
      cod <- merge(cod, mortality_envelope[, .(location_id, year_id, age_group_id, sex_id, mortality_envelope)], by=c("location_id", "year_id", "age_group_id", "sex_id"))
      cod[, deaths := cf_corr * mortality_envelope]
      cod_sum <- cod[, .(deaths=sum(deaths), sample_size=sum(sample_size)), by=c("location_id", "year_id")]
      # merge on locations
      cod_sum <- merge(cod_sum, locations[, .(location_id, ihme_loc_id, super_region_id)], by="location_id", all.x=TRUE)

      ### get WHO notifications
      case_notif <- prep_case_notifs()
      WHO <- merge(case_notif, cod_sum[, .(location_id, year_id, deaths)], by=c("location_id", "year_id"))
      # remove instances with zero cases
      WHO <- WHO[cases > 0, ]
      # round deaths
      WHO[, deaths := round(deaths, 0)]
      # merge on locations
      WHO <- merge(WHO[, .(nid, location_id, year_id, cases)], locations[, .(location_id, ihme_loc_id, super_region_id)], by="location_id", all.x=TRUE)
      WHO <- WHO[super_region_id %in% c(64, 31, 103) & !is.na(year_id), .(ihme_loc_id, year_id, deaths, cases, nid)]
      WHO[, source := "implied_cfr"]

      ### just keep 4 and 5 star countries (defined by "well-defined" vital registration data)
      cod_loc_include <- file.path(FILEPATH) %>% fread
      print(paste0("dropping locations in SR 64, 31, and 103 without well-defined vital registration: ", paste(WHO[!ihme_loc_id %in% cod_loc_include$ihme_loc_id, ihme_loc_id] %>% unique, collapse=", ")))
      WHO <- WHO[ihme_loc_id %in% cod_loc_include$ihme_loc_id]
    }

    ### get CFR literature data
    cfr_lit <- read_excel(FILEPATH), sheet="extraction") %>% as.data.table 
    cfr_lit[, hospital := hospital %>% as.integer]
    cfr_lit[, rural := rural %>% as.integer]
    cfr_lit[, outbreak := outbreak %>% as.integer]
    setnames(cfr_lit, c("cases", "sample_size"), c("deaths", "cases"))
    cfr_lit[is.na(midpointyear), midpointyear := floor((year_start + year_end) / 2)]
    cfr_lit[is.na(group_review), group_review := 0]
    cfr_lit[is.na(is_outlier), is_outlier := 0]
    cfr_lit[is.na(ignore), ignore := 0]
    cfr_lit <- cfr_lit[group_review != 1 & ignore != 1, .(nid, ihme_loc_id, year_start, year_end, midpointyear, age_start, age_end, cases, deaths, hospital, outbreak, rural, ignore, group_review, is_outlier)] %>% unique
    setnames(cfr_lit, "midpointyear", "year_id")
    cfr_lit[, source := "extracted_cfr"]

    ### bring together all CFR data
    if (add_vital_registration) all_cfr <- rbind(cfr_lit, WHO, fill=TRUE) else all_cfr <- copy(cfr_lit)
    all_cfr[ihme_loc_id=="HKG", "ihme_loc_id" := "CHN_354"]
    all_cfr <- merge(all_cfr, locations[, .(location_id, ihme_loc_id, super_region_id)], by="ihme_loc_id", all.x=TRUE)

    ### drop outliers
    all_cfr[, cfr := deaths / cases]
    all_cfr[is.na(is_outlier), is_outlier := 0]
    all_cfr[(ihme_loc_id=="BEL" & year_start==2009) |
              (ihme_loc_id=="BEL" & year_start==2010) |
              (ihme_loc_id=="GNB" & year_start==1979 & year_end==1982 & age_start==0) |
              cfr > 0.1 |  
              (cfr > 0.025 & super_region_id==64) |
              (ihme_loc_id=="THA" & year_start==1984) |
              (ihme_loc_id=="ETH" & year_start==1981) |
              (ihme_loc_id=="IND" & year_start==1991) |
              (ihme_loc_id=="IND" & year_start==1992 & cfr > 0.15) |
              (ihme_loc_id=="GNB" & year_start==1979) |
              (ihme_loc_id=="SEN" & year_start==1983) |
              (ihme_loc_id=="SEN" & year_start==1985),
            is_outlier := 1]
    all_cfr <- all_cfr[is_outlier != 1, ]

    ### fix, merge on covariates
    all_cfr[is.na(hospital), hospital := 0]
    all_cfr[is.na(outbreak), outbreak := 0]
    all_cfr[is.na(rural), rural := 0]
    all_cfr <- unique(all_cfr[, .(nid, location_id, ihme_loc_id, year_id, age_start, age_end, cases, deaths, hospital, outbreak, rural, source)])
    all_cfr <- all_cfr[year_id >= 1980, ] 
    all_cfr <- merge(all_cfr, covariates, by=c("location_id", "year_id"), all.x=TRUE)
    all_cfr[, ihme_loc_id := as.factor(ihme_loc_id)]

    return(all_cfr)

  }

  ### prep CoD regression
  covariates <- prep_covariates()
  all_cfr <- prep_cod(add_vital_registration=include_VR_data_in_CFR)
  all_cfr <- all_cfr[location_id %in% standard_locations]

  ### save regression input
  fwrite(all_cfr, FILEPATH"cfr_regression_input.csv"), row.names=FALSE)
  #***********************************************************************************************************************


  #----MODEL CFR----------------------------------------------------------------------------------------------------------
  ### set theta using output from GBD2015
  theta <- exp(1 / -.3633593)  

  ### mixed effects neg binomial regression
  ### model specifications
  # covariates
  if (which_covariate=="only_HAQI") covar1 <- "HAQI"
  if (which_covariate=="only_SDI") covar1 <- "SDI"
  if (which_covariate=="only_LDI") covar1 <- "LDI"
  if (which_covariate=="only_ln_LDI") covar1 <- "ln_LDI"
  if (which_covariate=="use_HAQI") { covar1 <- "ln_mal"; covar2 <- "HAQI" }
  if (which_covariate=="use_LDI") { covar1 <- "HAQI"; covar2 <- "ln_LDI" }
  # random effects
  res <- as.formula(~ 1 | ihme_loc_id)
  # formula
  if (exists("covar2")) formula <- paste0("deaths ~ ", covar1, " + ", covar2, " + hospital + outbreak + rural + offset(log(cases))") else
    formula <- paste0("deaths ~ ", covar1, " + hospital + outbreak + rural + offset(log(cases))")
  cfr_model <- MASS::glmmPQL(as.formula(formula),
                             random=res,
                             family=negative.binomial(theta=theta, link=log),
                             data=all_cfr) 

  ### save log
  saveRDS(cfr_model, FILEPATH, "cfr_model_menegbin.rds")
  capture.output(summary(cfr_model), file=(FILEPATH, "log_cfr_menegbin_summary.txt"), type="output")
  #***********************************************************************************************************************


  #----DRAWS--------------------------------------------------------------------------------------------------------------
  set.seed(0311)

  ### predict out for all country-year-age-sex
  pred_CFR <- merge(covariates, sum_pop_young, by=c("location_id", "year_id"), all.x=TRUE)
  pred_CFR <- merge(pred_CFR, locations[, .(location_id, ihme_loc_id, super_region_id, parent_id, location_type, level)],
                    by="location_id", all.x=TRUE)
  N <- nrow(pred_CFR)

  ### calculate location and super region random effects
  reffect_cfr <- ranef(cfr_model)[1] %>% data.frame
  colnames(reffect_cfr) <- "RE_loc"
  reffect_cfr$ihme_loc_id <- gsub(".*/", "", rownames(reffect_cfr))
  reffect_cfr <- reffect_cfr[, c("ihme_loc_id", "RE_loc")]
  # collapse super region / region REs
  reffect_cfr <- merge(reffect_cfr, locations[, .(ihme_loc_id, super_region_id, region_id)], by="ihme_loc_id", all.x=TRUE) %>% data.table
  super_region_re <- ddply(reffect_cfr, c("super_region_id"), summarise, RE_sr=mean(RE_loc) ) %>% data.table
  region_re <- ddply(reffect_cfr, c("region_id"), summarise, RE_reg=mean(RE_loc) ) %>% data.table
  reffect_cfr[, c("super_region_id", "region_id") := NULL]
  # create full dataset of location reffects
  reffect_cfr <- rbind(reffect_cfr, data.table(ihme_loc_id=pred_CFR[, ihme_loc_id] %>% unique %>% .[!. %in% reffect_cfr$ihme_loc_id]), fill=TRUE)
  reffect_cfr <- merge(reffect_cfr, locations[, .(location_id, ihme_loc_id, super_region_id, region_id, parent_id, location_type, level)], by="ihme_loc_id", all.x=TRUE)
  # if national location is missing random effect, fill with region RE
  reffect_cfr <- merge(reffect_cfr, region_re, by="region_id", all.x=TRUE)
  reffect_cfr[is.na(RE_loc) & level==3, RE_loc := RE_reg]
  # if national location is still missing random effect, fill with super region RE
  reffect_cfr <- merge(reffect_cfr, super_region_re, by="super_region_id", all.x=TRUE)
  reffect_cfr[is.na(RE_loc) & level==3, RE_loc := RE_sr]
  # keep just needed cols
  reffect_cfr <- reffect_cfr[, .(location_id, RE_loc, level, parent_id)]

  ### add on same RE as parent for subnationals
  for (LEVEL in 4:max(locations$level)) {
    reffect_subnats <- reffect_cfr[, .(location_id, RE_loc)] %>% setnames(., c("location_id", "RE_loc"), c("parent_id", "RE_parent"))
    reffect_cfr <- merge(reffect_cfr, reffect_subnats, by="parent_id", all.x=TRUE)
    reffect_cfr <- reffect_cfr[level==LEVEL & is.na(RE_loc), RE_loc := RE_parent]
    reffect_cfr <- reffect_cfr[, .(location_id, RE_loc, level, parent_id)]
  }

  ### merge on to prediction dataset
  pred_CFR <- merge(pred_CFR, reffect_cfr[, .(location_id, RE_loc)], by="location_id", all.x=TRUE)

  ### 1000 draws for uncertainty
  # keep only intercept and ln_mal coefficient
  # coefficient matrix
  coefmat <- c(fixef(cfr_model)) 
  names(coefmat)[1] <- "constant"
  names(coefmat) <- paste("b", names(coefmat), sep = "_")
  coefmat <- matrix(unlist(coefmat), ncol=length(coefmat), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
  if (!exists("covar2")) length <- 2 else if (exists("covar2")) length <- 3
  coefmat <- coefmat[1, 1:length]

  # covariance matrix
  vcovmat <- vcov(cfr_model) 
  vcovmat <- vcovmat[1:length, 1:length]

  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
  betas <- t(betadraws)


  ### estimate deaths at the draw level
  cfr_draw_cols <- paste0("cfr_draw_", draw_nums_gbd)
  # generate draws of the prediction using coefficient draws
  if (exists("covar2")) { pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                                                 ( betas[2, (i + 1)] * get(covar1) ) +
                                                                                                 ( betas[3, (i + 1)] * get(covar2) ) +
                                                                                                 RE_loc ) )] }
  if (!exists("covar2")) { pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                                                  ( betas[2, (i + 1)] * get(covar1) ) +
                                                                                                  RE_loc ) )] }

  ### save results
  CFR_draws_save <- pred_CFR[, c("location_id", "year_id", cfr_draw_cols), with=FALSE]
  if (WRITE_FILES == "yes") {
    fwrite(CFR_draws_save, file.path(FILEPATH,("05_cfr_model_draws.csv")), row.names=FALSE)
  }

  if (late_2019) {
    CFR_draws_save <- fread(""FILEPATH"05_cfr_model_draws.csv")
    fwrite(CFR_draws_save, file.path(FILEPATH, ("05_cfr_model_draws.csv")), row.names=FALSE)
  }
  #***********************************************************************************************************************


  ########################################################################################################################
  ##### PART SEVEN: DEATHS ###############################################################################################
  ########################################################################################################################


  #----CALCULATE DEATHS---------------------------------------------------------------------------------------------------
  ### merge together data
  predictions_deaths <- merge(rescaled_cases, CFR_draws_save, by=c("location_id", "year_id"))

  ### calculate deaths from cases and CFR
  death_draw_cols <- paste0("death_draw_", draw_nums_gbd)
  predictions_deaths[, (death_draw_cols) := lapply(draw_nums_gbd, function(ii) get(paste0("cfr_draw_", ii)) * get(paste0("case_draw_", ii)) )]

  ### save results
  predictions_deaths_save <- predictions_deaths[, c("location_id", "year_id", death_draw_cols), with=FALSE]
  if (WRITE_FILES == "yes") {
    fwrite(predictions_deaths_save, file.path(FILEPATH, "06_death_predictions_from_model.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************


  ########################################################################################################################
  ##### PART EIGHT: RESCALE DEATHS #######################################################################################
  ########################################################################################################################


  #----RESCALE------------------------------------------------------------------------------------------------------------
  ### run custom rescale function
  invisible( lapply(death_draw_cols, function(x) predictions_deaths_save[location_id %in% locations[level > 3, location_id] & get(x)==0, (x) := 1e-10]) )
  rescaled_deaths <- rake(predictions_deaths_save, measure="death_draw_")

  ### save results
  if (WRITE_FILES == "yes") {
    fwrite(rescaled_deaths, file.path(FILEPATH), row.names=FALSE)
  }
  #***********************************************************************************************************************


  ########################################################################################################################
  ##### PART NINE: SPLIT DEATHS ##########################################################################################
  ########################################################################################################################


  #----SPLIT--------------------------------------------------------------------------------------------------------------
  ### split using age pattern from cause of death data
  split_deaths <- age_sex_split(acause=acause, input_file=rescaled_deaths, measure="death")
  #***********************************************************************************************************************


  #----SAVE---------------------------------------------------------------------------------------------------------------
  ### save split draws
  split_deaths_save <- split_deaths[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("split_death_draw_", draw_nums_gbd)), with=FALSE]
  rm(split_deaths)
  gc(T)


  if (WRITE_FILES == "yes") {
    fwrite(split_deaths_save, file.path(FILEPATH), row.names=FALSE)
  }
  #***********************************************************************************************************************
  

  ########################################################################################################################
  ##### PART TEN: COMBINE CODEm DEATHS ##################################################################################
  ########################################################################################################################
  
  
  #----COMBINE------------------------------------------------------------------------------------------------------------
  ### read CODEm COD results for data-rich countries
  cod_M <- data.table(pandas$read_hdf(file.path(FILEPATH)), key="data"))
  cod_F <- data.table(pandas$read_hdf(file.path(FILEPATH)), key="data"))
  
  # save model version
  cat(paste0("Data-rich CODEm feeder model (males) - model version ", male_CODEm_version),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  cat(paste0("Data-rich CODEm feeder model (females) - model version ", female_CODEm_version),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  # combine M/F CODEm results
  cod_DR <- rbind(cod_M, cod_F, fill=TRUE)
  rm(cod_M)
  rm(cod_F)
  gc(T)
  
  cod_DR <- cod_DR[, draw_cols_upload, with=FALSE]
  
  # use model for USA and MEX
  MEX_USA_subnats <- locations[parent_id %in% c(102, 130), location_id]
  cod_DR <- cod_DR[!location_id %in% c(102, 130, MEX_USA_subnats), ]
  
  # hybridize data-rich and custom models
  data_rich <- unique(cod_DR$location_id)
  split_deaths_glb <- split_deaths_save[!location_id %in% data_rich, ]
  colnames(split_deaths_glb) <- draw_cols_upload
  split_deaths_hyb <- rbind(split_deaths_glb, cod_DR)
  
  # keep only needed age groups
  split_deaths_hyb <- split_deaths_hyb[age_group_id %in% c(age_start:age_end), ]
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART TEN POINT FIVE (NEW Step 4 GBD 2019): FORCE DEATHS TO 0 WHERE TRUSTED CASE NOTIFS 0 #########################
  ########################################################################################################################
  
  
  #----FORCE CODEM OR CALCULATED DEATHS TO 0------------------------------------------------------------------------------
  # Read in the smothed, trusted adjusted case notifications  
  adjusted_notifications_q <- lapply(list.files(FILEPATH, full.names=TRUE), fread) %>%
    rbindlist %>% .[, .(location_id, year_id, combined)] %>% setnames(., "combined", "cases")
  
if (late_2019) {
    annualized_cases <- fread(FILEPATH)
    setnames(annualized_cases, "annualized_report", "cases")
    replacements <- unique(annualized_cases$location_id)
    removed_to_replace <- adjusted_notifications_q[!(location_id %in% replacements & year_id==2019), ]
    adjusted_notifications_q <- rbind(removed_to_replace, annualized_cases[, .(location_id, year_id, cases)])
    
  }

  # subset down to trusted case notification super region locations
  trust_locs <- adjusted_notifications_q[location_id %in% unique(locations[super_region_id %in% c(31, 64, 103), location_id])]
  trust_locs <- merge(trust_locs, locations[, c("location_id", "ihme_loc_id", "parent_id", "level")], all.x=TRUE)
  
  # Subset to just location-years where cases are 0
  zero_cases <- trust_locs[cases==0]
  
  split_deaths_hyb <- merge(split_deaths_hyb, zero_cases, by=c("location_id", "year_id"), all.x = TRUE)
  ## cases either 0 or NA, so if cases 0 deaths must be 0
  cols <- grep("draw_", names(split_deaths_hyb), value=TRUE)
  split_deaths_hyb <- split_deaths_hyb[cases==0, (cols) := 0]
  split_deaths_hyb <- split_deaths_hyb[, cases := NULL]
  
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART ELEVEN: FORMAT FOR CODCORRECT ###############################################################################
  ########################################################################################################################
  
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### format for codcorrect
  split_deaths_hyb[, measure_id := 1]
  
  ### save to share directory for upload
  lapply(unique(split_deaths_hyb$location_id), function(x) write.csv(split_deaths_hyb[location_id==x, ],
                                                                     file.path(FILEPATH), row.names=FALSE))
  print(paste0("death draws saved in ", cl.death.dir))
  
  ### save_results
  job <- paste0("qsub -N s_cod_", acause, " -l m_mem_free=100G -l fthread=5 -l archive -l h_rt=8:00:00 -P proj_custom_models -q all.q -o "FILEPATH"",
                username, " -e "FILEPATH"", username,
                " "FILEPATH"save_results_wrapper.r",
                " --args",
                " --type cod",
                " --me_id ", cause_id,
                " --input_directory ", cl.death.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", mark_model_best)
  if (decomp) job <- paste0(job, " --decomp_step ", decomp_step)
  system(job)
  print(job)
  #***********************************************************************************************************************
}
