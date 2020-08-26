#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: "USERNAME"
# Purpose: Model pertussis fatal and nonfatal outcomes for GBD
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
pacman::p_load(magrittr, foreign, stats, MASS, data.table, plyr, dplyr, lme4, parallel, reticulate) 
if (Sys.info()["sysname"] == "Linux") {
  library(rhdf5)   
  library(mvtnorm) 
} else { 
  pacman::p_load(mvtnorm, rhdf5)
}

pandas <- import("pandas")
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "whooping"
age_start <- 4             ##
age_end   <- 16            ## age 55-59 years
a         <- 3             ## birth cohort years before 1980
cause_id  <- 339
me_id     <- 1424
gbd_round <- 6
year_end  <- if (gbd_round %in% c(3, 4, 5)) gbd_round + 2012 else if (gbd_round==6) 2019

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_gbd    <- paste0("draw_", draw_nums_gbd)
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
# mortality
cl.death.dir <- file.path(""FILEPATH"","draws") 
if (!dir.exists(cl.death.dir)) dir.create(cl.death.dir, recursive=TRUE)
# nonfatal
cl.version.dir <- file.path(""FILEPATH"", "draws")
if (!dir.exists(cl.version.dir)) dir.create(file.path(cl.version.dir), recursive=TRUE)

### directories
home <- file.path(j_root, ""FILEPATH"")
j.version.dir <- file.path(home, "models", custom_version)
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
if (!dir.exists(j.version.dir.inputs)) dir.create(j.version.dir.inputs, recursive=TRUE)
if (!dir.exists(j.version.dir.logs)) dir.create(j.version.dir.logs, recursive=TRUE)

### save description of model run
# which model types are being launched?
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
### reading .h5 files
""FILEPATH"read_hdf5_table.R" %>% source
""FILEPATH"collapse_point.R" %>% source

### load shared functions
source(""FILEPATH"get_population.R")
source(""FILEPATH"get_location_metadata.R")
source(""FILEPATH"get_covariate_estimates.R") 
source(""FILEPATH"get_envelope.R") 

### load personal functions
""FILEPATH"read_excel.R" %>% source
""FILEPATH"rake.R" %>% source
""FILEPATH"collapse_point.R" %>% source
file.path(j_root, ""FILEPATH""FILEPATH"/age_sex_split.R") %>% source
#***********************************************************************************************************************


########################################################################################################################
##### PART ONE: INCIDENCE NAT HIST MODEL ###############################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22)[, 
                                                                               .(location_id, ihme_loc_id, location_name, location_ascii_name, region_id, super_region_id, level, location_type, parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])
standard_locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=101)$location_id %>% unique

### get population for age less than 1 year
if (decomp) {
  population <- get_population(location_id=pop_locs, year_id=(1980-a):year_end, age_group_id=c(2:20, 30:32, 235), status="best", 
                               sex_id=1:3, gbd_round_id=gbd_round, decomp_step=decomp_step)
} else {
  population <- get_population(location_id=pop_locs, year_id=(1980-a):year_end, age_group_id=c(2:20, 30:32, 235), status="best", 
                               sex_id=1:3, gbd_round_id=gbd_round)
}
population <- merge(population, locations[, .(location_id, ihme_loc_id)], by="location_id", all.x=TRUE)
# save model version
cat(paste0("Population - model run ", unique(population$run_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

# collapse by location year
sum_pop <- population[age_group_id %in% 2:4 & sex_id==3, ] %>% .[, pop := sum(population), 
                                                                 by=c("ihme_loc_id", "location_id", "year_id")] %>% .[, .(ihme_loc_id, location_id, year_id, pop)] %>% unique
sum_pop[, year_id := year_id + a]
sum_pop <- sum_pop[year_id %in% 1980:year_end, ]

### get covariate
# covariate: DTP3_coverage_prop, covariate_id=32
if (use_lagged_covs) {
  covar <- get_covariate_estimates(covariate_id=2308, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
} else {
  if (decomp) {
    covar <- get_covariate_estimates(covariate_id=32, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
  } else {
    covar <- get_covariate_estimates(covariate_id=32, year_id=1980:year_end, location_id=pop_locs)[, .(location_id, year_id, mean_value, model_version_id)]
  }
}
setnames(covar, "mean_value", "DTP3")
# save model version
cat(paste0("Covariate DTP3 (NF) - model version ", unique(covar$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
covar[, model_version_id := NULL]

### prep incidence (case notification) data
prep_case_notifs <- function(cause="Pertussis") {

  # get WHO case notification data, downloaded from "ADDRESS"
  case_notif <- read_excel(file.path(j_root, ""FILEPATH""FILEPATH, "incidence_series.xls"), sheet=cause) %>% data.table
  case_notif[, c("WHO_REGION", "Cname", "Disease") := NULL]
  setnames(case_notif, "ISO_code", "ihme_loc_id")
  case_notif <- melt(case_notif, variable.name="year_id", value.name="cases")
  case_notif[, year_id := year_id %>% as.character %>% as.integer]
  case_notif <- merge(case_notif, locations[, .(location_id, ihme_loc_id)], by="ihme_loc_id", all.x=TRUE)
  case_notif[, nid := 83133]

  # drop unmapped locations
  drop.locs <- case_notif[!(ihme_loc_id %in% locations$ihme_loc_id)]$ihme_loc_id %>% unique
  if (length(drop.locs) > 0 ) {
    print(paste0("UNMAPPED LOCATIONS (DROPPING): ", toString(drop.locs)))
    case_notif <- case_notif[!(ihme_loc_id %in% drop.locs)]
  }

  # remove missing rows
  case_notif <- case_notif[!is.na(cases), .(nid, location_id, ihme_loc_id, year_id, cases)]

  return(case_notif)

}

### function to prep literature / expert data
prep_regression <- function(...) {

  # prep US territories
  US_terr <- fread(file.path(home, "data", "US_territories.csv"))
  US_terr <- US_terr[, c("ihme_loc_id", "year_start", "cases"), with=FALSE]
  setnames(US_terr, "year_start", "year_id")
  US_terr <- na.omit(US_terr)
  US_terr[, nid := 207924]

  ### prep pertussis historical data, converted to .csv 
  UK <- fread(file.path(home, "data", "UK_Aust_JPN_population_for_agesexsplit.csv"))[iso3=="XEW"]
  setnames(UK, c("iso3", "year"), c("ihme_loc_id", "year_id"))
  UK[ihme_loc_id=="XEW", ihme_loc_id := "GBR"]
  # collapse UK pop by year
  UK[, ukpop := sum(pop2) * 100000, by=c("ihme_loc_id", "year_id")]
  UK_pop <- UK[, .(ihme_loc_id, year_id, ukpop)] %>% unique
  UK_pop[, year_id := year_id + a]

  # pull together all historical data files
  pne <- fread(file.path(j_root, ""FILEPATH"/Pertussis_Notification_Extraction.csv"))[iso3=="XEW"]
  names(pne) <- tolower(names(pne))
  setnames(pne, c("iso3", "year"), c("ihme_loc_id", "year_id"))
  pne[, notifications := as.integer(notifications)]
  # GBR 1940+
  pne1940 <- read.dta(file.path(j_root, ""FILEPATH"/DTP3_pre_1940.dta")) %>% as.data.table
  setnames(pne1940, c("iso3", "year"), c("ihme_loc_id", "year_id"))

  # replace missing cases with notification data
  historical <- bind_rows(pne, pne1940)[, nid := 261647]
  historical <- bind_rows(historical, cases)
  historical[is.na(cases), cases := notifications]
  historical <- historical[, c("year_id", "cases", "ihme_loc_id", "vacc_rate", "nid"), with=FALSE]
  historical[ihme_loc_id=="XEW", ihme_loc_id := "GBR"]
  historical[year_id <= 1923, vacc_rate := 0]
  historical <- bind_rows(historical, US_terr)
  fwrite(historical, file.path(j.version.dir.inputs, paste0("incidence_data_inputs.csv")), row.names=FALSE)

  # prep inputs for regression
  # collapse duplicates
  regr <- historical[, .(cases=max(cases, na.rm=TRUE), vacc_rate=min(vacc_rate, na.rm=TRUE)), by=c("ihme_loc_id", "year_id", "nid")]
  # add on populations
  regr <- merge(regr, sum_pop[, c("ihme_loc_id", "year_id", "location_id", "pop")], by=c("ihme_loc_id", "year_id"), all.x=TRUE)
  regr <- merge(regr, UK_pop, by=c("ihme_loc_id", "year_id"), all.x=TRUE)
  regr[is.na(pop), pop := ukpop]
  # fix Great Britain
  regr[ihme_loc_id=="GBR", location_id := 95]
  if (nrow(regr[is.na(pop)]) > 0) print(paste0("dropping rows with missing population data (", nrow(regr[is.na(pop)]), ")"))
  regr <- regr[!is.na(pop)]
  regr[is.infinite(vacc_rate), vacc_rate := NA]
  # add on vaccine coverage
  regr <- merge(regr, covar, by=c("location_id", "year_id"), all.x=TRUE)
  regr[is.na(vacc_rate), vacc_rate := DTP3]
  if (nrow(regr[is.na(vacc_rate)]) > 0) print(paste0("dropping rows with missing vaccine coverage data (", nrow(regr[is.na(vacc_rate)]), ")"))
  regr <- regr[!is.na(vacc_rate)]
  # convert to log incidence
  regr[, pop := round(pop, 0)]
  regr[, incidence := (cases / pop) * 100000]
  regr[, ln_inc := log(incidence)]
  regr[is.infinite(ln_inc), ln_inc := NA] 
  regr[, ln_vacc := log(vacc_rate)]
  regr[, ln_unvacc := log(1 - vacc_rate)]
  regr <- regr[incidence <= 20000, ]

  return(regr)

}

### pull case notifications
cases <- prep_case_notifs(cause="Pertussis")
cases <- cases[!year_id %in% c(1980, 1981), ]

### prepare nonfatal regression input
regr <- prep_regression()
regr <- regr[location_id %in% standard_locations]

# save input for reference
fwrite(regr, file.path(j.version.dir.inputs, paste0("incidence_regression_input.csv")), row.names=FALSE)
#***********************************************************************************************************************


#----MIXED EFFECTS MODEL------------------------------------------------------------------------------------------------
### run mixed effects regression model
me_model <- lmer(ln_inc ~ ln_unvacc + (1 | ihme_loc_id), data=regr)

# save log
capture.output(summary(me_model), file = file.path(j.version.dir.logs, "log_incidence_mereg.txt"), type="output")
#***********************************************************************************************************************


#----DRAWS--------------------------------------------------------------------------------------------------------------
set.seed(0311)

### prep data
draws <- merge(covar, sum_pop, by=c("location_id", "year_id"), all.x=TRUE)
draws[, ln_unvacc := log(1 - DTP3)]
N <- nrow(draws)

### 1000 draws for uncertainty
# prep random effects
reffect <- data.frame(ranef(me_model)[[1]])
# standard random effect set to Switzerland, where the pertussis monitoring system is thought to capture a large percentage of cases
if (CHE_RE=="gbd2017") {
  reffect_CHE <- 4.639529  
} else {
  reffect_CHE <- reffect["CHE", ]
}

# coefficient matrix
beta1 <- coef(me_model)[[1]] %>% data.frame
beta1 <- beta1["CHE", "ln_unvacc"]
beta0 <- fixef(me_model)[[1]] %>% data.frame
coeff <- cbind(beta0, beta1, reffect_CHE)
colnames(coeff) <- c("constant", "b_unvax", "b_reffect_CHE")
# remove RE term
coefmat <- coeff[, !colnames(coeff)=="b_reffect_CHE"]
coefmat <- matrix(unlist(coefmat), ncol=2, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))

# covariance matrix
vcovs <- vcov(me_model)
vcovlist <- c(vcovs[1,1], vcovs[1,2], vcovs[2,1], vcovs[2,2])
vcovmat <- matrix(vcovlist, ncol=2, byrow=TRUE)

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)

# add random effect of CHE to coefficient draws
betadraws <- cbind(betadraws, rep(reffect_CHE, 1000))

# transpose coefficient matrix
betas <- t(betadraws)

### generate draws of the prediction using coefficient draws
draw_nums <- 1:1000
draw_cols <- paste0("case_draw_", draw_nums_gbd)
invisible(
  draws[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                            ( betas[2, x] * ln_unvacc ) +
                                                            ( betas[3, x] ) ) *
      ( pop / 100000 ) } )]
)
#***********************************************************************************************************************


#----SAVE---------------------------------------------------------------------------------------------------------------
# save results
save_case_draws <- draws[, c("location_id", "year_id", draw_cols), with=FALSE]

if (WRITE_FILES == "yes") {
  write.csv(save_case_draws, file.path(j.version.dir, "01_case_predictions_from_model.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART TWO: RESCALE CASES ##########################################################################################
########################################################################################################################


#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescale function
rescaled_cases <- rake(save_case_draws, measure="case_draw_")

### save results
if (WRITE_FILES == "yes") {
  fwrite(rescaled_cases, file.path(j.version.dir, "02_case_predictions_rescaled.csv"), row.names=FALSE)
  # make collapsed file and save that, too, for location specific case vetting
  rescaled_cases_collapsed <- collapse_point(rescaled_cases, draws_name="case_draw")
  fwrite(rescaled_cases_collapsed, file.path(j.version.dir, "025_case_predictions_rescaled_collapsed.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


if (CALCULATE_NONFATAL == "yes") {


########################################################################################################################
##### PART THREE: CASES AGE-SEX SPLIT ##################################################################################
########################################################################################################################


#----SPLIT--------------------------------------------------------------------------------------------------------------
### split measles cases by age/sex pattern from CoD database
split_cases <- age_sex_split(acause=acause, input_file=rescaled_cases, measure="case")

### save split draws
if (WRITE_FILES == "yes") {
  fwrite(split_cases, file.path(j.version.dir, "03_case_predictions_split.csv"), row.names=FALSE)
  # make collapsed file and save that, too, for location/age/sex specific case vetting
  split_cases_collapsed <- collapse_point(split_cases[age_group_id %in% c(2, 3, 4, 5)], draws_name="split_case_draw")  
  fwrite(split_cases_collapsed, file.path(j.version.dir, "035_case_predictions_split_collapsed_u5.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART FOUR: CASES TO PREVALENCE AND INCIDENCE #####################################################################
########################################################################################################################


#----PREVALENCE---------------------------------------------------------------------------------------------------------
### convert cases to prevalence
invisible( split_cases[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) (get(paste0("split_case_draw_", x)) * (50 / 365)) / population )] )
predictions_prev_save <- split_cases[, c("location_id", "year_id", "age_group_id", "sex_id", draw_cols_gbd), with=FALSE]
#***********************************************************************************************************************


#----INCIDENCE---------------------------------------------------------------------------------------------------------
### convert cases to incidence rate
invisible( split_cases[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("split_case_draw_", x)) / population )] )
predictions_inc_save <- split_cases[, c("location_id", "year_id", "age_group_id", "sex_id", draw_cols_gbd), with=FALSE]
#***********************************************************************************************************************


########################################################################################################################
##### PART FIVE: FORMAT FOR COMO #######################################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format prevalence for como
# prevalence, measure_id==5
predictions_prev_save[, measure_id := 5]

### format incidence for como
# incidence, measure_id==6
predictions_inc_save[, measure_id := 6]

### save flat files for upload
save_nonfatal <- rbind(predictions_prev_save, predictions_inc_save)
save_nonfatal <- save_nonfatal[age_group_id %in% c(age_start:age_end), ]
invisible( lapply(unique(save_nonfatal$location_id), function(x) write.csv(save_nonfatal[location_id==x, ], file.path(cl.version.dir, paste0(x, ".csv")))) )
print(paste0("nonfatal estimates saved in ", cl.version.dir))

# save whooping incidence draws, modelable_entity_id=1424
# FAIR:
job_nf <- paste0("qsub -N s_epi_", acause, " -l m_mem_free=150G -l fthread=5 -l archive -l h_rt=8:00:00 -P ", cluster_proj, " -q all.q -o "FILEPATH"",
              username, " -e "FILEPATH"", username,
              " "FILEPATH"save_results_wrapper.r",
              " --args",
              " --type epi",
              " --me_id ", me_id,
              " --input_directory ", cl.version.dir,
              " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
              " --best ", mark_model_best)
if (decomp) job_nf <- paste0(job_nf, " --decomp_step ", decomp_step)
system(job_nf); print(job_nf)
#***********************************************************************************************************************

}


if (CALCULATE_COD == "yes") {
  
  ########################################################################################################################
  ##### PART SIX: MODEL CFR ##############################################################################################
  ########################################################################################################################


  #----PREP---------------------------------------------------------------------------------------------------------------
  ### function to prep covariates
  prep_covariates <- function(...) {

    # covariate: LDI_pc, covariate_id=57
    if (decomp) {
      ldi <- get_covariate_estimates(covariate_id=57, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=decomp_step)
    } else {
      ldi <- get_covariate_estimates(covariate_id=57, year_id=1980:year_end, location_id=pop_locs)
    }
    ldi[, ln_ldi := log(mean_value)]
    setnames(ldi, "mean_value", "ldi")
    # save model version
    cat(paste0("Covariate LDI (CoD) - model version ", unique(ldi$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

    # covariate: HSA, covariate_id=208, Maternal Care & Immunization covariate (MCI)
    if (decomp) hsa <- get_covariate_estimates(covariate_id=208, year_id=1980:year_end, location_id=pop_locs, decomp_step=decomp_step) else hsa <- get_covariate_estimates(covariate_id=208, year_id=1980:year_end, location_id=pop_locs) 
    setnames(hsa, "mean_value", "health")
    # save model version
    cat(paste0("Covariate HSA capped (CoD) - model version ", unique(hsa$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

    # Age-standardized SEV for Child underweight, covariate_id=1230
    if (decomp) mal_cov <- get_covariate_estimates(covariate_id=1230, year_id=1980:year_end, location_id=pop_locs, decomp_step=decomp_step) else mal_cov <- get_covariate_estimates(covariate_id=1230, year_id=1980:year_end, location_id=pop_locs)
    mal_cov[, ln_mal := log(mean_value)]
    # save model version
    cat(paste0("Covariate malnutrition 1230 (CoD) - model version ", unique(mal_cov$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

    # healthcare access and quality index, covariate_id=1099, covariate_name_short="haqi"
    if (decomp) haqi <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs, decomp_step=decomp_step) else haqi <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs) 
    setnames(haqi, "mean_value", "HAQI")
    # save model version
    cat(paste0("Covariate HAQI (CoD) - model version ", unique(haqi$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

    # merge covars together
    cfr_covar <- merge(hsa[, c("location_id", "year_id", "health"), with=FALSE], ldi[, c("location_id", "year_id", "ln_ldi", "ldi"), with=FALSE], by=c("location_id", "year_id"), all.x=TRUE)
    cfr_covar <- merge(cfr_covar, mal_cov[, c("location_id", "year_id", "ln_mal"), with=FALSE], by=c("location_id", "year_id"), all.x=TRUE)
    cfr_covar <- merge(cfr_covar, haqi[, c("location_id", "year_id", "HAQI"), with=FALSE], by=c("location_id", "year_id"), all.x=TRUE)

    return(cfr_covar)

  }

  ### function to prep CFR literature data
  prep_cfr_data <- function(...) {

    ### read in CFR data - GBD 2010 literature review
    cfr <- fread(file.path(home, "data/cfr_data_gbd2010.csv")) 
    # prep cfr
    setnames(cfr, c("NID", "Country ISO3 Code", "Parameter Value", "numerator_number_of_cases_with_the_condition", "effective_sample_size", "mid_point_year_of_data_collection", "Age Start", "Age End", "Year Start", "Year End"),
                  c("nid", "ihme_loc_id", "cfr", "deaths", "cases", "year_id", "age_start", "age_end", "year_start", "year_end"))
    # drop outliers
    cfr[, ignore := as.numeric(ignore)]
    cfr[is.na(ignore), ignore := 0]
    if (nrow(cfr[ignore==1, ]) > 0) print(paste0("dropping outliers (", nrow(cfr[ignore==1, ]), " rows)"))
    cfr <- cfr[ignore != 1, ]
    cfr <- cfr[!(ihme_loc_id=="UGA" & year_start==1951), ]
    # cleanup
    cfr[year_id < 1980, year_id := 1980]
    cfr <- cfr[, c("nid", "ihme_loc_id", "year_id", "age_start", "age_end", "deaths", "cases", "cfr"), with=FALSE]

    # prep cfr_expert - additional CFR data from experts - GBD 2013 update
    cfr_expert <- fread(file.path(FILEPATH, "cfr_data_gbd2013.csv")) 
    setnames(cfr_expert, c("iso3", "mean", "numerator", "denominator", "year_start"), c("ihme_loc_id", "cfr", "deaths", "cases", "year_id"))
    cfr_expert <- cfr_expert[, c("nid", "ihme_loc_id", "year_id", "age_start", "age_end", "deaths", "cases", "cfr"), with=FALSE]

    # add in new CFR data from 2016 literature review
    cfr_2016 <- fread(file.path(home, ""FILEPATH"/cfr_GBD2016_extraction.csv"))
    setnames(cfr_2016, c("cases", "sample_size"), c("deaths", "cases"))
    cfr_2016[, cfr := deaths / cases]
    cfr_2016[, year_id := round((year_start + year_end) / 2, 0)]
    cfr_2016 <- cfr_2016[, c("nid", "ihme_loc_id", "year_id", "age_start", "age_end", "deaths", "cases", "cfr"), with=FALSE]

    # merge CFR data
    cfr_data <- bind_rows(cfr, cfr_expert, cfr_2016)

    # merge on location_id
    cfr_data <- merge(cfr_data, locations[, c("location_id", "ihme_loc_id"), with=FALSE], by="ihme_loc_id", all.x=TRUE)

    ### add covariates
    cfr_data <- merge(cfr_data, cfr_covar, by=c("location_id", "year_id"), all.x=TRUE)

    # calculate CFR
    cfr_data[, cfr := NULL]
    cfr_data[, cfr := deaths / cases]
    cfr_data <- cfr_data[cfr <= 0.5, ]
    cfr_data[!is.na(cases) & cases < 1 & cases > 0, cases := 1]

    return(cfr_data)

  }

  ### get covariates
  cfr_covar <- prep_covariates()
  ### get CFR data
  cfr_data <- prep_cfr_data()
  ### regression using standard locations only
  cfr_data <- cfr_data[location_id %in% standard_locations]

  # save CFR regression inputs
  fwrite(cfr_data, file.path(j.version.dir.inputs, "cfr_regression_input.csv"), row.names=FALSE)
  #***********************************************************************************************************************


  #----CFR REGRESSION-----------------------------------------------------------------------------------------------------
  theta <- 1 / 1.666942
  cfr_data[, deaths := round(deaths, 0)]

  if (HAQI_or_HSA == "use_HSA") {

    nb_model <- glm.nb(deaths ~ ln_ldi + health + offset(log(cases)), data=cfr_data)
    # save log
    capture.output(summary(nb_model), file = file.path(j.version.dir.logs, "log_cfr_negbin.txt"), type="output")

  } else if (HAQI_or_HSA == "use_HAQI") {

    nb_model <- glm.nb(deaths ~ ln_ldi + HAQI + offset(log(cases)), data=cfr_data)
    # save log
    capture.output(summary(nb_model), file = file.path(j.version.dir.logs, "log_cfr_negbin.txt"), type="output")

  } else if (HAQI_or_HSA == "HAQI_only") {

    nb_model <- glm.nb(deaths ~ HAQI + offset(log(cases)), data=cfr_data)
    # save log
    capture.output(summary(nb_model), file = file.path(j.version.dir.logs, "log_cfr_negbin.txt"), type="output")

  } else if (HAQI_or_HSA == "LDI_only") {

    nb_model <- glm.nb(deaths ~ ln_ldi + offset(log(cases)), data=cfr_data)
    # save log
    capture.output(summary(nb_model), file = file.path(j.version.dir.logs, "log_cfr_negbin.txt"), type="output")

  }
  #***********************************************************************************************************************


  #----DRAWS--------------------------------------------------------------------------------------------------------------
  set.seed(0311)

  ### predict out for all country-year-age-sex
  pred_CFR <- merge(sum_pop, cfr_covar, by=c("location_id", "year_id"))
  N <- nrow(pred_CFR)

  ### 1000 draws for uncertainty
  # coefficient matrix
  coefmat <- c(coef(nb_model))
  names(coefmat)[1] <- "constant"
  names(coefmat) <- paste("b", names(coefmat), sep = "_")
  coefmat <- matrix(unlist(coefmat), ncol=length(coefmat), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))

  # covariance matrix
  vcovmat <- vcov(nb_model)

  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
  betas <- t(betadraws)

  # create draws of disperion parameter
  alphas <- 1 / exp(rnorm(1000, mean=nb_model$theta, sd=nb_model$SE.theta))

  ### estimate deaths
  cfr_draw_cols <- paste0("cfr_draw_", draw_nums_gbd)
  # generate draws of the prediction using coefficient draws
  if (HAQI_or_HSA == "use_HSA") {
    if (GAMMA_EPSILON == "without") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                         ( betas[2, (i + 1)] * ln_ldi ) +
                                                                         ( betas[3, (i + 1)] * health ) )
      )]
    } else if (GAMMA_EPSILON == "with") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) rgamma( N,
                                                                              scale=(alphas[i + 1] *
                                                                                       exp( betas[1, (i + 1)] +
                                                                                          ( betas[2, (i + 1)] * ln_ldi ) +
                                                                                          ( betas[3, (i + 1)] * health ) )), shape=(1 / alphas[i + 1]) )
      )]
    }
  } else if (HAQI_or_HSA == "use_HAQI") {
    if (GAMMA_EPSILON == "without") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                         ( betas[2, (i + 1)] * ln_ldi ) +
                                                                         ( betas[3, (i + 1)] * HAQI ) )
      )]
    } else if (GAMMA_EPSILON == "with") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) rgamma( N,
                                                                              scale=(alphas[i + 1] *
                                                                                       exp( betas[1, (i + 1)] +
                                                                                          ( betas[2, (i + 1)] * ln_ldi ) +
                                                                                          ( betas[3, (i + 1)] * HAQI ) )), shape=(1 / alphas[i + 1]) )
      )]
    }
  } else if (HAQI_or_HSA == "HAQI_only") {
    if (GAMMA_EPSILON == "without") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                         ( betas[2, (i + 1)] * HAQI ) )
      )]
    } else if (GAMMA_EPSILON == "with") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) rgamma( N,
                                                                              scale=(alphas[i + 1] *
                                                                                       exp( betas[1, (i + 1)] +
                                                                                          ( betas[2, (i + 1)] * HAQI ) )), shape=(1 / alphas[i + 1]) )
      )]
    }
  }

  # save results
  CFR_draws_save <- pred_CFR[, c("location_id", "year_id", cfr_draw_cols), with=FALSE]
  if (WRITE_FILES == "yes") {
    fwrite(CFR_draws_save, file.path(j.version.dir, paste0("04_cfr_model_draws.csv")), row.names=FALSE)
  }
  #***********************************************************************************************************************


  ########################################################################################################################
  ##### PART SEVEN: CALCULATE DEATHS #####################################################################################
  ########################################################################################################################


  #----CALCULATE DEATHS---------------------------------------------------------------------------------------------------
  ### merge cases and cfr
  predictions_deaths <- merge(rescaled_cases, CFR_draws_save, by=c("location_id", "year_id"), all.x=TRUE)

  ### calculate deaths from cases and CFR
  death_draw_cols <- paste0("death_draw_", draw_nums_gbd)
  predictions_deaths[, (death_draw_cols) := lapply(draw_nums_gbd, function(ii) get(paste0("cfr_draw_", ii)) * get(paste0("case_draw_", ii)) )]

  ### save results
  predictions_deaths_save <- predictions_deaths[, c("location_id", "year_id", death_draw_cols), with=FALSE]
  if (WRITE_FILES == "yes") {
    fwrite(predictions_deaths_save, file.path(j.version.dir, "05_death_predictions_from_model.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************


  ########################################################################################################################
  ##### PART EIGHT: RESCALE DEATHS #######################################################################################
  ########################################################################################################################


  #----RESCALE------------------------------------------------------------------------------------------------------------
  ### run custom rescale function
  rescaled_deaths <- rake(predictions_deaths_save, measure="death_draw_")

  ### save results
  if (WRITE_FILES == "yes") {
    fwrite(rescaled_deaths, file.path(j.version.dir, "06_death_predictions_rescaled.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************


  ########################################################################################################################
  ##### PART NINE: DEATHS AGE-SEX SPLIT ##################################################################################
  ########################################################################################################################


  #----SPLIT--------------------------------------------------------------------------------------------------------------
  ### age-sex split using demographic pattern in cause of death data
  split_deaths <- age_sex_split(acause=acause, input_file=rescaled_deaths, measure="death")

  ### save split draws
  split_deaths_save <- split_deaths[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("split_death_draw_", 0:999)), with=FALSE]
  if (WRITE_FILES == "yes") {
    fwrite(split_deaths_save, file.path(j.version.dir, "07_death_predictions_split.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART TEN: COMBINE CODEm DEATHS ##################################################################################
  ########################################################################################################################
  
  
  #----COMBINE------------------------------------------------------------------------------------------------------------
  ### read CODEm COD results for data-rich countries
  cod_M <- data.table(pandas$read_hdf(file.path(""FILEPATH"")), key="data"))
  cod_F <- data.table(pandas$read_hdf(file.path(""FILEPATH"")), key="data"))
  
  # save model version
  cat(paste0("Data-rich CODEm feeder model (males) - model version ", male_CODEm_version), 
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  cat(paste0("Data-rich CODEm feeder model (females) - model version ", female_CODEm_version), 
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  # combine M/F CODEm results
  cod_DR <- rbind(cod_M, cod_F, fill=TRUE)
  cod_DR <- cod_DR[, draw_cols_upload, with=FALSE] 
  
  # hybridize data-rich and custom models
  data_rich <- unique(cod_DR$location_id)
  split_deaths_glb <- split_deaths_save[!location_id %in% data_rich, ]
  colnames(split_deaths_glb) <- draw_cols_upload
  split_deaths_hyb <- rbind(split_deaths_glb, cod_DR)
  
  # keep only needed age groups
  split_deaths_hyb <- split_deaths_hyb[age_group_id %in% c(age_start:age_end), ]
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART ELEVEN: FORMAT FOR CODCORRECT ###############################################################################
  ########################################################################################################################
  
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### format for codcorrect
  split_deaths_hyb[, measure_id := 1]
  
  ### save to share directory for upload
  lapply(unique(split_deaths_hyb$location_id), function(x) write.csv(split_deaths_hyb[location_id==x, ], file.path(cl.death.dir, paste0(x, ".csv")), row.names=FALSE))
  print(paste0("death draws saved in ", cl.death.dir))
  
  ### save_results
  job <- paste0("qsub -N s_cod_", acause, " -l m_mem_free=100G -l fthread=5 -l archive -l h_rt=8:00:00 -P proj_cov_vpd -q all.q -o "FILEPATH"", 
                username, " -e "FILEPATH"", username,
                " "FILEPATH"save_results_wrapper.r",
                " --args",
                " --type cod",
                " --me_id ", cause_id,
                " --input_directory ", cl.death.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", mark_model_best, 
                " --gbd_round ", gbd_round)
  if (decomp) job <- paste0(job, " --decomp_step ", decomp_step)
  system(job); print(job)
  #***********************************************************************************************************************
}