################################################################################
## DESCRIPTION: Calculate Self-report adjustment (systematic error) for USA anthropometric data (height, weight, BMI, and derivatives) ##
## INPUTS: Age-sex split bundle version data ##
## OUTPUTS: Adjustment values for USA self-report data ##
## AUTHOR: 
## DATE: 
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
code_dir <- "FILEPATH"
repo <- "FILEPATH"

data_dir <- "FILEPATH"
save_dir <- "FILEPATH"


library(reticulate)
reticulate::use_python("FILEPATH")
cw <- reticulate::import("crosswalk")
library(crosswalk002) ## only use this for calculate_diff() function

invisible(sapply(list.files("FILEPATH", full.names = T), source))
library(openxlsx)
library(dplyr)
library(plyr)
library(assertable)
library(gtools)
library(ggplot2)
library(splines)
library(stringr)

regions <- get_ids("location")[location_description=="region"]
locs <- get_location_metadata(location_set_id=1, release_id=9)[,c("location_id", "location_name_short", "super_region_id", "super_region_name",
                                                                  "region_id", "region_name", "ihme_loc_id")]
locs[, country_id := as.integer(as.factor(substr(ihme_loc_id, 1, 3)))][, country_name := substr(ihme_loc_id, 1, 3)]
ages <- get_age_metadata(age_group_set_id=24, release_id=9)[, .(age_group_id, age_start = age_group_years_start, age_end = age_group_years_end-1, 
                                                                age_mid = .5*(age_group_years_start + age_group_years_end))]
ages$age_start <- round(ages$age_start)
ages$age_end <- round(ages$age_end)


match.data <- function(data_df, # long data set with variables location_id, age_group_id, sex_id, year_id, nid, variable (indexing ref, non-ref), mean
                       fuzzy_match = T, # whether or not to match data based on inexact years
                       year_match_window = 2, # window for fuzzy year match (+/- arg)
                       gs_var = 'measured' # variable label corresponding to gold standard data
) {
  
  # TODO: should add an option for downweighting matched data farther away in the window (start with loess weights?)
  
  # Split data sets into gs and non-gs
  gs_data <- data_df[variable == gs_var][, variable := NULL]
  non_gs_data <- data_df[variable != gs_var][, variable := NULL]
  
  # Tag id variables to attribute with gs and non-gs 
  tag_vars <- c('year_id', 'nid', 'mean', 'se')
  setnames(gs_data, tag_vars, paste0(tag_vars, '_gs'))
  setnames(non_gs_data, tag_vars, paste0(tag_vars, '_non_gs'))
  
  # Match dataset
  matched_data <- merge(gs_data, non_gs_data, by = c('location_id', 'ihme_loc_id', 'region_id', 'super_region_id', 'age_group_id', 'sex_id'), allow.cartesian = T)
  
  # Keep sources that match within specified year window
  if(!fuzzy_match) year_match_window <- 0
  matched_data <- matched_data[abs(year_id_gs - year_id_non_gs) <= year_match_window]
  
  # Generate time-distance weights based on loess (tricubic function)
  matched_data[, w := (1-abs((year_id_gs-year_id_non_gs)/(1+year_match_window))^3)^3]
  
  message(sprintf('(%d) unique matches were found across all age-sex groups using a (%d) year window', nrow(matched_data), year_match_window))
  
  return(matched_data)
}

## most recent age-sex split data
df_ow <- fread("FILEPATH")[!(is.na(mean)|is.na(variance))]
df_ob <- fread("FILEPATH")[!(is.na(mean)|is.na(variance))]

for(ind in c("ow", "ob")){
  df <- copy(get(paste0("df_", ind))) %>% subset(is_outlier==0)
  save_loc <- "FILEPATH"
  
  ## LONG DATA: Need location, age, sex, year, nid, variable, value
  covariates <- c()
  long_vars <- c('location_id', 'age_group_id', 'year_id', 'sex_id', 'nid', 'variable', 'mean', 'se',"diagnostic", covariates)
  df[sample_size==0, sample_size:=1] ## replace 0 values so se is not calculated as "Inf"
  df[, mean := val]
  df[, variable := "Other"][grepl("National Health and Nutrition Examination",field_citation_value) | grepl("NATIONAL_HEALTH_AND_NUTRITION_EXAMINATION_SURVEY",field_citation_value), variable := "NHANES"]
  df[, se := sqrt(mean * (1 - mean) / sample_size)] 
  long_data <- df[, c(long_vars), with = F]
  
  ## drop observations with no SE
  long_data <- long_data[!is.na(se)]
  
  ## Merge on location hierarchy variables for random effects
  long_data <- merge(long_data, locs[, .(location_id, region_id, super_region_id, country_id, country_name, ihme_loc_id)], by = 'location_id', all.x=T)
  ##subset to USA only locations
  long_data <- subset(long_data, country_name == "USA")
  long_data$row_id <- paste0("row", 1:nrow(long_data))
  
  ##logit tranformaation cannot handle 0 and 1 values. Offset these cases.
  offset_val <- 1e-05
  long_data[mean == 0, mean := mean+offset_val][mean == 1, mean := mean-offset_val]
  
  ## add in decade dummy variable
  long_data[, decade := 1990][year_id>=2000, decade := 2000][year_id>=2010, decade := 2010]
  long_data[, decade := as.factor(decade)]
  long_data$decade <- relevel(long_data$decade, ref = 2)  ##update reference age group to 20-25
  
  # transform val and standard_error into logit space for adjustment later on
  long_data[, c("logit_val", "logit_se")] <- as.data.table(cw$utils$linear_to_logit(mean = array(long_data$mean), sd = array(long_data$se)))
  
  if(length(unique(long_data$variable)) > 2) stop("Dataset contains more than two referent categories. 
                                                    Check to makes sure only one gold-standard and one non-gold standard are present.")
  
  measured_data <- subset(long_data, diagnostic=="measured" & variable!="NHANES") ## create dataset of measured sources, not NHANES

  long_data <- subset(long_data, diagnostic=="self-report" | variable=="NHANES") ## NAHENS (gs) and self-report (non-gs) for crosswalking
  
  ## WIDE DATA: Need location, age, sex, year, gold-standard val, gs nid, substandard val, ss nid
  wide_data <- match.data(long_data, fuzzy_match = T, year_match_window =3, gs_var = "NHANES")
  
  # prepare data for crosswalking -- logit required for CWData function to ensure that the crosswalk adjustment remains bounded correctly.
  ## Get differences between gold and CF in logit form
  wide_data[, c("logit_val_gs", "logit_se_gs")] <- as.data.table(cw$utils$linear_to_logit(mean = array(wide_data$mean_gs), sd = array(wide_data$se_gs)))
  wide_data[, c("logit_val_non_gs", "logit_se_non_gs")] <- as.data.table(cw$utils$linear_to_logit(mean = array(wide_data$mean_non_gs), sd = array(wide_data$se_non_gs)))
    
  # get table of matched reference and gold standard data pairs
  ## calculates means and SDs for differences between random variables, ensuring that the alternative definition/method is in the numerator: 
  ## log(alt/ref) = log(alt) - log(ref)
  wide_data[, c("diff", "diff_se")] <- calculate_diff(df = wide_data, alt_mean = "logit_val_non_gs", alt_sd = "logit_se_non_gs", ref_mean = "logit_val_gs", ref_sd = "logit_se_gs" )
  
  wide_data <- merge(wide_data, ages[, .(age_group_id, age_mid = .5*(age_start + age_end) + .5)], by = 'age_group_id')
  
  matched <- unique(wide_data[,.N, c("ihme_loc_id","nid_gs","nid_non_gs","year_id_gs","year_id_non_gs")])

  mr_brt_df <- wide_data[, .(nid = paste(nid_gs), nid_non_gs, id = paste(row_id.y), diff, diff_se, age_group_id, age_mid, sex_id, decade =paste0(decade.y),
                             year_id = round(.5*(year_id_gs + year_id_non_gs)))]
  
  ## Add in reference, alternative information
  mr_brt_df[, dorm_ref := "NHANES"][, dorm_alt := "Other"]
  
  ##add in id2 based on what matches with original dataset
  mr_brt_df[, nid := as.numeric(nid)]
  mr_brt_df[, nid_non_gs := as.numeric(nid_non_gs)]
  
  all_data <- data.table()
  
  for(s in 1:2){
    ### Start crosswalk
    mr_brt_df_s <- subset(mr_brt_df, sex_id==s)
    long_xwalk <- subset(long_data, sex_id==s)
    
    mr_brt_df_s[,decade := as.factor(decade)]
    mr_brt_df_s$decade <- relevel(mr_brt_df_s$decade, ref = 2)  ##update reference age group to 20-25
  
    # format data for meta-regression; pass in data.frame and variable names
    dat1 <- cw$CWData(
      df = mr_brt_df_s,
      obs = "diff",       # matched differences in logit space
      obs_se = "diff_se", # SE of matched differences in logit space
      alt_dorms = "dorm_alt",   # var for the alternative def/method
      ref_dorms = "dorm_ref",   # var for the reference def/method
      covs = list("age_group_id","decade") ,         # list of (potential) covariate columns #
      study_id = "nid_non_gs"          # var for random intercepts; i.e. (1|study_id) ## Use NID as group to determine between-study heterogeneity
    )
    
    # create crosswalk object called fit1. Meta regression model, data needs to be formatted by CWData function.
    fit1 <- cw$CWModel(
      cwdata = dat1,               # result of CWData() function call
      obs_type = "diff_logit",     # must be "diff_logit" or "diff_log"
      cov_models = list(cw$CovModel("intercept"), cw$CovModel("age_group_id"), cw$CovModel("decade")),     #
      gold_dorm = "NHANES" #,  # level of 'ref_dorms' that's the gold standard, 
    )
    fit1$fit()
    
    ## adjust without study_id
    preds1 <- fit1$adjust_orig_vals(
      df = long_xwalk,
      orig_dorms = "variable",
      orig_vals_mean = "mean",
      orig_vals_se = "se" #,
    )
    
    long_xwalk[, 
               c("meanvar_adjusted", "sdvar_adjusted", 
                 "pred", "pred_se", "data_id")] <- preds1
    long_xwalk[, gamma := fit1$gamma]
    long_xwalk[pred==0, error := 0][pred!=0, error := meanvar_adjusted-mean]
    all_data <- rbind(all_data, long_xwalk, fill=T)
    
  }
  ## Save based on covariates and measure selected
  saveRDS(all_data, paste0(save_loc, "xwalked_", ind,"_sd.RDS"))

}
