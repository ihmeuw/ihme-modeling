#################################################################################################
#' Name: MR-BRT FOR EARLY SYPHILIS INFECTION (COMPOSITE MODEL)
#' 5 crosswalks total (sex, diagnostics (1), populations(2)). This script will be used for:
################################################################################################

#SETUP-----------------------------------------------------------------------------------------------
#rm(list=ls())
library(crosswalk)
library(msm)
library(stringr)
library(magrittr)
library(assertthat)
library(metafor)
library(boot)
library(arm)
pacman::p_load(data.table, openxlsx, ggplot2, dplyr)

#SOURCE FUNCTIONS
source_shared_functions(functions = c("get_bundle_data","get_bundle_version", "save_crosswalk_version", "get_crosswalk_version",
                                      "save_bulk_outlier", "get_draws", "get_population", "get_location_metadata",
                                      "get_age_metadata", "get_ids"))

#ARGS & DIRS
cause <- "early_syphilis"
xwalk_dir <- paste0("FILEPATH")
match_dir <- "FILEPATH"
dx_dir <- paste0(xwalk_dir, "diagnostic_network/")
pop_dir <- paste0(xwalk_dir, "populations/")
comp_dir <- paste0(xwalk_dir, "composite_crosswalk/")

#which bvid used to match/adjust
es_bvid <- OBJECT

#matching params (10 yrs used for each matching schema)
agematch <- 10
yearmatch <- 10
locmatch <- "strict"
narrow <- 10

#GET  MATCHES--------------------------------------------------------------------------------------------------------------------------------------------
get_dx_matches <- function(){
  print("getting diagnostic matches")
  #read in dx matches found by code
  alltrep_matched_fpath <- paste0(match_dir, "ALLTREP_bv", es_bvid, "_age", agematch, "_year", yearmatch, "_loc", locmatch, "_narrow" ,narrow,".csv")
  alltrep_matched_dt <- data.table(read.csv(alltrep_matched_fpath))
  dim(alltrep_matched_dt)

  manual_intra_fpath <- paste0(match_dir, "formatted_dx_crosswalk_use.xlsx")
  manual_intra_dt <- data.table(read.xlsx(manual_intra_fpath))
  dim(manual_intra_dt)

  #rbind
  names(alltrep_matched_dt)
  names(manual_intra_dt)
  all_matched_dt <- rbind(alltrep_matched_dt, manual_intra_dt, fill = TRUE)
  dim(all_matched_dt)

  return(all_matched_dt)
}
dx_matches <- get_dx_matches()
dim(dx_matches)

#read in population matches found by code
get_pop_matches <- function(){
  print("getting blood donor and/or pregnant matches")
  allpop_matched_fpath <- paste0(match_dir, "ALLPOP_bv", es_bvid, "_age", agematch, "_year", yearmatch, "_loc", locmatch, "_narrow" ,narrow,".csv")
  allpop_matched_dt <- data.table(read.csv(allpop_matched_fpath))
  dim(allpop_matched_dt)

  return(allpop_matched_dt)
}
pop_matches <- get_pop_matches()
dim(pop_matches)

#merge on all relevant columns for that nid from the bundle version
xwalk_matches <- rbind(dx_matches, pop_matches, fill = TRUE)
dim(xwalk_matches)
matched_nids <- c(xwalk_matches$alt_nid, xwalk_matches$ref_nid)
matched_nids <- unique(matched_nids)

#get the bundle version
es_bvdata <- get_bundle_version(bundle_version_id = es_bvid, fetch = "all", export = FALSE)
matched_data <- es_bvdata[nid %in% matched_nids]
matched_data <- matched_data[ ,c("nid", "sex","cv_treponemal_only", "cv_dx_nontreponly", "both", "cv_blood_donor", "cv_pregnant")]
matched_data <- unique(matched_data)

write.xlsx(x = xwalk_matches)
write.xlsx(x = matched_data)

#read in the composite dataset
composite_matches <- data.table(read.xlsx(xlsxFile = paste0(match_dir, "all_matches_composite.xlsx"), sheet = "composite"))
names(composite_matches)

##LOGIT PREPARE-----------------------------------------------------------------------------------------------------------------------------------------------
comp_logit_preparation <- function(dt){

  #get identifying cols
  demo_cols <- dt[ ,c("alt_nid", "ref_nid", "age_group", "year_group", "sex", "location_id", "alt3_composite", "ref3_composite")]
  setnames(demo_cols, names(demo_cols)[7:8], c("alt_composite", "ref_composite"))
  #transform alt mean into logit space
  alt_transform_dt <- data.table(delta_transform(mean = dt$alt_mean, sd = dt$alt_se, transformation = "linear_to_logit"))
  setnames(alt_transform_dt, c("mean_logit", "sd_logit"), c("alt_mean_logit", "alt_se_logit"))

  #transform ref mean into logit space
  ref_transform_dt <- data.table(delta_transform(mean = dt$ref_mean, sd = dt$ref_se, transformation = "linear_to_logit"))
  setnames(ref_transform_dt, c("mean_logit", "sd_logit"), c("ref_mean_logit", "ref_se_logit"))

  #bind them together
  logit_dt <- cbind(alt_transform_dt, ref_transform_dt)

  #calculate logit difference
  diff_dt <- calculate_diff(df = logit_dt, alt_mean = "alt_mean_logit", alt_sd = "alt_se_logit",
                            ref_mean = "ref_mean_logit", ref_sd = "ref_se_logit")
  setnames(diff_dt, c("diff_mean", "diff_sd"), c("logit_diff", "logit_diff_se"))

  #combine the dataset
  all_transformed <- cbind(demo_cols, logit_dt, diff_dt)

  all_transformed[ ,transformation := "logit_diff"]
  all_transformed[ ,study_id := .GRP, by = c("alt_nid", "ref_nid")]

  return(all_transformed)
}

logit_trans_dt <- comp_logit_preparation(dt = composite_matches)
logit_trans_dt[ ,id2 := 1:nrow(logit_trans_dt)]
View(logit_trans_dt)
names(logit_trans_dt)

#RUN COMPOSITE CROSSWALK------------------------------------------------------------------------------------------------------------------------------------

#FORMAT DATA DX FOR THE MODEL
comp_dat1 <- CWData(
  df = logit_trans_dt,
  obs = "logit_diff",       # logit difference
  obs_se = "logit_diff_se", # SE of logit difference
  alt_dorms = "alt_composite",   # var for the alternative def/method
  ref_dorms = "ref_composite",   # var for the reference def/method
  dorm_separator = "_",
  covs = list(),       # list of (potential) covariate columns
  study_id = "study_id", # var for random intercepts; i.e. (1|study_id)
  add_intercept = TRUE
)
comp_dat1

#RUN MATCHES FOR THE MODEL
comp1_pct <- 0.9
comp1 <- CWModel(
  cwdata = comp_dat1,           # result of CWData() function call
  obs_type = "diff_logit", # must be "diff_logit" or "diff_log"
  cov_models = list(CovModel("intercept")),
  gold_dorm = "bothgeneral", # level of 'ref_dorms' that's the gold standard
  inlier_pct = comp1_pct #1-pct(outlier)
)
comp1

#check coefs (aka do you actually want to save this)
print(data.frame(
  beta_true = comp1$fixed_vars,
  beta_mean = comp1$beta,
  beta_se = comp1$beta_sd,
  gamma = comp1$gamma
))


plot_vars <- names(comp1$fixed_vars)[!(names(comp1$fixed_vars) %in% c("bothgeneral"))]

for (var in plot_vars) {

  print(var)
  plot_fname <- paste0("composite_", var, "_int_pct", comp1_pct, "_age", agematch, "_yr",yearmatch)
  plot_note <-paste0(var, " adjustment, composite model: ", agematch,"yr age, ", yearmatch, "yr time, exact location, ", comp1_pct*100,"% inclusion")

  plots$funnel_plot(
    cwmodel = comp1,
    cwdata = comp_dat1,
    continuous_variables = list(),
    obs_method = var,
    plot_note = plot_note,
    plots_dir = paste0(comp_dir, "plots/"),
    file_name = plot_fname,
    write_file = TRUE)
}

#FORMAT DATA FOR ADJUSTMENTS
already_ssplit <- copy(full_ssplit_bv)
dim(already_ssplit)

format_es_data <- function(dt){

  #alt defs
  dt[cv_dx_nontreponly == 1, dx_dummy := "nontrep"]
  dt[cv_treponemal_only == 1, dx_dummy := "trep"]
  dt[cv_blood_donor == 1, pop_dummy := "blood"]
  dt$row_id <- paste0("row", 1:nrow(dt))

  dt[!is.na(dx_dummy) & !is.na(pop_dummy),obs_dummy := paste0(dx_dummy, "_", pop_dummy)]
  dt[!is.na(dx_dummy) & is.na(pop_dummy),obs_dummy := dx_dummy]
  dt[is.na(dx_dummy) & !is.na(pop_dummy),obs_dummy := pop_dummy]

  leave <- dt[(measure == "prevalence" & is.na(obs_dummy)) | measure != "prevalence"]
  #inflate se of this data
  inflate <- dt[measure == "prevalence" & !is.na(obs_dummy) & mean == 0]
  #adjust mean and se of this data

  adjust <- dt[measure == "prevalence" & !is.na(obs_dummy) & mean != 0]

  if (nrow(leave) + nrow(inflate) + nrow(adjust) == nrow(dt)){
    "returning 3 data tables."
    return(list(leave_dt = leave, inflate_dt = inflate, adjust_dt = adjust))
  }else{
    print("Error: leave, inflate, and adjust are not mutually exclusive.")
  }
}

formatted_dts <- format_es_data(dt = copy(already_ssplit))
leave_dt <- formatted_dts$leave_dt
inflate_dt <- formatted_dts$inflate_dt
adjust_dt <- formatted_dts$adjust_dt

#APPLY CROSSWALK TO ADJUST DT-----------------------------------------------------------------------------------------------------------------------
format_betas <- function(choose_model){
  model <- choose_model
  obs_dummy <- model$vars
  beta <- model$beta
  beta_se <- model$beta_sd

  vals_dt <- data.table(cbind(obs_dummy, beta, beta_se))
  vals_dt$beta <- as.numeric(vals_dt$beta)
  vals_dt$beta_se <- as.numeric(vals_dt$beta_se)
  return(vals_dt)

}
beta_dt <- format_betas(choose_model = comp1)
beta_dt

#GET PREDICTIONS
comp_preds <- adjust_orig_vals(fit_object = comp1, df = adjust_dt,
                               orig_dorms = "obs_dummy",orig_vals_mean = "mean",
                               orig_vals_se = "standard_error", data_id = "row_id")
setnames(comp_preds, "data_id", "row_id")


#adjust dt
adjust_alt_data <- function(statement){

  #merge comp preds onto original data set

  adjusted_dt <- merge(adjust_dt, comp_preds, by = c("row_id"))
  adjusted_dt[ ,`:=` (mean = ref_vals_mean, standard_error = ref_vals_sd)]

  #inflate mean zero data
  inflate_this <- copy(inflate_dt)
  print(unique(inflate_this$obs_dummy))

  inflate_this <- merge(inflate_this, beta_dt, by = "obs_dummy", all.x = TRUE)
  names(inflate_this)
  inflate_this[ ,standard_error := sqrt(standard_error^2 + beta_se^2)]

  setdiff(names(adjusted_dt), names(inflate_this))
  setdiff(names(inflate_this), names(adjusted_dt))

  #update cols
  change_dt <- rbind(adjusted_dt, inflate_this, fill = TRUE)
  change_dt[ ,c("ref_vals_mean", "ref_vals_sd", "pred_diff_mean", "pred_diff_sd", "beta", "beta_se",
                "row_id", "dx_dummy", "pop_dummy") := NULL]


  change_dt[is.na(crosswalk_parent_seq) ,crosswalk_parent_seq := seq]
  change_dt[ ,seq := NA]

  #return dts
  adjvals_only <- copy(change_dt)

  all_vals <- rbind(change_dt, leave_dt, fill = TRUE)
  all_vals[ ,c("dx_dummy", "pop_dummy", "row_id") := NULL]
  return(list(adjusted_only = adjvals_only, adjusted_all = all_vals ))

}

get_adjusted <- adjust_alt_data(statement = "adjusting and inflating yaaas")
formatted_adj_dt <- get_adjusted$adjusted_only
full_bv_xwalked <- get_adjusted$adjusted_all

write_xlsx(list(extraction = formatted_adj_dt))

write_xlsx(list(extraction = full_bv_xwalked))

















































































