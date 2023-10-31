################################################################################################
#' crosswalk: claims to inpatient data
################################################################################################

#SETUP-----------------------------------------------------------------------------------------------
#rm(list=ls())
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
user <- Sys.info()["user"]
source("FILEPATH")
library(crosswalk, lib.loc = "FILEPATH")
library(msm)
library(stringr)
library(magrittr)
library(assertthat)
library(metafor)
library(boot)
library(arm)
pacman::p_load(data.table, openxlsx, ggplot2, dplyr)

#SOURCE FUNCTIONS
source_shared_functions(functions = c("upload_bundle_data","get_bundle_data","save_bundle_version","get_bundle_version",
                                      "save_crosswalk_version","get_crosswalk_version", "save_bulk_outlier", "get_draws",
                                      "get_population","get_location_metadata", "get_age_metadata", "get_ids"))

#ARGS & DIRS
cause <- "ats"
xwalk_dir <- "FILEPATH"
match_dir <- "FILEPATH"
clinic_dir <- "FILEPATH"

#which bvid used to match/adjust
ats_bvid <- 29636
new_ats_bvid <- 34100

#matching params
agematch <- 5
yearmatch <- 5
locmatch <- "strict"

#GET YOUR MATCHES--------------------------------------------------------------------------------------------------------------------------------------------
clinical_matched_fpath <- "FILEPATH"
print(clinical_matched_fpath)
clinic_matched_dt <- data.table(read.csv(clinical_matched_fpath))
dim(clinic_matched_dt)

head(clinic_matched_dt)
names(clinic_matched_dt)

##LOGIT PREPARE-----------------------------------------------------------------------------------------------------------------------------------------------
ats_logit_preparation <- function(dt){

  #get identifying cols
  demo_cols <- dt[ ,c("alt_nid", "ref_nid", "age_group", "year_group", "sex", "location_id", "cdt_claims")]

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

  #alternate ref columns
  all_transformed[cdt_claims == 1, `:=` (dorm_alt = "claims", dorm_ref = "inpatient")]

  all_transformed[ ,transformation := "logit_diff"]
  all_transformed[ ,study_id := .GRP, by = c("alt_nid", "ref_nid")]

  #format age columns
  all_transformed$age_start <- as.numeric(lapply(X = all_transformed$age_group, function(x) substr(x, start = 5, stop = 6)))
  all_transformed$age_end <- as.numeric(lapply(X = all_transformed$age_group, function(x) substr(x, start = 8, stop = 9)))

  #format older age columns
  all_transformed[age_group == "age_100_104", `:=` (age_start = 100, age_end = 104)]
  all_transformed[ ,mid_age := (age_start + age_end)/2]

  return(all_transformed)
}

ats_logit_dt <- ats_logit_preparation(dt = clinic_matched_dt)
ats_logit_dt[ ,id2 := 1:nrow(ats_logit_dt)] 
View(ats_logit_dt)
names(ats_logit_dt)

#PLOT LOGIT DIFFERENCES BY AGE----------------------------------------------------------------------------------------------------------------------
plot_trans_by_age <- function(dt) { 
  trans_type <- unique(dt$transformation)
  print(trans_type)

  mid_age <- ggplot(dt, aes(x=mid_age, y=logit_diff, color = year_group)) +geom_point() + labs(title = "USA Claims - USA Inpatient Logit Difference by Mid-Age (ATS)")
  ggsave(filename = "FILEPATH", plot = mid_age, device = "pdf", path = match_dir, width = 12, height = 8)

  return(mid_age)
}
plot_trans_by_age(dt = ats_logit_dt)

#RUN THAT MODEL---------------------------------------------------------
#format data
ats_dat1 <- CWData(
  df = ats_logit_dt,
  obs = "logit_diff",       # matched ratios in log space
  obs_se = "logit_diff_se", # SE of F/M ratio in log space
  alt_dorms = "dorm_alt",   # var for the alternative def/method
  ref_dorms = "dorm_ref",   # var for the reference def/method
  covs = list("mid_age"),       # list of (potential) covariate columns
  study_id = "study_id"          # var for random intercepts; i.e. (1|study_id)
)
ats_dat1

ats_fit1_pct <- 0.9
ats_fit1 <- CWModel(
  cwdata = ats_dat1,           # result of CWData() function call
  obs_type = "diff_logit", # must be "diff_logit" or "diff_log"
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept"),
  CovModel(cov_name = "mid_age", spline = XSpline(knots = c(15,42,72,104), degree = 3L, l_linear =TRUE, r_linear = TRUE))),
  gold_dorm = "inpatient", # level of 'ref_dorms' that's the gold standard
  inlier_pct = ats_fit1_pct #1-pct(outlier)
)
ats_fit1

print(data.frame(
  beta_true = unique(ats_logit_dt$dorm_alt),
  beta_mean = ats_fit1$beta,
  beta_se = ats_fit1$beta_sd,
  gamma = ats_fit1$gamma
))

#PLOT
#repl_python()
#plots <- import("crosswalk.plots")

plots$dose_response_curve(
  dose_variable = "mid_age",
  obs_method = 'claims',
  continuous_variables = list(),
  cwdata = ats_dat1,
  cwmodel = ats_fit1,
  plot_note = paste0("Claims - Inpatient Logit Diff by Age Midpoint, ", ats_fit1_pct*100, "%"),
  plots_dir = "FILEPATH",
  file_name = paste0("spline_4pts_pct", ats_fit1_pct),
  write_file = TRUE
)

#SAVE CHOSEN CROSSWALK----------------------------------------------------------------------------------------------------------
#save csvs
df_result <- ats_fit1$create_result_df()
write.csv(df_result, "FILEPATH"), row.names = FALSE)

py_save_object(object = ats_fit1, filename = "FILEPATH", pickle = "dill")
ats_fit1 <- py_load_object(filename = "FILEPATH", pickle = "dill")

#save RDS
name_of_model <- paste0("logit_spline_4pts_pct", ats_fit1_pct, "_bv", ats_bvid, "_claims_inp", "_age", agematch, "yr",yearmatch, "_loc", locmatch)
name_of_model

save_model_RDS <- function(results, path, model_name){
  names <- c("beta",
             "beta_sd",
             "constraint_mat",
             "cov_mat",
             "cov_models",
             "cwdata",
             "design_mat",
             "fixed_vars",
             "gamma",
             "gold_dorm",
             "lt",
             "num_vars",
             "num_vars_per_dorm",
             "obs_type",
             "order_prior",
             "random_vars",
             "relation_mat",
             "var_idx",
             "vars",
             "w")
  model <- list()
  for (name in names){
    if(is.null(results[[name]])) {
      message(name, " is NULL in original object, will not be included in RDS")
    }
    model[[name]] <- results[[name]]
  }
  saveRDS(model, paste0(path, model_name, ".RDS"))
  message("RDS object saved to ", paste0(path, model_name, ".RDS"))
  return(model)
}

RDS_results <- ats_fit1
RDS_path <- clinic_dir
save_model_RDS(results = RDS_results, path = paste0(RDS_path), model_name = name_of_model)

#FORMAT ats DATA FOR ADJUSTMENTS------------------------------------------------------------------------------------------------------------------------------
ats_bvdata <- get_bundle_version(bundle_version_id = new_ats_bvid, fetch = "all", export = FALSE)
dim(ats_bvdata)
names(ats_bvdata)

loc_meta <- get_location_metadata(location_set_id = 35, gbd_round_id = 7, decomp_step = "step3")
us_locs <- loc_meta[parent_id == 102 | location_id == 102, location_id]

format_ats_data <- function(dt){

  dt[age_end == 124, age_end := 99]
  dt[ ,`:=` (obs_dummy = clinical_data_type, mid_age = (age_start + age_end)/2)]
  dt$row_id <- paste0("row", 1:nrow(dt))

  #leave this data alone for clinical xwalk
  #inpatient is the reference for ATS
  leave <- dt[!(location_id %in% us_locs) |
                (location_id %in% us_locs & obs_dummy %in% c("", "inpatient"))]

  #inflate se of this data
  inflate <- dt[location_id %in% us_locs & obs_dummy == "claims" & mean == 0]

  #adjust mean and se of this data
  adjust <- dt[location_id %in% us_locs & obs_dummy == "claims" & mean != 0]

  if (nrow(leave) + nrow(inflate) + nrow(adjust) == nrow(dt)){
    "returning 3 data tables."
    return(list(leave_dt = leave, inflate_dt = inflate, adjust_dt = adjust))
  }else{
    print("Error: leave, inflate, and adjust are not mutually exclusive.")
  }
}

all_dts <- format_ats_data(dt = copy(ats_bvdata))
leave_dt <- all_dts$leave_dt
inflate_dt <- all_dts$inflate_dt
adjust_dt <- all_dts$adjust_dt

#APPLY CROSSWALK TO ADJUST DT---------------------------------------------------------------------------------------------------------------------------------
ats_fit1 <- py_load_object(filename = "FILEPATH", pickle = "dill")

format_betas <- function(rds_file){
  model <- readRDS(rds_file)
  obs_dummy <- model$vars
  beta <- model$beta
  beta_se <- model$beta_sd

  vals_dt <- data.table(cbind(obs_dummy, beta, beta_se))
  vals_dt$beta <- as.numeric(vals_dt$beta)
  vals_dt$beta_se <- as.numeric(vals_dt$beta_se)
  vals_dt <- vals_dt[beta != 0]
  return(vals_dt)

}
beta_dt <- format_betas(rds_file = "FILEPATH")
beta_dt

#actually adjust the data
adjust_inp_data <- function(statement){

  print(paste0(statement, ". anywho, getting predictions"))
  #get predictions by age
  claims_preds <- data.table(adjust_orig_vals(fit_object = ats_fit1, df = adjust_dt,
                                                 orig_dorms = "obs_dummy", orig_vals_mean = "mean",
                                                 orig_vals_se = "standard_error", data_id = "row_id"))

  setnames(claims_preds, "data_id", "row_id")

  #merge mid_age values on to predictions
  params <- adjust_dt[ ,c("row_id", "mid_age")]
  adjust_params <- merge(claims_preds, params, by = c("row_id"))

  adjust_cols <- unique(adjust_params[ ,c("pred_diff_mean", "pred_diff_sd", "mid_age")])

  #combine all data to be changed for inpatient status
  adjust_this <- copy(adjust_dt)
  inflate_this <- copy(inflate_dt)
  change_dt <- rbind(adjust_this, inflate_this)

  #merge on betas
  print("merging betas to bundle version")
  change_this <- merge(x = change_dt, y = adjust_cols, by = c("mid_age"), all.x = TRUE)

  #adjust mean & se
  change_this[mean != 0 ,`:=` (adj_mean = invlogit(logit(mean) - pred_diff_mean),
                               adj_se = sqrt(standard_error^2 + pred_diff_sd^2))]

  #inflate_se
  change_this[mean == 0, `:=` (adj_mean = copy(mean),
                               adj_se = sqrt(standard_error^2 + pred_diff_sd^2))]

  #update cols
  change_this[ ,crosswalk_parent_seq := seq]
  change_this[ ,seq := NA]

  #return changed dt
  adjvals_only <- copy(change_this)
  adjvals_only[ ,`:=` (mean = adj_mean, standard_error = adj_se)]
  adjvals_only[ ,c("adj_mean", "adj_se", "pred_diff_mean", "pred_diff_sd", "row_id", "mid_age") := NULL]

  all_vals <- copy(change_this)

  return(list(adjusted_only = adjvals_only, adjusted_all = all_vals ))

}

get_adjusted <- adjust_inp_data(statement = "adjusting and inflating yaaas")
formatted_adj_dt <- get_adjusted$adjusted_only
plotting_dt <- get_adjusted$adjusted_all

nrow(formatted_adj_dt) == nrow(plotting_dt)

#RECOMBINE ALL THE DATA & SAVE------------------------------------------------------------------------------------------------------------------------------
dim(ats_bvdata)
dim(formatted_adj_dt)
dim(leave_dt)

setdiff(names(formatted_adj_dt), names(leave_dt))
setdiff(names(leave_dt), names(formatted_adj_dt))

full_dt <- rbind(formatted_adj_dt, leave_dt, fill = TRUE)
full_dt$obs_dummy <- NULL
write.xlsx(x = full_dt, file = "FILEPATH", sheetName = "extraction")

#the below should be NA
unique(full_dt[!(clinical_data_type %in% c("claims")), crosswalk_parent_seq])

#FORMAT FOR UPLOAD------------------------------------------------------------------------------------------------------------------------------------------
prep_upload <- copy(full_dt)
prep_upload <- prep_upload[sex %in% c("Male", "Female") & group_review %in% c(1,NA)]

#PLOT THE ADJ CLINICAL DATA---------------------------------------------------------------------------------------------------------------------------------
#combine the plotting dt w/ old and new inpatient data and the leave dt

claims_old <- plotting_dt[, c("location_id", "location_name", "mean", "clinical_data_type", "mid_age", "sex")]
claims_new <- plotting_dt[, c("location_id", "location_name", "adj_mean", "clinical_data_type", "mid_age", "sex")]
inp_old <- leave_dt[clinical_data_type == "inpatient" & location_id %in% us_locs, c("location_id", "location_name", "mean", "clinical_data_type", "mid_age", "sex")]
inp_new <- copy(inp_old)

setnames(claims_new, "adj_mean", "mean")
claims_old$adj_group <- "unadjusted"
inp_old$adj_group <- "unadjusted"

claims_new$adj_group <- "adjusted"
inp_new$adj_group <- "adjusted"

to_graph <- rbind(claims_old,inp_old, claims_new, inp_new)

to_graph <- transform(to_graph, adj_group=factor(adj_group,levels=c("unadjusted", "adjusted")))
p <- 1

pdf("FILEPATH", width = 15)
for (loc in sort(unique(plotting_dt$location_id))) {
  message(p)
  p <- p+1
  loc_data <- to_graph[location_id == loc]
  loc_name <- unique(loc_data$location_name)
  print(loc_name)
  ymax <- max(loc_data$mean)


  data_plot <- ggplot() +
    geom_point(loc_data, mapping = aes(x = mid_age, y = mean, color = clinical_data_type, shape = sex)) +
    facet_wrap(~adj_group) +
    #scale_color_manual( values = group.colors) +
    ggtitle(paste0("Clinical Adjustment Adult Tertiary Syphilis: ",loc_name )) +
    xlab("Mid_Age") + ylab("Prevalence")  + ylim(0, ymax)
  print(data_plot)


}
dev.off()

#DELETE ROWS
modeling_dt <- full_dt[sex %in% c("Female", "Male")]
modeling_dt[standard_error >= 1, standard_error := 0.999]
modeling_dt <- modeling_dt[group_review %in% c(1,NA)]
write.xlsx(x = modeling_dt, file = "FILEPATH", sheetName = "extraction")


