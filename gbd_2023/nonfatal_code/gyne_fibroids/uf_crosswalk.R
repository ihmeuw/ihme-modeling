#################################################################################################
#Purpose: Adjust data for uterine fibroids
################################################################################################

#SETUP-----------------------------------------------------------------------------------------------
#rm(list=ls())
source(paste0(h_root,"FILEPATH"))
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
cause <- "uterine_fibroids"
uf_dir <- "FILEPATH"
match_dir <- paste0(uf_dir, "FILEPATH")
clinic_dir <- paste0(uf_dir, "FILEPATH")
acog_dir <- paste0(uf_dir, "FILEPATH")

#which bvid used to match/adjust
uf_bvid <- OBJECT

#matching params (inp to claims data only)
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

network_matched_fpath <- "FILEPATH"
print(network_matched_fpath)
network_matched_dt <- data.table(read.csv(network_matched_fpath))
network_matched_dt <- network_matched_dt[!is.na(alt_nid)]

##LOGIT PREPARE-----------------------------------------------------------------------------------------------------------------------------------------------
uf_logit_preparation <- function(dt, type){

  if (type == "network"){
    demo_cols <- dt[ ,c("alt_nid", "ref_nid", "age_group", "year_group", "sex", "location_id", "alt_composite", "ref_composite")]
  } else {
  #get identifying cols
    demo_cols <- dt[ ,c("alt_nid", "ref_nid", "age_group", "year_group", "sex", "location_id", "cdt_inpatient")]
  }
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
  if (type != "network"){
    all_transformed[cdt_inpatient == 1, `:=` (dorm_alt = "inpatient", dorm_ref = "claims")]
  }
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

uf_logit_dt <- uf_logit_preparation(dt = network_matched_dt, type = "network")
uf_logit_dt[ ,id2 := 1:nrow(uf_logit_dt)] #this is for adjust_orig_vals
write.csv(x = uf_logit_dt, file = paste0(acog_dir, "FILEPATH"), row.names = FALSE)
View(uf_logit_dt)
names(uf_logit_dt)

#PLOT LOGIT DIFFERENCES BY AGE (Inp-Claims Only)----------------------------------------------------------------------------------------------------------------------
plot_trans_by_age <- function(dt) { 
  #double check that matches are us matches
  uf_locs <- unique(uf_logit_dt$location_id)
  loc_ids[location_id %in% uf_locs, location_name]

  trans_type <- unique(dt$transformation)
  print(trans_type)

  pdf(("FILEPATH"), width = 15)

  mid_age <- ggplot(dt, aes(x=mid_age, y=logit_diff, color = year_group)) +geom_point() + labs(title = "USA: Inpatient-Claims Logit Difference (Uterine Fibroids)")
  #+ facet_wrap(~year_group)
  mid_age

  dev.off()

  return(mid_age)
}
plot_trans_by_age(dt = uf_logit_dt)

#RUN THAT MODEL------------------------------------------------------------------------------------------------------------------------------------------
model_type <- "network"

if (model_type == "network"){
  uf_dat1 <- CWData(
    df = uf_logit_dt,
    obs = "logit_diff",       # matched ratios in log space
    obs_se = "logit_diff_se", # SE of F/M ratio in log space
    alt_dorms = "alt_composite",   # var for the alternative def/method
    ref_dorms = "ref_composite",   # var for the reference def/method
    dorm_separator = "_",
    covs = list(),       # list of (potential) covariate columns
    study_id = "study_id", # var for random intercepts; i.e. (1|study_id)
    add_intercept = TRUE
  )
  uf_dat1
  model_covs <- list(crosswalk::CovModel(cov_name = "intercept"))
  model_gdorm <- "litacog"

} else if (model_type == "clinical"){
  uf_dat1 <- CWData(
    df = uf_logit_dt,
    obs = "logit_diff",       # matched ratios in log space
    obs_se = "logit_diff_se", # SE of F/M ratio in log space
    alt_dorms = "dorm_alt",   # var for the alternative def/method
    ref_dorms = "dorm_ref",   # var for the reference def/method
    covs = list("mid_age"),       # list of (potential) covariate columns
    study_id = "study_id"          # var for random intercepts; i.e. (1|study_id)
  )
  uf_dat1
  model_covs <- list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept"),
    CovModel(cov_name = "mid_age", spline = XSpline(knots = c(15,27,62, 104), degree = 3L, l_linear = FALSE, r_linear = FALSE)))
  model_gdorm <- "claims"
}


#run model
uf_fit1_pct <- 0.9
uf_fit1 <- CWModel(
  cwdata = uf_dat1,           # result of CWData() function call
  obs_type = "diff_logit", # must be "diff_logit" or "diff_log"
  cov_models = model_covs,
  gold_dorm = model_gdorm, # level of 'ref_dorms' that's the gold standard
  inlier_pct = uf_fit1_pct #1-pct(outlier)
)
uf_fit1

if (model_type == "network"){
  print(data.frame(
    beta_true = uf_fit1$fixed_vars,
    beta_mean = uf_fit1$beta,
    beta_se = uf_fit1$beta_sd,
    gamma = uf_fit1$gamma
  ))
  }

#PLOT
repl_python()
plots <- import("crosswalk.plots")

if (model_type == "network"){
  plot_vars <- names(uf_fit1$fixed_vars)[!(names(uf_fit1$fixed_vars) %in% c("inpatient", "litacog"))]

  for (var in plot_vars) {

    print(var)
    plot_fname <- paste0("acog_network_", var, "_int_pct", uf_fit1_pct, "_age10_yr10")
    plot_note <-paste0(var, " adjustment to ACOG: 10 yr age, 10 yr time, ", uf_fit1_pct*100,"% inclusion")

    plots$funnel_plot(
      cwmodel = uf_fit1,
      cwdata = uf_dat1,
      continuous_variables = list(),
      obs_method = var,
      plot_note = plot_note,
      plots_dir = paste0(acog_dir, "plots/"),
      file_name = plot_fname,
      write_file = TRUE)
  }

} else if (model_type == "clinical") {
  plots$dose_response_curve(
    dose_variable = "mid_age",
    obs_method = 'inpatient',
    continuous_variables = list(),
    cwdata = uf_dat1,
    cwmodel = uf_fit1,
    plot_note = paste0("Inpatient - Claims Logit Diff by Age Midpoint, ", uf_fit1_pct*100, "%"),
    plots_dir = paste0("plots/"),
    file_name = paste0("gold_nl_spline_4pts_pct", uf_fit1_pct),
    write_file = TRUE
  )
}

#SAVE CHOSEN CROSSWALK----------------------------------------------------------------------------------------------------------
#save csvs
df_result <- uf_fit1$create_result_df()
write.csv(df_result, paste0(acog_dir,"FILEPATH"), row.names = FALSE)

py_save_object(object = uf_fit1, filename = paste0(acog_dir, "FILEPATH"), pickle = "dill")
uf_fit1 <- py_load_object(filename = paste0(acog_dir, "FILEPATH"), pickle = "dill")


#FORMAT UF BV-DATA FOR INPATIENT ADJUSTMENTS------------------------------------------------------------------------------------------------------------------------------
uf_bvdata <- get_bundle_version(bundle_version_id = uf_bvid, fetch = "all", export = FALSE)
dim(uf_bvdata)
names(uf_bvdata)

loc_meta <- get_location_metadata()
us_locs <- loc_meta[parent_id == 102 | location_id == 102, location_id]

format_uf_data <- function(dt){

  dt[age_end == 124, age_end := 99]
  dt[ ,`:=` (obs_dummy = clinical_data_type, mid_age = (age_start + age_end)/2)]
  dt$row_id <- paste0("row", 1:nrow(dt))

  #leave this data alone for clinical xwalk
  leave <- dt[!(location_id %in% us_locs) |
                (location_id %in% us_locs & obs_dummy %in% c("", "claims"))]

  #inflate se of this data
  inflate <- dt[location_id %in% us_locs & obs_dummy == "inpatient" & mean == 0]

  #adjust mean and se of this data
  adjust <- dt[location_id %in% us_locs & obs_dummy == "inpatient" & mean != 0]

  if (nrow(leave) + nrow(inflate) + nrow(adjust) == nrow(dt)){
    "returning 3 data tables."
    return(list(leave_dt = leave, inflate_dt = inflate, adjust_dt = adjust))
  }else{
    print("Error: leave, inflate, and adjust are not mutually exclusive.")
  }
}

all_dts <- format_uf_data(dt = copy(uf_bvdata))
leave_dt <- all_dts$leave_dt
inflate_dt <- all_dts$inflate_dt
adjust_dt <- all_dts$adjust_dt

#FORMAT BETAS FROM CHOSEN CROSSWALK FUNCTION-----------------------------
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
beta_dt <- format_betas(rds_file = paste0(clinic_dir, "FILEPATH"))
beta_dt

#APPLY INPATIENT CROSSWALK TO ADJUST DT---------------------------------------------------------------------------------------------------------------------------------
adjust_inp_data <- function(statement){

  print(paste0(statement, ". anywho, getting predictions"))
  #get predictions by age
  inpatient_preds <- data.table(adjust_orig_vals(fit_object = uf_fit1, df = adjust_dt,
                                      orig_dorms = "obs_dummy", orig_vals_mean = "mean",
                                      orig_vals_se = "standard_error", data_id = "row_id"))

  setnames(inpatient_preds, "data_id", "row_id")

  #merge mid_age values on to predictions
  params <- adjust_dt[ ,c("row_id", "mid_age")]
  adjust_params <- merge(inpatient_preds, params, by = c("row_id"))

  adjust_cols <- unique(adjust_params[ ,c("pred_diff_mean", "pred_diff_sd", "mid_age")])
  adjust_cols <- adjust_cols[1:17]

  #combine all data to be changed in some regard for inpatient status
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
dim(formatted_adj_dt)
dim(leave_dt)

setdiff(names(formatted_adj_dt), names(leave_dt))
setdiff(names(leave_dt), names(formatted_adj_dt))

full_dt <- rbind(formatted_adj_dt, leave_dt, fill = TRUE)
full_dt$obs_dummy <- NULL
write.xlsx(x = full_dt, file = paste0(clinic_dir, "FILEPATH"), sheetName = "extraction")

#the below should be NA
unique(full_dt[!(clinical_data_type %in% c("claims", "inpatient")), crosswalk_parent_seq])

#FORMAT ACOG DATA & BETAS---------------------------------------------------------------------------------------------------------------------------------

#format betas
acog_beta_dt <- format_betas(rds_file = paste0(acog_dir, "FILEPATH"))
acog_beta_dt

#format data
format_for_acogadj <- function(dt){
  dt[nid == 120728, `:=` (cv_self_report = 0, cv_dx_symptomatic_only = 1)]
  dt[clinical_data_type %in% c("claims", "inpatient"), obs_dummy := "clinical"]
  dt[cv_case_def_acog == 1, obs_dummy := "litacog"]
  dt[cv_self_report == 1, obs_dummy := "selfreport"]
  dt[cv_dx_symptomatic_only == 1, obs_dummy := "symptomatic"]
  dt$row_id <- paste0("row", 1:nrow(dt))

  #leave this data alone for clinical xwalk
  leave <- dt[obs_dummy %in% c(NA, "litacog")]

  #inflate se of this data
  inflate <- dt[obs_dummy %in% c("clinical", "selfreport", "symptomatic") & mean == 0]

  #adjust mean and se of this data
  adjust <- dt[obs_dummy %in% c("clinical", "selfreport", "symptomatic") & mean != 0]

  if (nrow(leave) + nrow(inflate) + nrow(adjust) == nrow(dt)){
    print("returning 3 data tables.")
    return(list(leave_dt = leave, inflate_dt = inflate, adjust_dt = adjust))
  }else{
    print("Error: leave, inflate, and adjust are not mutually exclusive.")
  }
}

dt_for_acog <- data.table(read.xlsx(paste0(clinic_dir, "FILEPATH")))
acog_dts <- format_for_acogadj(dt = dt_for_acog)
acog_leave_dt <- acog_dts$leave_dt
acog_inflate_dt <- acog_dts$inflate_dt
acog_adjust_dt <- acog_dts$adjust_dt

#APPLY ACOG ADJUSTMENTS-----------------------------------------------------------------
adjust_acog_data <- function(statement){

  print(paste0(statement, ". anywho, getting ACOG predictions"))
  #combine all data to be changed in some regard for clinical or self_report status
  acog_adjust_this <- copy(acog_adjust_dt)
  acog_adjust_this[ ,type := "adjust"]

  acog_inflate_this <- copy(acog_inflate_dt)
  acog_inflate_this[ ,`:=` (mean = mean + 0.0001, type = "inflate")]

  acog_change_dt <- rbind(acog_adjust_this, acog_inflate_this, fill = TRUE)

  acog_preds <- data.table(adjust_orig_vals(fit_object = uf_fit1, df = acog_change_dt,
                                                 orig_dorms = "obs_dummy", orig_vals_mean = "mean",
                                                 orig_vals_se = "standard_error", data_id = "row_id"))

  setnames(acog_preds, "data_id", "row_id")

  #merge on betas
  print("merging betas to crosswalked bundle version")
  update_mean_se <- merge(x = acog_change_dt, y = acog_preds, by = c("row_id"), all.x = TRUE)

  plotting_dt <- copy(update_mean_se)
  plotting_dt[ ,dt_name := "for_plotting"]

  #adjust mean & se
  update_mean_se[type == "adjust",`:=` (adj_mean = ref_vals_mean,
                               adj_se = ref_vals_sd)]

  #inflate_se
  update_mean_se[type == "inflate", `:=` (adj_mean =  0,
                               adj_se = ref_vals_sd)]

  #update cols
  update_mean_se[is.na(crosswalk_parent_seq) ,crosswalk_parent_seq := seq]
  update_mean_se[!is.na(crosswalk_parent_seq) ,seq := NA]

  #return changed dt
  adjvals_only <- copy(update_mean_se)
  adjvals_only[ ,`:=` (mean = adj_mean, standard_error = adj_se)]
  adjvals_only[ ,c("lower", "upper") := NA]
  adjvals_only[ ,c("adj_mean", "adj_se", "pred_diff_mean", "pred_diff_sd", "type", "ref_vals_mean", "ref_vals_sd") := NULL]

  fully_formatted_dt <- rbind(adjvals_only, acog_leave_dt)
  all_vals <- copy(fully_formatted_dt)

  full_plotting_dt <- rbind(plotting_dt, acog_leave_dt, fill = TRUE)

  return(list(adjusted_all = all_vals, plot_use = full_plotting_dt ))

}

uf_fit1 <- py_load_object(filename = paste0(acog_dir, "FILEPATH"), pickle = "dill")
get_adjusted <- adjust_acog_data(statement = "adjusting and inflating yaaas")
adjusted_acog_dt <- get_adjusted$adjusted_all
acog_plotting_dt <- get_adjusted$plot_use
write.xlsx(x = adjusted_acog_dt, file = paste0(acog_dir, "FILEPATH"), sheetName = "extraction")

#NOW DO THE NETWORK ANALYSIS (OLD)------------------------------------------------------------------------------------------------------------------------------

#format xwalk coefficient
coefs_network <- data.table(read.csv("FILEPATH"))
coefs_network <- unique(coefs_network)
head(coefs_network)
names(coefs_network)

cov_names <- c("X_cv_clinical", "X_cv_self_report")
beta0 <- coefs_network$Y_mean # predicted ratios by age
beta0_se_tau <-  (coefs_network$Y_mean_hi - coefs_network$Y_mean_lo) / 3.92 # standard
test <- as.data.frame(cbind(coefs_network[ ,..cov_names],beta0, beta0_se_tau))
setnames(test, c("X_cv_clinical", "X_cv_self_report", "beta0", "beta0_se_tau"), c("cv_clinical", "cv_self_report", "pred_diff_mean", "pred_diff_sd"))
test

#only data marked for the clinical and self report rows
network_dt <- copy(full_dt)

adjust_network <- function(statement){

  print(paste0(statement, ". anywho, getting predictions"))

  #format network dt
  network_dt[clinical_data_type %in% c("inpatient", "claims") ,cv_clinical := 1]
  network_dt[is.na(cv_clinical), cv_clinical := 0]
  network_dt[is.na(cv_self_report), cv_self_report := 0]
  #network_dt[is.na(X_cv_symptomatic), X_cv_symptomatic :- 0]

  #merge mid_age values on to predictions
  no_adjust <- network_dt[cv_clinical == 0 & cv_self_report == 0]
  network_adjust <- network_dt[cv_clinical == 1 | cv_self_report == 1]

  nrow(no_adjust) + nrow(network_adjust) == nrow(network_dt)

  #merge on betas
  print("merging betas to bundle version")
  network_this <- merge(x = network_adjust, y = test, by = c("cv_clinical", "cv_self_report"), all.x = TRUE)

  #adjust mean & se
  network_this[mean != 0 ,`:=` (adj_mean = invlogit(logit(mean) - pred_diff_mean),
                               adj_se = sqrt(standard_error^2 + pred_diff_sd^2))]

  #inflate_se
  network_this[mean == 0, `:=` (adj_mean = copy(mean),
                               adj_se = sqrt(standard_error^2 + pred_diff_sd^2))]

  nrow(network_this[is.na(crosswalk_parent_seq)])
  network_this[(is.na(crosswalk_parent_seq)) ,crosswalk_parent_seq := seq]
  network_this[ ,seq := NA]

  #return changed dt
  adjvals_only <- copy(network_this)
  adjvals_only[ ,`:=` (mean = adj_mean)] #standard_error = adj_se
  adjvals_only[ ,c("adj_mean", "adj_se", "pred_diff_mean", "pred_diff_sd") := NULL]

  fully_formatted_dt <- rbind(adjvals_only, no_adjust)

  all_vals <- copy(change_this)

  return(list(adjusted_formatted = fully_formatted_dt, adjusted_all = all_vals ))

}

processed_network <- adjust_network(statement = "adjusting network finally")
adjusted_formatted_dt <- processed_network$adjusted_formatted
adjusted_all_dt <- processed_network$adjusted_all

adjusted_formatted_dt[standard_error >= 1 ,standard_error := 0.99]
write.xlsx(x = adjusted_formatted_dt, file = paste0(clinic_dir, "FILEPATH"), sheetName = "extraction")


#PLOT THE ADJ CLINICAL DATA---------------------------------------------------------------------------------------------------------------------------------
#combine the plotting dt w/ old and new inpatient data and the leave dt

inp_old <- plotting_dt[, c("location_id", "location_name", "mean", "clinical_data_type", "mid_age")]
inp_new <- plotting_dt[, c("location_id", "location_name", "adj_mean", "clinical_data_type", "mid_age")]
claims_old <- leave_dt[clinical_data_type == "claims" & location_id %in% us_locs, c("location_id", "location_name", "mean", "clinical_data_type", "mid_age")]
claims_new <- copy(claims_old)

setnames(inp_new, "adj_mean", "mean")
inp_old$adj_group <- "unadjusted"
claims_old$adj_group <- "unadjusted"

inp_new$adj_group <- "adjusted"
claims_new$adj_group <- "adjusted"
to_graph <- rbind(inp_old,claims_old, inp_new, claims_new)

to_graph <- transform(to_graph, adj_group=factor(adj_group,levels=c("unadjusted", "adjusted")))
p <- 1

pdf(paste0(clinic_dir, "FILEPATH"), width = 15)
for (loc in sort(unique(plotting_dt$location_id))) {
  message(p)
  p <- p+1
  loc_data <- to_graph[location_id == loc]
  loc_name <- unique(loc_data$location_name)
  print(loc_name)
  ymax <- max(loc_data$mean)


  data_plot <- ggplot() +
    geom_point(loc_data, mapping = aes(x = mid_age, y = mean, color = clinical_data_type)) +
    facet_wrap(~adj_group) +
    #scale_color_manual( values = group.colors) +
    ggtitle(paste0("Clinical Adjustment Uterine Fibroids: ",loc_name )) +
    xlab("Mid_Age") + ylab("Prevalence")  + ylim(0, ymax)
  print(data_plot)


}
dev.off()

#PLOT THE ADJ NETWORK DATA---------------------------------------------------------------------------------------------------------------------------------
#combine the plotting dt w/ old and new inpatient data and the leave dt
acog_plotting_dt[ ,mid_age := (age_start + age_end)/2]
acog_plotting_dt[is.na(ref_vals_mean), ref_vals_mean := mean]
acog_old <- acog_plotting_dt[, c("location_id", "location_name", "mean", "obs_dummy", "mid_age")]
acog_new <- acog_plotting_dt[, c("location_id", "location_name", "ref_vals_mean", "obs_dummy", "mid_age")]

setnames(acog_new, "ref_vals_mean", "mean")
acog_old$adj_group <- "unadjusted"
acog_new$adj_group <- "adjusted"
to_graph <- rbind(acog_old, acog_new)

to_graph <- transform(to_graph, adj_group=factor(adj_group,levels=c("unadjusted", "adjusted")))
to_graph[is.na(obs_dummy), obs_dummy := "none"]
p <- 1

pdf(paste0(acog_dir, "FILEPATH"), width = 15, )
for (loc in sort(unique(acog_plotting_dt$location_id))) {
  message(p)
  p <- p+1
  loc_data <- to_graph[location_id == loc]
  loc_name <- unique(loc_data$location_name)
  print(loc_name)
  ymax <- 1


  data_plot <- ggplot() +
    geom_point(loc_data, mapping = aes(x = mid_age, y = mean, color = obs_dummy)) +
    facet_wrap(~adj_group) +
    scale_colour_manual(values = c("clinical" = "forestgreen", "selfreport" = "blue", "litacog" = "red", "symptomatic" = "purple"))+
    ggtitle(paste0("ACOG Adjustment Uterine Fibroids: ",loc_name )) +
    xlab("Mid_Age") + ylab("Prevalence")  + ylim(0, ymax)
  print(data_plot)


}
dev.off()


