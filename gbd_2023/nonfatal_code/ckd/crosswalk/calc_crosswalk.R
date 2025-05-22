#--------------------------------------------------------------
# Project: GBD Non-fatal CKD Estimation - Crosswalks
# Purpose: Prep microdata sources to estimate coefficients for the estimating equation and acr threshold crosswalks outside of DisMod
#          and run MRBRT on prepped CKD crosswalk data
#--------------------------------------------------------------

# HOUSEKEEPING SET UP -------------------------------------------------------
rm(list = ls())
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

user <- Sys.info()[["user"]]
ckd_repo<- paste0("FILEPATH",user,"FILEPATH")
source(paste0(ckd_repo,"general_func_lib.R"))
source(paste0(ckd_repo, "FILEPATH/data_processing_functions.R"))
output_dir <- "FILEPATH"
source_shared_functions("get_location_metadata")

# Import packages
require(data.table)
require(ggplot2)
require(msm)
library(dplyr)
library(reticulate)
reticulate::use_python("FILEPATH/python")
cw <- reticulate::import("crosswalk")
plots <- reticulate::import("crosswalk.plots")

# Specify filepaths to most recent winnower tabulations 
microdata_age_spec_file<-paste0(j_root,"FILEPATH/collapse_ckd_GBD2022.csv") # must be a .csv

# Pull in file mapping stages to crosswalks
cv_ref_map_path <- paste0(j_root,"FILEPATH/stage_cv_reference_map.csv") # must be a .csv # make sure that all possible crosswalks needed are a row in this file

# Set output filepaths for prepped data, diagnostic plots, and MRBRT model outputs, create directory for this run
path_notes <- "age_spline" # this will be the name of the new directory to store outputs/plots. Should be either "age_spline" or "no_covs"

out_dir<-paste0(output_dir,"FILEPATH", path_notes)
plot_dir<-paste0(output_dir,"FILEPATH", path_notes)
mrbrt_dir<-paste0(output_dir,"FILEPATH", path_notes)

dir.create(paste0(output_dir,"FILEPATH", path_notes), showWarnings = F)
dir.create(paste0(output_dir,"FILEPATH", path_notes), showWarnings = F)
dir.create(paste0(output_dir,"FILEPATH", path_notes), showWarnings = F)

# Set release id 
release_id <- 16

# Set match vars 
match_vars<-c("nid","ihme_loc_id","year_start","year_end","sex_id","age_start","age_end")
# ----------------------------------------------

# Prep microdata sources ------------------------------------------------------------------
# Loop for each stage-crosswalk combo in the mapping file
set.seed(555)
cv_ref_map <- fread(cv_ref_map_path)

for (map_row in 1:nrow(cv_ref_map)) {
  # Pull in settings from map
  stg <- cv_ref_map[map_row, stage]
  message(stg)
  study_level_cov <- unlist(strsplit(cv_ref_map[map_row, cv], split = ","))
  message(paste(study_level_cov, collapse = " "))
  study_level_cov_ref <- unlist(strsplit(cv_ref_map[map_row, reference], split = ","))
  message(paste(study_level_cov_ref, collapse = " "))
  study_level_cov_alt <- unlist(strsplit(cv_ref_map[map_row, alt], split = ","))
  message(paste(study_level_cov_alt, collapse = " "))
  comparison <- paste0(
    stg, "_", paste0(study_level_cov, collapse = "_"), "_",
    paste0(study_level_cov_alt, collapse = "_"), "_to_",
    paste0(study_level_cov_ref, collapse = "_")
  )
  message(comparison)
  xwalk_type <- cv_ref_map[map_row, xwalk_type]
  message(xwalk_type)
  
  # Read in collapsed data file
  dt <- fread(microdata_age_spec_file)
  
  dt <- unique(dt)
  
  # Don't keep subnational tabulations of the same survey - only use highest level
  # tabulation
  loc_dt <- get_location_metadata(35, release_id = release_id)
  loc_dt_level <- loc_dt[, .(ihme_loc_id, level)]
  dt <- merge(dt, loc_dt_level, by = "ihme_loc_id")
  dt[, min_level := min(level), by = c("nid", "survey_name")]
  dt <- dt[min_level == level]
  
  dt[age_end > 99, age_end := 99]
  
  # Create crosswalk variables -
  # (1) Equation: pull out equation -- if it doesn't say "mdrd" or "cg" in the var name,
  #     then it was calculated with CKD-EPI
  dt[, equation := ifelse(grepl("mdrd", var), "mdrd", ifelse(grepl("cg", var), "cg",ifelse(grepl("2009", var), "ckd_epi", "ckd_epi")))]
  # Remove the suffixes
  dt[, var := gsub("_mdrd", "", var)]
  dt[, var := gsub("_cg", "", var)]
  dt[, var := gsub("_2009", "", var)]
  dt[, var := gsub("_ckd_epi", "", var)]
  # (2) Threshold: pull out albuminuria threshold - assign it to 30 when missing
  dt[grepl("albuminuria", var), threshold := tstrsplit(x = var, split = "_", keep = 2)]
  dt[grepl("albuminuria", var) & is.na(threshold), threshold := "30"]
  # Remove the suffixes on var and create a stage variable
  dt[, var := gsub(paste(paste0("_", unique(dt[, na.omit(threshold)])), collapse = "|"), "", var)]
  dt[, stage := factor(var)]
  # Drop "var"
  dt[, var := NULL]
  
  # Estimating equation comparisons only apply to data age 18+ (all under 18 sources use dt schwartz)
  dt <- dt[age_end >= 18]
  
  # Dropping rows with < 10 sample size 
  dt <- dt[sample_size >= 10]
  
  dt[is.na(standard_error), standard_error := sqrt((mean * (1 - mean)) / sample_size)]
  
  # Create crosswalk input datasets for each stage
  dt <- dt[stage == stg]
  
  # If converting Stage 1-2 thresholds then you need to create a threshold_equation column 
  
  if(study_level_cov == "threshold_equation"){
    dt[, "threshold_equation" := paste0(threshold, "_", equation)]
  } else {
    message("Not creating threshold_equation column because not crosswalking Stage 1-2")
  }
  
  # Only keep rows for specified comparison ************************
  dt <- copy(dt[eval(parse(text = paste0(study_level_cov, " %in% c('", study_level_cov_alt, "','", study_level_cov_ref, "')", collapse = "&")))])
  
  # Check that there are no duplicate rows
  message("checking for duplicate rows of data")
  if (!(identical(unique(dt), dt))) stop("There are duplicate rows in the microdata_dt, please supply a unique dt")
  
  # dropping rows where mean = 0 or 1
  message("dropping rows where mean = 0")
  drop_count <- nrow(dt[mean == 0 | mean == 1])
  tot_count <- nrow(dt)
  dt <- dt[!(mean == 0 | mean == 1)]
  message(paste0("dropped ", drop_count, " of ", tot_count, " rows"))
  
  # Subset to a dt where data was extracted using only reference
  message("separating into reference and alterate datasets")
  dt_ref <- dt[eval(parse(text = paste0(study_level_cov, "== c('", study_level_cov_ref, "')", collapse = "&")))]
  dt_alt <- dt[eval(parse(text = paste0(study_level_cov, "== c('", study_level_cov_alt, "')", collapse = "&")))]
  
  # Set names of comparison columns
  comp_cols <- c("mean", "standard_error", study_level_cov)
  setnames(dt_ref, comp_cols, paste0(comp_cols, "_ref"))
  setnames(dt_alt, comp_cols, paste0(comp_cols, "_alt"))
  
  # Merge ref and alt data by match variables
  message(paste("merging reference and alternate dataset. matching on", paste(match_vars, collapse = ", ")))
  dt_merge <- merge(dt_ref, dt_alt, by = match_vars)
  
  # Transform alternate and ref into appropriate space
  message("Calculating logit difference and logit difference se")
  # Logit alternative
  dt_merge[, c("mean_alt_logit", "standard_error_alt_logit")]<-as.data.frame(
    cw$utils$linear_to_logit(
      mean = array(dt_merge$mean_alt),
      sd = array(dt_merge$standard_error_alt))
  )
  
  # Logit reference
  dt_merge[, c("mean_ref_logit", "standard_error_ref_logit")]<-as.data.frame(
    cw$utils$linear_to_logit(
      mean = array(dt_merge$mean_ref),
      sd = array(dt_merge$standard_error_ref))
  )
  
  # Logit difference 
  dt_merge<-  dt_merge %>%
    mutate(
      diff_logit_mean = mean_alt_logit - mean_ref_logit,
      diff_logit_se = sqrt(standard_error_alt_logit^2 + standard_error_ref_logit^2)
    )
  
  
  # Make dummy variable for comparison
  message("creating study-level covariate dummy.")
  dt_merge[, (paste0("cv_", study_level_cov, "_", study_level_cov_alt)) := 1]
  
  # Remove .x and .y vars
  merge_vars <- grep("\\.x|\\.y", names(dt_merge), value = T)
  dt_merge[, (merge_vars) := NULL]
  
  # Write outputs
  write.csv(x = dt_merge, file = paste0(out_dir, "/", comparison, ".csv"), row.names = F)
  message(paste0('writing CSV: ',out_dir, "/", comparison, ".csv" ))
  
  # Make vars for plotting
  dt_merge[, age_bin := paste(age_start, "to", age_end)]
  dt_merge[sex_id == 1, sex := "Male"]
  dt_merge[sex_id == 2, sex := "Female"]
  
  # Write plots
  # Scatters
  message('creating diagnostics ')
  pdf(paste0(plot_dir, "/", comparison, "_scatters_age_facet.pdf"), width = 12)
  gg <- if (nrow(dt_merge) > 0) {
    ggplot(data = dt_merge) +
      geom_point(aes(x = mean_ref, y = mean_alt, col = ihme_loc_id)) +
      facet_wrap(~age_bin, scales = "free") +
      geom_abline() +
      theme_bw() +
      xlab(paste0("Reference: Mean - ", study_level_cov_ref)) +
      ylab(paste0("Alternative: Mean - ", study_level_cov_alt)) +
      ggtitle(paste0(
        simpleCap(gsub("_", " ", stg)), " ", study_level_cov, " crosswalk - ",
        study_level_cov_alt, " to ", study_level_cov_ref
      ))
  }
  print(gg)
  dev.off()
  
  pdf(paste0(plot_dir, "/", comparison, "_scatters_sex_facet.pdf"), width = 12)
  gg <- if (nrow(dt_merge) > 0) {
    ggplot(data = dt_merge) +
      geom_point(aes(x = mean_ref, y = mean_alt, col = ihme_loc_id)) +
      facet_wrap(~sex, scales = "free") +
      geom_abline() +
      theme_bw() +
      xlab(paste0("Reference: Mean - ", study_level_cov_ref)) +
      ylab(paste0("Alternative: Mean - ", study_level_cov_alt)) +
      ggtitle(paste0(
        simpleCap(gsub("_", " ", stg)), " ", study_level_cov, " crosswalk - ",
        study_level_cov_alt, " to ", study_level_cov_ref
      ))
  }
  print(gg)
  dev.off()
  
  pdf(paste0(plot_dir, "/", comparison, "_scatters.pdf"), width = 12)
  gg <- if (nrow(dt_merge) > 0) {
    ggplot(data = dt_merge) +
      geom_point(aes(x = mean_ref, y = mean_alt, col = ihme_loc_id)) +
      geom_abline() +
      theme_bw() +
      xlab(paste0("Reference: Mean - ", study_level_cov_ref)) +
      ylab(paste0("Alternative: Mean - ", study_level_cov_alt)) +
      ggtitle(paste0(
        simpleCap(gsub("_", " ", stg)), " ", study_level_cov, " crosswalk - ",
        study_level_cov_alt, " to ", study_level_cov_ref
      ))
  }
  print(gg)
  dev.off()
  
  #Plot ratios
  pdf(paste0(plot_dir, "/", comparison, "_ratios_by_age_sex.pdf"), width = 12)
  gg <- if (nrow(dt_merge) > 0) {
    ggplot(data = dt_merge, aes(x = age_start, y = diff_logit_mean, col = ihme_loc_id)) +
      geom_jitter(size = 2, width = 1, alpha = .5) +
      facet_wrap(~sex) +
      theme_bw() +
      ggtitle(paste0(
        simpleCap(gsub("_", " ", stg)), " ", study_level_cov, " crosswalk - ",
        study_level_cov_alt, " to ", study_level_cov_ref
      ))
  }
  print(gg)
  dev.off()
  
  #Plot mean vs standard error
  pdf(paste0(plot_dir, "/", comparison, "_mean_se.pdf"), width = 12)
  gg <- ggplot(data = dt_merge, use.names = T, fill = T) +
    aes(x = diff_logit_mean, y = diff_logit_se, col = ihme_loc_id) +
    geom_point(alpha = 0.5) +
    theme_bw() +
    ggtitle(paste0(
      simpleCap(gsub("_", " ", stg)), " ", study_level_cov, " crosswalk - ",
      study_level_cov_alt, " to ", study_level_cov_ref
    ))
  print(gg)
  dev.off()
  
}

#   -----------------------------------------------------------------------
# Run MRBRT model ------------------------------------------------------------------
# Loop for each stage-crosswalk combo in the mapping file
cv_ref_map <- fread(cv_ref_map_path)
set.seed(555)

for (map_row in 1:nrow(cv_ref_map)) {
  # Pull in settings from map 
  stg<-cv_ref_map[map_row,stage]
  message(stg)
  study_level_cov<-unlist(strsplit(cv_ref_map[map_row,cv],split = ","))
  message(paste(study_level_cov,collapse = " "))
  study_level_cov_ref<-unlist(strsplit(cv_ref_map[map_row,reference],split = ","))
  message(paste(study_level_cov_ref,collapse = " "))
  study_level_cov_alt<-unlist(strsplit(cv_ref_map[map_row,alt],split = ","))
  message(paste(study_level_cov_alt,collapse = " "))
  comparison<-paste0(stg,"_",paste0(study_level_cov,collapse="_"),"_",
                     paste0(study_level_cov_alt,collapse="_"),"_to_",
                     paste0(study_level_cov_ref,collapse="_"))
  message(comparison)
  xwalk_type<-cv_ref_map[map_row,xwalk_type]
  message(xwalk_type)
  
  # read in prepped data for specified stage
  dt<-fread(paste0(out_dir, "/", comparison, ".csv"))
  
  # calculate any desired cov columns
  dt[,age_midpoint:=(age_start+age_end)/2]
  dt[,age_sex:=age_midpoint*sex_id]
  
  # drop rows where val or SE are NA
  dt<-dt[!is.na(diff_logit_se)]
  dt<-dt[!is.na(diff_logit_mean)]
  
  # CWData(), CovModel() and CWModel() functions
  dt$id <- as.integer(as.factor(dt$nid)) # ...housekeeping
  
  # format data for meta-regression; pass in data.frame and variable names
  if (xwalk_type == "all_age") {
    if (stg == "albuminuria") {
      dat1 <- cw$CWData(
        df = dt,
        obs = "diff_logit_mean",   # matched differences in log space
        obs_se = "diff_logit_se",  # SE of matched differences in log space
        alt_dorms = "threshold_equation_alt",   # var for the alternative def/method
        ref_dorms = "threshold_equation_ref",   # var for the reference def/method
        covs = list(),            # list of (potential) covariate column names
        study_id = "id", # var for random intercepts; i.e. (1|study_id)
        add_intercept = TRUE
      )
      
      fit1 <- cw$CWModel(
        cwdata = dat1,           # result of CWData() function call
        obs_type = "diff_logit",   # must be "diff_logit" or "diff_log"
        cov_models = list(       # specify covariate details
          cw$CovModel("intercept") ),
        gold_dorm = "30_ckd_epi" # level of 'ref_dorms' that's the gold standard
      )
      
      fit1$fit(inlier_pct=0.9)
      
    } else {
      dat1 <- cw$CWData(
        df = dt,
        obs = "diff_logit_mean",   # matched differences in log space
        obs_se = "diff_logit_se",  # SE of matched differences in log space
        alt_dorms = "equation_alt",   # var for the alternative def/method
        ref_dorms = "equation_ref",   # var for the reference def/method
        covs = list(),            # list of (potential) covariate column names
        study_id = "id",          # var for random intercepts; i.e. (1|study_id)
        add_intercept = TRUE
      )
      
      fit1 <- cw$CWModel(
        cwdata = dat1,           # result of CWData() function call
        obs_type = "diff_logit",   # must be "diff_logit" or "diff_log"
        cov_models = list(       # specify covariate details
          cw$CovModel("intercept") ),
        gold_dorm = "ckd_epi"  # level of 'ref_dorms' that's the gold standard
      )
      
      fit1$fit(inlier_pct=0.9)
      
    }
  }
  
  if (xwalk_type == "age_spec") {
    if (stg == "albuminuria") {
      dat1 <- cw$CWData(
        df = dt,
        obs = "diff_logit_mean",   # matched differences in log space
        obs_se = "diff_logit_se",  # SE of matched differences in log space
        alt_dorms = "threshold_equation_alt",   # var for the alternative def/method
        ref_dorms = "threshold_equation_ref",   # var for the reference def/method
        covs = list("age_midpoint"),            # list of (potential) covariate column names
        study_id = "id", # var for random intercepts; i.e. (1|study_id)
        add_intercept = TRUE,
        dorm_separator = " "
      )
      
      fit1 <- cw$CWModel(
        cwdata = dat1,           # result of CWData() function call
        obs_type = "diff_logit",   # must be "diff_logit" or "diff_log"
        cov_models = list(       # specify covariate details
          cw$CovModel("intercept"),
          cw$CovModel(cov_name = "age_midpoint", spline = cw$XSpline(knots = c(0,25,50,75,100), degree = 3L, l_linear = TRUE, r_linear = TRUE))),
        gold_dorm = "30_ckd_epi" # level of 'ref_dorms' that's the gold standard
      )
      
      fit1$fit(inlier_pct=0.9)
      
    } else {
      dat1 <- cw$CWData(
        df = dt,
        obs = "diff_logit_mean",   # matched differences in log space
        obs_se = "diff_logit_se",  # SE of matched differences in log space
        alt_dorms = "equation_alt",   # var for the alternative def/method
        ref_dorms = "equation_ref",   # var for the reference def/method
        covs = list("age_midpoint"), # list of (potential) covariate column names
        study_id = "id",          # var for random intercepts; i.e. (1|study_id)
        add_intercept = TRUE,
        dorm_separator = " "
      )
      
      fit1 <- cw$CWModel(
        cwdata = dat1,           # result of CWData() function call
        obs_type = "diff_logit",   # must be "diff_logit" or "diff_log"
        cov_models = list(       # specify covariate details
          cw$CovModel("intercept"),
          cw$CovModel(cov_name = "age_midpoint", spline = cw$XSpline(knots = c(0,25,50,75,100), degree = 3L, l_linear = TRUE, r_linear = TRUE))),
        gold_dorm = "ckd_epi"  # level of 'ref_dorms' that's the gold standard
      )
      
      fit1$fit(inlier_pct=0.9)
      
    }
  }
  
  # model predictions
  df_tmp<-as.data.table(fit1$create_result_df())
  
  # writing model results
  cols<-c("dorms", "cov_names", "beta", "beta_sd", "gamma")
  df_tmp2<-df_tmp %>% dplyr::select(cols) %>% filter(dorms==study_level_cov_alt)
  df_tmp2$gamma<- unique(df_tmp[!is.na(gamma),]$gamma)
  
  write.csv(x = df_tmp, file = paste0(mrbrt_dir, "/", comparison, "_full_predictions.csv"), row.names = F)
  write.csv(x = df_tmp2, file = paste0(mrbrt_dir, "/", comparison, "_summary_predictions.csv"), row.names = F)
  
  # save model object
  py_save_object(object = fit1, filename = paste0(mrbrt_dir, "/", comparison, "_mrbrt_object.pkl"), pickle = "dill")
  
  # funnel plot
  if (xwalk_type == "all_age") {
    plots$funnel_plot(
      cwmodel = fit1,
      cwdata = dat1,
      continuous_variables = list(),
      obs_method = study_level_cov_alt,
      plot_note = paste0(study_level_cov_alt," vs ",study_level_cov_ref),
      plots_dir = paste0(plot_dir, "/"),
      file_name = paste0(comparison,"_mrbrt_funnel_plot"),
      write_file = TRUE
    )
  }
  
  if (xwalk_type == "age_spec") {
    plots$funnel_plot(
      cwmodel = fit1,
      cwdata = dat1,
      continuous_variables = list("age_midpoint"),
      obs_method = study_level_cov_alt,
      plot_note = paste0(study_level_cov_alt," vs ",study_level_cov_ref),
      plots_dir = paste0(plot_dir, "/"),
      file_name = paste0(comparison,"_mrbrt_funnel_plot"),
      write_file = TRUE
    )
    
    plots$dose_response_curve(
      dose_variable = 'age_midpoint',
      obs_method = study_level_cov_alt, 
      continuous_variables=list(), 
      cwdata=dat1, 
      cwmodel=fit1, 
      plot_note=paste0(study_level_cov_alt," vs ",study_level_cov_ref), 
      plots_dir=paste0(plot_dir, "/"), 
      file_name = paste0(comparison,"_mrbrt_dose_response_plot"), 
      write_file=TRUE)
  }
}


# ------------------------------------------------------------------