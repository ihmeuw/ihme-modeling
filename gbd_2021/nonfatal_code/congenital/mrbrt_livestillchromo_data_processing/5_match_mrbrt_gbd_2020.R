# #######################################################################################
# ############################# MATCH AND MR-BRT SCRIPT #################################
# #######################################################################################
# Purpose: 
#   - pulls in matched data between alternate to reference covariate values, and creates mean ratio and standarderror
#   - runs a mr-brt model and generates a plot mr-brt job call
# 
# Arguments:
#   - outdir -- where you want your mr-brt folder saved, users must change this to their own directory
#   - bundle -- bundle_id of data to run
#   - cv -- covariate of interest, only runs a single covariate at a time
#   - measure -- measure of data to be run, either prevalence or incidence
#       - user specified at the moment, as not all bundle data have both measures
#   - match_type -- either between or within study
#       - columns to match on can be changed within the function manually
#   - inlier_percent -- percentage of inliers, aka trimming desired
# 
# --> All of these arguments create the 'mod_lab' which is the folder that will be created in your outdir to hold outputs
#       - this allows a unique folder for every iteration of arguments you provide above
# #######################################################################################

### set environment
Sys.umask(mode = 002)
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- paste0("FILEPATH", Sys.getenv("USER"),"/")
}

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('readxl')

source(paste0("FILEPATH/get_ids.R"))
lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  })
source("FILEPATH/congenital_source_script.R")
# source livestill/chromos cov function: 
source(paste0(h, "FILEPATH/prep_cv_ls_ec_function.R"))

####################################################################################
# set arguments
##########################################

args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]
bun_name <- cong_map_dt[bundle==bun_id]$bundle_name
measure <- args[2]
inlier_percent <- args[4]
cv <- args[5]
print(args)


match_type <- 'within_study' # between_study or within_study
outdir <- paste0("FILEPATH/", cv, "/") # where you want your mr-brt folder saved
mod_lab <- paste0(bun_id, "_", cv, "_inlier_pct_", inlier_percent)

####################################################################################
# Run function to generate livestill and excludes chromos data  #
##########################################

euro <- read_excel(paste0("FILEPATH/", cv, "_eurocat.xlsx"))
matched_merge <- euro

matched_merge <- as.data.table(matched_merge)
matched_merge <- matched_merge[,year_id:=(year_start+year_end)/2]
matched_merge <- matched_merge[nid==128835]

##################################################################################
## Begin looping through bundles to create ratios and run mrbrt 
##################################################################################

  print(bun)
  rm(fit1)
  rm(df1)
  bun_name <- cong_map_dt[bundle==bun,]$bundle_name
  matched_merge <- matched_merge[bundle_id==bun,]

############################
# Generate ratios ##########
############################
  #drop rows where that loc/age/sex/year was only in the first dataset
  matched_merge <- matched_merge[!is.na(mean.y)]
  
  #drop rows where both means are 0
  matched_merge <- matched_merge[!(mean.x == 0 & mean.y == 0)]

  #add offset where one value is zero in order to be able to calculate std error
  offset <- 0.5 * median(matched_merge[mean.x != 0, mean.x])
  matched_merge[(mean.x == 0 | mean.y == 0), `:=` (mean.x = mean.x + offset,
                                                   mean.y = mean.y + offset)]
  matched_merge <- matched_merge[mean.x != 0 & mean.y != 0]
  
  #calculate the ratio and the standard error of the ratio (mean.y is cv_livestill=1, mean.x is cv_livestill=0)
  matched_merge <- matched_merge[, mean:= mean.y / mean.x]
  matched_merge <- matched_merge[, standard_error:= sqrt( (mean.x^2 / mean.y^2) * ( (standard_error.x^2 / mean.x^2) + (standard_error.y^2 / mean.y^2) ) ) ]

  if (cv == "cv_livestill") {
    if (nrow(matched_merge[mean < 1])) {
      matched_merge <- matched_merge[mean >= 1,]
    }
  }
  if (cv == "cv_excludes_chromos") {
    if (nrow(matched_merge[mean > 1])) {
      write.csv(matched_merge[mean > 1], paste0(outdir, 'bad_ratios/', mod_lab, '.csv'), row.names = FALSE)
      matched_merge <- matched_merge[mean <= 1]
    }
  }

    write.csv(matched_merge, paste0(outdir, mod_lab, "_inputdata.csv"), row.names = FALSE)

####################################################################################
# mrbrt start
##########################################
library("reticulate", lib.loc="FILEPATH/")
library("crosswalk", lib.loc = "FILEPATH/")
library(metafor, lib.loc = "FILEPATH/")
library(msm)
library(RhpcBLASctl, lib.loc = "FILEPATH/")
RhpcBLASctl::omp_set_num_threads(threads = 4)
    
dat_metareg <- as.data.table(matched_merge)
ratio_var <- "mean"
ratio_se_var <- "standard_error"
cov_names <- "nmr"
nid <- "nid"

############################################################
### RUN MRBRT ### 
###################
  ## LIVESTILL ## 
  if(cv %like% "livestill"){
      metareg_vars <- c(ratio_var, ratio_se_var, "nmr", nid, "underlying_nid")
  
      nmr <- get_life_table(with_shock = 1, with_hiv = 1, life_table_parameter_id = 1,
                            gbd_round_id = 6, decomp_step = 'step1', age_group_id = 2)
      nmr$age_group_id <- NULL
      setnames(nmr, 'mean', 'nmr')
      
      # Merge nmr onto the main dataset
      dat_metareg <- merge(dat_metareg, nmr, by = c('location_id', 'year_id', 'sex_id'))
      
      tmp_metareg <- as.data.table(dat_metareg)
      tmp_metareg <- tmp_metareg[,..metareg_vars]
      setnames(tmp_metareg, "mean", "ratio")
      setnames(tmp_metareg, "standard_error", "ratio_se")
      
      # log everything
      tmp <- crosswalk::delta_transform(mean = tmp_metareg$ratio, sd = tmp_metareg$ratio_se, transformation = "linear_to_log")
      tmp_metareg <- cbind(tmp_metareg, tmp)
      setnames(tmp_metareg, "mean_log", "ratio_log")
      setnames(tmp_metareg, "sd_log", "ratio_se_log")

      tmp_metareg <- as.data.table(tmp_metareg)
      df_matched <- as.data.table(tmp_metareg)
      df_matched$altvar = "alternate"
      df_matched$refvar = "reference"
      df_matched <- df_matched[,group_id:=paste0(nid, "_", underlying_nid)]
      df_matched <- df_matched[, nid:=NULL]
      df_matched <- df_matched[, underlying_nid:=NULL]
      
      write.csv(df_matched, paste0("FILEPATH/", cv, "_input_data_bundle_", bun, ".csv"))
      df_matched <- fread(paste0("FILEPATH/", cv, "_input_data_bundle_", bun, ".csv"))
      
      df_matched <- as.data.table(df_matched)
      bun_name <- cong_map_dt[bundle==bun,]$bundle_name

    # MRBRT data prep # 
      df1 <- CWData(
        df = df_matched,          # dataset for metaregression
        obs = "ratio_log",       # column name for the observation mean
        obs_se = "ratio_se_log", # column name for the observation standard error
        alt_dorms = "altvar",     # column name of the variable indicating the alternative method
        ref_dorms = "refvar",     # column name of the variable indicating the reference method
        covs = list("nmr"),     # names of columns to be used as covariates later
        study_id = "group_id",    # name of the column indicating group membership, usually the matching groups
        add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
      )
      py_save_object(object=df1, filename = paste0("FILEPATH/df1", bun, ".pkl"), pickle = "dill")
      
    # MRBRT run model # 
      knots <- c(min(df_matched$nmr), 0.1, 0.2, 0.3, 0.4, max(df_matched$nmr))

      fit1 <- CWModel(
        cwdata = df1,                # object returned by `CWData()`
        obs_type = "diff_log",       # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
        use_random_intercept = FALSE,
        cov_models = list(           # specifying predictors in the model; see help(CovModel)
          CovModel(cov_name = "intercept"),
          CovModel(cov_name="nmr")),
        gold_dorm = "reference",   # the level of `alt_dorms` that indicates it's the gold standard
        inlier_pct = inlier_percent,
        outer_max_iter = 10L
      )
      
      # py_help(fit1$fit)
      py_save_object(object = fit1, filename = paste0("FILEPATH/fit1", bun, ".pkl"), pickle = "dill")
      df_result <- fit1$create_result_df()
      
      print(paste0("saving betas FILEPATH/", bun, ".csv"))
      write.csv(df_result, paste0("FILEPATH/", bun, ".csv"))
      
    # MRBRT predict out # 
      
      # Add detail 
      case_name_detail_df <- data.table()
      ratio_df <- data.table()
      ratio_df <- ratio_df[, crosswalk := cv]
      ratio_df <- ratio_df[, bundle := bun]
      beta_dir <- paste0('FILEPATH/', bun, '.csv')
      ratio <- fread(beta_dir) %>% as.data.table()
      intercept <- ratio[dorms == 'alternate']$beta
      intercept_se <- ratio[dorms == 'alternate']$beta_sd
      lower <- intercept - 1.96 * intercept_se
      upper <- intercept + 1.96 * intercept_se
      exp_intercept <- signif(exp(intercept), digits = 4)
      exp_lower <- signif(exp(lower), digits = 4)
      exp_upper <- signif(exp(upper), digits = 4)
      intercept <- signif(intercept, digits = 4)
      lower <- signif(lower, digits = 4)
      upper <- signif(upper, digits = 4)
      ratio_df <- ratio_df[, intercept := paste0(intercept, " (", lower, ", ", upper, ")")]
      ratio_df <- ratio_df[, intercept_exp := paste0(intercept_beta, " (", exp_lower, ", ", exp_upper, ")")]

      ratio_df <- ratio_df[, trim := .1]
      case_name_detail_df <- rbind(case_name_detail_df, ratio_df)

      write.csv(case_name_detail_df, paste0('FILEPATH/detail_', bun, '.csv'))
      
      repl_python()
      plots <- import("crosswalk.plots")
      
      # Plot some plots
      title <- paste0("Bundle ", bun_name, " (bun_id = ", bun, "), Crosswalk: ", cv, ", Inlier percent: ", inlier_percent, " (", Sys.Date(), ")" )
      
      plots$dose_response_curve(
        dose_variable = 'nmr',
        obs_method = 'alternate', 
        continuous_variables=list(), 
        cwdata=df1, 
        cwmodel=fit1, 
        plot_note=title,
        plots_dir=paste0('FILEPATH/', cv, '/'),
        file_name = paste0("dose_response_plot_no_spline_", bun), 
        write_file=TRUE)
      

    } 
  
###################
  ## EXCLUDES CHROMOS 
  if(cv %like% "excludes_chromos"){
      metareg_vars <- c(ratio_var, ratio_se_var, "nid", "underlying_nid")
      tmp_metareg <- as.data.table(dat_metareg)
      tmp_metareg <- tmp_metareg[,..metareg_vars]
      setnames(tmp_metareg, "mean", "ratio")
      setnames(tmp_metareg, "standard_error", "ratio_se")
      
      # log everything
      tmp <- crosswalk::delta_transform(mean = tmp_metareg$ratio, sd = tmp_metareg$ratio_se, transformation = "linear_to_log")
      tmp_metareg <- cbind(tmp_metareg, tmp)
      setnames(tmp_metareg, "mean_log", "ratio_log")
      setnames(tmp_metareg, "sd_log", "ratio_se_log")

      df_matched <- as.data.table(tmp_metareg)
      df_matched$altvar = "alternate"
      df_matched$refvar = "reference"
      df_matched <- df_matched[,group_id:=paste0(nid, "_", underlying_nid)]
      df_matched <- df_matched[, nid:=NULL]
      df_matched <- df_matched[, underlying_nid:=NULL]
      
      df_matched <- df_matched[!(is.na(ratio) | ratio==1)]
      
      
    # MRBRT data prep # 
      df1 <- CWData(
        df = df_matched,          # dataset for metaregression
        obs = "ratio_log",       # column name for the observation mean
        obs_se = "ratio_se_log", # column name for the observation standard error
        alt_dorms = "altvar",     # column name of the variable indicating the alternative method
        ref_dorms = "refvar",     # column name of the variable indicating the reference method
        covs = list(),     # names of columns to be used as covariates later
        study_id = "group_id",    # name of the column indicating group membership, usually the matching groups
        add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
      )
      py_save_object(object=df1, filename = paste0("FILEPATH/df1", bun, ".pkl"), pickle = "dill")
      
    # MRBRT run model # 
      fit1 <- CWModel(
        cwdata = df1,            # object returned by `CWData()`
        obs_type = "diff_log", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
        use_random_intercept = TRUE,
        cov_models = list(       # specifying predictors in the model; see help(CovModel)
          CovModel("intercept")),
        gold_dorm = "reference",   # the level of `alt_dorms` that indicates it's the gold standard
        # this will be useful when we can have multiple "reference" groups in NMA
        inlier_pct = inlier_percent
      )
      py_save_object(object = fit1, filename = paste0("FILEPATH/fit1", bun, ".pkl"), pickle = "dill")
      df_result <- fit1$create_result_df()
      
      
      print(paste0("saving betas FILEPATH/", bun, ".csv"))
      write.csv(df_result, paste0("FILEPATH/", bun, ".csv"))
  
    # MRBRT funnel plots # 
      # Add detail 
      case_name_detail_df <- data.table()
        ratio_df <- data.table()
        ratio_df <- ratio_df[, crosswalk := cv]
        ratio_df <- ratio_df[, bundle := bun]
        beta_dir <- paste0('FILEPATH/', bun, '.csv')
        ratio <- fread(beta_dir) %>% as.data.table()
        beta <- ratio[dorms == 'alternate']$beta
        beta_se <- ratio[dorms == 'alternate']$beta_sd
        lower <- beta - 1.96 * beta_se
        upper <- beta + 1.96 * beta_se
        exp_beta <- signif(exp(beta), digits = 4)
        exp_lower <- signif(exp(lower), digits = 4)
        exp_upper <- signif(exp(upper), digits = 4)
        beta <- signif(beta, digits = 4)
        lower <- signif(lower, digits = 4)
        upper <- signif(upper, digits = 4)
        ratio_df <- ratio_df[, beta := paste0(beta, " (", lower, ", ", upper, ")")]
        ratio_df <- ratio_df[, beta_exp := paste0(exp_beta, " (", exp_lower, ", ", exp_upper, ")")]
        
        ratio_df <- ratio_df[, trim := .1]
        case_name_detail_df <- rbind(case_name_detail_df, ratio_df)

      write.csv(case_name_detail_df, paste0('FILEPATH/detail_', bun, '.csv'))
      
      repl_python()
      plots <- import("crosswalk.plots")
      
      # Plot some plots
      title <- paste0("Bundle ", bun_name, " (bun_id = ", bun, "), Crosswalk: ", cv, ", Inlier percent: ", inlier_percent, " (", Sys.Date(), ")" )
      
      fit1 <- py_load_object(filename = paste0("FILEPATH/fit1", bun, ".pkl"), pickle = "dill")
      df1 <- py_load_object(filename = paste0("FILEPATH/df1", bun, ".pkl"), pickle = "dill")
      
      plots$funnel_plot(
        cwmodel = fit1,
        cwdata = df1,
        continuous_variables = list(),
        obs_method = 'alternate',
        plot_note = title,
        plots_dir = paste0('FILEPATH', cv, '/'),
        file_name = paste0("funnel_plot_", bun),
        write_file = TRUE
      )
      
  }
