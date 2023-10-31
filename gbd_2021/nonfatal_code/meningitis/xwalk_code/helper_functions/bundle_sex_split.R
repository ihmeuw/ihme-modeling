#' @author 
#' @date 2019/04/17
#' @description Sex-split GBD 2019 decomp2
#' @function run_sex_split
#' @param dt bundle data used to be sex split
#' @param model_name name of the directory in the mrbrt_dir where MR-BRT outputs are stored
#' @return predict_sex$final : Sex-split dataset
#' @return predict_sex$graph : Plot of both-sex mean against sex-split means saved in mrbrt_dir

pacman::p_load(data.table, parallel)
library(crosswalk, lib.loc = "/filepath/")

# SOURCE FUNCTIONS --------------------------------------------------------
# Source all GBD shared functions at once
shared.dir <- "/filepath/"
files.sources <- list.files(shared.dir)
files.sources <- paste0(shared.dir, files.sources) # writes the filepath to each function
invisible(sapply(files.sources, source)) # "invisible suppresses the sapply output

# SET OBJECTS -------------------------------------------------------------
sex_split_response <- "logit_diff" # user-defined
sex_split_data_se <- "logit_diff_se"
sex_mrbrt_response <- "diff_logit" #mr-brt defined 
sex_covs <- NULL 
id_vars <- "id_var"
sex_remove_x_intercept <- F
save_sex_split_xwalk <- F

# FUNCTIONS ---------------------------------------------------------------
## FIND MATCHES ON NID, AGE, LOCATION, MEASURE, AND YEAR
find_sex_match <- function(dt,
                           offset = T,
                           drop_zeros = F, 
                           fix_ones = T,
                           quiet = F) {
  sex_dt <- copy(dt)
  # Don't use data that is split elsewhere in the bundle (i.e, input_type == parent)
  sex_dt <- sex_dt[input_type != "parent" | is.na(input_type)]
  # Don't use data that was outliered
  sex_dt <- sex_dt[is_outlier != 1]
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% xw_measure]
  # Use rm_zeros to address zeros
  sex_dt <- rm_zeros(sex_dt, 
                     offset = offset,
                     fix_ones = fix_ones,
                     drop_zeros = drop_zeros)
  # Find rows that have corresponding male and female rows
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end")
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean", "standard_error", "cases", "sample_size")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, all_of(keep_vars))
  sex_dt <- data.table::dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error", "cases", "sample_size"))
  sex_dt[, id := .GRP, by = c("nid")]
  return(sex_dt)
}

# RUN SEX SPLIT -----------------------------------------------------------
run_sex_split <- function(dem_sex_dt, # has had group review sex split applied
                          data, # raw bundle version
                          out_dir, 
                          sex_model_name, 
                          offset,
                          drop_zeros,
                          fix_ones,
                          plot) {
  ## Find sex matches
  dem_sex_matches <- find_sex_match(data,
                                    offset = offset, 
                                    fix_ones = fix_ones,
                                    drop_zeros = drop_zeros,
                                    quiet = F)
  message(nrow(dem_sex_matches), " sex matches found")
  
  # Generate both sex mean from m_mean and f_mean
  dem_sex_matches[, cases_Both := cases_Male + cases_Female]
  dem_sex_matches[, sample_size_Both := sample_size_Male + sample_size_Female]
  dem_sex_matches[, mean_Both := cases_Both/sample_size_Both]
  # Use Wilson Score formula for proportion standard error
  z <- 1.96
  dem_sex_matches[, standard_error_Both := sqrt(mean_Both * (1-mean_Both) / sample_size_Both )]
  dem_sex_matches_original <- copy(dem_sex_matches)

  ## Pull out both-sex data and sex-specific data. both_sex is used for predicting and sex_specific will be the data we use.
  both_sex <- dem_sex_dt[sex=="Both"]
  both_sex[, original_zero_mean := ifelse(mean == 0, 1, 0)]
  both_sex[, original_one_mean := ifelse(mean == 1, 1, 0)]
  ## Temporarily offset mean so that the delta transformation works
  both_sex <- rm_zeros(both_sex,
                       offset = T,
                       drop_zeros = F,
                       fix_ones = T)
  ## Adjust to fit MR-BRT requirements
  both_sex[, age_mid := (age_start + age_end)/2]
  both_sex[, year_mid := round((year_start + year_end)/2)]
  both_sex[, id_var := nid]
  both_sex[, data_id := 1:nrow(both_sex)]
  ## Take mean, SE into logit space
  both_sex[, c("logit_mean", "logit_se") := data.table(delta_transform(mean = mean, sd = standard_error, transformation = "linear_to_logit"))]
  sex_specific <- dem_sex_dt[sex!="Both"]
  sex_specific$crosswalk_parent_seq <- sex_specific$seq
  n <- names(sex_specific)
    
  sexes <- c("Male", "Female")
  # Run sex splitting using each individual sex as the reference definition
  # Then adjust both_sex to male to make the male dataframe and female to make the female
  
  ## Generate covariate list
  covariates <- if (sex_remove_x_intercept) {
    c(lapply(sex_covs, CovModel))
  } else {
    # c(lapply(sex_covs, CovModel), CovModel("intercept"))
    list(CovModel(cov_name = "intercept"))
  }
  
  # initialize needed lists 
  formatted_data <- list()
  sex_results <- list()
  adjusted_data <- list()
  
  for (sex in sexes){
    
    dem_sex_matches <- copy(dem_sex_matches_original)
    setnames(dem_sex_matches,
             c("mean_Both", paste0("mean_", sex), "standard_error_Both", paste0("standard_error_", sex)),
             c("mean_alt", "mean_ref", "alt_se", "ref_se"))
    
    ## Logit transform the matched data points.
    ## Also takes logit difference 
    sex_matches <- logit_transform_mrbrt(dem_sex_matches)
    (message("Logit differences calculated"))
    
    ## Make an ID variable (in this case - nid only)
    sex_matches[, id_var := .GRP, by=.(nid)] # This really assumes we're all using "id_var" as the id_vars (SEB)
    ## Give names to Female/Male comparison
    sex_matches[, dorm_ref := sex]
    sex_matches[, dorm_alt := "Both"]
    ## Adjust the data to fit MR-BRT requirements
    sex_matches[, age_mid := (age_start + age_end)/2]
    sex_matches[, year_mid := round((year_start + year_end)/2)]
    
    ## The CW functions can only access global variables so I'm assigning these to be global variables
    sex_matches <<- sex_matches
    sex_split_response <<- sex_split_response
    sex_split_data_se <<- sex_split_data_se
    id_vars <<- id_vars
    
    ## Format data for meta-regression
    formatted_data[[sex]] <- CWData(
      df = sex_matches,
      obs = sex_split_response,
      obs_se = sex_split_data_se,
      alt_dorms = "dorm_alt",
      ref_dorms = "dorm_ref",
      study_id = id_vars
    )
    
    ## Launch MR-BRT model
    sex_results[[sex]] <- CWModel(
      cwdata = formatted_data[[sex]],
      obs_type = sex_mrbrt_response,
      cov_models = covariates,
      gold_dorm = sex, 
    )
    
    ## Pull out covariates to return later
    sex_covariates <- sex_results[[sex]]$fixed_vars
    
    # ## Assigning global variables
    sex_results <<- sex_results
    both_sex <<- both_sex
    id_vars <<- id_vars
    
    ## Predict onto original data (the both-sex data points)
    sex_preds <- adjust_orig_vals(fit_object = sex_results[[sex]],
                                  df = both_sex,
                                  orig_dorms = "sex",
                                  orig_vals_mean = "mean",
                                  orig_vals_se = "standard_error",
                                  # study_id = id_vars, 
                                  data_id = "data_id")
    sex_preds <- as.data.table(sex_preds)
    
    ## To return later
    ratio_mean <- sex_preds$pred_diff_mean
    ratio_se <- sqrt(sex_preds$pred_diff_sd^2 + as.vector(sex_results$gamma))
    
    ## Merge adjustments with unadjusted data
    ## Create ID variables for matching
    adjusted_data[[sex]] <- merge(both_sex, sex_preds, by="data_id", allow.cartesian = T)
    setnames(adjusted_data[[sex]], c("ref_vals_mean", "ref_vals_sd"), c("mean_adj", "se_adjusted"))
    
    # calculate the actual adjustment in mean: it is not necessarily equal to pred_diff_mean if you are using random effects in prediction
    ## include beta and gamma uncertainty in ratio uncertainty
    adjusted_data[[sex]][, c("mean_logit_adjusted", "se_logit_adjusted") := data.table(delta_transform(mean = mean_adj, sd = se_adjusted, transformation = "linear_to_logit"))]
    adjusted_data[[sex]][, logit_diff_mean := logit_mean - mean_logit_adjusted]

    ## However, if mean is 0, we need to adjust the uncertainty in normal space, not log-space
    # that is what we did above!
    adjusted_data[[sex]][original_zero_mean==1, mean_adj := 0]
    adjusted_data[[sex]][original_one_mean==1, mean_adj := 1]
    
    sex_name <- sex
    # set effective sample size to half of the original sample size 
    adjusted_data[[sex]][, `:=` (mean = mean_adj, standard_error = se_adjusted, crosswalk_parent_seq = seq, seq = NA,
                          effective_sample_size = sample_size/2, cases = "", sample_size = "", sex = sex_name,
                          note_modeler = paste0(note_modeler, " | sex split with logit difference btwn Both and ", sex_name, ":", round(pred_diff_mean, 2), " (",
                                                round(pred_diff_sd), ")"))]
    
    ## Save crossswalk info as an RDS
    sex_model <- save_crosswalk_RDS(sex_results, out_dir, paste0(sex_model_name, "_", sex))
    
    ## Save a pickle
    py_save_object(object = sex_results, filename = paste0(out_dir, sex_model_name, "_", sex, ".pkl"), pickle = "dill")
  }
  
  sex_split_data <- rbindlist(adjusted_data)
  sex_split_data_plot <- merge(adjusted_data[["Male"]], adjusted_data[["Female"]], by="data_id", allow.cartesian = T)
  sex_split_data_plot <- merge(sex_split_data_plot, both_sex, by="data_id", allow.cartesian = T)

  names_keep <- names(sex_specific)
  sex_split_data <- sex_split_data[, (names_keep), with = F]
  
  if (plot){
    setnames(sex_split_data_plot, c("mean.x", "mean.y", "mean"), c("m_mean", "f_mean", "mean"))
    graph_sex_split(sex_split_data_plot, out_dir, sex_model_name)
  }
  
  data <- rbind(sex_split_data, sex_specific, fill = T)
  
  ## Save an excel file of all sex split data
  file_out_path <- paste0(out_dir, sex_model_name, "all_logit_sex_split_data.xlsx")
  write.xlsx(data, file_out_path, sheetName = "extraction")
  
  if (save_sex_split_xwalk == T){
    file_out_path <- paste0(out_dir, sex_model_name, "all_logit_sex_split_data_formatted_for_xwalk.xlsx")
    data_grp_rev <- data[group_review == 1 | is.na(group_review)]
    data_grp_rev <- find_nondismod_locs(data_grp_rev)
    write.xlsx(data_grp_rev, file_out_path, sheetName = "extraction")
    desc <- paste("logit sex split data", name_short)
    result <- save_crosswalk_version(bundle_version_id = bv_id, 
                                     data_filepath = file_out_path,
                                     description = desc
                                      )
    
    if (result$request_status == "Successful") {
      df_tmp <- data.table(bundle_id = bundle,
                           bundle_version_id = bv_id,
                           crosswalk_version = result$crosswalk_version_id, 
                           parent_crosswalk_version = NA,
                           is_bulk_outlier = 0,
                           date = date,
                           description = desc,
                           filepath = file_out_path,
                           current_best = 1)
      
      cv_tracker <- read.xlsx(paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx'))
      cv_tracker <- data.table(cv_tracker)
      cv_tracker[bundle_id == bundle,`:=` (current_best = 0)]
      cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
      write.xlsx(cv_tracker, paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx'))
    }
  }
  
  return(list(data = data, 
              ratio_mean=ratio_mean,
              ratio_se=ratio_se, 
              model = sex_model,
              bv_id = bv_id, 
              offset = offset,
              sex_covariates = sex_covariates))
}

