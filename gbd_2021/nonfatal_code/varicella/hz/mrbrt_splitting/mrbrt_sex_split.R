#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:       USERNAME
# Date:         June 2020
# Purpose:      MR-BRT sex splitting functions for generalized sex splitting
# Location:     FILEPATH
# Notes:
#---------------------------------------------------------------------------------------------


# SOURCE
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_age_metadata.R")
source("/FILEPATH/get_population.R")
source('/FILEPATH/save_bundle_version.R')
source('/FILEPATH/get_bundle_version.R')
source('/FILEPATH/save_crosswalk_version.R')
source('/FILEPATH/get_crosswalk_version.R')
source("/FILEPATH/get_bundle_data.R")

# LIBRARIES
library(crosswalk, lib.loc = "/FILEPATH/")
library(dplyr)
library(data.table)
library(openxlsx)
library(stringr)
library(tidyr)
library(boot)
library(ggplot2)
setDTthreads(1)

# DEFINE OBJECTS
## Sex splitting only done in log space
transformation <- "log"
sex_split_response <- "log_diff" # user-defined
sex_split_data_se <- "log_diff_se"
sex_mrbrt_response <- "diff_log" #mr-brt defined - expects values formatted in log space
sex_covs <- NULL
id_vars <- "id_var"
sex_remove_x_intercept <- FALSE

# HELPER FUNCTIONS ---------------------------------------
source("/FILEPATH/within_study_sex_splitting.R")
source("/FILEPATH/apply_age_crosswalk_copy.R")
source("/FILEPATH/add_population_cols_fxn.R")

'%!in%' <- function(x,y)!('%in%'(x,y))

# MAKE A GRAPH
graph_predictions <- function(dt) {
  graph_dt <- copy(dt[, .(age_start, age_end, mean, m_mean, m_standard_error, f_mean, f_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("m_mean", "f_mean"))
  graph_dt_means[variable == "f_mean", variable := "Female"][variable == "m_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("m_standard_error", "f_standard_error"))
  graph_dt_error[variable == "f_standard_error", variable := "Female"][variable == "m_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"))
  graph_dt[, N := (mean*(1-mean)/error^2)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}

save_crosswalk_RDS <- function(results, path){
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

  saveRDS(model, paste0(path, "mrbrt_fit.RDS"))
  message("RDS object saved to ", paste0(path,"mrbrt_fit.RDS"))

  return(model)
}

## save betas and pkl
save_crosswalk_pkl <- function(fit, dir) {
  df_result <- fit$create_result_df()
  write.csv(df_result, paste0(dir, "/betas.csv"))

  py_save_object(object = fit, filename = paste0(dir, "/fit1.pkl"), pickle = "dill")

}

## Offsetting instances where mean = 0 or mean = 1 by 1% of median of non-zero values (like Dismod).
rm_zeros <- function(dt,
                     offset = T,
                     fix_ones = T,
                     drop_zeros = F,
                     quiet = F){
  if (offset) {
    off <- .01*median(dt[mean != 0, mean])
    if (nrow(dt[mean==0])>0) {
      if (!quiet) message("You have ", nrow(dt[mean==0])," zeros in your dataset that are being offset by 1% of the median of all non-zero values. To avoid this and instead drop all rows with mean 0, set offset=F and drop_zeros=T.")
      dt[mean==0, `:=`(mean = off)]
    }
    if (fix_ones){
      if (nrow(dt[mean==1])>0){
        if (!quiet) message("You have ", nrow(dt[mean==1])," ones in your dataset that are being offset by 1% of the median of all non-zero values. To avoid this and instead drop the rows with mean 1, set offset=F and drop_zeros=T.")
        dt[mean==1, `:=` (mean = 1-off)]
      }
    }
  }

  if (drop_zeros) {

    if (fix_ones){
      if (!quiet) message("Dropping ", nrow(dt[mean==1])," ones in your dataset.")
      dt <- dt[!mean == 1]
    }
      if (!quiet) message("Dropping ", nrow(dt[mean==0])," zeros in your dataset.")
      dt <- dt[!mean == 0]
  }
  return(dt)
}

## DATA PREP-FINDING SEX MATCHES ON NID, LOCATION, MEASURE, YEAR, AND EXACT MATCH OF AGE_START/AGE_END
find_sex_match <- function(dt,
                           measure,
                           offset,
                           fix_ones = T,
                           drop_zeros = F,
                           quiet = F) {
  sex_dt <- copy(dt)
  # Don't use data that was outliered to match and generate sex wts
  sex_dt <- sex_dt[is_outlier != 1]
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% xw_measure]
  # Don't use data that is flagged as group review
  sex_dt <- sex_dt[is.na(group_review)|!(group_review==0),]
  ## Offsetting (or dropping) instances where mean = 0 or mean = 1 by 1% of median of non-zero values (like Dismod). Needs to happen before making matches so don't get NAs
  sex_dt <- rm_zeros(sex_dt,
                     offset=offset,
                     fix_ones=fix_ones,
                     drop_zeros = drop_zeros)
  #Find nids that have corresponding male and female rows
  match_vars <- c("nid", "location_id", "location_name", "measure", "year_start", "year_end", "age_start", "age_end")
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]

  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = "nid"]
  sex_dt <- dplyr::select(sex_dt, all_of(keep_vars))


  sex_dt <- dcast(sex_dt, ...~ sex, value.var = c("mean", "standard_error"))
  sex_dt[, id := .GRP, by = c("nid")]
  return(sex_dt)
}

## LOG TRANSFORM THE MALE/FEMALE CFRs and SEs
log_transform_mrbrt <- function(df) {

  df[, c("log_mean_alt", "se_log_mean_alt") := data.table(crosswalk::delta_transform(mean = mean_alt, sd = alt_se, transformation = "linear_to_log"))]
  df[, c("log_mean_ref", "se_log_mean_ref") := data.table(crosswalk::delta_transform(mean = mean_ref, sd = ref_se, transformation = "linear_to_log"))]

  df[, c("log_diff", "log_diff_se")] <- calculate_diff(
    df = df,
    alt_mean = "log_mean_alt", alt_sd = "se_log_mean_alt",
    ref_mean = "log_mean_ref", ref_sd = "se_log_mean_ref" )
  return(df)
}

## LOGIT TRANSFORMATION-- IF MODELING MRBRT IN LOGIT SPACE
logit_transform_mrbrt <- function(df) {

  df[, c("temp_logit_alt", "se_logit_mean_alt") := data.table(crosswalk::delta_transform(mean = mean_alt, sd = alt_se, transformation = "linear_to_logit"))]
  df[, c("temp_logit_ref", "se_logit_mean_ref") := data.table(crosswalk::delta_transform(mean = mean_ref, sd = ref_se, transformation = "linear_to_logit"))]

  df[, logit_diff := temp_logit_alt - temp_logit_ref]
  df[, logit_diff_se := sqrt(se_logit_mean_alt^2 + se_logit_mean_ref^2)] # se for logit difference

  return(df)

}
## RUN THE MRBRT MODELING AND PREDICTION
run_sex_split <- function(dt,
                          out_dir,
                          offset = F,
                          fix_ones = F,
                          drop_zeros = F,
                          plot = F,
                          inlier_pct=1,
                          use_prev_mrbrt,
                          path_to_prev_mrbrt = NULL) {
  dem_sex_dt <- data.table::copy(dt)
  ## Remove any group review==0 data
  dem_sex_dt <- dem_sex_dt[(is.na(group_review) | !(group_review==0)),]
  ## Find sex matches
  dem_sex_matches <- find_sex_match(dem_sex_dt,
                                    measure = xw_measure,
                                    offset = offset,
                                    fix_ones = fix_ones,
                                    drop_zeros = drop_zeros,
                                    quiet = F)
  message(nrow(dem_sex_matches), " sex matches found")
  # Adjust formatting to be compatible with functions
  # Use male as reference
  setnames(dem_sex_matches,
           c("mean_Male", "mean_Female", "standard_error_Male", "standard_error_Female"),
           c("mean_ref", "mean_alt", "ref_se", "alt_se"))

  ## Log transform the matched data points.
  ## Also takes log difference
  if(transformation=="log"){
    sex_matches <- log_transform_mrbrt(dem_sex_matches)
    message("Log differences calculated")
  } else if (transformation=="logit"){
    sex_matches <- logit_transform_mrbrt(dem_sex_matches)
    message("Logit differences calculated")
  }

  ## Make an ID variable (in this case - nid only)
  sex_matches[, id_var := .GRP, by=.(nid)] # This assumes we're using "nid" as the id_vars -- good for sex splitting
  ## Give names to Female/Male comparison
  sex_matches[, dorm_alt := "Female"]
  sex_matches[, dorm_ref := "Male"]
  ## Adjust the data to fit MR-BRT requirements
  sex_matches[, age_mid := (age_start + age_end)/2]
  sex_matches[, year_id := round((year_start + year_end)/2)]
  sex_matches[, age_scaled := (age_mid - mean(age_mid))/sd(age_mid)]
  sex_matches[, year_scaled := (year_id - mean(year_id))/sd(year_id)]

  ## Save mrbrt input data
  if(use_prev_mrbrt == F) fwrite(sex_matches, file=paste0(out_dir, date, "_", "mrbrt_input_matched.csv"))

  ## Pull out both-sex data and sex-specific data. both_sex is used for predicting and sex_specific will be the data we use.
  both_sex <- dem_sex_dt[sex=="Both"]
  sex_specific <- dem_sex_dt[sex!="Both"]
  n <- names(sex_specific)

  ## Adjust both sex to fit MR-BRT requirements (adjust_orig_vals)
  both_sex[, age_mid := (age_start + age_end)/2]
  both_sex[, year_id := round((year_start + year_end)/2)]
  both_sex[, age_scaled := (age_mid - mean(age_mid))/sd(age_mid)]
  both_sex[, year_scaled := (year_id - mean(year_id))/sd(year_id)]
  both_sex[, id_var := nid]
  both_sex[, sex_dummy := "Female"]

  both_sex_0 <- both_sex[mean == 0 & sex=="Both"]
  both_sex_1 <- both_sex[mean ==1 & sex=="Both"]
  both_sex <- both_sex[!(mean == 0 | mean==1),]

  ## The CW functions can only access global variables
  sex_matches <<- sex_matches
  sex_split_response <<- sex_split_response
  sex_split_data_se <<- sex_split_data_se
  sex_covs <<- sex_covs
  id_vars <<- id_vars

  ## Format data for meta-regression, no covariates
  formatted_data <- CWData(
    df = sex_matches,
    obs = sex_split_response,
    obs_se = sex_split_data_se,
    alt_dorms = "dorm_alt",
    ref_dorms = "dorm_ref",
    study_id = id_vars
  )

  ## Generate covariate list - only relevant if have covariates
  covariates <- if (sex_remove_x_intercept) {
    c(lapply(sex_covs, CovModel))
  } else {
    # c(lapply(sex_covs, CovModel), CovModel("intercept"))
    list(CovModel(cov_name = "intercept"))
  }

  ## Launch MR-BRT model
  sex_results <- CWModel(
    cwdata = formatted_data,
    obs_type = sex_mrbrt_response,
    cov_models = covariates,
    gold_dorm = "Male",
    inlier_pct = inlier_pct
  )



  py_save_object(object = sex_results, filename = paste0(out_dir, "fit1.pkl"), pickle = "dill")
  if(use_prev_mrbrt == T) {
    sex_results <- py_load_object(filename = path_to_prev_mrbrt, pickle = "dill")
  }

  ## Save objects pkl and betas (if using prev mrbrt, this will save the previous model fit in this run date's folder)
  df_result <- sex_results$create_result_df()
  write.csv(df_result, paste0(out_dir, "crosswalk_results.csv"))

  #move to global environment so can make funnel plot in main script
  sex_results <<- sex_results
  formatted_data <<- formatted_data


  ## Pull out covariates to return later
  sex_covariates <- sex_results$fixed_vars

  ## Assigning global variables
  sex_results <<- sex_results
  both_sex <<- both_sex
  id_vars <<- id_vars

  ## Predict onto original data (the both-sex data points)
  sex_preds <- adjust_orig_vals(fit_object = sex_results,
                                df = both_sex,
                                orig_dorms = "sex_dummy",
                                orig_vals_mean = "mean",
                                orig_vals_se = "standard_error",
                                study_id = id_vars)             #this predicts out on the b/w study heterogeneity in US (do this if you believe the between nid heterogeneity is capturing important difference, not noise)
                                                                # predicting out with the gamma is no different than not doing so for the ratio because only affects the UI
  sex_preds <- as.data.table(sex_preds)

  ## To return later
  log_ratio_mean <- sex_preds$pred_diff_mean

  # include beta and gamma uncertainty in ratio uncertainty
  log_ratio_se <- sqrt(sex_preds$pred_diff_sd^2 + as.vector(sex_results$gamma))


  ## Merge adjustments with unadjusted data
  ## Create ID variables for matching
  sex_preds[, id := 1:nrow(sex_preds)]
  both_sex[, id := 1:nrow(both_sex)]
  both_sex <- merge(both_sex, sex_preds, by="id", allow.cartesian = T)

  ## Take mean, SEs back into linear space so we can combine and make adjustments
  ## include beta and gamma uncertainty in ratio uncertainty
  both_sex[, se_val:= unique(sqrt(both_sex$pred_diff_sd^2 + as.vector(sex_results$gamma)))]
  if(transformation=="log"){
    both_sex[, c("linear_pred_mean", "linear_pred_se") := data.table(crosswalk::delta_transform(mean = pred_diff_mean, sd = se_val, transform = "log_to_linear"))]
  } else if (transformation=="logit"){
    both_sex[, c("linear_pred_mean", "linear_pred_se") := data.table(crosswalk::delta_transform(mean = pred_diff_mean, sd = se_val, transform = "logit_to_linear"))]
  }

  #save the ratio in the model description text file for easy access
  cat(paste0("Estimated female/male ratio: ", unique(both_sex$linear_pred_mean)),
      paste0("Estimated female/male ratio se: ", unique(both_sex$linear_pred_se)),
      sep="\n",
      file=paste0(out_dir, "model_details.txt"),
      append=TRUE)

  ## ALGEBRA FOR THE MEANS
  ## we will use population for the midpoint year of the both sex data point to calculate an estimate of number of cases
  ## use our modeled ratio and the sex specific pops for that location year to split the both sex values into a male and female value

  ## Add on the population using custom function
  both_sex <- add_population_cols(both_sex, gbd_round = gbd_round, decomp_step = decomp_step)

  ## calculate the means
  both_sex[, m_mean:= (mean*both_population)/(male_population + linear_pred_mean*female_population)]
  both_sex[, f_mean:= linear_pred_mean*m_mean]

  ## Get combined standard errors
  both_sex[, m_standard_error := sqrt((linear_pred_se^2 * standard_error^2) + (linear_pred_se^2 * mean^2) + (standard_error^2 * linear_pred_mean^2))]#sqrt( (linear_pred_se^2 * mean^2) + (standard_error^2 * linear_pred_mean^2))
  both_sex[, f_standard_error := sqrt((linear_pred_se^2 * standard_error^2) + (linear_pred_se^2 * mean^2) + (standard_error^2 * linear_pred_mean^2))]

  #add back in zeros and ones
  # adjust standard error of 0 and 1 data points to reflect the increased uncertainty that comes from the smaller sample size once split
  both_sex_0[, ":="(m_mean = mean,
                   f_mean = mean,
                   m_standard_error = sqrt((unique(both_sex$linear_pred_se)^2 * mean^2) + (standard_error^2 * unique(both_sex$linear_pred_mean)^2)),
                   f_standard_error = sqrt((unique(both_sex$linear_pred_se)^2 * mean^2) + (standard_error^2 * unique(both_sex$linear_pred_mean)^2)))]
  both_sex_1[, ":="(m_mean = mean,
                    f_mean = mean,
                    m_standard_error = sqrt((unique(both_sex$linear_pred_se)^2 * mean^2) + (standard_error^2 * unique(both_sex$linear_pred_mean)^2)),# alternate equation from math team, but either should work: sqrt(2)*unique(both_sex$linear_pred_se),
                    f_standard_error = sqrt((unique(both_sex$linear_pred_se)^2 * mean^2) + (standard_error^2 * unique(both_sex$linear_pred_mean)^2)))]
  both_sex <- rbind(both_sex, both_sex_0, fill=T)
  both_sex <- rbind(both_sex, both_sex_1, fill=T)

  # some split values over 1
  # in log space, these values are coming from values that = 1 (or close) from the start
  message("\nYour sex split data has ", nrow(both_sex[m_mean>1 | f_mean>1]), " rows with a sex split mean over 1.  \nThe max of these original means is ", max(both_sex[m_mean>1 | f_mean>1, mean]),
          ". The min of these rows original mean is ", min(both_sex[m_mean>1 | f_mean>1, mean]))
  message("These values are being coerced to their original mean for both sexes, since they likely come from proportions equal to or near equal to 1, assuming you are in log space.")
  both_sex[m_mean > 1 | f_mean > 1, c("m_mean", "f_mean") := mean]

  #assign parent and clear seq so that we don't have duplicate seq columns
  both_sex[, crosswalk_parent_seq:=seq]
  both_sex[, seq:=NA]
  ## Make male- and female-specific dts
  male_dt <- copy(both_sex)
  male_dt[, note_modeler := paste0(note_modeler, " | sex split with female/male ratio: ", linear_pred_mean, " (",
                                   linear_pred_se, ") from original mean: ", mean)]
  male_dt[, `:=` (mean = m_mean, standard_error = m_standard_error, upper = NA, lower = NA,
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male")]
  male_dt <- dplyr::select(male_dt, all_of(n))
  female_dt <- copy(both_sex)
  female_dt[, note_modeler := paste0(note_modeler, " | sex split with female/male ratio: ", linear_pred_mean, " (",
                                     linear_pred_se, ") from original mean: ", mean)]
  female_dt[, `:=` (mean = f_mean, standard_error = f_standard_error, upper = NA, lower = NA,
                    cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female")]
  female_dt <- dplyr::select(female_dt, all_of(n)) ## original names of data

  data <- rbindlist(list(sex_specific, female_dt, male_dt))

  sex_specific_data <- copy(data)
  message("Done with sex-splitting.")

  ## Save crossswalk info as an RDS, pkl and df of betas
  sex_model <- save_crosswalk_RDS(sex_results, out_dir)

  ## plot adjusted vs original data
  if(plot){
    pdf(paste0(out_dir, "sex_adj_onegraph.pdf"))
    sex_plot <- graph_predictions(both_sex)
    print(sex_plot)
    dev.off()
  }


  return(list(data = sex_specific_data, model = sex_model))
}

