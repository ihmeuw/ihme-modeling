
##
## Author: "USERNAME"
## Purpose: Master script that contains the 2019/2020 MR-BRT crosswalk process. 
## Documentation: "FILEPATH".docx
##


os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
} else {
  j<- "FILEPATH"
}

date<-gsub("-", "_", Sys.Date())

## USERNAME MR-BRT package/library
library(crosswalk, lib.loc = "FILEPATH")

pacman::p_load(ggplot2, data.table, gtools, boot)

# wrappers, cleaning functions
source("FILEPATH/clean_mr_brt.R")
source("FILEPATH/data_tests.R")
source("FILEPATH/model_helper_functions.R")
#source("FILEPATH/plot_functions.R")

# central computation functions - interact with database
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_location_metadata.R")
source('FILEPATH/endo_upload.R')
mad.stats <- function(x) {
  c(
    mad = mad(x),
    median = median(x)
  )
}
"%ni%" <- Negate("%in%")


master_mrbrt_crosswalk <- function(
  model_abbr,
  data,
  storage_folder,
  folder_label_comment="",
  reference_def=NULL,
  alternate_defs=NULL,
  xw_measure,
  decomp_step="iterative",
  gbd_round_id=7,
  logit_transform=F,
  upload_crosswalk_version=F,
  write=F,
  save_crosswalk_result_file=NULL,
  bundle_version_id=NULL,
  sex_split_only=F,
  age_overlap=5,
  year_overlap=5,
  age_range=10,
  year_range=10,
  subnat_to_nat=F,
  sex_age_overlap=0,
  sex_year_overlap=0,
  sex_age_range=0,
  sex_year_range=0,
  sex_fix_zeros=T,
  fix_zeros=T,
  addl_x_covs,
  dorm_separator = "AND",
  use_lasso=F,
  id_vars="id_var",
  spline_covs=NULL,
  degree=3,
  knots=c(-1, 0, 1),
  #knot_placement_procedure="frequency",
  i_knots=NULL,
  r_linear=F,
  l_linear=F,
  spline_monotonicity=NULL,
  spline_convexity=NULL,
  opt_method="trim_maxL",
  trim_pct=0.1,
  remove_x_intercept=T,
  sex_remove_x_intercept=F,
  sex_covs="age_scaled",
  ignore_gr=F
) {

  ## Documentation of arguments passed in 
  match.call.defaults <- function(...) {
    call <- evalq(match.call(expand.dots = FALSE), parent.frame(1))
    formals <- evalq(formals(), parent.frame(1))

    for(i in setdiff(names(formals), names(call))) call[i] <- list( formals[[i]] )

    match.call(sys.function(sys.parent()), call)
  }
  arguments <- match.call.defaults()

  ## Put documentation into data.table
  args_dt <- data.table()
  for (i in 2:length(names(arguments)[names(arguments) != ""])) args_dt[,paste0(names(arguments)[i]) := paste0(arguments[i])]
  arguments <- args_dt

  ## Turn vectors into CSVs
  def_csv <- paste0(alternate_defs, collapse = ",")
  addl_covs_csv <- paste0(addl_x_covs, collapse = ",")
  arguments[1, alternate_defs := def_csv]
  arguments[1, addl_x_covs := addl_covs_csv]
  strg_fldr <- paste0(storage_folder)
  arguments[1, storage_folder := strg_fldr]
  
  ########### Function checks ######################
  ##################################################

  if (write & !(is.null(save_crosswalk_result_file))) stop("Provide a file path to write the crosswalk verison, or toggle write=F.")
  if (is.null(addl_x_covs) & remove_x_intercept) stop("You have to include an intercept if you have no additional covariates")

  ########### Naming variables #####################
  ##################################################

  if (!ignore_gr) data <- data[is_outlier==0 & input_type!="group_review" & measure == xw_measure,]

  message(paste0("You ", ifelse(nrow(data[sex=="Both",])>0, "do", "don't"), " need to sex-split."))
  message(paste0("You ", ifelse(sex_split_only, "won't be", "will also be"), " crosswalking."))

  datetime <- format(Sys.time(), '%Y%m%d')

  ## We can either use logit transformation or log transformation - but are encouraged to use logit unless prevalence is less than 50%.
  ## This ensures that prevalence doesn't go over 1.
  if(logit_transform) {
    response <- "logit_diff"
    data_se <- "logit_diff_se"
    mrbrt_response <- "diff_logit"
  } else {
    response <- "ratio_log"
    data_se <- "ratio_se_log"
    mrbrt_response <- "diff_log"
  }

  keep_x_intercept <- !remove_x_intercept

  ## Sex splitting only done in log space
  sex_split_response <- "ratio_log"
  sex_split_data_se <- "ratio_se_log"
  sex_mrbrt_response <- "diff_log"

  ## If we don't have a "reference" column, making one so we can figure out what we need to XW
  if (!sex_split_only) {
    if ((is.null(reference_def) | (missing(reference_def)))) {
      if (length(alternate_defs) > 1) data[rowSums(data[, ..alternate_defs])==0, reference := 1]
      if (length(alternate_defs) == 1) data[get(alternate_defs) == 0, reference := 1]
      data[is.na(reference), reference := 0]
      reference_def <- "reference"
    }
  }

  ## Creating the folders so we can write into them
  #if (!dir.exists(folder_root)) dir.create(folder_root)
  #logs_root <- paste0(folder_root, model_abbr, "/")
  #if (!dir.exists(logs_root)) dir.create(logs_root)

  ## Naming the log files. These should take the format: /folder/root/.../model_abbr/measure_comment/etc.
  log_name <- ifelse(folder_label_comment=="", paste0(xw_measure, "_", datetime), paste0(xw_measure, "_", datetime, "_", folder_label_comment))

  ## NRVDs don't have "note_modeler" as a column, which we need later.
  if (!("note_modeler" %in% names(data))) data[, note_modeler := ""]


  ################################################################
  ############## Working on sex-splitting: #######################
  ################################################################
  if(nrow(data[sex=="Both",]) == 0) {
    num_both_sex = 0
  }

  if (nrow(data[sex=="Both",]) > 0) {

    ## Give a heads up about how many sex-specific data points you have to XW.
    num_both_sex <- nrow(data[sex=="Both",])
    message(paste0("Starting to sex-split. You have ", num_both_sex, " both-sex data points to be split."))

    ## Finding matches:
    sex_matches <- sex_match(data, reference_def = reference_def, alternate_def = alternate_defs,
                             age_overlap = sex_age_overlap,
                             year_overlap=sex_year_overlap,
                             fix_zeros = sex_fix_zeros, exact_match = T, quiet=T)

    ## Pull out this object to return later.
    matches_result <- sex_matches$matches

    ## Log transform the matched data points.
    sex_matches <- log_transform_mrbrt(sex_matches$data)

    ## Adjust the age, sex, etc., make an ID variable
    sex_matches <- adj_mr_brt(sex_matches, sex = F)
    sex_matches[, id_var := 1:nrow(sex_matches)] # This really assumes we're all using "id_var" as the id_vars (USERNAME)

    ## Give names to Female/Male comparison
    sex_matches[, dorm_alt := "Female"]
    sex_matches[, dorm_ref := "Male"]

    ## Pull out both-sex data and sex-specific data. both_sex is used for predicting and sex_specific will be the data we use.
    both_sex <- adj_mr_brt(data[sex=="Both",], original_data = T, year = F, sex=F, fix_zeros = F,
                           age_mean = mean(sex_matches$age_mid_ref), age_sd = sd(sex_matches$age_mid_ref))
    both_sex[, id := 1:nrow(both_sex)]
    both_sex[, sex_dummy := "Female"]
    sex_specific <- data[sex!="Both",]
    n <- names(sex_specific)

    ## Take mean, SE into log-space
    both_sex[, c("mean_log", "log_se") := data.table(delta_transform(mean = mean, sd = standard_error, transformation = "linear_to_log"))]

    # The CW functions can only access global variables so I'm assigning these to be global variables
    sex_matches <<- sex_matches
    sex_split_response <<- sex_split_response
    sex_split_data_se <<- sex_split_data_se
    sex_covs <<- sex_covs
    id_vars <<- id_vars

    ## Format data for meta-regression
    formatted_data <- CWData(
      df = sex_matches,
      obs = sex_split_response,
      obs_se = sex_split_data_se,
      alt_dorms = "dorm_alt",
      ref_dorms = "dorm_ref",
      covs = list(sex_covs),
      study_id = id_vars
    )

    # pull out formatted data to return later
    formatted_data_sex = formatted_data

    ## Generate covariate list
    if (sex_remove_x_intercept) {
      covariates <- c(lapply(sex_covs, CovModel))
      all_covs <- sex_covs
    } else {
      covariates <- c(lapply(sex_covs, CovModel), CovModel("intercept"))
      all_covs <- c(sex_covs, "intercept")
    }

    ## Assigning to global variables
    formatted_data <<- formatted_data
    sex_mrbrt_response <<- sex_mrbrt_response
    covariates <<- covariates

    ## Launch MR-BRT model
    sex_results <- CWModel(
      cwdata = formatted_data,
      obs_type = sex_mrbrt_response,
      cov_models = covariates,
      gold_dorm = "Male",
      inlier_pct = 1-trim_pct
    )

    ## Pull out covariates to return later
    sex_covariates <- sex_results$fixed_vars
    sex_cov_dt <- data.table(covariate = "Female")
    for (cov in 1:length(all_covs)) {
      val <- sex_covariates["Female"][[1]][cov]
      cov_name <- all_covs[cov]
      sex_cov_dt[covariate == "Female", paste0(cov_name) := val]
    }
    sex_covariates <- sex_cov_dt

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
                                  study_id = id_vars)
    sex_preds <- as.data.table(sex_preds)

    ## To return later
    ratio_mean <- sex_preds$pred_diff_mean
    ratio_se <- sex_preds$pred_diff_sd


    ## We need the population for each data point. Instead of pooling the population (for ex if you were age 3-10) for all years, we just take the midpoint (3-10 would become 6)
    both_sex[, year_mid := (year_start + year_end)/2]
    both_sex[, year_floor := floor(year_mid)]
    both_sex[, age_floor:= floor(age_mid)]

    ## Pull out population data. We need age- sex- location-specific population.
    pop <- get_population(age_group_id = "all", sex_id = "all", decomp_step = decomp_step, year_id = unique(floor(both_sex$year_mid)),
                          location_id=unique(both_sex$location_id), single_year_age = T, gbd_round_id = gbd_round_id)
    ids <- get_ids("age_group") ## age group IDs
    pop <- merge(pop, ids, by="age_group_id", all.x=T, all.y=F)
    pop$age_group_name <- as.numeric(pop$age_group_name)
    pop <- pop[!(is.na(age_group_name))]
    pop$age_group_id <- NULL

    ## Merge in populations for both-sex and each sex. Turning age bins into the midpoint - because the population ratios, not population itself, is what's important.
    both_sex <- merge(both_sex, pop[sex_id==3,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
    setnames(both_sex, "population", "population_both")
    both_sex <- merge(both_sex, pop[sex_id==1,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
    setnames(both_sex, "population", "population_male")
    both_sex <- merge(both_sex, pop[sex_id==2,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
    setnames(both_sex, "population", "population_female")

    ## There are some rows where popuation is NA - get rid of them
    sex_preds[, id := 1:nrow(sex_preds)]
    both_sex[, id := 1:nrow(both_sex)]
    if (nrow(both_sex[is.na(population_both)]) > 0) dropped_nids <- unique(both_sex[is.na(population_both), nid])
    both_sex <- both_sex[!is.na(population_both)]
    ids <- unique(both_sex$id)
    num <- nrow(data[sex=="Both",]) - length(ids)
    sex_preds <- sex_preds[id %in% ids,]
    if (num > 0) message(paste0("WARNING: You lost ", num, " both-sex data point(s) because we couldn't find a population for that point. NIDs are: ", dropped_nids))

    ## Merge adjustments with unadjusted data
    both_sex <- merge(both_sex, sex_preds, by="id", allow.cartesian = T)

    ## Take mean, SEs into real-space so we can combine and make adjustments
    both_sex[, c("real_pred_mean", "real_pred_se") := data.table(delta_transform(mean = pred_diff_mean, sd = pred_diff_sd, transform = "log_to_linear"))]

    ## Make adjustments. See documentation for rationale behind algebra.
    both_sex[, m_mean := mean * (population_both/(population_male + real_pred_mean * population_female))]
    both_sex[, f_mean := real_pred_mean * m_mean]

    ## Get combined standard errors
    both_sex[, m_standard_error := sqrt(real_pred_se^2 + standard_error^2)]
    both_sex[, f_standard_error := sqrt(real_pred_se^2 + standard_error^2)]

    ## Make male- and female-specific dts
    male_dt <- copy(both_sex)
    male_dt[, `:=` (mean = m_mean, standard_error = m_standard_error, upper = NA, lower = NA,
                    cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", real_pred_mean, " (",
                                          real_pred_se, ")"))]
    male_dt <- dplyr::select(male_dt, n)
    female_dt <- copy(both_sex)
    female_dt[, `:=` (mean = f_mean, standard_error = f_standard_error, upper = NA, lower = NA,
                      cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                      note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", real_pred_mean, " (",
                                            real_pred_se, ")"))]
    female_dt <- dplyr::select(female_dt, n) ## original names of data

    data <- rbindlist(list(sex_specific, female_dt, male_dt))

    sex_specific_data <- copy(data)

    message("Done with sex-splitting.")

  }

  ##############################################################################
  #### Running MR-BRT model on sex-specific data ###############################
  ##############################################################################

  if (!sex_split_only) {

    ## Pull out original data. Later we will apply the XW to this dataset.
    data[, age_mid := (age_start + age_end)/2]
    data[, year_mid := (year_start + year_end)/2]
    original_data_to_xw <- copy(data[measure==xw_measure & get(reference_def) == 0,])
    original_data_no_xw <- copy(data[measure==xw_measure & get(reference_def) == 1,])

    ## Find matches/create ratios. Function in "clean_mr_brt.R".
    message("Beginning crosswalks between alternate definition(s).")
    data <- match_mr_brt(
      data=data,
      reference_def=reference_def,
      alternate_defs=alternate_defs,
      age_overlap=age_overlap,
      year_overlap=year_overlap,
      age_range=age_range,
      year_range=year_range,
      subnat_to_nat = subnat_to_nat,
      fix_zeros=fix_zeros,
      quiet=T
    )

    ## Pull out these objects to return later
    matches_count <- data$counts
    match_data_copy <- data

    ## Pull out prevalence matches, log-transform / logit-transform. Prep the matches for MR-BRT.
    if(logit_transform) {
      matches <- logit_transform_mrbrt(data$data)
    } else {
      matches <- log_transform_mrbrt(data$data)
    }

    lapply(names(matches),check_missing, df=matches, warn=T)
    matches <- adj_mr_brt(matches) ## Scaling age and year variables. Making sex into a indicator variable. Function in 'clean_mr_brt.R'
    original_data_to_xw[, year_mid := (year_start + year_end)/2]

    ## Prep the original data for predicting. We need to scale the covariates, make SE, log-transform / logit-transform the SE.
    to_adjust <- adj_mr_brt(original_data_to_xw, nid=F, original_data = T, fix_zeros = F,
                            age_mean = mean(matches$age_mid_ref), age_sd = sd(matches$age_mid_ref)) ## Same scaling function. Not creating an ID variable as you would for prepping MR-BRT.
    to_adjust[, id := 1:nrow(to_adjust)]
    to_adjust[standard_error == 1, standard_error := 0.99999]

    ## Make a covariate for "orig_dorms" column
    defs <- c(alternate_defs, reference_def)
    to_adjust$num_defs <- rowSums(to_adjust[, ..defs])
    multiple_defs <- ifelse(max(to_adjust$num_defs,na.rm=T) > 1, T, F)
    to_adjust[, (alternate_defs) := lapply(.SD,as.logical), .SDcols = alternate_defs]
    to_adjust[, newCol := toString(alternate_defs[unlist(.SD)]), by = 1:nrow(to_adjust), .SDcols = alternate_defs]

    if(multiple_defs){
      for (def in defs) to_adjust[get(def) == 1 & num_defs == 1, definition := paste0(def)]
      to_adjust[, (alternate_defs) := lapply(.SD,as.logical), .SDcols = alternate_defs]
      to_adjust[, definition := toString(alternate_defs[unlist(.SD)]), by = 1:nrow(to_adjust), .SDcols = alternate_defs]
      to_adjust[, definition := gsub(" ","",definition)]
      to_adjust[, definition := gsub(",",dorm_separator,definition)]
      to_adjust[, definition := str_replace(definition,", ",dorm_separator)]
      to_adjust[, (alternate_defs) := lapply(.SD,as.integer), .SDcols = alternate_defs] #convert back to numeric. 
      if (nrow(to_adjust[is.na(definition)]) > 0) stop("Definitions haven't been labeled appropriately")
    } else {
      dorm_separator <- NULL
      for (def in defs) to_adjust[get(def) == 1, definition := paste0(def)]
      if (nrow(to_adjust[is.na(definition)]) > 0) stop("Definitions haven't been labeled appropriately")
    }

    ## Take the original standard error into log- or logit-space using the delta method
    if(logit_transform) {

      ## Temporarily make mean=0 mean=0.00000001 so that the delta transformation works
      to_adjust[mean == 0, mean := 0.0000001]
      to_adjust[, c("logit_mean", "logit_se") := data.table(delta_transform(mean = mean, sd = standard_error, transformation = "linear_to_logit"))]
      to_adjust[mean == 0.0000001, mean := 0]

      matches[logit_diff_se == 0, logit_diff_se := .001] # check dataset

    } else {

      ## Temporarily make mean=0 mean=0.00000001 so that the delta transformation works
      to_adjust[mean == 0, mean := 0.0000001]
      to_adjust[, c("log_mean", "log_se") := data.table(delta_transform(mean = mean, sd = standard_error, transformation = "linear_to_log"))]
      to_adjust[mean == 0.0000001, mean := 0]

      matches[ratio_se_log == 0, ratio_se_log := .001]

    }

    matches[, age_scaled := (age_mid_ref - mean(age_mid_ref))/sd(age_mid_ref)]

    #######################################
    ##### Launch the MR-BRT model #########
    #######################################

    message("Launching MR-BRT...")

    ## If there are no matches, remove from list of alternate definitions and get rid of them
    for (cov in alternate_defs) {
      if (!(cov %in% names(matches))) {
        message(paste(cov, " has no matches. We cannot estimate a beta for it. It will be dropped from the list of x-covariates."))

        to_adjust <- to_adjust[get(cov) == 0,]

        alternate_defs <- setdiff(alternate_defs, paste0(cov))
      }
    }

    format_covs <- addl_x_covs
    if (!is.null(spline_covs) && !(spline_covs %in% addl_x_covs)) {
      format_covs <- c(spline_covs, addl_x_covs)
    }

    ## Assigning global variables
    assigned_matches <<- matches
    response <<- response
    data_se <<- data_se
    addl_x_covs <<- addl_x_covs
    id_vars <<- id_vars
    keep_x_intercept <<- keep_x_intercept
    format_covs <<- format_covs
    dorm_separator <<- dorm_separator


    ## Format data for MR-BRT
    formatted_data <- CWData(
      df = assigned_matches,
      obs = response,
      obs_se = data_se,
      alt_dorms = "comp_1",
      ref_dorms = "comp_2",
      covs = format_covs,
      study_id = id_vars,
      add_intercept = keep_x_intercept,
      dorm_separator = dorm_separator
    )

    # pull out this object to return later
    formatted_data_xw = formatted_data

    ## Generate covariate list and initalize

    if (!is.null(spline_covs)) {

      if (length(spline_covs) > 1) stop("Can you have >1 spline covariate? We should discuss.")

      ## Pull out spline and normal covs to initialize separately
      if (spline_covs %in% addl_x_covs) {
        non_spline_covs <- setdiff(addl_x_covs, spline_covs)
      } else {
        non_spline_covs <- addl_x_covs
      }

      ## Global variables
      knot_locations <<- knots
      degree <<- degree
      l_linear <<- l_linear
      r_linear <<- r_linear
      spline_monotonicity <<- spline_monotonicity
      spline_convexity <<- spline_convexity

      ## Initalize covariates
      non_spline_covs_initialized <- c(lapply(non_spline_covs, CovModel))
      spline_covs_initialized <- CovModel(cov_name = spline_covs, spline = XSpline(knots = knot_locations, degree = as.integer(degree), l_linear = l_linear, r_linear = r_linear),
               spline_monotonicity = spline_monotonicity, spline_convexity = spline_convexity)
      all_covs <- c(non_spline_covs_initialized, spline_covs_initialized)
      if (remove_x_intercept) {
        covariates <- all_covs
        cov_list <- non_spline_covs
      } else {
        covariates <- c(all_covs, CovModel("intercept"))
        cov_list <- c(non_spline_covs, "intercept")
      }

    } else {

      ## Generate covariate list
      if (remove_x_intercept) {
        covariates <- c(lapply(addl_x_covs, CovModel))
        cov_list <- c(addl_x_covs, rep("spline", length(knots) + 1))
      } else {
        covariates <- c(lapply(addl_x_covs, CovModel), CovModel("intercept"))
        cov_list <- c(addl_x_covs, rep("spline", length(knots) + 1), "intercept")
      }

    }

    ## Run MR-BRT

    ## Global variables
    formatted_data <<- formatted_data
    mrbrt_response <<- mrbrt_response
    covariates <<- covariates
    reference_def <<- reference_def
    trim_pct <<- trim_pct

    results <- CWModel(
      cwdata = formatted_data,
      obs_type = mrbrt_response,
      cov_models = covariates,
      gold_dorm = reference_def,
      inlier_pct = (1 - trim_pct)
    )

    covs <- results$create_result_df()


    message("Predicting onto non-reference data points.")

    ## Global variables
    results <<- results
    to_adjust <<- to_adjust


    ## Predict on our prepped original data. Take the log-adjusted / logit-adjusted means and SEs.
    new_preds <- adjust_orig_vals(fit_object = results,
                                  df = to_adjust,
                                  orig_dorms = "definition",
                                  orig_vals_mean = "mean",
                                  orig_vals_se = "standard_error")

    new_predictions <- data.table(new_preds)

    new_predictions[, id := 1:nrow(new_predictions)]

    ## Use the predictions to adjust the original data.
    ## If reference def, do nothing (should be no reference def in this dataset but..). If non-reference, adjust both mean & SE.
    adjusted_data <- merge(new_predictions, to_adjust, by="id")

    ## The below comes from the math (see documentation)
    if(logit_transform) {

      adjusted_data[, mean_logit_adjusted := ifelse(get(reference_def) == 1, mean_logit, mean_logit - pred_diff_mean)]
      adjusted_data[, se_logit_adjusted := ifelse(get(reference_def) == 1, logit_se, sqrt(pred_diff_sd^2 + logit_se^2))]
      adjusted_data[, high_logit_adjusted := ifelse(get(reference_def) == 1, logit(upper), mean_logit_adjusted + 1.96*se_logit_adjusted)]
      adjusted_data[, low_logit_adjusted := ifelse(get(reference_def) == 1, logit(lower), mean_logit_adjusted - 1.96*se_logit_adjusted)]
      adjusted_data[, upper := inv.logit(high_logit_adjusted)]
      adjusted_data[, lower := inv.logit(low_logit_adjusted)]

      ## Logit back to normal space
      adjusted_data[, c("mean_adj", "se_adjusted") := data.table(delta_transform(mean = mean_logit_adjusted, sd = se_logit_adjusted, transformation = "logit_to_linear"))]

      ## adjusted se of mean = 0 in normal space, see comment in log-ratio below
      adjusted_data[, c("mean_adj_normal", "pred_se_normal") := data.table(delta_transform(mean = pred_diff_mean, sd = pred_diff_sd, transformation = "logit_to_linear"))]
      adjusted_data[mean==0, se_adjusted := ifelse(get(reference_def) == 1, se_adjusted, sqrt(se^2 + pred_se_normal^2))]
      adjusted_data[mean==0, `:=` (upper=mean_adj + 1.96*se_adjusted, lower=mean_adj - 1.96*se_adjusted)]
      
      if(xw_measure!= 'mtexcess'){
      adjusted_data[upper>1, upper := 1]
      }
      adjusted_data[lower<0, lower := 0]

    } else {

      ## In log-space, the uncertainty is the sum of prediction SE and original SE
      adjusted_data[, mean_log_adjusted := ifelse(get(reference_def) == 1, mean_log, mean_log - pred_diff_mean)]
      adjusted_data[mean != 0, se_log_adjusted := ifelse(get(reference_def) == 1, log_se, sqrt(pred_diff_sd^2 + log_se^2))]
      adjusted_data[mean != 0, high_log_adjusted := ifelse(get(reference_def) == 1, log(upper), mean_log_adjusted + 1.96*se_log_adjusted)]
      adjusted_data[mean != 0, low_log_adjusted := ifelse(get(reference_def) == 1, log(lower), mean_log_adjusted - 1.96*se_log_adjusted)]
      adjusted_data[mean != 0, `:=` (upper=exp(high_log_adjusted), lower=exp(low_log_adjusted))]

      ## Take out of log space
      adjusted_data[, c("mean_adj", "se_adjusted") := data.table(delta_transform(mean = mean_log_adjusted, sd = se_log_adjusted, transformation = "log_to_linear"))]

      ## However, if mean is 0, we need to adjust the uncertainty in normal space, not log-space
      adjusted_data[, c("mean_adj_normal", "pred_se_normal") := data.table(delta_transform(mean = pred_diff_mean, sd = pred_diff_sd, transformation = "log_to_linear"))]
      adjusted_data[mean==0, se_adjusted := ifelse(get(reference_def) == 1, se_adjusted, sqrt(se^2 + pred_se_normal^2))]
      adjusted_data[mean==0, `:=` (upper=mean_adj + 1.96*se_adjusted, lower=mean_adj - 1.96*se_adjusted)]
      
      if(xw_measure=='mtexcess'){
      adjusted_data[upper>1, upper := 1]
      }
      adjusted_data[lower<0, lower := 0]
    }


    data_for_comparison <- copy(adjusted_data)
    data_for_comparison <- rbind(data_for_comparison, original_data_no_xw, fill=T)

    ## Collapse back to our preferred data structure
    full_dt <- copy(adjusted_data)

    ## remove "aggregated nationals", then remove the aggregated column
    if("aggregated" %in% names(full_dt)) {
      full_dt <- full_dt[aggregated == F]
      full_dt$aggregated <- NULL
      message("Aggreated location removed from full_dt \n")
    }

    if(logit_transform) {

      full_dt[, `:=` (mean = mean_adj, standard_error = se_adjusted, upper = upper, lower = lower,
                      cases = "", sample_size = "", uncertainty_type_value = "",
                      note_modeler = paste0(note_modeler, " | crosswalked with logit difference: ", round(pred_diff_mean, 2), " (",
                                            round(pred_diff_sd), ")"))]

    } else {

      full_dt[, `:=` (mean = mean_adj, standard_error = se_adjusted, upper = upper, lower = lower,
                      cases = "", sample_size = "", uncertainty_type_value = "",
                      note_modeler = paste0(note_modeler, " | crosswalked with log(ratio): ", round(pred_diff_mean, 2), " (",
                                            round(pred_diff_sd), ")"))]

    }

    extra_cols <- setdiff(names(full_dt), names(original_data_no_xw))
    full_dt[, c(extra_cols) := NULL]

    final_dt <- rbind(original_data_no_xw, full_dt, fill=T)

    message("Done crosswalking.")

  }

  if (sex_split_only) final_dt <- copy(sex_specific_data)

  ## save crosswalk results to some place

  if (write) {

    # adjust data to pass epiDataUploader
    final_dt_upload <- copy(final_dt)

    if(model_abbr == "endo_121") {
      final_dt_upload <- endo_upload(final_dt_upload)
    }
    final_dt_upload$underlying_nid <- NA
    final_dt_upload$crosswalk_parent_seq <- final_dt_upload$seq
    final_dt_upload$seq <- NA

    final_dt_upload$group_review <- NA
    final_dt_upload$specificity <- NA
    final_dt_upload$group <- NA

    final_dt_upload$upper <- NA
    final_dt_upload$lower <- NA
    final_dt_upload$uncertainty_type_value <- NA

    final_dt_upload$sampling_type <- NA

    final_dt_upload[, grep("cv_*", names(final_dt_upload)) := NULL]

    write.xlsx(final_dt_upload, save_crosswalk_result_file, sheetName="extraction")

    message(paste0("Crosswalk results saved to ", save_crosswalk_result_file , " as xlsx"))

    if (upload_crosswalk_version) {

      if (is.null(bundle_version_id) | missing(bundle_version_id)) {

        stop("You need to set bundle_version_id to upload data.")

      } else {

        description <- "MR-BRT network meta-regression"
        save_crosswalk_version_result <- save_crosswalk_version(
          bundle_version_id=bundle_version_id,
          data_filepath=save_crosswalk_result_file,
          description=description)

        print(sprintf('Request status: %s', save_crosswalk_version_result$request_status))
        print(sprintf('Request ID: %s', save_crosswalk_version_result$request_id))
        print(sprintf('Bundle version ID: %s', save_crosswalk_version_result$bundle_version_id))

        message("crosswalk version uploaded.")
      }

    }
  }

  if (num_both_sex > 0) {
    if (sex_split_only) {

      py_save_object(object = sex_results, filename = 
                       paste0(storage_folder, model_abbr , "_sex_results.pkl"), pickle = "dill")
      py_save_object(object = formatted_data_sex, filename = 
                       paste0(storage_folder, model_abbr , "_cwdata_sex.pkl"), pickle = "dill")
      
      sex_split_documentation <- list(arguments = arguments,
                                    cwdata_sex = formatted_data_sex,
                                    sex_results=sex_results,
                                    sex_counts=matches_result,
                                    sex_covs=sex_covariates,
                                    sex_ratios=sex_matches,
                                    sex_specific_data=sex_specific_data,
                                    ratio_mean = ratio_mean,
                                    ratio_se = ratio_se)
      return(sex_split_documentation)

    } else {

      py_save_object(object = results, filename = 
                       paste0(storage_folder, model_abbr , "_xw_results.pkl"), pickle = "dill")
      py_save_object(object = formatted_data_xw, filename = 
                       paste0(storage_folder, model_abbr , "_cwdata_xw.pkl"), pickle = "dill")
      py_save_object(object = sex_results, filename = 
                       paste0(storage_folder, model_abbr , "_sex_results.pkl"), pickle = "dill")
      py_save_object(object = formatted_data_sex, filename = 
                       paste0(storage_folder, model_abbr , "_cwdata_sex.pkl"), pickle = "dill")
      
      sex_split_crosswalk_documentation <- list(arguments = arguments,
                                                data=final_dt,
                                                cwdata_xw = formatted_data_xw,
                                                xw_results=results,
                                                xw_counts=matches_count,
                                                xw_covs=covs,
                                                xw_ratios=matches,
                                                data_for_comp=data_for_comparison,
                                                cwdata_sex = formatted_data_sex,
                                                sex_results=sex_results,
                                                sex_counts=matches_result,
                                                sex_covs=sex_covariates,
                                                sex_ratios=sex_matches,
                                                sex_specific_data=sex_specific_data,
                                                ratio_mean = ratio_mean,
                                                ratio_se = ratio_se)
      return(sex_split_crosswalk_documentation)

      }
    } else {

      py_save_object(object = results, filename = 
                       paste0(storage_folder, model_abbr , "_xw_results.pkl"), pickle = "dill")
      py_save_object(object = formatted_data_xw, filename = 
                       paste0(storage_folder, model_abbr , "_cwdata_xw.pkl"), pickle = "dill")
      
    crosswalk_documentation <- list(arguments = arguments,
                                    data=final_dt,
                                    cwdata_xw = formatted_data_xw,
                                    xw_results=results,
                                    xw_counts=matches_count,
                                    xw_covs=covs,
                                    xw_ratios=matches,
                                    data_for_comp=data_for_comparison)
    return(crosswalk_documentation)

    }

}
