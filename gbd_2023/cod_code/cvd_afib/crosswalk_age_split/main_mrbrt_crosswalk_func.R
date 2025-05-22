
##
## Author: USERNAME
## Purpose: Main script that contains the MR-BRT crosswalk process. 
## Documentation: FILEPATH/crosswalk_documentation.docx
##

library(data.table)
library(gtools)
library(boot)
library(reticulate)

## load MR-BRT package/library
reticulate::use_python("FILEPATH/python")
xwalk <- import("crosswalk")

# wrappers, cleaning functions
source('FILEPATH/clean_mr_brt.R')
source("FILEPATH/data_tests.R")
source("FILEPATH/model_helper_functions.R")
source("FILEPATH/mapping_gbd_release_and_round.R")

# central computation functions - interact with database
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_location_metadata.R")

mad.stats <- function(x) {
  c(
    mad = mad(x),
    median = median(x)
  )
}
"%ni%" <- Negate("%in%")


main_mrbrt_crosswalk <- function(
    model_abbr,
    data,
    storage_folder,
    reference_def=NULL,
    alternate_defs=NULL,
    xw_measure,
    release_id,
    logit_transform=F,
    save_crosswalk_result_file=NULL,
    bundle_version_id=NULL,
    sex_split_only=F,
    age_overlap=5,
    year_overlap=5,
    age_range=10,
    year_range=10,
    subnat_to_nat=F,
    allow_wide_age_bin_match=T,
    sex_age_overlap=0,
    sex_year_overlap=0,
    sex_age_range=0,
    sex_year_range=0,
    dorm_separator = "AND",
    id_vars="id_var",
    spline_covs=NULL,
    degree=3,
    knot_placement=NULL,
    knot_number=3,
    knots=c(-1, 0, 1),
    r_linear=F,
    l_linear=F,
    spline_monotonicity=NULL,
    spline_convexity=NULL,
    trim_pct=0.1,
    remove_x_intercept=F,
    sex_remove_x_intercept=F,
    addl_x_covs,
    sex_covs="age_scaled",
    use_gr=F,
    offset_method='set_value',
    offset=1e-7,
    set_NA_covs_to_0=T,
    
    if_save_bundle_version = NA,
    acause = NA,
    modelable_entity_id = NA,
    model = NA,
    bundle_id = NA,
    xw_index = NA,
    reference_def_text = NA,
    clinical_informatics = NA,
    subset_by_age = NA,
    add_aggregate_subnats = NA,
    add_cov_to_data = NA,
    added_cov_mv_id = NA,
    
    test_matches = F,
    plot_output_folder = NA,
    test_age_overlap = NA,
    test_year_overlap = NA,
    test_age_range = NA,
    test_year_range = NA,
    test_subnat_to_nat = NA,
    test_allow_wide_age_bin_match = NA
) {
  
  date<-gsub("-", "_", Sys.Date())
  
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
  pull_args <- arguments[!names(arguments) %in% c("", "data")]
  for (i in 1:length(pull_args)) {
    arg_name <- names(pull_args)[i]
    arg <- tryCatch({
      get(paste0(pull_args[i]))
    }, error = function(e) {
      return(paste0(pull_args[i]))
    })
    
    if(length(arg)==0){
      args_dt[,paste0(arg_name) := NA]
    } else if (length(arg)==1) {
      if(arg=='NULL'){
        args_dt[,paste0(arg_name) := NA]
      } else {
        args_dt[,paste0(arg_name) := arg]
      }
    } else {
      args_dt[,paste0(arg_name) := paste0(arg, collapse=",")]
    }
  }
  
  arguments <- args_dt
  
  ########### Function checks ######################
  ##################################################
  
  if (is.null(save_crosswalk_result_file)) stop("Provide a file path to write the crosswalk verison")
  if (is.null(addl_x_covs) & remove_x_intercept) stop("You have to include an intercept if you have no additional covariates")
  
  ########### Naming variables #####################
  ##################################################
  data <- data[is_outlier==0,]
  if (!use_gr) data <- data[input_type!="group_review",]
  
  message(paste0("You ", ifelse(nrow(data[sex=="Both",])>0, "do", "don't"), " need to sex-split."))
  message(paste0("You ", ifelse(sex_split_only, "won't be", "will also be"), " crosswalking."))
  
  ## We can either use logit transformation or log transformation 
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
  
  if (!sex_split_only) {
    
    # deal with missing study covariates
    for(def in alternate_defs){
      rw <- nrow(data[is.na(get(def))])
      if(rw>0){
        if(set_NA_covs_to_0){
          message(paste0("There are ", rw, " row(s) in the data with missing values for ", def, ". These will be set to 0."))
          data[is.na(get(def)), paste0(def) := 0]
        } else {
          stop(paste0("There are ", rw, " row(s) in the data with missing values for ", def, ". Please fix before continuing."))
        }
      }
    }
    
    ## If we don't have a "reference" column, making one so we can figure out what we need to XW
    if ((is.null(reference_def) | (missing(reference_def)))) {
      if (length(alternate_defs) > 1) data[rowSums(data[, ..alternate_defs])==0, reference := 1]
      if (length(alternate_defs) == 1) data[get(alternate_defs) == 0, reference := 1]
      data[is.na(reference), reference := 0]
      reference_def <- "reference"
    }
  }
  
  ## NRVDs don't have "note_modeler" as a column, which we need later.
  if (!("note_modeler" %in% names(data))) data[, note_modeler := ""]
  
  
  ################################################################
  ############## Offset data #####################################
  ################################################################
  
  if(offset_method=='drop'){
    
    warning("You are using the 'drop' offset method, which will remove all both-sex or alternate definition data points with 0s.")
    saved_zero_data <- data[mean==0&sex!='Both']
    if(!sex_split_only){
      saved_zero_data <- saved_zero_data[get(reference_def)==1]
    }
    
    data <- data[mean != 0]
    
  } else if(offset_method=='set_value'){
    
    data[mean == 0, `:=` (mean = offset, note_modeler = paste0(note_modeler, " offset for xwalking"))]
    
  } else if(offset_method=='dismod'){
    
    offset <- .01*(median(data[mean != 0, mean]))
    data[mean == 0, `:=` (mean = offset, note_modeler = paste0(note_modeler, " offset for xwalking"))]
    
  } else {
    warning("No offset method specified")
  }
  
  
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
                             year_overlap = sex_year_overlap,
                             age_range = sex_age_range,
                             year_range = sex_year_range,
                             exact_match = T, quiet=T)
    
    ## Pull out this object to return later.
    matches_result <- sex_matches$matches
    
    ## Log transform the matched data points.
    sex_matches <- log_transform_mrbrt(sex_matches$data)
    
    ## Adjust the age, sex, etc., make an ID variable
    sex_matches <- adj_mr_brt(sex_matches, sex = F, xw_covs = sex_covs)
    sex_matches[, id_var := 1:nrow(sex_matches)] 
    
    ## Give names to Female/Male comparison
    sex_matches[, dorm_alt := "Female"]
    sex_matches[, dorm_ref := "Male"]
    
    ## Pull out both-sex data and sex-specific data. both_sex is used for predicting and sex_specific will be the data we use.
    both_sex <- adj_mr_brt(data[sex=="Both",], original_data = T, year = F, sex=F, fix_zeros = F,
                           age_mean = mean(sex_matches$age_mid_ref), age_sd = sd(sex_matches$age_mid_ref), xw_covs = sex_covs)
    both_sex[, sex_dummy := "Female"]
    sex_specific <- data[sex!="Both",]
    n <- names(sex_specific)
    
    if(length(sex_covs)==1){
      sex_covs <- list(sex_covs)
    }
    
    # The CW functions can only access global variables 
    sex_matches <<- sex_matches
    sex_split_response <<- sex_split_response
    sex_split_data_se <<- sex_split_data_se
    sex_covs <<- sex_covs
    id_vars <<- id_vars
    
    ## Format data for meta-regression
    formatted_data <- xwalk$CWData(
      df = sex_matches,
      obs = sex_split_response,
      obs_se = sex_split_data_se,
      alt_dorms = "dorm_alt",
      ref_dorms = "dorm_ref",
      covs = sex_covs,
      study_id = id_vars
    )
    
    # pull out formatted data to return later
    formatted_data_sex <- copy(formatted_data)
    
    ## Generate covariate list
    if (sex_remove_x_intercept) {
      covariates <- c(lapply(sex_covs, xwalk$CovModel))
      all_covs <- sex_covs
    } else {
      covariates <- c(lapply(sex_covs, xwalk$CovModel), xwalk$CovModel("intercept"))
      all_covs <- c(sex_covs, "intercept")
    }
    
    ## Assigning to global variables
    formatted_data <<- formatted_data
    sex_mrbrt_response <<- sex_mrbrt_response
    covariates <<- covariates
    
    ## Launch MR-BRT model
    sex_results <- xwalk$CWModel(
      cwdata = formatted_data,
      obs_type = sex_mrbrt_response,
      cov_models = covariates,
      gold_dorm = "Male"
    )
    xwalk$CWModel$fit(sex_results, inlier_pct = 1-trim_pct)
    
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
    both_sex[, id := 1:nrow(both_sex)]
    sex_preds <- xwalk$CWModel$adjust_orig_vals(sex_results,
                                                df = both_sex,
                                                orig_dorms = "sex_dummy",
                                                orig_vals_mean = "mean",
                                                orig_vals_se = "standard_error",
                                                study_id = id_vars,
                                                data_id = 'id')
    sex_preds <- as.data.table(sex_preds)
    setnames(sex_preds, 'data_id', 'id')
    
    ## To return later
    ratio_mean <- sex_preds$pred_diff_mean
    ratio_se <- sqrt(sex_preds$pred_diff_sd^2 + as.numeric(sex_results$gamma))
    
    ## We need the population for each data point. Instead of pooling the population (for ex if you were age 3-10) for all years, we take the midpoint (3-10 would become 6)
    both_sex[, year_mid := (year_start + year_end)/2]
    both_sex[, year_floor := floor(year_mid)]
    both_sex[, age_floor:= floor(age_mid)]
    
    ## Pull out population data. We need age- sex- location-specific population.
    pop <- tryCatch({
      get_population(age_group_id = "all", sex_id = "all", release_id = release_id, year_id = unique(floor(both_sex$year_mid)),
                     location_id=unique(both_sex$location_id), single_year_age = T)
    }, error = function(e) {
      mapping <- map_release_and_round(release = release_id, type = 'epi')
      previous_release <- mapping$previous_release_id
      print(paste0('No population data available for release_id ', release_id, ', switching to previous release_id (release_id ', previous_release, ') population for sex-splitting'))
      return(get_population(age_group_id = "all", sex_id = "all", release_id = previous_release, year_id = unique(floor(both_sex$year_mid)),
                            location_id=unique(both_sex$location_id), single_year_age = T))
    })
    
    
    ids <- get_ids("age_group") ## age group IDs
    ids[age_group_id==28, age_group_name := '0']
    ids[age_group_id==238, age_group_name := '1']
    ids[age_group_id==235, age_group_name := '95']
    pop <- merge(pop, ids, by="age_group_id", all.x=T, all.y=F)
    # Age group names are sometimes characters
    suppressWarnings(pop$age_group_name <- as.numeric(pop$age_group_name))
    pop <- pop[!(is.na(age_group_name))]
    pop$age_group_id <- NULL
    
    ## Merge in populations for both-sex and each sex. Turning age bins into the midpoint - because the population ratios, not population itself, is what's important.
    both_sex <- merge(both_sex, pop[sex_id==3,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
    setnames(both_sex, "population", "population_both")
    both_sex <- merge(both_sex, pop[sex_id==1,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
    setnames(both_sex, "population", "population_male")
    both_sex <- merge(both_sex, pop[sex_id==2,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
    setnames(both_sex, "population", "population_female")
    
    ## There are some rows where population is NA - get rid of them
    if (nrow(both_sex[is.na(population_both)]) > 0) dropped_nids <- unique(both_sex[is.na(population_both), nid])
    both_sex <- both_sex[!is.na(population_both)]
    ids <- unique(both_sex$id)
    num <- nrow(data[sex=="Both",]) - length(ids)
    sex_preds <- sex_preds[id %in% ids,]
    if (num > 0) message(paste0("WARNING: You lost ", num, " both-sex data point(s) because we couldn't find a population for that point. NIDs are: ", paste0(dropped_nids, collapse=', ')))
    
    ## Merge adjustments with unadjusted data
    both_sex <- merge(both_sex, sex_preds, by="id", allow.cartesian = T)
    
    ## Take mean, SEs into real-space so we can combine and make adjustments. First adding gamma.
    both_sex[, se_val := (sqrt(both_sex$pred_diff_sd^2 + as.vector(sex_results$gamma)))]
    both_sex[, c("real_pred_mean", "real_pred_se") := data.table(delta_transform(mean = pred_diff_mean, sd = se_val, transform = "log_to_linear"))]
    
    ## Make adjustments. 
    both_sex[, m_mean := mean * (population_both/(population_male + real_pred_mean * population_female))]
    both_sex[, f_mean := real_pred_mean * m_mean]
    
    ## Get combined standard errors
    both_sex[, m_standard_error := sqrt((real_pred_se^2 * standard_error^2) + (real_pred_se^2 * mean^2) + (standard_error^2 * real_pred_mean^2))]
    both_sex[, f_standard_error := sqrt((real_pred_se^2 * standard_error^2) + (real_pred_se^2 * mean^2) + (standard_error^2 * real_pred_mean^2))]
    
    ## Make male- and female-specific dts
    male_dt <- copy(both_sex)
    male_dt[, `:=` (mean = m_mean, standard_error = m_standard_error, upper = NA, lower = NA,
                    cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", real_pred_mean, " (",
                                          real_pred_se, ")"))]
    male_dt <- male_dt[, paste0(n), with=F]
    female_dt <- copy(both_sex)
    female_dt[, `:=` (mean = f_mean, standard_error = f_standard_error, upper = NA, lower = NA,
                      cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                      note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", real_pred_mean, " (",
                                            real_pred_se, ")"))]
    female_dt <- female_dt[, paste0(n), with=F] ## original names of data
    
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
    
    if(test_matches){
      
      test_args <- data.table(age_overlap = test_age_overlap, 
                              year_overlap = test_year_overlap, 
                              age_range = test_age_range, 
                              year_range = test_year_range, 
                              subnat_to_nat = test_subnat_to_nat,
                              allow_wide_age_bin_match = test_allow_wide_age_bin_match)
      
      # create folder for diagnostics if it doesn't exist
      dir.create(file.path(plot_output_folder, "/FILEPATH"), showWarnings = FALSE)
      
      # render rmarkdown of diagnostics to test the set of arguments
      rmarkdown::render(
        input = 'FILEPATH/xwalk_matches_diagnostics.Rmd',
        output_file = paste0(plot_output_folder, "FILEPATH/test_matches_", model_abbr, "_", date, ".html"),
        clean = TRUE,
        
        params = list(release_id = release_id, data = data, alternate_defs = alternate_defs, reference_def = reference_def,
                      logit_transform = logit_transform, model_abbr = model_abbr,
                      test_args = test_args)
      )
      
      return(paste0("Matching diagnostics have been saved to: ", 
                    plot_output_folder, "FILEPATH/test_matches_", model_abbr, "_", date, ".html"))
    }
    
    ## Find matches/create ratios. Function in "clean_mr_brt.R".
    message("Beginning crosswalks between alternate definition(s).")
    data <- match_mr_brt(
      data=data,
      release_id=release_id,
      reference_def=reference_def,
      alternate_defs=alternate_defs,
      age_overlap=age_overlap,
      year_overlap=year_overlap,
      age_range=age_range,
      year_range=year_range,
      subnat_to_nat = subnat_to_nat,
      quiet=T,
      allow_wide_age_bin_match=allow_wide_age_bin_match
    )
    
    ## Pull out these objects to return later
    matches_count <- data$counts
    
    ## Pull out prevalence matches, log-transform / logit-transform. Prep the matches for MR-BRT.
    if(logit_transform) {
      matches <- logit_transform_mrbrt(data$data)
    } else {
      matches <- log_transform_mrbrt(data$data)
    }
    
    invisible(lapply(names(matches),check_missing, df=matches, warn=T))
    matches <- adj_mr_brt(matches, xw_covs=addl_x_covs) ## Scaling age and year variables. Making sex into a dummy variable. Function in 'clean_mr_brt.R'
    
    ## Prep the original data for predicting. We need to scale the covariates, make SE, log-transform / logit-transform the SE.
    to_adjust <- adj_mr_brt(original_data_to_xw, nid=F, original_data = T, fix_zeros = F,
                            age_mean = mean(matches$age_mid_ref), age_sd = sd(matches$age_mid_ref), xw_covs=addl_x_covs) ## Same scaling function. Not creating an ID variable as you would for prepping MR-BRT.
    to_adjust[standard_error >= 1, standard_error := 0.99999]
    
    ## Make a covariate for "orig_dorms" column
    defs <- c(alternate_defs, reference_def)
    to_adjust$num_defs <- rowSums(to_adjust[, ..defs])
    multiple_defs <- ifelse(max(to_adjust$num_defs,na.rm=T) > 1, T, F)
    if(multiple_defs){
      for (def in defs) to_adjust[get(def) == 1 & num_defs == 1, definition := paste0(def)]
      to_adjust[, (alternate_defs) := lapply(.SD,as.logical), .SDcols = alternate_defs]
      to_adjust[, definition := toString(alternate_defs[unlist(.SD)]), by = 1:nrow(to_adjust), .SDcols = alternate_defs]
      to_adjust[, definition := gsub(" ","",definition)]
      to_adjust[, definition := gsub(",",dorm_separator,definition)]
      to_adjust[, definition := str_replace(definition,", ",dorm_separator)]
      to_adjust[, (alternate_defs) := lapply(.SD,as.integer), .SDcols = alternate_defs] #convert back to numeric.
      if (nrow(to_adjust[is.na(definition) | definition=='NA']) > 0) stop("Definitions haven't been labeled appropriately")
    } else {
      dorm_separator <- NULL
      for (def in defs) to_adjust[get(def) == 1, definition := paste0(def)]
      if (nrow(to_adjust[is.na(definition) | definition=='NA']) > 0) stop("Definitions haven't been labeled appropriately")
    }
    
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
        #message(paste0("Your new x-covariates are: ", print(x_covs)))
      }
    }
    
    format_covs <- addl_x_covs
    if (!is.null(spline_covs) && !(spline_covs %in% addl_x_covs)) {
      format_covs <- c(spline_covs, addl_x_covs)
    }
    if(length(format_covs)==1){
      format_covs <- list(format_covs)
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
    formatted_data <- xwalk$CWData(
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
    formatted_data_xw <- copy(formatted_data)
    
    ## Generate covariate list and initalize
    
    if (!is.null(spline_covs)) {
      
      if (length(spline_covs) > 1) stop(".")
      
      ## Pull out spline and normal covs to initialize separately
      if (spline_covs %in% addl_x_covs) {
        non_spline_covs <- setdiff(addl_x_covs, spline_covs)
      } else {
        non_spline_covs <- addl_x_covs
      }
      
      # Dynamically allocate knot placement if knot_placement != NULL
      # Dependent on the length(spline_covs)==1 rule
      if (is.null(knot_placement)==F){
        lb <- min(assigned_matches[,get(spline_covs)])
        ub <- max(assigned_matches[,get(spline_covs)])
        if (knot_placement=="rel_domain"){
          knots <- seq(from=lb,
                       to=ub,
                       length.out=knot_number)
        } else if (knot_placement=="rel_freq"){
          knots <- c()
          for (i in seq(from=0, to=1, length.out=knot_number)){
            knots <- c(knots, quantile(assigned_matches[,get(spline_covs)], i))
          }
        }
      }
      
      ## Check to ensure MSCA xspline requirement met: number of intervals (number of knots - 1) >= 1 + l_linear + r_linear
      if(!((length(knots) - 1) >= 1 + l_linear + r_linear)){
        stop("Requirements for xspline are not met. The number of knots - 1 must be >= 1 + number of linear tails")
      }
      
      ## Global variables
      knot_locations <<- knots
      degree <<- degree
      l_linear <<- l_linear
      r_linear <<- r_linear
      spline_monotonicity <<- spline_monotonicity
      spline_convexity <<- spline_convexity
      
      ## Initalize covariates
      non_spline_covs_initialized <- c(lapply(non_spline_covs, xwalk$CovModel))
      spline_covs_initialized <- xwalk$CovModel(cov_name = spline_covs, spline = xwalk$XSpline(knots = knot_locations, degree = as.integer(degree), l_linear = l_linear, r_linear = r_linear),
                                                spline_monotonicity = spline_monotonicity, spline_convexity = spline_convexity)
      all_covs <- c(non_spline_covs_initialized, spline_covs_initialized)
      if (remove_x_intercept) {
        covariates <- all_covs
      } else {
        covariates <- c(all_covs, xwalk$CovModel("intercept"))
      }
      
    } else {
      
      ## Generate covariate list
      if (remove_x_intercept) {
        covariates <- c(lapply(addl_x_covs, xwalk$CovModel))
      } else {
        covariates <- c(lapply(addl_x_covs, xwalk$CovModel), xwalk$CovModel("intercept"))
      }
      
    }
    
    ## Run MR-BRT
    
    ## Global variables
    formatted_data <<- formatted_data
    mrbrt_response <<- mrbrt_response
    covariates <<- covariates
    reference_def <<- reference_def
    trim_pct <<- trim_pct
    
    results <- xwalk$CWModel(
      cwdata = formatted_data,
      obs_type = mrbrt_response,
      cov_models = covariates,
      gold_dorm = reference_def
    )
    xwalk$CWModel$fit(results, inlier_pct = (1 - trim_pct))
    
    ## Pull out covariate object to return later
    covs <- results$create_result_df()
    
    
    message("Predicting onto non-reference data points.")
    
    ## Global variables
    results <<- results
    to_adjust <<- to_adjust
    
    ## Predict on our prepped original data
    to_adjust[, c("ref_vals_mean", "ref_vals_sd", "pred_diff_mean", "pred_diff_sd", "id")] <- xwalk$CWModel$adjust_orig_vals(results,
                                                                                                                             df = to_adjust,
                                                                                                                             orig_dorms = "definition",
                                                                                                                             orig_vals_mean = "mean",
                                                                                                                             orig_vals_se = "standard_error")
    adjusted_data <- copy(to_adjust)
    
    ## Transform upper/lower
    adjusted_data[, ref_vals_upper := ifelse(get(reference_def) == 1, upper, ref_vals_mean + qnorm(0.975)*ref_vals_sd)]
    adjusted_data[, ref_vals_lower := ifelse(get(reference_def) == 1, lower, ref_vals_mean - qnorm(0.975)*ref_vals_sd)]
    
    if(logit_transform) {
      ## Adjust SE of mean = 0 in normal space, see comment in log-ratio below. Adjustment depends on the SE.
      adjusted_data[, c("mean_adj_normal", "pred_se_normal") := data.table(delta_transform(mean = pred_diff_mean, sd = sqrt(pred_diff_sd^2 + as.numeric(results$gamma)), transformation = "logit_to_linear"))]
    } else {
      ## However, if mean is 0, we need to adjust the uncertainty in normal space, not log-space
      adjusted_data[, c("mean_adj_normal", "pred_se_normal") := data.table(delta_transform(mean = pred_diff_mean, sd = sqrt(pred_diff_sd^2 + as.numeric(results$gamma)), transformation = "log_to_linear"))]
    }
    
    if(xw_measure!= 'mtexcess'){
      adjusted_data[ref_vals_upper>1, ref_vals_upper := 1]
    }
    adjusted_data[ref_vals_lower<0, ref_vals_lower := 0]
    
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
    full_dt <- full_dt[!(note_modeler %like% "REMOVE BEFORE UPLOAD")]
    if("xwalk_only" %in% names(full_dt)){
      full_dt <- full_dt[is.na(xwalk_only) | xwalk_only==0]
    }
    
    if(logit_transform) {
      
      full_dt[, `:=` (mean = ref_vals_mean, standard_error = ref_vals_sd, upper = ref_vals_upper, lower = ref_vals_lower,
                      cases = NA, sample_size = NA, uncertainty_type_value = 95,
                      note_modeler = paste0(note_modeler, " | crosswalked with logit difference: ", round(pred_diff_mean, 2), " (",
                                            round(pred_diff_sd), ")"))]
      
    } else {
      
      full_dt[, `:=` (mean = ref_vals_mean, standard_error = ref_vals_sd, upper = ref_vals_upper, lower = ref_vals_lower,
                      cases = NA, sample_size = NA, uncertainty_type_value = 95,
                      note_modeler = paste0(note_modeler, " | crosswalked with log(ratio): ", round(pred_diff_mean, 2), " (",
                                            round(pred_diff_sd), ")"))]
      
    }
    
    extra_cols <- setdiff(names(full_dt), names(original_data_no_xw))
    full_dt[, c(extra_cols) := NULL]
    
    final_dt <- rbind(original_data_no_xw, full_dt, fill=T)
    
    message("Done crosswalking.\n")
    
  }
  
  if (sex_split_only) final_dt <- copy(sex_specific_data)
  
  # remove offset
  if(offset_method %in% c('set_value', 'dismod')){
    final_dt[mean == offset & note_modeler %like% " offset for xwalking", `:=` (mean = 0, note_modeler = gsub(" offset for xwalking", "", note_modeler))]
  }
  if(offset_method=='drop'){
    final_dt <- rbind(final_dt, saved_zero_data, fill=T)
  }
  
  ## save crosswalk result
  # adjust data
  final_dt_upload <- copy(final_dt)
  
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
  
  message(paste0("\nCrosswalk results saved to ", save_crosswalk_result_file , " as xlsx. Results can be uploaded to the epi database after combining with results for other crosswalked measures."))
  
  
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
                                      sex_specific_data=final_dt,
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
