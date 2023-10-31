################################################################################################################
################################################################################################################
## Purpose: to crosswalk thyroid disorders
################################################################################################################
################################################################################################################

rm(list=ls())


## Source packages and functions
pacman::p_load(data.table, openxlsx, ggplot2, readr, RMySQL, openxlsx, readxl, stringr, tidyr, plyr, dplyr, gtools)
library(msm)
library(Hmisc, lib.loc = FILEPATH)
library(metafor, lib.loc=FILEPATH)
library(mortdb, lib.loc=FILEPATH)

base_dir <- FILEPATH
temp_dir <- FILEPATH
functions <- c("get_bundle_data", "upload_bundle_data","get_bundle_version", "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version",
               "get_age_metadata", "save_bulk_outlier","get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_draws", "get_population", "get_ids")
lapply(paste0(base_dir, functions, ".R"), source)


mrbrt_helper_dir <- FILEPATH
mrbrt_dir <- FILEPATH
draws <- paste0("draw_", 0:999)
functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}


date <- Sys.Date()
date <- gsub("-", "_", date)


#########################################################################################
##STEP 1: Set objects 
#########################################################################################
bundle <- BUNDLE_ID
acause <- CAUSE_NAME
step2_bundle_version <- BUNDLE_VERSION_ID 
gbd_round_id <- GBD_ROUND
decomp_step <- STEP

#For XWALK
mrbrt_cv <- "intercept"
xwalk_type<-"logit"

#For MAD outlier
MAD <- 2
output_filepath_bundle_data <-FILEPATH
output_filepath_mad <- FILEPATH
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) 
byvars <- c("location_id", "sex", "year_start", "year_end", "nid") 


#custom functions
## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}
## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}
## GET CASES IF THEY ARE MISSING
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "proportion", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

#########################################################################################
##STEP 2: get bundle version
#########################################################################################
    df_all = get_bundle_version(step2_bundle_version, fetch='all', export = FALSE)
    
    df_ci <- subset(df_all, clinical_data_type!="")
    df_lit <- subset(df_all, clinical_data_type=="")
    df_nhanes <- subset(df_all, nhanes==1 & group_review==1)
    

#########################################################################################
##STEP 3: TSH adjustment (within literature data)
#########################################################################################
    ratio <- as.data.table(read.xlsx(FILEPATH))
    ratio[, `:=` (prev_ref=cases_ref/sample_size, prev_alt = cases_alt/sample_size)]
    ratio[, `:=` (prev_se_ref=sqrt(prev_ref*((1-prev_ref)/sample_size)), prev_se_alt=sqrt(prev_alt*((1-prev_alt)/sample_size)))]
           
    #logit transform and take the difference between alt and ref       
    library(crosswalk, lib.loc = FILEPATH)
    ratio[, c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")]  <- as.data.frame(cbind(
                                                                                              delta_transform(
                                                                                                mean = ratio$prev_alt, 
                                                                                                sd = ratio$prev_se_alt,
                                                                                                transformation = "linear_to_logit" ),
                                                                                              delta_transform(
                                                                                                mean = ratio$prev_ref, 
                                                                                                sd = ratio$prev_se_ref,
                                                                                                transformation = "linear_to_logit")
                                                                                            ))
    ratio[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
                                                             df = ratio, 
                                                             alt_mean = "mean_alt", alt_sd = "mean_se_alt",
                                                             ref_mean = "mean_ref", ref_sd = "mean_se_ref" )
    
    
    #CWData() formats meta-regression data
    dat_ratio <- CWData(df = ratio, 
                       obs = "logit_diff",                         #matched differences in logit space
                       obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                       alt_dorms = "dorm_alt",                     #var for the alternative def/method
                       ref_dorms = "dorm_ref",                     #var for the reference def/method
                       study_id = "id" )                  
    
    
    #CWModel() runs mrbrt model
    results_ratio <- CWModel(
      cwdata = dat_ratio,                              #result of CWData() function  call
      obs_type = "diff_logit",                         #must be "diff_logit" or "diff_log"
      cov_models = list(CovModel("intercept")),        #specify covariate details
      inlier_pct = 0.9,
      gold_dorm = "reference")                         #level of "dorm_ref" that is the gold standard
    
    results_ratio$fixed_vars
    
    
    # Adjust lit data based on TSH cut-offs
    df_lit_tsh <- subset(df_lit, is_outlier==0 & group_review==1 & case_name == "total thyroidism")
    df_lit_tsh$note_modeler <- ""
    ratio_tsh <- unique(ratio[, c("hyperthyroidism", "hypothyroidism", "dorm_alt")])
    df_lit_tsh <- merge(df_lit_tsh, ratio_tsh)
    setnames(df_lit_tsh, "dorm_alt", "obs_method")
    
    df_lit_tsh_zero <- subset(df_lit_tsh, mean==0)
    df_lit_tsh_nonzero <- subset(df_lit_tsh, mean!=0)
    
    
    setnames(df_lit_tsh_nonzero, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
    df_lit_tsh_nonzero[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
                                                                                                        fit_object = results_ratio,            # result of CWModel()
                                                                                                        df = df_lit_tsh_nonzero,               # original data with obs to be adjusted
                                                                                                        orig_dorms = "obs_method",             # name of column with (all) def/method levels
                                                                                                        orig_vals_mean = "orig_mean",          # original mean
                                                                                                        orig_vals_se = "orig_standard_error" ) # standard error of original mean
                                                                                                      
    df_lit_tsh_nonzero[, `:=` (note_modeler = paste0(note_modeler, " | adjusted based on TSH"),
                            cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)]
    
    
    
    # Adjust mean = 0 based on TSH cut-offs---------------------------------------------------------------------------------------------
    unique(df_lit_tsh_zero$obs_method)
    offset <- 1e-7 
    df_lit_tsh_zero[, c("logit_mean", "logit_se") := data.table(delta_transform(mean = mean + offset, sd = standard_error, transform = "linear_to_logit"))]  # convert the mean and se of the original data to the space that the model was fit in
    
    results_ratio$beta_sd 
    results_ratio$gamma
    logit_pred <- sqrt( 0.06044845^2+0.01118401)          #(results$beta_sd)[1]^2 + results$gamma
    
    #add logit_pred_se and adjust original standard error
    df_lit_tsh_zero[, logit_pred_se := logit_pred]
    df_lit_tsh_zero[, logit_standard_error := sqrt(logit_se^2 + logit_pred_se^2)] 
    df_lit_tsh_zero[, c("mean_adj", "standard_error_adj") := data.table(delta_transform(mean = logit_mean, sd = logit_standard_error, transform = "logit_to_linear"))] # convert it back into normal space
    df_lit_tsh_zero$note_modeler <- ""
    df_lit_tsh_zero[, `:=` (mean = 0, standard_error = standard_error_adj,
                    note_modeler = paste0(note_modeler, " | uncertainty from TSH adjustment analysis added"),
                    cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)]
    
    
    #combine df_lit_tsh_zero and df_lit_tsh_nonzero---------------------------------------------------------------------------------------
    df_lit_tsh_adj <- as.data.table(rbind.fill(df_lit_tsh_zero, df_lit_tsh_nonzero, df_nhanes))
    df_lit_tsh_adj[, c("nid", "hyperthyroidism", "hypothyroidism", "mean", "orig_mean")]
    
    #remove unwanted variables
    df_lit_tsh_adj[, c("obs_method", "orig_mean", "orig_standard_error", "diff", "diff_se", "data_id", "mean_adj", "standard_error_adj", "logit_mean", "logit_se", "logit_pred_se", "logit_standard_error")] <- NULL

#########################################################################################
##STEP 4: Crosswalk: Intra-study matches
#########################################################################################
#NOTE: only included only_undetected; not enough matches within-study
    df_lit_tsh_adj_undected <- as.data.table(subset(df_lit_tsh_adj, refence==1 | only_undetected==1))
    df_lit_tsh_adj_undected[(refence==1), obs_method:="reference"]
    df_lit_tsh_adj_undected[(only_undetected==1), obs_method:="undiagnosed"]
    df_lit_tsh_adj_undected_zero <- subset(df_lit_tsh_adj_undected, mean==0)
    df_lit_tsh_adj_undected_nonzero <- subset(df_lit_tsh_adj_undected, mean!=0)
    
    
    #prep for crosswalk
    xwalk <- subset(df_lit, within_study_xwalk==1)
    xwalk <- xwalk[, c("nid", "location_id", "location_name", "site_memo", "sex", "age_start", "age_end", "measure", "mean", "standard_error", "refence", "exc_pregnant",	"only_undetected",	"undected_exc_preg",	"alt_combined", "case_name", "group_review", "is_outlier")]
    
    #1. subset to each case definition regardless of TSH cut-offics------------------------------------------------
    ref <- as.data.table(subset(xwalk, refence==1))
    only_undetected <- as.data.table(subset(xwalk, only_undetected==1))	

    #2. Create a variable identifying different case definitions----------------------------------------------------
    ref$dorm_ref<- "reference"
    only_undetected$dorm_alt<- "undiagnosed"
    
    #3. rename mean and se -----------------------------------------------------------------------------------------
    setnames(ref, c("mean", "standard_error"), c("prev_ref", "prev_se_ref"))
    setnames(only_undetected, c("mean", "standard_error"), c("prev_alt", "prev_se_alt"))
    
    #4. within-study matches-----------------------------------------------------------------------------------------
    only_undetected_matched <- merge(ref, only_undetected, by =  c("nid", "location_id", "site_memo", "sex", "age_start", "age_end", "measure", "case_name", "location_name"))
    only_undetected_matched$obs_method <- "undiagnosed"
    
    #5. logit transform mean
    dat_diff <- as.data.frame(cbind(
                                    delta_transform(
                                      mean = only_undetected_matched$prev_alt, 
                                      sd = only_undetected_matched$prev_se_alt,
                                      transformation = "linear_to_logit" ),
                                    delta_transform(
                                      mean = only_undetected_matched$prev_ref, 
                                      sd = only_undetected_matched$prev_se_ref,
                                      transformation = "linear_to_logit")
                                  ))
    names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")
    
    
    
    #6. calculate logit(prev_alt) - logit(prev_ref)
    only_undetected_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
                                                                      df = dat_diff, 
                                                                      alt_mean = "mean_alt", alt_sd = "mean_se_alt",
                                                                      ref_mean = "mean_ref", ref_sd = "mean_se_ref" )
    
    only_undetected_matched <- as.data.table(only_undetected_matched)
    only_undetected_matched[, id := .GRP, by = c("nid")]
    
    
    #CWData() formats meta-regression data
    dat_prev <- CWData(df = only_undetected_matched, 
                       obs = "logit_diff",                         #matched differences in logit space
                       obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                       alt_dorms = "dorm_alt",                     #var for the alternative def/method
                       ref_dorms = "dorm_ref",                     #var for the reference def/method
                       # covs = list("sex_id"),  #list of (potential) covariate columes 
                       study_id = "nid" )                  
    
    
    
    
    
    #CWModel() runs mrbrt model
    results_pre <- CWModel(
                              cwdata = dat_prev,                              #result of CWData() function  call
                              obs_type = "diff_logit",                    #must be "diff_logit" or "diff_log"
                              cov_models = list(CovModel("intercept")),   #specify covariate details
                            #    CovModel("sex_id")),  
                              inlier_pct = 0.9,
                              gold_dorm = "reference")                   #level of "dorm_ref" that is the gold standard
    
    results_pre$fixed_vars
    
    
    #save model output
    df_result<- results_pre$create_result_df()
    write.xlsx(df_result, FILEPATH) 
    py_save_object(object = results_pre, filename =  FILEPATH, pickle = "dill")
    
    

    #adjust
    setnames(df_lit_tsh_adj_undected_nonzero, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
    df_lit_tsh_adj_undected_nonzero[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
                                                                  fit_object = results_pre,                          # result of CWModel()
                                                                  df = df_lit_tsh_adj_undected_nonzero,              # original data with obs to be adjusted
                                                                  orig_dorms = "obs_method",                         # name of column with (all) def/method levels
                                                                  orig_vals_mean = "orig_mean",                      # original mean
                                                                  orig_vals_se = "orig_standard_error"               # standard error of original mean
                                                                )
    

    df_lit_tsh_adj_undected_nonzero[, `:=` (note_modeler = paste0(note_modeler, " | adjusted within-study undiag to ref"),
                               cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)]
    
    
    
    # Adjust mean = 0 based on TSH cut-offs---------------------------------------------------------------------------------------------
    offset <- 1e-7 
    df_lit_tsh_adj_undected_zero[, c("logit_mean", "logit_se") := data.table(delta_transform(mean = mean + offset, sd = standard_error, transform = "linear_to_logit"))]  # convert the mean and se of the original data to the space that the model was fit in
    
    results_pre$beta_sd 
    results_pre$gamma
    logit_pred <- sqrt(0.1327524^2+0.1024806)          #(results$beta_sd)[1]^2 + results$gamma
    
    #add logit_pred_se and adjust original standard error
    df_lit_tsh_adj_undected_zero[, logit_pred_se := logit_pred]
    df_lit_tsh_adj_undected_zero[, logit_standard_error := sqrt(logit_se^2 + logit_pred_se^2)] 
    df_lit_tsh_adj_undected_zero[, c("mean_adj", "standard_error_adj") := data.table(delta_transform(mean = logit_mean, sd = logit_standard_error, transform = "logit_to_linear"))] # convert it back into normal space
    df_lit_tsh_adj_undected_zero$note_modeler <- ""
    df_lit_tsh_adj_undected_zero[, `:=` (mean = 0, standard_error = standard_error_adj,
                            note_modeler = paste0(note_modeler, " | uncertainty from within-study undiag adjustment analysis added"),
                            cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)]
    
    
    #combine df_lit_tsh_zero and df_lit_tsh_nonzero---------------------------------------------------------------------------------------
    df_lit_tsh_adj_und <- as.data.table(rbind.fill(df_lit_tsh_adj_undected_zero, df_lit_tsh_adj_undected_nonzero))
    df_lit_tsh_adj_und[, c("nid", "hyperthyroidism", "hypothyroidism", "mean", "orig_mean", "obs_method", "measure")]
    
    #remove unwanted variables
    df_lit_tsh_adj_und[, c("obs_method", "orig_mean", "orig_standard_error", "diff", "diff_se", "data_id", "mean_adj", "standard_error_adj", "logit_mean", "logit_se", "logit_pred_se", "logit_standard_error")] <- NULL
    
    
#########################################################################################
##STEP 5: Sex-split; using all bundle version data (raw) aka df_all
#########################################################################################
    cv_drop <- c("")
    model_name <- "total_thyroid"
    
    sex_prev <-  copy(df_lit_tsh_adj_und[measure=="prevalence"])
    sex_inc <-  copy(df_lit_tsh_adj_und[measure=="incidence"])
    
    ## Prevalence first -------------------------------------------------------------------------------------
    #subset to both sex
    sex_specific_prev <- copy(sex_prev[sex != "Both" ])
    both_sex <- copy(sex_prev[sex == "Both" & measure=="prevalence"])
    both_sex[, id := 1:nrow(both_sex)]
    both_sex[, sex_dummy := "Female"]
    
    both_zero <- copy(both_sex)
    both_zero <- both_zero[mean == 0, ]
    nrow(both_zero)
    both_sex <- both_sex[mean != 0]
    
    #find sex matches--------------------------------------------------------------------------------------
    find_sex_match <- function(dt){
      sex_dt <- copy(dt)
      sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence", "incidence")]
      match_vars <- c( "location_name", "nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                       names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
      sex_dt[, match_n := .N, by = match_vars] #finding female-male pairs within each study, matched on measure, year, age, location_name
      sex_dt <- sex_dt[match_n >1] 
      keep_vars <- c(match_vars, "sex", "mean", "standard_error")
      sex_dt[, id := .GRP, by = match_vars] #ID: number of matches
      sex_dt <- dplyr::select(sex_dt, keep_vars) #keep only the variables in "keep_var" vector
      sex_dt<-data.table::dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate = mean) #reshape long to wide, to match male to female, rename mean and se with "x_Female" and "x_<ale"
      sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0] #drop where sex-specific mean is zero -> not informative 
      sex_dt[, id := .GRP, by = c("nid", "location_id")] #re-number based on nid and location id
      sex_dt$dorm_alt <- "Female"
      sex_dt$dorm_ref <- "Male"
      return(sex_dt)
    }
    sex_matches <- find_sex_match(sex_prev)

    #calculate logit difference between sex-specific mean estimates: female alternative; male reference-----
    #1. logit transform mean
    model <- paste0("sex_split_prevalence_", model_name,"_", date)
    sex_diff <- as.data.frame(cbind(
      delta_transform(
        mean = sex_matches$mean_Female, 
        sd = sex_matches$standard_error_Female,
        transformation = "linear_to_log" ),
      delta_transform(
        mean = sex_matches$mean_Male, 
        sd = sex_matches$standard_error_Male,
        transformation = "linear_to_log")
    ))
    names(sex_diff) <- c("mean_alt_Female", "mean_se_alt_Female", "mean_ref_Male", "mean_se_ref_Male")
    
    
    #2. calculate logit(prev_alt) - logit(prev_ref)
    sex_matches[, c("log_diff", "log_diff_se")] <- calculate_diff(
      df = sex_diff, 
      alt_mean = "mean_alt_Female", alt_sd = "mean_se_alt_Female",
      ref_mean = "mean_ref_Male", ref_sd = "mean_se_ref_Male" )
    
    
    #3. run MRBRT
    sex_model <- run_mr_brt(
      output_dir = FILEPATH,
      model_label = paste0(model_name, "_sex_split_prevalence_", date),
      data = sex_matches,
      overwrite_previous = TRUE,
      remove_x_intercept = FALSE,
      mean_var = "log_diff",
      se_var = "log_diff_se",
      study_id = "id",
      method = "trim_maxL",
      trim_pct = 0.10
    )
    
    
    
    #4. predict out
    sex_model$model_coefs
    sex_predictions <- predict_mr_brt(sex_model, newdata = data.table(midage = seq(42.5, 97.5, by = 5)), write_draws = T)
    
    #5. set up data for sex-splitting
    sex_plit_data <- both_sex
    sex_plit_data$midyear <- (sex_plit_data$year_end + sex_plit_data$year_start)/2
    
    #6. call sex-split functions
    get_row <- function(n, dt){
      row <- copy(dt)[n]
      pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                              age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]])
      agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
      row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
                  both_N = agg[sex == "Both", pop_sum])]
      return(row)
    }
    split_data <- function(dt, model){
      tosplit_dt <- copy(dt)
      nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)]
      tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))]
      tosplit_dt[, midyear := floor((year_start + year_end)/2)]
      location_ids <- tosplit_dt[, unique(location_id)]
      location_ids<-location_ids[!location_ids %in% NA]
      preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
      pred_draws <- as.data.table(preds$model_draws)
      pred_draws[, c("X_intercept", "Z_intercept") := NULL]
      pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
      ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
      ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
      
      pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                               year_ids = tosplit_dt[, unique(midyear)], location_ids =location_ids)
      
      
      pops[age_group_years_end == 125, age_group_years_end := 99]
      
      
      tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 9))
      tosplit_dt <- tosplit_dt[!is.na(both_N)] ## GET RID OF DATA THAT COULDN'T FIND POPS - RIGHT NOW THIS IS HAPPENING FOR 100+ DATA POINTS
      
      
      tosplit_dt[, merge := 1]
      pred_draws[, merge := 1]
      split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
      split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
      split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
      split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
      split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
      split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
      split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
      split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
      male_dt <- copy(split_dt)
      
      male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA, 
                      cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                      note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                            ratio_se, ")"))]
      male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
      male_dt <- dplyr::select(male_dt, names(sex_plit_data))
      female_dt <- copy(split_dt)
      
      female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA, 
                        cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                        note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                              ratio_se, ")"))]
      female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
      female_dt <- dplyr::select(female_dt, names(sex_plit_data))
      total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
      return(list(final = total_dt, graph = split_dt))
    }
    graph_predictions <- function(dt){
      graph_dt <- copy(dt[measure == "prevalence" | measure=="incidence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
      graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
      graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
      graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
      graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
      setnames(graph_dt_error, "value", "error")
      graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"))
      graph_dt[, N := (mean*(1-mean)/error^2)]
      graph_dt$N[graph_dt$N=="NaN"] <-NA
      graph_dt <-subset(graph_dt, N!="")
      wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
      graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
      graph_dt[, midage := (age_end + age_start)/2]
      ages <- c(60, 70, 80, 90)
      graph_dt[, age_group := cut2(midage, ages)]
      gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) + 
        geom_point() +
        geom_errorbar(aes(ymin = lower, ymax = upper)) +
        #  facet_wrap(~age_group) +
        labs(x = "Both Sex Mean", y = " Sex Split Means") +
        geom_abline(slope = 1, intercept = 0) +
        ggtitle("Sex Split Means Compared to Both Sex Mean by Age") +
        scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
        theme_classic()
      return(gg_sex)
    }
    
    #7. run sex-split functions
    predict_sex <- split_data(sex_plit_data, sex_model)
    final_dt_prev <- copy(predict_sex$final)
    final_dt_prev <- rbind.fill(final_dt_prev,sex_specific_prev )
    graph_dt_prev <- copy(predict_sex$graph)
    
    #8. create visualization 
    sex_graph <- graph_predictions(graph_dt_prev)
    sex_graph
    ggsave(filename = FILEPATH, plot = sex_graph, width = 6, height = 6)
    
    
    ## Incidence next ---------------------------------------------------------------------------------------
    #NOTE: No both sex data for incidence
    final_dt <- rbind.fill(final_dt_prev, sex_inc )
    write.csv(final_dt, FILEPATH, row.names = FALSE)
  
  
  
#########################################################################################
## STEP 6: Upload to age split model
#########################################################################################  
  agesplit <- as.data.table(subset(final_dt)) 
  
  agesplit$midage <- agesplit$age_end - agesplit$age_start
  agesplit <- subset(agesplit, midage <26)
  
  output_filepath_agesplit <-  FILEPATH
  write.xlsx(age, output_filepath_agesplit, sheetName = "extraction", col.names=TRUE)
  
  description <- DESCRIPTION
  result <- save_crosswalk_version(step2_bundle_version, FILEPATH, description=description)
  
  
#########################################################################################
## STEP 7: age-splitting
#########################################################################################
  # GET OBJECTS -------------------------------------------------------------
  b_id <- BUNDLE_ID 
  a_cause <- CAUSE_NAME
  name <- "thyroid"
  bundle_version_id <- step2_bundle_version
  id <- MODELVERSION_ID	      
  measure  <- 5 #5 is prevalence 6 is incidence
  region_pattern <- F #inc & prev global
  
  repo_dir <- FILEPATH
  date <- gsub("-", "_", date)
  draws <- paste0("draw_", 0:999)
  
  # GET FUNCTIONS -----------------------------------------------------------
  ## FILL OUT MEAN/CASES/SAMPLE SIZE
  get_cases_sample_size <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[is.na(mean), mean := cases/sample_size]
    dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
    dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
    return(dt)
  }

  ## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
  get_se <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
    z <- qnorm(0.975)
    dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    return(dt)
  }
  
  ## GET CASES IF THEY ARE MISSING
  calculate_cases_fromse <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[is.na(cases) & is.na(sample_size) & measure == "proportion", sample_size := (mean*(1-mean)/standard_error^2)]
    dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
    dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
    dt[is.na(cases), cases := mean * sample_size]
    return(dt)
  }
  
  ## MAKE SURE DATA IS FORMATTED CORRECTLY
  format_data <- function(unformatted_dt, sex_dt){
    dt <- copy(unformatted_dt)
    dt[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
               age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
    dt <- dt[measure %in% c("proportion", "prevalence", "incidence"),] 
    dt <- dt[!group_review==0 | is.na(group_review),] ##don't use group_review 0
    dt <- dt[is_outlier==0,] ##don't age split outliered data
    dt <- dt[(age_end-age_start)>25 & !is.na(exc_pregnant),] #for prevelance, incidence, proportion
    dt <- dt[(!mean == 0 & !cases == 0) |(!mean == 0 & is.na(cases))  , ] 
    dt <- merge(dt, sex_dt, by = "sex")
    dt[measure == "proportion", measure_id := 18]
    dt[measure == "prevalence", measure_id := 5]
    dt[measure == "incidence", measure_id := 6]
    
    dt[, year_id := round((year_start + year_end)/2, 0)] ##so that can merge on year later
    return(dt)
  }
  
  ## CREATE NEW AGE ROWS
  expand_age <- function(small_dt, age_dt = ages){
    dt <- copy(small_dt)
    
    ## ROUND AGE GROUPS
    dt[, age_start := age_start - age_start %%5]
    dt[, age_end := age_end - age_end %%5 + 4]
    dt <- dt[age_end > 99, age_end := 99]
    
    ## EXPAND FOR AGE
    dt[, n.age:=(age_end+1 - age_start)/5]
    dt[, age_start_floor:=age_start]
    dt[, drop := cases/n.age] 
    expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
    split <- merge(expanded, dt, by="id", all=T)
    split[, age.rep := 1:.N - 1, by =.(id)]
    split[, age_start:= age_start+age.rep*5]
    split[, age_end :=  age_start + 4]
    split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
    split[age_start == 0 & age_end == 4, age_group_id := 1]
    return(split)
  }
  
  ## GET DISMOD AGE PATTERN
  get_age_pattern <- function(locs, id, age_groups){
    age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, #
                             measure_id = measure, location_id = locs, source = "epi", ##Measure ID 5= prev, 6=incidence, 18=proportion
                             version_id = id,  sex_id = c(1,2), gbd_round_id =7, decomp_step = "iterative", #can replace version_id with status = "best" or "latest"
                             age_group_id = age_groups, year_id = 2010) ##imposing age pattern
    us_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2),
                                    age_group_id = age_groups, gbd_round_id =7, decomp_step = "iterative")
    us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
    age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
    age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
    age_pattern[, (draws) := NULL]
    age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
    
    ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
    age_1 <- copy(age_pattern)
    age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
    se <- copy(age_1)
    se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
    age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
    age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
    age_1[, frac_pop := population / total_pop]
    age_1[, weight_rate := rate_dis * frac_pop]
    age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
    age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
    age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
    age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
    age_1[, age_group_id := 1]
    age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
    age_pattern <- rbind(age_pattern, age_1)
    
    ## CASES AND SAMPLE SIZE
    age_pattern[measure_id == 18, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
    age_pattern[, cases_us := sample_size_us * rate_dis]
    age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
    age_pattern[is.nan(cases_us), cases_us := 0]
    
    ## GET SEX ID 3
    sex_3 <- copy(age_pattern)
    sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[, rate_dis := cases_us/sample_size_us]
    sex_3[measure_id == 18, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
    sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
    sex_3[is.nan(se_dismod), se_dismod := 0]
    sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
    sex_3[, sex_id := 3]
    age_pattern <- rbind(age_pattern, sex_3)
    
    age_pattern[, super_region_id := location_id]
    age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
    return(age_pattern)
  }
  
  ## GET POPULATION STRUCTURE
  get_pop_structure <- function(locs, years, age_groups){
    populations <- get_population(location_id = locs, year_id = years,decomp_step = "step2",
                                  sex_id = c(1, 2, 3), age_group_id = age_groups)
    age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
    age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
    age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
    age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
    age_1[, age_group_id := 1]
    populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
    populations <- rbind(populations, age_1)  ##add age group id 1 back on
    return(populations)
  }
  
  ## ACTUALLY SPLIT THE DATA
  split_data <- function(raw_dt){
    dt <- copy(raw_dt)
    dt[, total_pop := sum(population), by = "id"]
    dt[, sample_size := (population / total_pop) * sample_size]
    dt[, cases_dis := sample_size * rate_dis]
    dt[, total_cases_dis := sum(cases_dis), by = "id"]
    dt[, total_sample_size := sum(sample_size), by = "id"]
    dt[, all_age_rate := total_cases_dis/total_sample_size]
    dt[, ratio := mean / all_age_rate]
    dt[, mean := ratio * rate_dis ]
    dt <- dt[mean < 1, ]
    dt[, cases := mean * sample_size]
    return(dt)
  }
  
  ## FORMAT DATA TO FINISH
  format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
    dt <- copy(unformatted_dt)
    dt[, group := 1]
    dt[, specificity := "age,sex"]
    dt[, group_review := 1]
    dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
    blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
    dt[, (blank_vars) := NA]
    dt <- get_se(dt)
    if (region == T) {
      dt[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
    } else {
      dt[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_split_id, " ", date)]
    }
    split_ids <- dt[, unique(id)]
    dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
    dt <- dt[, c(names(df)), with = F]
    return(dt)
  }
  
  # RUN THESE CALLS ---------------------------------------------------------------------------
  final_dt$crosswalk_parent_seq <- final_dt$seq
  final_xwalked_prev <- subset(final_dt, measure=="prevalence")
  final_xwalked_inc <- subset(final_dt, measure=="incidence")
  
  ages <- get_age_metadata(19, gbd_round_id = 7)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  age_groups <- ages[age_start >= 5, age_group_id]
  
  df <- as.data.table(copy(final_xwalked_prev))
  df$sex_id <- NULL
  age <- age_groups
  gbd_id <- id
  
  location_pattern_id <- 1
  
  # AGE SPLIT FUNCTION -----------------------------------------------------------------------
  age_split <- function(gbd_id, df, age, region_pattern, location_pattern_id){
    
    ## GET TABLES
    sex_names <- get_ids(table = "sex")
    ages[, age_group_weight_value := NULL]
    ages[age_start >= 1, age_end := age_end - 1]
    ages[age_end == 124, age_end := 99]
    super_region_dt <- get_location_metadata(location_set_id = 22, gbd_round_id = 7)
    super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
    
    
    ## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
    original <- as.data.table(copy(df))
    original[, id := 1:.N]
    
    ## FORMAT DATA
    dt <- format_data(original, sex_dt = sex_names)
    dt <- get_cases_sample_size(dt)
    dt <- get_se(dt)
    dt <- calculate_cases_fromse(dt)
    
    ## EXPAND AGE
    split_dt <- expand_age(dt, age_dt = ages)
    
    ## GET PULL LOCATIONS
    if (region_pattern == T){
      split_dt <- merge(split_dt, super_region_dt, by = "location_id")
      super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
      locations <- super_regions
    } else {
      locations <- location_pattern_id
    }
    
    ##GET LOCS AND POPS
    pop_locs <- unique(split_dt$location_id)
    pop_years <- unique(split_dt$year_id)
    
    ## GET AGE PATTERN
    print("getting age pattern")
    age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age) #set desired super region here if you want to specify
    
    if (region_pattern == T) {
      age_pattern1 <- copy(age_pattern)
      split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
    } else {
      age_pattern1 <- copy(age_pattern)
      split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
    }
    
    ## GET POPULATION INFO
    print("getting pop structure")
    pop_locs<-pop_locs[!pop_locs %in% NA]
    
    pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
    split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))
    
    #####CALCULATE AGE SPLIT POINTS#######################################################################
    ## CREATE NEW POINTS
    print("splitting data")
    split_dt <- split_data(split_dt)
    
    
  }
  
  
  ######################################################################################################
  
  final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                   original_dt = original)
  
  #final_dt_inc <- copy(final_dt)
  
  final_dt <- rbind.fill(final_dt_inc, final_dt)
  
  final_dt$dup[duplicated(final_dt$seq)] <-1
  final_dt$dup[is.na(final_dt$dup)] <-0
  final_dt$seq[final_dt$dup==1] <-NA
  final_dt$crosswalk_parent_seq[final_dt$dup==1] <-NA
  final_dt$standard_error[final_dt$standard_error>1] <-1
  output_filepath2 <- FILEPATH
  write.xlsx(final_dt, output_filepath2, sheetName = "extraction", col.names=TRUE)
  
########################################################################################
#STEP 8: Upload MAD-outliered new data 
########################################################################################
  path_to_data2 <- output_filepath2
  description2 <- DESCRIPTION
  result <- save_crosswalk_version( step2_bundle_version, path_to_data2, description=description2)
  
  print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
  print(sprintf('Crosswalk version ID from decomp 2/3 best model: %s', result$previous_step_crosswalk_version_id))
  