###############################################################################################################
## The script calculates asymptomatic and symptomatic proportions of total gallbladder and biliary cases
## based on population-based studies
###############################################################################################################

##READ IN EXTRACTED LITERATURE DATA ON ASYMPTOMATIC PROPORTIONS
dta <- as.data.table(read.csv("FILEPATH"))

  ##CALCULATE LOG RATIOS AND SE OF ASYMPTOMATIC TO TOTAL CASES USING LOG TRANSFORMATION
  dta <- subset(dta, cv_asymp==1)
  dta$mean <- dta$cases / dta$sample_size
  dta[, id := .GRP, by = c("nid")]
  dta[,  standard_error := sqrt((mean*(1-mean)) /sample_size)]
  dta$log_ratio <- log(dta$mean)
  dta$delta_log_se <- sapply(1:nrow(dta), function(i) {
    ratio_i <- dta[i, "mean"]
    ratio_se_i <- dta[i, "standard_error"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })

write.csv(dta, "FILEPATH", row.names = F)


#########################################################################################################################################
#STEP 3: FIT THE MR-BRT MODEL 
#########################################################################################################################################

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

covariate_name <- "_asymp"    #set covariate of interest for crosswalk (alt), used for the model name
trim <- 0.30 #this is used for the model name; actual trimming must be specified within the run_mr_brt function below

#Run MR-BRT
fit1 <- run_mr_brt(
  output_dir =  paste0("FILEPATH"),
  model_label = paste0("Step3_asymp_proportion", trim),
  data = dta,
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = trim, 
  mean_var = "log_ratio",
  se_var = "delta_log_se"
  )


#USE MR-BRT OUTPUT (POOLED RATIO OF ASYMPTOMATIC TO TOTAL CASES) TO SPLIT ASYMPTOMATIC CASES FROM TOTAL


