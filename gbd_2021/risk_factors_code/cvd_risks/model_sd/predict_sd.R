##purpose:    -Predict bested SD model

if (!exists("rm_ctrl")) {
  rm(list = objects())
}
os <- .Platform$OS.type

  j <- FILEPATH
  h <- FILEPATH

date <- gsub("-", "_", Sys.Date())

library(data.table)
library(lme4)
library(boot)
library(mvtnorm)
library(ggplot2)

################### SCRIPTS #########################################
######################################################

code_root <- FILEPATH
results_folder <- FILEPATH
central <- FILEPATH

source(paste0(code_root, "utility/get_recent.R"))
source(paste0(code_root, "utility/data_tests.R"))
source(paste0(code_root, "utility/utility_functions.R"))
source(paste0(code_root, "utility/model_helper_functions.R"))

source(paste0(central, "get_model_results.R"))
source(paste0(central, "save_results_epi.R"))


################### ARGS AND PATHS #########################################
######################################################


if (!exists("me")) {
  me <- get_me_from_user()
  decomp_step <- get_step_from_user()
  
  make_preds<-T
}

step_num <- gsub('step', '', decomp_step)

usual_bp_folder <-paste0(results_folder, "usual_adjustment/", me, "/")
sd_mod_folder <- paste0(results_folder, "sd/", me, "_models/")
output_folder <- paste0(results_folder, "sd/", me, "_draws_", date, "/")

dir.create(output_folder)

################### CONDITIONALS #########################################
######################################################

if(me=="sbp"){
  me_dis<-"hypertension"
  meid<-2547
  sd_me_id<-15788
  bundle_id <- 4787
  crosswalk_version_id <- 29207
}

if(me=="chl"){
  me_dis<-"hypercholesterolemia"
  meid<-2546
}

if(me=="ldl"){
  me_dis<-"hypercholesterolemia"
  meid<-18822
  sd_me_id<-18823
  bundle_id <- 4904
  crosswalk_version_id <- 20540
}


################### GET DATA #########################################
######################################################

if(make_preds==T){
  ##USERNAME: get means to predict
  results <- get_model_results("epi", meid, location_set_id = 22, sex_id = c(1, 2),
                               decomp_step=decomp_step)
  results <- results[!age_group_id %in% c(22, 27)]
  
  ##USERNAME: get bested model
  sd_mod<-get_recent(sd_mod_folder, pattern=paste0("decomp", step_num))
  
  ##USERNAME: get usual_bp adjustments
  if(me=="sbp"){
    usual_bp <- get_recent(usual_bp_folder, pattern=paste0("decomp",step_num))
  }
  
  ################### CREATE DRAWS  #########################################
  ######################################################
  
  ##USERNAME: first get draws of covariates
  cov_mat<-vcov(sd_mod)
  par_draws<-rmvnorm(n=1000, mean=sd_mod$coefficients, sigma=cov_mat)
  pred_math<-"exp(X %*% betas)"
  
  ##USERNAME: clear out folder
  unlink(output_folder, recursive=T)
  dir.create(output_folder)
  
  ##USERNAME: loop over locations and predict, can parallelize at some point 
  for(loc in unique(results$location_id)){
    message("Creating draws for ", loc)
    results.l <- results[location_id == loc]
    
    results.l[, log_mean:=log(mean)]
    
    ##USERNAME: make design matrix
    X <- make_fixef_matrix(df=results.l, fixefs=c("log_mean", "factor(age_group_id)", "factor(sex_id)"), add_intercept = T)
    colnames(X)[1]<-"(Intercept)"
    if(!all(colnames(X)==colnames(par_draws))){stop("design matrix mismatch with draw matrix")}
    
    
    ##USERNAME:predict
    draw_list<-list(betas=par_draws)
    data_list<-list(X=X)
    temp_draws <- predict_draws(prediction_math = pred_math, draw_list=draw_list, data_list=data_list, return_draws=T)
    setnames(temp_draws, paste0("draw", 0:(ncol(temp_draws)-1)), paste0("draw_", 0:(ncol(temp_draws)-1)))
    out <- cbind(results.l[, .(location_id, year_id, age_group_id, sex_id)], temp_draws)
    
    
      
    ##USERNAME: apply usual bp adjustment if sbp
    if(me=="sbp"){
      out<-merge(out, usual_bp, by="age_group_id")
      out[, paste0("draw_", 0:999) := lapply(0:999, function(x) {
        get(paste0("draw_", x)) * value
      })]
      out[, value := NULL]
    }
    
    
    ##USERNAME: write out. 19 specifies 'continuous' measure id
    write.csv(out, file = paste0(output_folder, loc, "_19.csv"), row.names = F)
    
    message(sprintf('Your draws have been saved at %s', output_folder))
  }
}

################### SAVE RESULTS #########################################
######################################################

# If the user wants to save results, check
save_results_prompt <- 'Would you like to upload these results to the epi database? (y/[n]): '
save_results <- readline(prompt=save_results_prompt)

# they have enough resources
enough_resources_prompt <- paste0('Do you have enough resources ', 
                                  '(threads and memory) to run save_results? ',
                                  'Make sure to read the documentation! (y/[n]): ')
enough_resources <- readline(prompt=enough_resources_prompt)

if (tolower(enough_resources) != 'y') {
  exit_script('Find a node with enough resources and rerun!')
}

# whether they want to mark the model best
if (tolower(save_results) == 'y') {
  
  mark_best_prompt <- 'Would you like to mark this model best? (y/[n]): '
  mark_best_input <- readline(prompt=mark_best_prompt)
  if (tolower(mark_best_input) == 'y') {
    mark_best <- TRUE
    message('Saving model and marking best')
  } else {
    mark_best <- FALSE
    message('Saving model but not marking best')
  }
  
  # and ask for a model description
  description <- readline(prompt='Write a description of the model: ')
  description <- "Final SD results 1980-2022"
  save_results_epi(input_dir = output_folder, input_file_pattern = "{location_id}_{measure_id}.csv", year_id = 1980:2022, bundle_id = bundle_id, crosswalk_version_id = 29207,
                   modelable_entity_id = sd_me_id, description = description, mark_best = TRUE, measure_id = 19, decomp_step="iterative")

  } else {
  message('Draws not uploaded to epi database!')
}