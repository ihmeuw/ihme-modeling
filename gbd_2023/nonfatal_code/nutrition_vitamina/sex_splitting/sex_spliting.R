##########################################################################
### Author: 
### Date: 
### Project: Sex splitting for micronutreints
##########################################################################

rm(list=ls())
pacman::p_load(data.table, openxlsx, ggplot2, reshape2, dplyr,   msm, readxl)
date <- gsub("-", "_", Sys.Date())

# Config ##

library(reticulate)
Sys.setenv("RETICULATE_PYTHON" = "FILEPATH/python") 
use_python("FILEPATH/python")
library(boot) #for inverse logit
#library(scam)
library(data.table)
library(tidyverse)
library(mrbrt003, lib.loc = "FILEPATH/dev_packages/")
mr <- reticulate::import("mrtool")

# load shared functions

source("FILEPATH/get_outputs.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/save_bundle_version.R")  
source("/FILEPATH/get_bundle_data.R")
source("FILEPATHr/get_population.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_ids.R")

##``````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````


## get bundle version data 

bundle_id <- "bundle_id"  
release_id = 16
bv_id = "bv_id"
dt = get_bundle_version(bv_id)

### 

version = 1
raw_data = dt


### set the output directory

 output_dir = "FILEPATH"
 input_data_dir <- paste0(output_dir, "input_data/")
 if(!dir.exists(input_data_dir)){dir.create(input_data_dir)}
 data_file <- paste0(input_data_dir, "matched_sex_split_input_data_version", version, ".csv")
 label <- paste0("output_v", version)

##### make matches and save

 sex_specific_data <- dt[sex!="Both",]


## rename  the year_end and year_start variables
  
  
 match_vars <- c("underlying_nid", "original_age_start", "original_age_end", "location_id", "measure", "year_start", "year_end")
  
    # "underlying_nid" identify each of the surveys from WHO VMNIS  


  sex_specific_data[, full := paste(location_id, underlying_nid, original_age_start, original_age_end, sep = "-")]  
  female <- sex_specific_data[sex=="Female"]
  male <- sex_specific_data[sex=="Male"]
  fulls <- sex_specific_data[which(sex_specific_data$full %in% female$full & sex_specific_data$full %in% male$full )]
  setnames(female, c("val", "standard_error"), c("mean_Female", "standard_error_Female"))
  setnames(male, c("val", "standard_error"), c("mean_Male", "standard_error_Male"))
  
  new_data <- merge(female, male, by=c("full","original_age_start","original_age_end","location_id", "age_group_id"), allow.cartesian = TRUE)
  ratio_data <- new_data[,.(age_group_id, mean_Female, mean_Male, standard_error_Female, standard_error_Male, original_age_start, original_age_end, location_name.x, sample_size.x, sample_size.y, underlying_nid.x)]
  ratio_data <- ratio_data[mean_Female!=0 & mean_Male!=0]
  ratio_data[, `:=` (ratio = mean_Female/mean_Male,
                   ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
  ratio_data[, log_ratio:=log(ratio)]
  ratio_data$log_ratio_se <- sapply(1:nrow(ratio_data), function(i) {
    ratio_i <- ratio_data[i, "ratio"]
    ratio_se_i <- ratio_data[i, "ratio_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  
  ratio_data = ratio_data[!is.na(log_ratio)]
  ratio_data = ratio_data[!is.na(log_ratio_se)]
  
  ## Fit a linear regression model to see the relationship between male and females  
  
  model <- lm(mean_Female ~ mean_Male, data = ratio_data)

  # Print the summary of the model
  summary(model)

# plot male vs female prevalence
  
  pdf(paste0(output_dir,"output/diagnostic_plots.PDF"))
  gplot <- ggplot(ratio_data, aes(ratio_data$mean_Female, ratio_data$mean_Male, color=location_name.x))+
    geom_point(show.legend = FALSE)+
    geom_abline() +
     theme_bw()+
    labs(x="Mean Female Prevalence",y="Mean Male Prevalence")
  
  print(gplot)
   dev.off()
  message(paste0("There are ", nrow(ratio_data), " rows in the matched dataset. Now saving it to ", data_file))
  write.csv(ratio_data, data_file, row.names = FALSE )
  
  
  
## --------------------MRBRT sex splitting```````````````````````````````````````````````````````````````````````````#`
  
  
# load the data frame
  
  data <- mr$MRData()
  data$load_df(
    ratio_data,
    col_obs = "log_ratio",
    col_obs_se = "ratio_se",   
   col_covs = list("ratio"),
  col_study_id = "underlying_nid.x") # this is your random effect column
  
  
  # specify the model parameters:
  
  
  model <- mr$MRBRT(
    data = data,
    cov_models = list(mr$LinearCovModel("intercept", use_re=T)),
    inlier_pct = 1) # sets the portion of the data you want to keep

  
  model$fit_model()
  model$summary()
  
 df_pred1 <- data.frame(intercept = 1, ratio =1 )
  
  dat_pred1 <- mr$MRData()
  
  dat_pred1$load_df(
    data = df_pred1, 
    col_covs=list('ratio')
  )
  
  df_pred1$pred1 <- model$predict(data = dat_pred1)
  new_coefs <-  model$summary()
  new_coefs <- as.data.table(new_coefs)
  new_coefs[, beta:=exp(new_coefs$intercept)]
  
  #creating CI -- ONLY FOR FIXED EFFECTS
  
  n_samples1 <- as.integer(1000)
  
  # samples1 <- mr$other_sampling$sample_simple_lme_beta(
  samples1 <- model$sample_soln(
    sample_size = n_samples1
  )
  
  draws1 <- model$create_draws(
    data = dat_pred1,
    beta_samples = samples1[[1]],
    gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
    random_study = FALSE )
  
## creating CI including between study heterogeneity 
  
  nsamples2 <- 1000L

  gamma_vals2 <- matrix(
    data = model$gamma_soln,
    nrow = nsamples2,
    ncol = length(model$gamma_soln),
    byrow = TRUE # 'byrow = TRUE' is important to include
  )

  samples2 <- model$sample_soln(sample_size = nsamples2)

  draws2 <- model$create_draws(
    data = dat_pred1,
    beta_samples = samples2[[1]],
    gamma_samples = gamma_vals2,
    random_study = TRUE )
  

# creating datatable for results
  
  
  df_pred1$pred1_pt <- model$predict(data = dat_pred1)
  df_pred1$pred1_lo <- apply(draws1, 1, function(x) quantile(x, 0.025))
  df_pred1$pred1_up <- apply(draws1, 1, function(x) quantile(x, 0.975))
    pred1_pt_table <- as.data.frame(df_pred1$pred1_pt)

  
  new_coefs <- data.table(pred1_pt_table)
  setnames(new_coefs, "pred1_pt")
  new_coefs[, beta:=exp(new_coefs$pred1_pt)]
  new_coefs[, beta_se:=(df_pred1$pred1_up - df_pred1$pred1_lo) / 3.92]
  
  write.csv(new_coefs, paste0(output_dir, "/", label,"/prediction_coefficient.csv"), row.names = FALSE)
  result <- data.table(result="success", ratio_mean=new_coefs$beta, ratio_se=new_coefs$beta_se)
  
  
# applying the sex splitting 
    
    tosplit_dt <- dt[sex=="Both"]
    tosplit_dt <- tosplit_dt[location_id!=1]
    tosplit_dt[, year_id := floor((year_start + year_end)/2)]
    pops <- get_population(location_id = tosplit_dt[, unique(location_id)], sex_id = 1:3, release_id = 16,
                           year_id = tosplit_dt[, unique(year_id)], tosplit_dt$age_group_id)
    wide_pops <- reshape(pops, idvar = c("location_id","year_id", "run_id", "age_group_id"), timevar = c("sex_id"), direction = "wide")
    
    
##
    
    splitting_data <- merge(tosplit_dt, wide_pops, by = c("location_id", "year_id", "age_group_id"))
    split_dt <- copy(splitting_data)
    ratio_mean <- new_coefs$beta
    ratio_se <- new_coefs$beta_se
    pred_draws <- rnorm(1000, ratio_mean, ratio_se)
    split_dt[, paste0("male_", 1:1000) := lapply(1:1000, function(x)  val * (population.3 / (population.1 + pred_draws[x] *population.2 )))]
    split_dt[, paste0("female_", 1:1000) := lapply(1:1000, function(x) pred_draws[x] * get(paste0("male_", x))  )]
    
   
    split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 1:1000)]
    split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 1:1000)]
    split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 1:1000)]
    split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 1:1000)]
    split_dt[, c( paste0("male_", 1:1000), paste0("female_", 1:1000)) := NULL]
    split_dt[, old_prevalence := val]
    split_dt[, c("val", "standard_error", "run_id", "year_id", "age_group_id.x",  paste0("population.", 1:3)):= NULL]
    gplot <- ggplot(split_dt, aes(x=male_mean, y=female_mean))+ geom_point()
    
    print(gplot)
    
    female_data <- copy(split_dt)
    setnames(female_data, c("female_mean", "female_standard_error"), c("mean", "standard_error"))
    female_data$male_mean <- NULL
    female_data$male_standard_error <- NULL
    female_data$sex <- "Female"
    split_dt$female_mean <- NULL
    split_dt$female_standard_error <- NULL
    split_dt$sex <- "Male"
    setnames(split_dt, c("male_mean", "male_standard_error"), c("mean", "standard_error"))
    final_data <- rbind(split_dt, female_data, fill=TRUE)
    
## add identifier for splitted data 
    
    final_data$sex_splitted = 1
    final_data[, year_id := floor(original_year_start + original_year_end)/2]

## non splitted sex specific data
    
    setnames(sex_specific_data, "val", "mean")  # changed the prevalence column into mean because mean is the column name used in the age splitting functions. 
    sex_specific_data$old_prevalence = sex_specific_data$mean # For the sex specific data the mean and old prevalence data are exactly the same. created this column to make rbind easy. 
    sex_specific_data$sex_splitted = 0  # This column identified rows did not need sex splitting.
    sex_specific_data[, full := NULL]
    
## rbind the splitted and non splitted data
    
    final_data <- rbind(final_data, sex_specific_data)
    #final_data$origin_seq <- NA
    final_data[, upper:=mean+1.96*standard_error]
    final_data[, lower:=mean-1.96*standard_error]
    x <- nrow(final_data[mean < 0])
    y <- nrow(final_data[mean > 1])
    if(x!=0){message(paste0("There are ",x, " rows with a mean < 0. Setting those and lower bounds to 0."))}
    if(y!=0){message(paste0("There are ",y, " rows with a mean > 1. Setting those and upper bounds to 1."))}
    final_data[ lower < 0, lower:=0]
    final_data[mean < 0, mean:=0]
    final_data[upper > 1, upper:=1]
    final_data[mean > 1, mean:=1]
    
## load sex meta data and merge with final_data
    
    
    sex <- get_ids("sex")
    final_data = merge(final_data, sex, by = "sex")
    write_csv(final_data, "FILEPATH")
    
#############################################################################################################################################  
  

  