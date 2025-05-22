
#########################################################
## MODELING of prehospital scalar
## Use data prepared in prep_input_prehosp_data.R on ratio of
## diagnosed MI: (diagnosed + prehosp) MI
## Model in MR-BRT
#########################################################

## Source central functions: 
central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))
'%ni%' <- Negate('%in%')

##load necessary libraries. 

Sys.setenv(RETICULATE_PYTHON = 'FILEPATH')
reticulate::use_python("FILEPATH")
mr <- reticulate::import("mrtool")
cw <- reticulate::import("crosswalk")

library(boot)
library(data.table)
library(openxlsx)
library(data.table)
library(ggplot2)
library(plyr)
library(dplyr)
library(stringr)
library(tidyr)
library(cowplot)
library(gridExtra)

##age&loc
age_meta <- get_age_metadata(age_group_set_id=19)
age_meta[,age_mid := (age_group_years_start+age_group_years_end)/2]
age_meta[,age_round := floor(age_mid)]
age_meta[age_round==110, age_round := 97]

loc_meta <- get_location_metadata(location_set_id = 35, release_id=16)
date<-gsub("-", "_", Sys.Date())

##Read in the data: 

source('FILEPATH/prep_input_prehosp_data.R')

#Model in paratmeters for MR-BRT model
params <- data.table(openxlsx::read.xlsx('FILEPATH/mrbrt_parameters_sev.xlsx'))

args <- commandArgs(trailingOnly = TRUE)
model_id <- args[1]
covs <- params[model_run_id == model_id,]

message(paste0('MODEL ID:', model_id, ' RUNNING'))
spline_knots <- as.numeric(strsplit(as.character(covs$spline_knots),split=",",fixed=TRUE)[[1]])

write <- as.logical(covs$write)

## read in data
mi_model_id <- covs$mi_model_id
mi_xwalk_id <- covs$mi_xwalk_id
path_to_mi_xwalk <- "FILENAME"

make_data <- T
if(make_data){
  path_to_data <- make_mi_adj_data(mi_model_id, path_to_mi_xwalk, make_plots = F)
}else{
  path_to_data <- "FILEPATH/.xlsx"
}
ihd_sev_mi_dismod_input <- data.table(openxlsx::read.xlsx(path_to_data)) 

##prep the data

ihd_sev_mi_dismod_input <- ihd_sev_mi_dismod_input[nid != 120176,]                      ## SINO-MONICA studys
ihd_sev_mi_dismod_input <- ihd_sev_mi_dismod_input[nid %ni% c(528054,528051,496352)]    ## spain sources that are wide age/sex bin 

## Remove/offset over 1 percent
ihd_sev_mi_dismod_input <- ihd_sev_mi_dismod_input[remain_ihd_pct > 0 ] ## subset to proportion greater than 1

## calculate uncertainty for input proportion
ihd_sev_mi_dismod_input[is.na(cases), sample_size := (mean)/(standard_error^2)]
ihd_sev_mi_dismod_input[is.na(cases), cases := mean*sample_size]
ihd_sev_mi_dismod_input[cases < 1, cases:=1] ## need at least 1 case for stable standard error. 
ihd_sev_mi_dismod_input[,pct_se := sqrt(remain_ihd_pct*(1-remain_ihd_pct)/sample_size)]            ## calculate SE using binomial approx. 
ihd_sev_mi_dismod_input[,logit_mean := cw$utils$linear_to_logit(mean = 1-array(ihd_sev_mi_dismod_input$remain_ihd_pct), sd = array(ihd_sev_mi_dismod_input$pct_se))[[1]]]
ihd_sev_mi_dismod_input[,logit_se := cw$utils$linear_to_logit(mean = 1-array(ihd_sev_mi_dismod_input$remain_ihd_pct), sd = array(ihd_sev_mi_dismod_input$pct_se))[[2]]]

ihd_sev_mi_dismod_input[,age := (age_group_years_start+age_group_years_end)/2] ##Calculate midage
ihd_sev_mi_dismod_input[,sex := ifelse(sex_id == 1,0,1)]                       ##set binary dummy for sex. 
ihd_sev_mi_dismod_input[,age_scaled := (age-mean(ihd_sev_mi_dismod_input$age))/sd(ihd_sev_mi_dismod_input$age)]

## interaction between haqi & sev
ihd_sev_mi_dismod_input[,haqi_ihd_inter := haqi_mean*ihd_sev]

##subset age
ihd_sev_mi_dismod_input <- ihd_sev_mi_dismod_input[age > covs$age_above, ]
ihd_sev_mi_dismod_input <- ihd_sev_mi_dismod_input[age < covs$age_below,]

## group years
ihd_sev_mi_dismod_input[,year_mid := (year_start+year_end)/2]
ihd_sev_mi_dismod_input[is.na(year_mid),year_mid := year_id]
ihd_sev_mi_dismod_input[,`:=` (year_1990_2000 = ifelse(dplyr::between(year_mid,1987,2000),1,0),
                               year_2001_2005 = ifelse(dplyr::between(year_mid,2001,2005),1,0),
                               year_2006_2010 = ifelse(dplyr::between(year_mid,2006,2010),1,0),
                               year_2011_2015 = ifelse(dplyr::between(year_mid,2011,2015),1,0),
                               year_2016_2022 = ifelse(dplyr::between(year_mid,2016,2022),1,0),
                               year_2001_2010 = ifelse(dplyr::between(year_mid,2001,2010),1,0),
                               year_2011_2022 = ifelse(dplyr::between(year_mid,2011,2022),1,0),
                               year_2000_2004 = ifelse(dplyr::between(year_mid,2000,2004),1,0),
                               year_2005_2009 = ifelse(dplyr::between(year_mid,2005,2009),1,0),
                               year_2010_2014 = ifelse(dplyr::between(year_mid,2010,2014),1,0),
                               year_2015_2022 = ifelse(dplyr::between(year_mid,2015,2022),1,0))]
ihd_sev_mi_dismod_input[,year := year_mid]
ihd_sev_mi_dismod_input[,year_scaled := (year-mean(ihd_sev_mi_dismod_input$year))/sd(ihd_sev_mi_dismod_input$year)]

## HAQi for prediction datatable
haqi_df <- get_covariate_estimates(covariate_id = 1099,
                                   location_id = unique(loc_meta$location_id), 
                                   year_id = seq(1960,2022,1),
                                   release_id=16)
haqi_df[,mean_value := mean_value/100] #scale 0-1
haqi_df[,midyear := year_id ]
setnames(haqi_df, c('year_id','mean_value'),c('year','haqi'))

haqi_q <- quantile(haqi_df$haqi, c(0,.20,.40,.60,.80,1.0)) ## define quantiles of HAQi, 1980-2022 

min_haqi_loc <- haqi_df[which.min(haqi),location_id]
max_haqi_loc <- haqi_df[which.max(haqi),location_id]

ihd_sev_mi_dismod_input[haqi_mean>= 0 & haqi_mean < haqi_q[2] , haqi_group_cov := paste0('haqi_q1')]
ihd_sev_mi_dismod_input[haqi_mean>= haqi_q[2] & haqi_mean < haqi_q[3], haqi_group_cov := paste0('haqi_q2')]
ihd_sev_mi_dismod_input[haqi_mean>= haqi_q[3] & haqi_mean < haqi_q[4], haqi_group_cov := paste0('haqi_q3')]
ihd_sev_mi_dismod_input[haqi_mean>= haqi_q[4] & haqi_mean < haqi_q[5], haqi_group_cov := paste0('haqi_q4')]
ihd_sev_mi_dismod_input[haqi_mean>= haqi_q[5], haqi_group_cov := paste0('haqi_q5')]

## dummies for haqi quintiles
ihd_sev_mi_dismod_input[,`:=`(haqi_q1 = ifelse(haqi_group_cov=='haqi_q1',1,0),
                              haqi_q2 = ifelse(haqi_group_cov=='haqi_q2',1,0),
                              haqi_q3 = ifelse(haqi_group_cov=='haqi_q3',1,0),
                              haqi_q4 = ifelse(haqi_group_cov=='haqi_q4',1,0),
                              haqi_q5 = ifelse(haqi_group_cov=='haqi_q5',1,0))]

ihd_sev_mi_dismod_input[haqi_mean>= 0 & haqi_mean < haqi_q[3] , haqi_group_cov := paste0('haqi_q1_q2')]
ihd_sev_mi_dismod_input[,haqi_q1_q2 := ifelse(haqi_group_cov=='haqi_q1_q2',1,0)]

## interactions
ihd_sev_mi_dismod_input[,age_sex_inter := age*sex]
ihd_sev_mi_dismod_input[,age_haqi_inter := age*haqi_mean]
ihd_sev_mi_dismod_input[,haqi_sex_inter := haqi_mean*sex]
ihd_sev_mi_dismod_input[,ihd_sev_sex_inter := ihd_sev*sex]
ihd_sev_mi_dismod_input[,haqi_year_inter := haqi_mean*year]
ihd_sev_mi_dismod_input[,age_scaled_haqi_inter := age_scaled*haqi_mean]

## intercept
ihd_sev_mi_dismod_input[,intercept := 1]


message('MRBRT STARTING')
## Run MR-BRT
data <- mr$MRData()
data$load_df(
  data = ihd_sev_mi_dismod_input,  col_obs = "logit_mean", col_obs_se = "logit_se",
  col_covs = list("haqi_ihd_inter","haqi_mean","ihd_sev","age","age_scaled","sex",'year_1990_2000',
                  'year_2001_2005','year_2006_2010',
                  'year_2011_2015','year_2016_2022','year_2001_2010','year_2011_2022',
                  'year_2000_2004','year_2005_2009','year_2010_2014','year_2015_2022', 
                  'haqi_q1','haqi_q2','haqi_q3','haqi_q4','haqi_q5','haqi_q1_q2',
                  'age_sex_inter','haqi_sex_inter','ihd_sev_sex_inter', 'age_haqi_inter', 'age_scaled_haqi_inter','year', 'haqi_year_inter',
                  'intercept','year_scaled')) 

## add covariates
inc_spline <- as.logical(covs$use_spline)
mrbrt_nonspline_covs <- unlist(str_split(covs$non_spline_cov,","))
mrbrt_spline_covs <- ifelse(inc_spline,covs$spline_cov,NA)

## populate the covariate list for MR-BRT: 
i=1 
cov_list <- list()
use_re <- T
for(cov in mrbrt_nonspline_covs){
  ## special clause for intercept, where random effects implemented
  if(cov == 'intercept'){
    if(use_re){
      cov_list[[i]] = mr$LinearCovModel(cov, use_re = use_re, prior_beta_uniform = array(c(-300, 300)))
      i=i+1
    }else{
      cov_list[[i]] = mr$LinearCovModel(cov, prior_beta_uniform = array(c(-300, 300)))
      i=i+1
    }
  }else{
    if(cov=='year'){
      cov_list[[i]] = mr$LinearCovModel(cov, prior_beta_uniform = array(c(0, 0.2)))
      i=i+1
    }
    else{cov_list[[i]] = mr$LinearCovModel(cov)
    i=i+1}
  }
}

## if there are spline covariates: 
if(inc_spline){
  for(cov in mrbrt_spline_covs){
    
    if(covs$monotonicity == 'none'){
      cov_list[[i]] = mr$LinearCovModel(cov,
                                        use_spline = inc_spline,
                                        spline_knots_type = covs$knot_type,
                                        spline_degree = 3L,
                                        spline_knots = spline_knots,
                                        spline_l_linear = as.logical(covs$spline_l_linear), 
                                        spline_r_linear = as.logical(covs$spline_r_linear))
    }
    if(covs$monotonicity != 'none'){
      cov_list[[i]] = mr$LinearCovModel(cov,
                                        use_spline = inc_spline,
                                        spline_knots_type = covs$knot_type,
                                        spline_degree = 3L,
                                        spline_knots = spline_knots,
                                        spline_l_linear = as.logical(covs$spline_l_linear), 
                                        spline_r_linear = as.logical(covs$spline_r_linear),
                                        prior_spline_monotonicity = covs$monotonicity)
    }
  }}

trim_pct <- covs$trim_pct
model <- mr$MRBRT(data = data,
                  cov_models = cov_list,
                  inlier_pct = 1.00 - trim_pct)

## fit model
model$fit_model() #inner_print_level = 5L, inner_max_iter = 2000L, 200L, 300L
message('MRBRT FINISHED')

## create draws
n_samples <- 500L
samples <- model$sample_soln(sample_size = n_samples)

## Prediction datatable - estimation years, locations, ages, sexes

dummy_pred_df <- data.table(expand.grid(year=c(1980, 1985, 1990,1995,2000,2005,2010,2015,2020,2021,2022), location_id = loc_meta$location_id, 
                                        age=age_meta[age_mid >= 15, age_mid], sex=c(0,1)))
dummy_pred_df[age==110, age := 97.5] 
dummy_pred_df[,age_scaled := (age-mean(ihd_sev_mi_dismod_input$age))/sd(ihd_sev_mi_dismod_input$age)]
dummy_pred_df[,year_scaled := (year-mean(ihd_sev_mi_dismod_input$year))/sd(ihd_sev_mi_dismod_input$year)]

## IHD SEV for prediction datatable
ihd_df <- get_covariate_estimates(covariate_id = 580,
                                  location_id = unique(dummy_pred_df$location_id), 
                                  year_id = seq(1960,2022,1),
                                  release_id = 16)
ihd_df[,midyear := year_id ]
ihd_df <- data.table(merge(ihd_df, age_meta[,c('age_group_id','age_mid')], by='age_group_id'))
setnames(ihd_df, c('year_id','mean_value','age_mid'),c('year','ihd_sev','age'))
ihd_df[age==110, age := 97.5]
ihd_df[,sex:=ifelse(sex_id==1,0,1)]

## Merge on HAQi & IHD SEV
dummy_pred_df <- data.table(merge(dummy_pred_df,haqi_df[,c('year','location_id','haqi')], by=c('year','location_id')))
dummy_pred_df <- data.table(merge(dummy_pred_df,ihd_df[,c('age','sex','year','location_id','ihd_sev')], by=c('age','sex','year','location_id')))
setnames(dummy_pred_df, c('haqi'),c('haqi_mean'))
dummy_pred_df[,haqi_ihd_inter := haqi_mean*ihd_sev]

## year_id
dummy_pred_df[,`:=` (year_1990_2000 = ifelse(dplyr::between(year,1987,2000),1,0),
                     year_2001_2005 = ifelse(dplyr::between(year,2001,2005),1,0),
                     year_2006_2010 = ifelse(dplyr::between(year,2006,2010),1,0),
                     year_2011_2015 = ifelse(dplyr::between(year,2011,2015),1,0),
                     year_2016_2022 = ifelse(dplyr::between(year,2016,2022),1,0),
                     year_2001_2010 = ifelse(dplyr::between(year,2001,2010),1,0),
                     year_2011_2022 = ifelse(dplyr::between(year,2011,2022),1,0),
                     year_2000_2004 = ifelse(dplyr::between(year,2000,2004),1,0),
                     year_2005_2009 = ifelse(dplyr::between(year,2005,2009),1,0),
                     year_2010_2014 = ifelse(dplyr::between(year,2010,2014),1,0),
                     year_2015_2022 = ifelse(dplyr::between(year,2015,2022),1,0))]

## haqi groups
dummy_pred_df[haqi_mean>= 0 & haqi_mean < haqi_q[2] , haqi_group_cov := paste0('haqi_q1')]
dummy_pred_df[haqi_mean>= haqi_q[2] & haqi_mean < haqi_q[3], haqi_group_cov := paste0('haqi_q2')]
dummy_pred_df[haqi_mean>= haqi_q[3] & haqi_mean < haqi_q[4], haqi_group_cov := paste0('haqi_q3')]
dummy_pred_df[haqi_mean>= haqi_q[4] & haqi_mean < haqi_q[5], haqi_group_cov := paste0('haqi_q4')]
dummy_pred_df[haqi_mean>= haqi_q[5], haqi_group_cov := paste0('haqi_q5')]

## haqi group dummies
dummy_pred_df[,`:=`(haqi_q1 = ifelse(haqi_group_cov=='haqi_q1',1,0),
                    haqi_q2 = ifelse(haqi_group_cov=='haqi_q2',1,0),
                    haqi_q3 = ifelse(haqi_group_cov=='haqi_q3',1,0),
                    haqi_q4 = ifelse(haqi_group_cov=='haqi_q4',1,0),
                    haqi_q5 = ifelse(haqi_group_cov=='haqi_q5',1,0))]

if('haqi_q1' %ni% unique(ihd_sev_mi_dismod_input$haqi_group_cov)){
  dummy_pred_df[haqi_mean>= 0 & haqi_mean < haqi_q[3] , haqi_group_cov := paste0('haqi_q1_q2')]
  dummy_pred_df[,haqi_q1_q2 := ifelse(haqi_group_cov=='haqi_q1_q2',1,0)]
}


## interactions
dummy_pred_df[,age_sex_inter := age*sex]
dummy_pred_df[,age_haqi_inter := age*haqi_mean]
dummy_pred_df[,haqi_sex_inter := haqi_mean*sex]
dummy_pred_df[,ihd_sev_sex_inter := ihd_sev*sex]
dummy_pred_df[,haqi_year_inter := haqi_mean*year]
dummy_pred_df[,age_scaled_haqi_inter := age_scaled*haqi_mean]

## intercept
dummy_pred_df[,intercept := 1]

## Predict
dat_pred <- mr$MRData()

dat_pred$load_df(
  data = dummy_pred_df, 
  col_covs=list("haqi_ihd_inter","haqi_mean","ihd_sev","age","age_scaled","sex",'year_1990_2000',
                'year_2001_2005','year_2006_2010',
                'year_2011_2015','year_2016_2022','year_2001_2010','year_2011_2022',
                'year_2000_2004','year_2005_2009','year_2010_2014','year_2015_2022',
                'haqi_q1','haqi_q2','haqi_q3','haqi_q4','haqi_q5','haqi_q1_q2',
                'age_sex_inter','haqi_sex_inter','ihd_sev_sex_inter', 'age_haqi_inter','age_scaled_haqi_inter', 'year', 'haqi_year_inter',
                'intercept','year_scaled')
)


## create draws
draws <- model$create_draws(
  data = dat_pred,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = TRUE) 

## summarize draws and create predictions
prediction_df <- model$predict(dat_pred)
pred_df <- data.table(cbind(dummy_pred_df,prediction_df))
setnames(pred_df, 'prediction_df','logit_pred')

draws <- inv.logit(draws) ## convert to normal space

pred_df$pred_lo <- apply(draws, 1, function(x) quantile(x, 0.05))
pred_df$pred_hi <- apply(draws, 1, function(x) quantile(x, 0.95))
#pred_df$pred <- apply(draws, 1, function(x) quantile(x, 0.50)) #median of draws for prediction
pred_df$pred <- apply(draws, 1, function(x) mean(x)) #mean of draws for prediction
pred_df[,pred_lower := pred_lo]
pred_df[,pred_upper := pred_hi]

## plot results
message('PLOTTING RESULTS...')
model_dt <- data.table(pred_df)
model_dt$sex_name <- ifelse(model_dt$sex == 1, "Female", "Male")
model_dt[,Trimming := paste0("MR-BRT")]
model_dt <- data.table(merge(model_dt,loc_meta[,c('location_id','location_name')], by='location_id'))

dev.off()

if(as.logical(covs$write)){
  write.xlsx(model_dt[,c('age_group_name','sex_name','year','location_name','location_id','pred','pred_lower','pred_upper')],
             paste0('FILEPATH/sev_approach_model_results_model_id_',model_id,'.xlsx' ))
}