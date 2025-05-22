################################################################################################################
## Title: MR-BRT Proportion of IHD deaths with 30-day hospitalization for AMI 
## Purpose: Model the proportion of IHD deaths that have a hospitalization for AMI within 30-days of death 
##          1.) Read in data sources on linkage of MI hospitalization to IHD death
##          2.) Run MR-BRT on the proportion with covariates
##          3.) Save proportions to split CSMR
################################################################################################################

## Source central functions: 
central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))

## Source negate
'%ni%' <- Negate('%in%')

## Source libraries
library(openxlsx)
library(data.table)
library(ggplot2)
library(plyr)
library(dplyr)
library(stringr)
library(tidyr)
library(boot)
library(gridExtra)

Sys.setenv(RETICULATE_PYTHON = '/FILEPATH/python')
reticulate::use_python("/FILEPATH/python")
mr <- reticulate::import("mrtool")
cw <- reticulate::import("crosswalk")

## Get metadata for age and location
message('Getting metadata...')
age_meta <- get_age_metadata(age_group_set_id=19)
age_meta[,age_mid := (age_group_years_start+age_group_years_end)/2]
age_meta[,age_round := floor(age_mid)]
age_meta[age_round==110, age_round := 97]

loc_meta <- get_location_metadata(location_set_id = 35, release_id=16)

date<-gsub("-", "_", Sys.Date())

## ESTABLISH AGE GROUPS
youngest_ages <- c('15 to 19', '20 to 24', '25 to 29', '30 to 34', '35 to 39', '40 to 44')
middle_ages <- c('45 to 49','50 to 54','55 to 59', '60 to 64','65 to 69')
old_ages  <- c('70 to 74', '75 to 79','80 to 84','85 to 89','90 to 94', '95 plus')

ten_yr_ages_g <- c('20 to 29','30 to 39','40 to 49','50 to 59','60 to 69','70 to 79','80 to 89','90 to 99')
ten_yr_ages   <- c(24.5, 34.5, 44.5, 54.5, 64.5, 74.5, 84.5, 94.5)   
ten_yr_age_g_id <- c(202, 210, 217, 224, 229, 47, 268, 271)


ages <- c(8:20, 30, 31, 32, 235)
all_ages <- c(youngest_ages, middle_ages, old_ages)
midage   <- c(17.5,22.5,27.5,32.5,37.5,42.5,47.5,52.5,57.5,62.5,67.5,72.5,77.5,82.5,87.5,92.5,97.5)
age_map_dt <- data.table(age_group = all_ages, age = midage, age_group_id = as.character(ages))

age_map_dt_agd <- data.table(age_group = ten_yr_ages_g, age = ten_yr_ages, age_group_id = as.character(ten_yr_age_g_id))

age_map_dt <- rbind(age_map_dt, age_map_dt_agd)

get_knots <- function(model, cov_model_name){
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}


#########################################################
## Read in parameters for MR-BRT modeling
#########################################################
params <- data.table(openxlsx::read.xlsx('FILEPATH/mrbrt_parameters_csmr.xlsx', sheet = 'settings'))

args <- commandArgs(trailingOnly = TRUE)
model_id <- args[1]
covs <- params[model_run_id == model_id,]

message(paste0('MODEL ID:', model_id, ' RUNNING'))

use_spline <- as.logical(covs$use_spline)
if(use_spline){spline_knots <- as.numeric(strsplit(as.character(covs$spline_knots),split=",",fixed=TRUE)[[1]])}
if(use_spline){spline_l_linear <- as.logical(covs$spline_l_linear)}
if(use_spline){spline_r_linear <- as.logical(covs$spline_r_linear)}
logit_mod <- as.logical(covs$logit_mod)
calc_unc  <- as.logical(covs$calc_unc)
write_draws     <- as.logical(covs$write_draws)
use_re    <- as.logical(covs$use_re)
agg_ten   <- as.logical(covs$agg_ten)
spline_degree <- as.integer(covs$spline_degree)

#########################################################
## Step 1
#########################################################
offset_val <- .0000001 ## offsets for zeros. 

## read in proportion data from Italy and New Zealand

itl_and_nz_prop <- data.table(openxlsx::read.xlsx(paste0('FILEPATH/mean_data.xlsx')))

## Additional papers reporting on the linkage
all_prop  <- data.table(openxlsx::read.xlsx(paste0('FILEPATH')))

## clean up some things: 
##itl
itl_prop <- itl_and_nz_prop[location_id==35501]
itl_prop[,freq := mean]
itl_prop[freq==0, freq := offset_val]    ## offset zeros 
itl_prop[freq==1, freq := 1-offset_val]  ## offset ones 
itl_prop[,`:=` (prob = NULL, probability = NULL, sdi = NULL, cnt = (freq*(1-freq))/standard_error^2, age_group_id=NULL, mean = NULL, 
                death_label = "IHD Death within 30 days of AMI hospitalization")]
itl_prop[,sex_label := sex]
itl_prop[, sex := ifelse(sex=='F',1,0)]

##nzl
nz_prop <- itl_and_nz_prop[location_id==72]
nz_prop[,freq := mean]
nz_prop[freq==0, freq := offset_val]    ## offset zeros
nz_prop[freq==1, freq := 1-offset_val]  ## offset ones 
nz_prop[,`:=` (prob = NULL, probability = NULL, sdi = NULL, cnt = (freq*(1-freq))/standard_error^2, age_group_id = NULL, mean = NULL,
               death_label = "IHD Death within 30 days of AMI hospitalization")]
nz_prop[,sex_label := sex]
nz_prop[, sex := ifelse(sex=='F',1,0)]

## Format to match sources in ITL and NZL
all_max_se <- max(all_prop$standard_error, na.rm=T)
all_prop[standard_error==0, standard_error := all_max_se]
all_prop[is.na(standard_error), standard_error := all_max_se]
all_prop[is.na(freq), freq:=offset_val]
all_prop[freq==0, freq := offset_val]    ## offset zeros 
all_prop[freq >=1, freq := 1-offset_val]  ## offset ones 
all_prop[,sex_label := ifelse(sex_label=='male','M','F')]
all_prop[,sex := ifelse(sex_label=='M',0,1)]
all_prop[,`:=` (study=NULL, study_.number=NULL, age_start=NULL,age_end=NULL,year_start=NULL,year_end=NULL,metric=NULL,ihd_death=NULL,cvd_death=NULL)]
all_prop[,`:=` (death_label='IHD Death within 30 days of AMI hospitalization')]


## bind prop data
prop_data <- data.table(rbind(itl_prop, nz_prop,all_prop[,-c('source','notes')]))
prop_data <- prop_data[death_label=="IHD Death within 30 days of AMI hospitalization"]
prop_data[,year:= as.numeric(year)]

##aggregate to 10-year bins if wanted
if(agg_ten){
  
  already_agged_groups <- c('35 to 44','45 to 54','55 to 64','65 to 74','75 to 84','85 plus','85 to 99')
  prop_data_no_agg <- prop_data[age_group %in% already_agged_groups]
  
  prop_data_agg <- prop_data[age_group %ni% already_agged_groups]
  
  ## define agg
  prop_data_agg[age_group %in% c('20 to 24','25 to 29'), age_group_agg := '20 to 29']
  prop_data_agg[age_group %in% c('30 to 34','35 to 39'), age_group_agg := '30 to 39']
  prop_data_agg[age_group %in% c('40 to 44','45 to 49'), age_group_agg := '40 to 49']
  prop_data_agg[age_group %in% c('50 to 54','55 to 59'), age_group_agg := '50 to 59']
  prop_data_agg[age_group %in% c('60 to 64','65 to 69'), age_group_agg := '60 to 69']
  prop_data_agg[age_group %in% c('70 to 74','75 to 79'), age_group_agg := '70 to 79']
  prop_data_agg[age_group %in% c('80 to 84','85 to 89'), age_group_agg := '80 to 89']
  prop_data_agg[age_group %in% c('90 to 94','95 plus'), age_group_agg := '90 to 99']
  
  ## calculate cases
  prop_data_agg[,cases := freq*cnt]
  
  ## aggregate cases and cnt
  prop_data_agg <- prop_data_agg[,.(cases_agg = sum(cases),cnt_agg =sum(cnt)), by= list(year,location_id,age_group_agg,sex,location_name,sex_label)]
  
  ## assign ages
  prop_data_agg[age_group_agg=='20 to 29', age := 24.5]
  prop_data_agg[age_group_agg=='30 to 39', age := 34.5]
  prop_data_agg[age_group_agg=='40 to 49', age := 44.5]
  prop_data_agg[age_group_agg=='50 to 59', age := 54.5]
  prop_data_agg[age_group_agg=='60 to 69', age := 64.5]
  prop_data_agg[age_group_agg=='70 to 79', age := 74.5]
  prop_data_agg[age_group_agg=='80 to 89', age := 84.5]
  prop_data_agg[age_group_agg=='90 to 99', age := 94.5]
  
  prop_data_agg[,freq := cases_agg/cnt_agg]
  prop_data_agg[,standard_error := sqrt((freq*(1-freq))/cnt_agg)]
  prop_data_agg[,`:=` (cnt = cnt_agg)]
  prop_data_agg[,`:=` (cases_agg = NULL, cnt_agg = NULL, death_label = "IHD Death within 30 days of AMI hospitalization ")]
  setnames(prop_data_agg, c('age_group_agg'),c('age_group'))
  
  prop_data <- rbind(prop_data_agg, prop_data_no_agg)
}

prop_data[,year_scaled := (year - mean(year))/sd(year)]

if(logit_mod){
  prop_data[, logit_mean := cw$utils$linear_to_logit(mean = array(prop_data$freq), sd = array(prop_data$standard_error))[[1]]]
  prop_data[, logit_se := cw$utils$linear_to_logit(mean = array(prop_data$freq), sd = array(prop_data$standard_error))[[2]]]
  
}

## load SDI
sdi_df <- get_covariate_estimates(covariate_id = 881,
                                  location_id = loc_meta$location_id,
                                  year_id = seq(1980,2022,1),
                                  release_id=16) 
sdi_df[,sex:= ifelse(sex_id==1,0,1)]
setnames(sdi_df,c('year_id','mean_value'),c('year','sdi'))

## load HAQi
haqi_df <- get_covariate_estimates(covariate_id = 1099,
                                   location_id = loc_meta$location_id, 
                                   year_id = seq(1980,2022,1),
                                   release_id=16)
haqi_df[,sex:= ifelse(sex_id==1,0,1)]
haqi_df[,mean_value := mean_value/100] #scale to 0-1
setnames(haqi_df,c('year_id','mean_value'),c('year','haqi'))

## merge covariates
prop_data <- data.table(merge(prop_data, sdi_df[,c('year','location_id','sdi')], by=c('year','location_id')))
prop_data <- data.table(merge(prop_data, haqi_df[,c('year','location_id','haqi')], by=c('year','location_id')))

## shift sdi to minimum in linear space
min_sdi <- min(sdi_df[year >= min(prop_data$year),sdi])
prop_data[,sdi_shift := sdi - min_sdi]

prop_data[,age_sdi_shift_inter := age*sdi_shift]
prop_data[,sex_sdi_shift_inter := sex*sdi_shift]
prop_data[,age_sex_sdi_shift_inter := age*sex*sdi_shift]
prop_data[,age_sex_inter := age*sex]
prop_data[,age_sdi_inter := age*sdi]
prop_data[,sex_sdi_inter := sex*sdi]
prop_data[,age_sex_sdi_inter := age*sex*sdi]
prop_data[,intercept := 1]

## shift haqi to minimum in linear space
min_haqi <- min(haqi_df[year >= min(prop_data$year),haqi])
prop_data[,haqi_shift := haqi - min_haqi]

prop_data[,age_haqi_shift_inter := age*haqi_shift]
prop_data[,sex_haqi_shift_inter := sex*haqi_shift]
prop_data[,age_sex_haqi_shift_inter := age*sex*haqi_shift]
prop_data[,age_sex_inter := age*sex]
prop_data[,age_haqi_inter := age*haqi]
prop_data[,sex_haqi_inter := sex*haqi]
prop_data[,age_sex_haqi_inter := age*sex*haqi]
prop_data[,intercept := 1]

## create year indicators
prop_data[,`:=` (year_1990_2000 = ifelse(dplyr::between(year,1990,2000),1,0),
                 year_2001_2005 = ifelse(dplyr::between(year,2001,2005),1,0),
                 year_2006_2010 = ifelse(dplyr::between(year,2006,2010),1,0),
                 year_2011_2015 = ifelse(dplyr::between(year,2011,2015),1,0),
                 year_2016_2022 = ifelse(dplyr::between(year,2016,2022),1,0),
                 year_2001_2010 = ifelse(dplyr::between(year,2001,2010),1,0),
                 year_2011_2022 = ifelse(dplyr::between(year,2011,2022),1,0),
                 year_2000_2004 = ifelse(dplyr::between(year,2000,2004),1,0),
                 year_2005_2009 = ifelse(dplyr::between(year,2005,2009),1,0),
                 year_2010_2014 = ifelse(dplyr::between(year,2010,2014),1,0),
                 year_2015_2022 = ifelse(dplyr::between(year,2015,2023),1,0))]

## Keep a copy of the backup data to plot later
backup_prop_data <- copy(prop_data)

## Remove offset data
prop_data <- prop_data[-which(freq==offset_val |freq == 1-offset_val),]
## Choose locations to include

#########################################################
## Step 3
#########################################################
data <- mr$MRData()
table(prop_data$location_name)

if(logit_mod){
  message('LOGIT MODELLING')
  data$load_df(
    data = prop_data,  col_obs = "logit_mean", col_obs_se = "logit_se", col_study_id = 'location_id',
    col_covs = list("sdi","age","sex", "haqi",
                    "age_sdi_inter", "sex_sdi_inter","age_sex_sdi_inter", "age_sex_inter",
                    "sdi_shift","age_sdi_shift_inter","sex_sdi_shift_inter","age_sex_sdi_shift_inter",
                    "age_haqi_inter", "sex_haqi_inter","age_sex_haqi_inter", "age_sex_inter",
                    "haqi_shift","age_haqi_shift_inter","sex_haqi_shift_inter","age_sex_haqi_shift_inter",
                    "intercept","year",'year_1990_2000','year_2001_2005','year_2006_2010',
                    'year_2011_2015','year_2016_2022','year_2001_2010','year_2011_2022',
                    'year_2000_2004','year_2005_2009','year_2010_2014','year_2015_2022','year_scaled'))
}else{
  message('NORMAL-SPACE MODELLING')
  data$load_df(
    data = prop_data,  col_obs = "freq", col_obs_se = "standard_error", col_study_id = 'location_id',
    col_covs = list("sdi","age","sex", "age_sex_inter","haqi",
                    "age_sdi_inter", "sex_sdi_inter","age_sex_sdi_inter",
                    "sdi_shift","age_sdi_shift_inter","sex_sdi_shift_inter","age_sex_sdi_shift_inter",
                    "age_haqi_inter", "sex_haqi_inter","age_sex_haqi_inter",
                    "haqi_shift","age_haqi_shift_inter","sex_haqi_shift_inter","age_sex_haqi_shift_inter",
                    "intercept","year",'year_1990_2000','year_2001_2005','year_2006_2010',
                    'year_2011_2015','year_2016_2022','year_2001_2010','year_2011_2022',
                    'year_2000_2004','year_2005_2009','year_2010_2014','year_2015_2022','year_scaled'))
}

## initalize model
message('Running MR-BRT...')
trim_pct <- covs$trim_pct 

## add covariates

mrbrt_nonspline_covs <- unlist(str_split(covs$non_spline_cov,","))
mrbrt_spline_covs <- ifelse(use_spline,covs$spline_cov,NA)
prior_covs <- unlist(str_split(covs$prior_covs,','))
## populate the covariate list for MR-BRT: 

i=1 
cov_list <- list()
for(cov in mrbrt_nonspline_covs){
  ## special clause for intercept, where random effects implemented
  if(cov == 'intercept'){
    if(use_re){
      cov_list[[i]] = mr$LinearCovModel(cov, use_re = use_re)
      i=i+1
    }else{
      cov_list[[i]] = mr$LinearCovModel(cov)
      i=i+1
    }
  }else{
    if(cov %in% prior_covs){
      cov_list[[i]] = mr$LinearCovModel(cov,prior_beta_uniform=array(c(covs$prior_covs_lb,covs$prior_covs_up)))
      i=i+1
    }else{
      cov_list[[i]] = mr$LinearCovModel(cov)
      i=i+1
    }
  }
}

## if there are spline covariates: 
if(use_spline){
  for(cov in mrbrt_spline_covs){
    cov_list[[i]] = mr$LinearCovModel(cov,
                                      use_spline = use_spline,
                                      spline_knots_type = 'frequency',
                                      spline_degree = spline_degree,#3L,
                                      spline_knots = spline_knots,
                                      spline_l_linear = spline_l_linear, 
                                      spline_r_linear = spline_r_linear)
  }}

model <- mr$MRBRT(data = data,
                  cov_models = cov_list,
                  inlier_pct = 1.00 - trim_pct)

## Fit the model to the data
model$fit_model()


## Produce uncertainty 
n_samples <- 1000L
if(calc_unc){
  samples <- model$sample_soln(sample_size = n_samples)
}

## get knots 

knots <- get_knots(model, mrbrt_spline_covs)
knot_message <- paste0(round(knots,2),collapse=', ')

#########################################################
## Predict proportion for all age/sex/year/location
#########################################################
pred_ages <- c(17.5, 22.5, 27.5, 32.5, 37.5, 42.5, 47.5, 52.5, 57.5, 62.5, 67.5, 72.5, 77.5, 82.5, 87.5, 92.5, 97.5)
dummy_pred_df <- data.table(expand.grid(year=c(1990,1995,2000,2005,2010,2015,2020,2022), location_id = loc_meta[most_detailed==1,location_id] ,
                                        age=unique(c(pred_ages,unique(prop_data$age))), sex=c(0,1), intercept=1))

dummy_pred_df <- data.table(merge(dummy_pred_df,sdi_df[,c('year','location_id','sdi')], by=c('year','location_id')))
dummy_pred_df <- data.table(merge(dummy_pred_df,haqi_df[,c('year','location_id','haqi')], by=c('year','location_id')))

## year 
dummy_pred_df[,year_scaled := (year - mean(year))/sd(year)]

## SDI shift
dummy_pred_df[,sdi_shift := sdi - min(sdi)]
dummy_pred_df[,age_sdi_shift_inter := age*sdi_shift]
dummy_pred_df[,sex_sdi_shift_inter := sex*sdi_shift]
dummy_pred_df[,age_sex_sdi_shift_inter := age*sex*sdi_shift]
dummy_pred_df[,age_sex_inter := age*sex]

dummy_pred_df[,age_sdi_inter := age*sdi]
dummy_pred_df[,sex_sdi_inter := sex*sdi]
dummy_pred_df[,age_sex_sdi_inter := age*sex*sdi]

## HAQi shift
dummy_pred_df[,haqi_shift := haqi - min(haqi)]
dummy_pred_df[,age_haqi_shift_inter := age*haqi_shift]
dummy_pred_df[,sex_haqi_shift_inter := sex*haqi_shift]
dummy_pred_df[,age_sex_haqi_shift_inter := age*sex*haqi_shift]
dummy_pred_df[,age_sex_inter := age*sex]

dummy_pred_df[,age_haqi_inter := age*haqi]
dummy_pred_df[,sex_haqi_inter := sex*haqi]
dummy_pred_df[,age_sex_haqi_inter := age*sex*haqi]

## create year indicators
dummy_pred_df[,`:=` (year_1990_2000 = ifelse(dplyr::between(year,1990,2000),1,0),
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

## Predict
dat_pred <- mr$MRData()

dat_pred$load_df(
  data = dummy_pred_df, 
  col_covs=list("age","sex","sdi","haqi","age_sex_inter",
                "sdi_shift","age_sdi_inter","sex_sdi_inter","age_sex_sdi_inter",
                "age_sdi_shift_inter","sex_sdi_shift_inter","age_sex_sdi_shift_inter",
                "haqi_shift","age_haqi_inter","sex_haqi_inter","age_sex_haqi_inter",
                "age_haqi_shift_inter","sex_haqi_shift_inter","age_sex_haqi_shift_inter",
                "intercept","year",'year_1990_2000','year_2001_2005','year_2006_2010',
                'year_2011_2015','year_2016_2022','year_2001_2010','year_2011_2022',
                'year_2000_2004','year_2005_2009','year_2010_2014','year_2015_2022','year_scaled')
)

if(calc_unc){
  draws <- model$create_draws(
    data = dat_pred,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = TRUE )
}

if(write_draws){
  save_ages <- c(8:20, 30, 31, 32, 235)
  draws_save <- cbind(dummy_pred_df[,c('age','sex','year','location_id')],inv.logit(draws))
  draws_save <- merge(draws_save, age_map_dt[,c('age','age_group_id')], by='age')
  draws_save <- draws_save[age_group_id %in% save_ages,]
  draws_save[,sex_id := ifelse(sex==0, 1, 2 )]
  draws_save[,year_id := year]
  draws_save[,`:=` (sex = NULL, year = NULL, age = NULL)]
  names(draws_save)[grep('V',names(draws_save))] <- paste0('draw_',as.numeric(str_replace(names(draws_save)[grep('V',names(draws_save))],'V' , ""))-1)
  save_cols <- c('location_id','year_id','sex_id','age_group_id',paste0('draw_',0:999))
  saveRDS(draws_save[,save_cols,with=F],
          file = paste0('FILEPATH/proportion_split_draws_model_',model_id,'.RDS'))
  
}
prediction_df <- model$predict(dat_pred) ## add random effect
pred_df <- data.table(cbind(dummy_pred_df,prediction_df))

if(calc_unc){
  pred_df$pred_lo <- apply(draws, 1, function(x) quantile(x, 0.025))
  pred_df$pred_hi <- apply(draws, 1, function(x) quantile(x, 0.975))
}
setnames(pred_df, 'prediction_df','logit_pred')

## Convert to normal space
if(logit_mod){
  message('LOGIT TRANSFORM BACK TO NORMAL')
  pred_df[,pred := inv.logit(logit_pred)]
  if(calc_unc){
    pred_df[,pred_lower := inv.logit(pred_lo)]
    pred_df[,pred_upper := inv.logit(pred_hi)]
  }
}else{
  message('PREDICITON ALREADY IN NORMAL')
  pred_df[,pred := logit_pred]
  if(calc_unc){
    pred_df[,pred_lower := pred_lo]
    pred_df[,pred_upper := pred_hi]
  }
}


#########################################################
## save results
#########################################################
write <- ifelse(write_draws,T,F)
if(write){
  
  message('WRITING RESULTS TO FILEPATH')
  pred_df2 <- copy(pred_df)
  pred_df2[,sex_id := ifelse(sex==0, 1,2)]
  pred_df2 <- data.table(merge(pred_df2, age_map_dt,by='age'))
  setnames(pred_df2, c('year'),c('year_id'))
  
  
  setnames(pred_df2, c('year_id','pred','pred_lower','pred_upper'),
           c('year_id','proportion','lower','upper'))
  
  min_prop <- min(pred_df2[proportion > 0, proportion])
  if(nrow(pred_df2[proportion < 0])){
    message('WARNING: PROPORTION LESS THAN ZERO')
    pred_df2[proportion < 0 , proportion := min_prop]
  }
  
  if(nrow(pred_df2[proportion > 1])){
    message('WARNING: PROPORTION GREATER THAN ONE')
    pred_df2[proportion > 1, proportion := 1]
  }
  
  save_names <- c('location_id','year_id','sex_id','sex','age_group','age_group_id','proportion','lower','upper')
  write.xlsx(pred_df2[,save_names,with=F],file = paste0('FILEPATH/modeled_prop_',
                                                        date,'_modelid_',model_id,'.xlsx'))
}else{
  message('RESULTS NOT SAVED')
}

