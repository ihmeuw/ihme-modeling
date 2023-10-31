############################
## Afib EMR Predictions
## Obj: Purpose of this code is to predict out EMR for all locations minus a subset of high quality observations which get their
##      calculated EMR = CSMR/PREV values from CODEm and DisMod-MR. 
##      For GBD 2020, we used MR-BRT and added an additional covariate (HAQI)
#############################

rm(list=ls())

## source all central functions & libraries
central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))

## MR-BRT Source code
repo_dir <- "FILEPATH" 
source(paste0(repo_dir, "mr_brt_functions.R"))

## Custom functions and libraries. 
'%!in%' <- Negate('%in%')
library(matrixStats)
library(plyr)
library(ggplot2)
library(gridExtra)
library(msm)

## Location & age ids for building MR-BRT model
loc_ids <- c(71,76,101,94,81,78,92,95,82,84,83,85,86,67,89,90,72,91,93,102,68) #location_ids to model MR-BRT from based on conditions
ages    <- c(11,12,13,14,15,16,17,18,19,20,30,31,32,235)                       #age ids to pull csmr and prev from for use in MR-BRT

## Pull in location meta-data for modeling
loc_meta_data <- get_location_metadata(location_set_id=9)
loc_meta_data <- loc_meta_data[which(loc_meta_data$is_estimate==1),]
loc_meta_data <- loc_meta_data[ ,c('location_id','location_name','level','parent_id','most_detailed')]

## Pull in metadata for modeling & prediction
all_locs <- get_location_metadata(location_set_id=35)
pop <- get_population(age_group_id = ages, location_id = loc_ids,sex_id=c(1,2), gbd_round_id=7,year_id=2019, decomp_step='iterative')
age_meta <- get_age_metadata(age_group_set_id=19)

all_loc_ids <- unique(loc_meta_data[,location_id]) #all location ids.
location_names <- loc_meta_data[location_id %in% loc_ids, location_name]

print('Building MR-BRT EMR model on following locations: ')
print(location_names)

## get draws for csmr and prevalence for selected countries for latest year 

year <- 2019

print(paste0('Getting prevalence draws...'))

afib_epi_draws <- get_draws(gbd_id_type='modelable_entity_id', gbd_id=1859, measure_id=5, location_id=loc_ids, status='best', sex_id=c(1,2), year_id=year,decomp_step='iterative',source='epi', age_group_id = ages, gbd_round_id=7)  
colnames(afib_epi_draws)<- gsub('draw','prev', colnames(afib_epi_draws)) #replace "draws" with "prev"

## Pull CSMR draws
print('pulling CSMR from uncodcorrected CODEm...')
afib_cod_draws_csmr_m <- get_draws(gbd_id_type='cause_id', gbd_id=500, location_id=loc_ids, year_id = '2017',source='codem',version_id= '407906', gbd_round_id = 5) 
afib_cod_draws_csmr_f <- get_draws(gbd_id_type='cause_id', gbd_id=500, location_id=loc_ids, year_id = '2017',source='codem',version_id= '407909', gbd_round_id = 5) 
afib_cod_draws_csmr_b <- data.table(rbind(afib_cod_draws_csmr_m,afib_cod_draws_csmr_f))

## Calculate CSMR
print('Calculating CSMR')
for(i in 3:1002){
  afib_cod_draws_csmr_b[,i] <- afib_cod_draws_csmr_b[,i]/afib_cod_draws_csmr_b$pop
}

## Sub csmr in for draws.
colnames(afib_cod_draws_csmr_b)<- gsub('draw','csmr',colnames(afib_cod_draws_csmr_b))

## MERGE
afib_all <- data.table(merge(afib_cod_draws_csmr_b,afib_epi_draws, by=c('location_id','age_group_id','sex_id'))) 

## Clean up some columns
afib_all[,location_id:=as.character(location_id)]
afib_all[,age_group_id2:=as.character(age_group_id)]
afib_all[,age_group_id:=NULL]
afib_all[,age_group_id:= age_group_id2]
afib_all[,age_group_id2:=NULL]
afib_all[,sex_id:= as.character(sex_id)]
afib_all[,year_id:=year]

## Drop unnecessary columns
afib_all <- data.frame(afib_all)
afib_all_keep <- afib_all[,c("location_id","year_id","age_group_id","sex_id","pop",colnames(afib_all)[grep('csmr', colnames(afib_all))],colnames(afib_all)[grep('prev', colnames(afib_all))])]

## Calculate_emr
print('Calculating EMR...')
for(i in 0:999){
  EMR <- afib_all_keep[,paste0('csmr_',i)]/afib_all_keep[,paste0('prev_',i)]
  afib_all_keep[,paste0('EMR',i)]<-EMR
}


## Calculate means and upper/lower 95% interval
afib_all_keep$mean_EMR <- rowMeans(afib_all_keep[,grep('EMR',colnames(afib_all_keep))])
afib_subset <- afib_all_keep[,grep('EMR',colnames(afib_all_keep))]
afib_subset <- afib_subset[,-1001] #remove last column

afib_summary <- rowQuantiles(as.matrix(afib_subset), probs=c(.025,.975))

afib_all_keep$lower <- afib_summary[,1]
afib_all_keep$upper <- afib_summary[,2]

## Calculate log_emr
print('Calculating Log EMR for MR-BRT...')
for(i in 0:999){
  Log_EMR <- log(afib_all_keep[,paste0('csmr_',i)]/afib_all_keep[,paste0('prev_',i)])
  afib_all_keep[,paste0('Log_EMR',i)]<-Log_EMR
}

## Drop unneeded cols.
afib_all_keep <- afib_all_keep[,-c(grep('csmr',colnames(afib_all_keep)),grep('prev',colnames(afib_all_keep)))]
afib_all_keep$reg_loc <-1
afib_all_keep$emr_parent <- afib_all_keep$location_id

## Calculate mean
afib_all_keep$mean_Log_EMR <- rowMeans(afib_all_keep[,grep('Log_EMR',colnames(afib_all_keep))])
afib_all_keep <- data.table(afib_all_keep)

## Preserve - building models based on this data only
afib_preserve <- afib_all_keep[location_id %in% loc_ids, c('location_id','year_id','sex_id','age_group_id','mean_EMR','upper','lower')]
afib_preserve$reg_loc <- 1
afib_preserve$emr_parent <- afib_preserve$location_id

afib_emr <- data.table(afib_preserve) #will build mr-brt model on these EMRS - Add this exact data onto the end after words

## Add column for HAQi subsetted to location_id - use midyear for prevalence data to subset to haqi year_id

haq <- as.data.table(get_covariate_estimates(
   covariate_id=1099,
   gbd_round_id=7,
   decomp_step='step2'
 ))
 
haq2 <- haq[, c("location_id", "year_id", "mean_value", "lower_value", "upper_value")]
setnames(haq2, "mean_value", "haqi_mean")
setnames(haq2, "lower_value", "haqi_lower")
setnames(haq2, "upper_value", "haqi_upper")
haq2$location_id <- as.character(haq2$location_id)
haq2$year_id <- as.character(haq2$year_id)

afib_emr[,index:=.I]
n <- nrow(afib_emr)
afib_emr <- merge(afib_emr,haq2, by=c("location_id","year_id"))
afib_emr[, c("index", "haqi_lower", "haqi_upper", "year_id")] <- NULL

if(nrow(afib_emr)!=n){
   print("Warning! Issues in merge of HAQI")
}

## Convert the ratio (using EMR, which is CSMR/prevalence) to log space
afib_emr$log_ratio <- log(afib_emr$mean_EMR)

## Log standard error
afib_emr$standard_error<-(afib_emr$upper-afib_emr$lower)/(1.96*2)
afib_emr$delta_log_se <- sapply(1:nrow(afib_emr), function(i) {
  ratio_i <- afib_emr[i, "mean_EMR"]
  ratio_se_i <- afib_emr[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

## Get midage
age_id_to_midage_frame <- data.frame(ages,c(32,37,42,47,52,57,62,67,72,77,82,87,92,97.5),c(30,40,40,50,50,60,60,70,70,80,80,90,90,100))
colnames(age_id_to_midage_frame)<- c('age_group_id','midage','age_brt')
age_meta[,age_group_id:=as.character(age_group_id)]
age_meta[age_group_id=='235', age_group_years_end:=99]

## Merge on age metadata
afib_emr <- data.table(merge(afib_emr, age_meta,by=c('age_group_id')))
afib_emr[,midage:=(age_group_years_end+age_group_years_start)/2]

## Get sex_binary formatted as 0 & 1 for use in MR-BRT
afib_emr$sex_binary= as.character(as.numeric(afib_emr$sex_id)-1)
afib_emr <- data.table(afib_emr)

## Run MR-BRT
date <- gsub("-", "_", Sys.Date())
mrbrt_path <- "FILEPATH"
output_path <- "FILEPATH"
write.csv(afib_emr, paste0(mrbrt_path ,date, ".csv"), row.names = F)

## Link with launching a MR-BRT model ##
#########################################################################################################################################
print('Running MR-BRT...')

## Run MR-BRT -------------------------------------------------------------------------------------------------
cov_list <- "age_sex_haq"  #covariates included in MR-BRT run
age_predict <- c(unique(afib_emr[midage<=100,midage]))

trim <- .05 
knot_number <- 3 
spline_type <- 3 
spline_tail <- T 
knot_placement <- 'frequency'
bspline_gprior_mean= paste(rep("0",knot_number+1),collapse=",")
bspline_gprior_var = paste(rep("inf",knot_number+1),collapse=",")

fit1 <- run_mr_brt(
  output_dir = output_path,
  model_label = 'Afib_EMR_Fix',
  data = paste0(mrbrt_path ,date, ".csv"),
  mean_var = "log_ratio",
  se_var = "delta_log_se",
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE, 
  method = "trim_maxL",
  study_id = "location_id", 
  trim_pct = trim,
  covs = list(
    cov_info("midage", "X", degree = spline_type, n_i_knots = knot_number,
             bspline_gprior_mean = bspline_gprior_mean,
             bspline_gprior_var = bspline_gprior_var,
             r_linear = spline_tail, l_linear = spline_tail, knot_placement_procedure = knot_placement,
             bspline_mono = 'increasing'),
    cov_info("sex_binary", "X"),
    cov_info("haqi_mean", "X")
  )
)

pred_df <- expand.grid(year_id=c(year), location_id = all_locs[most_detailed==1,location_id], midage=age_predict, sex_binary=c(0,1))
pred_df <- join(pred_df, haq2[,c("haqi_mean","location_id","year_id")], by=c("location_id","year_id"))
pred_df$age_start <- pred_df$midage
pred_df$age_end <- pred_df$midage

## Predict mr brt
pred <- predict_mr_brt(fit1, newdata = pred_df)
check_for_preds(pred)
pred_object <- load_mr_brt_preds(pred)
predicted <- pred_object$model_summaries

## Clean up column names
setnames(predicted, "X_sex_binary", "sex_binary")
setnames(predicted, "X_midage", "midage")
setnames(predicted, "X_haqi_mean", "haqi_mean.x")
predicted[, c("X_intercept", "Z_intercept")] <- NULL

## Prepare predicted EMR for plotting
final_emr <- cbind(predicted, pred_df)
final_emr <- final_emr[, !duplicated(colnames(final_emr))]
final_emr[, c("Y_negp", "Y_mean_fe", "Y_mean_lo_fe", "Y_mean_hi_fe", "Y_negp_fe", "midage", "haqi_mean")] <- NULL

## Graph EMR data and model fit over age by sex and a few HAQi values

results <- fit1
predicts <- predicted
graph_mrbrt_results <- function(results, predicts){
  data_dt <- as.data.table(results$train_data)
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  model_dt <- as.data.table(predicts)
  quant_haq <- round(quantile(model_dt$haqi_mean, probs = seq(0,1, by = 0.25)), digits = 2)
  model_dt <- model_dt[, haqi_mean := round(haqi_mean.x, digits =2)]
  model_dt <- model_dt[model_dt$haqi_mean %in% quant_haq, ]
  model_dt$haqi_mean <- as.character(model_dt$haqi_mean)
  model_dt$sex <- ifelse(model_dt$sex_binary == 1, "Female", "Male")
  
  model_dt_min_haq <- model_dt[haqi_mean==quant_haq[1]]
  model_dt_min_haq[,line:= 'Min HAQI']
  model_dt_median_haq <- model_dt[haqi_mean==quant_haq[2]]
  model_dt_median_haq[, line:= 'Median HAQI']
  model_dt_max_haq <- model_dt[haqi_mean==quant_haq[3]]
  model_dt_max_haq[,line:='Max HAQI']
  
  coefs <- fit1$model_coefs[, c("x_cov", "beta_soln", "beta_var", "gamma_soln")]
  coefs <- coefs[, c("x_cov", "beta_soln", "variance_with_gamma")]
  coefs[,c("beta_soln","variance_with_gamma")] = format(round(coefs[,c("beta_soln", "variance_with_gamma")],3))

  data_dt[,Source := ifelse(excluded==0,' Not trimmed',' Trimmed')]
  data_dt[,sex:= ifelse(sex_binary==0,'Male','Female')]
  
  model_dt[,line:='GBD 2020 - Current']

  gg_subset<- ggplot() +
    geom_point(data = data_dt, aes(x = midage, y = exp(log_ratio), color = Source),alpha=.2)+
    labs(x = "Age", y = "EMR") +
    ggtitle(paste0("MR-BRT Model Results Overlay on EMR Input")) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black")) +
    geom_line(data = model_dt_min_haq, aes(x = midage, y = exp(Y_mean),colour=line), size=.5, alpha=2) +
    geom_line(data = model_dt_median_haq, aes(x = midage, y = exp(Y_mean),colour=line), size=.5,alpha=2) +
    geom_line(data = model_dt_max_haq, aes(x = midage, y = exp(Y_mean),colour=line), size=.5,alpha=2) +
    facet_wrap(~sex)
  return(gg_subset)
}


graph_emr <- graph_mrbrt_results(results = fit1, predicts = predicted)
graph_emr

ggsave(graph_emr, 
       filename = paste0(output_path,date, "exp_fit_graphs_no_haqi",trim,"_",knot_number,"_",paste(spline_tail),".pdf"), width = 12)

## Remove locations from all locations used in MR-BRT + their subnationals
## Build dataframe with raw EMR from locations: 

all_locs <- all_locs[most_detailed==1,]

final_keep_locs <- afib_preserve   ## Bring in earlier saved raw EMR data. 
final_keep_locs[,age_group_id:=as.character(age_group_id)]
final_keep_locs <- data.table(merge(final_keep_locs,age_meta[,c('age_group_id','age_group_years_start','age_group_years_end')]))
final_keep_locs[,location_id:=as.integer(location_id)]
final_keep_locs <- data.table(merge(final_keep_locs,all_locs[,c('location_id','parent_id')], by='location_id'))
final_keep_locs[,midage:=(age_group_years_start+age_group_years_end)/2]
final_keep_locs[,parent_id:=location_id] #strictly for purposes of merging. 

final_uk <- final_keep_locs[location_id==95]
final_not_uk <- final_keep_locs[location_id!=95]

has_subnat <- c(67,72,86,90,93,102)                              #have subnationals
no_subnat <-  c(76,78,81,82,83,84,85,89,92,94,101)               #do not have subnationals.
uk_has_subnat <- c(4520,4622,4626,4625,4619,4618,4624,4623,4621) #UK subnats

has_subnat_children <- locs[parent_id %in% has_subnat,c("location_id",'location_name','parent_id')]
has_subnat_children_uk <- locs[parent_id %in% uk_has_subnat,c("location_id",'location_name','parent_id')]

## Special for the UK - need to get lowest level in location heirarchy - different from other locations. 
final_uk_list <- list()
i=1
for(id in unique(has_subnat_children_uk$location_id)){
  temp <- copy(final_uk)
  temp[,location_id := id]
  final_uk_list[[i]] <- temp
  i = i+1
}
final_uk_df <- rbindlist(final_uk_list)

## For other locations beside uk with subnationals: 
## populate sub-nats (non-uk): 
final_non_uk_list_subnat <- list()
i=1
for(id in unique(has_subnat_children$location_id)){
  temp_parent_id <- all_locs[location_id==id,parent_id]
  temp <- copy(final_not_uk[location_id==temp_parent_id,])
  temp[,location_id := id]
  final_non_uk_list_subnat[[i]] <- temp
  i = i+1
}
final_non_uk_subnat_df <- rbindlist(final_non_uk_list_subnat)

## No subnats
final_non_uk_list_not_subnat <- list()
i=1
for(id in no_subnat){
  temp <- copy(final_not_uk[location_id==id,])
  temp[,location_id := id]
  final_non_uk_list_not_subnat[[i]] <- temp
  i = i+1
}

final_non_uk_not_subnat_df <- rbindlist(final_non_uk_list_not_subnat)
final_subnats <- data.table(rbind(final_non_uk_subnat_df,
                                  final_uk_df,
                                  final_non_uk_not_subnat_df))

## Clean-up the raw EMR data

final_subnats[,`:=` (age_group_id=NULL,emr_parent=NULL,age_group_years_start=NULL,age_group_years_end=NULL,parent_id=NULL)]
setnames(final_subnats,'mean_EMR','mean')
final_subnats[,`:=` (reg_loc=NULL, year_id=NULL)]
final_subnats[,sex := ifelse(sex_id==1,'Male','Female')]
final_subnats[,year_start:=1990]
final_subnats[,year_end:= 2019]
final_subnats[,age_group_id:=NA]
final_subnats[,uncertainty_type_value:=95]
final_subnats[,`:=`(cases=NA,sample_size=NA,standard_error=NA)]

## All remaining estimation locations

final_dt <-as.data.table(final_emr)
final_dt$Y_se <- (final_dt$Y_mean_hi-final_dt$Y_mean_lo)/(1.96*2)
final_dt[, `:=` (mean = exp(Y_mean), standard_error = deltamethod(~exp(x1), Y_mean, Y_se^2)), by = c("Y_mean", "Y_se")]

final_dt$sex_id <- as.character(final_dt$sex_binary+1)
final_dt[,midage:=age_end]
age_meta[,midage:=(age_group_years_start+age_group_years_end)/2]

final_dt$location_id <- as.character(final_dt$location_id)
final_dt$sex_id      <- as.character(final_dt$sex_id)
final_dt$age_group_id<- as.character(final_dt$age_group_id)

final_dt[,c("sex_binary", "year_id", "haqi_mean","haqi_mean.x", "log_ratio", "delta_log_se", "Y_mean", "Y_se", "Y_mean_lo", "Y_mean_hi")] <- NULL
final_dt$year_start <- 1990 
final_dt$year_end <- year
final_dt[,`:=`(age_start=NULL,age_end=NULL)]
final_dt[,`:=`(lower=NA,upper=NA)]
final_dt$sample_size <- 1000
final_dt$cases <- final_dt$sample_size * final_dt$mean
final_dt$sex <- ifelse(final_dt$sex_id == 1, "Male", "Female")
final_dt$uncertainty_type_value <- NA

## Merge on the locations with raw emr. 
## Remove locations used in regression from final_dt:
remove_locs <- unique(final_subnats$location_id)
final_dt <- final_dt[-which(location_id %in% remove_locs)]
final_dt <- rbind.fill(final_dt,final_subnats)
final_dt <- data.table(final_dt)

## Merge on GBD age groups
final_dt <- data.table(merge(final_dt,age_meta[,c('age_group_id','midage','age_group_years_start','age_group_years_end')],
                             by='midage'))
final_dt[,`:=` (age_start=NULL,age_end=NULL)]
setnames(final_dt,c('age_group_years_start','age_group_years_end'),c('age_start','age_end'))
final_dt[age_start==95, age_end:=99]
final_dt[age_end!=99,age_end:=age_end-1]               #adjust for using age_metadata, except for oldest age groups
final_dt[ , age_group_id := as.double(age_group_id.y)] #set age group
final_dt[,`:=` (age_group_id.x=NULL, age_group_id.y=NULL)] #remove merged age group ids. 

## For uploader validation - add in needed columns
final_dt$seq <- NA
final_dt <- join(final_dt, locs[,c("location_id", "location_name")], by="location_id")
final_dt$nid <- 416752
final_dt$year_issue <- 0
final_dt$age_issue <- 0
final_dt$sex_issue <- 0
final_dt$measure <- "mtexcess"
final_dt$source_type <- "Facility - inpatient"
final_dt$extractor <- 'USERNAME'
final_dt$is_outlier <- 0
final_dt$measure_adjustment <- 1
final_dt$uncertainty_type <- "Confidence interval"
final_dt$recall_type <- "Point"
final_dt$urbanicity_type <- "Mixed/both"
final_dt$unit_type <- "Person"
final_dt$representative_name <- "Nationally and subnationally representative"
final_dt$note_modeler <- "These data are modeled from dismod EMR using HAQI as a predictor"
final_dt$unit_value_as_published <- 1
final_dt$step2_location_year <- "This is modeled EMR that uses the NID for one row of EMR data added to the bundle data"
final_dt$underlying_nid <- NA
final_dt$sampling_type <- ""
final_dt$recall_type <- "Point"
final_dt$input_type <- "extracted"
final_dt$effective_sample_size <- NA
final_dt$design_effect <- NA
final_dt$recall_type_value <- ""

#final_dt$lower <- NA
#final_dt$upper <- NA

bundle_path <- "FILEPATH"
write.xlsx(final_dt, file = paste0(bundle_path,date,paste0("_uncorrected_codem_dismod_",mv_id,".xlsx")))