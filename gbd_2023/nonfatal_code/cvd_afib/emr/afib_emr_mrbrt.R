################################################################################################################
## Title: Atrial fibrillation EMR method with updated MRBRT
## Date: DATE
################################################################################################################

## Source central functions: 
central <- "/FILEPATH/"
for (func in paste0(central, list.files(central))) source(paste0(func))

## Source my helper functions
'%ni%' <- Negate('%in%')


## Source libraries
library(openxlsx)
library(data.table)
library(ggplot2)
library(plyr)
library(dplyr)
library(stringr)
library(tidyr)
library(matrixStats)
library(gridExtra)

## Get metadata for age and location
message('Getting metadata...')
age_meta <- get_age_metadata(age_group_set_id=19)
loc_meta <- get_location_metadata(location_set_id = 35, release_id = 16)

j <- "FILEPATH"
h <- "FILEPATH"
l <- "FILEPATH"
lib <- "FILEPATH"

date<-gsub("-", "_", Sys.Date())

##load MRBRT. 
library(mrbrt001, lib.loc = "/FILEPATH/")

## Location & age ids for building MR-BRT model, release id, stage-1 model version, latest year to pull prevalence from
###################

loc_ids <- c(71,76,135,101,94,81,78,92,95,82,84,83,85,86,67,89,90,72,91,93,102,68) #location_ids to model MR-BRT from based on conditions - obtain list from emr_locations_for_mrbrt.R
ages    <- c(11,12,13,14,15,16,17,18,19,20,30,31,32,235)                           #age ids to pull csmr and prev from for use in MR-BRT
release_id <- 16

mv_id <- 746208 
year <- 2020

## Pull in location meta-data for modeling
loc_meta_data <- get_location_metadata(location_set_id=9, release_id = release_id)
loc_meta_data <- loc_meta_data[ ,c('location_id','location_name','level','parent_id','most_detailed')]

## Pull in metadata for modeling & prediction
all_locs <- get_location_metadata(location_set_id=35, release_id = release_id)
pop <- get_population(age_group_id = ages, location_id = loc_ids,sex_id=c(1,2), release_id = release_id)
all_loc_ids <- unique(loc_meta_data[,location_id]) #all location ids.
location_names <- loc_meta_data[location_id %in% loc_ids, location_name]

print('Building MR-BRT EMR model on following locations: ')
print(location_names)

## get draws for csmr and prevalence for selected countries for latest year 

print(paste0('Getting prevalence draws from model version id: ',mv_id))
afib_epi_draws <- get_draws(gbd_id_type='modelable_entity_id', gbd_id=1859, measure_id=5, location_id=loc_ids, 
                            sex_id=c(1,2), year_id=year,source='epi', release_id = release_id,
                            age_group_id = ages, version_id=mv_id)  

colnames(afib_epi_draws)<- gsub('draw','prev', colnames(afib_epi_draws)) #replace "draws" with "prev"

## Pull CSMR draws
print('pulling CSMR from Uncorrect CODEm...')
afib_cod_draws_csmr_m <- get_draws(gbd_id_type='cause_id', gbd_id=500, location_id=loc_ids, year_id = '2017',source='codem',version_id= '407906', gbd_round_id = 5) #males - GBD 2020 step 2 '645407' - uncorrected: '407906'
afib_cod_draws_csmr_f <- get_draws(gbd_id_type='cause_id', gbd_id=500, location_id=loc_ids, year_id = '2017',source='codem',version_id= '407909', gbd_round_id = 5) #females - GBD 2020 step 2 '645404'- uncorrected: '407909'
afib_cod_draws_csmr_b <- data.table(rbind(afib_cod_draws_csmr_m,afib_cod_draws_csmr_f))

## Convert CSMR to rate space
print('Calculating CSMR')
cols <- names(afib_cod_draws_csmr_b)[grep('draw',names(afib_cod_draws_csmr_b))]
afib_cod_draws_csmr_b[, (cols) := lapply(.SD, function(d) d/get('pop')), .SDcols = cols]

## Sub csmr in for draws.
colnames(afib_cod_draws_csmr_b)<- gsub('draw','csmr',colnames(afib_cod_draws_csmr_b))

## MERGE
afib_all <- data.table(merge(afib_cod_draws_csmr_b,afib_epi_draws, by=c('location_id','age_group_id','sex_id'))) 

## Clean up columns
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
afib_preserve <- afib_all_keep[location_id %in% unique(afib_all_keep$location_id), c('location_id','year_id','sex_id','age_group_id','mean_EMR','upper','lower')]
afib_preserve$reg_loc <- 1
afib_preserve$emr_parent <- afib_preserve$location_id

afib_emr <- data.table(afib_preserve) #will build mr-brt model on these EMRS - Add this exact data onto the end after words

## Get HAQ and attach
haq <- as.data.table(get_covariate_estimates(
  covariate_id=1099,
  release_id = release_id
))

haq2 <- haq[, c("location_id", "year_id", "mean_value", "lower_value", "upper_value")]
setnames(haq2, "mean_value", "haqi_mean")
setnames(haq2, "lower_value", "haqi_lower")
setnames(haq2, "upper_value", "haqi_upper")
haq2$location_id <- as.character(haq2$location_id)
haq2$year_id <- as.character(haq2$year_id)

afib_emr[,index:=.I]
afib_emr[,year_id := NULL]
afib_emr[,year_id := as.character(year)]
afib_emr[,location_id := as.character(location_id)]
n <- nrow(afib_emr)
afib_emr <- merge(afib_emr,haq2, by=c("location_id","year_id"))
afib_emr[, c("index", "haqi_lower", "haqi_upper", "year_id")] <- NULL

if(nrow(afib_emr)!=n){
  print("Warning! Issues in merge of HAQI")
}


## Convert the ratio (using EMR, which is CSMR/prevalence) to log space
afib_emr$log_ratio <- log(afib_emr$mean_EMR)

## Log standard error
library(msm)
afib_emr$standard_error<-(afib_emr$upper-afib_emr$lower)/(1.96*2)
afib_emr$delta_log_se <- sapply(1:nrow(afib_emr), function(i) {
  ratio_i <- afib_emr[i, "mean_EMR"]
  ratio_se_i <- afib_emr[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

## Also produce logit ratio. 
afib_emr[, logit_mean := linear_to_logit(mean = array(mean_EMR), sd = array(standard_error))[[1]]]
afib_emr[, logit_se := linear_to_logit(mean = array(mean_EMR), sd = array(standard_error))[[2]]]

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

## visualize input data against HAQi 
afib_emr_temp <- copy(afib_emr)
afib_emr_temp[,location_id := as.integer(location_id)]
afib_emr_temp <- merge(afib_emr_temp, loc_meta[,c('location_id','ihme_loc_id')], by = 'location_id')

## Run MR-BRT
date <- gsub("-", "_", Sys.Date())
mrbrt_path <- 'FILEPATH/afib_emr_for_mr_brt_'
output_path <- 'FILEPATH'

## remove locations you do not want to use modeling: 
loc_summary <- by(afib_emr[,c('location_id','mean_EMR')],factor(afib_emr$location_id),summary)

make_plots <- function(){
  
  ## Create summary for later plots. 
  afib_epi_summary <- copy(afib_epi_draws)
  afib_epi_summary_draws <- afib_epi_summary[,colnames(afib_epi_summary)[grep('prev',colnames(afib_epi_summary))],with=F]
  
  afib_epi_summary_upper_lower <- rowQuantiles(as.matrix(afib_epi_summary_draws), probs=c(.025,.975))
  afib_epi_summary_mean <- rowMeans(as.matrix(afib_epi_summary_draws))
  afib_epi_summary$lower <- afib_epi_summary_upper_lower[,1]
  afib_epi_summary$upper <- afib_epi_summary_upper_lower[,2]
  afib_epi_summary$mean  <- afib_epi_summary_mean
  afib_epi_summary <- afib_epi_summary[,c('age_group_id','location_id','sex_id','year_id','mean','lower','upper')]
  
  ## Create summary for later plots. 
  afib_csmr_summary <- copy(afib_cod_draws_csmr_b)
  afib_csmr_summary_draws <- afib_csmr_summary[,colnames(afib_csmr_summary)[grep('csmr',colnames(afib_csmr_summary))],with=F]
  
  afib_csmr_summary_upper_lower <- rowQuantiles(as.matrix(afib_csmr_summary_draws), probs=c(.025,.975))
  afib_csmr_summary_mean <- rowMeans(as.matrix(afib_csmr_summary_draws))
  afib_csmr_summary$lower <- afib_csmr_summary_upper_lower[,1]
  afib_csmr_summary$upper <- afib_csmr_summary_upper_lower[,2]
  afib_csmr_summary$mean  <- afib_csmr_summary_mean
  afib_csmr_summary <- afib_csmr_summary[,c('age_group_id','location_id','sex_id','year_id','mean','lower','upper')]
  
  afib_emr_plot <- copy(afib_emr)
  afib_emr_plot[,location_id := as.integer(location_id)]
  afib_emr_plot <- merge(afib_emr_plot, loc_meta, by = 'location_id')
  
  afib_csmr_summary[,age_group_id := as.character(age_group_id)]
  afib_epi_summary[,age_group_id := as.character(age_group_id)]
  
  afib_csmr_summary <- merge(afib_csmr_summary, loc_meta, by='location_id')
  afib_csmr_summary <- merge(afib_csmr_summary, age_meta, by='age_group_id')
  
  afib_epi_summary  <- merge(afib_epi_summary, loc_meta, by ='location_id')
  afib_epi_summary  <- merge(afib_epi_summary, age_meta, by ='age_group_id')
  
  pdf(file = paste0('FILEPATH/input_prev_csmr_emr_vetting_prev_year_',year,'_',date,'.pdf'), width = 16, height = 8)
  ## EMR males
  p1 <- ggplot(afib_emr_plot[sex_id==1,],aes(x=location_name, y=mean_EMR, fill=location_name))+
    geom_bar(stat = 'identity')+facet_wrap(~midage, scales = 'free_y')+xlab('Location')+ylab('EMR')+
    ggtitle(paste0('Males - Input EMR faceted by age - location view, prevalence from year ',year,' CSMR from 2017'))#+
  #theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))
  
  cols <- data.table(unique(ggplot_build(p1)$data[1][[1]][,c('fill','group')]))
  
  p1 <- p1+theme_bw()+theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1, color = cols[order(group),fill]))+
    labs(fill='Location Name')
  print(p1)
  
  ## EMR females
  p2 <- ggplot(afib_emr_plot[sex_id==2,],aes(x=location_name, y=mean_EMR, fill=location_name))+
    geom_bar(stat = 'identity')+facet_wrap(~midage, scales = 'free_y')+xlab('Location')+ylab('EMR')+
    ggtitle(paste0('Females - Input EMR faceted by age - location view, prevalence from year ',year,' CSMR from 2017'))#+
  #theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))
  
  cols <- data.table(unique(ggplot_build(p2)$data[1][[1]][,c('fill','group')]))
  
  p2 <- p2+theme_bw()+theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1, color = cols[order(group),fill]))+
    labs(fill='Location Name')
  print(p2)
  
  ## PREV males
  p1 <- ggplot(afib_epi_summary[sex_id==1,],aes(x=location_name, y=mean, fill=location_name))+
    geom_bar(stat = 'identity')+facet_wrap(~age_group_name, scales = 'free_y')+xlab('Location')+ylab('PREV')+
    ggtitle(paste0('Males - Input prevalence faceted by age - location view, prevalence from year ',year))#+
  #theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))
  
  cols <- data.table(unique(ggplot_build(p1)$data[1][[1]][,c('fill','group')]))
  
  p1 <- p1+theme_bw()+theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1, color = cols[order(group),fill]))+
    labs(fill='Location Name')
  print(p1)
  
  ## PREV females
  p2 <- ggplot(afib_epi_summary[sex_id==2,],aes(x=location_name, y=mean, fill=location_name))+
    geom_bar(stat = 'identity')+facet_wrap(~age_group_name, scales = 'free_y')+xlab('Location')+ylab('PREV')+
    ggtitle(paste0('Females - Input prevalence faceted by age - location view, prevalence from year ',year))#+
  #theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))
  
  cols <- data.table(unique(ggplot_build(p2)$data[1][[1]][,c('fill','group')]))
  
  p2 <- p2+theme_bw()+theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1, color = cols[order(group),fill]))+
    labs(fill='Location Name')
  print(p2)
  
  ## CSMR males
  p1 <- ggplot(afib_csmr_summary[sex_id==1,],aes(x=location_name, y=mean, fill=location_name))+
    geom_bar(stat = 'identity')+facet_wrap(~age_group_name, scales = 'free_y')+xlab('Location')+ylab('CSMR')+
    ggtitle(paste0('Males - Input CSMR faceted by age - location view, CSMR from 2017'))#+
  #theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))
  
  cols <- data.table(unique(ggplot_build(p1)$data[1][[1]][,c('fill','group')]))
  
  p1 <- p1+theme_bw()+theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1, color = cols[order(group),fill]))+
    labs(fill='Location Name')
  print(p1)
  
  ## CSMR females
  p2 <- ggplot(afib_csmr_summary[sex_id==2,],aes(x=location_name, y=mean, fill=location_name))+
    geom_bar(stat = 'identity')+facet_wrap(~age_group_name, scales = 'free_y')+xlab('Location')+ylab('PREV')+
    ggtitle(paste0('Females - Input CSMR faceted by age - location view, CSMR from 2017'))#+
  #theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1))
  
  cols <- data.table(unique(ggplot_build(p2)$data[1][[1]][,c('fill','group')]))
  
  p2 <- p2+theme_bw()+theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1, color = cols[order(group),fill]))+
    labs(fill='Location Name')
  print(p2)
  
  dev.off()
  
  pdf(file = 'FILEPATH/afib_input_vs_haqi.pdf', width = 14, height = 8)
  p_haq <- ggplot(afib_emr_temp[sex_binary==0,], aes(x=haqi_mean, mean_EMR, color=ihme_loc_id))+
    facet_wrap(~midage, scales = 'free_y')+
    geom_point()+
    xlab('HAQi')+ylab('EMR')+
    ggtitle('Afib input EMR vs. HAQi - Males')+
    theme_bw()
  print(p_haq)
  
  p_haq <- ggplot(afib_emr_temp[sex_binary==1,], aes(x=haqi_mean, mean_EMR, color=ihme_loc_id))+
    facet_wrap(~midage, scales = 'free_y')+
    geom_point()+
    xlab('HAQi')+ylab('EMR')+
    ggtitle('Afib input EMR vs. HAQi - Females')+
    theme_bw()
  print(p_haq)
  
  dev.off()
}

#make_plots() ## active if you would like to make diagnostic plots. 

#########################################################
## End of Vetting plots
#########################################################

afib_emr_backup <- copy(afib_emr) ## to keep a copy convinently here
afib_emr <- copy(afib_emr_backup)

loc_remove <- c(68,95,135) ## Korea, England, BZL removed - unusally low EMR. 
afib_emr <- afib_emr[-which(location_id%in% loc_remove)] 
afib_emr[,sex_binary := as.double(sex_binary)]
afib_emr[,intercept := 1]

write.csv(afib_emr, paste0(mrbrt_path ,date, ".csv"), row.names = F)

message('MRBRT STARTING')
## Run MR-BRT
data <- MRData()

log_mod <- T
logit_mod <- ifelse(log_mod,F,T)

## log modeling
if(log_mod){
  message('Modeling in log-space')
  data$load_df(
    data = afib_emr,  col_obs = "log_ratio", col_obs_se = "delta_log_se", 
    col_covs = list("midage","sex_binary","haqi_mean"))
}

## logit modeling
if(logit_mod){
  message('Modeling in logit-space')
  data$load_df(
    data = afib_emr,  col_obs = "logit_mean", col_obs_se = "logit_se", #col_obs = freq, col_obs_se = standard_error
    col_covs = list("midage","sex_binary","haqi_mean","intercept"))
}

##
cov_list <- "age_sex_haq"  #covariates included in MR-BRT run
age_predict <- c(unique(afib_emr[midage<=100,midage]))

trim_pct <- 0 
knot_number <- 3
spline_type <- 3L
r_spline_tail_linear <- T
l_spline_tail_linear <- T
knot_placement <- 'frequency'
cov_message <- paste0('trim: ',trim_pct,', knots: ', knot_number, ', right tail: ', r_spline_tail_linear,
                      ', left tail: ',l_spline_tail_linear)


model <- MRBRT(
  data = data,
  cov_models = list(
    LinearCovModel("sex_binary"),
    LinearCovModel("haqi_mean", prior_beta_uniform = array(c(-1000,0))), ## haqi must be negative, -.006 in run_mr_brt
    LinearCovModel("intercept"),
    LinearCovModel(alt_cov = c('midage'),
                   use_spline = T, #TRUE, 
                   spline_knots_type = knot_placement,
                   spline_degree = spline_type, #3L,
                   spline_l_linear = r_spline_tail_linear, #F,
                   spline_r_linear = l_spline_tail_linear  #T,
                   
    )
  ),
  inlier_pct = 1.00-trim_pct)


## fit model
model$fit_model(inner_print_level = 5L, inner_max_iter = 2000L)

message('MRBRT FINISHED')

## draws
n_samples <- 1000L
samples <- model$sample_soln(sample_size = n_samples)

## prediction data frame: 
pred_df <- expand.grid(year_id=c(year), location_id = all_locs[most_detailed==1,location_id], midage=age_predict, sex_binary=c(0,1),intercept=1)
pred_df <- join(pred_df, haq2[,c("haqi_mean","location_id","year_id")], by=c("location_id","year_id"))
pred_df$age_start <- pred_df$midage
pred_df$age_end <- pred_df$midage

## Predict
dat_pred <- MRData()

dat_pred$load_df(
  data = pred_df, 
  col_covs=list('midage', 'sex_binary', 'haqi_mean','intercept')
)

## create draws
draws <- model$create_draws(
  data = dat_pred,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = TRUE )

draws_summary <- data.table(rowQuantiles(as.matrix(draws), probs=c(.025,.975)))
draws_summary[,pred_logit_se := (`97.5%`-`2.5%`)/(qnorm(.975)*2)]

pred_col <- model$predict(dat_pred)
pred_df <- data.table(cbind(pred_df,pred_col))
pred_df$pred_logit_se <- draws_summary$pred_logit_se

if(logit_mod){
  setnames(pred_df, 'pred_col','logit_pred')
  pred_df[,pred := logit_to_linear(mean=array(logit_pred),sd=array(pred_logit_se))[[1]]]
  pred_df[,pred_se := logit_to_linear(mean=array(logit_pred),sd=array(pred_logit_se))[[2]]]
}
if(log_mod){
  setnames(pred_df, 'pred_col','log_pred')
  pred_df[,pred := log_to_linear(mean=array(log_pred),sd=array(pred_logit_se))[[1]]]
  pred_df[,pred_se := log_to_linear(mean=array(log_pred),sd=array(pred_logit_se))[[2]]]
}

graph_mrbrt_results <- function(age_start_plot, age_end_plot, normal_space = F){
  
  end_plot <-F
  data_dt <- copy(afib_emr)
  data_dt$trimming <- model$w_soln
  data_dt[,source := ifelse(trimming==0, 'Trimmed','Not Trimmed')]
  
  model_dt <- copy(pred_df)
  quant_haq <- round(quantile(model_dt$haqi_mean, probs = seq(0,1, by = 0.25)), digits = 2)
  model_dt <- model_dt[, haqi_mean := round(haqi_mean, digits =2)]
  model_dt <- model_dt[model_dt$haqi_mean %in% quant_haq, ]
  model_dt$haqi_mean <- as.character(model_dt$haqi_mean)
  model_dt$sex <- ifelse(model_dt$sex_binary == 1, "Female", "Male")
  
  model_dt_min_haq <- data.table(model_dt[haqi_mean==quant_haq[1]])
  model_dt_min_haq[,line:= 'Min HAQI']
  model_dt_median_haq <- data.table(model_dt[haqi_mean==quant_haq[2]])
  model_dt_median_haq[, line:= 'Median HAQI']
  model_dt_max_haq <- data.table(model_dt[haqi_mean==quant_haq[3]])
  model_dt_max_haq[,line:='Max HAQI']
  
  coefs <- data.frame(unlist(model$fe_soln))
  coefs <- format(round(coefs, 4))
  names(coefs) <- 'coefficients'
  
  print(coefs)
  data_dt[,Source := ifelse(trimming==1,'Not trimmed','Trimmed')]
  data_dt[,sex:= ifelse(sex_binary==0,'Male','Female')]
  
  model_dt[,line:='GBD 2020 - Current']
  model_dt$line <- factor(model_dt$line, levels = c('Min HAQI','Median HAQI','Max HAQI'))
  if(normal_space){
    gg_subset<- ggplot() +
      geom_point(data = data_dt[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = mean_EMR, color = Source),alpha=.2)+
      labs(x = "Age", y = "EMR") +
      ggtitle(paste0("MR-BRT Model Results Overlay on EMR Input \n", cov_message)) +
      theme_classic() +
      theme(text = element_text(size = 15, color = "black")) +
      geom_line(data = model_dt_min_haq[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = pred,colour=line), size=.5, alpha=2) +
      geom_line(data = model_dt_median_haq[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = pred,colour=line), size=.5,alpha=2) +
      geom_line(data = model_dt_max_haq[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = pred,colour=line), size=.5,alpha=2) +
      #ylim(c(0,.15))+
      scale_color_manual(values = c('Trimmed'="#A3A500",'Not trimmed'="#F8766D",'Max HAQI'="#00BF7D",'Median HAQI'="#00B0F6",'Min HAQI'="#E76BF3"))+
      facet_wrap(~sex)
    end_plot <- T
  }
  if(logit_mod & !end_plot){
    gg_subset<- ggplot() +
      geom_point(data = data_dt[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = logit_mean, color = Source),alpha=.2)+
      labs(x = "Age", y = "EMR") +
      ggtitle(paste0("MR-BRT Model Results Overlay on EMR Input - Normal space \n", cov_message)) +
      theme_classic() +
      theme(text = element_text(size = 15, color = "black")) +
      geom_line(data = model_dt_min_haq[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = logit_pred,colour=line), size=.5, alpha=2) +
      geom_line(data = model_dt_median_haq[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = logit_pred,colour=line), size=.5,alpha=2) +
      geom_line(data = model_dt_max_haq[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = logit_pred,colour=line), size=.5,alpha=2) +
      #ylim(c(0,.15))+
      scale_color_manual(values = c('Trimmed'="#A3A500",'Not trimmed'="#F8766D",'Max HAQI'="#00BF7D",'Median HAQI'="#00B0F6",'Min HAQI'="#E76BF3"))+
      facet_wrap(~sex)
  }
  if(log_mod & !end_plot){
    gg_subset<- ggplot() +
      geom_point(data = data_dt[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = log_ratio, color = Source),alpha=.2)+
      labs(x = "Age", y = "EMR") +
      ggtitle(paste0("MR-BRT Model Results Overlay on EMR Input, \n",cov_message)) +
      theme_classic() +
      theme(text = element_text(size = 15, color = "black")) +
      geom_line(data = model_dt_min_haq[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = log_pred,colour=line), size=.5, alpha=2) +
      geom_line(data = model_dt_median_haq[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = log_pred,colour=line), size=.5,alpha=2) +
      geom_line(data = model_dt_max_haq[midage >= age_start_plot & midage <= age_end_plot], aes(x = midage, y = log_pred,colour=line), size=.5,alpha=2) +
      #ylim(c(0,.15))+
      scale_color_manual(values = c('Trimmed'="#A3A500",'Not trimmed'="#F8766D",'Max HAQI'="#00BF7D",'Median HAQI'="#00B0F6",'Min HAQI'="#E76BF3"))+
      facet_wrap(~sex)
  }
  return(gg_subset)
  
}

graph_emr <- graph_mrbrt_results(age_start_plot = 30, age_end_plot = 100, normal_space = T)
graph_emr_young_norm <- graph_mrbrt_results(age_start = 30, age_end = 60, normal_space = T)
graph_emr_young <- graph_mrbrt_results(age_start = 30, age_end = 60)
graph_emr_logit <- graph_mrbrt_results(age_start_plot = 30, age_end_plot = 100, normal_space = F)
graph_emr
graph_emr_young_norm
graph_emr_young
graph_emr_logit

ggsave(graph_emr, 
       filename = paste0(output_path,date, "without_bzl_exp_fit_graphs_no_haqi",trim_pct,"_",knot_number,"_",paste(spline_tail),".pdf"), width = 12, height=8)

ggsave(graph_emr_young_norm, 
       filename = paste0(output_path,date, "without_bzl_young_norm_exp_fit_graphs_no_haqi",trim_pct,"_",knot_number,"_",paste(spline_tail),".pdf"), width = 12, height=8)

ggsave(graph_emr_young, 
       filename = paste0(output_path,date, "without_bzl_young_logit_fit_graphs_no_haqi",trim_pct,"_",knot_number,"_",paste(spline_tail),".pdf"), width = 12, height=8)

ggsave(graph_emr_logit, 
       filename = paste0(output_path,date, "without_bzl_young_logit_fit_graphs_no_haqi",trim_pct,"_",knot_number,"_",paste(spline_tail),".pdf"), width = 12, height=8)


## Prepare predicted EMR for plotting

final_emr <- copy(pred_df)
final_emr[, c("logit_pred","pred_logit_se","midage", "haqi_mean")] <- NULL

# Remove locations from all locations used in MR-BRT + their subnationals
loc_meta <- get_location_metadata(location_set_id = 35, release_id = 16)
all_locs <- loc_meta[most_detailed==1,]

## Bring in earlier saved raw EMR data, format to attach to estimates. 
final_keep_locs <- afib_preserve   
final_keep_locs[,age_group_id:=as.character(age_group_id)]
final_keep_locs <- data.table(merge(final_keep_locs,age_meta[,c('age_group_id','age_group_years_start','age_group_years_end')])) # merge age info
final_keep_locs[,location_id:=as.integer(location_id)] #convert loc info

final_keep_locs <- data.table(merge(final_keep_locs,loc_meta[,c('location_id','parent_id')], by='location_id')) #append on parent id
final_keep_locs[,midage:=(age_group_years_start+age_group_years_end)/2]
final_keep_locs[,parent_id:=location_id] #strictly for purposes of merging. 

final_uk <- final_keep_locs[location_id==95] ## UK locations
final_not_uk <- final_keep_locs[location_id!=95] ##Not UK locations

## identify locations by whether they have subnationals or not
has_subnat <- c(67,72,86,90,102,135,4749,95)                        #have subnationals
has_subnat <- has_subnat[has_subnat %ni% loc_remove]

no_subnat <-  c(76,78,81,82,83,84,85,89,92,93,94,101)               #do not have subnationals.
uk_has_subnat <- c(4520,4622,4626,4625,4619,4618,4624,4623,4621)    #England one level down. 

has_subnat_children <- loc_meta[parent_id %in% has_subnat,c("location_id",'location_name','parent_id')]
has_subnat_children_uk <- loc_meta[parent_id %in% uk_has_subnat,c("location_id",'location_name','parent_id')]

## Special for the UK - need to get lowest level in location heirarchy - different from other locations. 
uk_used_in_model <- T

if(uk_used_in_model){
  final_uk_list <- list()
  i=1
  for(id in unique(has_subnat_children_uk$location_id)){
    temp <- copy(final_uk)
    temp[,location_id := id]
    final_uk_list[[i]] <- temp
    i = i+1
  }
  final_uk_df <- rbindlist(final_uk_list)
}

## For other locations beside uk with subnationals: 
## populate sub-nats (non-uk): 
final_non_uk_list_subnat <- list()
i=1
for(id in unique(has_subnat_children$location_id)){
  temp_parent_id <- loc_meta[location_id==id,parent_id]
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

#rebind everything together. 
if(uk_used_in_model){
  final_subnats <- data.table(rbind(final_non_uk_subnat_df,
                                    final_uk_df,
                                    final_non_uk_not_subnat_df))
}else{
  final_subnats <- data.table(rbind(final_non_uk_subnat_df,
                                    final_non_uk_not_subnat_df))
}

## Clean-up the raw EMR data

final_subnats[,`:=` (age_group_id=NULL,emr_parent=NULL,age_group_years_start=NULL,age_group_years_end=NULL,parent_id=NULL,reg_loc=NULL, year_id=NULL,
                     cases=NA,sample_size=NA,standard_error=NA)]
setnames(final_subnats,'mean_EMR','mean')
final_subnats[,sex := ifelse(sex_id==1,'Male','Female')]
final_subnats[,`:=` (year_start=1990, year_end=2022)]
final_subnats[,uncertainty_type_value:=95]

## Bind on all remaining estimation locations
final_dt <-as.data.table(final_emr)
setnames(final_dt, c('pred','pred_se'),c('mean','standard_error'))
final_dt[,`:=` (location_id = as.character(location_id), sex_id = as.character(sex_binary+1), midage=age_end,
                year_start = 1990, year_end = 2022,uncertainty_type_value = 95)]
final_dt[,`:=`(sex_binary = NULL, year_id = NULL, age_start=NULL,age_end=NULL, lower = NA, upper=NA)]
final_dt[,sex := ifelse(sex_id == 1, "Male", "Female")]
final_dt[,`:=` (lower = mean - qnorm(.975)*standard_error, upper = mean + qnorm(.975)*standard_error)]

## Bind on the locations with raw emr to modeled EMR
## Remove locations used in regression from final_dt:
remove_locs <- unique(final_subnats$location_id)           #locations with input data used in modeling
final_dt <- final_dt[-which(location_id %in% remove_locs)] #remove those locations from all modeled EMR
final_dt <- rbind.fill(final_dt,final_subnats)             #Rbind together
final_dt <- data.table(final_dt)

## Merge on GBD age groups
final_dt <- data.table(merge(final_dt,age_meta[,c('age_group_id','midage','age_group_years_start','age_group_years_end')],
                             by='midage'))
setnames(final_dt,c('age_group_years_start','age_group_years_end'),c('age_start','age_end'))
final_dt[age_start==95, age_end:=99]
final_dt[age_end!=99,age_end:=age_end-1]               #adjust for using age_metadata
final_dt[ , age_group_id := as.double(age_group_id)]   #set age group, both 

## For uploader validation - add in needed columns
final_dt$seq <- NA
final_dt <- join(final_dt, loc_meta[,c("location_id", "location_name")], by="location_id")
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
final_dt$step2_location_year <- "This is modeled EMR that uses the NID for one dummy row of EMR data added to the bundle data"
final_dt$underlying_nid <- NA
final_dt$sampling_type <- ""
final_dt$recall_type <- "Point"
final_dt$input_type <- "extracted"
final_dt$effective_sample_size <- NA
final_dt$design_effect <- NA
final_dt$recall_type_value <- ""

bundle_path <- "FILEPATH/afib_emr_from_mrbrt001_"
write.xlsx(final_dt, file = paste0(bundle_path,date,paste0("_uncorrected_codem_dismod_",mv_id,".xlsx")))

