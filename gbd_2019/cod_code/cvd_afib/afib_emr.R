############################
## Afib EMR Predictions
## Obj: Purpose of this code is to predict out EMR for all locations minus a subset of high quality observations.
##      We used a linear-mixed effects model in GBD 2017 to predict out EMR onto location-years. For GBD 2019, we upgraded
##      the approach to MR-BRT and added an additional covariate (HAQI)
#############################

rm(list=ls())

source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_ids.R")
repo_dir <- "FILEPATH"
source(paste0(repo_dir, "mr_brt_functions.R"))
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/get_draws.R")
'%!in%' <- Negate('%in%')

## Note to user: Lines 24-208 are adapting the previous approach we used in GBD 2017.
###########################
## NOTE: The first chunk of code is an adaptation of the original STATA code
##       that was used to create EMR estimates in GBD 2017 using the linear mixed effects model.
#Expand to all countries in epi database
source("FILEPATH/get_location_metadata.R")
loc_meta_data <- get_location_metadata(location_set_id=9)
loc_meta_data <- loc_meta_data[which(loc_meta_data$is_estimate==1),]
loc_meta_data <- loc_meta_data[ ,c('location_id','location_name','level','parent_id')]
#loc_meta_data <- loc_meta_data[which(loc_meta_data$level >= 3),] #keeps national and subnationals.

## get step one dismod afib bundle.

all_loc_ids <- unique(loc_meta_data$location_id) #all location ids.

## Select countries
loc_ids <- c(67,68,71,76,78,81,82,83,84,86,89,92,93,94,95,101,102) #location ids to pull csmr and prev from for use in MR-BRT #unique(loc_meta_data$location_id)#
ages    <- c(11,12,13,14,15,16,17,18,19,20,30,31,32,235)           #age ids to pull csmr and prev from for use in MR-BRT

#get draws for csmr and prevalence for selected countries for 2017.
# for prevalence:
afib_epi_draws <- get_draws(gbd_id_type='modelable_entity_id', gbd_id=1859, measure_id=5, location_id=all_loc_ids, status='best', sex_id=c(1,2), year_id='2017',decomp_step='step4',source='epi' )
colnames(afib_epi_draws)<- gsub('draw','prev', colnames(afib_epi_draws)) #replace "draws" with "prev"

#for CSMR
afib_cod_draws_csmr_m <- get_draws(gbd_id_type='cause_id', gbd_id=500, location_id=all_loc_ids, year_id = '2017',source='codem',version_id= '', gbd_round_id = ) #males
afib_cod_draws_csmr_f <- get_draws(gbd_id_type='cause_id', gbd_id=500, location_id=all_loc_ids, year_id = '2017',source='codem',version_id= '', gbd_round_id = ) #females
afib_cod_draws_csmr_b <- data.frame(rbind(afib_cod_draws_csmr_m,afib_cod_draws_csmr_f))

#Calculate CSMR
for(i in 3:1002){
  afib_cod_draws_csmr_b[,i] <- afib_cod_draws_csmr_b[,i]/afib_cod_draws_csmr_b$pop
}
#Sub csmr in for draws.
colnames(afib_cod_draws_csmr_b)<- gsub('draw','csmr',colnames(afib_cod_draws_csmr_b))

#MERGE
afib_all <- merge(afib_cod_draws_csmr_b,afib_epi_draws, by=c('location_id','year_id','age_group_id','sex_id'))

#convert to strings where appropriate:
afib_all$location_id  <- as.character(afib_all$location_id)
afib_all$age_group_id <- as.character(afib_all$age_group_id)
afib_all$sex_id       <- as.character(afib_all$sex_id)
afib_all$year_id      <- as.character(afib_all$year_id)

#drop unnecessary columns
afib_all_keep <- afib_all[,c("location_id","year_id","age_group_id","sex_id","pop",colnames(afib_all)[grep('csmr', colnames(afib_all))],colnames(afib_all)[grep('prev', colnames(afib_all))])]

#calculate_emr
for(i in 0:999){
  EMR <- afib_all_keep[,paste0('csmr_',i)]/afib_all_keep[,paste0('prev_',i)]
  afib_all_keep[,paste0('EMR',i)]<-EMR
}

library(matrixStats)
#calculate means and upper/lower 95% interval
afib_all_keep$mean_EMR <- rowMeans(afib_all_keep[,grep('EMR',colnames(afib_all_keep))])
afib_subset <- afib_all_keep[,grep('EMR',colnames(afib_all_keep))]
afib_subset <- afib_subset[,-1001]

test <- rowQuantiles(as.matrix(afib_subset), probs=c(.025,.975))

afib_all_keep$lower <- test[,1]
afib_all_keep$upper <- test[,2]

#Preserve - these are the locations that we model based on:

#calculate log_emr
for(i in 0:999){
  Log_EMR <- log(afib_all_keep[,paste0('csmr_',i)]/afib_all_keep[,paste0('prev_',i)])
  afib_all_keep[,paste0('Log_EMR',i)]<-Log_EMR
}


#drop unneeded cols.
afib_all_keep <- afib_all_keep[,-c(grep('csmr',colnames(afib_all_keep)),grep('prev',colnames(afib_all_keep)))]
afib_all_keep$reg_loc <-1
afib_all_keep$emr_parent <- afib_all_keep$location_id
#calculate mean
afib_all_keep$mean_Log_EMR <- rowMeans(afib_all_keep[,grep('Log_EMR',colnames(afib_all_keep))])

afib_all_keep <- data.table(afib_all_keep)
#Preserve - building models based on this data only
afib_preserve <- afib_all_keep[location_id %in% loc_ids, c('location_id','year_id','sex_id','age_group_id','mean_EMR','upper','lower')]
afib_preserve$reg_loc <- 1
afib_preserve$emr_parent <- afib_preserve$location_id

afib_emr <- data.table(afib_preserve) #will build mr-brt model on these EMRS.

## Add column for haqi subsetted to location_id - use midyear for prevalence data to subset to haqi year_id
source("FILEPATH/get_covariate_estimates.R")
haq <- as.data.table(get_covariate_estimates(
  covariate_id=1099,
  gbd_round_id=6,
  decomp_step='step4'
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

# Log standard error
afib_emr$standard_error<-(afib_emr$upper-afib_emr$lower)/(1.96*2)
afib_emr$delta_log_se <- sapply(1:nrow(afib_emr), function(i) {
  ratio_i <- afib_emr[i, "mean_EMR"]
  ratio_se_i <- afib_emr[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

#GET CORRECT MIDAGE
age_id_to_midage_frame <- data.frame(ages,c(32,37,42,47,52,57,62,67,72,77,82,87,92,97.5),c(30,40,40,50,50,60,60,70,70,80,80,90,90,100))
colnames(age_id_to_midage_frame)<- c('age_group_id','midage','age_brt')

#afib_emr$midage <- NA
for(i in 1:nrow(afib_emr)){
  if(i%%1000==0){
    print(paste0('iteration ', i))
  }
  afib_emr$midage[i] <- age_id_to_midage_frame$midage[which(age_id_to_midage_frame$age_group_id==afib_emr$age_group_id[i])]
}

#get sex_binary correctly formatted as 0/1
afib_emr$sex_binary= as.character(as.numeric(afib_emr$sex_id)-1)

## Export results as csv file

date <- gsub("-", "_", Sys.Date())
write.csv(afib_emr, paste0("FILEPATH/afib_emr_for_mr_brt" ,date, ".csv"), row.names = F)

## Link with launching a MR-BRT model ##
#########################################################################################################################################

repo_dir <- "FILEPATH/run_mr_brt/"
source(paste0(repo_dir, "mr_brt_functions.R"))


## Run MR-BRT -------------------------------------------------------------------------------------------------
cov_list <- "age_sex_haq"  #covariates included in MR-BRT run
age_predict <- c(30,40,50,60,70,80,90,100) #ages you want to predict EMR -
trim <- 0.1 #change this if you want to trim more/less in MR-BRT regression
knot_number <- 3 #three is standard
spline_type <- 3 # set to three for cubic
spline_tail <- T #set to T for linear tails
knot_placement <- "frequency" #"NA" if want evenly spaced knots, "frequency" if density

fit1 <- run_mr_brt(
  output_dir = "FILEPATH/Afib_data",
  model_label = 'Afib_EMR_Fix',
  data = paste0("FILEPATH/afib_emr_for_mr_brt" ,date, ".csv"),
  mean_var = "log_ratio",
  se_var = "delta_log_se",
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE,
  method = "trim_maxL",
  study_id = "location_id",
  trim_pct = trim,
  covs = list(
    cov_info("midage", "X", degree = spline_type, n_i_knots = knot_number,
             r_linear = spline_tail, l_linear = spline_tail, knot_placement_procedure = knot_placement),
    cov_info("sex_binary", "X"),
    cov_info("haqi_mean", "X", uprior_ub = 0)
  )
)

locs <- read.csv("FILEPATH/ihme_loc_metadata_2019.csv")
loc_pull <- unique(locs$location_id)

pred_df <- expand.grid(year_id=c(2017), location_id = locs$location_id[locs$level >= 3], midage=age_predict, sex_binary=c(0,1))
pred_df <- join(pred_df, haq2[,c("haqi_mean","location_id","year_id")], by=c("location_id","year_id"))

pred_df$age_start <- pred_df$midage
pred_df$age_end <- pred_df$midage
colnames(pred_df)[5] <- 'haqi_mean'

## Predict mr brt
pred <- predict_mr_brt(fit1, newdata = pred_df, write_draws = T)
check_for_preds(pred)
pred_object <- load_mr_brt_preds(pred)
predicted <- pred_object$model_summaries

setnames(predicted, "X_sex_binary", "sex_binary")
setnames(predicted, "X_midage", "midage")
setnames(predicted, "X_haqi_mean", "haqi_mean.x")
colnames(predicted)[4] <- "haqi_mean.x"

predicted[, c("X_intercept", "Z_intercept")] <- NULL

final_emr <- cbind(predicted, pred_df)
final_emr <- final_emr[, !duplicated(colnames(final_emr))]
final_emr[, c("Y_negp", "Y_mean_fe", "Y_mean_lo_fe", "Y_mean_hi_fe", "Y_negp_fe", "midage", "haqi_mean")] <- NULL

## Graph EMR data and model fit over age by sex and a few HAQ
library(gridExtra)

results <- fit1
predicts <- predicted
graph_mrbrt_results <- function(results, predicts){
  data_dt <- as.data.table(results$train_data)
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  model_dt <- as.data.table(predicts)
  #quant_haq <- round(quantile(model_dt$haqi_mean, probs = seq(0,1, by = 0.25)), digits = 2)
  quant_haq <- c(7.54 , 51.19, 94.34) .
  model_dt <- model_dt[, haqi_mean := round(haqi_mean.x, digits =2)]
  model_dt <- model_dt[model_dt$haqi_mean %in% quant_haq, ]
  model_dt$haqi_mean <- as.character(model_dt$haqi_mean)
  model_dt$sex <- ifelse(model_dt$sex_binary == 1, "Female", "Male")
  coefs <- fit1$model_coefs[, c("x_cov", "beta_soln", "beta_var", "gamma_soln")]
  coefs$gamma_soln <- coefs$gamma_soln[1]
  coefs$variance_with_gamma <- coefs$beta_var + coefs$gamma_soln^2
  coefs <- coefs[, c("x_cov", "beta_soln", "variance_with_gamma")]
  coefs[,c("beta_soln","variance_with_gamma")] = format(round(coefs[,c("beta_soln", "variance_with_gamma")],3))
  print(coefs)
  gg_subset<- ggplot() +
    geom_point(data = data_dt, aes(x = midage, y = exp(log_ratio), color = as.factor(excluded)))+
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) +
    labs(x = "Age", y = "EMR") +
    ggtitle(paste0("MR-BRT Model Results Overlay on EMR Input")) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black")) +
    geom_line(data = model_dt, aes(x = midage, y = exp(Y_mean), color = haqi_mean, linetype = sex), size=1.5) +
    #geom_ribbon(data = model_dt, aes(x = midage, ymin = Y_mean_lo, ymax = Y_mean_hi), alpha = 0.05) + #if you want to see uncertainty, but would need to specify just one HAQ to use this
    scale_color_manual(name = "", values = c("blue", "red", "purple", "black", "orange"),
                       labels = c("Included EMR", "Trimmed EMR", "50th Percentile HAQ", "Min HAQ", "Max HAQ")) +
    #xlim(0,100) + ylim(0,5) +
    annotation_custom(grob = tableGrob(coefs, rows = NULL),
                      xmin=50, xmax=80, ymin=0.05)
  return(gg_subset)
}


graph_emr <- graph_mrbrt_results(results = fit1, predicts = predicted)
graph_emr
ggsave(graph_emr, filename = paste0(home_dir, cause_path, cause_name, "exp_fit_graphs.pdf"), width = 12)

final_dt <-as.data.table(final_emr)
final_dt$Y_se <- (final_dt$Y_mean_hi-final_dt$Y_mean_lo)/(1.96*2)
final_dt[, `:=` (mean = exp(Y_mean), standard_error = deltamethod(~exp(x1), Y_mean, Y_se^2)), by = c("Y_mean", "Y_se")]

final_dt$sex_id <- as.character(final_dt$sex_binary+1)
#need to add in an age_group_id.
final_dt$age_group_id <- NA
for(i in 1:nrow(final_dt)){
  if(i %% 1000 == 0){
    print(paste0('iteration ',i))
  }
  final_dt$age_group_id[i] <- age_id_to_midage_frame$age_group_id[which(age_id_to_midage_frame$age_brt==final_dt$age_start[i])[1]]
}

final_dt$location_id <- as.character(final_dt$location_id)
final_dt$sex_id      <- as.character(final_dt$sex_id)
final_dt$age_group_id<- as.character(final_dt$age_group_id)

## need age start and age end.
## For uploader validation - add in needed columns
final_dt$seq <- 869734125
final_dt$sex <- ifelse(final_dt$sex_binary == 1, "Female", "Male")
final_dt$year_start <- 1990 #final_dt$year_id
final_dt$year_end <- 2019 #final_dt$year_id
final_dt[,c("sex_binary", "year_id", "haqi_mean", "log_ratio", "delta_log_se", "midage", "Y_mean", "Y_se", "Y_mean_lo", "Y_mean_hi")] <- NULL
final_dt <- join(final_dt, locs[,c("location_id", "location_name")], by="location_id")
final_dt$nid <- 416752
final_dt$year_issue <- 0
final_dt$age_issue <- 0
final_dt$sex_issue <- 0
final_dt$measure <- "mtexcess"
final_dt$source_type <- "Facility - inpatient"
final_dt$extractor <- uw_name
final_dt$is_outlier <- 0
final_dt$sample_size <- 1000
final_dt$cases <- final_dt$sample_size * final_dt$mean
final_dt$measure_adjustment <- 1
final_dt$uncertainty_type <- "Confidence interval"
final_dt$recall_type <- "Point"
final_dt$urbanicity_type <- "Mixed/both"
final_dt$unit_type <- "Person"
final_dt$representative_name <- "Nationally and subnationally representative"
final_dt$note_modeler <- "These data are modeled from dismod EMR using HAQI as a predictor"
final_dt$unit_value_as_published <- 1
final_dt$step2_location_year <- "This is modeled EMR that uses the NID for one dummy row of EMR data added to the bundle data"
final_dt$lower <- NA
final_dt$upper <- NA
write.xlsx(final_dt, file = "FILEPATH/afib_emr_from_mrbrt.xlsx" )


