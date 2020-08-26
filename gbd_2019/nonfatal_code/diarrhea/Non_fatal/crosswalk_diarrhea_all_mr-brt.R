#######################################################################
## The purpose of this code is to generate input data sheets for MR-BRT
## for the inpatient data (prepared by clinical team).
#######################################################################
library(metafor)
library(msm)
library(plyr)
library(boot)
library(ggplot2)
library(openxlsx)
source() # filepaths to central functions for MRBRT
source() # filepaths to central functions for data upload and download
source("/filepath/bundle_crosswalk_collapse.R")
source("/filepath/convert_inc_prev_function.R")

locs <- read.csv("filepath")

output_vals <- data.frame()

## get SDI as a potential predictor
source("filepath/get_covariate_estimates.R")
sdi <- get_covariate_estimates(location_id="all", covariate_id=881, decomp_step="step4", year_id=1980:2019)
  sdi <- join(sdi, locs[,c("location_id","ihme_loc_id")], by="location_id")
  sdi$sdi <- sdi$mean_value
haqi <- get_covariate_estimates(location_id="all", covariate_id=1099, decomp_step="step4", year_id=1980:2019)
  haqi <- join(haqi, locs[,c("location_id","ihme_loc_id")], by="location_id")
  haqi$haqi <- haqi$mean_value

##----------------------------------------------------------------------------
  ## Pull all data
##----------------------------------------------------------------------------

 bundle_version_id <- 10772

## Pull data
  # all_data <- get_bundle_version(bundle_version_id = bundle_version_id, export = FALSE)
  # all_data <- data.frame(all_data)
  # all_data <- all_data[, -which(names(all_data) %in% c("cv_low_income_hosp","cv_dhs","cv_explicit_test","cv_nine_plus_test","cv_whs","duration","duration_lower","duration_upper",
  #                                                      "cv_hospital_child","unnamed..75.1","unnamed..75","level","cv_marketscan_all_2000","incidence_corrected","cv_marketscan_inp_2000",
  #                                                      "cv_marketscan_all_2010","cv_hospital_adult","cv_marketscan_inp_2010","cv_marketscan_all_2012","cv_marketscan_inp_2012"))]
  # all_data[is.na(all_data)] <- ""
  # write.csv(all_data, "filepath", row.names=F)

  all_data <- read.csv("filepath")
    all_data$sample_size <- as.numeric(all_data$sample_size)
    all_data$gbd_2019_new <- 0

## Pull new data
  step4_bvid <- 13046
  new_data <- get_bundle_version(bundle_version_id = 13046)
  new_data$gbd_2019_new <- 1
  new_data$gbd_round <- 2019

  # All data
  all_data <- rbind.fill(all_data, new_data)
  all_data$gbd_round[is.na(all_data$gbd_round)] <- 2017

  non_clin_df <- subset(all_data, measure != "incidence")
    non_clin_df$cv_hospital <- ifelse(non_clin_df$cv_hospital==1,1,ifelse(non_clin_df$cv_clin_data==1,1,0))
    non_clin_df$cv_clin_data <- 0
    non_clin_df$mean_original <- non_clin_df$mean

  # Calculate cases and sample size (missing in some data)
    sample_size <- with(non_clin_df, mean*(1-mean)/standard_error^2)
    cases <- non_clin_df$mean * sample_size
    non_clin_df$cases <- ifelse(is.na(non_clin_df$cases), cases, non_clin_df$cases)
    non_clin_df$sample_size <- ifelse(is.na(non_clin_df$sample_size), sample_size, non_clin_df$sample_size)

  clin_df <- subset(all_data, measure == "incidence")
    clin_df$is_reference <- 0
    clin_df$cases <- clin_df$mean * clin_df$sample_size
    clin_df$age_end <- ifelse(clin_df$age_end > 99, 99, clin_df$age_end)
    clin_df$age_mid <- floor((clin_df$age_start + clin_df$age_end)/2)

  ## Use a function to convert incidence to prevalence
  prevalence_means <- convert_inc_prev(clin_df, duration = 4.2, duration_lower = 4.1, duration_upper = 4.4)
    clin_df$mean <- prevalence_means$mean_prevalence
    clin_df$standard_error <- prevalence_means$standard_error_prevalence

  # Calculate cases and sample size (missing in inpatient data)
    sample_size <- with(clin_df, mean*(1-mean)/standard_error^2)
    cases <- with(clin_df, mean * sample_size)
    clin_df$cases <- ifelse(is.na(clin_df$cases), cases, clin_df$cases)
    clin_df$sample_size <- ifelse(is.na(clin_df$sample_size), sample_size, clin_df$sample_size)

    clin_df$cases <- clin_df$mean * clin_df$sample_size
    clin_df$lower <- ""
    clin_df$upper <- ""
    clin_df$cv_inpatient <- ifelse(clin_df$clinical_data_type=="inpatient",1,0)

  # The data in Japan are extremely high, don't crosswalk
    jpn_locs <- locs$location_id[locs$parent_id==67]
    clin_df$cv_inpatient <- ifelse(clin_df$location_id %in% jpn_locs, 0, clin_df$cv_inpatient)

    clin_df$cv_marketscan <- ifelse(clin_df$clinical_data_type=="claims",1,0)
    clin_df$cv_clin_data <- 1

  # Don't adjust Taiwain claims data
    clin_df$cv_marketscan[clin_df$location_name=="Taiwan" & clin_df$cv_marketscan==1] <- 0
    clin_df$measure <- "prevalence"

  # Calculate cases and sample size (missing in inpatient data)
    sample_size <- with(clin_df, mean*(1-mean)/standard_error^2)
    cases <- clin_df$mean * sample_size
    clin_df$cases <- ifelse(is.na(clin_df$cases), cases, clin_df$cases)
    clin_df$sample_size <- ifelse(is.na(clin_df$sample_size), sample_size, clin_df$sample_size)
    clin_df$mean_original <- clin_df$mean

  diarrhea <- rbind.fill(non_clin_df, clin_df)


  ##-----------------------------------------------------------------------------
  # I am recoding Clinical Informatics
  # inpatient data to be 'cv_inpatient = 1' and identifying that by extractor.
  # I will leave a cv_miscoded = 1 if cv_inpatient = 1 or cv_inpatient_sample = 1
  # but the extractor isn't some specific users.
  diarrhea$cv_inpatient_lit <- with(diarrhea, ifelse(cv_clin_data==1,0,ifelse(cv_inpatient==1,1,ifelse(cv_inpatient_sample==1,1,0))))
  diarrhea$cv_inpatient_sample <- 0

  diarrhea$is_reference <- ifelse(diarrhea$cv_hospital==0 & diarrhea$cv_clin_data==0 & diarrhea$cv_inpatient_lit==0,1,0)

  diarrhea_full <- diarrhea

# Join with SDI as it is a predictor for cv_hospital
  diarrhea_full$year_id <- floor((diarrhea_full$year_start + diarrhea_full$year_end) / 2)
  diarrhea_full <- join(diarrhea_full, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
  diarrhea_full$sdi <- round(diarrhea_full$sdi,2)

## Set some parameters ##
  age_bins <- c(0,1,5,10,20,40,60,80,100)
  age_bins <- c(0,1,2,seq(5,100,5))

## Subset to working data frame ##
  df <- diarrhea_full[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
                  "cv_diag_selfreport","cv_hospital","cv_inpatient","cv_clin_data","is_reference","is_outlier","group_review","cv_marketscan", "gbd_round","gbd_2019_new")]
  df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")


######################################################################
## Clinical data crosswalk. This is for data from the clinical
## informatics team that produces estimates of the incidence of diarrhea.
## Use the data that are currently in the epi db that are CF2.
######################################################################

#################################################################################
  ## Separate crosswalks for inpatient, claims clinical data ##
#################################################################################
##----------------------------------------------------------------------------------------------------------
cv_inpatient <- bundle_crosswalk_collapse(df[df$gbd_2019_new == 0, ], 
                                          covariate_name="cv_inpatient", 
                                          age_cut=age_bins, 
                                          year_cut=c(seq(1980,2015,5),2019), 
                                          merge_type="between", 
                                          location_match="exact", 
                                          include_logit = T)
cv_marketscan <- bundle_crosswalk_collapse(df[df$gbd_2019_new == 0, ], 
                                           covariate_name="cv_marketscan", 
                                           age_cut=age_bins, 
                                           year_cut=c(seq(1980,2015,5),2019), 
                                           merge_type="between", 
                                           location_match="exact", 
                                           include_logit = T)

  cv_inpatient$year_id <- floor((cv_inpatient$year_end + cv_inpatient$year_start)/2)
  cv_inpatient$location_id <- cv_inpatient$location_match
  cv_inpatient$age_mid <- floor((cv_inpatient$age_end + cv_inpatient$age_start)/2)

  cv_inpatient <- join(cv_inpatient, sdi[,c("location_id","year_id","sdi")], by=c("year_id","location_id"))
  cv_inpatient <- join(cv_inpatient, locs[,c("location_id","super_region_name","ihme_loc_id")], by="location_id")
  cv_inpatient$high_income <- ifelse(cv_inpatient$super_region_name=="High-income",1,0)
  cv_inpatient$study_id <- paste0(cv_inpatient$location_match,"_",cv_inpatient$age_bin)

##########################################################################
############################## Inpatient  ################################
##########################################################################
  cv_inpatient$match_id <- paste0(cv_inpatient$nid,"_",cv_inpatient$n_nid)
  cv_inpatient$age_u5 <- ifelse(cv_inpatient$age_end <=5, 1, 0)

# ## Run an MR-BRT model. ##

## Step 2/3 Best version has no covariates ##
fit1 <- run_mr_brt(
  output_dir = paste0("filepath"),
  model_label = "cv_inpatient",
  data = cv_inpatient, #[cv_inpatient$ratio>1,],
  mean_var = "logit_ratio",
  se_var = "logit_ratio_se",
  # covs = list(cov_info("age_mid","X",
  #                      degree = 3, n_i_knots=4,
  #                      l_linear=FALSE, r_linear = FALSE, bspline_gprior_mean = "0,0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf,inf")),
  # covs = list(cov_info("age_mid","X",
  #                      degree = 2, n_i_knots = 3,
  #                      l_linear=TRUE, r_linear = FALSE, bspline_gprior_mean = "0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf"),
  #             cov_info("sdi","X")),
   # covs = list(cov_info("sdi","X",
   #                      degree = 3, n_i_knots=4,
   #                      l_linear=TRUE, r_linear = FALSE, bspline_gprior_mean = "0,0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf,inf")),
  overwrite_previous = TRUE,
  study_id = "match_id",
  method = "trim_maxL",
  trim_pct = 0.1
)

  df_pred <- data.frame(intercept=1, age_mid=seq(0,100,1))
  df_pred <- data.frame(expand.grid(intercept=1, sdi = seq(0,1,0.01), age_mid=seq(0,100,1)))
  pred1 <- predict_mr_brt(fit1, newdata = df_pred)

  pred_object <- load_mr_brt_preds(pred1)
  preds <- pred_object$model_summaries
  preds$age_mid <- preds$X_age_mid
  preds$sdi <- preds$X_sdi

  mod_data <- fit1$train_data
  mod_data$outlier <- floor(abs(mod_data$w - 1))
  ggplot(preds, aes(x=X_age_mid, y=(Y_mean))) + geom_line() +
    geom_ribbon(aes(ymin=(Y_mean_lo), ymax=(Y_mean_hi)), alpha=0.3) +
    geom_point(data=mod_data, aes(x=age_mid, y=logit_ratio, col=super_region_name, shape=factor(outlier))) + scale_shape_manual(values=c(19,4)) + guides(col=F) +
    geom_hline(yintercept=0, lty=2, col="red") + scale_y_continuous("Log ratio") + scale_x_continuous("Age", limits=c(0,100), breaks=seq(0,100,5)) + theme_bw() + ggtitle("Inpatient ratio")

# Calculate log se
  preds$inpatient_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)

# # Convert the mean and standard_error to linear space
  preds$inpatient_linear_se <- sapply(1:nrow(preds), function(i) {
    ratio_i <- preds[i, "Y_mean"]
    ratio_se_i <- preds[i, "inpatient_se"]
    deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
  })
  preds$inpatient_ratio <- inv.logit(preds$Y_mean)
  preds$inpatient_logit <- preds$Y_mean

### Rejoin inpatient ratio with clinical data  ###
  clin_df$age_mid <- ceiling((clin_df$age_end + clin_df$age_start)/2)
  clin_df$year_id <- round((clin_df$year_start + clin_df$year_end)/2,0)
  clin_df <- join(clin_df, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
  clin_df$sdi <- round(clin_df$sdi, 2)

  clin_df <- cbind(clin_df, preds[1,c("inpatient_ratio","inpatient_linear_se","inpatient_logit","inpatient_se")])

## Want to see the adjusted age pattern
  adf <- clin_df[,c("mean","age_start","age_end","inpatient_logit","location_id")]
    adf_median <- median(adf$mean)
  bdf <- adf
    bdf_median <- median(bdf$mean)
  cdf <- non_clin_df[,c("mean","age_start","age_end","location_id")]
    cdf_median <- median(cdf$mean)
  cdf$age_start <- round(cdf$age_start, 0)
  adf$type <- "Unadjusted"
  bdf$type <- "Adjusted"
  cdf$type <- "Non-clinical"
  bdf$mean <- inv.logit(logit(bdf$mean) + bdf$inpatient_logit)

  ddf <- data.frame(type="Filler", age_start=seq(0,98,2), age_end=seq(1,99,2), mean=0.01, location_id=1)

##########################################################################
######################## Claims  #########################################
##########################################################################
  cv_marketscan$match_id <- paste0(cv_marketscan$nid,"_",cv_marketscan$n_nid)
  cv_marketscan$study_id <- paste0(cv_marketscan$location_match,"_",cv_marketscan$age_bin)
  cv_marketscan$age_mid <- floor((cv_marketscan$age_end + cv_marketscan$age_start)/2)

  # ## Run an MR-BRT model. ##
  fit1 <- run_mr_brt(
    output_dir = paste0("filepath"),
    model_label = "cv_marketscan",
    data = cv_marketscan, #[cv_marketscan$ratio>1,],
    mean_var = "logit_ratio",
    se_var = "logit_ratio_se",
    covs = list(cov_info("age_mid","X",
                         degree = 2, n_i_knots = 3,
                         l_linear=FALSE, r_linear = TRUE, bspline_gprior_mean = "0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf")),
    #i_knots=c("2,5,20,60"))), #, bspline_cvcv = "concave")),
    overwrite_previous = TRUE,
    study_id = "study_id",
    method = "trim_maxL",
    trim_pct = 0.1
  )
    check_for_outputs(fit1)
    df_pred <- data.frame(intercept=1, age_mid=seq(0,100,1))
    pred1 <- predict_mr_brt(fit1, newdata = df_pred)
    check_for_preds(pred1)
    pred_object <- load_mr_brt_preds(pred1)
    preds <- pred_object$model_summaries
    preds$age_mid <- preds$X_age_mid

    mod_data <- fit1$train_data
    mod_data$outlier <- floor(abs(mod_data$w - 1))
    ggplot(preds, aes(x=X_age_mid, y=exp(Y_mean))) + geom_line() +
      geom_ribbon(aes(ymin=exp(Y_mean_lo), ymax=exp(Y_mean_hi)), alpha=0.3) +
      geom_point(data=mod_data, aes(x=age_mid, y=exp(log_ratio), size=1/se^2, col=factor(outlier))) + guides(size=F) + scale_color_manual(values=c("black","red")) + guides(col=F) +
      geom_hline(yintercept=1, lty=2, col="red") + scale_y_continuous("Log ratio") + scale_x_continuous("Age", limits=c(0,100)) + theme_bw() + ggtitle("Claims ratio")

  # Calculate log se
    preds$marketscan_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
  # # Convert the mean and standard_error to linear space
    preds$marketscan_linear_se <- sapply(1:nrow(preds), function(i) {
      ratio_i <- preds[i, "Y_mean"]
      ratio_se_i <- preds[i, "marketscan_se"]
      deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
    })
    preds$marketscan_ratio <- inv.logit(preds$Y_mean)
    preds$marketscan_logit <- preds$Y_mean

  ### Rejoin marketscan ratio with clinical data  ###
    clin_df <- join(clin_df, preds[,c("age_mid","marketscan_ratio","marketscan_linear_se","marketscan_logit","marketscan_se")], by="age_mid")

  # Keep a record of the values
    p <- preds[,c("X_cv_inpatient","X_cv_marketscan","Y_mean","Y_mean_lo","Y_mean_hi")]
    p <- unique(p)
    p$variable <- c("cv_marketscan","cv_inpatient")
    p$count_obs <- c(length(cv_inpatient$ratio), length(cv_marketscan$ratio) + length(cv_ms_inp$ratio))
    output_vals <- rbind.fill(output_vals, p)

#################################################################################
  ## Perform the crosswalks for the clinical data ##
#################################################################################
  clin_df$logit_mean <- logit(clin_df$mean)
  clin_df$logit_se <- sapply(1:nrow(clin_df), function(i) {
      ratio_i <- clin_df[i, "mean"]
      ratio_se_i <- clin_df[i, "standard_error"]
      deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
  })

  # Taiwan data processing
    clin_df$cv_marketscan <- ifelse(clin_df$location_name == "Taiwan", 0, clin_df$cv_marketscan)
    clin_df$mean <- ifelse(clin_df$cv_inpatient==1, inv.logit(clin_df$logit_mean + clin_df$inpatient_logit),
                           ifelse(clin_df$cv_marketscan==1, inv.logit(clin_df$logit_mean + clin_df$marketscan_logit), clin_df$mean))

  std_inpatient <- sqrt(with(clin_df, standard_error^2 * inpatient_linear_se^2 + standard_error^2*inpatient_ratio^2 + inpatient_linear_se^2*mean^2))
  std_marketscan <- sqrt(with(clin_df, standard_error^2 * marketscan_linear_se^2 + standard_error^2*marketscan_ratio^2 + marketscan_linear_se^2*mean^2))

  clin_df$standard_error <- ifelse(clin_df$cv_inpatient == 1 , std_inpatient, ifelse(clin_df$cv_marketscan == 1, std_marketscan, clin_df$standard_error))

## Rbind back with full dataset ##
  diarrhea <- rbind.fill(clin_df, non_clin_df)

##-----------------------------------------------------------------------------
# Recoding Clinical Informatics
# inpatient data to be 'cv_inpatient = 1' and identifying that by extractor.
# I will leave a cv_miscoded = 1 if cv_inpatient = 1 or cv_inpatient_sample = 1
# but the extractor isn't some specific users.
diarrhea$cv_inpatient_lit <- with(diarrhea, ifelse(cv_clin_data==1,0,ifelse(cv_inpatient==1,1,ifelse(cv_inpatient_sample==1,1,0))))
  diarrhea$cv_inpatient_sample <- 0

  diarrhea$is_reference <- ifelse(diarrhea$cv_hospital==0 & diarrhea$cv_clin_data==0 & diarrhea$cv_inpatient_lit==0,1,0)

## Save that for use elsewhere ##
  write.csv(diarrhea, "filepath", row.names=F)


#######################################################################################################################
## Now crosswalk for the literature indications (hospitalized, clinical data) ##
  diarrhea <- read.csv("filepath")

  ## Set some parameters ##
  age_bins <- c(0,1,2,3,4,5,20,40,60,80,100)

## Subset to working data frame ##
  df <- diarrhea[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
                    "cv_inpatient_lit","cv_hospital","cv_inpatient","is_reference","group_review","is_outlier", "gbd_round")]
  df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")

######################################################################
## Hospital data crosswalk. This is for data that report the incidence
## of severe or hospitalized diarrhea. There are plenty of sources that
## report the incidence of both clinician-diagnosed (reference) and
## hospitalized diarrhea so we are going to use within study-age-location
## matching for preparing these data for the meta-regression.
######################################################################
  cv_hospital <- bundle_crosswalk_collapse(df[df$gbd_round != 2019, ], 
                                           covariate_name="cv_hospital", 
                                           age_cut=age_bins, 
                                           year_cut=c(seq(1980,2015,5),2019), 
                                           merge_type="within", 
                                           location_match="exact", 
                                           include_logit=T)

  cv_hospital$year_id <- floor((cv_hospital$year_end + cv_hospital$year_start)/2)
  cv_hospital$location_id <- cv_hospital$location_match
  cv_hospital$age_mid <- floor((cv_hospital$age_end + cv_hospital$age_start)/2)

  # SDI isn't a significant predictor
  cv_hospital <- join(cv_hospital, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
  cv_hospital <- join(cv_hospital, locs[,c("location_id","super_region_name","location_name","ihme_loc_id")], by="location_id")

  # Values here should not be greater than 1 (inpatient incidence cannot be greater than population incidence),
  cv_hospital <- subset(cv_hospital, ratio > 1)
  cv_hospital$log_ratio <- cv_hospital$log_ratio - 1
  write.csv(cv_hospital, "filepath", row.names=F)

  # Test a couple linear models for predictors
  summary(lm(log_ratio ~ age_mid, data=cv_hospital))
  summary(lm(log_ratio ~ sdi, data=cv_hospital))

  fit2 <- run_mr_brt(
    output_dir = "filepath",
    model_label = "cv_hospital",
    data = cv_hospital[cv_hospital$ratio < 20, ],
    mean_var = "log_ratio",
    se_var = "delta_log_se",
    overwrite_previous = TRUE,
    study_id = "nid",
    method = "trim_maxL",
    trim_pct = 0.1
  )

  check_for_outputs(fit2)
  df_pred <- data.frame(expand.grid(intercept = 1))
  pred1 <- predict_mr_brt(fit2, newdata = df_pred)
  check_for_preds(pred1)
  pred_object <- load_mr_brt_preds(pred1)
  preds <- pred_object$model_summaries
  mod_data <- fit2$train_data

# Calculate log se
    preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# Convert the mean and standard_error to linear space
    preds$linear_se <- deltamethod(~exp(x1), preds$Y_mean, preds$se^2)
    preds$ratio <- exp(preds$Y_mean) + 1

# Pull in DisMod 2017 values
  dismod <- data.frame(mean=1/0.18, lower=1/0.16, upper=1/0.20)

## produce approximation of a funnel plot
  mod_data$outlier <- abs(mod_data$w - 1)
  f1 <- ggplot(mod_data, aes(x=log_ratio, y=delta_log_se, col=factor(outlier))) + geom_point(size=3) + geom_vline(xintercept=preds$Y_mean) + scale_y_reverse("Standard error") +
    theme_bw() + scale_x_continuous("Log ratio") + scale_color_manual("", labels=c("Used","Trimmed"), values=c("#0066CC","#CC0000")) + ggtitle(paste0("Diarrhea", " Hospital ratio")) +
    geom_vline(xintercept=preds$Y_mean_lo, lty=2) + geom_vline(xintercept=preds$Y_mean_hi, lty=2)
  print(f1)

## Create essentially a forest plot
  mod_data$label <- with(mod_data, paste0(nid,"_",ihme_loc_id,"_",age_start,"-",age_end,"_",year_start,"-",year_end,"_",sex))
  f3 <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=exp(log_ratio + delta_log_se*1.96)+1, ymin=exp(log_ratio - delta_log_se*1.96)+1)) + geom_point(aes(y=exp(log_ratio)+1, x=label)) +
    geom_errorbar(aes(x=label), width=0) + scale_y_continuous("Linear ratio", limits=c(0,10)) +
    theme_bw() + xlab("") + coord_flip() + ggtitle(paste0("Diarrhea hospital ratio (",round(preds$ratio,3),")")) +
    geom_hline(yintercept=1) + geom_hline(yintercept=preds$ratio, col="purple") +
    geom_rect(data=preds, aes(ymin=exp(Y_mean_lo)+1, ymax=exp(Y_mean_hi)+1, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
  print(f3)

# Calculate log se
  preds$log_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# Convert the mean and standard_error to linear space
  preds$hospital_linear_se <- sapply(1:nrow(preds), function(i) {
    ratio_i <- preds[i, "Y_mean"]
    ratio_se_i <- preds[i, "log_se"]
    deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
  })
  preds$hospital_ratio <- exp(preds$Y_mean) + 1
# rename
  preds$log_hospital_ratio <- preds$Y_mean
  preds$log_hospital_se <- preds$log_se

  # Keep a record of the values
    p <- preds[,c("hospital_ratio","hospital_linear_se","Y_mean","Y_mean_lo","Y_mean_hi")]
    p <- unique(p)
    p$count_obs <- c(length(cv_hospital$ratio))

  output_vals <- rbind.fill(output_vals, p)

# Join back with master data_frame
  diarrhea$hospital_ratio <- preds$hospital_ratio
  diarrhea$hospital_linear_se <- preds$hospital_linear_se
  diarrhea$log_hospital_ratio <- preds$log_hospital_ratio
  diarrhea$log_hospital_se <- preds$log_hospital_se

######################################################################
## Scientific literature inpatient crosswalk.
## This is for data that report the incidence of hospitalized diarrhea.
######################################################################
  cv_inp_lit <- bundle_crosswalk_collapse(df[df$gbd_round != 2019, ], 
                                          covariate_name="cv_inpatient_lit", 
                                          age_cut=age_bins, 
                                          year_cut=c(seq(1980,2015,5),2019), 
                                          merge_type="between", 
                                          location_match="exact")

  cv_inp_lit$year_id <- floor((cv_inp_lit$year_end + cv_inp_lit$year_start)/2)
  cv_inp_lit$location_id <- cv_inp_lit$location_match
  cv_inp_lit$age_mid <- floor((cv_inp_lit$age_end + cv_inp_lit$age_start)/2)

  cv_inp_lit <- join(cv_inp_lit, locs[,c("location_id","super_region_name","location_name","ihme_loc_id")], by="location_id")
  cv_inp_lit$high_income <- ifelse(cv_inp_lit$super_region_name=="High-income",1,0)

  # Values here should not be greater than 1 (inpatient incidence cannot be greater than population incidence),
  cv_inp_lit <- subset(cv_inp_lit, ratio > 1)
  cv_inp_lit$log_ratio <- cv_inp_lit$log_ratio - 1
  write.csv(cv_inp_lit, "filepath", row.names=F)

  fit1 <- run_mr_brt(
    output_dir = "filepath",
    model_label = "cv_inp_lit",
    data = cv_inp_lit,
    mean_var = "log_ratio",
    se_var = "delta_log_se",
    overwrite_previous = TRUE,
    method = "trim_maxL",
    trim_pct = 0.1
  )

  check_for_outputs(fit1)
  df_pred <- data.frame(expand.grid(intercept = 1))
  pred1 <- predict_mr_brt(fit1, newdata = df_pred)
  check_for_preds(pred1)
  pred_object <- load_mr_brt_preds(pred1)
  preds <- pred_object$model_summaries
  mod_data <- fit1$train_data

  # Calculate log se
  preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
  # Convert the mean and standard_error to linear space
  preds$linear_se <- deltamethod(~exp(x1) + 1, preds$Y_mean, preds$se^2)
  preds$ratio <- exp(preds$Y_mean) + 1

  # Keep a record of the values
    p <- preds[,c("ratio","linear_se","Y_mean","Y_mean_lo","Y_mean_hi")]
    p <- unique(p)
    p$count_obs <- c(length(cv_inp_lit$ratio))

    output_vals <- rbind.fill(output_vals, p)

  diarrhea$inpatient_lit_ratio <- preds$ratio
  diarrhea$inpatient_lit_linear_se <- preds$linear_se
  diarrhea$log_inpatient_lit_ratio <- preds$Y_mean
  diarrhea$log_inpatient_lit_se <- preds$se

  ## Create essentially a forest plot
  mod_data$outlier <- abs(mod_data$w - 1)
  mod_data$location_id <- mod_data$location_match
  mod_data <- join(mod_data, locs[,c("location_id","location_name","ihme_loc_id")], by="location_id")
  mod_data$label <- with(mod_data, paste0(ihme_loc_id,"_",age_start,"-",age_end,"_",year_start,"-",year_end))
  f4 <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=exp(log_ratio + delta_log_se*1.96)+1, ymin=exp(log_ratio - delta_log_se*1.96)+1)) + geom_point(aes(y=exp(log_ratio)+1, x=label)) +
    geom_errorbar(aes(x=label), width=0) +
    theme_bw() + ylab("Linear ratio") + xlab("") + coord_flip() + ggtitle(paste0("Diarrhea inpatient literature ratio (",round(preds$ratio,3),")")) +
    geom_hline(yintercept=1) + geom_hline(yintercept=preds$ratio, col="purple") +
    geom_rect(data=preds, aes(ymin=exp(Y_mean_lo)+1, ymax=exp(Y_mean_hi)+1, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
  print(f4)

################################################################################
## Perform actual crosswalk, save results for upload ##
################################################################################
  diarrhea$raw_mean <- diarrhea$mean
  diarrhea$raw_standard_error <- diarrhea$standard_error

  ## calculate the adjusted standard_errors outside of ifelse statement
  std_hospital <- sqrt(with(diarrhea, standard_error^2 * hospital_linear_se^2 + standard_error^2*hospital_ratio^2 + hospital_linear_se^2*mean))
  std_inp_lit <- sqrt(with(diarrhea, standard_error^2 * inpatient_lit_linear_se^2 + standard_error^2*inpatient_lit_ratio^2 + inpatient_lit_linear_se^2*mean))

#####################################
## Convert the mean by the crosswalk!
# Hospital
  diarrhea$cv_hospital[is.na(diarrhea$cv_hospital)] <- 0
  diarrhea$mean <- ifelse(diarrhea$cv_hospital==1, diarrhea$mean * diarrhea$hospital_ratio, diarrhea$mean)
  diarrhea$standard_error <- ifelse(diarrhea$cv_hospital==1, std_hospital, diarrhea$standard_error)
  diarrhea$note_modeler <- ifelse(diarrhea$cv_hospital==1, paste0("This data point was adjusted for being in a clinical sample population. The code to produce
                                                                  this estimated ratio is from diarrhea_crosswalk_hospital_mr-brt.R. This occurred in MR-BRT
                                                                  during decomposition step 2. The original mean was ", round(diarrhea$raw_mean,3),". ", diarrhea$note_modeler), diarrhea$note_modeler)

# Inpatient from literature
  diarrhea$cv_inpatient_lit[is.na(diarrhea$cv_inpatient_lit)] <- 0
  diarrhea$mean <- ifelse(diarrhea$cv_inpatient_lit==1, diarrhea$mean * diarrhea$inpatient_lit_ratio, diarrhea$mean)
  diarrhea$standard_error <- ifelse(diarrhea$cv_inpatient_lit==1, std_inp_lit, diarrhea$standard_error)
  diarrhea$note_modeler <- ifelse(diarrhea$cv_inpatient_lit==1, paste0("This data point was adjusted for being in a hospitalized inpatient sample population from the literature. The code to produce
                                                                       this estimated ratio is from diarrhea_crosswalk_hospital_mr-brt.R. This occurred in MR-BRT
                                                                       during decomposition step 2. The original mean was ", round(diarrhea$raw_mean,3),". ", diarrhea$note_modeler), diarrhea$note_modeler)

diarrhea$crosswalk_type <- ifelse(diarrhea$cv_hospital==1, "Hospitalized", ifelse(diarrhea$cv_inpatient_lit==1,"Inpatient from Literature",
                                                                                  ifelse(diarrhea$cv_marketscan==1,"Claims",ifelse(diarrhea$cv_inpatient==1,"Inpatient","No crosswalk"))))
diarrhea$crosswalk_type[is.na(diarrhea$crosswalk_type)] <- "No crosswalk"

ggplot(diarrhea, aes(x=mean_original, y=mean, col=crosswalk_type)) + geom_point() + scale_y_log10(limits=c(0.000001,1)) + scale_x_log10(limits=c(0.000001,1))

## Save out crosswalk values ##
xwvals <- data.frame(cv_inpatient_lit = unique(diarrhea$inpatient_lit_ratio), cv_inpatient_lit_se = unique(diarrhea$inpatient_lit_linear_se),
                     cv_hospital = unique(diarrhea$hospital_ratio), cv_hospital_se = unique(diarrhea$hospital_linear_se),
                     cv_inpatient = unique(diarrhea$inpatient_ratio), cv_marketscan = unique(diarrhea$marketscan_ratio))
xwvals <- rbind.fill(xwvals, output_vals)
write.csv(xwvals, "filepath", row.names=F)

## Data cleaning ##
# This NID had some duplicate rows group_reviewed, undo that.
  diarrhea$group_review <- ifelse(diarrhea$nid==267492, "", diarrhea$group_review)
  diarrhea <- subset(diarrhea, !is.na(mean))
  diarrhea$mean <- as.numeric(diarrhea$mean)
  diarrhea$cases <- diarrhea$mean * diarrhea$sample_size


## Both sex data should be duplicated ##
source("/filepath/sex_split_mrbrt_weights.R")
  diarrhea$seq_parent <- ""
  diarrhea$crosswalk_parent_seq <- ""

  sex_df <- duplicate_sex_rows(diarrhea)

  sex_df$mean <- ifelse(is.na(sex_df$mean), sex_df$cases / sex_df$sample_size, sex_df$mean)
  sex_df$group_review[is.na(sex_df$group_review)] <- ""
  sex_df <- subset(sex_df, group_review != 0)
  sex_df[is.na(sex_df)] <- ""
  sex_df$standard_error <- as.numeric(sex_df$standard_error)
  sex_df$group_review <- ifelse(sex_df$specificity=="","",1)
  sex_df$uncertainty_type <- ifelse(sex_df$lower=="","", as.character(sex_df$uncertainty_type))
  sex_df$uncertainty_type_value <- ifelse(sex_df$lower=="","",sex_df$uncertainty_type_value)

  sex_df <- subset(sex_df, !is.na(standard_error))

  sex_df <- sex_df[, -which(names(sex_df) %in% c("cv_dhs","cv_whs","cv_nine_plus_test","cv_explicit_test","duration","duration_lower","duration_upper",
                                                     "cv_clin_data","unnamed..75","unnamed..75.1","survey","original_mean","cv_diag_selfreport","is_reference",
                                                     "age_mid","ratio","linear_se","std_clinical","cv_inpatient_lit","hospital_ratio","hospital_linear_se",
                                                     "log_hospital_ratio","log_hospital_se","inpatient_lit_ratio","cv_miscoded","inpatient_lit_linear_se","log_inpatient_lit_ratio",
                                                     "log_inpatient_lit_se", "raw_mean","raw_standard_error","crosswalk_type","parent_id","cv_hospital_child","level",
                                                     "cv_low_income_hosp","incidence_corrected", "cv_marketscan_inp_2000","cv_marketscan_all_2000"))]
  # Subset if data are greater than 1
    sex_df <- subset(sex_df, mean < 1)
  # Make sure cases < sample size
    sex_df$sample_size <- as.numeric(sex_df$sample_size)
    sex_df$cases <- as.numeric(sex_df$cases)
    sex_df$cases <- ifelse(sex_df$cases > sex_df$sample_size, sex_df$sample_size * 0.9, sex_df$cases)

## Opportunity to quickly look for outliers ##
  source("/filepath/map_dismod_input_function.R")
  source("/filepath/input_data_scatter_sdi_function.R")
  library(scales)
  sex_df <- join(sex_df, locs[,c("location_id","parent_id","level","region_name","super_region_name")], by="location_id")

  pdf("/filepath/potential_epi_outlier_boxplots.pdf", height=8, width=12)
    map_dismod_input(sex_df, type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Unadjusted data")
    map_dismod_input(sex_df[sex_df$cv_inpatient==1,], type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Clinical inpatient")
    map_dismod_input(sex_df[sex_df$cv_diag_selfreport==1,], type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Survey data")

    input_scatter_sdi(sex_df, error = (-100), title = "All data")
    input_scatter_sdi(sex_df[sex_df$cv_inpatient==1,], error = (-0.75), title = "Inpatient data")
    input_scatter_sdi(sex_df[sex_df$cv_diag_selfreport==1,], error = (-0.75), title = "Survey data")

    for(s in unique(sex_df$region_name)){
      p <- ggplot(subset(sex_df, level ==3 & region_name==s & is_outlier == 0), aes(x=location_name, y=mean)) + geom_boxplot() + 
        geom_point(alpha=0.4) + scale_y_log10(label=percent) + ggtitle(s) + theme_bw() +
        geom_hline(yintercept = median(sex_df$mean[sex_df$level==3 & sex_df$region_name==s]), col="red") +
        geom_hline(yintercept = quantile(sex_df$mean[sex_df$level==3 & sex_df$region_name==s], 0.25), col="red", lty=2) +
        geom_hline(yintercept = quantile(sex_df$mean[sex_df$level==3 & sex_df$region_name==s], 0.75), col="red", lty=2)
      print(p)
    }
    for(l in unique(sex_df$parent_id[sex_df$level==4])){
      p <- ggplot(subset(sex_df, level==4 & parent_id==l & is_outlier==0), aes(x=location_name, y=mean)) + geom_boxplot() + 
        geom_point(alpha=0.4) + scale_y_log10(label=percent) + theme_bw() +
        theme(axis.text.x=element_text(angle=90, hjust=1)) + 
        geom_hline(yintercept = median(sex_df$mean[sex_df$level==4 & sex_df$parent_id==l]), col="red") +
        geom_hline(yintercept = quantile(sex_df$mean[sex_df$level==4 & sex_df$parent_id==l], 0.25), col="red", lty=2) +
        geom_hline(yintercept = quantile(sex_df$mean[sex_df$level==4 & sex_df$parent_id==l], 0.75), col="red", lty=2)
      print(p)
    }
  dev.off()

## Join with SDI for some plotting
  sex_df$year_id <- floor((sex_df$year_end + sex_df$year_start)/2)
  sex_df <- join(sex_df, sdi[,c("sdi","year_id","location_id")], by=c("location_id","year_id"))

## Mark outliers
  # This study has an incompatable case definition
  drop_nids <- c(220487)

  # Create an outlier table
    outlier_table <- data.frame(drop_nids, data_outliered=c("all"), reason_outliered=c("Case definition"))
    write.csv(outlier_table, "filepath", row.names=F)

  sex_df$is_outlier <- ifelse(sex_df$nid %in% drop_nids, 1, sex_df$is_outlier)

## Outlier Taiwan claims
  sex_df$is_outlier <- ifelse(sex_df$location_name=="Taiwan" & sex_df$clinical_data_type=="claims", 1, sex_df$is_outlier)

## Fix years within this survey
  sex_df$year_end <- ifelse(sex_df$nid==19950, 1999, sex_df$year_end)

## Remove values with prevalence SE greater than 1
  sex_df <- subset(sex_df, standard_error < 1)

## Fill out step2_location_year
  sex_df$step2_location_year <- "These data may have been changed in the data processing"

#### Outlier extreme values (Incidence prior in DisMod is 10, which is roughly 11.8% prev) ##
  sex_df$is_outlier <- ifelse(log(sex_df$mean) < (-10), 1, ifelse(sex_df$mean > 0.10, 1, sex_df$is_outlier))

  ggplot(sex_df, aes(x=sdi, y=mean, col=factor(is_outlier))) + geom_point() + scale_y_log10() + scale_color_manual(values=c("black","red")) +
    geom_hline(yintercept=0.05, col="purple")

## Outlier inpatient clinical data with very high values in young children ##
  sex_df$sdi[is.na(sex_df$sdi)] <- 0.5
  child_outliers <- sex_df
  uq_ref_5 <- quantile(sex_df$mean[sex_df$age_end < 5 & sex_df$cv_inpatient==0 & sex_df$cv_marketscan==0], 0.75, na.rm=T)
  med_ref_5 <- quantile(sex_df$mean[sex_df$age_end < 5 & sex_df$cv_inpatient==0 & sex_df$cv_marketscan==0], 0.5, na.rm=T)
  u90_ref_5 <- quantile(sex_df$mean[sex_df$age_end < 5 & sex_df$cv_inpatient==0 & sex_df$cv_marketscan==0], 0.9, na.rm=T)

  child_outliers$is_outlier <- ifelse(child_outliers$age_end < 5 & child_outliers$mean > uq_ref_5 & child_outliers$sdi > 0.75, 1, child_outliers$is_outlier)
  child_outliers$is_outlier <- ifelse(child_outliers$age_start == 5 & child_outliers$age_end == 9 & child_outliers$mean > med_ref_5, 1, child_outliers$is_outlier)
  child_outliers$is_outlier <- ifelse(child_outliers$age_end < 1 & child_outliers$mean > u90_ref_5, 1, child_outliers$is_outlier)

  ggplot(child_outliers, aes(x=sdi, y=mean, col=factor(is_outlier))) + geom_point() + scale_y_log10() + scale_color_manual(values=c("black","red")) +
    geom_hline(yintercept=uq_ref_5, col="purple", lty=2)
  ggplot(child_outliers[child_outliers$location_id==4755,], aes(x=age_start, y=mean, col=factor(is_outlier))) + geom_point() + scale_color_manual(values=c("black","red")) +
    geom_hline(yintercept=uq_ref_5, col="purple", lty=2)
##


##------------------------------------------------------------------##
## Save!
 write.csv(sex_df, "filepath", row.names=F)

 # Save a version without SDI outliering
  write.xlsx(sex_df[sex_df$gbd_2019_new != 1, ], "filepath", sheetName="extraction")
 # Save a version with high U5 outliered (greater than 75th% of the reference data)
  write.xlsx(child_outliers[child_outliers$gbd_2019_new != 1, ], "filepath", sheetName="extraction")

 # Save only new data (using step 3 best dataset)
  write.xlsx(child_outliers[child_outliers$gbd_2019_new == 1,], "filepath", sheetName="extraction")

##--------------------------------------------------------------------------------
  ## Upload!
source("/filepath/save_crosswalk_version.R")
  bundle_version_id <- 10772
  save_crosswalk_version(bundle_version_id=bundle_version_id, data_filepath="filepath",
                         description= "SDI and Age xw for inpatient, extreme value outliers")
  save_crosswalk_version(bundle_version_id=bundle_version_id, data_filepath="filepath",
                         description= "SDI and age xw for inpatient, aggressive outliers children U5 based on reference medians, SDI.")

## Upload for step 4
  save_crosswalk_version(bundle_version_id = step4_bvid, data_filepath = "filepath",
                         description = "Step 4! Single xw for inpatient, age for claims, aggressive U5 outliering")
