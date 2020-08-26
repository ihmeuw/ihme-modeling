###########################################################################
## This file is intended to house all LRI crosswalks.
## First, it takes active survey data in bundle, replaces with
## the raw, un-crosswalked survey data. This has been done already (5-24-19).
## Second, it takes the survey data from GBD 2019 step 1 and performs
## a series of crosswalks for the case definition in the surveys.
## Third, it calculates crosswalk values for literature data that are
## from hospitalized populations and for self-reported data (typically surveys).
## Fourth, it calculates a single crosswalk for all clinical data (mix of inpatient
## and claims data).
## Lastly, it saves a bundle_version and uploads as a crosswalk_version.
###########################################################################

library(plyr)
library(openxlsx)
source() # filepaths for uploading and downloading functions
source("filepath/plot_mr_brt_function.R")
source("filepath/run_mr_brt_function.R")
source("filepath/bundle_crosswalk_collapse.R")
source("filepath/convert_inc_prev_function.R")
library(plyr)
library(ggplot2)
library(metafor)
library(msm)
library(lme4)
library(scales)
locs <- read.csv('filepath')

xwvals <- data.frame()

#################################################################################
## get the new data for Step 4 ##
#################################################################################

## First, upload new survey data ##
  # upload_bundle_data(bundle_id = 19, decomp_step = "step4", filepath="filepath")
  # save_bundle_version(bundle_id = 19, decomp_step = "step4", include_clinical = T)

  step4_data <- get_bundle_version(bundle_version_id = 13226)
  step4_data$gbd_round <- 2019
  step4_data$gbd_2019_new <- 1

## Now rbind to the existing data! ##
  step2_data <- read.csv("filepath")
  step2_data$gbd_2019_new <- 0

  full_data <- rbind.fill(step2_data, step4_data)

  write.csv(full_data, "filepath", row.names=F)

###################################################################
## The purpose of this part of the code is to generate data adjustments
## (cross-walks) for the various survey definitions of symptoms
## related to LRI. For GBD 2019 we will
## run these regressions in MR-BRT as a network meta-analysis.
## This file should be run after 03_lri_convert_survey_period-point.R
## and note that the input data "tabulated_survey_2017_best_definition.csv"
## is a custom file that was created manually in Excel. ##
####################################################################
  ## Generate crosswalks using surveys that provide good data only ##
  ## Explore using only good data ##
  ## This file is created manually! It was done by taking "tabulated_survey_point_2017.csv"
  ## and subsetting such that only surveys with the most specific case definition
  ## (cough with difficulty breathing with symptoms in the chest and fever) were included.
  ## Those rows are then resaved as this file. ##
  df <- read.csv("filepath")

  df$location_name <- df$location
  df <- join(df, locs, by="location_name")

  ## Define ratios of prevalence in various definitions
    df$chest_fever_chest <- df$chest_fever / df$chest_symptoms
    df$chest_fever_diff_fever <- df$chest_fever / df$diff_fever
    df$chest_fever_diff <- df$chest_fever / df$diff_breathing
    df$diff_fever_diff <- df$diff_fever / df$diff_breathing
    df$chest_diff <- df$chest_symptoms / df$diff_breathing

  ## Define standard errors for each symptom
  for(v in c("chest_fever","chest_symptoms","diff_fever","diff_breathing")){
    prev <- df[,v]
    se <- sqrt(prev * (1-prev) / df$sample_size)
    df[,paste0(v,"_std")] <- se
  }

  ## Define standard error for each ratio
    df$chest_fever_chest_se <- sqrt(df$chest_fever^2 / df$chest_symptoms^2 * (df$chest_fever_std^2/df$chest_symptoms^2 + df$chest_symptoms_std^2/df$chest_fever^2))
    df$chest_fever_diff_fever_se <- sqrt(df$chest_fever^2 / df$diff_fever^2 * (df$chest_fever_std^2/df$diff_fever^2 + df$diff_fever_std^2/df$chest_fever^2))
    df$chest_fever_diff_se <- sqrt(df$chest_fever^2 / df$diff_breathing^2 * (df$chest_fever_std^2/df$diff_breathing^2 + df$diff_breathing_std^2/df$chest_fever^2))
    df$chest_diff_se <- sqrt(df$chest_symptoms^2 / df$diff_breathing^2 * (df$chest_symptoms_std^2/df$diff_breathing^2 + df$diff_breathing_std^2/df$chest_symptoms^2))

  ## Okay, so the network meta-analysis must be long (currently wide)
  ## and must have dummies for each of the definitions that are crosswalked.
    df_chest_chest <- df[,c("age_year","nid","end_year","location_id","chest_fever_chest","chest_fever_chest_se")]
    df_chest_diff_fever <- df[,c("age_year","nid","end_year","location_id","chest_fever_diff_fever","chest_fever_diff_fever_se")]
    df_chest_diff <- df[,c("age_year","nid","end_year","location_id","chest_fever_diff","chest_fever_diff_se")]

  # rename
    colnames(df_chest_chest)[5:6] <- c("ratio","standard_error")
    colnames(df_chest_diff_fever)[5:6] <- c("ratio","standard_error")
    colnames(df_chest_diff)[5:6] <- c("ratio","standard_error")

  # set dummy indicator
    df_chest_chest$indictor <- "chest"
    df_chest_diff_fever$indictor <- "diff_fever"
    df_chest_diff$indictor <- "diff"

    cv_survey <- rbind(df_chest_chest, df_chest_diff_fever, df_chest_diff)

  # Create dummies for network analysis
    cv_survey$cv_chest <- ifelse(cv_survey$indictor=="chest",1,0)
    cv_survey$cv_diff_fever <- ifelse(cv_survey$indictor=="diff_fever",1,0)
    cv_survey$cv_diff <- ifelse(cv_survey$indictor=="diff",1,0)

  # remove zeros and NAs
    cv_survey <- subset(cv_survey, ratio>0)
    cv_survey <- subset(cv_survey, standard_error!="NaN")

  # Now we need these ratios in log space
    cv_survey$log_ratio <- log(cv_survey$ratio)
    # This is from Reed Sorensen
    cv_survey$log_se <- sapply(1:nrow(cv_survey), function(i) {
      ratio_i <- cv_survey[i, "ratio"]
      ratio_se_i <- cv_survey[i, "standard_error"]
      deltamethod(~log(x1), ratio_i, ratio_se_i^2)
    })

  # Save
    write.csv(cv_survey, paste0("filepath"), row.names=F)

  # Now run in MR-BRT
    cov_names <- c("cv_chest","cv_diff_fever","cv_diff")
    covs1 <- list()
    for (nm in cov_names) covs1 <- append(covs1, list(cov_info(nm, "X")))

  fit2 <- run_mr_brt(
    output_dir = paste0("filepath"),
    model_label = "survey_chest",
    data = cv_survey[cv_survey$cv_chest==1,], # You can also save this as a CSV and have this read it.
    mean_var = "log_ratio",
    se_var = "log_se",
    #covs = list(cov_info("age_year","X")),
    overwrite_previous = TRUE,
    study_id = "nid",
    method = "trim_maxL",
    trim_pct = 0.1
  )
  fit3 <- run_mr_brt(
    output_dir = paste0("filepath"),
    model_label = "survey_diff_fever",
    data = cv_survey[cv_survey$cv_diff_fever==1,], 
    mean_var = "log_ratio",
    se_var = "log_se",
    #covs = list(cov_info("age_year","X")),
    overwrite_previous = TRUE,
    study_id = "nid",
    method = "trim_maxL",
    trim_pct = 0.1
  )
  fit4 <- run_mr_brt(
    ooutput_dir = paste0("filepath"),
    model_label = "survey_diff",
    data = cv_survey[cv_survey$cv_diff==1,], 
    mean_var = "log_ratio",
    se_var = "log_se",
    #covs = list(cov_info("age_year","X")),
    overwrite_previous = TRUE,
    study_id = "nid",
    method = "trim_maxL",
    trim_pct = 0.1
  )

  df_pred <- data.frame(intercept=1, age_year = c(0,1,2,3,4))
  # Chest
    pred2 <- predict_mr_brt(fit2, newdata = df_pred)
    check_for_preds(pred2)
    pred_object <- load_mr_brt_preds(pred2)
    preds_chest <- pred_object$model_summaries
  # Diff + fever
    pred3 <- predict_mr_brt(fit3, newdata = df_pred)
    check_for_preds(pred3)
    pred_object <- load_mr_brt_preds(pred3)
    preds_diff_fever <- pred_object$model_summaries
  # Just diff
    pred4 <- predict_mr_brt(fit4, newdata = df_pred)
    check_for_preds(pred4)
    pred_object <- load_mr_brt_preds(pred4)
    preds_diff <- pred_object$model_summaries


  ## Collect outputs
    survey_outputs <- data.frame(age_start = c(0,1,2,3,4),
                                 log_chest_ratio = preds_chest$Y_mean,
                                 log_diff_fever_ratio = preds_diff_fever$Y_mean,
                                 log_diff_ratio = preds_diff$Y_mean,
                                 log_chest_se = (preds_chest$Y_mean_hi - preds_chest$Y_mean_lo)/2/qnorm(0.975),
                                 log_diff_fever_se = (preds_diff_fever$Y_mean_hi - preds_diff_fever$Y_mean_lo)/2/qnorm(0.975),
                                 log_diff_se = (preds_diff$Y_mean_hi - preds_diff$Y_mean_lo)/2/qnorm(0.975))

    ggplot(cv_survey, aes(x=factor(age_year), y=log_ratio, col=indictor)) + geom_boxplot(position=position_dodge())
    lplot <- ggplot(survey_outputs, aes(x=age_start)) +
      geom_line(aes(y=log_chest_ratio), col="purple") + geom_ribbon(aes(ymin=log_chest_ratio - log_chest_se*1.96, ymax=log_chest_ratio + log_chest_se*1.96), alpha=0.2, fill="purple") +
      geom_line(aes(y=log_diff_fever_ratio), col="blue") + geom_ribbon(aes(ymin=log_diff_fever_ratio - log_diff_fever_se*1.96, ymax=log_diff_fever_ratio + log_diff_fever_se*1.96), alpha=0.2, fill="blue") +
      geom_line(aes(y=log_diff_ratio), col="green") + theme_bw() + geom_ribbon(aes(ymin=log_diff_ratio - log_diff_se*1.96, ymax=log_diff_ratio + log_diff_se*1.96), alpha=0.2, fill="green") +
      geom_hline(yintercept=0, lty=2)
    print(lplot)

  ####################################################################################################
  ## Pull in all the survey data, merge, and make data adjustments ##
  ####################################################################################################
    period <- read.csv("filepath")

  # Append new data
    period$had_fever_survey <- ifelse(period$gbd_2019_new == 1 & period$cv_diag_selfreport == 1, 1, period$had_fever_survey)
    #survey_gbd2019_s4$gbd_round <- 2019

    # period <- rbind.fill(period, survey_gbd2019_s4)

    period$cv_diag_selfreport[is.na(period$cv_diag_selfreport)] <- 0
    period <- subset(period, cv_diag_selfreport==1)
    period <- subset(period, !is.na(had_fever_survey))

    period$mean_original <- period$mean
    period$standard_error_original <- period$standard_error

    period$log_mean <- log(period$mean)
    period$log_mean_original <- period$log_mean
    period$cv_had_fever <- period$had_fever_survey
    period$cv_no_fever <- (1-period$had_fever_survey)
    period$indicator <- with(period, ifelse(cv_diag_valid_good==1 & cv_had_fever==1, "Reference", ifelse(cv_diag_valid_good==1 & cv_had_fever==0,"Chest only",
                                                                                                         ifelse(cv_diag_valid_good==0 & cv_had_fever==1,"Difficulty and fever",
                                                                                                                "Difficulty only"))))

    pcomp <- join(period, survey_outputs, by=c("age_start"))

  ############################################################################
    pcomp$log_mean <- ifelse(pcomp$cv_diag_valid_good==1, ifelse(pcomp$cv_had_fever==0, pcomp$log_mean + pcomp$log_chest_ratio, pcomp$log_mean), pcomp$log_mean)
    pcomp$log_mean <- ifelse(pcomp$cv_diag_valid_good==0, ifelse(pcomp$cv_had_fever==1, pcomp$log_mean + pcomp$log_diff_fever_ratio, pcomp$log_mean), pcomp$log_mean)
    pcomp$log_mean <- ifelse(pcomp$cv_diag_valid_good==0, ifelse(pcomp$cv_had_fever==0, pcomp$log_mean + pcomp$log_diff_ratio, pcomp$log_mean), pcomp$log_mean)

  ## Get the linear standard errors ##
    log_standard_error <- sapply(1:nrow(pcomp), function(i) {
      ratio_i <- pcomp[i, "mean"]
      ratio_se_i <- pcomp[i, "standard_error"]
      deltamethod(~log(x1), ratio_i, ratio_se_i^2)
    })
    se_xw_lri2 <- sqrt(log_standard_error^2 + pcomp$log_chest_se^2)
    se_xw_lri3 <- sqrt(log_standard_error^2 + pcomp$log_diff_fever_se^2)
    se_xw_lri4 <- sqrt(log_standard_error^2 + pcomp$log_diff_se^2)

    final_se <- ifelse(pcomp$indicator=="Reference", pcomp$standard_error, ifelse(pcomp$indicator=="Chest only", se_xw_lri2, ifelse(pcomp$indicator=="Difficulty and fever", se_xw_lri3, se_xw_lri4)))
    final_se <- sapply(1:nrow(pcomp), function(i) {
      ratio_i <- pcomp[i, "log_mean"]
      ratio_se_i <- final_se[i]
      deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
    })

    pcomp$mean <- exp(pcomp$log_mean)
    pcomp$standard_error <- ifelse(pcomp$indicator=="Reference", pcomp$standard_error, final_se)

    setnames(pcomp, c("start_year","end_year"), c("year_start","year_end"))
    pcomp$cases <- pcomp$mean * pcomp$sample_size

###### Save! ######
  write.csv(pcomp, "filepath")

  pdf("filepath", height=8, width=8)
    lplot
    ggplot(pcomp, aes(x=mean_original, y=mean, col=indicator)) + geom_point() + geom_abline(intercept=0, slope=1) + theme_bw()
    ggplot(pcomp, aes(x=standard_error_original, y=(standard_error), col=indicator)) + geom_point() + geom_abline(intercept=0, slope=1) + theme_bw()
  dev.off()

#######################################################################
## The purpose of this part of the code is to generate input data sheets for MR-BRT
## for self-reported to clinical definition of LRI
## for the surveys. We will also use this code for prepping sex splitting
## and for determining a ratio scalar for hospital data (not prepared
## by the clinical data team) and for the inpatient data (prepared by clinical team).
## Some of the decisions regarding data processing and how the data
## will be merged come from "estimate_crosswalks_selfreport_hosp.R"
#######################################################################

  ## get SDI, HAQI as a potential predictor
  sdi <- get_covariate_estimates(location_id="all", covariate_id=881, decomp_step="step1", year_id=1980:2019)
    sdi <- join(sdi, locs[,c("location_id","ihme_loc_id")], by="location_id")
    sdi$sdi <- sdi$mean_value
  haqi <- get_covariate_estimates(location_id="all", covariate_id=1099, decomp_step="step1", year_id=1980:2019)
    haqi <- join(haqi, locs[,c("location_id","ihme_loc_id")], by="location_id")
    haqi$haqi <- haqi$mean_value

  ##-----------------------------------------------------------------------------
  lri <- read.csv("filepath")
  ##----------------------------------------------------------------------------
  ## These data work for the reference and cv_hospital ##
  ##----------------------------------------------------------------------------
  lri$group_review[is.na(lri$group_review)] <- 1
  lri <- subset(lri, is_outlier==0 & lri$group_review==1)
  lri$cv_inpatient <- ifelse(lri$clinical_data_type != "",1,0)

  lri$age_mid <- floor((lri$age_end + lri$age_start) / 2)

  # Hospital and claims data added later
    lri <- subset(lri, cv_inpatient == 0)

  # Remove survey (self-reported data)
    lri <- subset(lri, cv_diag_selfreport == 0)

  lri$is_reference <- ifelse(lri$cv_hospital==0 & lri$cv_diag_selfreport==0 & lri$cv_marketscan==0 & lri$cv_inpatient==0 & lri$cv_inpatient_sample==0, 1, 0)

  # Join with SDI as it is a predictor for cv_hospital
    lri$year_id <- floor((lri$year_start + lri$year_end) / 2)
    lri <- join(lri, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
    lri$sdi <- round(lri$sdi,2)

  ## Set some parameters ##
  age_bins <- c(0,1,5,20,40,60,80,100)
  age_bins <- c(0,1,seq(5,100,5))

  ## Subset to working data frame ##
  df <- lri[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
               "cv_diag_selfreport","cv_hospital","cv_inpatient","is_reference","group_review","is_outlier")]
  df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")

  ## Some rows don't have sample size and cases, use closed form estimate of SE to get them.
  sample_size <- with(df, mean*(1-mean)/standard_error^2)
  cases <- df$mean * sample_size
  df$cases <- ifelse(is.na(df$cases), cases, df$cases)
  df$sample_size <- ifelse(is.na(df$sample_size), sample_size, df$sample_size)

######################################################################
## Hospital data crosswalk. This is for data that report the incidence
## of severe or hospitalized LRI. There are plenty of sources that
## report the incidence of both clinician-diagnosed (reference) and
## hospitalized LRI so we are going to use within study-age-location
## matching for preparing these data for the meta-regression.
######################################################################
  cv_hospital <- bundle_crosswalk_collapse(df, covariate_name="cv_hospital", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2019), merge_type="within", location_match="exact")

  cv_hospital$year_id <- floor((cv_hospital$year_end + cv_hospital$year_start)/2)
  cv_hospital$location_id <- cv_hospital$location_match
  cv_hospital$age_mid <- floor((cv_hospital$age_end + cv_hospital$age_start)/2)
  cv_hospital$num_age_bin <- as.numeric(cv_hospital$age_bin)

  cv_hospital <- join(cv_hospital, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
  cv_hospital <- join(cv_hospital, locs[,c("location_id","super_region_name","location_name")], by="location_id")
  cv_hospital$high_income <- ifelse(cv_hospital$super_region_name=="High-income",1,0)

  cv_hospital$log_ratio <- cv_hospital$log_ratio
    write.csv(cv_hospital[cv_hospital$ratio>1,], "filepath", row.names=F)

  # Test a couple linear models for predictors
      sr_hosp <-  ggplot(cv_hospital, aes(x=super_region_name, y=ratio, col=super_region_name)) + geom_boxplot() + theme_bw() + theme(axis.text.x =element_text(angle=90, hjust=1)) + xlab("") + ylab("Ratio") +
        ggtitle("LRI hospital ratio") + scale_color_discrete("")
    sr_hosp
      age_hosp <- ggplot(cv_hospital, aes(x=age_mid, y=log_ratio)) + geom_point() + stat_smooth(method="loess", se=F) + theme_bw() + xlab("Age midpoint") + ylab("Log ratio") + ggtitle("LRI hospital ratio")
    age_hosp

    summary(lm(log_ratio ~ age_mid, data=cv_hospital))
    summary(lm(log_ratio ~ sdi^3 + sdi^2 + sdi, data=cv_hospital))
    summary(lm(log_ratio ~ high_income + age_mid, data=cv_hospital))

  ##-------------------------------------------------------------------------------------------------
  ## No covariates (USED)
    ##-------------------------------------------------------------------------------------------------
    fit2 <- run_mr_brt(
      output_dir = "filepath",
      model_label = "cv_hospital",
      data = cv_hospital,
      mean_var = "log_ratio",
      se_var = "delta_log_se",
      covs = list(cov_info("age_mid","X",
                           degree = 2, n_i_knots = 3,
                           l_linear=TRUE, r_linear = TRUE, bspline_gprior_mean = "0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf")),
      overwrite_previous = TRUE,
      study_id = "nid",
      method = "trim_maxL",
      trim_pct = 0.1
    )

    df_pred <- data.frame(expand.grid(intercept = 1, age_mid= seq(0,100,1)))
    pred1 <- predict_mr_brt(fit2, newdata = df_pred)
    check_for_preds(pred1)
    pred_object <- load_mr_brt_preds(pred1)
    preds <- pred_object$model_summaries

    # Calculate log se
      preds$log_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
    # Convert the mean and standard_error to linear space
      preds$hospital_linear_se <- sapply(1:nrow(preds), function(i) {
        ratio_i <- preds[i, "Y_mean"]
        ratio_se_i <- preds[i, "log_se"]
        deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
      })
      preds$hospital_ratio <- exp(preds$Y_mean)
      # rename
        preds$log_hospital_ratio <- preds$Y_mean
        preds$log_hospital_se <- preds$log_se
        preds$age_mid <- preds$X_age_mid

      mod_data <- fit2$train_data
      mod_data <- join(mod_data, locs[,c("location_id","location_name","super_region_name")], by="location_id")


    ## Look at the age curve ##
    mod_data$outlier <- floor(abs(mod_data$w - 1))
    ggplot(preds, aes(x=X_age_mid, y=(Y_mean))) + geom_line() +
      geom_ribbon(aes(ymin=(Y_mean_lo), ymax=(Y_mean_hi)), alpha=0.3) +
      geom_point(data=mod_data, aes(x=age_mid, y=log_ratio, size=1/se^2, shape=factor(outlier))) + guides(size=F) +
      scale_shape_manual(values=c(19,4)) + #guides(col=F) +
      geom_hline(yintercept=0, lty=2, col="red") + scale_y_continuous("Log ratio") + scale_x_continuous("Age mid") + theme_bw() + ggtitle("Inpatient ratio")


  # Keep a record of the values
    p <- preds[,c("age_mid","hospital_ratio","hospital_linear_se","Y_mean","Y_mean_lo","Y_mean_hi")]
    p <- unique(p)
    p$count_obs <- c(length(cv_hospital$ratio))
    p$variable <- "cv_hospital"

    xwvals <- rbind.fill(xwvals, p)

  # Join back with master data_frame
  # preds$sdi <- preds$X_sdi
  lri <- join(lri, preds[,c("age_mid","hospital_ratio","hospital_linear_se","log_hospital_ratio","log_hospital_se")], by="age_mid")
  # lri$hospital_ratio <- preds$hospital_ratio
  # lri$hospital_linear_se <- preds$hospital_linear_se
  # lri$log_hospital_ratio <- preds$log_hospital_ratio
  # lri$log_hospital_se <- preds$log_hospital_se

######################################################################
## Self-reported crosswalk. This is for data that report the prevalence
## of self-reported LRI (from surveys). There are no sources, by definition,
## that reported both the prevalence in self reported and reference definition.
######################################################################
  ## Subset to working data frame by removing self-report in Epi bundle data,
  ## adding in the newly-crosswalked survey data ##
  surveys <- read.csv("filepath")
  surveys <- surveys[, -which(names(surveys) %in% c("location","location_update","X","child_sex","age_year","scalar_lri","scalar_se",
                                                    "base_lri","good_no_fever","poor_no_fever","missing","duration","duration_lower",
                                                    "duration_upper","urban","location_ascii_name","log_mean","log_mean_original",
                                                    "cv_no_fever","log_chest_ratio","log_diff_fever_ratio","log_diff_ratio","log_chest_se",
                                                    "log_diff_fever_se","log_diff_se"))]
  surveys$group_review <- 1
  surveys$is_reference <- 0
  surveys$cv_hospital <- 0
  surveys <- join(surveys, locs[,c("location_id","ihme_loc_id","location_name")], by="ihme_loc_id")
  lri <- rbind.fill(lri, surveys)
  lri <- join(lri, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")

  cv_selfreport <- bundle_crosswalk_collapse(lri[lri$gbd_round!=2019,], 
                                             covariate_name="cv_diag_selfreport", 
                                             age_cut=c(0,1,2,3,4,5,20,40,60,80,100), 
                                             year_cut=c(seq(1980,2015,5),2019), 
                                             merge_type="between", 
                                             location_match="country"
                                             )

  cv_selfreport$year_id <- floor((cv_selfreport$year_end + cv_selfreport$year_start)/2)
  cv_selfreport$age_mid <- floor((cv_selfreport$age_end + cv_selfreport$age_start)/2)
  cv_selfreport$ihme_loc_id <- cv_selfreport$location_match

  cv_selfreport <- join(cv_selfreport, locs[,c("location_id","super_region_name","ihme_loc_id")], by="ihme_loc_id")
  cv_selfreport <- join(cv_selfreport, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
  cv_selfreport$row <- 1:length(cv_selfreport$location_id)

  # Save this file. We think that the ratio must be less than 1, right? That the specificity of self-report must be lower
  # than for clinician-diagnosis?
  write.csv(cv_selfreport[cv_selfreport$ratio<1,], "filepath", row.names=F)

  # Test a couple linear models for predictors
  summary(lm(log_ratio ~ age_mid, data=cv_selfreport))
  summary(lm(log_ratio ~ sdi, data=cv_selfreport))
  summary(lm(log_ratio ~ super_region_name, data=cv_selfreport))

  ## Run an MR-BRT model. ##
  # Including NID as a study id
  fit1 <- run_mr_brt(
    output_dir = "filepath",
    model_label = "cv_selfreport",
    data = "filepath",
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

  # Calculate log se
  preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)

  # Convert the mean and standard_error to linear space
  preds$linear_se <- deltamethod(~exp(x1), preds$Y_mean, preds$se^2)
  preds$ratio <- exp(preds$Y_mean)

  mod_data <- fit1$train_data
  mod_data <- join(mod_data, locs[,c("location_id","location_name","super_region_name")], by="location_id")

  ## Create essentially a forest plot
  mod_data$outlier <- ceiling(abs(mod_data$w - 1))
  mod_data$row_num <- 1:length(mod_data$age_bin)
  mod_data$label <- with(mod_data, paste0(location_name,"_",age_start,"-",age_end,"_",year_start,"-",year_end,"_",row_num))
  f2 <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=log_ratio + delta_log_se*1.96, ymin=log_ratio - delta_log_se*1.96)) + geom_point(aes(y=log_ratio, x=label)) + geom_errorbar(aes(x=label), width=0) +
    theme_bw() + ylab("Log ratio") + xlab("") + coord_flip() + ggtitle(paste0("Self-report ratio (",round(preds$ratio,3),")")) +
    geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean, col="purple") +
    geom_rect(data=preds, aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
  print(f2)

  ## Inlcude the value from DisMod 2017 for comparison
  dismod <- data.frame(mean=1/3.8, lower=1/3.58, upper=1/3.99)
  # Calculate log se
  preds$log_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
  # Convert the mean and standard_error to linear space
  preds$selfreport_linear_se <- sapply(1:nrow(preds), function(i) {
    ratio_i <- preds[i, "Y_mean"]
    ratio_se_i <- preds[i, "log_se"]
    deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
  })
  preds$selfreport_ratio <- exp(preds$Y_mean)
  # rename
  preds$log_ratio <- preds$Y_mean

  # Keep a record of the values
    p <- preds[,c("selfreport_ratio","selfreport_linear_se","Y_mean","Y_mean_lo","Y_mean_hi")]
    p <- unique(p)
    p$count_obs <- c(length(cv_selfreport$ratio))
    p$variable <- "cv_hospital"

    xwvals <- rbind.fill(xwvals, p)

  # Join back with master data_frame
  lri$selfreport_ratio <- preds$selfreport_ratio
  lri$selfreport_linear_se <- preds$selfreport_linear_se

  ################################################################################
  ## Perform actual crosswalk, save results for upload ##
  ################################################################################
  lri$raw_mean <- lri$mean
  lri$raw_standard_error <- lri$standard_error
  lri$crosswalk_type <- ifelse(lri$cv_hospital==1, "Hospitalized", ifelse(lri$cv_diag_selfreport==1, "Self-reported","No crosswalk"))

  ## calculate the adjusted standard_errors outside of ifelse statement
  std_hospital <- sqrt(with(lri, standard_error^2 * hospital_linear_se^2 + standard_error^2*hospital_ratio^2 + hospital_linear_se^2*mean^2))
  std_selfreport <- sqrt(with(lri, standard_error^2 * selfreport_linear_se^2 + standard_error^2*selfreport_ratio^2 + selfreport_linear_se^2*mean^2))

  #####################################
  ## Convert the mean by the crosswalk!
  # Hospital
  lri$mean <- ifelse(lri$cv_hospital==1, lri$mean * lri$hospital_ratio, lri$mean)
  lri$standard_error <- ifelse(lri$cv_hospital==1, std_hospital, lri$standard_error)
  lri$note_modeler <- ifelse(lri$cv_hospital==1, paste0("This data point was adjusted for being in a hospitalized sample population. The code to produce
                                                      this estimated ratio is from prepare_crosswalks_selfreport_hosp_mr-brt.R. This occurred in MR-BRT
                                                      during decomposition step 2. The original mean was ", round(lri$raw_mean,3),". ", lri$note_modeler), lri$note_modeler)
  # Self-report
  lri$mean <- ifelse(lri$cv_diag_selfreport==1, lri$mean * lri$selfreport_ratio, lri$mean)
  lri$standard_error <- ifelse(lri$cv_diag_selfreport==1, std_selfreport, lri$standard_error)
  lri$note_modeler <- ifelse(lri$cv_diag_selfreport==1, paste0("This data point was adjusted for being from a self-reported sample population (typically surveys). The code to produce
                                                      this estimated ratio is from prepare_crosswalks_selfreport_hosp_mr-brt.R. This occurred in MR-BRT
                                                      during decomposition step 2. The original mean was ", round(lri$raw_mean,3),". ", lri$note_modeler), lri$note_modeler)

  ## Pull out data only used to inform crosswalk
  lri <- subset(lri, nid!=999999)

  ## Clear lower/upper if adjusted
  lri$lower <- ifelse(lri$cv_hospital==1, "", ifelse(lri$cv_diag_selfreport==1,"",lri$lower))
  lri$upper <- ifelse(lri$cv_hospital==1, "", ifelse(lri$cv_diag_selfreport==1,"",lri$upper))

  ## Save!
  write.csv(lri, "filepath", row.names=F)

  ## Pull out the data from this crosswalk
  lit_survey_output <- lri

  ######################################
  ## Plot these changes ##
  # Pull the results from DisMod for comparison
  dismod <- data.frame(crosswalk=c("Hospital","Selfreport"), value=c(0.78, 3.8))

  pdf("filepath", height=11, width=11)
  print(sr_hosp)
  print(age_hosp)
  print(f1)
  print(f2)
  ggplot(lri, aes(x=raw_mean, y=mean, col=crosswalk_type)) + geom_point() + geom_abline(intercept=0, slope=1) + theme_bw() +
    xlab("Unadjusted mean") + ylab("Mean after crosswalk") + scale_color_discrete("") +
    geom_abline(intercept=0, slope = 1/dismod$value[1], col="red", lty=2) +
    geom_abline(intercept=0, slope = 1/dismod$value[2], col="blue", lty=2) +
    ggtitle(paste0("LRI crosswalks\nThe dashed line is what the values would be after DisMod decomp 1\nDisMod value hospital= ",round(1/dismod$value[1],3),
                   "\nDisMod value self-report= ",round(1/dismod$value[2],3)))
  dev.off()

#######################################################################
## The purpose of this code is to generate input data sheets for MR-BRT
## for the inpatient data (prepared by clinical team).
#######################################################################
  duration <- read.csv("filepath")

  ##-----------------------------------------------------------------------------
  ## First step is to pull all data, convert clinical data from incidence to prevalence ##
  ##-----------------------------------------------------------------------------
  lri <- read.csv("filepath")
  clin_df <- subset(lri, clinical_data_type!="")

  clin_df$is_reference <- 0
  clin_df$extractor <- "clinical_team"

  lri_bundle <- subset(lri, clinical_data_type == "")
    lri_bundle$cv_hospital[is.na(lri_bundle$cv_hospital)] <- 0
    lri_bundle$cv_inpatient <- 0
    lri_bundle$cv_diag_selfreport[is.na(lri_bundle$cv_diag_selfreport)] <- 0
  lri_bundle$is_reference <- with(lri_bundle, ifelse(cv_hospital == 0 & cv_inpatient == 0 & cv_diag_selfreport == 0 & cv_marketscan == 0 & cv_inpatient_sample == 0, 1, 0))

  ## Use a function convert incidence to prevalence
    prevalence_means <- convert_inc_prev(clin_df, duration$mean, duration$lower, duration$upper)
    clin_df$mean <- prevalence_means$mean_prevalence
    clin_df$standard_error <- prevalence_means$standard_error_prevalence
    clin_df$lower <- ""
    clin_df$upper <- ""
    clin_df$cv_inpatient <- ifelse(clin_df$clinical_data_type == "inpatient", 1, 0)
    clin_df$cv_marketscan <- ifelse(clin_df$clinical_data_type == "claims", 1, 0)
    clin_df$note_modeler <- "Converted from incidence to prevalence using mean duration 7.79"
    clin_df$measure <- "prevalence"
    clin_df$age_end <- ifelse(clin_df$age_end > 99, 99, clin_df$age_end)

  # Used in crosswalk
  lri_full <- rbind.fill(lri_bundle, clin_df)

  # Data to be saved
  output_data <- rbind.fill(lri_bundle, clin_df)

  ##-----------------------------------------------------------------------------

  # Join with SDI as it is a predictor for cv_hospital
    lri_full$year_id <- floor((lri_full$year_start + lri_full$year_end) / 2)
    lri_full <- join(lri_full, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
    lri_full$sdi <- round(lri_full$sdi,2)

  ## Set some parameters ##
    age_bins <- c(0,1,5,10,20,40,60,80,100)
    age_bins <- c(0,1,seq(5,100,5))

  ## Some data don't have cases or sample size
    # Calculate cases and sample size (missing in inpatient data)
    sample_size <- with(lri_full, mean*(1-mean)/standard_error^2)
    cases <- lri_full$mean * sample_size
    lri_full$cases <- ifelse(is.na(lri_full$cases), cases, lri_full$cases)
    lri_full$sample_size <- ifelse(is.na(lri_full$sample_size), sample_size, lri_full$sample_size)

  ## Subset to working data frame ##
    df <- lri_full[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
                      "cv_diag_selfreport","cv_hospital","cv_inpatient","cv_marketscan","is_reference","is_outlier","group_review")]
    df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")
    df$group_review[is.na(df$group_review)] <- 1

    cv_inpatient <- bundle_crosswalk_collapse(df, covariate_name="cv_inpatient", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2019), merge_type="between", location_match="country", include_logit = T)
      cv_inpatient$age_mid <- floor((cv_inpatient$age_end + cv_inpatient$age_start)/2)
      cv_inpatient$match_id <- paste0(cv_inpatient$nid, "_", cv_inpatient$n_nid)
      cv_inpatient$year_id <- round((cv_inpatient$year_end + cv_inpatient$year_start)/2,0)
      cv_inpatient$ihme_loc_id <- cv_inpatient$location_match
      cv_inpatient$location_id <- cv_inpatient$location_match
      cv_inpatient <- join(cv_inpatient, sdi[,c("ihme_loc_id","sdi","year_id")], by=c("ihme_loc_id","year_id"))
      cv_marketscan <- bundle_crosswalk_collapse(df, covariate_name="cv_marketscan", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2019), merge_type="between", location_match="country", include_logit = T)
      cv_marketscan$age_mid <- floor((cv_marketscan$age_end + cv_marketscan$age_start)/2)
      cv_marketscan$match_id <- paste0(cv_marketscan$nid, "_", cv_marketscan$n_nid)
## ---------------------------------------------------------------------------
  ##########################################################################
  ## Model for clinical inpatient data ##
  ##########################################################################

  ## model with SDI and age
      fit1 <- run_mr_brt(
        output_dir = "filepath",
        model_label = "cv_inpatient_dec",
        data = cv_inpatient,
        mean_var = "logit_ratio",
        se_var = "logit_ratio_se",
        covs = list(cov_info("sdi","X",
                             degree = 2, n_i_knots=3,
                             l_linear=TRUE, r_linear = TRUE, bspline_gprior_mean = "0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf",
                             bspline_mono = "decreasing", bspline_cvcv = "convex"
                             )),
        study_id = "match_id",
        overwrite_previous = TRUE,
        method = "trim_maxL",
        trim_pct = 0.1
      )

      df_pred <- data.frame(intercept=1, sdi=seq(0,1,0.01))
      pred1 <- predict_mr_brt(fit1, newdata = df_pred)

      pred_object <- load_mr_brt_preds(pred1)
      preds <- pred_object$model_summaries
      preds$age_mid <- preds$X_age_mid
      preds$sdi <- preds$X_sdi

      ## Look at the age curve ##
        mod_data <- fit1$train_data
        mod_data$outlier <- floor(abs(mod_data$w - 1))
        ggplot(preds, aes(x=sdi, y=(Y_mean))) + geom_line() +
          geom_ribbon(aes(ymin=(Y_mean_lo), ymax=(Y_mean_hi)), alpha=0.3) +
          geom_point(data=mod_data, aes(x=sdi, y=logit_ratio, size=1/se^2, col=factor(location_match), shape=factor(outlier))) + guides(size=F) + scale_shape_manual(values=c(19,4)) + guides(shape=F) +
          geom_hline(yintercept=0, lty=2, col="red") + scale_y_continuous("Log ratio") + scale_x_continuous("SDI") + theme_bw() + ggtitle("Inpatient ratio")

        mod_data$label <- paste0(mod_data$nid,"_", mod_data$location_match,"_", mod_data$obs_id)
        ggplot() + geom_point(data=subset(mod_data, outlier == 0), aes(x=label, y=logit_ratio, ymin = logit_ratio - logit_ratio_se * 1.96, ymax = logit_ratio + logit_ratio_se * 1.96)) +
          geom_errorbar(width=0, data=subset(mod_data, outlier == 0), aes(x=label, y=logit_ratio, ymin = logit_ratio - logit_ratio_se * 1.96, ymax = logit_ratio + logit_ratio_se * 1.96)) +
          coord_flip() + theme_bw() + geom_hline(yintercept=preds$Y_mean) + geom_hline(yintercept=0, lty=2) +
          geom_rect(data=preds[1,], aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="blue")


    # Calculate log se
      preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
    
    # # Convert the mean and standard_error to linear space
      preds$inpatient_linear_se <- sapply(1:nrow(preds), function(i) {
        ratio_i <- preds[i, "Y_mean"]
        ratio_se_i <- preds[i, "se"]
        deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
      })
      preds$inpatient_ratio <- inv.logit(preds$Y_mean)
      preds$inpatient_logit <- preds$Y_mean

    ### Rejoin inpatient ratio with clinical data  ###
    # Data to be saved
      output_data <- rbind.fill(lri_bundle, clin_df)
      output_data$age_mid <- ceiling((output_data$age_end + output_data$age_start)/2)
      
      output_data$year_id <- round((output_data$year_start + output_data$year_end)/2,0)
      output_data <- join(output_data, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
      output_data$sdi <- round(output_data$sdi, 2)
      preds$sdi <- round(preds$sdi, 2)
      output_data <- join(output_data, preds[,c("sdi","inpatient_ratio","inpatient_linear_se","inpatient_logit")], by=c("sdi"))

    # Keep a record of the values
      p <- preds[,c("inpatient_logit","Y_mean","Y_mean_lo","Y_mean_hi")]
      p <- unique(p)
      p$variable <- c("cv_inpatient")
      p$count_obs <- c(length(cv_inpatient$ratio))
      xwvals <- rbind.fill(xwvals, p)

    ## see the adjusted age pattern
      adf <- output_data[output_data$cv_inpatient==1,c("mean","age_start","age_end","inpatient_logit","location_id")]
      bdf <- adf
      cdf <- output_data[output_data$is_reference==1,c("mean","age_start","age_end","location_id")]
      cdf$age_start <- round(cdf$age_start, 0)
      adf$type <- "Unadjusted"
      bdf$type <- "Adjusted"
      cdf$type <- "Non-clinical"
      bdf$mean <- inv.logit(logit(bdf$mean) + bdf$inpatient_logit)

      ddf <- data.frame(type="Filler", age_start=seq(0,98,2), age_end=seq(1,99,2), mean=0.001, location_id=1)

      mean_ref <- median(output_data$mean[output_data$is_reference==1], na.rm=T)

      ggplot(rbind.fill(adf,bdf,cdf,ddf), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
        geom_hline(yintercept=0.0005, lty=2) + geom_hline(yintercept=0.0001, lty=2) + geom_hline(yintercept=mean_ref) + ggtitle("All locations, SDI adjusted")

      ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==520), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
        geom_hline(yintercept=0.0005, lty=2) + geom_hline(yintercept=0.0001, lty=2) + geom_hline(yintercept=mean_ref) + ggtitle("Yunnan")
      ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==35460), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
        geom_hline(yintercept=0.0005, lty=2) + geom_hline(yintercept=0.0001, lty=2) + geom_hline(yintercept=mean_ref) + ggtitle("Kagawa")
      ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==4726), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
        geom_hline(yintercept=0.0005, lty=2) + geom_hline(yintercept=0.0001, lty=2) + geom_hline(yintercept=mean_ref) + ggtitle("Bali")
      ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==570), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
        geom_hline(yintercept=0.0005, lty=2) + geom_hline(yintercept=0.0001, lty=2) + geom_hline(yintercept=mean_ref) + ggtitle("Washington")
      ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==4767), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
        geom_hline(yintercept=0.0005, lty=2) + geom_hline(yintercept=0.0001, lty=2) + geom_hline(yintercept=mean_ref) + ggtitle("Piaui")

  ##########################################################################
  ## Model for Claims data ##
  ##########################################################################

  ## Run an MR-BRT model. ##
  fit1 <- run_mr_brt(
    output_dir = 'filepath',
    model_label = "cv_marketscan",
    data = cv_marketscan,
    mean_var = "logit_ratio",
    se_var = "logit_ratio_se",
    # covs = list(cov_info("age_mid","X",
    #                      degree = 2, n_i_knots = 3,
    #                      l_linear=FALSE, r_linear = TRUE, bspline_gprior_mean = "0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf")),
    study_id = "match_id",
    overwrite_previous = TRUE,
    method = "trim_maxL",
    trim_pct = 0.1
  )

    df_pred <- data.frame(intercept=1, age_mid=seq(0,100,1))
    pred1 <- predict_mr_brt(fit1, newdata = df_pred)

    pred_object <- load_mr_brt_preds(pred1)
    preds <- pred_object$model_summaries
    preds$age_mid <- 0:100

    ## Look at the age curve ##
      mod_data <- fit1$train_data
      mod_data$outlier <- floor(abs(mod_data$w - 1))
      mod_data$row_num <- 1:length(mod_data$mean)
      ggplot(preds, aes(x=X_age_mid, y=(Y_mean))) + geom_line() +
        geom_ribbon(aes(ymin=(Y_mean_lo), ymax=(Y_mean_hi)), alpha=0.3) +
        geom_point(data=mod_data, aes(x=age_mid, y=logit_ratio, size=1/se^2, col=factor(outlier))) + guides(size=F) + scale_color_manual(values=c("black","red")) + guides(col=F) +
        geom_hline(yintercept=0, lty=2, col="red") + scale_y_continuous("Log ratio") + scale_x_continuous("Age", limits=c(0,100)) + theme_bw() + ggtitle("Claims ratio")

    ## Create essentially a forest plot
      mod_data$label <- with(mod_data, paste0(match_id,"_",location_match,"_",age_bin,"_",year_bin,"_",row_num))
      f <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=logit_ratio + logit_ratio_se*1.96, ymin=logit_ratio - logit_ratio_se*1.96)) + 
        geom_point(aes(y=logit_ratio, x=label)) + 
        geom_errorbar(aes(x=label), width=0) +
        theme_bw() + ylab("Transformed ratio") + xlab("") + coord_flip() + ggtitle("Claims data") +
        geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean[1], col="purple") +
        geom_rect(data=preds[1,], aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
      print(f)

    # Calculate log se
    preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
    
    # # Convert the mean and standard_error to linear space
    preds$marketscan_linear_se <- sapply(1:nrow(preds), function(i) {
      ratio_i <- preds[i, "Y_mean"]
      ratio_se_i <- preds[i, "se"]
      deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
    })
    preds$marketscan_ratio <- inv.logit(preds$Y_mean)
    preds$marketscan_logit <- preds$Y_mean

    ### Rejoin marketscan ratio with clinical data  ###
      output_data$age_mid <- ceiling((output_data$age_end + output_data$age_start)/2)
      output_data <- join(output_data, preds[,c("age_mid","marketscan_ratio","marketscan_linear_se","marketscan_logit")], by="age_mid")

    # Keep a record of the values
      p <- preds[,c("marketscan_logit","Y_mean","Y_mean_lo","Y_mean_hi")]
      p <- unique(p)
      p$variable <- c("cv_marketscan")
      p$count_obs <- c(length(cv_marketscan$ratio))
      xwvals <- rbind.fill(xwvals, p)

  ################################################################################
  ## Perform actual crosswalk, save results for upload ##
  ################################################################################
  output_data$raw_mean <- output_data$mean
  output_data$raw_standard_error <- output_data$standard_error

  ## calculate the adjusted standard_errors outside of ifelse statement
    std_marketscan <- sqrt(with(output_data, standard_error^2 * marketscan_linear_se^2 + standard_error^2*marketscan_ratio^2 + marketscan_linear_se^2*mean^2))
    std_inpatient <- sqrt(with(output_data, standard_error^2 * inpatient_linear_se^2 + standard_error^2*inpatient_ratio^2 + inpatient_linear_se^2*mean^2))

  #####################################
  ## Convert the mean by the crosswalk

  output_data$logit_mean <- logit(output_data$mean)

## Back to crosswalking claims data
  # Claims
    output_data$mean <- ifelse(output_data$clinical_data_type=="claims", inv.logit(output_data$logit_mean + output_data$marketscan_logit), output_data$mean)
    output_data$standard_error <- ifelse(output_data$clinical_data_type=="claims", std_marketscan, output_data$standard_error)

  # Inpatient
    output_data$mean <- ifelse(output_data$clinical_data_type=="inpatient", inv.logit(output_data$logit_mean + output_data$inpatient_logit), output_data$mean)
    output_data$standard_error <- ifelse(output_data$clinical_data_type=="inpatient", std_inpatient, output_data$standard_error)

  output_data$note_modeler <- ifelse(output_data$clinical_data_type!="", paste0("This data point was adjusted for being in a clinical dataset. 
                                                                                The code to produce this estimated ratio is from crosswalk_clinical_inpatient_mr-brt.R. 
                                                                                This occurred in MR-BRT during decomposition step 2. The original mean was ",
                                                                                round(output_data$raw_mean,3),". ", output_data$note_modeler), output_data$note_modeler)
  ggplot(output_data, aes(x=raw_mean, y=mean, col=clinical_data_type)) + geom_point() + geom_abline()

## Save!
  write.csv(output_data, "filepath", row.names=F)

## Save crosswalk values
  xwvals <- rbind.fill(xwvals, survey_outputs)
  write.csv(xwvals, "filepath", row.names=F)

## Collect output from this crosswalk.
  clinical_output <- subset(output_data, clinical_data_type != "")

#######################################################################################
  ## The last step it so upload everything!!
  data_for_upload <- rbind.fill(clinical_output, lit_survey_output)
    ggplot(data_for_upload, aes(x=raw_mean, y=mean, col=clinical_data_type)) + geom_point() + geom_abline()

## Both sex data should be duplicated ##
  source("filepath/sex_split_mrbrt_weights.R")
  data_for_upload$seq_parent <- ""
  data_for_upload$crosswalk_parent_seq <- ""
  sex_df <- duplicate_sex_rows(data_for_upload)

  sex_df$mean <- ifelse(is.na(sex_df$mean), sex_df$cases / sex_df$sample_size, sex_df$mean)
  sex_df$group_review[is.na(sex_df$group_review)] <- ""
  sex_df <- subset(sex_df, group_review != 0)
  sex_df[is.na(sex_df)] <- ""
  sex_df$group_review <- ifelse(sex_df$specificity=="","",1)
  sex_df$uncertainty_type <- ifelse(sex_df$lower=="","", as.character(sex_df$uncertainty_type))
  sex_df$uncertainty_type_value <- ifelse(sex_df$lower=="","",sex_df$uncertainty_type_value)
  sex_df$uncertainty_type <- ifelse(sex_df$lower=="", "", sex_df$uncertainty_type)
  sex_df$uncertainty_type_value <- ifelse(sex_df$lower=="", "", sex_df$uncertainty_type_value)

  # After age splitting, sometimes cases > sample size
  sex_df$cases <- as.numeric(sex_df$cases)
  sex_df$sample_size <- as.numeric(sex_df$sample_size)
  sex_df$cases <- ifelse(sex_df$cases > sex_df$sample_size, sex_df$sample_size * 0.99, sex_df$cases)
  sex_df$mean <- as.numeric(sex_df$mean)

  # Subset if data are greater than 1
  sex_df <- subset(sex_df, mean < 1)
  # Make sure cases < sample size
    sex_df$sample_size <- as.numeric(sex_df$sample_size)
    sex_df$cases <- as.numeric(sex_df$cases)
    sex_df$cases <- ifelse(sex_df$cases > sex_df$sample_size, sex_df$sample_size * 0.9, sex_df$cases)
    sex_df$xwalk_type <- ifelse(sex_df$clinical_data_type!="","Clinical", ifelse(sex_df$cv_hospital==1,"Literature hospital", ifelse(sex_df$cv_diag_selfreport==1,"Survey","None")))
    ggplot(sex_df, aes(x=raw_mean, y=mean, col=xwalk_type)) + geom_point() + geom_abline()

    sex_df <- sex_df[, -which(names(sex_df) %in% c("cv_dhs","cv_whs","cv_nine_plus_test","cv_explicit_test","duration","duration_lower","duration_upper",
                                                   "cv_clin_data","unnamed..75","unnamed..75.1","survey","original_mean","is_reference",
                                                   "age_mid","ratio","linear_se","std_clinical","cv_inpatient_lit","hospital_ratio","hospital_linear_se",
                                                   "log_hospital_ratio","log_hospital_se","inpatient_lit_ratio","cv_miscoded","inpatient_lit_linear_se","log_inpatient_lit_ratio",
                                                   "log_inpatient_lit_se", "raw_mean","raw_standard_error","crosswalk_type","parent_id","sdi","mean_original",
                                                   "standard_error_original","cv_had_fever","indicator","region_name","super_region_name","ihme_loc_id.1",
                                                   "selfreport_rateio","selfreport_linear_se","exists_subnational","year_id","note_SR", "cv_diag_x.ray","cv_diag_x.ray.1"))]
  # Assign seq_parent and crosswalk_parent_seq
    sex_df$seq_parent <- ifelse(sex_df$seq_parent=="", sex_df$seq, sex_df$seq_parent)
    sex_df$crosswalk_parent_seq <- ifelse(sex_df$crosswalk_parent_seq=="", sex_df$seq, sex_df$crosswalk_parent_seq)



  ## Opportunity to quickly look for outliers ##
  source("filepath/map_dismod_input_function.R")
  source("filepath/input_data_scatter_sdi_function.R")
  library(scales)
    sex_df <- join(sex_df, locs[,c("location_id","parent_id","level","region_name","super_region_name")], by="location_id")

  pdf("filepath", height=8, width=12)

    map_dismod_input(sex_df, type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Unadjusted data")
    map_dismod_input(sex_df[sex_df$cv_inpatient==1,], type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Clinical inpatient")
    map_dismod_input(sex_df[sex_df$cv_diag_selfreport==1,], type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Survey data")

    input_scatter_sdi(sex_df, error = (-0.75), title = "All data")
    input_scatter_sdi(sex_df[sex_df$cv_inpatient==1,], error = (-0.75), title = "Inpatient data")
    input_scatter_sdi(sex_df[sex_df$cv_diag_selfreport==1,], error = (-0.75), title = "Survey data")

    for(s in unique(sex_df$region_name)){
      p <- ggplot(subset(sex_df, level ==3 & region_name==s), aes(x=location_name, y=mean)) + geom_boxplot() + geom_point(alpha=0.4 , aes(col=xwalk_type)) + scale_y_log10(label=percent) + ggtitle(s) + theme_bw() +
        geom_hline(yintercept = median(sex_df$mean), col="red") +
        geom_hline(yintercept = quantile(sex_df$mean, 0.25), col="red", lty=2) +
        geom_hline(yintercept = quantile(sex_df$mean, 0.75), col="red", lty=2)
      print(p)
    }
    for(l in unique(sex_df$parent_id[sex_df$level==4])){
      p <- ggplot(subset(sex_df, level==4 & parent_id==l), aes(x=location_name, y=mean)) + geom_boxplot() + geom_point(alpha=0.4, aes(col=xwalk_type)) + scale_y_log10(label=percent) + theme_bw() +
        theme(axis.text.x=element_text(angle=90, hjust=1)) + geom_hline(yintercept = median(sex_df$mean), col="red") +
        geom_hline(yintercept = quantile(sex_df$mean, 0.25), col="red", lty=2) +
        geom_hline(yintercept = quantile(sex_df$mean, 0.75), col="red", lty=2)
      print(p)
    }
  dev.off()

  sex_df$is_outlier <- ifelse(log(sex_df$mean) < (-12), 1, ifelse(log(sex_df$mean) > (-4), 1, 0))

## SUSENAS remove ##
  sex_df <- subset(sex_df, !(nid %in% c(5376,5401,6687,6706,6724,6767,6790,6811,6842,6874,6904,6970,30235,43510,43526,85265,150884,151184)))

  write.csv(sex_df, "filepath", row.names=F)

  write.xlsx(sex_df[sex_df$gbd_2019_new == 0,], "filepath", sheetName="extraction")

## Save a version with new data
  write.xlsx(sex_df[sex_df$gbd_2019_new == 1,], "filepath", sheetName="extraction")

  # And a version with SDI outliering
    # outlier_df <- sex_df
    # outlier_df$is_outlier <- ifelse(outlier_df$sdi_outlier == 1, 1, outlier_df$is_outlier)
    # outlier_df$is_outlier[is.na(outlier_df$is_outlier)] <- 0
    # write.xlsx(outlier_df, "filepath", sheetName="extraction")

## Upload. ##
  save_crosswalk_version(bundle_version_id=9509, data_filepath= "filepath/lri_all_crosswalks_decomp_step2.xlsx",
                         description = "SDI for clinical inpatient, single claims, extreme value outliers")
  save_crosswalk_version(bundle_version_id=13226, data_filepath= "filepath/lri_all_crosswalks_decomp_step4.xlsx",
                         description = "Step 4: SDI inpatient, no claims, outliers by SDI quintile.")