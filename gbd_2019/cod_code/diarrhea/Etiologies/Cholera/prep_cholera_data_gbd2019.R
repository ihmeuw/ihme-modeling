##############################################################
## Cholera data processing: GBD 2019.
## Prep data for CFR model in DisMod
## Determine an age-curve, use it to split age-non-specific
## Determine a crosswalk for severe diarrhea
## Anything else to make Stata code easier.
##############################################################
# Prepare your needed functions and information
library(metafor)
library(msm)
library(openxlsx)
library(plyr)
library(boot)
library(data.table)
library(ggplot2)
source() # filepaths to central functions for data upload and download
source() # filepaths to central functions for MR-BRT
source("/filepath/bundle_crosswalk_collapse.R")
source("/filepath/age_split_mrbrt_weights.R")
source("/filepath/age_split_dismod.R")
source("/filepath/sex_split_mrbrt_weights.R")

locs <- read.csv("filepath")

eti_info <- read.csv("filepath")

####################################
## These two are the same ##
## Proportion data
prop_data <- read.csv("filepath")

## All bundle data
# all_data <- get_bundle_data(bundle_id=4, decomp_step = "step2")
#   write.csv(all_data, "filepath", row.names=F)
####################################

## Case fatality bundle data
cf_data <- get_bundle_data(bundle_id=3083, decomp_step = "step2")

################################################################
## Prep data for the case fatality dismod model
################################################################
df <- cf_data
  df$cases <- df$mean * df$sample_size
  df$new_data <- 0

# Some newly extracted data by age to help with age curve
new_age_cfr <- read.csv("filepath")
  new_age_cfr$nid <- 999999
  new_age_cfr$is_outlier <- 0

df <- rbind(df, new_age_cfr)

  age_snip <- c(0,1,2,5,20,40,60,80,100)
  year_snip <- c(1990, 1995, 2000, 2005, 2010, 2015, 2020)
  df$age_mid <- (df$age_start + df$age_end) / 2
  df$year_mid <- (df$year_start + df$year_end) / 2

  df$age_snip <- cut(df$age_mid, age_snip)
  df$year_snip <- cut(df$year_mid, year_snip)

## Lots of age-non-specific data, need to age-split them.
# All of the surveillance data are 0-99 years. This might give
# some information about the country, which is what we want.
# A problem is that they have been split into 3 (0-29, 30-59, 60-99)
# How to pull them out, group_review some. Let's identify them.
  df$cv_age_split <- ifelse(df$age_start==0 & df$age_end == 29, 1, ifelse(df$age_start == 30 & df$age_end == 59, 1, ifelse(df$age_start == 60 & df$age_end == 99, 1, 0)))
    df$split_three <- df$cv_age_split
  df$cv_age_split <- ifelse(df$age_start == 0 & df$age_end == 99, 1, df$cv_age_split)

# Set a dummy variable where age_start/end rounded doesn't equal age_start/end
  df$round_age_start <- round(df$age_start, 1)
  df$round_age_end <- round(df$age_end, 1)

  df$cv_age_split <- with(df, ifelse(age_end > 10, ifelse(df$round_age_start != df$age_start, 1, ifelse(df$round_age_end != df$age_end, 1, df$cv_age_split)), df$cv_age_split))
    df$doubled <- with(df, ifelse(age_end > 10, ifelse(df$round_age_start != df$age_start, 1, ifelse(df$round_age_end != df$age_end, 1, 0)), 0))

# Keep a single row from the three times split data
  keep_one <- subset(df, cv_age_split == 1 & split_three == 1 & age_start == 0)
    keep_one$age_end <- 99

# Keep a single row from the doubled data
  keep_double <- subset(df, doubled == 1 & age_start == 0)
    keep_double$age_end <- 99

# Keep all non split data
  keep_non_split <- subset(df, split_three == 0 & doubled == 0)

  age_df <- rbind(keep_non_split, keep_one, keep_double)

# Keep track of which rows should now be group_review = 0 (will append later to upload)
  group_review <- subset(df, !(seq %in% age_df$seq))
  group_review <- group_review[,c("seq")]

#### These data can now be age-split and we should
  ## just use a DisMod model, I think, given data sparcity and this:
  ggplot(subset(age_df, cv_age_split==0), aes(x=age_mid, y=mean)) + geom_point() + stat_smooth(method="loess")

# reassign df
  df <- age_df
  df$group_review <- NA

##################################################################
  ## Do the actual age splitting ##

## Determine where age splitting should occur. ##
  age_map <- read.csv("filepath")
  age_map <- age_map[age_map$age_pull==1,c("age_group_id","age_start","age_end","order","age_pull")]
  age_cuts <- c(0,1,5,20,40,60,100)
  length <- length(age_cuts) - 1

  age_map$age_dummy <- cut(age_map$age_end, age_cuts, labels = paste0("Group ", 1:length))

## Create prevalence weights to age-split the data from the global diarrhea prevalence age pattern in GBD 2017 ##
#  dmod_global <- get_outputs(topic="cause", cause_id=302, age_group_id = age_info$age_group_id, year_id=1990:2017, location_id=1, sex_id=3, measure_id=5, metric_id=1, gbd_round_id=5)

# Cholera YLDs 
  dmod_global <- get_outputs(topic="rei", rei_id = 173, cause_id=302, age_group_id = age_info$age_group_id, year_id=1990:2017, location_id=1, sex_id=3, measure_id=3, metric_id=1, gbd_round_id=5)
  dmod_global$number <- dmod_global$val
  denom_weights <- dmod_global

  denom_weights <- join(denom_weights, age_map[,c("age_group_id","age_dummy","order")], by="age_group_id")
  ggplot(subset(denom_weights, year_id==2017), aes(x=order, y=val)) + geom_line() + theme_bw()

  # denom_weights$age_dummy <- with(denom_weights, ifelse(age_group_id < 5, "Group 1", ifelse(age_group_id ==5, "Group 2",
  #                                                                                           ifelse(age_group_id < 9, "Group 3", ifelse(age_group_id < 13, "Group 4",
  #                                                                                                                                      ifelse(age_group_id < 17, "Group 5",
  #                                                                                                                                             ifelse(age_group_id < 30, "Group 6", "Group 7")))))))

  denom_age_groups <- aggregate(number ~ location_id + year_id + age_dummy, data=denom_weights, function(x) sum(x))
  denom_all_ages <- aggregate(number ~ location_id + year_id, data=denom_weights, function(x) sum(x))
  denom_all_ages$total_number <- denom_all_ages$number

  denominator_weights <- join(denom_age_groups, denom_all_ages[,c("location_id","year_id","total_number")], by=c("location_id","year_id"))
  denominator_weights$prop_denom <- denominator_weights$number / denominator_weights$total_number

age_df <- age_split_dismod(df, denominator_weights, age_map = age_map, model_version_id=347663, bounded=T, title="Cholera", global_merge = T, sex_id = 2, measure_id=18, gbd_round_id = 5, location_id=1)

# Get rid of new data
age_df <- subset(age_df, new_data == 0)

##########################################################################
# Last year, we had two crosswalks.
# 1) cv_endemic (if the data are from a cholera endemic region)
# 2) cv_surveillance (if the data are from a notification system)

df <- age_df

df$cv_endemic[is.na(df$cv_endemic)] <- 0
df$cv_surveillance[is.na(df$cv_surveillance)] <- 0
df$is_reference <- ifelse(df$cv_endemic == 0 & df$cv_surveillance == 0, 1, 0)
# Endemic
  table(df$cv_endemic)
  t.test(mean ~ cv_endemic, data=df)
  ggplot(df, aes(x=cv_endemic, y=mean, col=factor(cv_endemic))) + geom_boxplot() + theme_bw() + scale_y_log10()
  ggplot(df, aes(x=age_snip, y=mean, col=factor(cv_endemic))) + geom_boxplot() + theme_bw() + scale_y_log10()
  ggplot(df, aes(x=year_snip, y=mean, col=factor(cv_endemic))) + geom_boxplot() + theme_bw() + scale_y_log10()

# Surveillance
  table(df$cv_surveillance)
  t.test(mean ~ cv_surveillance, data=df)
  ggplot(df, aes(x=cv_endemic, y=mean, col=factor(cv_surveillance))) + geom_boxplot() + theme_bw() + scale_y_log10()
  ggplot(df, aes(x=age_snip, y=mean, col=factor(cv_surveillance))) + geom_boxplot() + theme_bw() + scale_y_log10()
  ggplot(df, aes(x=year_snip, y=mean, col=factor(cv_surveillance))) + geom_boxplot() + theme_bw() + scale_y_log10()

## There are systematic differences in the reference and alternative definitions.
  ## Choosing not to apply a crosswalk for cv_endemic given that this could just be a product of a location.
  cv_surveillance <- bundle_crosswalk_collapse(df, 
                                               covariate_name="cv_surveillance", 
                                               age_cut=c(0,1,2,5,20,40,60,80,100), 
                                               year_cut=c(seq(1980,2015,5),2019), 
                                               merge_type="between", 
                                               location_match="country", 
                                               include_logit = T)

  cv_surveillance$location_match <- ifelse(cv_surveillance$location_match=="","Other",cv_surveillance$location_match)
  cv_surveillance$match_nid <- paste0(cv_surveillance$nid,"_",cv_surveillance$n_nid)

  # After age-splitting, we might have multiple rows of same values
    # we can simply find unique match IDs / locations because all surveillance data were age split from 0-99
  cv_surveillance$unique <- paste0(cv_surveillance$match_nid,"_",cv_surveillance$location_match)
  cv_surveillance <- subset(cv_surveillance, !duplicated(unique))

  fit1 <- run_mr_brt(
    output_dir = "filepath",
    model_label = "surveillance",
    data = cv_surveillance,
    mean_var = "logit_ratio",
    se_var = "logit_ratio_se",
    trim_pct = 0.1,
    study_id = "match_nid",
    method = "trim_maxL",
    overwrite_previous = TRUE
  )

  df_pred <- data.frame(intercept = 1)
  pred1 <- predict_mr_brt(fit1, newdata = df_pred)
  pred_object <- load_mr_brt_preds(pred1)
  preds <- pred_object$model_summaries

  # Calculate log se
  preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
  # Convert the mean and standard_error to linear space
  preds$linear_se <- sapply(1:nrow(preds), function(i) {
    ratio_i <- preds[i, "Y_mean"]
    ratio_se_i <- preds[i, "se"]
    deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i)
  })
  preds$ratio <- inv.logit(preds$Y_mean)

  ## Create essentially a forest plot
  mod_data <- fit1$train_data
  mod_data$outlier <- ceiling(abs(mod_data$w - 1))
  mod_data$row_num <- 1:length(mod_data$mean)
  mod_data$label <- with(mod_data, paste0(match_nid,"_",location_match,"_",age_bin,"_",year_bin))
  f <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=logit_ratio + logit_ratio_se*1.96, ymin=logit_ratio - logit_ratio_se*1.96)) + 
    geom_point(aes(y=logit_ratio, x=label)) + geom_errorbar(aes(x=label), width=0) +
    theme_bw() + ylab("Logit ratio") + xlab("") + coord_flip() + ggtitle(paste0("Cholera surveillance settings (",round(preds$ratio,3),")")) +
    geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean, col="purple") +
    geom_rect(data=preds, aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
  print(f)

  # Convert the mean by the crosswalk!
  df$raw_mean <- df$mean
  df$xw_surveillance <- preds$ratio
  df$xw_surv_se <- preds$linear_se
    df$mean <- ifelse(df$cv_surveillance==1, inv.logit(logit(df$mean) + df$xw_surveillance), df$mean)
    std_surv <- sqrt(with(df, standard_error^2 * xw_surv_se^2 + standard_error^2*xw_surveillance^2 + xw_surv_se^2*mean^2))
    df$standard_error <- ifelse(df$cv_surveillance==1, std_surv, df$standard_error)

  ## Make a diagnostic ggplot ##

    df$adjustment_type <- ifelse(df$cv_surveillance == 1, "Surveillance data", "None")
    g <- ggplot(data=subset(df, !is.na(adjustment_type)), aes(x=raw_mean, y=mean, col=adjustment_type)) + geom_point(size=3) + geom_abline(intercept=0, slope=1) + theme_bw(base_size=12) +
      xlab("Raw mean") + ylab("Adjustment mean") + scale_color_discrete("") +
      geom_point(size=3) +
      ggtitle("Cholera")
    g

#######################################################################
# Pull out new data and save in the Upload folder
  og_cases <- df$cases
  og_ss <- df$sample_size

  df$cases <- ifelse(df$cases > df$sample_size, og_ss, df$cases)
  df$sample_size <- ifelse(og_cases > og_ss, og_cases, df$sample_size)

  df$step2_location_year <- ifelse(df$nid==357024, "These data were changed the year so that they could be used","")

  df$crosswalk_parent_seq <- ifelse(!is.na(df$seq),df$seq, df$seq_parent)
  df[is.na(df)] <- ""

  # Pull out new data, duplicate sexes (can't upload sex="Both"), remove group_review = 0, and save in the Upload folder
  xw_sex_df <- duplicate_sex_rows(df)
  xw_sex_df <- xw_sex_df[ , -which(names(xw_sex_df) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split","note_SR",
                                                           "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id",
                                                           "age_sum","raw_mean","inpatient","is_reference","log_inpatient","se_log_inpatient",
                                                           "mrbrt_inpatient","mrbrt_se_inpatient","mrbrt_explicit","mrbrt_se_explicit","adjustment_type"))]
  xw_sex_df$group_review[is.na(xw_sex_df$group_review)] <- 1
  xw_sex_df <- subset(xw_sex_df, group_review != 0)
  xw_sex_df$group_review <- ifelse(xw_sex_df$specificity=="","",xw_sex_df$group_review)
  xw_sex_df$group_review <- ifelse(xw_sex_df$group_review==FALSE,"",xw_sex_df$group_review)
  xw_sex_df[is.na(xw_sex_df)] <- ""
  xw_sex_df$crosswalk_parent_seq <- ifelse(xw_sex_df$crosswalk_parent_seq=="",xw_sex_df$seq,xw_sex_df$crosswalk_parent_seq)
  xw_sex_df$uncertainty_type <- ifelse(xw_sex_df$lower=="","", as.character(xw_sex_df$uncertainty_type))
  xw_sex_df$uncertainty_type_value <- ifelse(xw_sex_df$lower=="","",xw_sex_df$uncertainty_type_value)
  xw_sex_df <- subset(xw_sex_df, location_id != 95)

  write.xlsx(xw_sex_df, paste0("filepath"), sheetName="extraction")

##########################################################################
  ## Start the uploading process
  save_crosswalk_version(bundle_version_id=3500, data_filepath = "filepath",
                         description = "Surveillance crosswalk, updated age split: outliered Hungary, Azerbaijan, Jordan")

