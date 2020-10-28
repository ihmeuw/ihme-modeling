####################################################################
## This code produces crosswalks for influenza and RSV ##
## These etiologies have two crosswalks:
## Inpatient and ELISA diagnostic. This should be relatively straight-
## forward and follows the pattern written previously for diarrheal
## etiologies.
#####################################################################

# Prepare your needed functions and information
library(metafor)
library(msm)
library(plyr)
library(boot)
library(data.table)
library(openxlsx)
library(ggplot2)

source() # MR BRT central functions
source("filepath/bundle_crosswalk_collapse.R")
source("filepath/age_split_mrbrt_weights.R")
source("filepath/sex_split_mrbrt_weights.R")

eti_info <- read.csv("filepath")
eti_info <- subset(eti_info, model_source=="dismod" & cause_id==322)
eti_info$name_abv <- c("influenza","rsv")

# DisMod values
dmvs <- data.frame(rei_name=c("Influenza","Respiratory syncytial virus"), value_inpatient = c(0.77,1.21), value_elisa=c(0.75,1))

## Find Saudi Arabia subnationals, England areas (like Northwest, West Midlands) to remove ##
locs_old <- read.csv("filepath")
bad_ids <- c(locs_old$location_id[locs_old$parent_id==152], locs_old$location_id[locs_old$parent_id==73 & locs_old$most_detailed==0])
bad_ids <- ifelse(bad_ids==93,95,bad_ids)

##---------------------------------------------------------------------------
pdf("filepath", height=8, width=9)
out_res <- data.frame()

for(i in 1:2){
  name <- eti_info$name_colloquial[i]
  name_abv <- eti_info$name_abv[i]
  me_name <- eti_info$modelable_entity[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]

  print(paste0("Modeling an MR-BRT crosswalk for ", name))

  df <- read.csv("filepath") # bundle data

  # Some rows have missing means
    means <- df$cases / df$sample_size
  df$mean <- ifelse(is.na(df$mean), means, df$mean)

  # Keep if proportion
  df <- subset(df, measure=="proportion")

  # Drop problematic location_ids
  df <- subset(df, !(location_id %in% bad_ids))

##################################################################################################
  ## Age split these data!! ##
##################################################################################################
  ## Determine where age splitting should occur. ##
  age_map <- read.csv("filepath")
  age_map <- age_map[age_map$age_pull==1,c("age_group_id","age_start","age_end","order","age_pull")]
  age_cuts <- c(0,1,5,10,20,40,60,100)
  length <- length(age_cuts) - 1

  age_map$age_dummy <- cut(age_map$age_end, age_cuts, labels = paste0("Group ", 1:length))

  ## Create prevalence weights to age-split the data from the global LRI prevalence age pattern in GBD 2017 ##
  dmod_global <- get_outputs(topic="cause", cause_id=322, age_group_id = age_info$age_group_id, year_id=1990:2017, location_id=1, sex_id=3, measure_id=5, metric_id=1, gbd_round_id=5)
  dmod_global$number <- dmod_global$val
  denom_weights <- dmod_global
  denom_weights <- join(denom_weights, age_map[,c("age_group_id","age_dummy","order")], by="age_group_id")

  denom_age_groups <- aggregate(number ~ location_id + year_id + age_dummy, data=denom_weights, function(x) sum(x))
  denom_all_ages <- aggregate(number ~ location_id + year_id, data=denom_weights, function(x) sum(x))
  denom_all_ages$total_number <- denom_all_ages$number

  denominator_weights <- join(denom_age_groups, denom_all_ages[,c("location_id","year_id","total_number")], by=c("location_id","year_id"))

##---------------------------------------------------
  # # Pull out age specific data
    df$age_sum <- df$age_start + df$age_end

  # Part 1Test a dummy variable where age_start/end rounded doesn't equal age_start/end
  df$round_age_start <- round(df$age_start, 1)
  df$round_age_end <- round(df$age_end, 1)
  df$age_sum <- df$age_start + df$age_end

  df$dummy_age_split <- with(df, ifelse(age_end > 10, ifelse(df$round_age_start != df$age_start, 1, ifelse(df$round_age_end != df$age_end, 1, 0)), 0))

  # Part 2, where the age_range is not 0-99
  df$dummy_age_split <- ifelse(df$dummy_age_split==1,1,ifelse(df$age_start == 0 & df$age_end == 99,1,0))

  # Part 3, where the age_start/end rounded isn't equal to the unrounded value
  df$dummy_age_split <- ifelse(df$dummy_age_split==1,1,ifelse(df$note_modeler %like% "riginal age end", 1, 0))

  age_specific <- subset(df, dummy_age_split == 0)
  age_non_specific <- subset(df, dummy_age_split == 1)

  # Some age-split data have already been duplicated, all rows should be removed
  age_specific$cv_age_split <- 0
  age_non_specific$cv_age_split <- 1

  ##------------------------------------------------------
  # Get rid of now duplicated age split data #
  age_group_review <- subset(age_non_specific, age_sum > 100)
  age_group_review <- data.frame(seq = age_group_review[,"seq"])

  write.csv(age_group_review, "filepath", row.names=F)
  ##------------------------------------------------------

  age_non_specific <- subset(age_non_specific, age_sum < 100)

  df <- rbind(age_specific, age_non_specific)

##---------------------------------------------------------------
  # This is the function I wrote to do the age splitting. It takes
  # an estimated ratio of the observed to expected data for all age-specific
  # data points and runs that through MR-BRT. The result is then predicted
  # for five age groups which are then merged 1:m with the age-non-specific
  # data to make five new points with the new proportion equal to the
  # original mean times the modeled ratio. It keeps a set of rows
  # from the age-non-specific data to mark as group_review = 1.

  age_df <- age_split_mrbrt_weights(df, denominator_weights, output_dir = "filepath", bounded=T, title=name)

##----------------------------------------------------------------

  ## Create a file to fix the data so that duplicates by age are cleared and group_review = 0 in the bundle
  data_for_bundle <- subset(age_df, specificity == "Non-age specific data")
  data_for_bundle <- rbind.fill(data_for_bundle, age_group_review)
  data_for_bundle[is.na(data_for_bundle)] <- ""
  data_for_bundle <- subset(data_for_bundle, nid!=999999)
  data_for_bundle <- data_for_bundle[ , -which(names(data_for_bundle) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split","age_sum",
                                                                             "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id"))]

  write.xlsx(data_for_bundle, "filepath", sheetName="extraction")

##--------------------------------------------------------------
  # Pull out new data, duplicate sexes (can't upload sex="Both") and save in the Upload folder
  age_sex_df <- duplicate_sex_rows(age_df)
  age_sex_df <- age_sex_df[ , -which(names(age_sex_df) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split",
                                                              "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id"))]
  age_sex_df$group_review[is.na(age_sex_df$group_review)] <- 1
  age_sex_df <- subset(age_sex_df, group_review != 0)
  age_sex_df[is.na(age_sex_df)] <- ""
  age_sex_df$group_review <- ifelse(age_sex_df$specificity=="","",age_sex_df$group_review)
  age_sex_df$uncertainty_type <- ifelse(age_sex_df$lower=="", "", age_sex_df$uncertainty_type)
  age_sex_df$uncertainty_type_value <- ifelse(age_sex_df$lower=="", "", age_sex_df$uncertainty_type_value)

  # After Crosswalking, resetting cases, sometimes cases > sample size
    age_sex_df$cases <- as.numeric(age_sex_df$cases)
    age_sex_df$sample_size <- as.numeric(age_sex_df$sample_size)
    age_sex_df$cases <- ifelse(age_sex_df$cases > age_sex_df$sample_size, age_sex_df$sample_size * 0.99, age_sex_df$cases)

  write.xlsx(age_sex_df, "filepath", sheetName="extraction")

  df <- age_df

  df$raw_mean <- df$mean
  df$cv_inpatient[is.na(df$cv_inpatient)] <- 0
  df$is_reference <- ifelse(df$cv_inpatient==1, 0, ifelse(df$cv_diag_elisa==1,0,1))

####################################################################################################
  #### Inpatient sample population ####
####################################################################################################

    g <- ggplot(df, aes(x=factor(cv_inpatient), y=mean, col=factor(cv_inpatient))) + 
    geom_boxplot() + theme_bw() + geom_point(alpha=0.5) + xlab("Inpatient") + 
    ylab("Mean") + guides(col=F) +
      ggtitle(paste0(name,"\nInpatient"))
    print(g)

    df$is_reference <- ifelse(df$cv_inpatient==1,0,1)

# Subset outliers and data extracted only for crosswalks (group = 99)
   cv_inpatient <- bundle_crosswalk_collapse(df, covariate_name="cv_inpatient", include_logit = T,
                                             age_cut=c(0,1,5,20,40,60,80,100), year_cut=c(seq(1980,2015,5),2019), merge_type="within", location_match="exact")
   cv_inpatient$study <- paste0(cv_inpatient$nid,"_",cv_inpatient$location_match)

  # Launch MR-BRT #
  fit1 <- run_mr_brt(
    output_dir = "filepath",
    model_label = "cv_inpatient",
    data = cv_inpatient,
    mean_var = "logit_ratio",
    se_var = "logit_ratio_se",
    #covs = list(cov_info("prevalence","X")),
    trim_pct = 0.1,
    method = "trim_maxL",
    overwrite_previous = TRUE
  )

    check_for_outputs(fit1)
    df_pred <- data.frame(intercept = 1)
    pred1 <- predict_mr_brt(fit1, newdata = df_pred)
    check_for_preds(pred1)
    pred_object <- load_mr_brt_preds(pred1)
    preds <- pred_object$model_summaries

  # Calculate log se
    preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
    preds$linear_se <- deltamethod(~exp(x1)/(1+exp(x1)), preds$Y_mean, preds$se^2)
    preds$ratio <- inv.logit(preds$Y_mean)
    preds$transformation <- "logit"

  ## produce approximation of a funnel plot
    mod_data <- fit1$train_data
    mod_data$outlier <- abs(mod_data$w - 1)
    mod_data$row_num <- 1:length(mod_data$mean)
    f <- ggplot(mod_data, aes(x=logit_ratio, y=logit_ratio_se, col=factor(outlier))) + geom_point(size=3) + 
      geom_vline(xintercept=preds$Y_mean) + scale_y_reverse("Standard error") +
      theme_bw() + scale_x_continuous("Logit difference") + scale_color_manual("", labels=c("Used","Trimmed"), values=c("#0066CC","#CC0000")) + 
      ggtitle(paste0(name, " inpatient ratio")) +
      geom_vline(xintercept=preds$Y_mean_lo, lty=2) + geom_vline(xintercept=preds$Y_mean_hi, lty=2)
    print(f)
  ## Create essentially a forest plot
    mod_data$label <- with(mod_data, paste0(nid,"_",location_match,"_",age_bin,"_",year_bin,"_",row_num))
    f <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=logit_ratio + logit_ratio_se*1.96, ymin=logit_ratio - logit_ratio_se*1.96)) + geom_point(aes(y=logit_ratio, x=label)) +
          geom_errorbar(aes(x=label), width=0) +
          theme_bw() + ylab("Logit difference") + xlab("") + coord_flip() + ggtitle(paste0(name, " inpatient ratio (",round(exp(preds$Y_mean),3),")")) +
          geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean, col="purple") +
          geom_rect(data=preds, aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
    print(f)

  # Keep a record of the values
    p <- preds[,c("ratio","linear_se", "Y_mean","Y_mean_lo","Y_mean_hi")]
    p$etiology <- name
    p$rei_name <- rei_name
    p$variable <- "cv_inpatient"
    p$count_obs <- length(cv_inpatient$ratio)
    out_res <- rbind(out_res, p)

    df$log_inpatient <- preds$Y_mean
    df$se_log_inpatient <- preds$se
    df$mrbrt_inpatient <- preds$ratio
    df$mrbrt_se_inpatient <- preds$linear_se

    p_inpatient <- preds$ratio
    p_inpatient_se <- preds$linear_se

  out_inp <- p

  # Convert the mean by the crosswalk

    df$logit_mean <- logit(df$mean)
    df$mean <- ifelse(df$cv_inpatient==1, inv.logit(df$logit_mean + preds$Y_mean), df$mean)

    df$mean <- ifelse(df$raw_mean == 0, 0, df$mean)

    std_inpatient <- sqrt(with(df, standard_error^2 * mrbrt_se_inpatient^2 + standard_error^2*mrbrt_inpatient^2 + mrbrt_se_inpatient^2*mean))
    df$standard_error <- ifelse(df$cv_inpatient==1, std_inpatient, df$standard_error)

####################################################################################################
  #### Use of ELISA for diagnostic testing ####
####################################################################################################

    # This file has validation study data for use of ELISA/PCR #
elisa_df <- read.csv(paste0("filepath"))

    g <- ggplot(elisa_df, aes(x=factor(cv_diag_elisa), y=mean, col=factor(cv_diag_elisa))) + 
      geom_boxplot() + theme_bw() + geom_point(alpha=0.5) + 
      xlab("ELISA") + ylab("Mean") + guides(col=F) +
      ggtitle(paste0(name,"\nUse of ELISA as diagnostic"))
    print(g)

  elisa_df$is_reference <- ifelse(elisa_df$cv_diag_elisa==0 & elisa_df$cv_diag_pcr==1 & elisa_df$cv_inpatient==0,1,0)

  cv_elisa <- bundle_crosswalk_collapse(elisa_df[elisa_df$is_outlier==0,], 
                                        covariate_name="cv_diag_elisa", 
                                        age_cut=c(0,1,5,10,20,40,60,80,100), 
                                        year_cut=c(seq(1980,2015,5),2019),
                                        merge_type="within", 
                                        location_match="exact", 
                                        include_logit=T)
    cv_elisa <- subset(cv_elisa, ratio!=1)
    cv_elisa$study <- paste0(cv_elisa$nid,"_",cv_elisa$location_match)

  # Launch MR-BRT
  fit1 <- run_mr_brt(
    output_dir = "filepath",
    model_label = "cv_elisa",
    data = cv_elisa,
    mean_var = "logit_ratio",
    se_var = "logit_ratio_se",
    trim_pct = 0.1,
    study_id = "study",
    method = "trim_maxL",
    overwrite_previous = TRUE
  )

#######################################################################################################
    check_for_outputs(fit1)
    df_pred <- data.frame(intercept = 1)
    pred1 <- predict_mr_brt(fit1, newdata = df_pred)
    check_for_preds(pred1)
    pred_object <- load_mr_brt_preds(pred1)
    preds <- pred_object$model_summaries

  # Calculate log se
    preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
    preds$linear_se <- deltamethod(~exp(x1)/(1+exp(x1)), preds$Y_mean, preds$se^2)
    preds$ratio <- inv.logit(preds$Y_mean)
    preds$transformation <- "logit"

  # plot_mr_brt(fit1)

  ## produce approximation of a funnel plot
    mod_data <- fit1$train_data
    mod_data$outlier <- abs(mod_data$w - 1)
    f <- ggplot(mod_data, aes(x=logit_ratio, y=logit_ratio_se, col=factor(outlier))) + 
      geom_point(size=3) + 
      geom_vline(xintercept=preds$Y_mean) + 
      scale_y_reverse("Standard error") +
      theme_bw() + 
      scale_x_continuous("Logit difference") + 
      scale_color_manual("", labels=c("Used","Trimmed"), values=c("#0066CC","#CC0000")) + 
      ggtitle(paste0(name, " ELISA"))
    print(f)
  ## Create essentially a forest plot
    mod_data$row_num <- 1:length(mod_data$mean)
    mod_data$label <- with(mod_data, paste0(location_match,"_",age_bin,"_",year_bin))
    mod_data$label <- with(mod_data, paste0(location_match,"_",age_bin,"_",year_bin,"_",row_num))
    f <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=logit_ratio + logit_ratio_se*1.96, ymin=logit_ratio - logit_ratio_se*1.96)) + 
      geom_point(aes(y=logit_ratio, x=label)) + geom_errorbar(aes(x=label), width=0) +
      theme_bw() + ylab("Logit difference") + xlab("") + coord_flip() + ggtitle(paste0(name, " ELISA (",round(preds$ratio,3),")")) +
      geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean, col="purple") +
      geom_rect(data=preds, aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
    print(f)

  # Keep a record of the values
    p <- preds[,c("ratio","linear_se", "Y_mean","Y_mean_lo","Y_mean_hi")]
    p$etiology <- name
    p$rei_name <- rei_name
    p$variable <- "cv_elisa"
    p$count_obs <- length(cv_elisa$ratio)
    out_res <- rbind(out_res, p)

    df$mrbrt_elisa <- preds$ratio
    df$mrbrt_se_elisa <- preds$linear_se
    p_elisa <- preds$ratio
    p_elisa_se <- preds$linear_se

  # Convert the mean by the crosswalk
    #df$mean <- ifelse(df$cv_diag_elisa==1, df$mean * df$mrbrt_elisa, df$mean)
    df$logit_mean <- logit(df$mean)
    df$mean <- ifelse(df$cv_diag_elisa==1, inv.logit(df$logit_mean + preds$Y_mean), df$mean)
    std_elisa <- sqrt(with(df, standard_error^2 * mrbrt_se_elisa^2 + standard_error^2*mrbrt_elisa^2 + mrbrt_se_elisa^2*mean))
    df$standard_error <- ifelse(df$cv_diag_elisa==1, std_elisa, df$standard_error)

    df$mean <- ifelse(df$raw_mean == 0, 0, df$mean)

  # Adjust data greater than 1 post crosswalk
    df$mean <- ifelse(df$mean >= 1, 0.99, df$mean)

  ## Make a diagnostic ggplot ##
    df$adjustment_type <- ifelse(df$cv_inpatient==1 & df$cv_diag_elisa==1, 
                                 "Inpatient and ELISA", 
                                 ifelse(df$cv_inpatient==1, "Inpatient",
                                        ifelse(df$cv_diag_elisa==1,"ELISA","Reference")))
    g <- ggplot(data=subset(df, !is.na(adjustment_type)), aes(x=raw_mean, y=mean, col=adjustment_type)) + 
      geom_point(size=3) + 
      geom_abline(intercept=0, slope=1) + 
      theme_bw(base_size=12) +
      xlab("Raw mean") + ylab("Adjustment mean") +
      geom_abline(intercept=0, slope=1/dmvs$value_inpatient[i], lty=2, col="#669933") + geom_abline(intercept=0, slope=1/dmvs$value_elisa[i], lty=2, col="#FF0033") +
      geom_point(size=3) + scale_color_discrete("") +
      ggtitle(paste0(name,"\nInpatient ratio ", round(p_inpatient,3)," (SE = ",round(p_inpatient_se,3),") [DisMod decomp1: ",round(1/dmvs$value_inpatient[i],3),"]",
                     "\nELISA ratio ",round(p_elisa,3)," (SE = ",round(p_elisa_se,3),") [DisMod decomp1: ",round(1/dmvs$value_elisa[i],3),"]"))
    print(g)

  ## Save the modified file ##
    df$note_modeler <- paste0(df$note_modeler, " | The crosswalks for cv_inpatient and cv_elisa were performed using MR-BRT. Both were merged to nearby data based on location.")

  ## Remove data only used for crosswalk ##
    df <- subset(df, nid != 999999)
  # subset out Saudi subnationals and other unusable location_ids #
    df <- subset(df, !(location_id %in% c(bad_ids, 71, 23)))

    write.csv(df, "filepath", row.names=F)

  # Pull out new data and save in the Upload folder
    diag_sex_df <- duplicate_sex_rows(df)

    lri_sex_weights <- get_outputs(topic="cause", cause_id=322, age_group_id=22, year_id=2017, gbd_round_id=5, measure_id=6, sex_id=c(1,2), location_id=1, metric_id=3)
      lri_sex_weights$number <- lri_sex_weights$val
    diag_sex_df <- scalar_sex_rows(df, denominator_weights=lri_sex_weights, bounded=T, global_merge=T, title=name_abv)
      diag_sex_df$mean <- as.numeric(diag_sex_df$mean)

  # Make a nice ggplot for sex splitting
    p <- ggplot(data=diag_sex_df, aes(x=factor(age_start), y=mean, col=sex)) + geom_boxplot() + theme_bw()
    print(p)

    diag_sex_df <- diag_sex_df[ , -which(names(diag_sex_df) %in% c("cv_explicit","cv_pcr","round_age_start","round_age_end","dummy_age_split",
                                                                   "cv_age_split","logit_mean","delta_logit_se", "pred_mean","pred_linear_se","year_id"))]

    diag_sex_df$group_review[is.na(diag_sex_df$group_review)] <- 1
    diag_sex_df <- subset(diag_sex_df, group_review != 0)
    diag_sex_df[is.na(diag_sex_df)] <- ""
    diag_sex_df$group_review <- ifelse(diag_sex_df$specificity=="","",diag_sex_df$group_review)
    diag_sex_df$uncertainty_type <- ifelse(diag_sex_df$lower=="", "", diag_sex_df$uncertainty_type)
    diag_sex_df$uncertainty_type_value <- ifelse(diag_sex_df$lower=="", "", diag_sex_df$uncertainty_type_value)

    # After Crosswalking, resetting cases, sometimes cases > sample size
      diag_sex_df$cases <- as.numeric(diag_sex_df$cases)
      diag_sex_df$sample_size <- as.numeric(diag_sex_df$sample_size)
      diag_sex_df$cases <- ifelse(diag_sex_df$cases > diag_sex_df$sample_size, diag_sex_df$sample_size * 0.99, diag_sex_df$cases)

    write.xlsx(diag_sex_df, "filepath", sheetName="extraction")

}
dev.off()
## Save the summary file of all the crosswalks
write.csv(out_res, "filepath", row.names=F)

