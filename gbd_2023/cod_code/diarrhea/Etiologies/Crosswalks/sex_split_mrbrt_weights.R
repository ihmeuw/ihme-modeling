########################################################
## Test writing sex-splitting code for etiologies ##
## This version uses an MR-BRT model to sex split the
## number of cases (numerator) and another data set
## to sex split the denominator. This results in new
## data points where the sum of the sex split rows
## is equal to the both sex row for cases and sample_size.
## The denominator_weights can be different things
## such as population, but I currently have the function
## designed to use prevalence for diarrhea/LRI etiologies
## (i.e. the prevalence of diarrhea or LRI).
########################################################
## Function two simply duplicates rows of data where
## sex=="Both" and keeps the same mean, sample size, etc.
########################################################

## Create prevalence weights to sex-split the data from the global diarrhea prevalence sex pattern in GBD 2017 ##

##--------------------------------------------------------------------------------------
library(plyr)

sex_split_mrbrt_weights <- function(df, denominator_weights, output_dir, bounded=T, title="", global_merge = T){
  df$pre_sex_split_mean <- df$mean
  df$cv_sex_split <- ifelse(df$sex == "Both", 0, 1)

  sex_specific <- subset(df, sex!="Both")
  sex_agnostic <- subset(df, sex=="Both")

  group_review <- sex_agnostic
  group_review$group <- 1
  group_review$group_review <- 0
  group_review$specificity <- "Non-sex specific data"

  # If data mean = 0, set to linear floor
  l_floor <- median(sex_specific$mean[sex_specific$mean>0]) * 0.01
  sex_specific$f_mean <- ifelse(sex_specific$mean==0, l_floor, sex_specific$mean)

  # Standard error missing from some new data
  sex_specific$sample_size1 <- ifelse(is.na(sex_specific$effective_sample_size), sex_specific$sample_size, sex_specific$effective_sample_size)
  standard_error <- with(sex_specific, sqrt(mean * (1-mean) / sample_size1))
  sex_specific$standard_error <- ifelse(is.na(sex_specific$standard_error), standard_error, sex_specific$standard_error)

  ##---------------------------------------------------------------------------
  ## Model 1 is an MR-BRT model of logit mean to get an sex pattern
  sex_specific$sex_id <- ifelse(sex_specific$sex=="Male",1,2)

  sex_specific$logit_mean <- logit(sex_specific$f_mean)
  sex_specific$delta_logit_se <- sapply(1:nrow(sex_specific), function(i) {
    ratio_i <- sex_specific[i, "f_mean"]
    ratio_se_i <- sex_specific[i, "standard_error"]
    deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
  })

  # Run an MR-BRT model #
  fit1 <- run_mr_brt(
    output_dir = output_dir,
    model_label = "sex_curve",
    data = sex_specific,
    mean_var = "logit_mean",
    se_var = "delta_logit_se",
    covs = list(cov_info("sex_id","X")),
    trim_pct = 0.1,
    method = "trim_maxL",
    #study_id = "nid",
    overwrite_previous = TRUE
  )

  check_for_outputs(fit1)
  df_pred <- data.frame(intercept = 1, sex_id=c(1,2))
  pred1 <- predict_mr_brt(fit1, newdata = df_pred)
  check_for_preds(pred1)
  pred_object <- load_mr_brt_preds(pred1)
  preds <- pred_object$model_summaries

  # Calculate log se
  preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
  # Convert the mean and standard_error to linear space
  preds$pred_linear_se <- sapply(1:nrow(preds), function(i) {
    ratio_i <- preds[i, "Y_mean"]
    ratio_se_i <- preds[i, "se"]
    deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i)
  })
  preds$sex_pred_mean <- exp(preds$Y_mean)

  mod_data <- fit1$train_data
  mod_data$outlier <- ceiling(abs(mod_data$w - 1))
  preds$linear_lower <- with(preds, sex_pred_mean - pred_linear_se*1.96)
  preds$linear_lower <- ifelse(preds$linear_lower < 0, 0.001, preds$linear_lower)

  p <- ggplot(preds) + geom_point(data=mod_data, aes(x=sex_id, y=mean, col=factor(outlier))) + geom_line(aes(x=X_sex_id, y=sex_pred_mean)) +
    geom_ribbon(aes(x=X_sex_id, ymin=linear_lower, ymax= sex_pred_mean + pred_linear_se*1.96), alpha=0.5) + ggtitle(title) + theme_bw() + xlab("Age mid") + ylab("Mean proportion") + scale_color_discrete("Outlier")
  print(p)

  ## Determine the fraction of the total proportion from each age group ##
  sex_preds <- preds
  sex_preds$sex_id <- sex_preds$X_sex_id

  sex_preds <- sex_preds[,c("sex_id","sex_pred_mean","pred_linear_se")]
  sex_preds$dummy <- 1
  sex_preds$sex_sum_pred <- sum(sex_preds$sex_pred_mean)
  sex_preds$sex_prop_pred <- sex_preds$sex_pred_mean / sex_preds$sex_sum_pred

  sex_agnostic$dummy <- 1
  sex_agnostic <- join(sex_agnostic, sex_preds, by="dummy")

  denom_sum <- sum(denominator_weights$number)
  denominator_weights$prop_sex_w <- denominator_weights$number / denom_sum

  sex_agnostic <- join(sex_agnostic, denominator_weights[,c("prop_sex_w","sex_id")], by="sex_id")

  sex_agnostic$cases <- sex_agnostic$cases * sex_agnostic$sex_prop_pred
  sex_agnostic$effective_sample_size <- sex_agnostic$sample_size * sex_agnostic$prop_sex_w
  sex_agnostic$mean <- sex_agnostic$cases / sex_agnostic$effective_sample_size

  sex_agnostic$standard_error <- with(sex_agnostic, sqrt(mean * (1 - mean) / effective_sample_size))
  sex_agnostic$standard_error <- with(sex_agnostic, sqrt(standard_error^2 * pred_linear_se^2 + standard_error^2*pred_mean^2 + pred_linear_se^2*raw_mean))

  if(bounded==T){
    sex_agnostic$mean <- ifelse(sex_agnostic$mean >= 1, 0.99, sex_agnostic$mean)
  }

  # Clean up some columns
  sex_agnostic$note_modeler <- paste0(sex_agnostic$note_modeler," | these data were split from original sex == Both from MR-BRT. Numerator weights from the MR-BRT model,
                                                                                and denominator weights from the global prevalence sex distribution. The original mean was ",sex_agnostic$pre_sex_split_mean,".")
  sex_agnostic$crosswalk_parent_seq <- sex_agnostic$seq
  sex_agnostic$seq <- ""
  sex_agnostic$lower <- ""
  sex_agnostic$upper <- ""
  sex_agnostic$uncertainty_type <- ""
  sex_agnostic$uncertainty_type_value <- ""
  sex_agnostic$age_issue <- 0

  # Rbind
  output <- rbind.fill(sex_specific, group_review, sex_agnostic)
  output <- output[ , -which(names(output) %in% c("f_mean","age_mid","log_ratio","delta_log_se","dummy","age_starts","age_ends","sample_size1","age_dummy","sum_pred","prop_pred","number","total_number",
                                                  "run_id","modelable_entity_name.1","cv.","cv_.","pre_age_split_mean","start_date","end_date","pmid_doi_pmcID_URL", "prop_sex_w","sex_pred_mean"))]

  return(output)
}

##---------------------------------------------------------------------------
## Duplicate sex rows in data set
##---------------------------------------------------------------------------

duplicate_sex_rows <- function(df) {
  sex_specific <- subset(df, sex != "Both")
  sex_agnostic <- subset(df, sex == "Both")

  sex_agnostic$note_modeler <- paste0(sex_agnostic$note_modeler, " | these data were originally sex == 'Both' and have been copied (no change to mean, sample size).")
  sex_agnostic$crosswalk_parent_seq <- ifelse(sex_agnostic$seq=="", sex_agnostic$crosswalk_parent_seq, sex_agnostic$seq)
  sex_agnostic$seq_parent <- ifelse(sex_agnostic$seq=="", sex_agnostic$seq_parent, sex_agnostic$seq)

  males <- sex_agnostic
    males$sex <- "Male"
    males$seq <- ""
  females <- sex_agnostic
    females$sex <- "Female"
    females$seq <- ""

  output <- rbind(sex_specific, males, females)

  return(output)
}

##---------------------------------------------------------------------------
## Sex split rows in data set using single numerator scalar
##---------------------------------------------------------------------------

# Scalar should be proportion Male
scalar_sex_rows <- function(df, scalar="", denominator_weights, bounded=T, title="", global_merge = T) {
  df$cases <- as.numeric(df$cases)
  df$sample_size <- as.numeric(df$sample_size)

  sex_specific <- subset(df, sex != "Both")
  sex_agnostic <- subset(df, sex == "Both")

    if(nrow(sex_agnostic > 0)) {
      sex_agnostic$row_num <- 1:length(sex_agnostic$mean)
    }
  
  males <- sex_agnostic
  males$sex <- "Male"
  males$sex_id <- 1
  females <- sex_agnostic
  females$sex <- "Female"
  females$sex_id <- 2

  sex_agnostic <- rbind(males, females)
  sex_agnostic$crosswalk_parent_seq <- sex_agnostic$seq
  sex_agnostic$seq_parent <- sex_agnostic$seq
  sex_agnostic$seq <- ""

  denom_sum <- sum(denominator_weights$number)
  denominator_weights$prop_sex_w <- denominator_weights$number / denom_sum

  sex_agnostic <- join(sex_agnostic, denominator_weights[,c("prop_sex_w","sex_id")], by="sex_id")

  if(scalar == ""){
    scalar_val <- mean(sex_specific$mean[sex_specific$sex=="Male"], na.rm=T) / (mean(sex_specific$mean[sex_specific$sex=="Male"], na.rm=T) + mean(sex_specific$mean[sex_specific$sex=="Female"], na.rm=T))
  } else {
    scalar_val <- scalar
  }

  sex_agnostic$cases <- with(sex_agnostic, ifelse(sex=="Male", cases * scalar_val, cases * (1-scalar_val)))
  sex_agnostic$effective_sample_size <- sex_agnostic$sample_size * sex_agnostic$prop_sex_w
  sex_agnostic$mean <- as.numeric(sex_agnostic$cases / sex_agnostic$effective_sample_size)


  sex_agnostic$note_modeler <- paste0(sex_agnostic$note_modeler, " | these data were originally sex == 'Both' and have been changed using a single scalar.")

  output <- rbind.fill(sex_specific, sex_agnostic)

  return(output)
}
