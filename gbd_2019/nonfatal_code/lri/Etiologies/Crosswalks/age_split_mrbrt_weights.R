########################################################
## Test writing age-splitting code for etiologies ##
## This version uses an MR-BRT model to age split the
## number of cases (numerator) and another data set
## to age split the denominator. This results in new
## data points where the sum of the age split rows
## is equal to the all age row for cases and sample_size.
## The denominator_weights can be different things
## such as population, but I currently have the function
## designed to use prevalence for diarrhea/LRI etiologies
## (i.e. the prevalence of diarrhea or LRI).
########################################################

## Define age groups
age_info <- read.csv("filepath")
age_info <- subset(age_info, !is.na(order))
age_info <- subset(age_info, age_group_id != 33)


##---------------------------------------------------------------------------------------

# Must have column called "cv_age_split" that marks which rows should be split
age_split_mrbrt_weights <- function(df, denominator_weights, output_dir, bounded=T, title="", global_merge = T){

  # New value
  denominator_weights$prop_denom <- denominator_weights$number / denominator_weights$total_number

  age_starts <- age_cuts[1:length(age_cuts)-1]
  age_ends <- age_cuts[2:length(age_cuts)]
  age_splits <- (age_starts + age_ends) / 2
  age_dummy <- unique(age_map$age_dummy)

  mean <- df$cases / df$sample_size
  df$mean <- ifelse(is.na(df$mean), mean, df$mean)
  df$pre_age_split_mean <- df$mean
  df$raw_mean <- df$mean
  age_specific <- subset(df, cv_age_split == 0)

  # Pull out age_non_specific, save for later.
    age_non_specific <- subset(df, cv_age_split == 1)
    # set any cut age groups to age_end 99
    age_non_specific$age_end <- ifelse(age_non_specific$age_end != age_non_specific$round_age_end, 99, age_non_specific$age_end)

  # set of non-specific age data for group_review
  group_review <- age_non_specific
  group_review$group <- 1
  group_review$group_review <- 0
  group_review$specificity <- "Non-age specific data"
  group_review$age_end <- 99

  # overall_mean <- mean(age_specific$mean)

  # If data mean = 0, set to linear floor
  l_floor <- median(age_specific$mean[age_specific$mean>0]) * 0.01
  age_specific$f_mean <- ifelse(age_specific$mean==0, l_floor, age_specific$mean)

  # Standard error missing from some new data
  age_specific$sample_size1 <- ifelse(is.na(age_specific$effective_sample_size), age_specific$sample_size, age_specific$effective_sample_size)
  standard_error <- with(age_specific, sqrt(mean * (1-mean) / sample_size1))
  age_specific$standard_error <- ifelse(is.na(age_specific$standard_error), standard_error, age_specific$standard_error)
  age_specific <- subset(age_specific, standard_error > 0)

  ##---------------------------------------------------------------------------
  ## Model 1 is an MR-BRT model of logit mean to get an age pattern
  age_specific$age_mid <- floor((age_specific$age_end + age_specific$age_start) / 2)

  age_specific$logit_mean <- logit(age_specific$f_mean)
  age_specific$delta_logit_se <- sapply(1:nrow(age_specific), function(i) {
    ratio_i <- as.numeric(age_specific[i, "f_mean"])
    ratio_se_i <- as.numeric(age_specific[i, "standard_error"])
    deltamethod(~log(x1 / (1 - x1)), ratio_i, ratio_se_i^2)
  })

  age_specific$group_review[is.na(age_specific$group_review)] <- 1

  input_data <- age_specific[age_specific$delta_logit_se > 0 & age_specific$is_outlier == 0 & !is.na(age_specific$nid) & age_specific$group_review != 0,] 

  # using 999999 for new data NIDs, these are not all the same sources
  input_data$nid <- paste0(input_data$nid,"_",input_data$location_id)

  ## adding data as dummy anchors for older age groups
  age_anchor <- input_data[1:3,]
  age_anchor$logit_mean <- c(quantile(input_data$logit_mean[input_data$age_mid > 20], 0.5),
                             quantile(input_data$logit_mean[input_data$age_mid > 20], 0.5,),
                             quantile(input_data$logit_mean[input_data$age_mid > 20], 0.5,))
  age_anchor$mean <- inv.logit(age_anchor$logit_mean)
  age_anchor$age_mid <- c(75,85,99)
  age_anchor$nid <- 5:7

  input_data <- rbind(input_data, age_anchor)

  # Run an MR-BRT model #
  fit1 <- run_mr_brt(
    output_dir = output_dir,
    model_label = "age_curve",
    data = input_data,
    mean_var = "logit_mean",
    se_var = "delta_logit_se",
    #covs = list(cov_info("age_mid","X", degree = 2, i_knots = "2,5,40")),
    covs = list(cov_info("age_mid","X", degree = 2, n_i_knots = 3)),
    trim_pct = 0.1,
    method = "trim_maxL",
    #study_id = "nid",
    overwrite_previous = TRUE
  )

  df_pred <- data.frame(intercept = 1, age_mid=seq(0,99,0.5))
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
  preds$pred_mean <- inv.logit(preds$Y_mean)

  ## approximation of a funnel plot
  mod_data <- fit1$train_data
  mod_data$outlier <- ceiling(abs(mod_data$w - 1))
  preds$linear_lower <- inv.logit(preds$Y_mean_lo)
  preds$linear_upper <- inv.logit(preds$Y_mean_hi)

  p <- ggplot(preds) + geom_point(data=mod_data, aes(x=age_mid, y=mean, col=factor(outlier)), size=2, alpha=0.5) + geom_line(aes(x=X_age_mid, y=pred_mean)) +
    geom_ribbon(aes(x=X_age_mid, ymin=linear_lower, ymax= linear_upper), alpha=0.5) + ggtitle(title) + theme_bw() + xlab("Age mid") + ylab("Mean proportion") +
    scale_color_manual("Trimmed", values = c("darkblue","red"), labels=c("No","Yes"))
  print(p)

##----------------------------------------------------------------------------------------------------
  ## make a dummy to merge back onto original non-age-specific data.

  ## Determine the fraction of the total proportion from each age group ##

  age_preds <- subset(preds, X_age_mid %in% age_splits)

  # age_curve <- join(age_curve, age_map[,c("order","age_group_id","age_start","age_end","age_pull","age_dummy")], by="age_group_id")
  # age_curve <- subset(age_curve, !is.na(order))
  # age_curve$pred_linear_se <- with(age_curve, (upper - lower)/2/qnorm(0.975))
  # setnames(age_curve, "mean", "pred_mean")

    age_preds$age_mid <- age_preds$X_age_mid

    age_preds <- age_preds[,c("age_mid","pred_mean","pred_linear_se","X_age_mid")]
    age_preds <- cbind(age_preds, age_starts, age_ends, age_dummy)
    age_preds$dummy <- 1
    age_preds$sum_pred <- sum(age_preds$pred_mean)
    age_preds$prop_pred <- age_preds$pred_mean / age_preds$sum_pred

    age_non_specific$dummy <- 1

    # Using seq is a good idea but some data are "new"
    age_non_specific$row_number <- 1:length(age_non_specific$nid)

    age_non_specific <- join(age_non_specific, age_preds, by="dummy")

  ## Now merge with population data ##
    age_non_specific$year_id <- floor((age_non_specific$year_start + age_non_specific$year_end)/2)
  # Has to be greater than equal to 1990
    age_non_specific$year_id <- ifelse(age_non_specific$year_id < 1990, 1990, age_non_specific$year_id)

  if(global_merge == T){
    age_non_specific <- join(age_non_specific, 
                             denominator_weights[,c("year_id","age_dummy","number","total_number","prop_denom")], by=c("year_id","age_dummy"))
  } else {
    age_non_specific <- join(age_non_specific, 
                             denominator_weights[,c("year_id","location_id","age_dummy","number","total_number","prop_denom")], by=c("location_id","year_id","age_dummy"))
  }

  ## Keep if new age splits are within original age ranges
    age_non_specific <- subset(age_non_specific, age_start <= age_starts & age_end >= X_age_mid)

  # recalculate prop_pred, prop_pop for data within age ranges.

    aggregates <- aggregate(cbind(prop_pred, prop_denom) ~ row_number, data=age_non_specific, function(x) sum(x))
    setnames(aggregates, c("prop_pred","prop_denom"), c("study_prop_pred","study_prop_denom"))
    age_non_specific <- join(age_non_specific, aggregates, by="row_number")

  ## Calculate new cases and sample size as the fraction in each age group
    # keep original
    age_non_specific$og_cases <- age_non_specific$cases
    age_non_specific$og_sample_size <- age_non_specific$sample_size

    # Calculate new values
    age_non_specific$cases <- age_non_specific$cases * age_non_specific$prop_pred / age_non_specific$study_prop_pred
    age_non_specific$effective_sample_size <-  age_non_specific$sample_size * age_non_specific$prop_denom / age_non_specific$study_prop_denom
    age_non_specific$sample_size <- age_non_specific$effective_sample_size
    age_non_specific$mean <- age_non_specific$cases / age_non_specific$effective_sample_size

    if(bounded==T){
      age_non_specific$mean <- ifelse(age_non_specific$mean >= 1, 0.99, age_non_specific$mean)
    }


  ## Two ways to calculate SE ##
    age_non_specific$standard_error <- with(age_non_specific, sqrt(mean * (1 - mean) / effective_sample_size))
    age_non_specific$standard_error <- with(age_non_specific, sqrt(standard_error^2 * pred_linear_se^2 + standard_error^2*pred_mean^2 + pred_linear_se^2*raw_mean))


  ## Plot ##
    mlabels <- paste0(age_preds$age_starts," to ",age_preds$age_ends)
    p <- ggplot(age_non_specific, aes(x=pre_age_split_mean, y=mean, col=factor(age_starts), size=1/standard_error^2)) + geom_point() + geom_abline(intercept=0, slope=1) + theme_bw() +
      xlab("Pre split mean") + ylab("Post split mean") + scale_color_discrete("Age group", labels=mlabels) + guides(size=F)
    print(p)

  # Reassign age groups, keep age_end if it is less than the upper bound.
      age_non_specific$og_age_start <- age_non_specific$age_start
      age_non_specific$og_age_end <- age_non_specific$age_end

      age_non_specific$age_start <- age_non_specific$age_starts
      age_non_specific$age_end <- with(age_non_specific, ifelse(age_end < age_ends, age_end, age_ends))

      age_non_specific <- data.table(age_non_specific)
      age_non_specific[, re_age_start := min(age_start), by = "seq"]
      age_non_specific[, re_age_end := max(age_end), by="seq"]

      age_non_specific <- data.frame(age_non_specific)
      age_non_specific$age_start <- with(age_non_specific, ifelse(re_age_start == age_start, og_age_start, age_start))
      age_non_specific$age_end <- with(age_non_specific, ifelse(re_age_end == age_end, og_age_end, age_end))

  # Clean up some columns
  age_non_specific$note_modeler <- paste0(age_non_specific$note_modeler," 
  | these data were split from original age 0-99 using an age curve from MR-BRT. Numerator weights from the MR-BRT model,
  and denominator weights from the global prevalence age curve. The original mean was ",age_non_specific$pre_age_split_mean,".")
  age_non_specific$crosswalk_parent_seq <- age_non_specific$seq
  age_non_specific$seq_parent <- age_non_specific$seq

  age_non_specific$crosswalk_parent_seq <- ifelse(age_non_specific$seq=="", age_non_specific$crosswalk_parent_seq, age_non_specific$seq)
  age_non_specific$seq_parent <- ifelse(age_non_specific$seq=="",age_non_specific$seq_parent, age_non_specific$seq)

  age_non_specific$seq <- ""
  age_non_specific$lower <- ""
  age_non_specific$upper <- ""
  age_non_specific$uncertainty_type <- ""
  age_non_specific$uncertainty_type_value <- ""
  age_non_specific$age_issue <- 0

  # Rbind
  output <- rbind.fill(age_specific, group_review, age_non_specific)

  plot_data <- rbind.fill(age_specific, age_non_specific)
  dot_plot <- ggplot(data=plot_data, aes(x=age_mid, y=mean)) + geom_point(aes(col=factor(cv_age_split))) + theme_bw() + xlab("Age mid") + ylab("Proportion") +
    scale_color_discrete("", labels=c("Age specific","Age split")) + stat_smooth(method="loess", se=F, col="black") +
    annotate(geom="text", label="MR-BRT", col="purple", x=80, y=0.69) +
    annotate(geom="text", label="Loess (all data)", col="black", x=80, y=0.66) +
    geom_line(data=preds, aes(x=X_age_mid, y=pred_mean), col="purple") + ggtitle(title)
  print(dot_plot)

  output <- output[ , -which(names(output) %in% c("f_mean","age_mid","log_ratio","delta_log_se","dummy","age_starts","age_ends","sample_size1","age_dummy","sum_pred","prop_pred","number","total_number",
                                                  "run_id","modelable_entity_name.1","cv.","cv_.","pre_age_split_mean","start_date","end_date","pmid_doi_pmcID_URL","X_age_mid","prop_denom", "row_number",
                                                  "row_num","study_prop_pred","study_prop_denom","prop_pop","og_cases","og_sample_size","re_age_start","re_age_end","og_age_start","og_age_end"))]

  return(output)

}
