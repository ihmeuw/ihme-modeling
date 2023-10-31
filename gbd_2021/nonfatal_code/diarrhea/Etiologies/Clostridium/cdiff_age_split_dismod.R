########################################################
## Age-splitting code for non-fatal data using DisMod
## global pattern to  age split the
## number of cases (numerator) and another data set
## to age split the denominator. This results in new
## data points where the sum of the age split rows
## is equal to the all age row for cases and sample_size.
## The denominator_weights can be different things
## such as population, but I currently have the function
## designed to use prevalence for diarrhea/LRI etiologies
## (i.e. the prevalence of diarrhea or LRI).
########################################################
source("/get_model_results.R")

## Define age groups
age_info <- read.csv("/age_mapping_current.csv")
age_info <- subset(age_info, !is.na(order))
age_info <- subset(age_info, age_group_id != 33)

# Must have column called "cv_age_split" that marks which rows should be split
age_split_dismod <- function(df, denominator_weights, age_map, model_version_id, bounded=T, title="", global_merge = T, sex_id = 2, measure_id, gbd_round_id = 6, location_id=1){
  # New value
  denominator_weights$prop_denom <- denominator_weights$number / denominator_weights$total_number

  # How many age groups to split into?
  # age_splits <- c(0.5,2.5,12.5,30,50,70,90)
  # age_starts <- c(0,1,5,20,40,60,80)
  # age_ends <- c(1,4,19,39,59,79,99)
  # age_dummy <- c("Group 1","Group 2","Group 3","Group 4","Group 5","Group 6","Group 7")
  age_starts <- age_cuts[1:length(age_cuts)-1]
  age_ends <- age_cuts[2:length(age_cuts)]
  age_splits <- (age_starts + age_ends) / 2
  age_dummy <- unique(age_map$age_dummy)

  age_curve <- get_model_results(gbd_team = "epi", model_version_id = model_version_id, location_id = location_id, year_id = 2019, measure_id = measure_id,
                                    age_group_id = "all", sex_id = sex_id, gbd_round_id=gbd_round_id, decomp_step = "step5")

    #age_curve <- join(age_curve, age_info[,c("order","age_group_id","age_start","age_end")], by="age_group_id")
    age_curve <- join(age_curve, age_map[,c("order","age_group_id","age_start","age_end","age_pull","age_dummy")], by="age_group_id")
    age_curve <- subset(age_curve, !is.na(order))
    age_curve$pred_linear_se <- with(age_curve, (upper - lower)/2/qnorm(0.975))
    setnames(age_curve, "mean", "pred_mean")

  mean <- df$cases / df$sample_size
  df$mean <- ifelse(is.na(df$mean), mean, df$mean)
  df$pre_age_split_mean <- df$mean
  df$raw_mean <- df$mean
  age_specific <- subset(df, cv_age_split == 0)
  age_non_specific <- subset(df, cv_age_split == 1)


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

  ##----------------------------------------------------------------------------------------------------
	 ## Determine the fraction of the total proportion from each age group ##
	  age_preds <- aggregate(pred_mean ~ age_dummy+sex_id, data=age_curve, function(x) sum(x))
	
	  # age_preds <- subset(age_curve, age_start %in% age_starts)
	  #   age_preds$X_age_mid <- (age_preds$age_end + age_preds$age_start) / 2
	  #age_preds$age_mid <- age_preds$X_age_mid
	  #age_preds <- age_preds[,c("age_mid","pred_mean","pred_linear_se","X_age_mid")]
	  age_preds <- cbind(age_preds, age_starts, age_ends, age_dummy)

	  age_preds$X_age_mid <- (age_preds$age_ends + age_preds$age_starts) / 2
	  age_preds$dummy <- 1
	  age_preds$sum_pred <- sum(age_preds$pred_mean)
	  age_preds$prop_pred <- age_preds$pred_mean / age_preds$sum_pred

	  age_non_specific$dummy <- 1

	  age_non_specific$row_number <- 1:length(age_non_specific$nid)

	 ## Now merge with population data ##
	  age_non_specific <- join(age_non_specific, age_preds, by=c("dummy"))
	  age_non_specific$year_id <- floor((age_non_specific$year_start + age_non_specific$year_end)/2)

	 # Has to be greater than equal to 1990
	  age_non_specific$year_id <- ifelse(age_non_specific$year_id < 1990, 1990, age_non_specific$year_id)

	  if(global_merge == T){
	    age_non_specific <- join(age_non_specific, denominator_weights[,c("year_id","age_dummy","number","total_number","prop_denom")], by=c("year_id","age_dummy"))
	  } else {
	    age_non_specific <- join(age_non_specific, denominator_weights[,c("year_id","location_id","age_dummy","number","total_number","prop_denom")], by=c("location_id","year_id","age_dummy"))
	  }

	  ## Keep if new age splits are within original age ranges
	    age_non_specific <- subset(age_non_specific, age_start <= age_starts & age_end >= X_age_mid)

	  # recalculate prop_pred, prop_pop for data within age ranges.
	    aggregates <- aggregate(cbind(prop_pred, prop_denom) ~ row_number, data=age_non_specific, function(x) sum(x))
  	  setnames(aggregates, c("prop_pred","prop_denom"), c("study_prop_pred","study_prop_denom"))
  	  age_non_specific <- join(age_non_specific, aggregates, by="row_number")

	  ## Calculate new cases and sample size as the fraction in each age group
	  # keep original just to validate it is working
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


  ## Two possible ways
	age_non_specific$standard_error <- with(age_non_specific, sqrt(mean * (1 - mean) / effective_sample_size))
  #age_non_specific$standard_error <- with(age_non_specific, sqrt(standard_error^2 * pred_linear_se^2 + standard_error^2*pred_mean^2 + pred_linear_se^2*raw_mean))


  # Reassign age groups, keep age_end if it is less than the upper bound
	  age_non_specific$age_start <- age_non_specific$age_starts
	  #age_non_specific$age_end <- age_non_specific$age_ends
	  age_non_specific$age_end <- with(age_non_specific, ifelse(age_end < age_ends, age_end, age_ends))

  # Clean up some columns
	  age_non_specific$note_modeler <- paste0(age_non_specific$note_modeler," | these data were split from original age 0-99 using an age curve from DisMod. Numerator weights from the DisMod model,
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

  output <- output[ , -which(names(output) %in% c("f_mean","age_mid","log_ratio","delta_log_se","dummy","age_starts","age_ends","sample_size1","age_dummy","sum_pred","prop_pred","number","total_number",
                                                  "run_id","modelable_entity_name.1","cv.","cv_.","pre_age_split_mean","start_date","end_date","pmid_doi_pmcID_URL",
                                                  "model_version_id","location_id.1","year_id","age_group_id","sex_id","measure_id","measure.1","pred_mean","lower.1",
                                                  "upper.1","order","age_start.1","age_end.1","pred_linear_se","X_age_mid"))]

  return(output)

}
