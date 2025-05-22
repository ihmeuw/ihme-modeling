########################################################
## This version uses last round's etiology dismod models to age split the
## number of cases (numerator) and another data set
## to age split the denominator. This results in new
## data points where the sum of the age split rows
## is equal to the all age row for cases and sample_size.
## The denominator_weights can be different things
## such as population, but I currently have the function  
## designed to use prevalence for diarrhea/LRI etiologies
## (i.e. the prevalence of diarrhea or LRI).
########################################################
set.seed(7)
library(reticulate)
library(ggplot2)
reticulate::use_python("/FILEPATH/python")
mr <- reticulate::import("mrtool")
library(data.table)

# Must have column called "cv_age_split" that marks which rows should be split
age_split_dismod_weights <- function(df, case_weights, denominator_weights, me_name, bundle, output_dir, bounded=T, title="", global_merge = T, date){
  me_name <- me_name
  bundle_id <- bundle
  
  diarrhea_dir <- paste0("/FILEPATH/gbd_", gbd_year, "/", date, "/")
  vetting_dir <- paste0(diarrhea_dir, "age_splitting/",me_name,"/")
  dir.create(vetting_dir, recursive = T, showWarnings = F)
  
  # New value
  denominator_weights$prop_denom <- denominator_weights$number / denominator_weights$total_number
  
  age_starts <- age_cuts[1:length(age_cuts)-1]
  age_ends <- age_cuts[2:length(age_cuts)]
  age_splits <- (age_starts + age_ends) / 2
  age_dummy <- unique(age_map$age_dummy)
  age_dummy <- age_dummy[!is.na(age_dummy)]
  
  mean <- df$cases / df$sample_size
  df$mean <- ifelse(is.na(df$mean), mean, df$mean)
  df$pre_age_split_mean <- df$mean
  df$raw_mean <- df$mean
  df$rownum <- 1:nrow(df)
  age_specific <- subset(df, cv_age_split == 0)
  age_specific$crosswalk_parent_seq <- age_specific$seq
  
  # Pull out age_non_specific, save for later.
  age_non_specific <- subset(df, cv_age_split == 1)
  
  # set of non-specific age data for group_review
  group_review <- age_non_specific
  group_review$group <- 1
  group_review$group_review <- 0
  group_review$specificity <- "Non-age specific data"
  group_review$crosswalk_parent_seq <- group_review$seq
  
  # If data mean = 0, set to linear floor
  l_floor <- median(age_specific$mean[age_specific$mean>0]) * 0.1
  age_specific$f_mean <- ifelse(age_specific$mean==0, l_floor, age_specific$mean)
  
  # Standard error missing from some new data
  age_specific$sample_size1 <- ifelse(is.na(age_specific$effective_sample_size), age_specific$sample_size, age_specific$effective_sample_size)
  standard_error <- with(age_specific, sqrt(mean * (1-mean) / sample_size1))
  age_specific$standard_error <- ifelse(is.na(age_specific$standard_error), standard_error, age_specific$standard_error)
  age_specific <- subset(age_specific, standard_error > 0)
  age_specific$group_review[is.na(age_specific$group_review)] <- 1
  age_specific$age_mid <- floor((age_specific$age_end + age_specific$age_start) / 2)
  
  case_weights$expected_age_width <- case_weights$age_ends - case_weights$age_starts
  case_weights$dummy <- 1
  write.csv(case_weights, paste0(vetting_dir, "case_weights.csv"), row.names=FALSE)
  
  age_non_specific$dummy <- 1
  
  age_non_specific$row_number <- 1:length(age_non_specific$nid)
  age_non_specific <- join(age_non_specific, case_weights, by="dummy")
  
  age_non_specific$year_id <- floor((age_non_specific$year_start + age_non_specific$year_end)/2)
  age_non_specific$year_id <- ifelse(age_non_specific$year_id < 1990, 1990, age_non_specific$year_id)
  
  ## Now merge with parent diarrhea prevalence data ##
  if(global_merge == T){
    age_non_specific <- join(age_non_specific, denominator_weights[,c("year_id","age_dummy","number","total_number","prop_denom")], by=c("year_id","age_dummy"))
  } else {
    age_non_specific <- join(age_non_specific, denominator_weights[,c("year_id","location_id","age_dummy","number","total_number","prop_denom")], by=c("location_id","year_id","age_dummy"))
  }
  
  # Subset to the weights relevant to the age groups of the non_age_specific rows
  age_non_specific <- subset(age_non_specific, age_start <= age_ends & age_end >= age_starts) 
  age_non_specific <- data.table(age_non_specific)
  
  age_non_specific[age_start > age_starts, age_starts := age_start]
  age_non_specific[age_end < age_ends, age_ends := age_end]
  age_non_specific <- data.frame(age_non_specific)
  
  aggregates <- aggregate((cbind(prop_pred, prop_denom) ~ row_number), data=age_non_specific, FUN=sum) 
  setDT(aggregates)
  setnames(aggregates, c("prop_pred","prop_denom"), c("study_prop_pred","study_prop_denom"))
  age_non_specific <- join(age_non_specific, aggregates, by="row_number")
  
  ## Calculate new cases and sample size as the fraction in each age group (adjusting for different age_start/age_end cutoffs)
  age_non_specific$og_cases <- age_non_specific$cases
  age_non_specific$og_sample_size <- age_non_specific$sample_size
  
  # Calculate new values
  age_non_specific$cases <- age_non_specific$cases * age_non_specific$prop_pred / age_non_specific$study_prop_pred # applying another weight (prop_pred / aggregate study_prop_pred)
  age_non_specific$effective_sample_size <-  age_non_specific$sample_size * age_non_specific$prop_denom / age_non_specific$study_prop_denom
  age_non_specific$sample_size <- age_non_specific$effective_sample_size
  age_non_specific$mean <- age_non_specific$cases / age_non_specific$effective_sample_size
  
  write.xlsx(data.table(age_non_specific)[,.(nid,field_citation_value,location_name,sex,year_start,year_end,
                                             age_start,age_end,age_mid,age_starts, age_ends, og_cases, og_sample_size, cases,sample_size, 
                                             mean, prop_pred,study_prop_pred,
                                             prop_denom,study_prop_denom)], paste0(vetting_dir, "age_non_specific_line135.xlsx"))
  if(bounded==T){
    age_non_specific$mean <- ifelse(age_non_specific$mean >= 1, 0.99, age_non_specific$mean)
  }
  
  age_non_specific$standard_error <- with(age_non_specific, sqrt(standard_error^2 * pred_se^2 + standard_error^2*pred_mean^2 + pred_se^2*raw_mean))
  write.csv(age_non_specific, paste0(vetting_dir, "age_non_specific.csv"), row.names=FALSE)
  
  ## Reassign age start and end
  age_non_specific$age_start <- age_non_specific$age_starts
  age_non_specific$age_end <- age_non_specific$age_ends
  
  # calculate weight to adjust sample size in affected bins
  if(any(age_non_specific$actual_age_width > age_non_specific$expected_age_width)){stop("some age bins are wider than they should be.")}
  
  setDT(age_non_specific)
  age_non_specific <- age_non_specific[cases > sample_size,sample_size := cases/mean]
  broken_split <- subset(age_non_specific, cases > sample_size) # If resettig sample_size still didn't fix it...
  print(paste0("There are ", nrow(broken_split), " rows where cases > sample_size after age splitting."))
  age_non_specific <- subset(age_non_specific, ! rownum %in% broken_split$rownum)
  age_non_specific <- rbind.fill(age_non_specific,subset(df,rownum %in% broken_split$rownum))
  
  
  # Clean up some columns
  age_non_specific$note_modeler <- paste0(age_non_specific$note_modeler," | these data were split from original aggregate age group. Numerator weights from the gbd21 etiology dismod model,
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
  age_non_specific$group_review <- 1
  
  # Rbind
  output <- rbind.fill(age_specific, group_review, age_non_specific)
  
  all_data <- rbind.fill(age_specific, age_non_specific)
  write.csv(all_data, paste0(vetting_dir, "all_data_age_split.csv"), row.names=FALSE)
  
  output <- output[ , -which(names(output) %in% c("f_mean","age_mid","log_ratio","delta_log_se","dummy","age_starts","age_ends","sample_size1","age_dummy","sum_pred","prop_pred","number","total_number",
                                                  "run_id","modelable_entity_name.1","cv.","cv_.","pre_age_split_mean","start_date","end_date","pmid_doi_pmcID_URL","X_age_mid","prop_denom", "row_number",
                                                  "row_num","study_prop_pred","study_prop_denom","prop_pop","og_cases","og_sample_size","re_age_start","re_age_end","og_age_start","og_age_end",
                                                  "actual_age_width","expected_age_width","sample_age_weight","w_norm"))]
  print("Finished age splitting")
  return(output)
}
