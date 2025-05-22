##########################################################################
### Purpose: Custom functions used for crosswalks and sex-splitting
##########################################################################


# GET METADATA ------------------------------------------------------------
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")

release_id <- "OBJECT"
loc_dt <- get_location_metadata(location_set_id = "OBJECT", release_id = "OBJECT")
super_region_dt <- loc_dt[, .(location_id, super_region_id, super_region_name)]
age_dt <- get_age_metadata( release_id = "OBJECT")

# DATA PROCESSING FUNCTIONS -----------------------------------------------
## REPLACE THIS FUNCTION WITH WHATEVER YOU WANT TO USE TO PULL IN DATA
get_file_data <- function(){
  dt <- as.data.table(read.xlsx(file_path))
  dt <- dt[measure %in% c(measures)]
  return(dt)
}

## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}


## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
   dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
   dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

## CALCULATE CASES FROM STANDARD ERROR
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

## FIND SEX MATCHES
find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% measures]
  match_vars <- c("nid", "age_start","site_memo", "age_end", "location_id", "measure", "year_start", "year_end", 
                  names(sex_dt)[names(sex_dt) %in% sex_covs], 
                  names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  match_vars <- match_vars[match_vars != ""]
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- data.table::dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate=mean)
  message("mean_female, mean_male, standard_error_female, or standard_error_male NA in ",unique(length(unique((sex_dt[(is.na(mean_Female) | is.na(mean_Male)| is.na(standard_error_Female) | is.na(standard_error_Male)), nid]))))," nids") 
  message("dropping ", unique(length(unique((sex_dt[(is.na(mean_Female) | is.na(mean_Male) | is.na(standard_error_Female) | is.na(standard_error_Male)), nid])))), " NIDs from sex-match")
  sex_dt <- subset(sex_dt, !is.na(mean_Female) & !is.na(mean_Male) & !is.na(standard_error_Female) & !is.na(standard_error_Male))
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, study_id := .GRP, by = c("nid", "location_id")]
  sex_dt[, dorm_alt := "Female"]
  sex_dt[, dorm_ref := "Male"]
  return(sex_dt)
}

## CALCULATE RATIOS OF FEMALE:MALE MATCHES
calc_sex_ratios <- function(dt){ 
  ratio_dt <- as.data.table(copy(dt))
  ratio_dt[, c("ref_mean_Male", "ref_se_Male")] <- cw$utils$linear_to_log(
    mean = array(ratio_dt$mean_Male), 
    sd = array(ratio_dt$standard_error_Male)
  )
  ratio_dt[, c("alt_mean_Female", "alt_se_Female")] <- cw$utils$linear_to_log(
    mean = array(ratio_dt$mean_Female), 
    sd = array(ratio_dt$standard_error_Female)
  )
  
  ratio_dt2 <- ratio_dt %>%
    mutate(
      ydiff_log_mean = alt_mean_Female - ref_mean_Male,
      ydiff_log_se = sqrt(alt_se_Female^2 + ref_se_Male^2)
    )
  
  return(ratio_dt2)
}


# SPLIT_BOTH_SEX_CODE
split_both_sex <- function(full_dt, sex_results) {
  
  offset <- min(full_dt[full_dt$mean>0,mean])/2 
  
  full_dt <- get_cases_sample_size(full_dt)
  full_dt <- get_se(full_dt)
  full_dt <- calculate_cases_fromse(full_dt)
  n <- names(full_dt)
  
  both_sex <- full_dt[sex == "Both" & measure == measures]
  both_sex[, sex_dummy := "Female"]
  both_zero_rows <- nrow(both_sex[mean == 0, ]) #Both sex, zero mean data 
  message(paste0("There are ", both_zero_rows, " data points that will be offset by ", offset))
  both_sex[mean == 0, mean := offset]  # apply offset to mean zero data points 
  sex_specific <- copy(full_dt[sex != "Both"]) # sex specific data before sex splitting 
  
  #Adjust mean and standard error in both_sex using beta coeff and sd from sex_results. Ref_vals_mean and ref_vals_sd are already post-adjustment and exponentiated to linear space
  both_sex[, c("ref_vals_mean", "ref_vals_sd", "pred_diff_mean", "pred_diff_se", "data_id")] <- sex_results$adjust_orig_vals(
                      df = both_sex,            # original data with obs to be adjusted
                      orig_dorms = "sex_dummy", # name of column with (all) def/method levels
                      orig_vals_mean = "mean",  # original mean
                      orig_vals_se = "standard_error"  # standard error of original mean
                    ) 
  
  # the value of pred_diff_mean and pred_diff_se is not impacted by adding an offset 
  log_ratio_mean <- both_sex$pred_diff_mean
  log_ratio_se <- sqrt(both_sex$pred_diff_se^2 + as.numeric(sex_results$gamma))
  
  adjust_both <- copy(both_sex)
  
  ## To return later
  adjust_both[, year_mid := (year_start + year_end)/2]
  adjust_both[, year_floor := floor(year_mid)]
  adjust_both[, age_mid := (age_start + age_end)/2]
  adjust_both[, age_floor:= floor(age_mid)]
  
  ## Pull out population data. We need age- sex- location-specific population.
  message("Getting population")
  pop <- get_population(age_group_id = "all", sex_id = "all", year_id = unique(floor(adjust_both$year_mid)),
                        location_id=unique(adjust_both$location_id), single_year_age = T,release_id = "OBJECT") 
  ids <- get_ids("age_group") ## age group IDs
  pop <- merge(pop, ids, by="age_group_id", all.x=T, all.y=F)
  pop$age_group_name[pop$age_group_name=="<1 year"] <- 0
  pop$age_group_name[pop$age_group_name=="12 to 23 months"] <- 1
  pop$age_group_name[pop$age_group_name=="95 plus"] <- 95
  
  pop$age_group_name <- as.numeric(pop$age_group_name)
  pop <- pop[!(is.na(age_group_name))]
  pop$age_group_id <- NULL
  
  ## Merge in populations for both-sex and each sex. Turning age bins into the midpoint - because the population ratios, not population itself, is what's important.
  adjust_both <- merge(adjust_both, pop[sex_id==3,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(adjust_both, "population", "population_both")
  adjust_both <- merge(adjust_both, pop[sex_id==1,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(adjust_both, "population", "population_male")
  adjust_both <- merge(adjust_both, pop[sex_id==2,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(adjust_both, "population", "population_female")
  
  ## Transform mean, SEs so we can combine and make adjustments
  adjust_both[, se_val  := unique(sqrt(both_sex$pred_diff_se^2 + as.vector(sex_results$gamma)))]
  adjust_both[, c("real_pred_mean", "real_pred_se")] <-cw$utils$log_to_linear(mean = array(adjust_both$pred_diff_mean), 
                                                                              sd = array(adjust_both$se_val))
                                                                          
  # Subtract the offset before making any adjustments
  adjust_both[mean == paste0(offset), mean := 0 ] 
  
  ## Make adjustments. See documentation for rationale behind algebra.
  adjust_both[, m_mean := mean * (population_both/(population_male + real_pred_mean * population_female))]
  adjust_both[, f_mean := real_pred_mean * m_mean]
  
  ## Get combined standard errors
  adjust_both[, m_standard_error := sqrt((real_pred_se^2 * standard_error^2) + (real_pred_se^2 * mean^2) + (standard_error^2 * real_pred_mean^2))]
  adjust_both[, f_standard_error := sqrt((real_pred_se^2 * standard_error^2) + (real_pred_se^2 * mean^2) + (standard_error^2 * real_pred_mean^2))]
  
  
  ## Adjust the standard error of mean 0 data points 
  adjust_both[mean == 0, `:=` (m_standard_error = sqrt(2)*standard_error, f_standard_error = sqrt(2)*standard_error)]
  
  
  ## Make male- and female-specific dts
  message("Getting male and female specific data tables")
  male_dt <- copy(adjust_both)
  male_dt[, `:=` (mean = m_mean, standard_error = m_standard_error, upper = NA, lower = NA,
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", real_pred_mean, " (",
                                        real_pred_se, ")"), crosswalk_parent_seq=seq)]
  male_dt <- dplyr::select(male_dt, all_of(n))
  female_dt <- copy(adjust_both)
  female_dt[, `:=` (mean = f_mean, standard_error = f_standard_error, upper = NA, lower = NA,
                    cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", real_pred_mean, " (",
                                          real_pred_se, ")"), crosswalk_parent_seq=seq)]
  female_dt <- dplyr::select(female_dt, all_of(n))
  dt_all <- rbindlist(list(sex_specific, female_dt, male_dt))
  
  sex_specific_data <- copy(dt_all)
  drop_nids <- setdiff(full_dt[sex == "Both" & measure == measures, unique(nid)], adjust_both$nid)
  message("Dropped nids:", list(drop_nids))
  
  message("Finished sex splitting.")
  return(list(final = sex_specific_data, graph = adjust_both, missing_nids = drop_nids))
}

## CREATE FIGURE FOR VETTING
graph_sex_predictions <- function(dt){
  graph_dt <- copy(dt[measure == measures, .(age_start, age_end, mean, m_mean, m_standard_error, f_mean, f_standard_error)])
  graph_dt_means <- as.data.table(melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("m_mean", "f_mean")))
  graph_dt_means[variable == "f_mean", variable := "Female"][variable == "m_mean", variable := "Male"]
  graph_dt_means[, id := .I]
  graph_dt_error <- as.data.table(melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("m_standard_error", "f_standard_error")))
  graph_dt_error[variable == "f_standard_error", variable := "Female"][variable == "m_standard_error", variable := "Male"]
  graph_dt_error[, id := .I]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable", "id"))
  graph_dt[, N := (mean*(1-mean)/error^2)]
  # wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  # graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    # geom_errorbar(aes(ymin = lower, ymax = upper)) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}


## GET DEFINITIONS (ALL BASED ON CV'S - WHERE ALL 0'S IS REFERENCE)
get_definitions <- function(ref_dt){
  dt <- copy(ref_dt)
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop & !names(dt) %in% reference]
  cvs <- cvs[!cvs %in% cv_drop] 
  dt[, definition := ""]
  if (reference == "") {
    message("Matching based on old reference_cov method")
    for (cv in cvs){
      dt[get(cv) == 1, definition := paste0(definition, "_", cv)]
    }
    dt[definition == "", definition := "reference"]
  } else {
    message("Still need to figure out when reference is a specific covariate")
    dt[get(reference) == 1, definition := gsub("cv_", "", paste0(reference))]
    for (cv in cvs) {
      dt[get(cv) == 1, definition := paste0(definition, "_", cv)]
    }
  }
  return(list(dt, cvs))
}

# MATCH AT SUBNATIONALS
subnat_to_nat <- function(subnat_dt, subnat){
  dt <- copy(subnat_dt)
  dt[, ihme_loc_id := NULL]
  dt <- merge(dt, loc_dt[, .(location_id, level, parent_id, location_type, ihme_loc_id)], by = "location_id")
  dt[, loc_match := location_id]
  if (subnat == T) {
    dt[level >= 4, loc_match := parent_id]
    dt[level == 6, loc_match := "OBJECT"] 
    dt[level == 5 & grepl("GBR", ihme_loc_id), loc_match := "OBJECT"] 
    dt[level == 5 & location_type %in% c("urbanicity", "admin2"), loc_match := "OBJECT"]
    dt[level == 5 & grepl("CHN", ihme_loc_id), loc_match := "OBJECT"]
    dt[level == 5 & grepl("KEN", ihme_loc_id), loc_match := "OBJECT"]
    message("Matching subnational locations to other subnational locations")
  } else {
    message("NOT matching subnationals locations to other subnational locations")
  }
  return(dt)
}

# CALCULATE YEAR FOR MATCHING
calc_year <- function(year_dt){
  dt <- copy(year_dt)
  dt[, year_match := (year_start+year_end)/2]
  return(dt)
}

## GET UNIQUE "AGE SERIES" FOR THE PURPOSE OF AGGREGATING
get_age_combos <- function(agematch_dt){
  dt <- copy(agematch_dt)
  by_vars <- c("nid", "location_id","year_match", "sex", "measure", names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop])
  dt[, age_n := .GRP, by = by_vars]
  small_dt <- copy(dt[, c(by_vars, "age_start", "age_end","age_n"), with = F])
  return(list(dt, small_dt))
}

## HELPER FUNCTION TO ROUND TO NEAREST AGE START/AGE END
get_closest_age <- function(i, var, start = T, dt){
  age <- dt[i, get(var)]
  if (start == T){
    age_dt[, age_group_years_start][which.min(abs(age_dt[, age_group_years_start] - age))]
  } else if (start == F){
    age_dt[, age_group_years_end][which.min(abs(age_dt[, age_group_years_end] - age))]
  }
}

## AGGREGATE FUNCTION
aggregation <- function(start_match, end_match, dt = pull_dt){
  agg <- nrow(dt[start_match == age_start & end_match == age_end]) == 0 ## FLAGS TO AGGREGATE EACH SIDE OF THE RATIO
  agg1 <- nrow(dt[start_match == age_start2 & end_match == age_end2]) == 0
  z <- qnorm(0.975)
  if (agg == T){ ## AGGREGATE FIRST SIDE
    row_dt <- unique(dt[age_start >= start_match & age_end <= end_match], by = c("age_start", "age_end"))
    row_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size))]
    row_dt[, `:=` (mean = cases/sample_size, age_start = min(age_start), age_end = max(age_end))]
    row_dt[measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    row_dt[measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    row_dt[measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    row_dt <- dplyr::select(row_dt, names(row_dt)[!grepl("2$", names(row_dt))])
    row_dt <- unique(row_dt, by = "nid")
  } else { ## OR LEAVE THE SAME
    row_dt <- unique(dt[age_start == start_match & age_end == end_match], by = c("age_start", "age_end"))
    row_dt <- dplyr::select(row_dt, names(row_dt)[!grepl("2$", names(row_dt))])
  }
  if (agg1 == T){ ## AGGREGATE SECOND SIDE
    row_dt2 <- unique(dt[age_start2 >= start_match & age_end2 <= end_match], by = c("age_start2", "age_end2"))
    row_dt2[, `:=` (cases2 = sum(cases2), sample_size2 = sum(sample_size2))]
    row_dt2[, `:=` (mean2 = cases2/sample_size2, age_start2 = min(age_start2), age_end2 = max(age_end2))]
    row_dt2[measure == "prevalence", standard_error2 := sqrt(mean2*(1-mean2)/sample_size2 + z^2/(4*sample_size2^2))]
    row_dt2[measure == "incidence" & cases2 < 5, standard_error2 := ((5-mean2*sample_size2)/sample_size2+mean2*sample_size2*sqrt(5/sample_size2^2))/5]
    row_dt2[measure == "incidence" & cases2 >= 5, standard_error2 := sqrt(mean2/sample_size2)]
    row_dt2 <- dplyr::select(row_dt2, names(row_dt2)[grepl("2$", names(row_dt2))])
    row_dt2 <- unique(row_dt2, by = "nid2")
  } else { ## OR LEAVE THE SAME
    row_dt2 <- unique(dt[age_start2 == start_match & age_end2 == end_match], by = c("age_start2", "age_end2"))
    row_dt2 <- dplyr::select(row_dt2, names(row_dt2)[grepl("2$", names(row_dt2))])
  }
  new_row <- cbind(row_dt, row_dt2) ## BUT THE SIDES BACK TOGETHER
  return(new_row)
}

## SET UP AGGREGATION
aggregate_tomatch <- function(match_dt, id){
  print(id)
  pull_dt <- copy(match_dt[agg_id == id])
  start_matches <- pull_dt[age_start_match == T, age_start] 
  allend_matches <- pull_dt[age_end_match == T, unique(age_end)] 
  end_matches <- c() 
  for (x in 1:length(start_matches)){
    match <- allend_matches[allend_matches > start_matches[x]]
    match <- match[which.min(match-start_matches[x])]
    end_matches <- c(end_matches, match)
  }
  aggregated <- rbindlist(lapply(1:length(start_matches), function(x) aggregation(start_match = start_matches[x],  end_match = end_matches[x], dt = pull_dt)))
  return(aggregated)
}

## FULL FUNCTION TO GET MATCHES
get_matches <- function(n, pair_dt, year_span = 10, age_span = 10){
  dt <- copy(pair_dt)
  pair <- pairs[,n]
  print(pair)
  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan") year_span <- 20
  keep_vars <- c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end",
                 "mean", "standard_error", "cases", "sample_size", "loc_match", "year_match", "age_n")
  dt1 <- copy(dt[definition == pair[1]])
  dt1 <- dplyr::select(dt1, keep_vars)
  dt2 <- copy(dt[definition == pair[2]])
  dt2 <- dplyr::select(dt2, keep_vars)
  setnames(dt2, keep_vars[!keep_vars  == "loc_match"], paste0(keep_vars[!keep_vars  == "loc_match"], "2"))
  matched <- merge(dt1, dt2, by = "loc_match", allow.cartesian = T) ## INITIAL CARTISIAN MERGE ONLY MATCHING ON LOCATION
  matched <- matched[sex == sex2 & measure == measure2 & year_match >=year_match2-year_span/2 &
                       year_match <= year_match2 + year_span/2] ## FILETER OUT SEX, MEASURE, YEAR
  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan"){ ## EXACT LOCS FOR MARKETSCAN BUT CURRENTLY ALL ARE EXACT LOCS
    matched <- matched[location_id == location_id2]
  }
  matched[, age_match := (age_start >= age_start2 - age_span/2 & age_start <= age_start2 + age_span/2) &
            (age_end >= age_end2 - age_span/2 & age_end <= age_end2 + age_span/2)]
  unmatched <- copy(matched[age_match == F])
  unmatched <- unmatched[!age_start >= 100] 
  ## AGE ROUNDING
  unmatched$age_start <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start", dt = unmatched))
  unmatched$age_end <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end", start = F, dt = unmatched))
  unmatched$age_start2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start2", dt = unmatched))
  unmatched$age_end2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end2", start = F, dt = unmatched))
  
  ## FIND WHERE IT IS POSSIBLE TO AGGREGATE
  unmatched[, age_start_match := age_start == age_start2]
  unmatched[, age_end_match := age_end == age_end2]
  unmatched[, sum_start := sum(age_start_match), by = c("age_n", "age_n2")]
  unmatched[, sum_end := sum(age_end_match), by = c("age_n", "age_n2")]
  agg_matches <- copy(unmatched[sum_start > 0 & sum_end > 0 & !is.na(cases) & !is.na(sample_size) & !is.na(cases2) & !is.na(sample_size2)])
  agg_matches[, agg_id := .GRP, by = c("age_n", "age_n2")]
  
  ## AGGREGATE IF THERE ARE PLACES TO AGGREGATE OTHERWISE DO NOTHING
  if (nrow(agg_matches) > 0){
    aggregated <- rbindlist(lapply(1:agg_matches[, max(agg_id)], function(x) aggregate_tomatch(match_dt = agg_matches, id = x)))
    aggregated[, c("age_start_match", "age_end_match", "sum_start", "sum_end", "agg_id") := NULL]
    final_match <- rbind(matched[age_match == T], aggregated)
  } else {
    final_match <- copy(matched[age_match == T])
  }
  
  final_match[, c("age_match") := NULL]
  final_match[, `:=` (def = pair[1], def2 = pair[2])] ## LABEL WITH DEFINITIONS
  message(paste(nrow(final_match), pair[1], pair[2]))
  return(final_match)
}

create_mr_ratio <- function(ratio_dt){
  dt <- copy(ratio_dt)
  
  if (logit_transform == T) {
    message("Logit transforming")
    dt <- dt[!mean > 1]
    dt[, c("ref_mean", "ref_se")] <- cw$utils$linear_to_logit(
      mean = array(dt$mean), 
      sd = array(dt$standard_error)
    )
    dt[, c("alt_mean", "alt_se")] <- cw$utils$linear_to_logit(
      mean = array(dt$mean2), 
      sd = array(dt$standard_error2)
    ) 
    
    dt <- dt %>%
      mutate(
        ldiff = alt_mean - ref_mean,
        ldiff_se = sqrt(alt_se^2 + ref_se^2)
      )
    
    
    drop_count <- nrow(dt[is.na(ldiff)])
    if (drop_count > 0) {
      message(paste0("Dropping zeroes: Drop count of ", drop_count))
      dt <- dt[!is.na(ldiff), ]
    }
  } else {
    message("Log tranforming")
    dt <- as.data.table(copy(dt))
    dt[, c("ref_mean", "ref_se")] <- cw$utils$linear_to_log(
      mean = array(dt$mean), 
      sd = array(dt$standard_error)
    )
    dt[, c("alt_mean", "alt_se")] <- cw$utils$linear_to_log(
      mean = array(dt$mean2), 
      sd = array(dt$standard_error2)
    )
    
    dt <- dt %>%
      mutate(
        ldiff = alt_mean - ref_mean,
        ldiff_se = sqrt(alt_se^2 + ref_se^2))
    
    drop_count <- nrow(dt[is.na(ldiff)])
    if (drop_count > 0) {
      message(paste0("Dropping zeroes: Drop count of ", drop_count))
      dt <- dt[!is.na(ldiff), ]
    }
  }
  return(dt)
}

create_dual_ratios <- function(ratio_dt) {
  dt <- copy(ratio_dt)
  message("Log tranforming")
  dt[, ratio := mean/mean2]
  dt[, ratio_se := sqrt((mean^2 / mean2^2) * (standard_error^2/mean^2 + standard_error2^2/mean2^2))]
  dt[, log_ratio := log(ratio)]
  dt$log_rse <- sapply(1:nrow(dt), function(i) {
    mean_i <- dt[i, "ratio"]
    se_i <- dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  drop_count <- nrow(dt[is.na(log_rse)])
  if (drop_count > 0) {
    message(paste0("Dropping zeroes: Drop count of ", drop_count))
    dt <- dt[!is.na(log_rse), ]
    return(dt)
  }
}



create_mrbrtdt <- function(match_dt, vars = cvs, age = F){
  dt <- copy(match_dt)
  dt$dorm_ref <- gsub("_cv_", "", dt$def)
  dt$dorm_alt <- gsub("_cv_", "", dt$def2)
  loc_map <- copy(loc_dt[, .(location_ascii_name, location_id)])
  dt <- merge(dt, loc_map, by = "location_id")
  setnames(loc_map, c("location_id", "location_ascii_name"), c("location_id2", "location_ascii_name2"))
  if(nash_cryptogenic == T) {
    message("Nash crytogenic")
    setnames(dt, "location_id", "location_id2")
    dt <- merge(dt, loc_map, by = "location_id2")
    dt[, `:=` (age_start2 = age_start, age_end2 = age_end)]
  } else {
    dt <- merge(dt, loc_map, by = "location_id2")
  }
  dt <- merge(dt, loc_map, by = "location_id2")
  dt[, age := (age_start + age_end + age_start2 + age_end2) / 4]
  dt[, id := paste0(nid, " (", location_ascii_name, ": ", sex, " ", age_start, "-", age_end, ")")]
    if (age == F){
      dt <- dt[, c("id", "ldiff", "ldiff_se", "dorm_ref", "dorm_alt", "nid"), with = F]
    } else if (age == T){
      dt <- dt[, c("id", "age","ldiff", "ldiff_se", "dorm_ref", "dorm_alt", "nid"), with = F]
    }
  dt[, study_id := .GRP, by = "nid"]
  return(list(dt, vars))
}


save_mrbrt <- function(model, mrbrt_directory) {
  df_result <- model$create_result_df()
  write.csv(df_result, paste0(mrbrt_directory, "/df_result_crosswalk.csv"), row.names = FALSE)
  py_save_object(object = model, filename = paste0(mrbrt_directory, "/results.pkl"), pickle = "dill")
}

save_model_RDS <- function(results, path){
  names <- c("beta", 
             "beta_sd", 
             "constraint_mat",
             "cov_mat",
             "cov_models",
             "cwdata",
             "design_mat",
             "fixed_vars",
             "gamma",
             "gold_dorm", 
             "lt",
             "num_vars",
             "num_vars_per_dorm",
             "obs_type",
             "order_prior",
             "random_vars",
             "relation_mat",
             "var_idx",
             "vars",
             "w")
  model <- list()
  for (name in names){
    if(is.null(results[[name]])) {
      message(name, " is NULL in original object, will not be included in RDS")
    }
    model[[name]] <- results[[name]]
  }
  saveRDS(model, paste0(path, "/model_object.RDS"))
  message("RDS object saved to ", paste0(path, "/model_object.RDS"))
  return(model)
}

graph_combos <- function(model, logit_transform = T){
  if(logit_transform == T) {
    data <- as.data.table(model$cwdata$df)
    data <- get_weights(model)
    data$w <- round(data$w, 0) # indicate trimming based on percentage 
    data <- as.data.table(data)
    data[, diff_name := paste0(dorm_alt, " - ", dorm_ref)]
    data[, `:=` (ldiff_l = ldiff - 1.96*ldiff_se, ldiff_u = ldiff + 1.96*ldiff_se)]
    preds <- model$fixed_vars
    preds <- as.data.frame(preds)
    preds <- rbind(preds, model$beta_sd)
    for (cov_name in unique(data$dorm_alt)) {
      graph_dt <- data[dorm_alt == cov_name]
      pred_mean <- preds[cov_name][[1]][1]
      pred_se <- preds[cov_name][[1]][2]
      graph_dt[, `:=` (pred_l = pred_mean - 1.96*pred_se, pred_u = pred_mean + 1.96*pred_se)]
      gg_fit <- ggplot() +
        geom_rect(data = graph_dt, aes(xmin = pred_l, xmax = pred_u, ymin = -Inf, ymax = Inf), alpha = 0.2, fill = "mediumorchid3") +
        geom_point(data = graph_dt, aes(x = ldiff, y = as.factor(id), color = as.factor(w))) +
        geom_errorbarh(data = graph_dt, aes(y = as.factor(id), xmin = ldiff_l, xmax = ldiff_u)) +
        geom_vline(xintercept = pred_mean, linetype = "dashed", color = "darkorchid") +
        geom_vline(xintercept = 0) +
        labs(x = "Logit Difference", y = "") +
        scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
        # scale_y_discrete(labels = graph_dt[, id]) +
        ggtitle(paste0("Model fit for difference ", graph_dt$diff)) +
        # xlim(-7, 7) +
        theme_classic() +
        theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
              axis.text.y = element_text(size = 5))
      ggsave(gg_fit, filename = paste0(mrbrt_dir, model_name, "/fit_graph_", cov_name, ".pdf"), width = 12)
    }
    return(gg_fit)
  } else {
    data <- as.data.table(model$cwdata$df)
    data <- get_weights(model)
    data$w <- round(data$w, 0) # indicate trimming based on percentage 
    data <- as.data.table(data)
    data[, diff_name := paste0(dorm_alt, " - ", dorm_ref)]
    data[, `:=` (log_ratio_l = log_ratio - 1.96*log_rse, log_ratio_u = log_ratio + 1.96*log_rse)]
    preds <- model$fixed_vars
    preds <- as.data.frame(preds)
    preds <- rbind(preds, model$beta_sd)
    for (cov_name in unique(data$dorm_alt)) {
      graph_dt <- data[dorm_alt == cov_name]
      pred_mean <- preds[cov_name][[1]][1]
      pred_se <- preds[cov_name][[1]][2]
      graph_dt[, `:=` (pred_l = pred_mean - 1.96*pred_se, pred_u = pred_mean + 1.96*pred_se)]
      gg_fit <- ggplot() +
        geom_rect(data = graph_dt, aes(xmin = pred_l, xmax = pred_u, ymin = -Inf, ymax = Inf), alpha = 0.2, fill = "mediumorchid3") +
        geom_point(data = graph_dt, aes(x = log_ratio, y = as.factor(id), color = as.factor(w))) +
        geom_errorbarh(data = graph_dt, aes(y = as.factor(id), xmin = log_ratio_l, xmax = log_ratio_u)) +
        geom_vline(xintercept = pred_mean, linetype = "dashed", color = "darkorchid") +
        geom_vline(xintercept = 0) +
        labs(x = "Log Ratio ", y = "") +
        scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
        # scale_y_discrete(labels = graph_dt[, id]) +
        ggtitle(paste0("Model fit for log ratio ", graph_dt$diff_name)) +
        # xlim(-7, 7) +
        theme_classic() +
        theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
              axis.text.y = element_text(size = 5))
      ggsave(gg_fit, filename = paste0(mrbrt_dir, model_name, "/fit_graph_", cov_name, ".pdf"), width = 12)
    }
    return(gg_fit)
  }
}



# ADJUST ALT DATA POINTS TOWARDS REF DATA POINTS
make_adjustment <- function(model, full_dt, offset){
  adjust_dt <- copy(full_dt)
  
  mean_zero_nonref <- nrow(adjust_dt[mean == 0 & definition != reference_def ])
  message(paste0("There are ", mean_zero_nonref, " non-reference data points that have mean=0"))
  adjust_dt[(mean == 0 ),`:=` (mean_0 = 1) ]
  adjust_dt[(mean == 0 ),`:=` (mean = 0.001)]
  
  # Change mean >1 to 0.99
  over_one <- nrow(adjust_dt[mean>1])
  message(paste0("There are ", over_one, " data points that have mean over 1. Will be changed to mean=1"))
  adjust_dt[(mean>1), mean:=0.99]
  
  
  # Get pred mean and se
  preds <- model$fixed_vars
  preds <- as.data.frame(preds)
  preds <- rbind(preds, model$beta_sd)
  cvs <- unique(full_dt$definition)
  cvs <- cvs[cvs != "reference"]

  # Adjust data points based on mrbrt and nonzero
  adjust_dt [, c("mean_adj", "standard_error_adj", "diff", "diff_se", "data_id")] <- model$adjust_orig_vals(
    df = adjust_dt ,                  # original data with obs to be adjusted
    orig_dorms = "definition",        # name of column with (all) def/method levels
    orig_vals_mean = "mean",          # original mean
    orig_vals_se = "standard_error"   # standard error of original mean
  )

  adjust_dt$crosswalk_parent_seq <- adjust_dt$seq
  adjust_dt$seq <- NA
  graph_adjust <- copy(adjust_dt)

  for (cv in cvs) {
    ladj <- preds[cv][[1]][1]
    ladj_se <- preds[cv][[1]][2]
    adjust_dt[definition == cv & diff != 0, `:=` (mean = mean_adj, standard_error = standard_error_adj,
                                                  note_modeler = paste0(note_modeler, " | crosswalked with logit(difference): ",
                                                                        round(ladj, 2), " (", round(ladj_se, 2), ")"),
                                                  cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)]
    adjust_dt[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
  }

  ## CONVERT DUMMY MEAN VALUE FROM MEAN=0 (MEAN_0 = 1) BACK TO 0
  adjust_dt[(mean_0==1), `:=`(mean=0)]

  # Combine everything, make sure there are no drops, drop extra cols
  if (nrow(full_dt) == nrow(adjust_dt)) {
    message("All NIDs present")
  } else {
    missing <- setdiff(full_dt$nid, adjust_dt$nid)
    message(paste("Dropped NIDs:", missing, " Stop!"))
  }

  extra_cols <- setdiff(names(adjust_dt), names(full_dt))
  final_dt <- copy(adjust_dt)
  final_dt[, (extra_cols) := NULL]
  return(list(epidb = final_dt, vetting_dt = graph_adjust))
}

prediction_plot <- function(vetting_data, by_state = F){
  dt <- copy(vetting_data)
  dt <- merge(dt, loc_dt[, .(location_id, super_region_name)], by = "location_id")
  dt[, `:=` (N_adj = (mean_adj*(1-mean_adj)/standard_error_adj^2),
             N = (mean*(1-mean)/standard_error^2))]
  wilson_norm <- as.data.table(binconf(dt$mean*dt$N, dt$N, method = "wilson"))
  wilson_adj <- as.data.table(binconf(dt$mean_adj*dt$N_adj, dt$N_adj, method = "wilson"))
  dt[, `:=` (lower = wilson_norm$Lower, upper = wilson_norm$Upper,
             lower_adj = wilson_adj$Lower, upper_adj = wilson_adj$Upper)]
  cvs <- unique(dt$definition)
  cvs <- cvs[cvs != "reference"]
  for (cv in cvs){
    graph_dt <- dt[definition == cv]
    gg_pred <- ggplot(graph_dt, aes(x = mean, y = mean_adj, color = as.factor(super_region_name))) +
      geom_point() +
      geom_errorbar(aes(ymin = lower_adj, ymax = upper_adj)) +
      geom_errorbarh(aes(xmin = lower, xmax = upper)) +
      scale_color_brewer(palette = "Spectral", name = "Super Region") +
      labs(x = "Unadjusted means", y = "Adjusted means") +
      ggtitle(paste0("Adjusted vs Unadjusted Means: ", cv)) +
      theme_classic()
    ggsave(gg_pred, filename = paste0(mrbrt_crosswalk_dir, "/adjust_graph", cv, ".pdf"), width = 12, height = 12)
  }
  return(gg_pred)
}

adjustment_plot <- function(vetting_data) {
  dt <- copy(vetting_data) 
  dt[, midage := round((age_start + age_end) / 2, 0)]
  gg_orig <- ggplot(dt, aes(x = midage, y = mean, color = as.factor(definition))) + geom_point() +
    ggtitle("Unadjusted Means")
  gg_adj <- ggplot(dt, aes(x = midage, y = mean_adj, color = as.factor(definition))) + geom_point() + 
    ggtitle("Adjusted Means")
  ggsave(gg_orig, filename = paste0(mrbrt_crosswalk_dir, "/original_mean.pdf"), width = 12)
  ggsave(gg_adj, filename = paste0(mrbrt_crosswalk_dir, "/adj_mean.pdf"), width = 12)
}


# NASH - CRYPOTGENIC CROSSWALK FUNCTIONS 
## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
prep_data_matches <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt <- dt[grepl("Y|N", case_name_specific)]
  cases <- c("Y", "N")
  for (case in cases) {
    dt[, paste0("cv_", case) := ifelse(case_name_specific == case, 1, 0)]
  }
  dt[, id := .GRP, by=.(location_id, nid, sex, age_start, age_end, year_start, year_end)]
  dt[, has_both := sum(cv_Y, cv_N), by = .(id)]
  drop_nids1 <- dt[grepl("crosswalk", case_definition), unique(nid)]
  drop_nids2 <- dt[cases == 0, unique(nid)]
  drop_nids3 <- 439918  
  drop_nids <- c(drop_nids1, drop_nids2, drop_nids3)
  crosswalk <- dt[has_both == 2, ]
  crosswalk <- crosswalk[!(nid %in% drop_nids)]
  crosswalk <- crosswalk[!(nid == 439909 & sex %in% c("Male", "Female"))]
  crosswalk <- crosswalk[!grepl("child", note_modeler), ]
  # Find the places with crytogenic only 
  adjust_nids <- dt[cv_N == 1 & mean == 0 & grepl("crosswalk", case_definition), unique(nid)]
  adjust <- copy(dt)
  adjust <- adjust[cv_Y == 1 & nid %in% adjust_nids]
  return(list(crosswalk, adjust))
}

prep_data_denom <- function(raw_dt){
  dt <- copy(raw_dt)
  dt <- unique(dt[, .(cases2 = sum(cases), sample_size2 = sample_size, id_var = id_var), by = c("nid", "location_id", "sex", "year_start", "year_end", "age_start", "age_end")])
  dt[, mean2 := cases2/sample_size2]
  z <- qnorm(0.975)
  dt[, standard_error2 := sqrt(mean2*(1-mean2)/sample_size2 + z^2/(4*sample_size2^2))]
  return(dt)
}

prep_data_comb <- function(raw_dt, denom_dt) {
  prep_dt <- copy(raw_dt)
  prep_dt <- prep_dt[cv_Y == 1, .(nid,location_id, sex, mean, cases, sample_size, standard_error, age_start, age_end, year_start, year_end, id_var)]
  denom_dt <- copy(denom_dt)
  comb_dt <- merge(prep_dt, denom_dt, by = c("nid", "location_id", "sex", "year_start", "year_end", "age_start", "age_end", "id_var"))
  return(comb_dt)
}

# GET NASH PROPORTION FROM CRYPTOGENIC
make_nash_estimates <- function(vetting, epidb,  original_data) {
  original <- copy(original_data)
  cryptogenic_keep <- copy(epidb)
  adjusted_dt <- copy(vetting)
  adjusted_dt[, cases_adj := mean_adj * sample_size]
  
  # Get seqs and NIDs that were adjusted to figure out corresponding NASH
  # Convert mean to mean_adj to adjust NASH values 
  y_id_var <- unique(adjusted_dt$id_var)
  y_nids <- unique(adjusted_dt$nid)
  keep_cols <- c("mean_adj", "standard_error_adj", "cases_adj", "mean", "standard_error", "cases",
                 "nid", "sex", "location_id", "age_start", "age_end", "year_start", "year_end", "site_memo")
  adjusted_dt <- adjusted_dt[, ..keep_cols]
  merge_cols <- c("nid", "sex", "location_id", "age_start", "age_end", "year_start", "year_end", "site_memo")
  
  to_adjust <- copy(original[case_name == "N" & nid %in% y_nids])
  to_adjust[, c("cases", "mean", "standard_error") := NULL]
  dt <- merge(to_adjust, adjusted_dt, by = merge_cols, all.y = T)
  n_id_var <- unique(dt$id_var)
  
  # Subtract adjusted cryptogenic cases and assign to NASH 
  dt[, cases_nash := cases - cases_adj]
  dt[, se_adj := sqrt(standard_error^2 + standard_error_adj^2)]
  dt <- unique(dt)
  dt$crosswalk_parent_seq <- dt$seq
  dt$mean_value <- NA
  dt[, `:=` (lower = NA, upper = NA, uncertainty_type_value = NA, 
             seq = NA, bundle_id = 6704, case_name = "N")]
  dt[, `:=` (mean_adj = NULL, mean = NULL, standard_error = NULL, standard_error_adj = NULL, cases_adj = NULL, cases = NULL) ]
  dt[, `:=` (note_modeler = "crosswalked cryptogenic estimates and subtracted adjusted from original for NASH estimates") ]
  dt$nash_crosswalk <- 1
  setnames(dt, c("cases_nash", "se_adj", "mean_value"), c("cases", "standard_error", "mean"))
  
  # Create full adjusted dataset 
  original1 <- original[!(id_var %in% y_id_var)] # drop seqs that are cryptogenic adjusted 
  original2 <- original1[!(id_var %in% n_id_var)] # drop seqs that are nash adjusted
  
  full_dt <- rbind(original2, cryptogenic_keep, dt, fill = TRUE)
  return(full_dt)
}

collapse_other <- function(collapsed) {
  collapsed <- get_cases_sample_size(collapsed)
  collapsed <- get_se(collapsed)
  collapsed[, `:=` (cases = sum(cases), standard_error = sqrt(sum(standard_error^2)),
                    note_modeler = paste0(note_modeler, "| cases sum of other and cryptogenic after crosswalk, se adjusted from other and crypto")),
            by = list(nid, location_id, sex, age_start, age_end, year_start, year_end , sample_size)]
  collapsed[, `:=` (mean = NA, lower = NA, upper = NA, uncertainty_type_value = NA)]
  collapsed <- collapsed[case_name_specific == "O", ]
  collapsed <- unique(collapsed)
} 



# CIRRHOSIS FUNCTIONS ----------------
fix_mkscn_cvs <- function(raw_dt, outlier) {
  dt <- copy(raw_dt)
  dt <- dt[grepl("MarketScan", field_citation_value) & grepl("2000", field_citation_value), cv_ms2000 :=1 ]
  dt <- dt[grepl("MarketScan", field_citation_value) & !grepl("2000", field_citation_value), cv_marketscan := 1]
  dt <- dt[grepl("HCUP", field_citation_value), cv_hospital := 1]
  # cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  # for (cov in cvs) {
  #   dt[is.na(get(cov)), paste(cov) := 0]
  # }
  return(dt)
}

append_pdf <- function(dir, starts_with) {
  files <- list.files(dir, pattern = paste0("^", starts_with), full.names = T)
  files <- paste(files, collapse = " ")
  cmd <- paste0("/usr/bin/ghostscript -dBATCH -dSAFER -dNOGC -DNOPAUSE -dNumRenderingThreads=4 -q -sDEVICE=pdfwrite -sOutputFile=",
                dir, "/", starts_with, ".pdf ", files)
  system(cmd)
}
 