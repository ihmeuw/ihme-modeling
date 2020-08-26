##########################################################################
### Author
### Purpose: Functions to assist with nonfatal sex splitting (vision loss, visible goiter, vitamin A deficiency) and age splitting (visible goiter).

##########################################################################

#rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "~/"
  l_root <- "/ihme/limited_use/"
} else { 
  j_root <- "J:/"
  h_root <- "H:/"
  l_root <- "L:/"
}

pacman::p_load(data.table, openxlsx, ggplot2, reshape2, dplyr)
library(mortdb, lib = "/ihme/mortality/shared/r/")
#library(msm, lib.loc = paste0(j_root, "temp/eln1/packages/grant_singularity/"))
#library(Hmisc, lib.loc = paste0(j_root, "temp/eln1/packages/grant_singularity/"))
date <- gsub("-", "_", Sys.Date())


source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_age_metadata.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/save_crosswalk_version.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/run_mr_brt/mr_brt_functions.R")
source("/FILEPATH/run_mr_brt/cov_info_function.R")

make_sex_match_vision_loss <- function(dt){
  ### use only for vision loss ##
  #----------------------------------------------------------------------------------
  if(length(colnames(dt))!=length(unique(colnames(dt)))){
    stop("Column names are not unique")
  }
  data <- dt[sex!="Both"]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end")
  data[, full := paste(location_id, nid, year_start, year_end, age_start, age_end, sep = "-")]  
  female <- data[sex=="Female"]
  male <- data[sex=="Male"]
  
  ## do age aggregation here (up to age group 1-5)
  z <- qnorm(0.975)
  female[, `:=` (cases = sum(cases), sample_size = sum(sample_size))]
  female[, `:=` (mean = cases/sample_size, age_start = min(age_start), age_end = max(age_end))]
  female[measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]  
  
  
  male[, `:=` (cases = sum(cases), sample_size = sum(sample_size))]
  male[, `:=` (mean = cases/sample_size, age_start = min(age_start), age_end = max(age_end))]
  male[measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  
  setnames(female, c("mean", "standard_error"), c("mean_Female", "standard_error_Female"))
  setnames(male, c("mean", "standard_error"), c("mean_Male", "standard_error_Male"))
  
  new_data <- merge(female, male, by=c("full","age_start","age_end","location_id"))
  new_data[,.(mean_Female, mean_Male, standard_error_Female, standard_error_Male, age_start, age_end, location_name.x, sample_size.x, sample_size.y)]
  
  
  ratio_dt <- unique(new_data[,.(mean_Female, mean_Male, standard_error_Female, standard_error_Male, age_start, age_end)])   
  ratio_dt[, `:=` (ratio = mean_Female/mean_Male,
                   ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
  
  return(ratio_dt)
}

get_sex_split_ratio <- function(dt, output_dir, version=1){
  #identifies all the sex specific matches, saves input data set, and then runs mr-brt
  
  #### prep files and folders to save
  input_data_dir <- paste0(output_dir, "/input_data")
  if(!dir.exists(input_data_dir)){dir.create(input_data_dir)}
  data_file <- paste0(input_data_dir, "/matched_sex_split_input_data_v", version, ".csv")
  label <- paste0("output_v", version)
  ##### make matches and save
  data <- dt[sex!="Both"]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end")
  data[, full := paste(location_id, underlying_nid, year_start, year_end, age_start, age_end, sep = "-")]  
  female <- data[sex=="Female"]
  male <- data[sex=="Male"]
  fulls <- data[which(data$full %in% female$full & data$full %in% male$full )]
  
  setnames(female, c("mean", "standard_error"), c("mean_Female", "standard_error_Female"))
  setnames(male, c("mean", "standard_error"), c("mean_Male", "standard_error_Male"))
  
  new_data <- merge(female, male, by=c("full","age_start","age_end","location_id"), allow.cartesian = TRUE)
  ratio_data <- new_data[,.(mean_Female, mean_Male, standard_error_Female, standard_error_Male, age_start, age_end, location_name.x, sample_size.x, sample_size.y, underlying_nid.x)]
  ratio_data <- ratio_data[mean_Female!=0 & mean_Male!=0]
  ratio_data[, `:=` (ratio = mean_Female/mean_Male,
                   ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
  ratio_data[, log_ratio:=log(ratio)]
  ratio_data$log_ratio_se <- sapply(1:nrow(ratio_data), function(i) {
    ratio_i <- ratio_data[i, "ratio"]
    ratio_se_i <- ratio_data[i, "ratio_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  
  pdf(paste0(output_dir,"/diagnostic_plots.PDF"))
  gplot <- ggplot(ratio_data, aes(ratio_data$mean_Female, ratio_data$mean_Male, color=location_name.x))+
    geom_point(show.legend = FALSE)+
    theme_bw()+
    labs(x="Mean Female Prevalence",y="Mean Male Prevalence")
  print(gplot)
  dev.off()
  
  message(paste0("There are ", nrow(ratio_data), " rows in the matched dataset. Now saving it to ", data_file))
  write.csv(ratio_data, data_file, row.names = FALSE )
  
  cov_list <- list()
  
  fit1 <- run_mr_brt(
    output_dir = paste0(output_dir),
    model_label = label,
    data = data_file,
    mean_var = "log_ratio",
    se_var = "log_ratio_se",
    covs = cov_list, 
    method = "trim_maxL",
    trim_pct = 0.1,
    study_id = "underlying_nid.x",
    overwrite_previous = TRUE
  )
  coefs <- fit1$model_coefs
  
  plot_mr_brt(fit1)
  df_pred <- data.table("intercept"=c(1))
  pred1 <- predict_mr_brt(fit1, newdata = df_pred, write_draws = TRUE)
  new_coefs <- pred1$model_summaries
  new_coefs <- as.data.table(new_coefs)
  new_coefs[, beta:=exp(new_coefs$Y_mean)]
  new_coefs[, beta_se:=(Y_mean_hi - Y_mean_lo) / 3.92 ]
  write.csv(new_coefs, paste0(output_dir, "/",label,"/prediction_coefficient.csv"), row.names = FALSE)
  result <- data.table(result="success", ratio_mean=new_coefs$beta, ratio_se=new_coefs$beta_se)
  return(result)
  }

  
to_split_data <- function(data, ratio_mean, ratio_se){

  tosplit_dt <- data[sex=="Both"]
  tosplit_dt[, was_sexsplit:=1]
  tosplit_dt <- tosplit_dt[location_id!=1]
  tosplit_dt[, year_id := floor((year_start + year_end)/2)]

  pops <- get_population(location_id = tosplit_dt[, unique(location_id)], sex_id = 1:3, decomp_step = "step2",
                                            year_id = tosplit_dt[, unique(year_id)], age_group_id = 1)

  wide_pops <- reshape(pops, idvar = c("location_id","year_id", "run_id", "age_group_id"), timevar = c("sex_id"), direction = "wide")

  splitting_data <- merge(tosplit_dt, wide_pops, by = c("location_id", "year_id"))

  split_dt <- copy(splitting_data)
  
  pred_draws <- rnorm(2000, ratio_mean, ratio_se)
  split_dt[, paste0("male_", 1:2000) := lapply(1:2000, function(x)  mean * (population.3 / (population.1 + pred_draws[x] *population.2 )))]
  split_dt[, paste0("female_", 1:2000) := lapply(1:2000, function(x) pred_draws[x] * get(paste0("male_", x))  )]
  
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 1:2000)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 1:2000)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 1:2000)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 1:2000)]
  
  split_dt[, c( paste0("male_", 1:2000), paste0("female_", 1:2000)) := NULL]
  split_dt[, old_mean := mean]
  split_dt[, c("mean", "standard_error", "run_id","year_id", "age_group_id", paste0("population.", 1:3)):= NULL]
  
  female_data <- copy(split_dt)
  setnames(female_data, c("female_mean", "female_standard_error"), c("mean", "standard_error"))
  female_data$male_mean <- NULL
  female_data$male_standard_error <- NULL
  female_data$sex <- "Female"
  split_dt$female_mean <- NULL
  split_dt$female_standard_error <- NULL
  split_dt$sex <- "Male"
  setnames(split_dt, c("male_mean", "male_standard_error"), c("mean", "standard_error"))
  
  final_data <- rbind(split_dt, female_data, fill=TRUE)
  final_data$crosswalk_parent_seq <- final_data$seq
  final_data$seq <- NA
  data$crosswalk_parent_seq <- NA
  final_data <- rbind(final_data, data[sex!="Both",], fill=TRUE)
  final_data[,c("lower","upper", "seq_parent") := NULL]
  final_data$origin_seq <- NA
  final_data[, upper:=mean+1.96*standard_error]
  final_data[, lower:=mean-1.96*standard_error]
  
  x <- nrow(final_data[mean < 0])
  y <- nrow(final_data[mean > 1])
  
  if(x!=0){message(paste0("There are ",x, " rows with a mean < 0. Setting those and lower bounds to 0."))}
  if(y!=0){message(paste0("There are ",y, " rows with a mean > 1. Setting those and upper bounds to 1."))}
  final_data[ lower < 0, lower:=0]
  final_data[mean < 0, mean:=0]
  final_data[upper > 1, upper:=1]
  final_data[mean > 1, mean:=1]
  
  return(final_data)

}




#######################################################################################################
# age split functions


## ADD ROWS FOR AGE GROUPS
expand_age <- function(small_dt, age_dt = ages){
  dt <- copy(small_dt)
 
  ## ROUND AGE GROUPS
  dt <- dt[age_end > 99, age_end := 99]
  
  ## EXPAND FOR AGE  
  dt[, n.age:=(age_end+1 - age_start)/5]
  dt[, age_start_floor:=age_start]
  dt[, drop := cases/n.age] 
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age.rep := 1:.N - 1, by =.(id)]
  split[, age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4]
  split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
  split[age_start == 0 & age_end == 4, age_group_id := 1]
  split <- split[age_group_id %in% ages$age_group_id | age_group_id == 1] 
  return(split)
}


## GET DISMOD AGE PATTERN
get_age_pattern <- function(locs, id, age_groups, dstep="iterative"){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, ## USING 2010 AGE PATTERN BECAUSE LIKELY HAVE MORE DATA FOR 2010
                           measure_id = c(5), location_id = locs, source = "epi",
                           status = "best", sex_id = c(1,2), gbd_round_id = 6, decomp_step = dstep,
                           age_group_id = age_groups, year_id = 2010) 
  us_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2), 
                                  age_group_id = age_groups, decomp_step = "step1")
  us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
  
  ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
  age_1 <- copy(age_pattern)
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
  se <- copy(age_1)
  se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
  age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
  age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
  age_1[, frac_pop := population / total_pop]
  age_1[, weight_rate := rate_dis * frac_pop]
  age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
  age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
  age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
  age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
  age_1[, age_group_id := 1]
  age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
  age_pattern <- rbind(age_pattern, age_1)
  
  ## CASES AND SAMPLE SIZE
  age_pattern[measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
  age_pattern[is.nan(cases_us), cases_us := 0]
  
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  return(age_pattern)
}

## SPLIT THE DATA
age_split_data <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[, total_pop := sum(population), by = "id"]
  dt[, sample_size := (population / total_pop) * effective_sample_size]
  dt[, cases_dis := sample_size * rate_dis]
  dt[, total_cases_dis := sum(cases_dis), by = "id"]
  dt[, total_sample_size := sum(sample_size), by = "id"]
  dt[, all_age_rate := total_cases_dis/total_sample_size]
  dt[, ratio := mean / all_age_rate]
  dt[, mean := ratio * rate_dis]
  dt[, cases := mean * sample_size]
  return(dt)
}



age_split_data_proportion <- function(raw_dt){  #
  
  # Goal of this function: shift age pattern of CFRs from dismod such that
  # The population weighted average of the shifted age-specific CFRs
  # Is the same as what was observed within the original group
  
  # Define some functions
  
  # Logit & inverse logit
  logit <- function(x) {
    log(x/(1-x))
  }
  
  ilogit <- function(x) {
    exp(x)/(1+exp(x))
  }
  
  # Logit transformation functions
  # Below adopted from LBD logit raking; uses a bisection method to find the correct value of `k`
  
  #' @param targetval  The target value that you want the eventual weighted mean of your distribution defined
  #'                      in `transval` to match. For example, observed CFR from study
  #' @param transval   A vector of values that represent the distribution that should be shifted so that its
  #'                      weighted average is equal to `targetval`. For example, CFRs by age from dismod
  #' @param weightval  A vector of weights to use in creating population-weighted averages as discussed above
  #'                      For example, a vector of population weights
  #' @param MaxIter    How many iterations should the bisection method make before giving up?
  #' @param MaxJump    What is the maximum logit-space jump that should be made between each iteration?
  #' @param FunTol     How close does this function need to get the `weightval`-weighted average of `transval` to
  #'                      the target in `targetval` before it can stop trying?
  #' @param approx_0_1 Should 0 and 1 values be approximated? (Otherwise function won't work)
  #' @returns a value of `k` such that the `weightval`-weighted mean of `invlogit(logit(transval)+k)` equals `targetval`
  #'
  LogitFindK <- function(targetval, transval, weightval, MaxIter = 40, MaxJump = 10, FunTol = 1e-5, approx_0_1 = T, heuristic_0_1 = T){
    
    # Logit raking won't work with any values of 0 or 1 in cell_pred
    # Adjust values slightly to avoid -Inf or Inf in NewVal
    if (approx_0_1) {
      transval[transval == 0] <- 1e-10
      transval[transval == 1] <- 1-(1e-10)
    }
    
    if (heuristic_0_1) {
      if (targetval == 0) return(-999)
      if (targetval == 1) return(99)
    }
    
    NumIter <- ceiling(-log2(FunTol / MaxJump))
    
    if(NumIter > MaxIter){
      stop(paste("Maximum number of iterations is less than projected iterations required:", NumIter / MaxIter))
    }
    
    CurrentError <- EvalDiff(targetval, transval, weightval)
    if (CurrentError > 0){
      Range <- c(0, MaxJump)
    } else {
      Range <- c(-MaxJump, 0)
    }
    
    a <- Range[1]
    b <- Range[2]
    F_a <- EvalDiff(targetval, NewVal(a, transval), weightval)
    F_b <- EvalDiff(targetval, NewVal(b, transval), weightval)
    
    if (F_a * F_b > 0){
      stop("Your estimates are WAY too far away from the target")
    } else {
      i <- 1
      c <- (a + b) / 2
      F_c <- EvalDiff(targetval, NewVal(c, transval), weightval)
      Success <- (abs(F_c) <= FunTol)
      while (!Success & i < NumIter){
        if (sign(F_c) == sign(F_a)){
          a <- c
          F_a <- F_c
        } else {
          b <- c
          F_b <- F_c
        }
        c <- (a + b) / 2
        F_c <- EvalDiff(targetval, NewVal(c, transval), weightval)
        Success <- (abs(F_c) <= FunTol)
        i <- i + 1
      }
      if (Success){
        return(c)
      } else {
        return(sprintf("Need more iterations, current output: K = %g, F(K) = %g",c, F_c))
      }
    }
  }
  # Convenience function to calculate new values after applying transformation
  # using some candidate value of k
  NewVal <- function(K, vals){
    return(ilogit(logit(vals)+K))
  }
  
  # Convenience function to calculate new estimate of our parameter of interest;
  # in this case it's just a weighted mean
  NewEst <- function(vals, weightval){
    return(weighted.mean(vals, weightval))
  }
  
  # Convenience function to evaluate the difference between the target value
  # and the current set of transformed values to see if we're getting
  # closer to the target
  EvalDiff <- function(targetval, vals, weightval){
    return(targetval - NewEst(vals, weightval))
  }
  
  # Wrapper function to use the above to generate a value of `k` for each `id`
  # Generates a value `k_i` such that `invlogit(logit(dismod_cfr_ij) + k_i)`
  # will produce the appropriate transformation for each id `i` and age bin `i`
  # Returns a data table of ids and k values
  logit_trans <- function(the_id, df) {
    
    # Actual processing starts here
    
    id_df <- subset(df, id == the_id)
    
    # Use sample size as weights (same distribution as population)
    weights <- id_df$sample_size
    
    # Pull cfrs
    cfrs <- id_df$rate_dis
    
    # Check to ensure equal lengths
    if (length(!is.na(weights)) != length(!is.na(cfrs))) {
      stop(paste0("The length of weights and cfrs for id ", id, " are not equal"))
    }
    
    # Pull target (original CFR)
    target <- unique(id_df$mean)
    
    # Check target to ensure only one mean per id
    if (length(target) > 1) stop(paste0("More than one mean CFR for id ", id, " - need to re-write code"))
    
    # Find `k`
    k <- LogitFindK(target, cfrs, weights)
    
    # Return `k`
    return(data.table(id = the_id,
                      k = k))
    
  }
  
  cat("\nCalling custom split_data function\n")
  dt <- copy(raw_dt)
  
  # Calculate the total population (from GBD pop numbers)
  dt[, total_pop := sum(population), by = "id"]
  
  # Make a copy of overall sample size in the group
  dt[, total_sample_size := copy(sample_size)]
  
  # Make a copy of total cases in the group
  dt[, total_cases := copy(cases)]
  
  # Calculate the fraction of GBD pop that fall within this age bin
  dt[, pop_fraction_dis := population / total_pop]
  
  # Redistribute the sample size proportional to GBD pop estimates
  dt[, sample_size := total_sample_size * pop_fraction_dis]
  
  # Generate table of `k` values for each id and merge to dt
  k_table <- rbindlist(lapply(unique(dt$id), function(i) {logit_trans(i, dt)}))
  dt <- merge(dt, k_table, by = "id")
  
  # Create copy of original mean
  dt[, original_mean := copy(mean)]
  
  # Transform mean using our values of `k` for each id
  dt[, mean := ilogit(logit(rate_dis) + k)]
  
  # Calculate cases
  dt[, cases := sample_size * mean]
  
  return(dt)
}



## GET POPULATION STRUCTURE
get_pop_structure <- function(locs, years, age_groups){
  populations <- get_population(location_id = locs, year_id = years,decomp_step = "step1",
                                sex_id = c(1, 2, 3), age_group_id = age_groups)
  age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]
  populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
  populations <- rbind(populations, age_1)  ##add age group id 1 back on
  return(populations)
}

## FORMAT SPLIT DATA TO GET IT MOSTLY READY FOR UPLOAD
format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
  
  date <- gsub("-", "_", Sys.Date())
  date <- Sys.Date()
  dt <- copy(unformatted_dt)
  #dt[, group := 1]
  dt[, was_age_split := 1]
  #dt[, group_review := 1]
  dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
  dt[, (blank_vars) := NA]
  dt <- get_se(dt)
  if (region ==T) {
    dt[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
  } else {
    dt[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_split_id, " ", date)]
  }
  split_ids <- dt[, unique(id)]
  orig_not_split <- original_dt[!(id %in% split_ids),]
  dt <- rbind(orig_not_split, dt, fill = T)
  dt <- dt[, c(names(original_dt)), with = F]
  return(dt)
}


get_se <- function(raw_dt){
  cat("\nCalling custom get_se function\n")
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]  # NaNs....varicella
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "cfr", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  return(dt)
}

