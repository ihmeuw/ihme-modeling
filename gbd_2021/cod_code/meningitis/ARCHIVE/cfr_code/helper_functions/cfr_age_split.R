#' @author 
#' @date 2020/04/14
#' @description age_split
#' @function run_sex_split
#' @param dt bundle data used to be age split
#' @return final_age_split_dt , all data, age-split

library(dplyr); library(parallel); library(arm)

# SOURCE FUNCTIONS --------------------------------------------------------
# Source GBD shared functions
invisible(sapply(list.files("/filepath/", full.names = T), source))
## HELPER FUNCTIONS FOR ETIOLOGY AGE SPLITTING
source('/filepath/get_cases_sample_size.R')
source('/filepath/get_closest_age.R')
source('/filepath/get_gbd_age_group_id.R')
source('/filepath/mean_ui.R')

# find the correct age group(s)
find_closest <- function(number, vector){
  index <- which.min(abs(number-vector))
  closest <- vector[index]
  return(closest)
}

## CREATE NEW AGE ROWS
expand_age <- function(small_dt, age_dt = ages){
  cat("\nCalling custom expand_age function\n")
  dt <- copy(small_dt)
  
  ## EXPAND FOR AGE  - note here the default is that cases ~<20 all-age are getting dropped because those inform age splitting pattern and don't want to split them
  #dt[, n.age:=(age_end+1 - age_start)/5] #find number age groups the original row will be split into (only works if all 5 year age groups)
  dt[, n.age := sapply(1:nrow(dt), function(x) nrow(age_dt[gbd_age_start >= dt$gbd_age_start[x] & gbd_age_end <= dt$gbd_age_end[x]]))]
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .) #make row for each of age groups that original row will be split into
  split <- merge(expanded, dt, by="id", all=T) #make a copy of original row for each of new rows it will be split into
  for (my_id in unique(split$id)) {
    source_age_start <- unique(split[`id` == my_id,gbd_age_start])
    source_age_end <- unique(split[`id`== my_id,gbd_age_end])
    my_ages <- age_dt[gbd_age_start >= source_age_start & gbd_age_end <= source_age_end]
    split[id==my_id, age_group_name := my_ages$age_group_name]
  }
  
  #split[, age.rep := 1:.N - 1, by =.(id)] #add a column counting the number of new rows the original row is being split into (starts counting at 0)
  #split[, age_start:= age_start+age.rep*5] #this line and line below are only valid if you are splitting into five year age groups
  #split[, age_end :=  age_start + 4]
  
  # #set column names so can merge - now merging on age group id so can flex to accomodate more granular age groups
  split[, c("age_start", "age_end", "gbd_age_start", "gbd_age_end") := NULL]
  split <- merge(split, age_dt, by = "age_group_name", all.x = T)
  setnames(split,c("gbd_age_start", "gbd_age_end"), c("age_start", "age_end"))
  #split <- split[age_group_id %in% age | age_group_id == 1] ##don't keep where age group id isn't estimated for cause
  return(split)
}

get_meningitis_mortality <- function(gbd_round_id){
  cat("\nCalling custom mortality pull function\n")

  # get age pattern for 
  mort_pattern <- get_draws(source = "codcorrect",
                            gbd_id_type = "cause_id",
                           gbd_id = 332,
                           measure_id = 1, 
                           metric_id = 1,
                           decomp_step = "step2",
                           sex_id = 3,
                           year_id = 2010,
                           status = "latest",
                           # age_group_id = "all",
                           location_id = 1, 
                           gbd_round_id = gbd_round_id
  )
  
  case_pattern <- get_draws(source = "epi",
                           gbd_id_type = "modelable_entity_id",
                           gbd_id = 1296,
                           version_id = 531500,
                           measure_id = 6,
                           decomp_step = ds,
                           sex_id = 3,
                           year_id = 2010,
                           # age_group_id = "all",
                           location_id = 1, 
                           gbd_round_id = gbd_round_id
  )
  
  pop <- get_population(location_id = 1,
                        year_id = 2010,
                        sex_id = 3,
                        gbd_round_id = 7,
                        age_group_id = "all",
                        decomp_step = "iterative", 
                        status = "best")
  
  case_pattern <- merge(case_pattern, pop, by = c("age_group_id", "location_id", "sex_id", "year_id"), all.x = T)
  cols.remove <- c("model_version_id", "modelable_entity_id", "measure_id", "metric_id", "cause_id", "year_id", "run_id")
  case_pattern[, (cols.remove):= NULL]
  mort_pattern[, (cols.remove):= NULL]
  
  # multiply rate in case pattern by population to get counts
  draws <- names(case_pattern)[which(names(case_pattern) %like% "draw")]
  case_pattern[ , (draws) := lapply(.SD, "*", population), .SDcols = draws]
  
  # reshape wide to long
  id_vars <- c("location_id", "sex_id", "age_group_id")
  case_pattern <- melt(case_pattern, id.vars = id_vars, variable.name = "draw", 
                     value.name = "cases")
  mort_pattern <- melt(mort_pattern, id.vars = id_vars, variable.name = "draw", 
                       value.name = "deaths")
  
  cfr_pattern <- merge(case_pattern, mort_pattern, by = c(id_vars, "draw"))
  cfr_pattern[, cfr := deaths/cases]

  # reshape back to wide 
  cfr_pattern <- data.table::dcast(cfr_pattern, location_id + sex_id + age_group_id ~ draw, 
                                                value.var = c("cases", "deaths", "cfr"))
  
  # take the mean and SE of the draws 
  cfr_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = paste0("cfr_", draws[which(nchar(draws)<8)])]
  cfr_pattern[, rate_dis := rowMeans(.SD), .SDcols = paste0("cfr_", draws[which(nchar(draws)<8)])]
  
  cfr_pattern[, cases_us := apply(.SD, 1, sd), .SDcols = paste0("deaths_", draws[which(nchar(draws)<8)])]
  cfr_pattern[, sample_size_us := apply(.SD, 1, sd), .SDcols = paste0("cases_", draws[which(nchar(draws)<8)])]
  
  return(cfr_pattern)
}

get_overall_meningitis <- function(gbd_round_id, ds){
  # get age pattern for overall meningitis
  
  overall_men <- get_draws(source = "epi",
                           gbd_id_type = "modelable_entity_id",
                           gbd_id = 1296,
                           version_id = 531500,
                           measure_id = 6,
                           decomp_step = ds,
                           sex_id = 3,
                           year_id = 2010,
                           # age_group_id = "all",
                           location_id = 1, 
                           gbd_round_id = gbd_round_id
  )
  
  pop <- get_population(location_id = 1,
                        year_id = 2010,
                        sex_id = 3,
                        gbd_round_id = 7,
                        age_group_id = "all",
                        decomp_step = "iterative", 
                        status = "best")
  draws <- names(overall_men)[which(names(overall_men) %like% "draw")]
  
  overall_men[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  overall_men[, rate_dis := rowMeans(.SD), .SDcols = draws]
  overall_men[, (draws) := NULL]
  overall_men <- overall_men[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
  
  pop <- pop[,.(sex_id, age_group_id, location_id, population)]
  overall_men <- merge(overall_men, pop, by = c("age_group_id", "location_id", "sex_id"), all.x = T)
  
  ## CASES AND SAMPLE SIZE
  setnames(overall_men, "population", "sample_size_us")
  overall_men[, cases_us := sample_size_us * rate_dis]
  overall_men[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
  overall_men[is.nan(cases_us), cases_us := 0]
  
  overall_men <- overall_men[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod)]
  return(overall_men)
}



split_data <- function(raw_dt){  # original function name -- split_data_logit() -- changed here to not have to change fxn call
  
  # Goal of this function: shift age pattern of CFRs from dismod such that
  # The population weighted average of the shifted age-specific CFRs
  # Is the same as what was observed within the original group
  
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
      stop(paste0("The length of weights and cfrs for id ", the_id, " are not equal"))
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
  
  # Calculate the total overall meningitis (population)
  dt[, total_overall_meningitis := sum(cases_us), by = "id"]
  
  # Make a copy of overall sample size in the group
  dt[, total_sample_size := copy(sample_size)]
  
  # Make a copy of total cases in the group
  dt[, total_cases := copy(cases)]
  
  # Calculate the fraction of GBD overall meningitis that fall within this age bin
  dt[, men_fraction_dis := cases_us / total_overall_meningitis]
  
  # Redistribute the sample size proportional to GBD meningitis estimates
  dt[, sample_size := total_sample_size * men_fraction_dis]
  
  # Generate table of `k` values for each id and merge to dt
  k_table <- rbindlist(lapply(unique(dt$id), function(i) {logit_trans(i, dt)}))
  dt <- merge(dt, k_table, by = "id")
  
  # Create copy of original mean
  dt[, original_mean := copy(mean)]
  
  # Transform mean using our values of `k` for each id
  dt[, mean := invlogit(logit(rate_dis) + k)]  #dt[, mean := invlogit(logit(rate_dis) + k)]  #NG: change from 'invlogit' to 'ilogit', weird
  
  # Calculate cases
  dt[, cases := sample_size * mean]
  
  return(dt)
}

## AGGREGATE SPECIFIC AGES FOR ETIOS OR OVERALL MENINGITIS
aggregate_ages <- function(dt, age_dt, age_start_vector, age_end_vector){
  dt_subset_list <- mclapply(1:length(age_start_vector), function(i, age_dt, age_start_vector, age_end_vector){
    age_start <- age_start_vector[i]
    age_end <- age_end_vector[i]
    # merge on age metadata to get age start and end
    dt <- merge(dt, age_dt, all.x = T, by = "age_group_id")
    dt$measure_id <- 18
    dt_subset <- dt[gbd_age_start >= age_start & gbd_age_start < age_end]
    # sum cases, sample size, and age group weight values values for the dt_subset 
    dt_subset[, total_subset_age_weight:=sum(age_group_weight_value), by=c("sex_id")]
    dt_subset[, cases_us:=sum(cases_us), by=c("sex_id")]
    dt_subset[, sample_size_us:=sum(sample_size_us), by=c("sex_id")]
    dt_subset[,age_group_weight_value := age_group_weight_value/total_subset_age_weight]
    # multiply age group weights by rate
    dt_subset[,weighted_rate := age_group_weight_value*rate_dis]
    # sum those across all relevant age groups to get rate
    dt_subset[, rate_dis:=sum(weighted_rate), by=c("sex_id")]
    # finalize
    dt_subset <- dt_subset[age_group_id == unique(dt_subset$age_group_id)[1]]
    dt_subset$age_group_name <- paste(age_start, "to", age_end)
    dt_subset$age_group_id <- NA
    dt_subset[, `:=` (gbd_age_start = age_start, gbd_age_end = age_end)]
    # back calculate SE from the opposite of the formulas in get_overall_meningitis
    # dt_subset[measure_id == 5, se_dismod := sqrt(rate_dis * (1-rate_dis)/sample_size_us)]
    # dt_subset[measure_id == 6, se_dismod := sqrt(rate_dis/sample_size_us)]
    dt_subset[, se_dismod := sqrt(rate_dis * (1-rate_dis)/sample_size_us)] 
    dt_subset <- dt_subset[, c("sex_id", "measure_id", "cases_us", "sample_size_us", 
                               "rate_dis", "se_dismod", "age_group_name", "gbd_age_start", "gbd_age_end")]
  } , age_dt = age_dt, age_start_vector = age_start_vector, age_end_vector = age_end_vector,
  mc.cores = length(age_start_vector)) 
  dt <- rbindlist(dt_subset_list)
  return(dt)
}

## AGGREGATE AGE_DT TO AGG AGES
agg_age_dt <- function(dt, age_start_vector, age_end_vector){
  dt_subset_list <- mclapply(1:length(age_start_vector), function(i, age_start_vector, age_end_vector){
    age_start <- age_start_vector[i]
    age_end <- age_end_vector[i]
    dt_subset <- dt[gbd_age_start >= age_start & gbd_age_start < age_end]
    # scale age group weight values for the dt_subset 
    dt_subset[, age_group_weight_value:=sum(age_group_weight_value)]
    # dt_subset[,age_group_weight_value := age_group_weight_value/total_subset_age_weight]
    dt_subset <- dt_subset[1,]
    dt_subset$age_group_name <- paste(age_start, "to", age_end)
    dt_subset$age_group_id <- NA
    dt_subset[, `:=` (gbd_age_start = age_start, gbd_age_end = age_end)]
  } , age_start_vector = age_start_vector, age_end_vector = age_end_vector,
  mc.cores = length(age_start_vector)) 
  dt <- rbindlist(dt_subset_list)
  return(dt)
}

## MAIN FUNCTION FOR ETIOLOGY AGE SPLITTING

age_split_data <- function(xwalk_final_dt, age_cats, gbd_round_id, ds, bundle){
  
  dt <- copy(xwalk_final_dt)
  # age_cats <- c("age_u5", "age_5_65", "age_65plus")

  # pull gbd age group mapping
  age_dt <- get_age_metadata(19, gbd_round_id = gbd_round_id)
  setnames(age_dt, c("age_group_years_start", "age_group_years_end"), c("gbd_age_start", "gbd_age_end"))
  age_start_vector <- c(0, 5.00, 65.00)
  age_end_vector <- c(4.99, 64.99, 125)
  age_dt_agg <- agg_age_dt(dt = age_dt, age_start_vector = age_start_vector, age_end_vector = age_end_vector)
  
  ages <- copy(age_dt_agg)
  
  #create variable age_diff to enable filtering of output dataset to check splits
  dt$age_diff<-dt$age_end - dt$age_start
  
  dt$effective_sample_size <- dt$sample_size
  original <- copy(dt)
  original[, id := 1:.N]
  
  # fill in cases, sample size, mean - including getting SS from effective SS!
  dt <- get_cases_sample_size(original)
  
  # age split rows that don't fit into the <5, 5-65, or 65+ age bins neatly
  # sample size will be NA but you can use effective_sample_size and set that to sample_size
  # only split rows with a large enough sample size.
  no_split_rows <- copy(dt[, Reduce(`|`, lapply(.SD, `==`, 1)),.SDcols = age_cats])
  no_split_dt <- copy(dt[no_split_rows])
  split_dt <- copy(dt[!(no_split_rows)])

  if (nrow(split_dt) + nrow(no_split_dt) != nrow(dt)) {
    stop("Dataframe to be age-split and dt not to be split are not mutually exclusive and collectively exhauastive")
  }
  if (nrow(split_dt) == 0) {
    stop("there are no rows to age split.  confirm that this is correct.  if so, skip age split.")
  }

  # convert to age_start and age_end to closest gbd age groups
  split_dt[, gbd_age_start:= sapply(1:nrow(split_dt), function(i) {
    age <- split_dt[i, age_start]
    get_closest_age(start = T, age, ages)
  })]
  split_dt[, gbd_age_end:= sapply(1:nrow(split_dt), function(i) {
    age <- split_dt[i, age_end]
    get_closest_age(start = F, age, ages)
  })]
  
  # hacky moment sorry
  split_dt[gbd_age_start > gbd_age_end, `:=` (gbd_age_start = 0, gbd_age_end = 125)]
  
  # "save a copy" of split_dt before splitting numerator and denominator
  pre_split_dt <- copy(split_dt)
  
  # get overall meningitis (SAMPLE SIZE)
  overall_meningitis <- get_overall_meningitis(gbd_round_id, ds)
  overall_meningitis <- aggregate_ages(overall_meningitis, age_dt, age_start_vector, age_end_vector)
  # set naming of meningitis cases to "population" for the sake of the following code
  overall_meningitis[sex_id == 3, sex := "Both"]
  overall_meningitis$rate_dis <- NULL
  # overall_meningitis[sex_id == 2, sex := "Female"]
  # overall_meningitis[, c("rate_dis", "se_dismod", "sample_size_us", "measure_id", "sex_id") := NULL]
  
  # get overall deaths (RATE)
  mort_pattern <- get_meningitis_mortality(gbd_round_id)
  mort_pattern <- aggregate_ages(mort_pattern, age_dt, age_start_vector, age_end_vector)
  # need to recalculate the proportion since only cases and sample size were aggregated
  mort_pattern[, rate_dis := cases_us/sample_size_us]
  mort_pattern[sex_id == 3, sex := "Both"]
  # mort_pattern[sex_id == 2, sex := "Female"]
  mort_pattern[, c("sex_id", "cases_us", "sample_size_us") := NULL]
  
  split_dt <- expand_age(split_dt, age_dt = ages)
  
  # SPLIT THE DATA
  # rename the age group names in overall meningitis and in the age pattern
  # overall_meningitis[age_group_name == "60.01 to 80", age_group_name := "60.01 to 125"]
  # age_pattern[age_group_name == "60.01 to 80", age_group_name := "60.01 to 125"]
  split_dt <- merge(split_dt, overall_meningitis, by = c("age_group_name", "sex"))
  split_dt <- merge(split_dt, mort_pattern, by = c("age_group_name", "sex"))
  dt <- copy(split_dt)
  split_dt <- split_data(split_dt)
  
  # PREP FOR FINAL
  split_dt[, `:=` (specificity = "age,sex", group = 1, group_review = 1)]
  split_dt$crosswalk_parent_seq <- NA
  split_dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq] #if this isn't a row that was already sex split, need to assign xw_parent_seq
  blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
  split_dt[, (blank_vars) := NA]
  # dt <- get_se(dt)
  # dt <- col_order(dt)
  split_dt[, note_modeler := "age split"] 
  cols_keep <- names(no_split_dt)
  split_dt <- split_dt[, (cols_keep), with = F]
  final_dt <- rbind(split_dt, no_split_dt)
  return(final_dt)
}


