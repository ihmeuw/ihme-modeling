#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  USERNAME
# Date:    July 2019 (edits May 2020)
# Purpose: Pull DisMod age pattern draws and apply it to sex-split data for upload to bundle for GBD 2019 decomp step 2
# Source:  FILEPATH
#***********************************************************************************************************************



# apply_age_split encapsualted from shared code with change of putting dismod model version id (mvid) instead of "best" to accompany quick iterating of different models

apply_age_split <- function(data,
                            dismod_meid,
                            dismod_mvid,
                            loc_pattern = 1,
                            decomp_step_pop,
                            decomp_step_meid,
                            write_out = F,
                            out_file = NULL,
                            gbd_round = 7){

  cat("\nIf writing out, make sure to set write_out = TRUE, and out_file with complete path including .xlsx at end (not .csv!!!)\n")

  pacman::p_load(data.table, openxlsx, ggplot2, magrittr)


  # GET OBJECTS -------------------------------------------------------------

  functions_dir <- "/FILEPATH/"
  date <- gsub("-", "_", Sys.Date())
  draws <- paste0("draw_", 0:999)

  # GET FUNCTIONS -----------------------------------------------------------

  functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids")
  invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

  #' [Helper Functions]

  ## FILL OUT MEAN/CASES/SAMPLE SIZE
  get_cases_sample_size <- function(raw_dt){
    cat("\nCalling custom get_cases_sample_size function")
    cat("\nMake sure both cases and sample size are adjusted (and not the orig vals) post sex- and any dx- crosswalks!\n")
    dt <- copy(raw_dt)
    dt[is.na(mean), mean := cases/sample_size]
    dt[is.na(sample_size), sample_size := effective_sample_size]
    dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
    dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
    return(dt)
  }

  ## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
  get_se <- function(raw_dt){
    cat("\nCalling custom get_se function\n")
    dt <- copy(raw_dt)
    dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
    z <- qnorm(0.975)
    dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    dt[is.na(standard_error) & measure == "cfr", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    return(dt)
  }

  ## GET CASES IF THEY ARE MISSING
  calculate_cases_fromse <- function(raw_dt){
    cat("\nCalling custom calculate_cases_fromse function\n")
    dt <- copy(raw_dt)
    dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
    dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
    dt[is.na(cases), cases := mean * sample_size]
    return(dt)
  }

  ## MAKE SURE DATA IS FORMATTED CORRECTLY
  format_data <- function(unformatted_dt, sex_dt){
    cat("\nCalling custom format_data function\n")
    dt <- copy(unformatted_dt)
    dt[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
               age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
    dt <- dt[measure %in% c("prevalence", "incidence", "proportion", "cfr", "remission"),]
    dt <- dt[is_outlier != 1,]
    # only split rows with over 20 year age bin
    dt <- dt[(age_end-age_start)>= 20,]
    dt <- dt[!mean == 0 & !cases == 0, ] ## don't split points with zero prevalence. If have missing cases or mean in dt, this will return empty frame!
     if(!("sex_id" %in% colnames(dt))){
       dt <- merge(dt, sex_dt, by = "sex")
     }
    dt[measure == "prevalence", measure_id := 5]
    dt[measure == "incidence", measure_id := 6]
    dt[measure == "proportion", measure_id := 18]
    dt[measure == "cfr", measure_id := 18]  # calculations equivalent to proportions
    dt[measure == "remission", measure_id := 5]  # dismod treats as a rate, so calculate like prevalence
    dt[, year_id := round((year_start + year_end)/2, 0)] ##so that can merge on year later
    return(dt)
  }

  ## CREATE NEW AGE ROWS
  expand_age <- function(small_dt, age_dt = ages){
    cat("\nCalling custom expand_age function\n")
    dt <- copy(small_dt)

    ## ROUND AGE GROUPS
    dt[, age_start := sapply(1:nrow(dt), function(x) age_dt$gbd_age_start[which.min(abs(age_dt$gbd_age_start - dt$age_start[x]))])] #setting age start and age end to gbd values that are closest to the extracted vals
    dt[, age_end := sapply(1:nrow(dt), function(x) age_dt$gbd_age_end[which.min(abs(age_dt$gbd_age_end - dt$age_end[x]))])]

    ## EXPAND FOR AGE  - note here the default is that cases ~<20 all-age are getting dropped because those inform age splitting pattern and don't want to split them
    #find number age groups the original row will be split into
    dt[, n.age := sapply(1:nrow(dt), function(x) nrow(age_dt[gbd_age_start >= dt$age_start[x] & gbd_age_end <= dt$age_end[x]]))]
    dt[, age_start_floor:=age_start]
    dt[, drop := cases/n.age] # in past dropped if there would be < 1 case per age group once split but no longer dropping
    expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .) #make row for each of age groups that original row will be split into
    split <- merge(expanded, dt, by="id", all=T) #make a copy of original row for each of new rows it will be split into
    split[, age.rep := 1:.N - 1, by =.(id)] #add a column counting the number of new rows the original row is being split into (starts counting at 0)
    split[, age_start:= age_start+age.rep*5] #this line and line below are only valid if you are splitting into five year age groups
    split[, age_end :=  age_start + 4]
    setnames(age_dt, c("gbd_age_start", "gbd_age_end"), c("age_start", "age_end")) #set column names so can merge
    split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
    return(split)
  }

  ## GET DISMOD AGE PATTERN- remember to set decomp_step argument!

  #'[change this to pull from model version id]

  get_age_pattern <- function(locs, id, age_groups, decomp_step_meid){
    cat("\nCalling custom get_age_pattern function\n")

    cat(paste0("\n\n !!! Pulling Dismod draws from meid ", dismod_meid, " mvid ", dismod_mvid, " !!! \n\n"))
    cat(paste0("\n Locs are: ", locs))
    cat("\n age_group_ids are")
    cat(age_groups)

    age_pattern <- get_draws(source = "epi",
                             gbd_id_type = "modelable_entity_id",
                             gbd_id = dismod_meid,
                             version_id = dismod_mvid,
                             measure_id = c(5, 6, 18), 
                             decomp_step = "iterative",
                             sex_id = c(1,2),
                             year_id = 2010, 
                             age_group_id = age_groups,
                             location_id = locs,
                             gbd_round_id = gbd_round
    )

    cat("\nDismod draws pulled\n")

    us_population <- get_population(location_id = locs,
                                    year_id = 2010,
                                    sex_id = c(1, 2),
                                    age_group_id = age_groups,
                                    decomp_step = decomp_step_pop,
                                    gbd_round_id = gbd_round)

    us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
    age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
    age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
    age_pattern[, (draws) := NULL]
    age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]

    ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
    age_1 <- copy(age_pattern)
    age_1 <- age_1[age_group_id %in% c(2, 3, 388, 389, 238, 34), ]
    se <- copy(age_1)
    se <- se[age_group_id==34, .(measure_id, sex_id, se_dismod, location_id)] ## use standard error from 2-4 age grp gbd 2020
    age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
    age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
    age_1[, frac_pop := population / total_pop]
    age_1[, weight_rate := rate_dis * frac_pop]
    age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
    age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
    age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
    age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
    age_1[, age_group_id := 1]
    age_pattern <- age_pattern[!age_group_id %in% c(2,3,388, 389, 238, 34)]
    age_pattern <- rbind(age_pattern, age_1)

    ## CASES AND SAMPLE SIZE
    age_pattern[measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
    age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
    age_pattern[measure_id == 18, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]  # confirm probably (NG)
    age_pattern[, cases_us := sample_size_us * rate_dis]
    age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
    age_pattern[is.nan(cases_us), cases_us := 0]

    ## GET SEX ID 3
    sex_3 <- copy(age_pattern)
    sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[, rate_dis := cases_us/sample_size_us]
    sex_3[measure_id == 5, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
    sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
    sex_3[measure_id == 18, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)]
    sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0, rate and standard error should both be 0
    sex_3[is.nan(se_dismod), se_dismod := 0]
    sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
    sex_3[, sex_id := 3]
    age_pattern <- rbind(age_pattern, sex_3)

    age_pattern[, super_region_id := location_id]
    age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
    return(age_pattern)
  }

  ## GET POPULATION STRUCTURE
  get_pop_structure <- function(locs, years, age_groups){
    cat("\nCalling custom get_pop_structure function\n")
    cat(paste0("Getting pop for age groups:", age_groups))
    cat("\n")
    cat(paste0("Getting pop from decomp step: ", decomp_step_pop))
    cat("\n")
    cat(paste0("Getting pop for years: ", years, " The years for which there is data to split."))
    cat("\n")
    cat(paste0("Getting pop for location_ids: ", locs))
    cat("\n")
    cat("Calling get_population")
    populations <- get_population( location_id = locs,
                                   year_id = years,
                                   decomp_step = decomp_step_pop,
                                   sex_id = c(1, 2, 3),
                                   age_group_id = age_groups,
                                   gbd_round_id = gbd_round)
    cat("Successfully called  get_population\n")
    age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
    age_1 <- age_1[age_group_id %in% c(2, 3, 388, 389, 238, 34)]
    age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
    age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
    age_1[, age_group_id := 1]
    populations <- populations[!age_group_id %in% c(2, 3, 388, 389, 238, 34)]
    populations <- rbind(populations, age_1)  ##add age group id 1 back on
    return(populations)
  }

  ## ACTUALLY SPLIT THE DATA
  if (dismod_meid %in% c(24320)) {  # these are NON-PROPORTIONAL models: herpes zoster incidence
    split_data <- function(raw_dt){
      cat("\nCalling custom split_data function\n")
      dt <- copy(raw_dt)
      dt[, total_pop := sum(population), by = "id"]
      dt[, sample_size := (population / total_pop) * sample_size]
      dt[, cases_dis := sample_size * rate_dis]
      dt[, total_cases_dis := sum(cases_dis), by = "id"]
      dt[, total_sample_size := sum(sample_size), by = "id"]
      dt[, all_age_rate := total_cases_dis/total_sample_size]
      dt[, ratio := mean / all_age_rate]
      dt[, mean := ratio * rate_dis]
      dt[, cases := mean * sample_size]
      return(dt)
    }
  } else if (dismod_meid %in% c(24317, 24318, 24319)) {  # these are the PROPORTIONAL models: tetanus & diphtheria CFR; varicella seroprevalence
    split_data <- function(raw_dt){

      # Goal of this function: shift age pattern of CFRs from dismod such that
      # The population weighted average of the shifted age-specific CFRs
      # Is the same as what was observed within the original group

      # Define some functions
      library(arm)

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
      dt[, mean := invlogit(logit(rate_dis) + k)]

      # Calculate cases
      dt[, cases := sample_size * mean]

      return(dt)
    }
  }


  ## FORMAT DATA TO FINISH
  format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
    cat("\nCalling custom format_data_forfinal function\n")
    dt <- copy(unformatted_dt)
    dt[, group := 1]
    dt[, specificity := "age,sex"]
    dt[, group_review := 1]
    dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq] #if this isn't a row that was already sex split, need to assign xw_parent_seq
    blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
    dt[, (blank_vars) := NA]
    dt <- get_se(dt)
    if (region ==T) {
      dt[, note_modeler := paste0(note_modeler, "| age split from xw_id ", ss_xw_id, " using the super region age pattern", date)]
    } else {
      dt[, note_modeler := paste0(note_modeler, "| age split from xw_id ", ss_xw_id, " using the age pattern from location id ", location_split_id, " ", date, " ", dismod_mvid)]
    }
    split_ids <- dt[, unique(id)]
    dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
    dt <- dt[, c(names(df)), with = F]
    return(dt)
  }

  ########################################################################################################
  ######### MAIN WORKER FUNCTION THAT ESSENTIAL CALLS ALL THE SMALLER FUNCTIONS WE DEFINED ABOVE ##########
  ########################################################################################################
  dt <- copy(data)

  #create variable age_diff to enable filtering of output dataset to check splits
  dt$age_diff<-dt$age_end - dt$age_start
  cat("\nCalculating age_diff")

  ages <- get_age_metadata(19, gbd_round_id = gbd_round)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("gbd_age_start", "gbd_age_end"))
  #age_groups - here you can indicate a start year (emma's example was 40)
  #age_groups <- ages[age_start >= 1, age_group_id]
  age_groups <- ages[,age_group_id]


  #MEID from dismod model
  df <- copy(dt)
  age <- age_groups
  mv_id <- dismod_mvid

  age_split <- function(gbd_id, df, age, region_pattern, location_pattern_id){
    cat("\nStarting Age Split \n")
    ## GET TABLES
    sex_names <- get_ids(table = "sex")
    ages <- get_age_metadata(19, gbd_round_id = gbd_round)
    setnames(ages, c("age_group_years_start", "age_group_years_end"), c("gbd_age_start", "gbd_age_end"))
    ages[, c("age_group_weight_value", "age_group_name", "most_detailed") := NULL]
   #create custom u5 age group
    under5 <- ages[gbd_age_start<5]$age_group_id
    ages <- rbind(ages,
                  data.table(age_group_id = 1, gbd_age_start = 0, gbd_age_end = 5))
    # remove unused age_group_ids
    ages <- ages[!age_group_id %in% under5]

    ages[ , gbd_age_end := gbd_age_end - 1]
    ages[gbd_age_end == 124, gbd_age_end := 99]
    super_region_dt <- get_location_metadata(location_set_id = 22, gbd_round_id = gbd_round, decomp_step = decomp_step)
    super_region_dt <- super_region_dt[, .(location_id, super_region_id)]


    ## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
    original <- copy(df)
    original[, id := 1:.N]

    ## FORMAT DATA -- CALLING FUNCTIONS SOURCED ABOVE!!!
    dt <- get_cases_sample_size(original)
    dt <- get_se(dt)
    dt <- calculate_cases_fromse(dt)
    dt <- format_data(dt, sex_dt = sex_names)
    if(length(which(is.na(dt$cases)))==0){
      cat("\nAwesome, you have 0 rows missing cases.")
    } else{
      cat("\nSTOP!!!! YOU ARE MISSING CASES! This is probably why function will break.")
    }

    ## EXPAND AGE
    split_dt <- expand_age(dt, age_dt = ages)

    ## GET PULL LOCATIONS
    if (region_pattern == T){
      split_dt <- merge(split_dt, super_region, by = "location_id")
      super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
      locations <- super_regions
    } else {
      locations <- location_pattern_id
    }

    ##GET LOCS AND POPS
    pop_locs <- unique(split_dt$location_id)
    cat(paste0("pop locs are :", pop_locs))
    pop_years <- unique(split_dt$year_id)

    ## GET AGE PATTERN
    print("getting age pattern")
    age_pattern <- get_age_pattern(locs = locations, id = mv_id, age_groups = age)

    if (region_pattern == T) {
      age_pattern1 <- copy(age_pattern)
      split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
    } else {
      age_pattern1 <- copy(age_pattern)
      if (!("sex_id" %in% names(split_dt))) { split_dt[, sex_id := ifelse(sex == "Male", 1, ifelse(sex == "Female", 2, 3))]}
      split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
    }

    ## GET POPULATION INFO
    print("getting pop structure")
    pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
    split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))

    #####CALCULATE AGE SPLIT POINTS#######################################################################
    ## CREATE NEW POINTS
    print("\nsplitting data")
    split_dt <- split_data(split_dt)
    ######################################################################################################

    final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                     original_dt = original)

    ## BREAK IF NO ROWS
    if (nrow(final_dt) == 0){
      print("nothing in bundle to age-sex split")
      break
    }
    return(final_dt)
  }

  ############################################################
  ### THIS IS LINE WHERE THE FUNCTIONS AS DEFINED ABOVE GET APPLIED TO DT WHEN CALL apply_age_split()
  ############################################################

  final_split <- age_split(gbd_id = id,
                           df = dt,
                           age = age_groups,
                           region_pattern = F,
                           location_pattern_id = loc_pattern)

  cat(paste0("\n Age-Splits by location pattern: ", loc_pattern,"\n"))

  if (write_out == T){
    openxlsx::write.xlsx(final_split, out_file, sheetName = "extraction")
    cat(paste0("Wrote file at ", out_file))
  }
  return(final_split)
}
