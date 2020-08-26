apply_age_split <- function(data, 
                            dismod_meid, 
                            dismod_mvid, 
                            loc_pattern = 1,
                            decomp_step_pop, 
                            decomp_step_meid, 
                            write_out = F, 
                            out_file = NULL,
                            start_split = 1,
                            end_split = 99, 
                            min_cases = NA,
                            prop_smm = NA,
                            round_id = 6){
  
  if (!("sex_id" %in% names(data))) { data[, sex_id := ifelse(sex == "Male", 1, ifelse(sex == "Female", 2, 3))]}
  
  if (Sys.info()["sysname"] == "Linux") {
    j_root <- "FILEPATH" 
    h_root <- "FILEPATH"
    l_root <- "FILEPATH"
  } else { 
    j_root <- "FILEPATH"
    h_root <- "FILEPATH"
    l_root <- "FILEPATH"
  }
  
  pacman::p_load(data.table, openxlsx, ggplot2, magrittr)
  
  date <- gsub("-", "_", Sys.Date())
  date <- Sys.Date()
  
  # GET OBJECTS -------------------------------------------------------------
  
  functions_dir <- "FILEPATH"
  date <- gsub("-", "_", date)
  draws <- paste0("draw_", 0:999)
  
  # GET FUNCTIONS -----------------------------------------------------------
  
  functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids")
  invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))
  
  #' [Helper Functions]
  
  get_cases_sample_size <- function(raw_dt){
    cat("\nCalling custom get_cases_sample_size function")
    dt <- copy(raw_dt)
    dt[is.na(mean), mean := cases/sample_size]
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
    dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
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
    dt <- dt[measure %in% c("prevalence", "incidence", "proportion"),]
    dt <- dt[is_outlier==0,] 
    dt <- dt[(age_end-age_start)>25,]
    dt <- dt[!mean == 0 & !cases == 0, ] 
    if (!('sex_id' %in% names(dt))){
      dt <- merge(dt, sex_dt, by = "sex")
    }
    dt[measure == "prevalence", measure_id := 5]
    dt[measure == "incidence", measure_id := 6]
    dt[measure == "proportion", measure_id := 18]
    dt[, year_id := round((year_start + year_end)/2, 0)] ##so that can merge on year later
    return(dt)
  }
  
  ## CREATE NEW AGE ROWS
  expand_age <- function(small_dt, age_dt = ages){
    cat("\nCalling custom expand_age function\n")
    dt <- copy(small_dt)
    dt[, age_group_id := NULL]
    ## ROUND AGE GROUPS
    dt[, age_start := age_start - age_start %%5]
    dt[, age_end := age_end - age_end %%5 + 4]
    dt <- dt[age_end > 99, age_end := 99]
    
    ## EXPAND FOR AGE  
    dt[, n.age:=(age_end+1 - age_start)/5]
    dt[, age_start_floor:=age_start]
    dt[, drop := cases/n.age] 
    
    if (!(is.na(min_cases))){
      dt <- dt[cases >= min_cases,]
      dt[cases >= min_cases, drop := 0.5]
      } else {
      }
    
    expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
    split <- merge(expanded, dt, by="id", all=T)
    split[, age.rep := 1:.N - 1, by =.(id)]
    split[, age_start:= age_start+age.rep*5]
    split[, age_end :=  age_start + 4]
    split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
    split[age_start == 0 & age_end == 1, age_group_id := 1]
    split <- split[age_group_id %in% age | age_group_id == 1] ##don't keep where age group id isn't estimated for cause
    return(split)
  }
  
  ## GET DISMOD AGE PATTERN
  
  #'[change this to pull from model version id]
  
  get_age_pattern <- function(locs, id, age_groups){
    cat("\nCalling custom get_age_pattern function\n")

    cat(paste0("\n\n !!! Pulling Dismod draws from meid ", dismod_meid, " mvid ", dismod_mvid, " !!! \n\n"))
    cat(paste0("\nLocs are: ", locs))
    cat("\nage_group_ids are") 
    cat(age_groups)
    
    if (is.na(prop_smm)){
    age_pattern <- get_draws(source = "epi",
                             gbd_id_type = "modelable_entity_id", 
                             gbd_id = dismod_meid,
                             version_id = dismod_mvid,
                             measure_id = c(5, 6),
                             decomp_step = decomp_step_meid,
                             sex_id = c(1,2),
                             year_id = 2010,
                             age_group_id = age_groups,
                             location_id = locs,
                             gbd_round_id = round_id
    )} else {
      age_pattern <- get_draws(source = "epi",
                               gbd_id_type = "modelable_entity_id", 
                               gbd_id = dismod_meid,
                               version_id = dismod_mvid,
                               #measure_id = c(5, 6),
                               decomp_step = decomp_step_meid,
                               sex_id = c(1,2),
                               year_id = 2010,
                               age_group_id = age_groups,
                               location_id = locs,
                               gbd_round_id = round_id
      )
    } 
    
    cat("\nDismod draws pulled\n")
    
    us_population <- get_population(location_id = locs, 
                                    year_id = 2010, 
                                    sex_id = c(1, 2), 
                                    age_group_id = age_groups, 
                                    decomp_step = decomp_step_pop,
                                    gbd_round_id = round_id)
    
    draws <- paste0("draw_", 0:999)
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
    
    # split proportion as would split prevalence
    age_pattern[measure_id == 18, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
    age_pattern[measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
    age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
    age_pattern[, cases_us := sample_size_us * rate_dis]
    age_pattern[is.nan(sample_size_us), sample_size_us := 0] 
    age_pattern[is.nan(cases_us), cases_us := 0]
    
    ## GET SEX ID 3
    sex_3 <- copy(age_pattern)
    sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
    sex_3[, rate_dis := cases_us/sample_size_us]
    
    # do for proportion as prevalence
    sex_3[measure_id == 18, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
    sex_3[measure_id == 5, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
    sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
    sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
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
    cat(age_groups)
    cat("\n")
    cat(decomp_step_pop)
    cat("\n")
    cat(years)
    cat("\n")
    cat(locs)
    cat("calling population")
    populations <- get_population( location_id = locs, 
                                  year_id = years,
                                  decomp_step = decomp_step_pop,
                                  sex_id = c(1, 2, 3), 
                                  age_group_id = age_groups,
                                  gbd_round_id = round_id)
    cat("after population")
    age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
    age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
    age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
    age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
    age_1[, age_group_id := 1]
    populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
    populations <- rbind(populations, age_1)  ##add age group id 1 back on
    return(populations)
  }
  
  ## SPLIT THE DATA
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
  
  ## FORMAT DATA TO FINISH
  format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
    cat("\nCalling custom format_data_forfinal function\n")
    dt <- copy(unformatted_dt)
    dt[, group := 1]
    dt[, specificity := "age,sex"]
    dt[, group_review := 1]
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
    dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
    dt <- dt[, c(names(df)), with = F]
    return(dt)
  }
  
  ## EXAMPLE
  dt <- data
  
  dt$cases1<-ceiling(dt$mean*dt$sample_size)
  cat("\n Make sure cases reflect adjusted prevalence")
  
  #change any age = 0 to age = 1
  dt[age_start == 0, age_start := 1]
  
  #create variable age_diff to enable filtering of output dataset to check splits
  dt$age_diff<-dt$age_end - dt$age_start
  
  ages <- get_age_metadata(12,
                           gbd_round_id = round_id)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  #age_groups - here you can indicate a start year 
  age_groups <- ages[age_start >= start_split & age_end <= end_split, age_group_id]
  
  #Model id from dismod model
  id <- dismod_mvid
  
  df <- copy(dt)
  age <- age_groups
  gbd_id <- id
  
  age_split <- function(gbd_id, df, age, region_pattern, location_pattern_id){
    cat("\nStarting Age Split \n")
    ## GET TABLES
    sex_names <- get_ids(table = "sex")
    ages <- get_age_metadata(12,
                             gbd_round_id = round_id)
    setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
    ages[, age_group_weight_value := NULL]
    ages[age_start >= 1, age_end := age_end - 1]
    ages[age_end == 124, age_end := 99]
    super_region_dt <- get_location_metadata(location_set_id = 22,
                                             gbd_round_id = round_id)
    super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
    
    ## SAVE ORIGINAL DATA
    original <- copy(df)
    original[, id := 1:.N]
    
    ## FORMAT DATA
    dt <- format_data(original, sex_dt = sex_names)
    dt <- get_cases_sample_size(dt)
    dt <- get_se(dt)
    dt <- calculate_cases_fromse(dt)
    
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
    age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age)
  
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
    print("splitting data")
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
  
  final_split <- age_split(gbd_id = id, 
                           df = dt, age = 
                             age_groups, 
                           region_pattern = F, 
                           location_pattern_id = loc_pattern)
  
  cat(paste0("\n Age-Splits by location pattern: ", loc_pattern,"\n"))
  return(final_split)

  if (write_out == T){
    openxlsx::write.xlsx(final_split, out_file, sheetName = "extraction")
    cat(paste0("Wrote file at ", out_file))
  }
}
