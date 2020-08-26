#### Suite of functions for use in do_agesex_split.R
# - expand_test_data
# - add_pops
# - pull_model_weights
# - gen_cases_sample_size
# - gen_se
# - calculate_cases_fromse
# - get_upper_lower
# - pull_bundle_data
# - pool_across_years

####################################################################################
expand_test_data <- function(agg.test){
  #' @description Expands the number of rows in the test/aggregated age group dataset to 
  #' appropriate number of GBD age and sex groups
  #' @param agg.test data.table. The test data from the output of divide_data
  #' @return A data.table with additional rows. One row in the old test data will lead to 'n' number of rows in new test data
  #' where 'n' is the number of GBD age*sex groups within the test data interval
  test <- copy(agg.test)
  setnames(test, c("age_start", "age_end", "sex_id"), c("agg_age_start", "agg_age_end", "agg_sex_id"))
  test[, `:=`(split.id = 1:.N,
              n.sex = ifelse(agg_sex_id==3, 2, 1))]
  test[agg_age_start > 1, n.age := (agg_age_end + 1 - agg_age_start)/5]
  test[agg_age_start == 1, n.age := (agg_age_end + 1)/5]
  test[agg_age_start == 0.07671233 & agg_age_end != 0.999, n.age := ((agg_age_end + 1)/5) + 1]
  test[agg_age_start == 0.07671233 & agg_age_end == 0.999, n.age := 1]
  test[agg_age_start == 0.01917808 & agg_age_end != 0.999 & agg_age_end != 0.07671233, n.age := ((agg_age_end + 1)/5) + 2]
  test[agg_age_start == 0.01917808 & agg_age_end == 0.07671233, n.age := 1]
  test[agg_age_start == 0.01917808 & agg_age_end == 0.999, n.age := 2]
  test[agg_age_start == 0 & agg_age_end != 0.07671233 & agg_age_end != 0.999 & agg_age_end != 0.01917808, n.age := ((agg_age_end + 1)/5) + 3]
  test[agg_age_start == 0 & agg_age_end == 0, n.age := 1]
  test[agg_age_start == 0 & agg_age_end == 0.01917808, n.age := 1]
  test[agg_age_start == 0 & agg_age_end == 0.07671233, n.age := 2]
  test[agg_age_start == 0 & agg_age_end == 0.999, n.age := 3]
  
  ## Expand for age
  test[, age_start_floor := agg_age_start]
  expanded <- rep(test[,split.id], test[,n.age]) %>% data.table("split.id" = .)
  test <- merge(expanded, test, by="split.id", all=T)
  test[, age.rep := (1:.N) - 1, by=split.id]
  
  test[age_start_floor > 1, age_start:= agg_age_start + age.rep * 5 ]
  test[age_start_floor == 1 & age.rep == 0, age_start:= 1]
  test[age_start_floor == 1 & age.rep > 0, age_start:= age.rep * 5]
  test[age_start_floor == 0.07671233 & age.rep == 0, age_start:= 0.07671233]
  test[age_start_floor == 0.07671233 & age.rep == 1, age_start:= 1]
  test[age_start_floor == 0.07671233 & age.rep > 1, age_start:= (age.rep - 1) * 5]
  test[age_start_floor == 0.01917808 & age.rep == 1, age_start:= 0.07671233]
  test[age_start_floor == 0.01917808 & age.rep == 2, age_start:= 1]
  test[age_start_floor == 0.01917808 & age.rep > 2, age_start:= (age.rep - 2) * 5]
  test[age_start_floor == 0 & age.rep == 0, age_start:= 0.00]
  test[age_start_floor == 0 & age.rep == 1, age_start := 0.01917808]
  test[age_start_floor == 0 & age.rep == 2, age_start:= 0.07671233]
  test[age_start_floor == 0 & age.rep == 3, age_start:= 1]
  test[age_start_floor == 0 & age.rep > 3, age_start:= (age.rep - 3) * 5]
  
  test[age_start > 1, age_end :=  age_start + 4]
  test[age_start == 1, age_end := 4]
  test[age_start == 0.07671233, age_end := 0.999]
  test[age_start == 0.01917808, age_end := 0.0764]
  test[age_start == 0, age_end := 0.01917808]
  test[agg_age_end == 0, age_end := 0]
  
  ## Expand for sex
  test[, sex_split_id := paste0(split.id, "_", age_start)]
  expanded <- rep(test[,sex_split_id], test[,n.sex]) %>% data.table("sex_split_id" = .)
  test <- merge(expanded, test, by="sex_split_id", all=T)

  if (nrow(test[agg_sex_id ==3]) > 0) {
    test <- test[agg_sex_id==3, sex_id := 1:.N, by=sex_split_id]
  }
  test$sex_id <- as.double(test$sex_id)
  test[is.na(sex_id), sex_id:=agg_sex_id]
  
  # hard-code sex restrictions for Turner (437 - should only be females) and 
  # Kleinfelter (438 - should only be males)
  if (bun_id == 437) {
    test[sex_id == 1, cases := 0]
  }
  if (bun_id == 438) {
    test[sex_id == 2, cases := 0]
  }
  
  test
}
####################################################################################

####################################################################################
add_pops <- function(expanded){
  
  #pull populations for every year and location of data that need to be split
  pops <- get_population(year_id = unique(expanded$est_year_id), sex_id = c(1,2), 
                         location_id = unique(expanded$location_id), 
                         age_group_id = unique(age_map$age_group_id),
                         gbd_round_id = 6, decomp_step = 'step1')
  pops$run_id <- NULL
  setnames(pops, 'year_id', 'est_year_id')
  
  #round age_start and age_end for neonatal age groups, then add age_start and age_end to pops
  age_map[age_group_id %in% c(2:4), `:=` (age_start = round(age_start, 3),
                                          age_end = round(age_end, 3))]
  
  #add age end and age start to pops
  pops <- merge(pops, age_map, by = 'age_group_id')
  
  #round age_start and age_end for neonatal in the aggregate dataset, in preparation for the merge
  #(when there are too many decimal places, the merge can't handle it)
  #merge the populations onto the aggregate dataset
  expanded[age_end > 0 & age_end < 1, `:=` (age_start = round(age_start, 3),
                                            age_end = round(age_end, 3))]
  expanded <- merge(expanded, pops, by = c("age_start", "age_end", "sex_id", "location_id", "est_year_id"),
                    all.x = TRUE)
  
  #' Create an expand ID for tracking the draws. 1000 draws for each expand ID
  expanded[,expand.id := 1:.N]
  
  # Split ID is related to each orignial aggregated test data point. So add up the population of each individual
  #' group within each split ID. That is the total population of the aggregated age/sex group
  expanded[, pop.sum := sum(population), by = split.id]
  
  return(expanded)
  
  print("Expanding done")
}
####################################################################################

####################################################################################

pull_model_weights <- function(model_id, measure_name){
  
  measure_id <- ifelse(measure_name == "prevalence", 5, 6)
  if (measure_name == 'mtexcess') {measure_id = 9}
  if (measure_name == 'proportion') {measure_id = 18}
  wt_data <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = model_id, source = 'epi',
                       measure_id = measure_id, 
                       location_id = unique(expanded$location_id),
                       sex_id = unique(expanded$sex_id), year_id = unique(expanded$est_year_id),
                       gbd_round_id = 6, decomp_step = "step1")
  
  if (nrow(wt_data)==0) { 
    print(paste("ME ID",model_id,"does not have DisMod results for",measure_name))
    stop(paste("ME ID",model_id,"does not have DisMod results for",measure_name))
  }
  
  wt_data[, c("measure_id", "metric_id", 'model_version_id', 'modelable_entity_id') := NULL]
  
  setnames(wt_data, 'year_id', 'est_year_id')
}
####################################################################################

####################################################################################
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
  dt[(is.na(standard_error) | standard_error == 0) & is.numeric(lower) & is.numeric(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[(is.na(standard_error) | standard_error == 0) & (measure == "prevalence" | measure == "proportion"),
     standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

get_upper_lower <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[!is.na(mean) & !is.na(standard_error) & is.na(lower), lower := mean - (1.96 * standard_error)]
  dt[!is.na(mean) & !is.na(standard_error) & is.na(upper), upper := mean + (1.96 * standard_error)]
  return(dt)
}
####################################################################################

####################################################################################
pull_bundle_data <- function(measure_name, bun_id, bun_data){
  bun_data <- bun_data[measure == measure_name]
  
  #Drop data from nid 135199 (extracted into congenital bundle in error)
  bun_data <- bun_data[!(nid == 135199 & sex == "Both")]
  
  #Change Saudi Arabia subnationals to national level
  bun_data <- bun_data[location_id %in% c(44551, 44547, 44553, 44541, 44549, 44550, 44545, 44546, 44552, 44548, 44542, 44543, 44544), location_id := 152]
  
  ### Fill in any blank values in mean, sample size, or cases, based on the available values
  bun_data[!is.numeric(standard_error), standard_error := as.numeric(standard_error)]
  bun_data[!is.numeric(upper), upper := as.numeric(upper)]
  bun_data[!is.numeric(lower), lower := as.numeric(lower)]
  
  bun_data <- get_cases_sample_size(raw_dt = bun_data)
  bun_data <- get_se(raw_dt = bun_data)
  bun_data <- calculate_cases_fromse(raw_dt = bun_data)
  bun_data <- get_upper_lower(raw_dt = bun_data)
}

####################################################################################

####################################################################################
# Use this if you want to pool the data into five year bins
####################################################################################
pool_across_years <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]
  dt[year_id %in% c(1985:1992), year_pool := 1990]
  dt[year_id %in% c(1993:1997), year_pool := 1995]
  dt[year_id %in% c(1998:2002), year_pool := 2000]
  dt[year_id %in% c(2003:2007), year_pool := 2005]
  dt[year_id %in% c(2008:2012), year_pool := 2010]      
  dt[year_id %in% c(2013:2017), year_pool := 2015]
  dt[year_id %in% c(2018:2019), year_pool := 2017]      
  
  #duplicate any rows that are in 2015-2017, and also put them in the 2015-2019 age group
  overlap <- copy(dt[year_id %in% c(2015:2017)])
  overlap[, year_pool := 2017]
  dt <- rbind(dt, overlap)
  dt <- dt[year_id >= 1985]
  
  #sum cases and sample size within those year groups
  key_cols <- c('nid', 'location_id', 'age_start', 'age_end', 'age_demographer', 'sex', 
                'year_pool', 'site_memo', 'cv_livestill', 'cv_excludes_chromos',
                'cv_worldatlas_to_total','cv_icbdms_to_total','cv_nbdpn_to_total','cv_congmalf_to_total')
  dt[, `:=` (pooled_cases = sum(cases), 
             pooled_sample_size = sum(sample_size)), by = key_cols]
  count <- dt[,.N, by = key_cols]
  count[, pool.id := 1:.N]
  dt <- merge(dt, count, by = key_cols)
  dt <- unique(dt, by = 'pool.id')
  
  #relabel the cases, sample_size, mean and year columns
  dt$cases <- NULL
  dt$sample_size <- NULL
  setnames(dt, c('pooled_cases','pooled_sample_size','year_pool'), c('cases','sample_size','est_year_id'))
  dt[, `:=` (year_start = est_year_id, year_end = est_year_id)]
  dt[, mean := cases / sample_size]
  
  #recalculate upper, lower, standard error from new mean
  dt[, effective_sample_size := NA]
  dt <- get_se(raw_dt = dt)
  dt <- get_upper_lower(raw_dt = dt)
  
  dt
}
####################################################################################