
####################################################################################
expand_test_data <- function(agg.test){
  #' @description Expands the number of rows in the test/aggregated age group dataset to appropriate number of
  #' GBD age and sex groups
  #' @param agg.test data.table. The test data from the output of divide_data
  #' @return A data.table with additional rows. One row in the old test data will lead to 'n' number of rows in new test data
  #' where 'n' is the number of GBD age*sex groups within the test data interval
  test <- copy(agg.test)
  
  test[sex == "Both", sex_id := 3]
  test[sex == "Male", sex_id := 1]
  test[sex == "Female", sex_id := 2]
  setnames(test, c("sex_id"), c("agg_sex_id"))
  
  # assign a unique split.id for every original data point that needs to be split, and 
  # code if each data point represents two sexes or one
  test[, `:=`(split.id = .I,
              n.sex = ifelse(agg_sex_id==3, 2, 1))]
  
  # subset out birth prevalence (age_start and age_end = 0) because it does not need age expansion,
  # it only needs sex expansion
  birth_prev <- test[age_start == 0 & age_end == 0]
  birth_prev[, `:=` (agg_age_start = age_start, 
                     agg_age_end = age_end, 
                     n.age = 1,
                     need_split = 0,
                     age_group_id = 164)]
  test <- test[age_end != 0]
  
  
  #### EXPAND FOR AGE ####
  test[, orig_age_end := age_end]
  test[age_demographer == 0 & age_end >= 1, age_end := age_end - 0.01]
  setnames(test, c("age_start", "age_end"), c("agg_age_start", "agg_age_end"))
  
   age_map <- fread("FILEPATH")
  age_map <- age_map[age_end != 0]
  
  # split the data table into a list, where each data point to be split is now its own data table 
  test <- split(test, by = "split.id")
  
  # then apply this function to each of those data tables in that list
  test <- lapply(test, function(t){
    
    # Find age_map's order where age_start is closest to but less than or equal to t's agg_age_start  
    order_start <- age_map[age_start == max(age_map[age_start <= t$agg_age_start, age_start]), order]
    
    # Find age_map's order where age_end is closest to but greater than t's agg_age_end  
    if (t$agg_age_end > 124) { t$agg_age_end <- 124 }
    order_end <- age_map[age_end_compute == min(age_map[age_end_compute > t$agg_age_end, age_end_compute]), order]
    t[, agg_age_end := orig_age_end]
    
    t <- cbind(t, age_map[order %in% order_start:order_end, .(age_start, age_end, age_group_id)])
    t[, n.age := length(order_start:order_end)]
    
    t[n.age == 1, `:=` (age_start = agg_age_start, age_end = agg_age_end)]
    
    t[length(order_start:order_end) == 1, need_split := 0]
    t[length(order_start:order_end) > 1, need_split := 1]
    
    return(t)
    
  })
  
  test <- rbindlist(test)
  
  
  #### EXPAND FOR SEX ####
  ## Append birth prevalence back on
  test <- rbind(test, birth_prev, fill = TRUE)
  
  test[, sex_split_id := paste0(split.id, "_", age_start)]
  
  sex_specific <- test[n.sex == 1]
  sex_specific[, sex_id := agg_sex_id]
  test <- test[n.sex == 2]
  test[, need_split := 1]
  
  expanded <- rep(test[,sex_split_id], test[,n.sex]) %>% data.table("sex_split_id" = .)
  test <- merge(expanded, test, by="sex_split_id", all=T)
  
  if (nrow(test[agg_sex_id ==3]) > 0) {
    test <- test[agg_sex_id==3, sex_id := 1:.N, by=sex_split_id]
  }
  
  test$sex_id <- as.double(test$sex_id)
  test[is.na(sex_id), sex_id:=agg_sex_id]
  
  test <- rbind(test, sex_specific, use.names = TRUE, fill = TRUE)
  
  # hard-code sex restrictions for Turner (437 - should only be females) and Kleinfelter (438 - should only be males)
  if (bun_id == 437) {
    test[sex_id == 1, cases := 0]
  }
  if (bun_id == 438) {
    test[sex_id == 2, cases := 0]
  }
  
  return(test)
}
####################################################################################

####################################################################################
add_pops <- function(expanded){
  
  #pull populations for every year and location of data that need to be split
  pops <- get_population(year_id = unique(expanded$est_year_id), sex_id = c(1,2), 
                         location_id = unique(expanded$location_id), 
                         age_group_id = unique(expanded$age_group_id),
                         gbd_round_id = 7, decomp_step = 'iterative')
  pops$run_id <- NULL
  setnames(pops, 'year_id', 'est_year_id')
  
  #round age_start and age_end for neonatal in the aggregate dataset, in preparation for the merge
  #(when there are too many decimal places, the merge can't handle it)
  #merge the populations onto the aggregate dataset
  expanded$population <- NULL ## make sure there is no population column in the original dataset that we are about to merge populations onto
  expanded <- merge(expanded, pops, by = c("age_group_id", "sex_id", "location_id", "est_year_id"),
                    all.x = TRUE)
  
  #' Create an expand ID for tracking the draws. 1000 draws for each expand ID
  expanded[,expand.id := .I]
  
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
  wt_data <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = me_id, source = 'epi',
                       measure_id = measure_id, 
                       location_id = unique(expanded$location_id),
                       sex_id = unique(expanded$sex_id), year_id = unique(expanded$est_year_id),
                       gbd_round_id = 7, decomp_step = "iterative")
  
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
  bun_data_not_prev <- bun_data[measure != measure_name]
  write.xlsx(bun_data_not_prev, file = paste0("FILEPATH"), sheetName = 'extraction')
  
  bun_data <- bun_data[measure == measure_name]
  
  #do not process outliers
  # bun_data <- bun_data[is_outlier != 1]
  
  #drop some extraction errors from China data for congenital bundles
  bun_data <- bun_data[!(nid == 135199 & sex == "Both")]
  
  #correct spelling of nbdpn covariate in all congenital bundles
  #setnames(bun_data, 'cv_ndbpn_to_total', 'cv_nbdpn_to_total')
  
  #### special cases -- change saudi subnationals to national level
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
  
  year_table <- data.table(est_year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017),
                           year_start = c(1985, 1993, 1998, 2003, 2008, 2013, 2018),
                           year_end = c(1992, 1997, 2002, 2007, 2012, 2017, 2019))
  
  dt <- dt[, c('year_start', 'year_end') := NULL]
  dt <- merge(dt, year_table, by = 'est_year_id', all.x = T)
  
  
  dt[, mean := cases / sample_size]
  
  #recalculate upper, lower, standard error from new mean
  dt[, effective_sample_size := NA]
  dt <- get_se(raw_dt = dt)
  dt <- get_upper_lower(raw_dt = dt)
  
  dt
}
####################################################################################
