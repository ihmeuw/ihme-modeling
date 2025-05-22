####Variables to import in this file,
# bundle_id -> bun_id
# save_dir
# measure
# decomp_step
# gbd_round_id

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
  bun_data <- data.table(bun_data)
  
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
    orig_age_end <- t$agg_age_end
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

add_pops <- function(expanded, gbd_round_id, decomp_step){
  
  #pull populations for every year and location of data that need to be split
  pops <- get_population(year_id = unique(expanded$est_year_id), sex_id = c(1,2), 
                         location_id = unique(expanded$location_id), 
                         age_group_id = unique(expanded$age_group_id),
                         gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  pops$run_id <- NULL
  setnames(pops, 'year_id', 'est_year_id')
  
  #round age_start and age_end for neonatal in the aggregate dataset, in preparation for the merge
  #(when there are too many decimal places, the merge can't handle it)
  #merge the populations onto the aggregate dataset
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





pull_model_weights <- function(expanded = expanded, model_id = me_id, measure_name, gbd_round_id, decomp_step){
  
  measure_id <- ifelse(measure_name == "prevalence", 5, 6)
  
  wt_data <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = model_id, source = 'epi',
                       measure_id = measure_id, 
                       location_id = unique(expanded$location_id),
                       sex_id = unique(expanded$sex_id), 
                       gbd_round_id = gbd_round_id, decomp_step = decomp_step, status = 'latest')
  
  if (nrow(wt_data)==0) { 
    print(paste("ME ID",model_id,"does not have DisMod results for",measure_name))
    stop(paste("ME ID",model_id,"does not have DisMod results for",measure_name))
  }
  
  wt_data[, c("measure_id", "metric_id", 'model_version_id', 'modelable_entity_id') := NULL]
  # wt_data2 <- interpolate(gbd_id_type = 'modelable_entity_id', gbd_id = model_id, source = 'epi',
  #                        measure_id = 5, location_id = unique(expanded$location_id),
  #                        sex_id = c(1,2), gbd_round_id = 6, decomp_step = "step1",
  #                        reporting_year_start = 1980, reporting_year_end = 2017)
  
  setnames(wt_data, 'year_id', 'est_year_id')
}




agesex_split <- function(bun_id, bun_version, gbd_round_id, decomp_step, measure_name, save_dir){
  #' @description Takes in a bundle_id and a measure, performs age_sex splitting on the 
  #' corresponding bundle, saves out a file to the 'FILEPATH'
  #' filepath
  #' @param bun_id interger. The bundle to upload data to.
  #' @param gbd_round_id interger. GBD Round to upload bundle data to. 
  #' @param decomp_step string. Decomp step to upload bundle data to.
  #' @param measure_name string. Measure to split. Set to "prevalence" for hemog bundles
  #' @param save_dir string. Path to the folder where you want the split data to go.
  #' @return a filepath to the created Excel file containing the age_sex split bundle data
  
  bun_data <- get_bundle_version(bun_version, fetch = 'new')
  
  bun_data <- pull_bundle_data(measure_name = measure_name, bun_id = bun_id, bun_data = bun_data)
  
  bun_data <- bun_data[!is.na(mean)]
  
  norway_map <- fread(input = paste0('FILEPATH'))
  setnames(norway_map, 'gbd19_loc_id', 'location_id')
  bun_data <- merge(bun_data, norway_map, by = 'location_id', all.x = TRUE)
  bun_data[!is.na(gbd20_loc_id), `:=`(cases = sum(.SD$cases),
                                      sample_size = sum(.SD$sample_size)),
           by=c('gbd20_loc_id', 'year_start', 'sex', 'age_start', 'age_end','nid')]
  #replace old norway data with new subnat rows
  norway_data <- bun_data[!is.na(gbd20_loc_id)]
  bun_data <- bun_data[is.na(gbd20_loc_id)]
  norway_data <- norway_data[!duplicated(norway_data[,c('gbd20_loc_id', 'year_start', 'sex', 'age_start', 'age_end', 'nid')]),]
  bun_data <- rbind(bun_data, norway_data)
  bun_data[!is.na(gbd20_loc_id), location_id := gbd20_loc_id]
  bun_data$gbd20_loc_id <- NULL
  
  #drop any location data where the location_id is not in the dismod location set. CHANGE IF USING ST-GPR.
  dismod_locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 7, decomp_step = 'iterative')
  bun_data <- bun_data[location_id %in% dismod_locs$location_id]
  
  df <- copy(bun_data)
  
  
  #Remove points with a very small mean, no mean, or no standard error
  mean_removes <- df[df$mean > 0 & df$mean <= 1e-10]
  se_removes <- df[df$standard_error == 0]
  
  write.csv(rbind(mean_removes, se_removes), file = paste0('FILEPATH', bun_id, '_unused.csv'), row.names = FALSE)
  
  df <- df[!df$seq %in% mean_removes$seq]
  df <- df[!df$seq %in% se_removes$seq]
  
  df <- df[!is.na(mean)]
  
  df$narrow_age <- ifelse(df$age_end - df$age_start <= 2, 1, 0)
  
  df$orig_age_start <- df$age_start
  df$orig_age_end <- df$age_end
    
  
  #-----------------------------------------------------------------------------------
  # Expand each row into its constituent GBD age and sex groups. Subset off data 
  # that is already GBD-age and sex-specific, and therefore does not need to be 
  #-----------------------------------------------------------------------------------
  expanded <- expand_test_data(agg.test = df)
  
  good_data <- data.table(expanded[need_split == 0])
  
  
  ####Write out good data for testing
  write.xlsx(good_data, 
             file = paste0(save_dir, 'FILEPATH', bun_id,'_',measure_name,'_good_data.xlsx'),
             sheetName = 'extraction',
             row.names = FALSE,
             showNA = FALSE)
  
  
  expanded <- fsetdiff(expanded, good_data, all = TRUE)
  
  if (nrow(expanded)==0) { 
    print(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
    stop(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
  }
  
  
  
  expanded[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]
  
  #' label each row with the closest dismod estimation year for matching to dismod model results
  expanded[year_id%%5 < 3, est_year_id := year_id - year_id%%5]
  #' round up
  expanded[year_id%%5 >= 3, est_year_id := year_id - year_id%%5 + 5]
  
  expanded[est_year_id < 1990, est_year_id := 1990]
  expanded[year_id == 2018, est_year_id := 2020]
  
  
  expanded <- add_pops(expanded, gbd_round_id, decomp_step)
  print("Loaded populations")
  
  
  
  
  
  
  #' logit transform the original data mean and se
  expanded$mean_logit <- log(expanded$mean / (1- expanded$mean))
  expanded$standard_error_logit <- sapply(1:nrow(expanded), function(i) {
    mean_i <- as.numeric(expanded[i, "mean"])
    se_i <- as.numeric(expanded[i, "standard_error"])
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  
  expanded[is.infinite(mean_logit), `:=` (mean_logit = 0, standard_error_logit = 0)]
  
  
  
  
  #' Pull Dismod draw data for each age-sex-yr for every location in the current aggregated test data 
  #' needed to be split.
  weight_draws <- pull_model_weights(expanded, me_id, measure_name, gbd_round_id, decomp_step)
  print("Pulled DisMod results")
  
  
  #' Append draws to the aggregated dataset
  draws <- merge(expanded, weight_draws, by = c("age_group_id", "sex_id", "location_id", "est_year_id"), 
                 all.x=TRUE)
  
  #' Take all the columns labeled "draw" and melt into one column, row from expanded now has 1000 
  #' rows with same data with a unique draw
  draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                           variable.name = "draw.id", value.name = "model.result")
  
  #' Save a dataset of 1000 rows per every input data point that needs to be split.
  #' Including mean & standard error
  orig.data.to.split <- unique(draws[, .(split.id, draw.id, mean_logit, standard_error_logit)])
  
  
  #' Generate 1000 draws from the input data (assuming a normal distribution), and replace the identical means
  #' in orig.data.to.split with draws of that mean
  set.seed(123)
  # split the data table into a list, where each data point to be split is now its own data table 
  orig.data.to.split <- split(orig.data.to.split, by = "split.id")
  
  # then apply this function to each of those data tables in that list
  orig.data.to.split <- lapply(orig.data.to.split, function(input_i){
    
    mean.vector <- input_i$mean_logit
    se.vector <- input_i$standard_error_logit
    #Generate a random draw for each mean and se 
    input.draws <- rnorm(length(mean.vector), mean.vector, as.numeric(se.vector))
    input_i[, input.draw := input.draws]
    
    # troubleshooting code
    if (input_i[1, split.id] == 14495) { write.csv(input_i, file = 'FILEPATH',
                                                   row.names = FALSE)}
    
    return(input_i)
    
  })
  
  orig.data.to.split <- rbindlist(orig.data.to.split)
  
  
  #' Now each row of draws has a random draw from the distribution N(mean, SE) of the original
  #' data point in that row
  draws <- merge(draws, orig.data.to.split[,.(split.id, draw.id, input.draw)], by = c('split.id','draw.id'))
  
  
  #' This is the numerator of the equation, the total number of cases for a specific age-sex-loc-yr
  #' based on the modeled prevalence
  draws[, numerator := model.result * population]
  
  #' This is the denominator, the sum of all the numerators by both draw and split ID. 
  #' i.e. The number of cases in the aggregated age/sex group.
  draws[, denominator := sum(numerator), by = .(split.id, draw.id)]
  
  #' Calculate the weight for a specific age/sex group as: model prevalence of specific age/sex group (logit_split) divided by 
  #' model prevalence of aggregate age/sex group (logit_aggregate)
  draws[, logit_split := log(model.result / (1- model.result))]
  draws[, logit_aggregate := log( (denominator/pop.sum) / (1 - (denominator/pop.sum)))]
  draws[, logit_weight := logit_split - logit_aggregate]
  
  #' Apply the weight to the original mean (in draw space and in logit space)
  draws[, logit_estimate := input.draw + logit_weight]
  #draws[, estimate := exp(logit_estimate) / (1+exp(logit_estimate))]
  draws[ ,estimate := logit_estimate]
  
  #' If the original mean is 0, or the modeled prevalance is 0, set the estimate draw to 0
  draws[is.infinite(logit_weight) | is.nan(logit_weight), estimate := 0]
  
  draws[, sample_size_new := sample_size * population / pop.sum]
  
  #' Save weight and input.draws in linear space to use in numeric check (recalculation of original data point)
  draws <- draws[, weight := model.result / (denominator/pop.sum)]
  #draws[, input.draw := exp(input.draw) / (1+exp(input.draw))]
  
  
  #' Output current mean model results in order to troubleshoot if there are missing values in the model being used to inform the splits
  draws_df <- draws[, .(mean.est = mean(estimate)), by = expand.id]
  write.xlsx(draws_df, 
              file = paste0('FILEPATH',bun_id,'_',measure_name,'_troubleshoot_mean_model_results.xlsx'),
              sheetName = 'extraction',
              showNA = FALSE)

  
  
  #' Collapsing the draws by the expansion ID, 
  #' by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
  final <- draws[, .(mean.est = mean(estimate),
                     sd.est = sd(estimate),
                     upr.est = quantile(estimate, .975),
                     lwr.est = quantile(estimate, .025),
                     sample_size_new = unique(sample_size_new),
                     cases.est = mean(numerator),
                     orig.cases = mean(denominator),
                     orig.standard_error = unique(standard_error),
                     mean.input.draws = mean(input.draw),
                     mean.weight = mean(weight),
                     mean.logit.weight = mean(logit_weight)), by = expand.id] %>% merge(expanded, by = "expand.id")
  
  
  final[mean != 0 & mean != 1, reaggregated.mean.check.logit := mean(mean.est - mean.logit.weight), by = .(split.id)]
  final[, reaggregated.mean.check.logit := exp(reaggregated.mean.check.logit) / (1+exp(reaggregated.mean.check.logit))]
  
  write.csv(final, file = paste0("FILEPATH", bun_id, "_", measure_name, "_final.csv"),
            row.names = FALSE)
  
  
  
  #' Convert mean and SE back to linear space -- testing if there is any difference in the SE if I convert it back to linear space
  #' after collapsing draws, rather than before
  final$sd.est <- sapply(1:nrow(final), function(i) {
    mean_i <- as.numeric(final[i, "mean.est"])
    mean_se_i <- as.numeric(final[i, "sd.est"])
    deltamethod(~exp(x1)/(1+exp(x1)), mean_i, mean_se_i^2)
  })
  final[, mean.est := exp(mean.est) / (1+exp(mean.est))]
  final[, lwr.est := exp(lwr.est) / (1+exp(lwr.est))]
  final[, upr.est := exp(upr.est) / (1+exp(upr.est))]
  final[, mean.input.draws := exp(mean.input.draws) / (1+exp(mean.input.draws))]
  
  #' If the original mean is 0, recode the mean estimate to 0, and calculate the adjusted standard error using Wilson's formula
  #' and the split sample sizes.
  final[mean == 0, mean.est := 0]
  final[mean == 0, lwr.est := 0]
  final[mean == 0, mean.input.draws := NA]
  
  final[mean == 1, mean.est := 1]
  final[mean == 1, upr.est := 1]
  final[mean == 1, mean.input.draws := NA]
  
  
  
  
  #if the standard deviation is zero, calculate the standard error using Wilson's formula instead of the standard deviation of the mean
  z <- qnorm(0.975)
  
  
  
  final[(mean.est == 0 | mean.est == 1) & (measure == "prevalence" | measure == "proportion"),
        sd.est := (1/(1+z^2/sample_size_new)) * sqrt(mean.est*(1-mean.est)/sample_size_new + z^2/(4*sample_size_new^2))]
  final[(mean.est == 0 | mean.est == 1) & measure == "incidence", 
        sd.est := ((5-mean.est*sample_size_new)/sample_size_new+mean.est*sample_size_new*sqrt(5/sample_size_new^2))/5]
  final[mean.est == 0, upr.est := mean.est + 1.96 * sd.est]
  final[mean.est == 1, lwr.est := mean.est - 1.96 * sd.est]
  
  #' Print a warning for any seqs where the model did not contain any results for all the age/sex groups that cover an original data point
  if (nrow(final[orig.cases == 0]) > 0) { 
    print(paste0('No model results for any of the age/sex groups you want to split the following seqs into: ',unique(final[orig.cases == 0, seq]),
                 '. Please double-check the model you are using to inform the split, and confirm it contains results for all the age and sex groups you need.'))
  }
  
  
  final[, se.est := sd.est]
  final[, orig.sample.size := sample_size]
  final[, sample_size := sample_size_new]
  final[,sample_size_new:=NULL]
  
  #' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
  final[, case_weight := cases.est / orig.cases]
  final$orig.cases <- NULL
  final$standard_error <- NULL
  setnames(final, c("mean", "cases"), c("orig.mean", "orig.cases"))
  setnames(final, c("mean.est", "se.est"), c("mean", "standard_error"))
  setnames(final, 'seq','crosswalk_parent_seq')
  final[, seq := ''] 
  
  
  write.csv(final, file = paste0("FILEPATH", bun_id, "_", measure_name, "_final.csv"),
            row.names = FALSE)
  
#---------------------------------------------------  
  
  #' Save off just the split data for troubleshooting/diagnostics
  split_data <- final[, c('split.id', 'nid','crosswalk_parent_seq','age_start','age_end','sex_id','mean',
                          'standard_error','case_weight','sample_size',
                          'agg_age_start','agg_age_end','agg_sex_id', 'orig.mean','mean.input.draws',
                          'reaggregated.mean.check.logit','mean.weight',
                          'orig.standard_error','orig.cases','orig.sample.size',
                          'population','pop.sum',
                          'age_group_id','age_demographer','n.age','n.sex',
                          'location_id','est_year_id', 'year_start','year_end')]
  split_data <- split_data[order(split.id)]
  
  write.xlsx(split_data, 
             file = paste0(save_dir, 'FILEPATH', bun_id,'_',measure_name,'_split_only_logit.xlsx'),
             sheetName = 'extraction',
             showNA = FALSE)
  
  
  age_summary <- split_data[n.age > 1, .N, by = .(agg_age_start, agg_age_end, age_demographer, n.age, age_start, age_end)]
  age_summary <- age_summary[order(agg_age_start)]
  write.xlsx(age_summary, 
             file = paste0('FILEPATH',bun_id,'_',measure_name,'_summary_age_ranges_post_split.xlsx'),
             sheetName = 'extraction',
             showNA = FALSE)
  
  
  print(unique(good_data$location_id))
  
  #' Append split data back onto fully-specified data and save the fully split bundle as Excel file
  
  good_data <- good_data[,names(df),with = FALSE]
  good_data[, crosswalk_parent_seq := seq]
  good_data[, seq := '']
  
  final[, sex := ifelse(sex_id == 1, "Male", "Female")]
  final[, `:=` (lower = lwr.est, upper = upr.est,
                cases = mean * sample_size, effective_sample_size = NA)]
  final$cases.est <- NULL
  final[is.nan(case_weight), `:=` (mean = NaN, cases = NaN)]
  
  
  final <- final[,names(good_data),with = FALSE]
  
  full_bundle <- rbind(good_data, final)
  
  agesex_name <- paste0(save_dir,'FILEPATH',bun_id,'_',measure_name,'.xlsx')
  
  write.xlsx(full_bundle, 
            file = paste0(save_dir,'FILEPATH',bun_id,'_',measure_name,'_final.xlsx'),
            sheetName = "extraction")
  
  return(agesex_name)
}


##########

agesex_split_linear <- function(bun_id, bun_version, gbd_round_id, decomp_step, measure_name, save_dir){
  #' @description Takes in a bundle_id and a measure, performs age_sex splitting on the 
  #' corresponding bundle, saves out a file to the '/share/mnch/hemog/non_fatal/3_agesex_split/'
  #' filepath
  #' @param bun_id interger. The bundle to upload data to.
  #' @param gbd_round_id interger. GBD Round to upload bundle data to. 
  #' @param decomp_step string. Decomp step to upload bundle data to.
  #' @param measure_name string. Measure to split. Set to "prevalence" for hemog bundles
  #' @param save_dir string. Path to the folder where you want the split data to go.
  #' @return a filepath to the created Excel file containing the age_sex split bundle data
  
  bun_data <- get_bundle_version(bun_version, fetch = 'new')
  
  bun_data <- pull_bundle_data(measure_name = measure_name, bun_id = bun_id, bun_data = bun_data)
  
  bun_data <- bun_data[!is.na(mean)]
  
  #JUST FOR GBD2020 Step 2: aggregate old norway data into new subnat groups
  norway_map <- fread(input = paste0('FILEPATH'))
  setnames(norway_map, 'gbd19_loc_id', 'location_id')
  bun_data <- merge(bun_data, norway_map, by = 'location_id', all.x = TRUE)
  bun_data[!is.na(gbd20_loc_id), `:=`(cases = sum(.SD$cases),
                                      sample_size = sum(.SD$sample_size)),
           by=c('gbd20_loc_id', 'year_start', 'sex', 'age_start', 'age_end','nid')]
  #replace old norway data with new subnat rows
  norway_data <- bun_data[!is.na(gbd20_loc_id)]
  bun_data <- bun_data[is.na(gbd20_loc_id)]
  norway_data <- norway_data[!duplicated(norway_data[,c('gbd20_loc_id', 'year_start', 'sex', 'age_start', 'age_end', 'nid')]),]
  bun_data <- rbind(bun_data, norway_data)
  bun_data[!is.na(gbd20_loc_id), location_id := gbd20_loc_id]
  bun_data$gbd20_loc_id <- NULL
  
  #drop any location data where the location_id is not in the dismod location set. CHANGE IF USING ST-GPR.
  dismod_locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 7, decomp_step = 'iterative')
  bun_data <- bun_data[location_id %in% dismod_locs$location_id]
  
  df <- copy(bun_data)
  
  
  #Remove points with a very small mean, no mean, or no standard error
  mean_removes <- df[df$mean > 0 & df$mean <= 1e-10]
  se_removes <- df[df$standard_error == 0]
  
  write.csv(rbind(mean_removes, se_removes), file = paste0('FILEPATH', bun_id, '_unused.csv'), row.names = FALSE)
  
  df <- df[!df$seq %in% mean_removes$seq]
  df <- df[!df$seq %in% se_removes$seq]
  
  df <- df[!is.na(mean)]
  
  df$narrow_age <- ifelse(df$age_end - df$age_start <= 2, 1, 0)
  
  df$orig_age_start <- df$age_start
  df$orig_age_end <- df$age_end
    
  
  #-----------------------------------------------------------------------------------
  # Expand each row into its constituent GBD age and sex groups. Subset off data 
  # that is already GBD-age and sex-specific, and therefore does not need to be 
  # split ("good_data")
  #-----------------------------------------------------------------------------------
  expanded <- expand_test_data(agg.test = df)
  
  good_data <- data.table(expanded[need_split == 0])
  
  
  ####Write out good data for testing
  write.xlsx(good_data, 
             file = paste0(save_dir, 'FILEPATH', bun_id,'_',measure_name,'_good_data.xlsx'),
             sheetName = 'extraction',
             row.names = FALSE,
             showNA = FALSE)
  
  
  expanded <- fsetdiff(expanded, good_data, all = TRUE)
  
  if (nrow(expanded)==0) { 
    print(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
    stop(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
  }
  
  
  
  expanded[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]
  
  #' label each row with the closest dismod estimation year for matching to dismod model results
  expanded[year_id%%5 < 3, est_year_id := year_id - year_id%%5]
  #' round up
  expanded[year_id%%5 >= 3, est_year_id := year_id - year_id%%5 + 5]
  
  expanded[est_year_id < 1990, est_year_id := 1990]
  expanded[year_id == 2018, est_year_id := 2020]
  
  
  expanded <- add_pops(expanded, gbd_round_id, decomp_step)
  print("Loaded populations")
  print(paste0('Number of rows in dataset where pop is NA: ',nrow(expanded[is.na(population)])))
  
  
  
  
  #' Pull Dismod draw data for each age-sex-yr for every location in the current aggregated test data 
  #' needed to be split.
  weight_draws <- pull_model_weights(expanded, me_id, measure_name, gbd_round_id, decomp_step)
  print("Pulled DisMod results")
  print(paste0('Number of rows in dataset where draw_0 is NA: ',nrow(weight_draws[is.na(draw_0)])))
  
  
  
  #' Append draws to the aggregated dataset
  draws <- merge(expanded, weight_draws, by = c("age_group_id", "sex_id", "location_id", "est_year_id"), 
                 all.x=TRUE)
  
  #' Take all the columns labeled "draw" and melt into one column, row from expanded now has 1000 
  #' rows with same data with a unique draw
  draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                           variable.name = "draw.id", value.name = "model.result")
  
  #' SAMPLE FROM THE RAW INPUT DATA: 
  #' Save a dataset of 1000 rows per every input data point that needs to be split.
  #' Keep columns for mean and standard error, so that you now have a dataset with 
  #' 1000 identical copies of the mean and standard error of each input data point.
  orig.data.to.split <- unique(draws[, .(split.id, draw.id, mean, standard_error)])
  
  #' Generate 1000 draws from the input data (assuming a normal distribution), and replace the identical means
  #' in orig.data.to.split with draws of that mean
  #' Currently setting all negative values to 0
  set.seed(123)
  mean.vector <- orig.data.to.split$mean
  se.vector <- orig.data.to.split$standard_error
  #Generate a random draw for each mean and se 
  input.draws <- rnorm(length(mean.vector), mean.vector, as.numeric(se.vector))
  orig.data.to.split[, input.draw := input.draws]
  
  #' Now each row of the dataset draws has a random draw from the distribution N(mean, SE) of the original
  #' data point in that row. The mean of these draws will not be exactly the same as the input data point 
  #' mean that it was randomly sampled from (because the sampling is random and the standard error can be
  #' large).
  draws <- merge(draws, orig.data.to.split[,.(split.id, draw.id, input.draw)], by = c('split.id','draw.id'))
  
  
  #' This is the numerator of the equation, the total number of cases for a specific age-sex-loc-yr
  #' based on the modeled prevalence
  draws[, numerator := model.result * population]
  
  #' This is the denominator, the sum of all the numerators by both draw and split ID. 
  #' i.e. The number of cases in the aggregated age/sex group.
  draws[, denominator := sum(numerator), by = .(split.id, draw.id)]
  
  #' Calculate the actual estimate of the split point from the input data (mean) and a unique draw from the modelled 
  #' prevalence (model.result)
  draws[, estimate := input.draw * model.result / denominator * pop.sum]
  draws[, sample_size_new := sample_size * population / pop.sum]
  
  draws[numerator == 0 & denominator == 0, estimate := 0]
  
  #' Save weight and input.draws in linear space to use in numeric check (recalculation of original data point)
  draws <- draws[, weight := model.result / (denominator/pop.sum)]
  #draws[, input.draw := exp(input.draw) / (1+exp(input.draw))]
  
  
  #' Output current mean model results in order to troubleshoot if there are missing values in the model being used to inform the splits
  draws_df <- draws[, .(mean.est = mean(estimate)), by = expand.id]
  write.xlsx(draws_df, 
             file = paste0('FILEPATH',bun_id,'_',measure_name,'_troubleshoot_mean_model_results.xlsx'),
             sheetName = 'extraction',
             showNA = FALSE)
  
  
  
  #' Collapsing the draws by the expansion ID, 
  #' by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
  final <- draws[, .(mean.est = mean(estimate),
                     sd.est = sd(estimate),
                     upr.est = quantile(estimate, .975),
                     lwr.est = quantile(estimate, .025),
                     sample_size_new = unique(sample_size_new),
                     cases.est = mean(numerator),
                     agg.cases = mean(denominator),
                     orig.standard_error = unique(standard_error),
                     mean.input.draws = mean(input.draw),
                     mean.weight = mean(weight)), by = expand.id] %>% merge(expanded, by = "expand.id")
  
  
  #final[mean != 0 & mean != 1, reaggregated.mean.check.logit := mean(mean.est - mean.logit.weight), by = .(split.id)]
  #final[, reaggregated.mean.check.logit := exp(reaggregated.mean.check.logit) / (1+exp(reaggregated.mean.check.logit))]
  
  write.csv(final, file = paste0("FILEPATH", bun_id, "_", measure_name, "_final.csv"),
            row.names = FALSE)
  
  
  
  #if the standard deviation is zero, calculate the standard error using Wilson's formula instead of the standard deviation of the mean
  z <- qnorm(0.975)
  final[sd.est == 0 & (measure == 'prevalence' | measure == 'proportion'), 
        sd.est := (1/(1+z^2/sample_size_new)) * sqrt(mean.est*(1-mean.est)/sample_size_new + z^2/(4*sample_size_new^2))]
  
  final[, se.est := sd.est]
  final[, orig.sample.size := sample_size]
  final[, sample_size := sample_size_new]
  final[, sample_size_new:=NULL]
  
  #' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
  final[mean==0, mean.est := 0]
  final[, case_weight := cases.est / agg.cases]
  final$agg.cases <- NULL
  final$standard_error <- NULL
  setnames(final, c("mean", "cases"), c("orig.mean", "orig.cases"))
  setnames(final, c("mean.est", "se.est"), c("mean", "standard_error"))
  setnames(final, 'seq','crosswalk_parent_seq')
  final[, seq := ''] 
  
  #drop rows that don't match the cause sex-restrictions
  if (bun_id == 437) { final <- final[sex_id == 2] }
  if (bun_id == 438) { final <- final[sex_id == 1] }
  
  final[, sex := ifelse(sex_id == 1, "Male", "Female")]
  final[, `:=` (lower = lwr.est, upper = upr.est,
                cases = mean * sample_size, effective_sample_size = NA)]
  final$cases.est <- NULL
  final[is.nan(case_weight), `:=` (mean = NaN, cases = NaN)]
  
  setnames(final, c('agg_sex_id'),
           c('orig_sex_id'))
  
  #recalculate the original mean as a check
  final[, reagg_mean := sum(cases) / sum(sample_size), by = .(split.id)]
  final[, mean_diff_percent := (abs(reagg_mean - orig.mean)/orig.mean) * 100]
  final[reagg_mean == 0 & orig.mean == 0, mean_diff_percent := 0]
  
  
  write.csv(final, file = paste0("FILEPATH", bun_id, "_", measure_name, "_final.csv"),
            row.names = FALSE)
  
  #---------------------------------------------------  
  
  #' Save off just the split data for troubleshooting/diagnostics
  split_data <- final[, c('split.id', 'nid','crosswalk_parent_seq','age_start','age_end','sex_id','mean',
                          'standard_error','case_weight','sample_size',
                          'orig_age_start','orig_age_end','orig_sex_id', 'orig.mean','mean.input.draws',
                          'reagg_mean', 'mean.weight',
                          'orig.standard_error','orig.cases','orig.sample.size',
                          'population','pop.sum',
                          'age_group_id','age_demographer','n.age','n.sex',
                          'location_id','est_year_id', 'year_start','year_end')]
  split_data <- split_data[order(split.id)]
  
  write.xlsx(split_data, 
             file = paste0(save_dir, 'FILEPATH', bun_id,'_',measure_name,'_split_only_logit.xlsx'),
             sheetName = 'extraction',
             showNA = FALSE)
  
  
  
  
  print(unique(good_data$location_id))
  
  #' Append split data back onto fully-specified data and save the fully split bundle as Excel file
  
  good_data <- good_data[,names(df),with = FALSE]
  good_data[, crosswalk_parent_seq := seq]
  good_data[, seq := '']
  
  
  
  final <- final[,names(good_data),with = FALSE]
  
  full_bundle <- rbind(good_data, final)
  
  agesex_name <- paste0(save_dir,'FILEPATH',bun_id,'_',measure_name,'.xlsx')
  
  write.xlsx(full_bundle, 
             file = paste0(save_dir,'FILEPATH',bun_id,'_',measure_name,'_final.xlsx'),
             sheetName = "extraction")
  
  return(agesex_name)
}


########


create_crosswalk <- function(bundle_id, bundle_version, measure = 'prevalence', trim = 'trim', 
                             trim_percent, out_dir, match_type = "within_study", decomp_step){
  #' @description Takes in a bundle_id, a bundle version_id, .
  #' @param bundle_id interger. The bundle to upload data to.
  #' @param bundle_version interger. Bundle version id.
  #' @param measure string. Measure of bundle data to crosswalk. Set to prevalence.
  #' @param trim string. Trim or no_trim. 
  #' @param trim_percent decimal. 
  #' @param decomp_step string. Decomp step to upload bundle data to.
  #' @param gbd_round_id interger. GBD Round to upload bundle data to.
  #' @return folder with model_effects.csv, which contains the 
  
  ##Setting up directory to put mr-brt results/plots
  
  cov_df <- read.csv('FILEPATH')
  cv <- as.character(cov_df$covariate[cov_df$bundle_id == bundle_id])
  
  if (trim_percent == 0) {
    mod_lab <- paste0(bundle, "_", cv, "_", match_type, '_', trim, '_', decomp_step)
  } else {
    mod_lab <- paste0(bundle, "_", cv, "_", match_type, '_', trim, '_', trim_percent, '_', decomp_step)
  }
  
  
  #Pulling down bundle data
  df <- get_bundle_version(bundle_version_id = bundle_version, fetch = 'new', transform = TRUE)
  
  df <- df[,unique(names(df)),with=FALSE]
  
  #Merging demographics maps into the bundle version data
  df <- merge(df, locs, by = 'location_id', all.x = TRUE)
  df <- merge(df, sex_names, by = c('sex'), all.x = TRUE)
  df <- merge(df, age_map, by = c('age_start', 'age_end'), all.x = TRUE)
  
  #Rounding ages/years, removing rows w/ no standard error
  df <- df[, age := round((age_start + age_end)/2)]
  df <- df[, year_id := round((year_start + year_end) / 2)]
  df <- df[!is.na(standard_error)]
  
  #Subsetting to relevant rows for matching
  df <- df[, c('nid', 'location_id', 'sex_id', 'age', 'age_start', 'age_end', 'year_id', 'mean', 'standard_error', 'sample_size', cv), with = F]
  
  
  ####################################################################################
  # match data
  ##########################################
  
  match_split <- split(df, df[, c(cv), with = FALSE])
  
  if(length(match_split) == 2){
    
    if(match_type == 'within_study'){
      matched_merge <- merge(match_split[['1']], match_split[['0']],  by = c('nid', 'location_id', 'age_start', 'age', 'age_end', 'sex_id', 'year_id'), all.x = TRUE, allow.cartesian=TRUE)
    } 
    if(match_type == 'between_study'){
      matched_merge <- merge(match_split[['1']], match_split[['0']],  by = c('location_id', 'age_start', 'age_end', 'sex_id', 'year_id'), all.x = TRUE, allow.cartesian=TRUE)
    }
    
    #drop rows where that loc/age/sex/year was only in the first dataset
    matched_merge <- matched_merge[!is.na(mean.y)]
    
    #drop rows where both means are 0
    matched_merge <- matched_merge[!(mean.x == 0 & mean.y == 0)]
    
    #add offset where both values are zero in order to be able to calculate std error
    offset <- 0.5 * median(all_split[mean != 0, mean])
    matched_merge[(mean.x == 0 | mean.y == 0), `:=` (mean.x = mean.x + offset,
                                                     mean.y = mean.y + offset)]
    
    print(paste0('# rows where livestill is nonzero but live is zero: ',nrow(matched_merge[mean.y > 0 & mean.x == 0])))
    
    matched_merge <- matched_merge[!mean.x==0] # uncomment me if you want to get rid of ALTERNATE means that are zero
    matched_merge <- matched_merge[!mean.y==0] # uncomment me if you want to get rid of REFERENCE means that are zero
    
    matched_merge <- matched_merge[, mean:= mean.x / mean.y]
    matched_merge <- matched_merge[, standard_error:= sqrt( (mean.x^2 / mean.y^2) * ( (standard_error.x^2 / mean.x^2) + (standard_error.y^2 / mean.y^2) ) ) ]
    
    #output csv if the ratio is less than zero when looking at cv_livestill
    #output csv if the ratio is greater than zero if looking at cv_excludes_chromos
    #then drop from crosswalk
    #add here
    
    if(nrow(matched_merge) == 0){
      print(paste0('no ', match_type, ' matches between demographics for ', cv))
    } else{
      write.csv(matched_merge, paste0(out_dir, "/", mod_lab, "_inputdata.csv"), row.names = FALSE)
    }
    
  } else{
    print('only one value for given covariate')
  }
  
  
  
  #Used to remove points which may throw off mrbrt modeling
  matched_merge <- matched_merge[!matched_merge$standard_error > 6]
  
  
  ####
  dat_metareg <- as.data.frame(matched_merge)
  dat_metareg$sex_id <- ifelse(dat_metareg$sex_id == 2, 0, 1)
  
  
  #Setting mr_brt variables
  ratio_var <- "mean"
  ratio_se_var <- "standard_error"
  age_var <- "age"
  sex_var <- "sex_id"
  cov_names <- c('sex_id')
  metareg_vars <- c(ratio_var, ratio_se_var, sex_var)
  
  tmp_metareg <- as.data.frame(dat_metareg) %>%
    .[, metareg_vars] %>%
    setnames(metareg_vars, c("ratio", "ratio_se", 'sex_id'))
  
  tmp_metareg$ratio_log <- log(tmp_metareg$ratio)
  tmp_metareg$ratio_se_log <- sapply(1:nrow(tmp_metareg), function(i) {
    ratio_i <- tmp_metareg[i, "ratio"]
    ratio_se_i <- tmp_metareg[i, "ratio_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  
  
  
  #MR-BRT modeling
  
  
  if(trim == 'no_trim'){
    fit1 <- run_mr_brt(
      output_dir = out_dir, # user home directory
      model_label = mod_lab,
      data = tmp_metareg,
      mean_var = "ratio_log",
      se_var = "ratio_se_log",
      overwrite_previous = TRUE
    )
  } else if(trim == 'trim'){
    ## trim ###
    fit1 <- run_mr_brt(
      output_dir = out_dir, # user home directory
      model_label = mod_lab,
      data = tmp_metareg,
      mean_var = "ratio_log",
      se_var = "ratio_se_log",
      overwrite_previous = TRUE,
      method = "trim_maxL",
      trim_pct = trim_percent,
      covs = list(
        cov_info('sex_id', 'X')
      )
    )
  }
  
  ##########################################
  # plot mr-brt
  plot <- plot_mr_brt_custom(fit1)
  #plot <- plot_mr_brt(fit1)
  
  #Review MR-BRT results (w/covariates)
  check_for_outputs(fit1)
  results <- load_mr_brt_outputs(fit1)
  names(results)
  coefs <- results$model_coefs
  metadata <- results$input_metadata
  
  #store the mean_effect model with covariates
  df_pred <- data.frame(expand.grid(sex_id = c(1,2)))
  pred1 <- predict_mr_brt(fit1, newdata = df_pred)
  check_for_preds(pred1)
  
  pred_object <- load_mr_brt_preds(pred1)
  names(pred_object)
  preds <- pred_object$model_summaries
  draws <- pred_object$model_draws
  
  ##plot with splines
  plot <- plot_mr_brt(pred1, dose_vars = 'age')
  
  beta0 <- preds$Y_mean # predicted ratios by age  
  beta0_se_tau <-  (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92 # standard
  sex_id <- preds$X_sex_id
  
  test <- as.data.frame(cbind(beta0, beta0_se_tau, sex_id))
  colnames(test)[colnames(test)=="beta0"] <- "ratio"
  colnames(test)[colnames(test)=="beta0_se_tau"] <- "se"
  
  output_file <- write.csv(test, paste0(out_dir, mod_lab, '/', bundle_id, '_', cv, '_', measure, '_model_effects.csv'))
  
  print(paste0("Completed, model effects in: FILEPATH", mod_lab, '/', bundle_id, '_', cv, '_', measure, '_model_effects.csv'))
}





