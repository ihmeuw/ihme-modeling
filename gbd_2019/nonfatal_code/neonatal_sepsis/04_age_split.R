#############################################################################################################
### Project: Neonatal Sepsis and Other Neonatal Infections
### Purpose: Age-split data using DisMod Age trend
### Inputs: df: dataframe of bundle data to be age split
#############################################################################################################

age_split <- function(df){
  
  #-----------------------------------------------------------------------------------
  source("FILEPATH/get_age_metadata.R")
  ### constants
  save_dir <- "FILEPATH"
  me_id <- fread("FILEPATH/all_me_bundle.csv")[bundle_id==92,me_id]
  age_map <- get_age_metadata(12)[,.(age_group_id,age_group_years_start,age_group_years_end)]
  setnames(age_map,c("age_group_years_start","age_group_years_end"),c("age_start","age_end"))
  age_map[age_end==1, age_end := 0.999]
  
  #-----------------------------------------------------------------------------------
  #load bundle data
  #-----------------------------------------------------------------------------------
  
  bun_data <- copy(df)
  
  #-----------------------------------------------------------------------------------
  #subset data into an aggregate dataset and a fully-specified dataset
  #-----------------------------------------------------------------------------------
  data <- divide_data(input_data = bun_data)
  good_data <- data[need_split == 0 | measure=="mtexcess"]
  aggregate <- fsetdiff(data, good_data, all = TRUE)
  
  if (nrow(aggregate)==0) { 
    print(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
    stop(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
  }
  
  #-----------------------------------------------------------------------------------
  #expand the aggregate dataset into its constituent age and sexes
  #-----------------------------------------------------------------------------------
  expanded <- expand_test_data(agg.test = aggregate)
  aggregate <- NULL
  
  #-----------------------------------------------------------------------------------
  #merge populations and age group ids onto the expanded dataset
  #-----------------------------------------------------------------------------------
  if ("age_group_id" %in% names(expanded) == TRUE) {
    expanded$age_group_id <- NULL
  }
  
  expanded[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]
  
  #label each row with the closest dismod estimation year for matching to dismod model results
  #round down
  expanded[year_id%%5 < 3, est_year_id := year_id - year_id%%5]
  #round up
  expanded[year_id%%5 >= 3, est_year_id := year_id - year_id%%5 + 5]
  
  expanded[est_year_id < 1990, est_year_id := 1990]
  expanded[year_id == 2017, est_year_id := 2017]
  
  expanded <- add_pops(expanded)
  print("Loaded populations")
  print(paste0('Number of rows in dataset where pop is NA: ',nrow(expanded[is.na(population)])))
  
  #-----------------------------------------------------------------------------------
  # Pull model results from DisMod to use as age weights
  #-----------------------------------------------------------------------------------
  
  #' SAMPLE FROM THE WEIGHTS:
  #' Pull draw data for each age-sex-yr for every location in the current aggregated test data 
  #' needed to be split.
  weight_draws <- pull_model_weights(me_id, measure_name="incidence", expanded=expanded)
  print("Pulled DisMod results")
  
  #' Append draws to the aggregated dataset
  draws <- merge(expanded, weight_draws, by = c("age_group_id", "sex_id", "location_id", "est_year_id"), 
                 all.x=TRUE)
  
  #' Take all the columns labeled "draw" and melt into one column. 
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
  input.draws <- rnorm(length(mean.vector), mean.vector, se.vector)
  orig.data.to.split[, input.draw := input.draws]
  
  #p.sample <- "zero"
  #orig.data.to.split[input.draw < 0 & p.sample == "reflect", input.draw := input.draw * -1]
  #orig.data.to.split[input.draw < 0 & p.sample == "zero", input.draw := 0]
  #if (measure_name == 'prevalence') {orig.data.to.split[input.draw > 1] <- 1}
  
  #' Now each row of draws has a random draw from the distribution N(mean, SE) of the original
  #' data point in that row
  draws <- merge(draws, orig.data.to.split[,.(split.id, draw.id, input.draw)], by = c('split.id','draw.id'))
  
  ####################################################################################
  
  #' This is the numerator of the equation, the total number of cases for a specific age-sex-loc-yr
  #' based on the modeled prevalence
  draws[, numerator := model.result * population]
  
  #' This is the denominator, the sum of all the numerators by both draw and split ID. The number of cases in the aggregated age/sex 
  #' group.
  #' The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point
  draws[, denominator := sum(numerator), by = .(split.id, draw.id)]
  
  #' Calculate the actual estimate of the split point from the input data (mean) and a unique draw from the modelled 
  #' prevalence (model.result)
  draws[, estimate := input.draw * model.result / denominator * pop.sum]
  draws[, sample_size_new := sample_size * population / pop.sum]
  
  # If the numerator and denominator is zero, set the estimate to zero
  draws[numerator == 0 & denominator == 0, estimate := 0]
  
  # Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
  #' Each expand ID has 1000 draws associated with it, so just take summary statistics by expand ID
  final <- draws[, .(mean.est = mean(estimate),
                     sd.est = sd(estimate),
                     upr.est = quantile(estimate, .975),
                     lwr.est = quantile(estimate, .025),
                     sample_size_new = unique(sample_size_new),
                     cases.est = mean(numerator),
                     agg.cases = mean(denominator),
                     agg.standard_error = unique(standard_error)), by = expand.id] %>% merge(expanded, by = "expand.id")
  
  #if the standard deviation is zero, calculate the standard error using Wilson's formula instead of the standard deviation of the mean
  z <- qnorm(0.975)
  final[sd.est == 0 & measure == 'prevalence', 
        sd.est := (1/(1+z^2/sample_size_new)) * sqrt(mean.est*(1-mean.est)/sample_size_new + z^2/(4*sample_size_new^2))]
  
  final[, se.est := sd.est]
  final[, agg.sample.size := sample_size]
  final[, sample_size := sample_size_new]
  final[,sample_size_new:=NULL]
  
  #' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
  final[mean==0, mean.est := 0]
  final[, case_weight := cases.est / agg.cases]
  final$agg.cases <- NULL
  final$standard_error <- NULL
  setnames(final, c("mean", "cases"), c("agg.mean", "agg.cases"))
  setnames(final, c("mean.est", "se.est"), c("mean", "standard_error"))
  setnames(final, 'seq','crosswalk_parent_seq')
  final[, seq := ''] 
  
  #-----------------------------------------------------------------------------------
  #' Append split data back onto fully-specified data and save the fully split bundle
  #-----------------------------------------------------------------------------------
  good_data <- good_data[,names(bun_data),with = FALSE]
  good_data[, crosswalk_parent_seq := seq]
  good_data[, seq := '']
  
  final[, sex := ifelse(sex_id == 1, "Male", "Female")]
  final[, `:=` (lower = lwr.est, upper = upr.est,
                cases = mean * sample_size, effective_sample_size = NA)]
  final$cases.est <- NULL
  
  final <- final[,names(good_data),with = FALSE]
  
  full_bundle <- rbind(good_data, final)
  
  write.xlsx(full_bundle, 
             file = paste0("FILEPATH"),
             sheetName = 'extraction',
             showNA = FALSE)
  
  return(full_bundle)
  
}