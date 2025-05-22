#############################################################################################################
### Project: Neonatal Sepsis and Other Neonatal Infections
### Purpose: Age-split data using DisMod Age trend
#############################################################################################################

age_split <- function(df){

  source(paste0(h, "FILEPATH/functions_agesex_split.R"))
  source(paste0(h, "FILEPATH/agesex_split_diagnostics.R"))
  #-----------------------------------------------------------------------------------
  ### constants
  save_dir <- "FILEPATH"
  map <- fread("FILEPATH/all_me_bundle.csv")
  
  bun_id <- 92
  me_id <- map[bundle_id == 92, me_id]
  measure_name <- "incidence"
  
  age_map <- get_age_metadata(19, gbd_round_id=7)[,.(age_group_id,age_group_years_start,age_group_years_end)]
  setnames(age_map,c("age_group_years_start","age_group_years_end"),c("age_start","age_end"))
  age_map[age_end==1, age_end := 0.999]
  
  bundle_data_output_filepath <- paste0("FILEPATH/92_testing.xlsx")

  #-----------------------------------------------------------------------------------
  #subset data into an aggregate dataset and a fully-specified dataset (and remove non-prev data)
  #-----------------------------------------------------------------------------------
  df[,sex_id:=ifelse(sex=="Male",1,2)]
  expanded <- expand_test_data(agg.test = df)
  good_data <- expanded[need_split == 0]
  expanded <- fsetdiff(expanded, good_data, all = TRUE)
  
  if (nrow(expanded)==0) { 
    print(paste("Bundle",bun_id,"does not contain any data that needs to be split"))
    stop(paste("Bundle",bun_id,"does not contain any data that needs to be split"))
  }
  
  #-----------------------------------------------------------------------------------
  #merge populations and age group ids onto the expanded dataset
  #-----------------------------------------------------------------------------------
  expanded[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]
  
  #label each row with the closest dismod estimation year for matching to dismod model results
  #round down
  expanded[year_id%%5 < 3, est_year_id := year_id - year_id%%5]
  #round up
  expanded[year_id%%5 >= 3, est_year_id := year_id - year_id%%5 + 5]
  
  expanded[est_year_id < 1990, est_year_id := 1990]
  expanded[year_id == 2018 | year_id == 2019, est_year_id := 2019]
  expanded[year_id > 2019, est_year_id := year_id]
  
  expanded <- add_pops(expanded)
  print("Loaded populations")
  print(paste0('Number of rows in dataset where pop is NA: ',nrow(expanded[is.na(population)])))
  
   
 
  #-----------------------------------------------------------------------------------
  # Pull model results from DisMod to use as age/sex weights
  #-----------------------------------------------------------------------------------
  
  #' logit transform the original data mean and se
  expanded$mean_log <- log(expanded$mean)
  expanded$standard_error_log <- sapply(1:nrow(expanded), function(i) {
    mean_i <- as.numeric(expanded[i, "mean"])
    se_i <- as.numeric(expanded[i, "standard_error"])
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  expanded[is.infinite(mean_log), `:=` (mean_log = 0, standard_error_log = 0)]
  
  
  #' SAMPLE FROM THE WEIGHTS:
  #' Pull draw data for each age-sex-yr for every location in the current aggregated test data 
  #' needed to be split.
  weight_draws <- pull_model_weights(me_id, "incidence", expanded)
  print("Pulled DisMod results")
  print(paste0('Number of rows in dataset where draw_0 is NA: ',nrow(weight_draws[is.na(draw_0)])))
  
  #' Append draws to the aggregated dataset
  draws <- merge(expanded, weight_draws, by = c("age_group_id", "sex_id", "location_id", "est_year_id"), 
                 all.x=TRUE)
  
  nrow(draws[is.na(draw_0)])
  #' Take all the columns labeled "draw" and melt into one column. 
  draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                           variable.name = "draw.id", value.name = "model.result")
  
  #' SAMPLE FROM THE RAW INPUT DATA: 
  #' Save a dataset of 1000 rows per every input data point that needs to be split.
  #' Keep columns for mean and standard error, so that you now have a dataset with 
  #' 1000 identical copies of the mean and standard error of each input data point.
  orig.data.to.split <- unique(draws[, .(split.id, draw.id, mean_log, standard_error_log)])
  
  #' Generate 1000 draws from the input data (assuming a normal distribution), and replace the identical means
  #' in orig.data.to.split with draws of that mean
  # split the data table into a list, where each data point to be split is now its own data table 
  orig.data.to.split <- split(orig.data.to.split, by = "split.id")
  
  # then apply this function to each of those data tables in that list
  orig.data.to.split <- lapply(orig.data.to.split, function(input_i){
    
    mean.vector <- input_i$mean_log
    se.vector <- input_i$standard_error_log
    #Generate a random draw for each mean and se 
    set.seed(123)
    input.draws <- rnorm(length(mean.vector), mean.vector, as.numeric(se.vector))
    input_i[, input.draw := input.draws]
    
    return(input_i)
    
  })
  
  orig.data.to.split <- rbindlist(orig.data.to.split)

  draws <- merge(draws, orig.data.to.split[,.(split.id, draw.id, input.draw)], by = c('split.id','draw.id'))
  
  ####################################################################################
  
  #' This is the numerator of the equation, the total number of cases for a specific age-sex-loc-yr
  #' based on the modeled prevalence
  draws[, numerator := model.result * population]
  
  #' This is the denominator, the sum of all the numerators by both draw and split ID. The number of cases in the aggregated age/sex 
  #' group.
  draws[, denominator := sum(numerator), by = .(split.id, draw.id)]
  
  #' Calculate the weight for a specific age/sex group as: model prevalence of specific age/sex group (logit_split) divided by 
  #' model prevalence of aggregate age/sex group (logit_aggregate)
  draws[, log_split := log(model.result)]
  draws[, log_aggregate := log((denominator/pop.sum))]
  draws[, log_weight := log_split - log_aggregate]
  
  #' Apply the weight to the original mean (in draw space and in logit space)
  draws[, log_estimate := input.draw + log_weight]
  draws[, estimate := log_estimate]
  
  #' If the original mean is 0, or the modeled prevalance is 0, set the estimate draw to 0
  draws[is.infinite(log_weight) | is.nan(log_weight), estimate := 0]
  
  draws[, sample_size_new := sample_size * population / pop.sum]
  
  #' Save weight and input.draws in linear space to use in numeric check (recalculation of original data point)
  draws <- draws[, weight := model.result / (denominator/pop.sum)]
  
  # Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
  #' Each expand ID has 1000 draws associated with it, so just take summary statistics by expand ID
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
                     mean.log.weight = mean(log_weight)), by = expand.id] %>% merge(expanded, by = "expand.id")
  
  #' Convert mean and SE back to linear space 
  final$sd.est <- sapply(1:nrow(final), function(i) {
    mean_i <- as.numeric(final[i, "mean.est"])
    mean_se_i <- as.numeric(final[i, "sd.est"])
    deltamethod(~exp(x1), mean_i, mean_se_i^2)
  })
  final[, mean.est := exp(mean.est)]
  final[, lwr.est := exp(lwr.est)]
  final[, upr.est := exp(upr.est)]
  final[, mean.input.draws := exp(mean.input.draws)]
  
  final[mean == 0, mean.est := 0]
  final[mean == 0, lwr.est := 0]
  final[mean == 0, mean.input.draws := NA]
  
  final[mean == 1, mean.est := 1]
  final[mean == 1, upr.est := 1]
  final[mean == 1, mean.input.draws := NA]
  
  z <- qnorm(0.975)
  final[(mean.est == 0 | mean.est == 1) & (measure == "prevalence" | measure == "proportion"),
        sd.est := (1/(1+z^2/sample_size_new)) * sqrt(mean.est*(1-mean.est)/sample_size_new + z^2/(4*sample_size_new^2))]
  final[(mean.est == 0 | mean.est == 1) & measure == "incidence", 
        sd.est := ((5-mean.est*sample_size_new)/sample_size_new+mean.est*sample_size_new*sqrt(5/sample_size_new^2))/5]
  final[mean.est == 0, upr.est := mean.est + 1.96 * sd.est]
  final[mean.est == 1, lwr.est := mean.est - 1.96 * sd.est]
  
    if (nrow(final[orig.cases == 0]) > 0) { 
    print(paste0('No model results for any of the age/sex groups you want to split the following seqs into: ',unique(final[orig.cases == 0, seq]),
                 '. Please double-check the model you are using to inform the split, and confirm it contains results for all the age and sex groups you need.'))
  }
  
  final[, se.est := sd.est]
  final[, orig.sample.size := sample_size]
  final[, sample_size := sample_size_new]
  final[, sample_size_new:=NULL]
  
  #' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
  final[, case_weight := cases.est / orig.cases]
  final$orig.cases <- NULL
  final$standard_error <- NULL
  setnames(final, c("mean", "cases"), c("orig.mean", "orig.cases"))
  setnames(final, c("mean.est", "se.est"), c("mean", "standard_error"))
  setnames(final, 'seq','crosswalk_parent_seq')
  final[, seq := ''] 
  
  final[, sex := ifelse(sex_id == 1, "Male", "Female")]
  final[, `:=` (lower = lwr.est, upper = upr.est,
                cases = mean * sample_size, effective_sample_size = NA)]
  final$cases.est <- NULL
  final[is.nan(case_weight), `:=` (mean = NaN, cases = NaN)]
  
  setnames(final, c('agg_age_start','agg_age_end','agg_sex_id'),
           c('orig_age_start', 'orig_age_end', 'orig_sex_id'))
  
  #recalculate the original mean as a check
  final[, reagg_mean := sum(cases) / sum(sample_size), by = .(split.id)]
  final[, mean_diff_percent := (abs(reagg_mean - orig.mean)/orig.mean) * 100]
  final[reagg_mean == 0 & orig.mean == 0, mean_diff_percent := 0]
  
  #-----------------------------------------------------------------------------------
  #' Save off just the split data for troubleshooting/diagnostics
  #-----------------------------------------------------------------------------------
  split_data <- final[, c('split.id', 'nid','crosswalk_parent_seq','age_start','age_end','sex_id','mean',
                          'standard_error','cases','sample_size',
                          'orig_age_start','orig_age_end','orig_sex_id', 'orig.mean',
                          'reagg_mean', 'mean_diff_percent','mean.input.draws',
                          'orig.standard_error','orig.cases','orig.sample.size',
                          'population','pop.sum',
                          'age_group_id','age_demographer','n.age','n.sex',
                          'location_id','est_year_id', 'year_start','year_end')]
  split_data <- split_data[order(split.id)]
  
  write.xlsx(split_data, 
             file = paste0(save_dir, bun_id,'_',measure_name,'_split_only.xlsx'),
             sheetName = 'extraction',
             showNA = FALSE)
  
  age_summary <- split_data[n.age > 1, .N, by = .(orig_age_start, orig_age_end, age_demographer, n.age, age_start, age_end)]
  age_summary <- age_summary[order(orig_age_start)]
  write.xlsx(age_summary, 
             file = paste0(save_dir, bun_id,'_',measure_name,'_summary_age_ranges_post_split.xlsx'),
             sheetName = 'extraction',
             showNA = FALSE)
  
  #-----------------------------------------------------------------------------------
  #' Append split data back onto fully-specified data and save the fully split bundle
  #-----------------------------------------------------------------------------------
  good_data <- good_data[,names(df),with = FALSE]
  good_data[, crosswalk_parent_seq := seq]
  good_data[, seq := '']
  
  final <- final[,names(good_data),with = FALSE]
  
  full_bundle <- rbind(good_data, final)

  write.xlsx(full_bundle, 
             file = paste0(save_dir, bun_id,'_',measure_name,'_full.xlsx'),
             sheetName = 'extraction',
             showNA = FALSE)

  return(full_bundle)
  
}