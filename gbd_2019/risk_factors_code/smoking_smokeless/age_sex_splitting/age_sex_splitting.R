# Functions for the age-sex split.

library(data.table);  library(ggplot2); library(tidyr);
library(gridExtra); library(broom); library(magrittr); library(parallel)

############### FUNCTIONS FOR CREATING TRAIN AND TEST DATA #####################################################

divide_data <- function(xwalked, round.ages = T){
  #' @description Divides the full dataset into "train" and "test" sets. The train set
  #' consists of all rows which are in proper age and sex groups. The test is all others (both aggregate and small age groups).
  #' Also creates a column for the number of smokers in that sample
  #' @param xwalked data.table. Post-xwalked data.All input data.
  #' @param round.ages bool. If true age_start and age_end columns should be rounded to their nearest GBD age groups, if False leave as is.
  #' @return a list of two elements, the first is the training data, the second is the test data as described above
  
  data <- copy(xwalked)
  
  # If data has age start below 10 but age end above 10, then we want to keep this data and assume it is 10-14
  data <- data[age_start <10 & age_end >= 10, age_start := 10]
  #data <- data[age_end==9, age_end := 10]
  #' Only keep above 10 year olds in data. Drop all children
  data<- data[age_end >=10]
  if(round.ages == T){
    data[, `:=`(age_start = age_start - age_start %% 5,
                age_end = age_end - age_end %% 5 + 4)]
    
    data <- data[age_start > 80, age_start := 80]
    data <- data[age_end > 80, age_end := 84]
  }
  
  train <- data[(age_start %% 5 == 0) & (age_end - age_start) == 4 & (sex_id != 3), ]
  test <- fsetdiff(data, train)
  
  list(train = train, test = test)
}

save_stgpr_train <- function(data, save.path){
  #' @description Saves the training data for the Age sex splitting into a specified path to be used for ST-GPR split.
  col.stgpr <- c("location_id",
                 "nid",
                 "year_id",
                 "age_group_id",
                 "sex_id",
                 "data",
                 "variance",
                 "sample_size",
                 "split",
                 "xwalk",
                 "offset",
                 "me_name",
                 "location_name",
                 "ihme_loc_id")
  train.prep <- merge(data, ages, by = c("age_start", "age_end"), all.x = T) %>%
    merge(locs[,.(location_id, location_name, ihme_loc_id)], by = "location_id", all.x = T) %>%
    setnames("mean", "data")
  
  train.prep[,`:=`(split = 0,
                   offset = ifelse(data < .001 | data > 0.99, 1, 0))]
  
  train.in.stgpr <- train.prep[,col.stgpr,with = F]
  
  message(paste0("Writing training set "))
  write_csv(train.in.stgpr, save.path)
  message(paste0("Done Writing training set "))
}

################ FUNCTIONS FOR STGPR METHOD OF SPLITTING ################################################################
get_stgpr_draws <- function(run_id, location_ids=0){
  #' @description Reads in draws for an ST-GPR run. Can specify location. 
  #' @param run_id int. The STGPR run_id that you want to get draws from
  #' @param location_ids numeric. A numeric vector
  #' @return a data.table of all N number of draws from the STGPR output
  
  path <- FILEPATH
  if(location_ids==0){files <- list.files(path)}else{files <- paste0(location_ids, ".csv")}
  
  read_draw <- function(file){
    message(paste("Reading", file))
    data <- fread(file)
    message(paste("Done Reading", file))
    data
  }
  
  #' Internal parallelization to read draws from each csv (csvs divided up by location)
  mclapply(paste0(path, files), read_draw, mc.cores = 40) %>% rbindlist
}

expand_test_data <- function(agg.test){
  #' @description Expands the number of rows in the test/aggregated age group dataset to appropriate number of
  #' GBD age and sex groups within the range
  #' @param agg.test data.table. The test data from the output of divide_data
  #' @return A data.table with additional rows. One row in the old test data will lead to 'n' number of rows in new test data
  #' where 'n' is the number of GBD age*sex groups within the test data interval
  test <- copy(agg.test)
  setnames(test, c("age_start", "age_end", "sex_id"), c("agg_age_start", "agg_age_end", "agg_sex_id"))
  test[, `:=`(split.id = 1:.N,
              n.age = (agg_age_end + 1 - agg_age_start)/5,
              n.sex = ifelse(agg_sex_id==3, 2, 1))]
  
  ## Expand for age
  expanded <- rep(test[,split.id], test[,n.age]) %>% data.table("split.id" = .)
  test <- merge(expanded, test, by="split.id", all=T)
  test[, age.rep := (1:.N) - 1, by=split.id]
  test[, age_start := agg_age_start + age.rep * 5]
  test[, age_end := age_start + 4]
  
  ## Expand for sex
  test[, sex_split_id := paste0(split.id, "_", age_start)]
  expanded <- rep(test[,sex_split_id], test[,n.sex]) %>% data.table("sex_split_id" = .)
  test <- merge(expanded, test, by="sex_split_id", all=T)
  test <- test[agg_sex_id==3, sex_id := 1:.N, by=sex_split_id][is.na(sex_id), sex_id:=agg_sex_id]
  
  test
}

split_points <- function(agg.test, run_id, p.sample = "reflect"){
  #' @description Takes in aggregated age/sex data and splits them apart thorugh split equation, split estimate
  #' for all the age_sex groups within each original aggregated data point. Requires run id to pull data from the STGPR model that was used
  #' to calculate the "true prevalence" estimates for the equation.
  #' @param agg.test data.table. Data with aggregated age and sex groups
  #' @param run_id int. The run_id for STGPR for the model to be used to estimate the true prevalence
  #' @param p.sample char. Either "reflect" or "zero." If reflect, any draw from the sample data point that is negative will be multiplied by -1.
  #'                       If "zero," any draw from the sample data point that is negative will be converted to 0.
  
  #' BELOW is description of each data set that is used and transformed
  #' agg.test - data with aggregated age groups. These points need to be split
  #' expanded - data that is the agg.test, but expanded age and sex groups. Each original row from agg.test produces however many age/sex groups are in the original
  #' draws - data that has 100 draws from STGPR for each row in expanded, or each proper age/sex group originally in agg.test
  
  #' Expand ID identifies each individual proper age-sex group that requires an estimate. 100 draws for each.
  #' Split ID identifies each aggregated age group that must be split up into proper age-sex groups
  
  #' Adds a population and age_group_id columns
  expanded <- (expand_test_data(agg.test) %>%
                 merge(pops, by = c("age_start", "age_end", "sex_id", "location_id", "year_id")))[,run_id:=NULL]
  
  #' Pulls draw data for each age-sex-yr for every location in the current aggregated test data needed to be split
  st.draw <- get_stgpr_draws(run_id, expanded[,unique(location_id)])[,measure_id := NULL]
  
  #' Expand ID for tracking the draws. 100 draws for each expand ID
  expanded[,expand.id := 1:.N]
  # Split ID is related to each orignial aggregated test data point. So add up the population of each individual
  #' group within each split ID. That is the total population of the aggregated age/sex group
  expanded[, pop.sum := sum(population), by = split.id]
  draws <- merge(expanded, st.draw, by = c("age_group_id", "sex_id", "location_id", "year_id"))
  
  # Take all the columns labeled "draw" and melt into one column, row from expanded now has 100 rows with same data with a unique draw. Draw ID for the equation
  draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                           variable.name = "draw.id", value.name = "pi.value")
  
  ########### SAMPLING FROM THE INPUT DATA POINT #########################
  p <- draws[,mean]
  se <- draws[,variance] %>% sqrt
  
  # This will capture uncertainty within the input data, need to figure out how to sample from it. Currently multiplying all negative values by -1
  set.seed(123)
  sample.draws <- rnorm(1:nrow(draws), p, se)
  sample.draws[sample.draws < 0] <- ifelse(p.sample == "reflect", sample.draws[sample.draws < 0]*-1, 0);
  
  sample.draws[sample.draws > 1] <- 1;
  
  #' Now each row of draws has a random draw from the distribution N(mean, SE)
  draws[,sample.draws := sample.draws]
  
  #' This is the numerator of the equation, the total number of smokers for a specific age-sex-loc-yr. Estimated prevalence of age/sex group multiplied by population
  draws[, numerator := pi.value * population]
  #' This is the denominator, the sum of all the numerators by both draw and split ID. The number of smokers in the aggregated age/sex group
  #' The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point
  draws[, denominator := sum(numerator), by = .(split.id, draw.id)]
  #' Calculating the actual estimate of the split point from an individual draw of both the input data and the age pattern
  draws[, estimate := sample.draws * pi.value / denominator * pop.sum]
  draws[, sample_size_new := sample_size * population / pop.sum]
  # Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 100 draws of each calculated split point
  #' Each expand ID has 100 draws associated with it, so just take summary statistics by expand ID
  final <- draws[, .(mean.est = mean(estimate),
                     var.est = var(estimate),
                     upr.est = quantile(estimate, .975),
                     lwr.est = quantile(estimate, .025),
                     sample_size_new = unique(sample_size_new)), by = expand.id] %>% merge(expanded, by = "expand.id")
  final[,sample_size := sample_size_new]
  final[,sample_size_new:=NULL]
  #' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
  final[mean==0, mean.est := 0]
  setnames(final, c("mean", "variance"), c("agg.mean", "agg.variance"))
  setnames(final, c("mean.est", "var.est"), c("mean", "variance"))
  final
}

