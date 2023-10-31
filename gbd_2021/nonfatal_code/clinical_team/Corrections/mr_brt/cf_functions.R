#############################################################
##Author: USER
##Date: 3/4/2019
##Purpose: Review and plot MR-BRT models
###########################################################
user <- Sys.info()[7]
library(ggplot2)
library(data.table)
library(stringr)
library(gridExtra)
library(RMySQL)
library(tidyr)
library(boot)
source(FILEPATH)
lapply(dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)

####### Functions #######
loadBundles <- function(bundle_ids) {
  # Get bundles for which we have scalars
  db_con = fread(FILEPATH)
  
  # Get list of ids and names
  con <- dbConnect(dbDriver("MySQL"),
                   username = db_con$username,
                   password = db_con$pass,
                   host = db_con$host)
  df <- dbGetQuery(con, FILEPATH)
  df <- data.table(df)
  df <- df[is.na(sort_order), sort_order := 9999]
  df <- df[order(sort_order)][, sort_order := NULL]
  dbDisconnect(con)
  return(df)
}

###### Files and format #####
# pull out ones that broke to test out for CF3
pullestimates <- function(filename){
  info <- str_split(filename, 'by_cfbundle/mrbrt_')[[1]][2]
  bun <- as.numeric(str_split(info, '_')[[1]][1])
  cf <- str_split(info, '_|/')[[1]][2]
  estimate <- list('bun' = bun, 'cf' = cf)
  return(estimate)
}

# Returns a vector of age group ids based on what each bundle uses in clinical.bundle
get_age_groups <- function(bundle, write_folder){
  age_groups <- fread(paste0(write_folder,'age_groups.csv'))
  
  db_con = fread(FILEPATH)
  
  # Get list of ids and names
  con <- dbConnect(dbDriver("MySQL"),
                   username = db_con$username,
                   password = db_con$pass,
                   host = db_con$host)
  bundle_ages <- dbGetQuery(con, QUERY)

  dbDisconnect(con)
  bundle_ages <- merge(bundle_ages, age_groups, by='age_group_id')
  bundle_ages <- as.data.table(bundle_ages)
  return(bundle_ages)
}

find_models <- function(bundle,cfs,versions){
  cf_mods <- expand.grid(cf = cfs, vers = versions) %>% as.data.table()
  models <- lapply(split(cf_mods,by=c('cf','vers')), function(x){
    if(file.exists(FILEPATH){
    }else{
      x <- NA
    }
    return(x)
  })
  models <- models[!is.na(models)]
  models <- rbindlist(models)
  return(models)
}

# Takes folder for MR BRT output of CF model and compares outputs / step in the process that broke
cf_modelcheck <- function(data_folder){
  # Get files
  input_data_files <- Sys.glob(FILEPATH)
  train_data_files <- Sys.glob(FILEPATH)
  results_files <- Sys.glob(FILEPATH)

  input_bundles <- data.table()
  data_bundles <- data.table()
  results_bundles <- data.table()
  
  for(file in input_data_files) input_bundles <- rbind(input_bundles, pullestimates(file))
  for(file in train_data_files) data_bundles <- rbind(data_bundles, pullestimates(file))
  for(file in results_files) results_bundles <- rbind(results_bundles, pullestimates(file))
  
  b1 <- setdiff(input_bundles, data_bundles) %>%
    mutate(issue = 'input data exists but train dataset does not')
  issues <- setdiff(data_bundles, results_bundles) %>%
    mutate(issue = 'train data exists but result dataset does not') %>%
    rbind(b1)
  return(issues)
}

cf_plotprep <- function(bun, cf, data_folder, from_file = FALSE){
  restrictions <- get_icg_restrictions()
  max_map <- max(restrictions$map_version)
  bundle_map <- bundle_icg() %>% merge(restrictions[map_version == max_map], by = c('icg_id', 'icg_name')) %>% .[, c('bundle_id', 'male', 'female', 'yld_age_start', 'yld_age_end')]
  bundle_map[, min_age := min(yld_age_start), by = 'bundle_id'][, max_age := max(yld_age_end), by = 'bundle_id']
  bundle_map <- unique(bundle_map[, c('bundle_id', 'min_age', 'max_age')])
  bundle_map[max_age == 95, max_age := 110]
  
  restrictions_map <- bundle_map[bundle_id == bun]

  bun_df <- loadBundles(bun)
  bundle_name <- bun_df[bundle_id == bun]$bundle_name[1]
  print(bundle_name)
  
  if(!from_file){
    train_data <- fread(paste0(data_folder,'train_data.csv'))
    model_sum <- fread(paste0(data_folder,'model_summaries.csv'))
    if('X_sex_id' %in% names(model_sum)) setnames(model_sum, c('X_sex_id'), c('sex_id'))
    if('X_age_start' %in% names(model_sum)) setnames(model_sum,'X_age_start','age_midpoint')
    if('X_age_midpoint' %in% names(model_sum)) setnames(model_sum,'X_age_midpoint','age_midpoint')
    model_sum <- merge(model_sum, age_groups[,.(age_group_id,age_midpoint,age_start)], by='age_midpoint')
    
    if(cf != 'cf1'){
      model_sum$Y_mean <- exp(model_sum$Y_mean)
      model_sum$Y_mean_hi <- exp(model_sum$Y_mean_hi)
      model_sum$Y_mean_lo <- exp(model_sum$Y_mean_lo)
    } else{
      model_sum$Y_mean <- inv.logit(model_sum$Y_mean)
      model_sum$Y_mean_hi <- inv.logit(model_sum$Y_mean_hi)
      model_sum$Y_mean_lo <- inv.logit(model_sum$Y_mean_lo)
    }
  
  }else{
    train_data <- fread(paste0(data_folder, FILEPATH)
    age_groups <- fread(paste0(data_folder,'age_groups.csv'))
    model_sum <- fread(paste0(data_folder,FILEPATH,bun,'_',cf,'.csv'))
    model_sum <- model_sum[bundle_id == bun & cf_type == cf]
    model_sum[, age_start := NULL]
    model_sum[, age_midpoint := NULL]
    model_sum <- merge(model_sum, age_groups[,.(age_group_id,age_midpoint,age_start)], by='age_group_id')
  }
  
  if('cf_median' %in% names(model_sum)){
    model_sum[,cf_val := cf_median]
  }else{
    model_sum[,`:=`(cf_val = Y_mean, upper = Y_mean_hi, lower = Y_mean_lo)]
  }
  
  if('log_mean' %in% names(train_data)) setnames(train_data,'log_mean','cf')
  if('logit_mean' %in% names(train_data)) setnames(train_data,'logit_mean','cf')
  
  if(cf != 'cf1'){
    train_data$mean_val <- exp(train_data$cf)
  } else{
    train_data$mean_val <- inv.logit(train_data$cf)
  }
  
  train_data[w < 0.5, trimmed := 'Trimmed data'][w >= 0.5, trimmed := 'Untrimmed data']
  train_data[sex_id == 1, sex := 'Males'][sex_id == 2, sex := 'Females']
  
  if(!('age_midpoint' %in% names(train_data))) setnames(train_data,'age_start','age_midpoint')
  if('sex_id' %in% names(model_sum)){
    model_sum[sex_id == 1, sex := 'Males'][sex_id == 2, sex := 'Females']
  } else{
    model_sum[, sex := 'Females'] 
  }
  # Fixes ages 
  model_sum <- model_sum[age_start > restrictions_map$min_age[1] & age_start < restrictions_map$max_age[1] + 5]
  
  data <- list('restrictions_map'=restrictions_map, 'bundle_name'=bundle_name, 'train_data'=train_data, 'model_sum'=model_sum)
  
  return(data)
}

# Worker functions -----
# Create age covariate object with spline information to feed MR-BRT. Determines knot placement procedure and sets priors, etc. based on knot_vers input 
# and bundle input data - ex. a bundle with age restriction > 15 but manual knot placement won't have knots set at 1 and 5.
# Types of splines available: frequency w/ 3 knots, manually placed knots at age 1, 5, 20, every 30 years after, or knots placed on number of total admissions over age pattern,
# so a bundle with lots of admissions age 65+ and very few age 20-65 would have knots centered around older ages. All splines have uninformative Gaussian priors on each knot, linear
# tails & are cubic.
getknots <- function(knot_vers, input){
  n_knots <- NA
  knots_placement <- NA
  knots <- NA
  
  if(knot_vers == 'freq'){
    print('Knots placed by data frequency')
    n_knots <- 3
    knots_placement <- 'frequency'
    spline_m <- paste0(rep(0, n_knots+1), collapse = ', ')
    spline_v <- paste0(rep('inf', n_knots+1), collapse = ', ') 
  }else if(knot_vers == 'admissions'){
    a_max <- max(input$age_start)
    
    if(a_max <= 5){
      knots <- c(1,5)
    }else if(a_max < 15){
      knots <- c(1,5,a_max)
    }else{
      if(a_max == min(input[age_start >= 15]$age_start)){
        knots <- c(a_max)
      }else{
        adms <- copy(input)[, .(total_inp_pri_claims_cases = sum(total_inp_pri_claims_cases)), by = 'age_midpoint']
        setorder(adms, total_inp_pri_claims_cases)
        knots <- tail(adms,3)$age_midpoint
        knots <- sort(knots)
      }
      
      if(min(input$age_start) < 5) knots <- c(5,knots)
      if(min(input$age_start) < 1) knots <- c(1,knots)
      
      if(length(knots) == 1) knots <- c(knots,a_max)
    }
    print('Knots for spline:')
    print(knots)
    
    spline_m <- paste0(rep(0, length(knots)+1), collapse = ', ')
    spline_v <- paste0(rep('inf', length(knots)+1), collapse = ', ') 
    knots <- paste0(knots, collapse = ', ')
    
  }else if(knot_vers == 'manual'){
    a_max <- max(input$age_start)
    
    if(a_max <= 5){
      knots <- c(1,5)
    } else if(a_max < 15){
      knots <- c(1,5,a_max)
    }else{
      if(a_max == min(input[age_start >= 15]$age_start)){
        knots <- c(a_max)
      }else{
        a_min <- min(input[age_start >= 15]$age_start) + 5
        knots <- seq(a_min,a_max,30)
      }
      
      if(min(input$age_start) < 5) knots <- c(5,knots)
      if(min(input$age_start) < 1) knots <- c(1,knots)
      
      if(length(knots) == 1) knots <- c(knots,a_max)
    }
    print('Knots for spline:')
    print(knots)
    
    spline_m <- paste0(rep(0, length(knots)+1), collapse = ', ')
    spline_v <- paste0(rep('inf', length(knots)+1), collapse = ', ') 
    knots <- paste0(knots, collapse = ', ')
    
  }
  
  agecov <- cov_info('age_midpoint', 'X',
                     gprior_mean = 0, 
                     gprior_var = 'inf',
                     bspline_gprior_mean = spline_m, 
                     bspline_gprior_var = spline_v,
                     degree = 3,
                     i_knots = knots,
                     n_i_knots = n_knots,
                     r_linear = TRUE,
                     l_linear = TRUE)
  
  return(agecov)
}

### Checks for data availability by sex & age group. Only includes covariates where data exists. Includes age without a spline when limited age groups are available.
# Wrapper over MR-BRT to set covariates based on data availability, rerun model if trimming reduces age variability.
# tr: trim (expressed as decimal)
# knot_vers: user-specified age spline knot placement method - currently using 'freq' for 2 frequency-placed knots or 'manual' for knots at 1,5,20,50,80,110 (or fewer 
# based on input data / age sex restrictions)
# bundle, cf self-explanatory
mrbrtrun <- function(tr, knot_vers, bundle, cf){
  input <- fread(paste0(data_folder,bundle,'.csv'))
  print('Size of input data: ')
  print(nrow(input))
  
  # Determine method by trimming input - use remL if specified trimming is 0% (can automatically be set to 0 in super sparse bundles)
  tr <- as.numeric(tr)
  if(tr == 0){
    mthd <- 'remL'
    tr <- NULL
  }else{
    mthd <- 'trim_maxL'
    
    if((tr * nrow(input)) < 1){
      print('Trimming percentage is less than one row of data - increasing')
      tr = ceiling(100/nrow(input))/100
    }
  }
  
  # Determine sex covariate - both sexes have to have > 5 data points
  input[, num_sex_points := .N, by = 'sex_id']
  sexes <- length(unique(input$sex_id))
  if(min(input$num_sex_points) < 5) sexes <- 1
  if(sexes > 1){
    print('Sex covariate')
    sexcov <- cov_info("sex_id", "X")
  }else{
    print('No sex covariate')
    sexcov <- c()
  }
  
  # Determine age covariate
  # 1 age - no covariate. < 4 ages - no spline. Otherwise get spline using method specified on input
  ages <- length(unique(input$age_group_id))
  if(ages == 1){
    print('No age covariate')
    agecov <- c()
  }else if(ages < 4){
    print('Age covariate no spline')
    agecov <- cov_info("age_midpoint", "X")
  }else{
    print('Age covariate w/ spline')
    # getknots - creates covinfo() input for MR-BRT based on age spline method, available input data for model
    agecov <- getknots(knot_vers, input)
    covs <- list(sexcov, agecov)
  }
  
 
  covs <- list(sexcov, agecov)

  # run mr-brt model. Study-location is set as study id, covariates, trim & method from above
  fit <- run_mr_brt(
    output_dir = bun_write_folder, 
    model_label = paste0("mrbrt_",bundle,"_",cf),
    data = paste0(data_folder, bundle, '.csv') ,
    mean_var = 'cf',
    se_var = 'cf_se',
    covs = covs,
    overwrite_previous = TRUE,
    method = mthd,
    trim_pct = tr,
    study_id = 's_loc',
    max_iter = 1000)

  # Sometimes models just don't run. Try again in that case.
  tries <- 0
  while(tries < 3 & !check_for_outputs(fit)){
    print('Relaunching the model, something happened')
    fit <- run_mr_brt(
      output_dir = bun_write_folder, 
      model_label = paste0("mrbrt_",bundle,"_",cf),
      data = paste0(data_folder, bundle, '.csv') ,
      mean_var = 'cf',
      se_var = 'cf_se',
      covs = covs,
      overwrite_previous = TRUE,
      method = mthd,
      trim_pct = tr,
      study_id = 's_loc',
      max_iter = 100)
    tries <- tries + 1
  }
  
  # Check if age or sex variability is getting trimmed - unlikely with 5% trim
  w_check <- fit$train_data %>% as.data.table()
  w_check <- w_check[w == 1]
  nage_w <- length(unique(w_check$age_group_id))
  nsex_w <- length(unique(w_check$sex_id))
  
  # Automatically run w/o sex if sex is trimmed
  if(sexes != nsex_w){
    print("Sex variability trimmed - running without sex")
    covs <- list(agecov)
    fit <- run_mr_brt(
      output_dir = bun_write_folder, 
      model_label = paste0("mrbrt_",bundle,"_",cf),
      data = paste0(data_folder, bundle, '.csv') ,
      mean_var = 'cf',
      se_var = 'cf_se',
      covs = covs,
      overwrite_previous = TRUE,
      method = mthd,
      trim_pct = tr,
      study_id = 's_loc',
      max_iter = 100)
  }
  
  if(ages < 4) stopifnot(ages == nage_w)
  if(ages >= 4){
    stopifnot(min(input$age_midpoint) >= min(w_check$age_midpoint)-5)
    stopifnot(max(input$age_midpoint) <= max(w_check$age_midpoint)+10)
  }
  
  return(fit)
}

# Download restrictions to see what ages and sexes there should be. Subset preddf for it
# Sets the min and max to PREDICT for
createpreddf <- function(bundle, write_folder){
  restrictions <- get_bundle_restrictions() %>% as.data.table() %>% .[bundle_id == bundle]
  preddf <- get_age_groups(bundle, write_folder)
  if(restrictions$yld_age_start < 1 & restrictions$yld_age_start != 0 & 0.5 %in% preddf$age_midpoint){
    preddf <- preddf[age_start <= restrictions$yld_age_end]
  }else{
    preddf <- preddf[age_start >= restrictions$yld_age_start & age_start <= restrictions$yld_age_end]
  }
  if(restrictions$male == 0 & restrictions$female == 1){
    preddf <- preddf[,sex_id := 2]
  } else if(restrictions$male == 1 & restrictions$female == 0){
    preddf <- preddf[,sex_id := 1]
  }else{
    preddf_m <- copy(preddf[, sex_id := 1])
    preddf <- preddf[, sex_id := 2]
    preddf <- rbind(preddf, preddf_m)
  }
  
 
  return(preddf)
}

# Function to pull min/max ages based on data availability
get_bounds <- function(fit,preddf){
  # Extend using min and max ages in input data to apply model fit from where data exists to age groups without data. 
  train_data <- fit$train_data %>% as.data.table()
  train_data[w < 0.5, trimmed := 'Trimmed data'][w >= 0.5, trimmed := 'Untrimmed data']
  train_data[sex_id == 1, sex := 'Males'][sex_id == 2, sex := 'Females']
  untrimmed <- train_data[trimmed == 'Untrimmed data']
  untrimmed[, num_points := .N, by = 'age_start']
  
  # Sets the min and max THRESHOLD of age to predict at (due to unstability in data sparse ages)
  min_age <- min(untrimmed[w == 1 & num_points >= 10]$age_midpoint)
  max_age <- max(untrimmed[w == 1 & num_points >= 10]$age_midpoint)
  
  # If there are less than 10 points (or less than 2 later)
  # Iterate through a series of thresholds.
  if(min_age == Inf){
    min_age <- suppressWarnings(min(untrimmed[w == 1 & num_points >= 5]$age_midpoint))
    max_age <- suppressWarnings(max(untrimmed[w == 1 & num_points >= 5]$age_midpoint))
  }
  
  if(min_age == Inf){
    min_age <- suppressWarnings(min(untrimmed[w == 1 & num_points >= 2]$age_midpoint))
    max_age <- suppressWarnings(max(untrimmed[w == 1 & num_points >= 2]$age_midpoint))
  }
  
  if(min_age == Inf){
    min_age <- suppressWarnings(min(untrimmed[w == 1 & num_points >= 1]$age_midpoint))
    max_age <- suppressWarnings(max(untrimmed[w == 1 & num_points >= 1]$age_midpoint))
  }
  
  # Reset minimum age to the neonatal age if the min age is 0.5
  if(min_age == 0.5){
    min_age <- min(preddf$age_midpoint)
    # If preddf has 0.5 in it, just set it to ENN
    if(min_age == 0.5) min_age <- 0.00958904
  }
  if(max_age == 0.5){
    max_age <- min(preddf$age_midpoint)
  }
  
  sex_cov <- as.logical("sex_id" %in% fit$input_metadata$covariate)
  age_cov <- as.logical("age_midpoint" %in% fit$input_metadata$covariate)
  
  return(list(youngest_age = min_age, oldest_age = max_age, sex_effect = sex_cov, age_effect = age_cov))
}

# Extend predictions from min/max ages w/ input data to other ages w/o data
extender <- function(df, preddf, bounds){
  # Subset to only the ages you are going to predict for, will extend out from there based on min and max in preddf
  min_age <- bounds[[1]]
  max_age <- bounds[[2]]
  sex_cov <- bounds[[3]]
  age_cov <- bounds[[4]]
  
  # Adapt for new ages since they aren't in CF inputs
  if(min_age == 3){min_age <- 1.5}
  if(max_age == 3){max_age <- 3.5}
  if(min_age == 0.5){min_age <- 0.00958904}
  if(max_age == 0.5){max_age <- 0.00958904}
  
  if(age_cov){
    df <- df[X_age_midpoint >= min_age & X_age_midpoint <= max_age] %>% unique()
  }
  
  # Add sexes if the model didn't predict on them
  if(!sex_cov){
    df1 <- copy(df) %>% .[, X_sex_id := 1]
    df2 <- copy(df) %>% .[, X_sex_id := 2]
    df <- rbind(df1, df2) 
    df <- df[X_sex_id %in% unique(preddf$sex_id)]
  }
  
  limits <- df[X_age_midpoint == min_age | X_age_midpoint == max_age]
  stopifnot('X_sex_id' %in% names(limits))
  setnames(limits, 'X_age_midpoint', 'limit')
  limits <- unique(limits)
  print(limits[, c('limit')])
  
  # Make limits square for the sexes you want to predict for
  extra_ages <- preddf[age_midpoint < min_age | age_midpoint > max_age]
  extra_ages[age_midpoint <= min_age, limit := min_age][age_midpoint >= max_age, limit := max_age]
  setnames(extra_ages, 'sex_id', 'X_sex_id')
  
  # Merge on extra ages to limits to extend
  extend <- merge(limits, extra_ages, by = c('limit', 'X_sex_id'), allow.cartesian = TRUE)
  setnames(extend, 'age_midpoint', 'X_age_midpoint')
  
  df <- rbind(df, extend, fill = TRUE)
  df[X_sex_id == 1, sex := 'Males'][X_sex_id == 2, sex := 'Females']
  df[, limit := NULL]
  df[, age_group_id := NULL]
  df[, sex := NULL]
  df[, age_group_name := NULL]
  df[, age_start := NULL]

  return(df)
}

get_medians <- function(draws){
  cols <- names(draws)
  cols <- str_subset(cols,"^draw_",negate=TRUE)
  draws <- melt(draws, id.vars = cols, measure.vars = patterns('^draw_'), variable.name = 'draw_level', value.name = 'draw')
  draws <- draws[,.(cf_mean = mean(draw), cf_median = median(draw), upper = quantile(draw, 0.975), lower = quantile(draw, 0.025)), by=cols]
  return(draws)
}

# Format to be digestable by the rest of the pipeline
ernielite <- function(df, bundle, cf, write_folder){
  df <- unique(df)
  age_groups <- fread(paste0(write_folder,'age_groups.csv'))
  age_groups$age_midpoint <- round(age_groups$age_midpoint, 6) 
  
  # Modify age variables
  df$X_age_midpoint <- round(df$X_age_midpoint, 6)
  if(0.5 %in% df$X_age_midpoint){
    u1 <- df[X_age_midpoint == 0.5]
    u1[, X_age_midpoint := NULL]
    u1[, index := 1]
    u1ages <- age_groups[age_group_id %in% c(2,3,388,389),c('age_midpoint')]
    u1ages[, index := 1]
    u1 <- merge(u1, u1ages, by='index', allow.cartesian = TRUE)
    u1[, index := NULL]
    setnames(u1, 'age_midpoint','X_age_midpoint')
    df <- rbind(df[X_age_midpoint != 0.5], u1)
  }
  
  df <- merge(df, age_groups, by.x = "X_age_midpoint", by.y = "age_midpoint")
  
  df[, "cf" := cf]
  df[, "bundle_id" := bundle]
  df[, "age_group_name" := NULL]
  setnames(df, c('X_sex_id', 'X_age_midpoint', 'cf'), c('sex_id', 'age_midpoint', 'cf_type'))
  setcolorder(df, c('age_start', 'age_group_id', 'age_midpoint', 'sex_id', 'bundle_id', 'cf_type'))
  
  # Get outta log/logit space
  if(cf == 'cf1'){
    tform <- function(x) inv.logit(x)
  } else {
    tform <- function(x) exp(x)
  }
  drs <- str_subset(names(df), "^draw_")
  df[, (drs) := lapply(.SD, tform), .SDcols = drs]
  for(col in drs) set(df, i=which(df[[col]] > 100000), j=col, value=100000)
  return(df)
}

# Plotting -----
cf_plots <- function(bun, cf, data_folder, from_file=FALSE){
  
  data <- cf_plotprep(bun, cf, data_folder, from_file)
  model_sum <- data$model_sum
  restrictions_map <- data$restrictions_map
  bundle_name <- data$bundle_name
  train_data <- data$train_data
  train_data[, cf := NULL]
  
  data_plot <- ggplot(train_data, aes(x = age_midpoint, y = mean_val, color = factor(trimmed))) +
    geom_point() + theme_bw() +
    ylab('CF input data') + xlab('Age') + 
    ggtitle(paste0(bun, " - ", bundle_name, ' ', cf, ' input data')) +
    scale_color_manual(name = '', values = c('#33cc33', '#D354AC')) +
    facet_wrap(~sex)
  
  results_plot_noUI <- ggplot(unique(model_sum), aes(x = age_midpoint, y = cf_val)) +
    geom_point(color = 'black') + theme_bw() +
    ylab('CF model predictions') + xlab('Age') +
    ggtitle(paste0(bundle_name, ' model comparison')) +
    facet_wrap(~sex) +
    xlim(c(restrictions_map$min_age[1],restrictions_map$max_age[1])) #+

  
  results_plot_UI <- ggplot(unique(model_sum), aes(x = age_midpoint, y = cf_val)) + 
    theme_bw() +
    ylab('CF model predictions') + xlab('Age') +
    ggtitle(paste0(bundle_name, ' model comparison')) +
    facet_wrap(~sex) +
    xlim(c(restrictions_map$min_age[1],restrictions_map$max_age[1])) +
    geom_ribbon(aes(ymin = lower, ymax = upper),fill = 'gray', alpha = 0.5) +
    geom_point(color = 'black')
  
  # #Other additions to results_plot, commented out
  all <- grid.arrange(data_plot, results_plot_noUI, results_plot_UI, nrow = 3)
  print(all)
}

cf_compare <- function(bun, cf, old_dir, new_dir){
  old <- cf_plotprep(bun, cf, old_dir)
  restrictions_map <- old$restrictions_map
  bundle_name <- old$bundle_name
  train_old <- old$train_data
  model_old <- old$model_sum
  
  new <- cf_plotprep(bun, cf, new_dir)
  train_new <- new$train_data
  model_new <- new$model_sum 
    
  model_sum <- bind_rows(model_old, model_new, .id='version')
  
  limit_recs <- model_sum %>%
    select(limit, version) %>%
    unique() %>%
    drop_na() %>%
    group_by(version) %>%
    summarize(xlo = min(limit), xhi = max(limit)) %>%
    ungroup()
  
  # Get out of logit space for cf1, logspace for cf2 & cf3
  if(cf != 'cf1'){
    model_sum <- mutate_at(model_sum, vars(Y_mean, lo_lo, hi_hi), exp)
  } else {
    model_sum <- mutate_at(model_sum, vars(Y_mean, lo_lo, hi_hi), inv.logit)
  }
  
  comparison_plot <- ggplot() + 
    geom_rect(data=limit_recs, aes(xmin = xlo, xmax = xhi, ymin = -Inf, ymax = Inf, fill = version), alpha = 0.08) +
    geom_point(data = model_sum, aes(x = age_start, y = Y_mean, color = version)) + 
    geom_line(data = model_sum, aes(x = age_start, y = Y_mean, color = version)) +
    theme_bw()  +
    ylab('CF model predictions') + xlab('Age') +
    ggtitle(paste0(bundle_name, ' model comparison')) +
    ylim(0,max(model_sum$Y_mean)) +
    facet_wrap(~sex) +
    scale_fill_discrete(name = "Version", breaks = c("1","2"), labels = c("V1", "V2")) +
    scale_color_discrete(name = "Version", breaks = c("1","2"), labels = c("V1", "V2")) +
    xlim(c(restrictions_map$min_age[1],restrictions_map$max_age[1])) +
    theme(legend.position="top")
  
  comparison_plot
  
}

