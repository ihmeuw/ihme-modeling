#####Calculate SE if missing values
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

#Pooling hemog VR data for other Hemog - Cause ID 618

compile_other_hemog <- function(gbd_round_id, decomp_step, all_years){
  
  lm <- get_location_metadata(location_set_id = 43, gbd_round_id = 7)
  lm <- dr_locs[parent_id == 44640 & location_id != 44640, .(location_id, location_name, ihme_loc_id)]$location_id
  
  df <- get_cod_data(cause_id = 618, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
  
  pooled_years <- copy(all_years)
  
  test <- copy(df)
  
  test <- test[data_type == "Vital Registration"] # Subset to only VR data
  
  test <- merge(test, lm, by = c("location_id"), all.x = TRUE) # Subset to only data rich locations

  test <- test %>% select(cause_id, location_id, year, age_group_id, sex, sample_size, rate)
  
  # Grouping by age, year and sex, take the mean rate and sum the sample size across all data rich locations
  catch_df <- test %>%
    group_by(age_group_id, year, sex) %>%
    summarise(mean = mean(rate),
              sample_size = sum(sample_size))

  
  test_se <- data.table(copy(catch_df))
  
  test_se$standard_error <- 0
  test_se$lower <- NA
  test_se$upper <- NA
  test_se$cases <- NA
  
  # Calculate the SE, variance, mean of the variance, and mean of the sample size in each a/s/y group
  test_se <- test_se[, standard_error := sqrt(mean / sample_size)]
  test_se$variance <- test_se$standard_error^2
  test_se$var_mean <- test_se$mean * test_se$variance
  test_se$ss_mean <- test_se$mean * test_se$sample_size 
  
  
  print("Pooling")
  
  pooling_df <- data.frame()
  
  
  
  for (py in all_years){
    
    py <- as.integer(py)
    
    if (py %in% c(2019, 2020, 2021, 2022)){
      year_range <- c(2014:2022)
    } else{
      year_range <- c((py-5): (py+5))
    }
    
    # Grouping by age and sex, across the year span, get the mean of the mortality rate (mean), mean of SE, and sum sample size,
    # sum mean of the variance, sum sample size mean, and sum variance. Now you have a 10 year pooled data frame.
    inter_df <- test_se[test_se$year %in% year_range, ] %>%
      group_by(age_group_id, sex) %>%
      summarise(mean = mean(mean),
                standard_error = mean(standard_error),
                sample_size = sum(sample_size),
                var_mean = sum(var_mean),
                ss_mean = sum(ss_mean),
                variance = sum(variance))
    
    inter_df$year <- py
    
    inter_df$mean_1 <- inter_df$ss_mean / inter_df$sample_size # think this is avg deaths over avg sample size?
    
    inter_df$standard_error <- 0
    inter_df$lower <- NA
    inter_df$upper <- NA
    inter_df$cases <- NA
    
    inter_df <- data.table(inter_df)
    
    inter_df <- get_se(inter_df)
    
    colnames(inter_df)[which(names(inter_df) == "standard_error")] <- "se_1"
    
    inter_df$lower <- NULL
    inter_df$upper <- NULL
    inter_df$measure <- NULL
    inter_df$cases <- NULL
    
    pooling_df <- rbind(pooling_df, inter_df)
  }
  
  print("Years pooled")
  
  age_map <- data.table()
  age_map$age_group_id <- unique(pooling_df$age_group_id)
  age_map$age_group_name <- unique(df$age_name)
  
  pooling_df <- merge(pooling_df, age_map, by = ('age_group_id'))
  
  
  ##### Get 1000 draws of the global deaths by age, sex, and year and save out flat file
  draws_df <- pooling_df %>%
    select(age_group_id, sex, year, mean_1, se_1)
  
  
  ls <- c(0:999)
  
  draws_list <- c()
  
  for (n in ls){
    draws_list <- append(draws_list, paste0("draw_", n))
  }
  
  
  
  test_list <- c(1:length(draws_df$mean_1))
  
  final_df <- data.frame()
  
  
  for (n in test_list){
    print(n)
    
    test_df <- draws_df[n,]
    
    new_norm <- rnorm(1000, mean = test_df$mean_1, sd = test_df$se_1)
    
    
    sapply(1:length(draws_list), function(i)
      test_df[, draws_list[i]] <<- new_norm[i]
    )
    
    final_df <- rbind(final_df, test_df)
  }
  
  
  
  return(final_df)
  
}


####Interpolate Dismod results for hemog causes, save out flat files
dismod_interpolate <- function(gbd_round_id, decomp_step, loc_id, year_start, year_end){
  
  full_set <- data.frame()
  
  for (me in input_map$modelable_entity_id[input_map$source == "dismod"]){
    print(me)
    
    # Interpolate CSMR for each nonfatal hemoglobinopathy ME
    interp_df <- interpolate(gbd_id_type = 'modelable_entity_id', gbd_id = me,
                             source = 'epi', measure_id = 15, location_id = loc_id,
                             reporting_year_start = year_start, reporting_year_end = year_end,
                             gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                             status = 'best')
    
    full_set <- rbind(full_set, interp_df)
    
    
    
    
  }
  
  merged_interp <- merge(full_set, input_map, by = c("modelable_entity_id")) %>%
    select(-model_version_id, -source, -measure_id, -metric_id, -modelable_entity_id)
  
  agg_interp <- merged_interp %>%
    group_by(age_group_id, sex_id, year_id, cause_id, location_id) %>%
    summarise_all(sum)
  
  return(agg_interp)
  
  
}

####### Pull down model draws of parent hemog CoD model, convert to rate

get_model_draws <- function(gbd_round_id, decomp_step, all_years, loc_id){
  print("Getting model draws")
  
  model_df <- get_draws(gbd_id_type = 'cause_id', gbd_id = 613,
                        location_id = loc_id, year_id = all_years, num_workers = 10,
                        source = 'codem', gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                        status = "best")
  
  
  pop_other <- get_population(age_group_id = unique(model_df$age_group_id), location_id = loc_id,
                              year_id = all_years, sex_id = c(1,2), gbd_round_id = gbd_round_id,
                              decomp_step = decomp_step)
  
  model_w_pop <- merge(model_df, pop_other, by = c("age_group_id", 'sex_id', "year_id", "location_id"))
  
  model_w_pop <- data.frame(model_w_pop)
  
  df_only_data <- model_w_pop[, grepl("draw", colnames(model_w_pop))]
  
  model_data_rate <- df_only_data / model_w_pop$population
  
  model_data_rate <- cbind(model_w_pop[, c("age_group_id", "sex_id", "year_id", "cause_id", "location_id")],
                           model_data_rate)
  
  return(model_data_rate)
}



####### Scale data to parent hemog cod model (cause id 613) -- pulls in whichever model is marked best

scale_cod_data <- function(loc, gbd_round_id, decomp_step, out_dir, causes){
  
  
  df <- data.frame()
  
  
  # read in the dismod interpolated CSMR for each cause and append for each location
  for (cause in causes){
    agg_interp <- read.csv(paste0(out_dir, "interp_files/", cause, "/interp_hemog_", loc, ".csv"))
    
    df <- rbind(df, agg_interp)
  }
  

  #Reading in parent hemog cod model draws for each location
  
  model_df <- read.csv(paste0(out_dir, 'model_draws/model_hemog_', loc,'.csv'))
  
  
  
  #Scale all hemog subcause dismod interpolated plus "other hemog" pooled cod VR data to parent (this is effectively a "squeeze" or "rake")
  
  all_interp <- data.table(df)
  
  # Grouping by year, age, sex, and location, sum every column
  prop_df <- all_interp %>%
    group_by(year_id, age_group_id, sex_id, location_id) %>%
    summarise_all(sum)
  

  prop_df <- prop_df[order(prop_df$age_group_id, prop_df$sex_id), ]
  
  scaled_df <- data.frame()
  
  prop_divisor <- data.table(prop_df[, grepl("draw", colnames(prop_df))])
  
  prop_divisor$seq <- c(1:length(prop_divisor$draw_0))
  
  model_multiple <- data.table(model_df[, grepl("draw", colnames(model_df))])
  
  model_multiple$seq <- c(1:length(model_multiple$draw_0))
  
  
  
  for (ce in unique(all_interp$cause_id)) {
    temp_df <- all_interp[cause_id == ce, ] 
    
    temp_df <- temp_df[order(temp_df$age_group_id, temp_df$sex_id), ]
    
    
    temp_df_2 <- temp_df %>%
      select(-age_group_id, -sex_id, -year_id, -location_id, -cause_id)
    
    
    temp_df_2$seq <- c(1:length(temp_df_2$draw_0))
    
    
    applied_df <- temp_df_2 / prop_divisor
    
    print(applied_df$seq[1:10])
    
    another_df <- applied_df * model_multiple
    
    print(another_df$seq[1:10])
    
    inter_df <- cbind(temp_df[, c("age_group_id", "year_id", "sex_id", "location_id", "cause_id")],
                      another_df)
    
    scaled_df <- rbind(scaled_df, inter_df)
  }
  
  scaled_df$seq <- NULL
  
  return(scaled_df)
  
}


get_synth_draws <- function(gbd_round_id, decomp_step, all_years, loc_id, sex_id, cause, version_id){
  
  synth_df <- get_draws('cause_id', gbd_id = cause, source = 'codem', 
                        location_id = loc, version_id = version_id, 
                        gbd_round_id = gbd_round, decomp_step = decomp_step)
  
  pop_df <- get_population(age_group_id = unique(synth_df$age_group_id), location_id = loc_id,
                           year_id = all_years, sex_id = sex_id, gbd_round_id = gbd_round_id,
                           decomp_step = decomp_step)
  
  synth_w_pop <- merge(synth_df, pop_df, by = c("age_group_id", 'sex_id', "year_id", "location_id"))
  
  synth_w_pop <- data.frame(synth_w_pop)
  
  df_only_data <- synth_w_pop[, grepl("draw", colnames(synth_w_pop))]
  
  synth_data_rate <- df_only_data / synth_w_pop$population
  
  synth_data_rate <- cbind(synth_w_pop[, c("age_group_id", "sex_id", "year_id", "cause_id", "location_id")],
                           synth_data_rate)
  
  return(synth_data_rate)
  
}



