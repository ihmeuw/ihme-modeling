## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 6_long_covid.R
## Description: Stream short-term outcomes into long-term outcomes.
##              Numbered comments correspond to documentation on this HUB page:
##              ADDRESS
## Contributors: NAME, NAME
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----

.repo_base <-
  strsplit(
    whereami::whereami(path_expand = TRUE),
    "nf_covid"
  )[[1]][1]

.repo_base <- 'FILEPATH'
source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k, 'FILEPATH/get_location_metadata.R'))
source(paste0(roots$k, 'FILEPATH/get_population.R'))

## Data Processing Functions ------------------------------------------- ----

.apply_proportions <- function(df, df_name, long_seq, definition) {
  #' Convenience function to apply any_lc_proportions for each outcome
  #' @param df [data.table]
  #' @param df_name [str]
  #' @param long_seq [vector]
  
  df <- copy(df)
  
  # Ensure cols for long_seq exist
  if (any(paste0(long_seq) %ni% names(df))) {
    stop(paste0('Supplied ', df_name, ' df missing one or more of the ',
                'following columns: ', paste0(long_seq, collapse=', '), '.'))
  }
  # Ensure col for {df_name}_risk_num exists
  if (paste0(df_name, '_risk_num') %ni% names(df)) {
    stop(paste0('Supplied ', df_name, ' df missing column for: ', df_name, '_risk_num.'))
  }
  
  # For each outcome
  for (outcome in long_seq) {
    
    
    ###############################################################################################################################################
    ### Multiply the risk number by the outcome proportion at 3 months after symptom onset for long COVID
    df[, eval(paste0(df_name, '_', outcome, '_inc')) :=
         get(paste0(df_name, '_risk_num')) * get(paste0(outcome))]
    
    # Remove the proportion
    df[, eval(paste0(outcome)) := NULL]
  }
  
  return(df)
}


.apply_durations <- function(df, df_name, long_seq, estimation_years) {
  df <- copy(df)
  
  ## apply durations to all symptom clusters
  
  df[, year := year(date)]
  for (outcome in long_seq) {
    ## LONGTERM SEQUELA: either from end of acute phase or 3 months from 
    #      symptom onset, with durations depending on value of definition
    # Multiply the risk number by the duration
    df[, eval(paste0(df_name, '_', outcome, '_prev')) := 0]
    
    # calculate prev for the year when the person was at risk
    df[!is.na(fat_or_resp_or_cog), eval(paste0(df_name, '_', outcome, '_prev')) :=
         get(paste0(df_name, '_', outcome, '_inc')) * fat_or_resp_or_cog]
    
    # calculate prev for the years following the one at risk
    for (yr in seq(estimation_years[2:length(estimation_years)])) {
      df[, eval(paste0(df_name, '_', outcome, '_prev_', yr)) := 0]
      df[!is.na(get(paste0('dur', yr))), eval(paste0(df_name, '_', outcome, '_prev_', yr)) :=
           get(paste0(df_name, '_', outcome, '_inc')) * get(paste0('dur', yr))]
    }
    
    # assign that roll-over prevalence to the proper year
    for (yr in estimation_years[2:length(estimation_years)]) {
      tictoc::tic(msg=paste0(outcome, " ", yr))
      df$yr <- yr
      df[, eval(paste0(df_name, '_', outcome, '_prev_', yr)) := 0]
      df[yr == (year + 1), eval(paste0(df_name, '_', outcome, '_prev_', yr)) :=
           get(paste0(df_name, '_', outcome, '_prev_1'))]
      df[yr == (year + 2), eval(paste0(df_name, '_', outcome, '_prev_', yr)) :=
           get(paste0(df_name, '_', outcome, '_prev_2'))]
      if (max(estimation_years)-min(estimation_years)==3) {
        df[yr == (year + 3), eval(paste0(df_name, '_', outcome, '_prev_', yr)) :=
             get(paste0(df_name, '_', outcome, '_prev_3'))]
      }
      tictoc::toc()
    }
    
    
    # Remove extra vars
    for (yr in seq(estimation_years[2:length(estimation_years)])) {
      df[, eval(paste0(df_name, '_', outcome, '_prev_', yr)) := NULL]
    }
    df[, yr := NULL]
  }
  
  for (yr in estimation_years[2:length(estimation_years)]) {
    follow_up_y <- ifelse((((yr - 1) %% 4) == 0), (yr - estimation_years[1]), (yr - estimation_years[1]))
    df[, eval(paste0('dur', follow_up_y)) := NULL]
  }
  
  df$fat_or_resp_or_cog <- NULL
  
  return(df)
}


.prep_to_save <- function(df, df_name, long_seq, refs, definition, estimation_years) {
  df <- subset_date(df, estimation_years)
  df$infections <- NULL
  
  df[, year_id := year(date)]
  
  df <- df[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'year_id', 'age_group_id', 
                                             'sex_id', 'draw'), 
           .SDcols=c(paste0(df_name, '_', refs$long_calc_cols))]
  
  if (long_seq[1] == 'any') {
#    df[, eval(paste0(df_name, '_any_prev')) := 0]
    df[(year_id %% 4) == 0, eval(paste0(df_name, '_any_prev')) := get(paste0(df_name, '_any_prev')) / 366] # Dividing by 366 to calculate into year space because applied duration is in days
    df[(year_id %% 4) > 0, eval(paste0(df_name, '_any_prev')) := get(paste0(df_name, '_any_prev')) / 365] # Dividing by 365 to calculate into year space because applied duration is in days
  } else {
    for (var in paste0(df_name, '_', refs$long_calc_cols[grepl('prev', refs$long_calc_cols)])) {
      df[(year_id %% 4) == 0, eval(var) := get(var) / 366] # Dividing by 366 to calculate into year space because applied duration is in days
      df[(year_id %% 4) > 0, eval(var) := get(var) / 365] # Dividing by 365 to calculate into year space because applied duration is in days
    }
  }
  
  df_rolled_over <- copy(df)
  for (var in paste0(df_name, '_', long_seq, '_prev')) {
    df_rolled_over[, eval(var) := NULL]
  }
  
  df_rolled_over <- df_rolled_over[, lapply(.SD, sum, na.rm=T), 
                                   by=c('location_id', 'draw', 'age_group_id', 'sex_id'), 
                                   .SDcols=c(colnames(df_rolled_over)[grepl('prev', colnames(df_rolled_over))])]
  
  
  for (var in c(paste0(df_name, '_', long_seq, '_prev_'))) {
    for (yr in estimation_years[2:length(estimation_years)]) {
      df[, eval(paste0(var, yr)) := NULL]
    }
  }
  # make df of onset-year estimates wide by year
  df <- dcast(df, formula = paste0('location_id + age_group_id + sex_id + draw ~ year_id'),
              value.var=c(paste0(df_name, '_', long_seq, '_prev'), paste0(df_name, '_', long_seq, '_inc')))
  
  if (long_seq[1] != 'any') {
    for (var in paste0(df_name, '_', refs$long_calc_cols[grepl('_prev_', refs$long_calc_cols)])) {
      setnames(df_rolled_over, var, paste0('rolled_over_', var))
    }
    df <- merge(df, df_rolled_over, by = c('location_id', 'age_group_id', 'sex_id',
                                           'draw'))
  }
  
  return(df)
}


.add_rollover <- function(df, df_name, long_seq, estimation_years) {
  for (yr in estimation_years) {
    for (outcome in long_seq) {
      if (yr == estimation_years[1]) {
        df[, eval(paste0('rolled_over_', df_name, '_', outcome, '_prev_', yr)) := 0]
      }
      df[, eval(paste0(df_name, '_', outcome, '_prev_', yr)) := get(paste0(df_name, '_', outcome, '_prev_', yr)) + 
           get(paste0('rolled_over_', df_name, '_', outcome, '_prev_', yr))]
      df[, eval(paste0('rolled_over_', df_name, '_', outcome, '_prev_', yr)) := NULL]
      setnames(df, paste0(df_name, '_', outcome, '_prev_', yr), paste0(df_name, '_', outcome, '_prev.', yr))
      setnames(df, paste0(df_name, '_', outcome, '_inc_', yr), paste0(df_name, '_', outcome, '_inc.', yr))
    }
  }
  
  df <- reshape(df, idvar = c('location_id', 'age_group_id', 'sex_id', 'draw'),
                timevar = 'year_id', times = estimation_years, 
                direction = 'long', varying = c(colnames(df)[grepl('prev', colnames(df))], colnames(df)[grepl('inc', colnames(df))]),
                sep = '.')
  
  #  df <- .year_scale_prevalence(df, long_seq)
  
  return(df)
}


.save_rolling_prev <- function(df, df_name, long_seq, refs, estimation_years) {
  
  df <- copy(df)
  
  df[, year := year(date)]
  
  if (df_name == 'midmod') {
    cols_extra <- c('infections', 'midmod_inc', 'hospital_inc', 'midmod_risk_num')
  } else if (df_name == 'hospital') {
    cols_extra <- c('hospital_inc', 'icu_inc', 'hsp_deaths', 'hospital_risk_num')
  } else if (df_name == 'icu') {
    cols_extra <- c('icu_inc', 'icu_deaths', 'icu_risk_num')
  }
  
  
  # Take mean of draws
  save <- subset_date(df, estimation_years)
  save <- save[, lapply(.SD, mean, na.rm=TRUE), by=c('date', 'sex_id', 'location_id', 'age_group_id', 'year'), 
               .SDcols = c(paste0(df_name, '_', refs$long_calc_cols), cols_extra)]
  
  save <- subset_date(save, estimation_years)
  save <- save[, lapply(.SD, sum, na.rm=TRUE), by=c('sex_id', 'location_id', 'age_group_id', 'year'), 
               .SDcols=c(paste0(df_name, '_', refs$long_calc_cols), cols_extra)]
  
  out_dir <- file.path('FILEPATH', output_version, 'location_vetting_csvs')
  
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  fwrite(save, file.path(out_dir, paste0(loc_id, df_name, '.csv')))
  rm(save)
  
  # cumulative cases for 2020 vetting
  save <- subset_date(df, estimation_years)
  save <- save[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'draw', 'year'), 
               .SDcols=c(paste0(df_name, '_', refs$long_calc_cols), cols_extra)]
  
  for (yr in estimation_years[2:length(estimation_years)]) {
    save <- save[, eval(paste0(df_name, '_long_covid_prev_', yr)) := 
                   get(paste0(df_name, '_cognitive_severe_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_mild_prev_', yr)) + 
                   get(paste0(df_name, '_fatigue_prev_', yr)) + 
                   get(paste0(df_name, '_respiratory_mild_prev_', yr)) + 
                   get(paste0(df_name, '_respiratory_moderate_prev_', yr)) +
                   get(paste0(df_name, '_respiratory_severe_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_mild_fatigue_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_severe_fatigue_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_mild_respiratory_mild_prev_', yr)) +
                   get(paste0(df_name, '_cognitive_severe_respiratory_mild_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_mild_respiratory_moderate_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_severe_respiratory_moderate_prev_', yr)) +
                   get(paste0(df_name, '_cognitive_mild_respiratory_severe_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_severe_respiratory_severe_prev_', yr)) + 
                   get(paste0(df_name, '_fatigue_respiratory_mild_prev_', yr)) +
                   get(paste0(df_name, '_fatigue_respiratory_moderate_prev_', yr)) + 
                   get(paste0(df_name, '_fatigue_respiratory_severe_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_mild_prev_', yr)) +
                   get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_mild_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_moderate_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_moderate_prev_', yr)) +
                   get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_severe_prev_', yr)) + 
                   get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_severe_prev_', yr))]
    
    for (outcome in long_seq) {
      save[, eval(paste0(df_name, '_', outcome, '_prev_', yr)) := NULL]
    }
    
  }
  
  save <- save[, eval(paste0(df_name, '_long_covid_inc')) := 
                 get(paste0(df_name, '_cognitive_severe_inc')) + 
                 get(paste0(df_name, '_cognitive_mild_inc')) + 
                 get(paste0(df_name, '_fatigue_inc')) + 
                 get(paste0(df_name, '_respiratory_mild_inc')) + 
                 get(paste0(df_name, '_respiratory_moderate_inc')) +
                 get(paste0(df_name, '_respiratory_severe_inc')) + 
                 get(paste0(df_name, '_cognitive_mild_fatigue_inc')) + 
                 get(paste0(df_name, '_cognitive_severe_fatigue_inc')) + 
                 get(paste0(df_name, '_cognitive_mild_respiratory_mild_inc')) + 
                 get(paste0(df_name, '_cognitive_severe_respiratory_mild_inc')) + 
                 get(paste0(df_name, '_cognitive_mild_respiratory_moderate_inc')) + 
                 get(paste0(df_name, '_cognitive_severe_respiratory_moderate_inc')) +
                 get(paste0(df_name, '_cognitive_mild_respiratory_severe_inc')) + 
                 get(paste0(df_name, '_cognitive_severe_respiratory_severe_inc')) + 
                 get(paste0(df_name, '_fatigue_respiratory_mild_inc')) + 
                 get(paste0(df_name, '_fatigue_respiratory_moderate_inc')) + 
                 get(paste0(df_name, '_fatigue_respiratory_severe_inc')) + 
                 get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_mild_inc')) +
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_mild_inc')) + 
                 get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_moderate_inc')) + 
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_moderate_inc')) + 
                 get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_severe_inc')) + 
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_severe_inc'))]
  
  save <- save[, eval(paste0(df_name, '_long_covid_prev')) := 
                 get(paste0(df_name, '_cognitive_severe_prev')) + 
                 get(paste0(df_name, '_cognitive_mild_prev')) + 
                 get(paste0(df_name, '_fatigue_prev')) + 
                 get(paste0(df_name, '_respiratory_mild_prev')) + 
                 get(paste0(df_name, '_respiratory_moderate_prev')) +
                 get(paste0(df_name, '_respiratory_severe_prev')) + 
                 get(paste0(df_name, '_cognitive_mild_fatigue_prev')) + 
                 get(paste0(df_name, '_cognitive_severe_fatigue_prev')) + 
                 get(paste0(df_name, '_cognitive_mild_respiratory_mild_prev')) + 
                 get(paste0(df_name, '_cognitive_severe_respiratory_mild_prev')) + 
                 get(paste0(df_name, '_cognitive_mild_respiratory_moderate_prev')) + 
                 get(paste0(df_name, '_cognitive_severe_respiratory_moderate_prev')) +
                 get(paste0(df_name, '_cognitive_mild_respiratory_severe_prev')) + 
                 get(paste0(df_name, '_cognitive_severe_respiratory_severe_prev')) + 
                 get(paste0(df_name, '_fatigue_respiratory_mild_prev')) + 
                 get(paste0(df_name, '_fatigue_respiratory_moderate_prev')) + 
                 get(paste0(df_name, '_fatigue_respiratory_severe_prev')) + 
                 get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_mild_prev')) +
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_mild_prev')) + 
                 get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_moderate_prev')) + 
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_moderate_prev')) + 
                 get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_severe_prev')) + 
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_severe_prev'))]
  
  
  if (df_name == 'midmod') {
    
    save_cols <- c('year', 'location_id', 'draw', 'infections', 'midmod_inc', 'hospital_inc', 
                   'midmod_risk_num', 'midmod_long_covid_inc')
    
    save_cols <- c(
      save_cols,
      "midmod_long_covid_prev",
      paste0("midmod_long_covid_prev_", estimation_years[2:length(estimation_years)])
    )
    
  } else if (df_name == 'hospital') {
    
    save_cols <- c('year', 'location_id', 'draw', 'hospital_inc', 'icu_inc', 
                   'hsp_deaths', 'hospital_risk_num', 'hospital_long_covid_inc')
    
    save_cols <- c(
      save_cols,
      "hospital_long_covid_prev",
      paste0("hospital_long_covid_prev_", estimation_years[2:length(estimation_years)])
    )
    
  } else if (df_name == 'icu') {
    
    save_cols <- c('year', 'location_id', 'draw', 'icu_inc', 'icu_deaths', 
                   'icu_risk_num', 'icu_long_covid_inc')
    
    save_cols <- c(
      save_cols,
      "icu_long_covid_prev",
      paste0("icu_long_covid_prev_", estimation_years[2:length(estimation_years)])
    )
    
  }
  
  save <- save[, ..save_cols]
  
  out_dir <- file.path('FILEPATH', output_version, 'location_vetting_csvs_draws')
  
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  fwrite(save, file.path(out_dir, paste0(loc_id, df_name, '.csv')))
  rm(save)
}


.aggregate_severities <- function(df, long_seq) {
  #' Convenience function to add severities together for each outcome
  #' @param df [data.table]
  #' @param long_seq [vector]
  
  # Ensure cols for inc and prev exist
  if (any(c(paste0('midmod_', long_seq, '_inc'), paste0('midmod_', long_seq, '_prev'),
            paste0('hospital_', long_seq, '_inc'), paste0('hospital_', long_seq, '_prev'),
            paste0('icu_', long_seq, '_inc'), paste0('icu_', long_seq, '_prev')) %ni% names(df))) {
    stop(paste0('Supplied df missing one or more of the ',
                'following columns: ', 
                paste0(paste0('midmod_', long_seq, '_inc', collapse=', '), 
                       paste0('midmod_', long_seq, '_prev', collapse=', '),
                       paste0('hospital_', long_seq, '_inc', collapse=', '), 
                       paste0('hospital_', long_seq, '_prev', collapse=', '),
                       paste0('icu_', long_seq, '_inc', collapse=', '), 
                       paste0('icu_', long_seq, '_prev', collapse=', '), collapse=', '), '.'))
  }
  
  
  # For each outcome
  for (outcome in long_seq) {
    
    # Aggregate incidence
    df[, eval(paste0(outcome, '_inc')) := 
         get(paste0('midmod_', outcome, '_inc')) + 
         get(paste0('hospital_', outcome, '_inc')) + 
         get(paste0('icu_', outcome, '_inc'))]
    df[, eval(paste0('midmod_', outcome, '_inc')) := NULL]
    df[, eval(paste0('hospital_', outcome, '_inc')) := NULL]
    df[, eval(paste0('icu_', outcome, '_inc')) := NULL]
    
    
    # Aggregate prevalence
    df[, eval(paste0(outcome, '_prev')) := 
         get(paste0('midmod_', outcome, '_prev')) + 
         get(paste0('hospital_', outcome, '_prev')) + 
         get(paste0('icu_', outcome, '_prev'))]
    df[, eval(paste0('midmod_', outcome, '_prev')) := NULL]
    df[, eval(paste0('hospital_', outcome, '_prev')) := NULL]
    df[, eval(paste0('icu_', outcome, '_prev')) := NULL]
    
  }
  
  
  return(df)
}


.apply_rates <- function(df, long_seq) {
  #' Convenience function to divide incidence and prevalence by population
  #' @param df [data.table]
  #' @param long_seq [vector]
  
  # Ensure cols for long_seq exist
  if (any(c(paste0(long_seq, '_inc'), paste0(long_seq, '_prev')) %ni% names(df))) {
    stop(paste0('Supplied df missing one or more of the following columns: ',
                paste0(long_seq, '_inc', collapse=', '), ', ', 
                paste0(long_seq, '_prev', collapse=', '), '.'))
  }
  # Ensure col for population exists
  if ('population' %ni% names(df)) {
    stop(paste0('Supplied df missing column for: population.'))
  }
  
  
  # For each outcome
  for (outcome in long_seq) {
    
    # Divide incidence by population
    df[, eval(paste0(outcome, '_inc_rate')) := get(paste0(outcome, '_inc')) / population]
    
    # Cap incidence rate to 1
    df[get(paste0(outcome, '_inc_rate')) > 1, eval(paste0(outcome, '_inc_rate')) := 1]
    
    # Divide prevalence by population
    df[, eval(paste0(outcome, '_prev_rate')) := get(paste0(outcome, '_prev')) / population]
    
    # Cap prevalence rate to 1
    df[get(paste0(outcome, '_prev_rate')) > 1, eval(paste0(outcome, '_prev_rate')) := 1]
    
  }
  
  # Drop population
  df[, population := NULL]
  
  
  return(df)
}


.apply_dws <- function(df, long_seq) {
  #' Convenience function to calculate YLDs for each outcome
  #' @param df [data.table]
  #' @param long_seq [vector]
  
  # Ensure all prev_rate cols exist
  if (any(paste0(long_seq, '_prev_rate') %ni% names(df))) {
    stop(paste0('Supplied df missing columns for one or more of the following: ',
                paste0(long_seq, '_prev_rate', collapse=', '), '.'))
  }
  # Ensure all dw cols exist
  if (any(paste0(long_seq, '_dw') %ni% names(df))) {
    stop(paste0('Supplied df missing columns for one or more of the following: ',
                paste0(long_seq, '_dw', collapse=', '), '.'))
  }
  
  
  # For each outcome
  for (outcome in long_seq) {
    
    # Multiply the prev_rate by the disability weight
    df[, eval(paste0(outcome, '_YLD')) := 
         get(paste0(outcome, '_prev_rate')) * get(paste0(outcome, '_dw'))]
    
    
    # Remove the disability weight
    df[, eval(paste0(outcome, '_dw')) := NULL]
  }
  
  
  return(df)
}


.save_dataset_wrapper <- function(df, output_version, loc_id, loc_name, long_seq) {
  #' Wrapper function to loop over long_seq and save data for each outcome
  #' @param df [data.table]
  #' @param output_version [str]
  #' @param loc_id [int]
  #' @param loc_name [str]
  #' @param long_seq [vector]
  
  # Ensure cols for long_seq exist
  if (any(c(paste0(long_seq, '_inc'), paste0(long_seq, '_prev'),
            paste0(long_seq, '_inc_rate'), paste0(long_seq, '_prev_rate'),
            paste0(long_seq, '_YLD')) %ni% names(df))) {
    stop(paste0('Supplied df missing columns for one or more of the following: ',
                paste0(long_seq, '_inc', collapse=', '), ', ', 
                paste0(long_seq, '_prev', collapse=', '), ', ',
                paste0(long_seq, '_inc_rate', collapse=', '), ', ', 
                paste0(long_seq, '_prev_rate', collapse=', '), ', ',
                paste0(long_seq, '_YLD', collapse=', '), '.'))
  }
  
  
  # For each outcome
  for (outcome in long_seq) {
    
    # Setup output columns
    out_cols <- c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw',
                  paste0(outcome, '_inc'), paste0(outcome, '_prev'),
                  paste0(outcome, '_inc_rate'), paste0(outcome, '_prev_rate'),
                  paste0(outcome, '_YLD'))
    
    # Output data
    save_dataset(dt = df[, out_cols, with=F], 
                 filename = outcome, stage = 'stage_2', 
                 output_version = output_version, loc_id = loc_id, 
                 loc_name = loc_name)
    
  }
}



main <- function(loc_id, output_version, definition, estimation_years, 
                 age_groups, location_set_id, release_id) {
  
  ## Base setup for later ----------------------------------------------------
  loc_meta <- get_location_metadata(location_set_id = location_set_id, 
                                    release_id = release_id)
  # Pull location ascii name
  loc_name <- loc_meta[location_id == loc_id, location_ascii_name]
  
  long_seq <- names(roots$me_ids$long_term)
  
  short_seq <- names(roots$me_ids$short_term)
  
  # References for later
  .measures <- c('_inc', '_prev', paste0('_prev_', estimation_years[2:length(estimation_years)]))
  
  # Set up column names to calculate long covid symptom clusters
  refs <- list(
    "measures" = .measures,
    # Columns to calculate
    "short_calc_cols" = unlist(
      lapply(short_seq, function(x) paste0(x, .measures))
    ),
    "long_calc_cols" = unlist(
      lapply(long_seq, function(x) paste0(x, .measures))
    )
  )
  
  rm(.measures)
  
  
  # Make temporary output directory
  temp_dir <- file.path(roots$mnt, "FILEPATH", output_version, "temp_output", 
                        paste0(loc_name, "_", loc_id))
  .ensure_dir(temp_dir)
  
  daily_dir <- file.path(roots$mnt, "pub/daily_outputs", output_version)
  .ensure_dir(daily_dir)
  ## --------------------------------------------------------------------- ----
  
  
  cat('Reading in durations and proportions...\n')
  ## Read in durations and proportions ----------------------------------- ----
  
  # Step 1
  
  # Durations
  # duration = duration for the year
  # dur1 = 1 year after
  # dur2 = 2 year after
  # dur3 = 3 year after
  # date = date of infection
  durs <- lapply(c('midmod', 'hosp', 'icu'), function(file){
    
    d <- fread(paste0('FILEPATH', 
                      'duration_draws_', file, '_clean_by_day.csv'))[, !c("V1")]
    
    # Dynamically identify all of the "dur*#" columns
    dur_cols_seq <- seq(length(estimation_years) - 1)
    dur_cols <- paste0("dur", dur_cols_seq) # dur1, dur2, dur3,etc
    dur_who_cols <- paste0("dur_who", dur_cols_seq) # dur_who1, dur_who2, dur_who3, etc
    dur_who_hosp_cols <- paste0("dur_who_hosp", dur_cols_seq) # dur_who_hosp1, dur_who_hosp2, dur_who_hosp3, etc
    dur_who_icu_cols <- paste0("dur_who_icu", dur_cols_seq) # dur_who_icu1, dur_who_icu2, dur_who_icu3, etc
    
    # Duration cols depend on index timing of long covid cases
    # gbd index = immediately following acute episode
    # WHO index = 3 months after infection, only relevant for publications/media
    # 12mo index = 12 months after infection, only relevant for publications/media
    if (definition == "who" | definition == "12mo") {
      
      remove_cols <- c("duration", dur_cols)
      
      d[, (remove_cols) := NULL]
      
      # Set duration columns depending on file
      if (file == 'midmod') {
        setnames(d,
                 c("duration_who_comm", dur_who_cols),
                 c("duration", dur_cols)
        )
      } else if(file == 'hosp') {
        setnames(d,
                 c("duration_who_hosp", dur_who_hosp_cols),
                 c("duration", dur_cols)
        )
      } else if (file == 'icu') {
        setnames(d,
                 c("duration_who_icu", dur_who_icu_cols),
                 c("duration", dur_cols)
        )
      }
      
    } else {
      
      # Remove columns that exist in the dataframe
      remove_cols <- c('duration_who_comm', dur_who_cols, 
                       'duration_who_hosp', dur_who_hosp_cols, 
                       'duration_who_icu', dur_who_icu_cols)
      
      remove_cols <- names(d)[names(d) %in% remove_cols]
      d[,  (remove_cols) := NULL]
    }
    
    # Generate series of dates for each estimation year
    d <- lapply(estimation_years, function (yr) {
      
      dur_year <- copy(d)
      
      # Cap duration to 0 if negative
      dur_year[duration < 0, duration := 0]
      
      # Convert day number to date
      # Start with January 1 of the given year
      start_date <- as.Date(paste0(yr, "-01-01"))
      
      # Generate date by adding day number to start date where 0 is the first day
      # of the year
      dur_year[, date := start_date + day]
      
      # Filter to only the year of interest, i.e. only apply leap day to leap years
      dur_year <- dur_year[lubridate::year(date) == yr]
      
    }) %>% 
      rbindlist()
    
  }) %>% 
    rbindlist()
  
  durs[, day := NULL]
  durs[, outcome := NULL]
  setnames(durs, 'duration', 'fat_or_resp_or_cog')
  
  # "any long_covid" proportions
  lc_cols <- c("follow_up_days", "hospital", "icu", "female", "children", "outcome",
               "variable", "proportion")
  
  any_lc_props <- fread(
    "FILEPATH/final_proportion_draws.csv"
  )[, ..lc_cols]
  
  any_lc_props[is.na(proportion), proportion := 0]
  
  any_lc_props_any <- fread(
    'FILEPATH/final_proportion_draws_any.csv'
  )[, ..lc_cols]
  
  # now outcome = 'value_any_main' is the proportion any long COVID
  any_lc_props <- rbind(any_lc_props, any_lc_props_any)
  rm(any_lc_props_any)
  
  # Combine and continue formatting
  any_lc_props[, `:=`(sex_id = ifelse(any_lc_props$female == 0, 1, 2), 
                      female = NULL)]
  setnames(any_lc_props, 'proportion', 'value')
  any_lc_props[follow_up_days %in% c(53, 60, 81), follow_up_days := 3]
  any_lc_props[follow_up_days %in% c((365-53), (365-60), (365-81)), follow_up_days := 12]
  
  if (definition == "who") {
    any_lc_props <- any_lc_props[follow_up_days == 3]
    any_lc_props$follow_up_days <- NULL
  } else if (definition == "12mo") {
    any_lc_props <- any_lc_props[follow_up_days == 12]
    any_lc_props$follow_up_days <- NULL
  } else if (definition == "gbd") {
    any_lc_props <- any_lc_props[follow_up_days == 0]
    any_lc_props$follow_up_days <- NULL
  } else {
    print("argument definition needs to be who or 12mo or gbd")
  }
  
  any_lc_props <- dcast(any_lc_props, 
                        formula = 'hospital + icu + sex_id + children + variable ~ outcome', 
                        value.var = 'value')
  
  if (definition == "gbd") {
    any_lc_props <- any_lc_props[, c('hospital', 'icu', 'sex_id', 'children', 'variable', 'c_mild', 'c_mod', 'f', 
                                     'r_mild', 'r_mod', 'r_sev', 'fc_mild', 'fc_mod', 'cr_mild_mild', 'cr_mild_mod', 
                                     'cr_mild_sev', 'cr_mod_mild','cr_mod_mod', 'cr_mod_sev', 
                                     'fr_mild', 'fr_mod', 'fr_sev', 'fcr_mild_mild', 'fcr_mild_mod', 
                                     'fcr_mild_sev', 'fcr_mod_mild', 'fcr_mod_mod', 'fcr_mod_sev', 'value_any_main')]
    setnames(any_lc_props, 
             c('variable', 'c_mild', 'c_mod', 'f', 'r_mild', 'r_mod', 'r_sev', 'fc_mild', 
               'fc_mod', 'cr_mild_mild', 'cr_mild_mod', 'cr_mild_sev', 'cr_mod_mild',
               'cr_mod_mod', 'cr_mod_sev', 'fr_mild', 'fr_mod', 'fr_sev', 'fcr_mild_mild',
               'fcr_mild_mod', 'fcr_mild_sev', 'fcr_mod_mild', 'fcr_mod_mod', 'fcr_mod_sev', 'value_any_main'),
             c('draw', long_seq, 'any'))
  } else {
    any_lc_props <- any_lc_props[, c('hospital', 'icu', 'sex_id', 'children', 'variable', 'c_mild', 'c_mod', 'f', 
                                     'r_mild', 'r_mod', 'r_sev', 'fc_mild', 'fc_mod', 'cr_mild_mild', 'cr_mild_mod', 
                                     'cr_mild_sev', 'cr_mod_mild','cr_mod_mod', 'cr_mod_sev', 
                                     'fr_mild', 'fr_mod', 'fr_sev', 'fcr_mild_mild', 'fcr_mild_mod', 
                                     'fcr_mild_sev', 'fcr_mod_mild', 'fcr_mod_mod', 'fcr_mod_sev', 'value_any_main')]
    setnames(any_lc_props, 
             c('variable', 'c_mild', 'c_mod', 'f', 'r_mild', 'r_mod', 'r_sev', 'fc_mild', 
               'fc_mod', 'cr_mild_mild', 'cr_mild_mod', 'cr_mild_sev', 'cr_mod_mild',
               'cr_mod_mod', 'cr_mod_sev', 'fr_mild', 'fr_mod', 'fr_sev', 'fcr_mild_mild',
               'fcr_mild_mod', 'fcr_mild_sev', 'fcr_mod_mild', 'fcr_mod_mod', 'fcr_mod_sev', 'value_any_main'),
             c('draw', long_seq, 'any'))
  }
  
  any_lc_props <- any_lc_props[, c('sex_id', 'children', 'hospital', 'icu', 'draw', long_seq, 'any'), with = F]
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Reading in short-term outputs...\n')
  ## Read in short-term datasets ----------------------------------------- ----
  
  # Step 2
  covid_counts <- fread(
    file.path(
      get_core_ref('data_output', 'stage_1'), 
      output_version,
      'stage_1',
      "_for_long_covid",
      paste0(loc_name, "_", loc_id, ".csv")
    )
  )
  
  # Distribute quantities across month within each year 
  # For each year create 12 months then divide the quantity by 12
  covid_counts <- covid_counts %>% 
    split(.$year_id) %>%
    lapply(function(df) {
      
      # Expand by month
      df <- expand_grid(df, month_id = 1:12)
      setDT(df)
      df[, date := as.IDate(paste0(year_id, '-', month_id, '-15'))]
      df[, month_id := NULL]
      df[, year_id := NULL]
      
      # Divide by 12
      df[, c('midmod_inc', 'hospital_inc', 'hsp_deaths', 'icu_inc', 'icu_deaths') := 
           lapply(.SD, function(x) x/12), 
         .SDcols = c('midmod_inc', 'hospital_inc', 'hsp_deaths', 'icu_inc', 'icu_deaths')]
      
    }) %>% 
    rbindlist()
  
  cat('Calculating mild/moderate incidence & prevalence...\n')
  ## Mild/Moderate Incidence and Prevalence ------------------------------ ----
  
  # Step 3.c
  # mild/moderate at risk number = (mild/moderate incidence - hospital admissions|7 days later) |
  #                                 shift forward by {incubation period + mild/moderate duration|no hospital}
  covid_counts[is.na(hospital_inc), hospital_inc := 0]
  covid_counts[is.na(midmod_inc), midmod_inc := 0]
  covid_counts[, midmod_risk_num := midmod_inc - hospital_inc]
  covid_counts[midmod_inc == 0, midmod_risk_num := 0]
  covid_counts[midmod_risk_num < 0, midmod_risk_num := 0]
  
  tictoc::tic(msg="merging kids and adults")
  
  # Step 3.d
  
  midmod <- covid_counts[, .(location_id, age_group_id, sex_id, draw, date, 
                             midmod_risk_num, 
                             infections, 
                             midmod_inc, 
                             hospital_inc)]
  
  # By age_group_id merge appropriate proportion
  midmod <- midmod %>%
    split(.$age_group_id) %>%
    lapply(function(df) {
      
      this_age_group_id <- unique(df$age_group_id)
      kids_age_group_ids <- c(2, 3, 388, 389, 238, 34, 6, 7, 8)
      adults_age_group_ids <- c(9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)
      
      # Identify age group specific proportions to merge
      if (this_age_group_id %in% kids_age_group_ids) {
        props <- any_lc_props[hospital == 0 & icu == 0 & children == 1, !c("hospital", "icu", "children")]
      } else if (this_age_group_id %in% adults_age_group_ids) {
        props <- any_lc_props[hospital == 0 & icu == 0 & children == 0, !c("hospital", "icu", "children")]
      } else {
        stop("Unaccounted age_group_id in midmod calculation. Please inspect.\nProblematic age_group_id: ", this_age_group_id)
      }
      
      df <- merge(df, props, by = c("sex_id", "draw"), all.x = T)
      
      return(df)
    }) %>%
    rbindlist()
  
  # Adjust risk of long COVID by changing risk over calendar time and variant
  waning_risk <- fread(
    "FILEPATH/final_beta_calendar_risk_adjustment.csv"
  )
  waning_risk[, draw := paste0("draw_", draw)]
  
  # Merge calendar time risk adjustment draws
  midmod <- merge(midmod, waning_risk, by = c("draw"), all.x = T)
  midmod[, months_since_01012020 := (date - as.IDate('2020-06-30')) / (365/12)]
  #  fwrite(midmod[age_group_id == 15 & sex_id == 2], "FILEPATH/TEST_final_beta_calendar_risk_adjustment.csv")
  midmod_orig <- copy(midmod)
  midmod <- copy(midmod_orig)
  midmod[, c(long_seq, "any") := lapply(.SD, function(x) (log(x / (1 - x)) + (beta_months_since_01012020 * months_since_01012020))),
         .SDcols = c(long_seq, "any")]
  midmod[, c(long_seq, "any") := lapply(.SD, function(x) (exp(x) / (1 + exp(x)))),
         .SDcols = c(long_seq, "any")]
  #  fwrite(midmod[age_group_id == 15 & sex_id == 2], "FILEPATH/TEST2_final_beta_calendar_risk_adjustment.csv")
  
  
  # Step 3.e
  # mild/moderate long_term incidence = mild/moderate at risk * proportion outcome among long covid
  tictoc::tic(msg="apply proportions")
  midmod <- .apply_proportions(midmod, 'midmod', c(long_seq, 'any'), definition)
  tictoc::toc()
  
  # Step 3.f
  # Merge on midmod durations
  midmod <- merge(midmod, durs[population=='midmod', !c('population')], 
                  by=c('date', 'draw'), all.x=T)
  
  # Step 3.g
  # mild/moderate long-term prevalence = mild/moderate long-term incidence * duration
  tictoc::tic(msg = "apply durations")
  #  midmod_orig <- copy(midmod)
  #  midmod <- copy(midmod_orig)
  midmod <- .apply_durations(midmod, 'midmod', c(long_seq, 'any'), estimation_years)
  tictoc::toc()

  # cumulative cases for annual vetting
  tictoc::tic(msg="saving rolling prev")
  .save_rolling_prev(midmod, 'midmod', c(long_seq, 'any'), refs, estimation_years)
  tictoc::toc()
  
  midmod_any <- midmod[, c('sex_id', 'draw', 'location_id', 'age_group_id', 
                           'date', 'midmod_risk_num', 'midmod_any_inc', 'midmod_any_prev')]
  
  midmod[, c('midmod_any_inc', 'midmod_any_prev') := NULL]

  cat('Calculating severe incidence and prevalence...\n')
  ## Severe Incidence and Prevalence ------------------------------------- ----
  
  # Read in hospital / icu data
  tictoc::tic(msg="reading in hospital/icu data")
  hospital <- covid_counts[, .(location_id, age_group_id, sex_id, draw, date, 
                               hospital_inc,
                               icu_inc,
                               hsp_deaths)]
  
  
  # Step 4.d
  # severe at risk number = (hospital admissions - ICU admissions|3 days later - hospital deaths|6 days later) |
  #                          shift forward by {hospital duration if no ICU no death + hospital mild moderate duration after discharge}
  hospital[, hospital_risk_num := hospital_inc - icu_inc - hsp_deaths]
  hospital[hospital_risk_num < 0, hospital_risk_num := 0]
  
  # hospital[, date := date + roots$defaults$hsp_no_icu_no_death_duration + 
  #            roots$defaults$hsp_midmod_after_discharge_duration]
  
  # Step 4.e
  # By age_group_id merge appropriate proportion
  hospital <- hospital %>%
    split(.$age_group_id) %>%
    lapply(function(df) {
      
      this_age_group_id <- unique(df$age_group_id)
      kids_age_group_ids <- c(2, 3, 388, 389, 238, 34, 6, 7, 8)
      adults_age_group_ids <- c(9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)
      
      # Identify age group specific proportions to merge
      if (this_age_group_id %in% kids_age_group_ids) {
        props <- any_lc_props[hospital == 1 & icu == 0 & children == 1, !c("hospital", "icu", "children")]
      } else if (this_age_group_id %in% adults_age_group_ids) {
        props <- any_lc_props[hospital == 1 & icu == 0 & children == 0, !c("hospital", "icu", "children")]
      } else {
        stop("Unaccounted age_group_id in hospital calculation. Please inspect.\nProblematic age_group_id: ", this_age_group_id)
      }
      
      df <- merge(df, props, by = c("sex_id", "draw"), all.x = T)
      
      return(df)
    }) %>%
    rbindlist()
  
  # Adjust risk of long COVID by changing risk over calendar time
  # Merge calendar time risk adjustment draws
  hospital <- merge(hospital, waning_risk, by = c("draw"), all.x = T)
  hospital[, months_since_01012020 := (date - as.IDate('2020-06-30')) / (365/12)]
  #  fwrite(hospital[age_group_id == 15 & sex_id == 2], "FILEPATH/TEST_final_beta_calendar_risk_adjustment.csv")
  hospital[, c(long_seq, "any") := lapply(.SD, function(x) (log(x / (1 - x)) + (beta_months_since_01012020 * months_since_01012020))),
           .SDcols = c(long_seq, "any")]
  hospital[, c(long_seq, "any") := lapply(.SD, function(x) (exp(x) / (1 + exp(x)))),
           .SDcols = c(long_seq, "any")]
  #  fwrite(hospital[age_group_id == 15 & sex_id == 2], "FILEPATH/TEST2_final_beta_calendar_risk_adjustment.csv")
  
  
  # Step 4.f
  # hospital long_term incidence = hospital at risk * proportion outcome among long covid
  tictoc::tic(msg="apply proportions")
  hospital <- .apply_proportions(hospital, 'hospital', c(long_seq, 'any'), definition)
  tictoc::toc()
  
  # Step 4.g
  # Merge on hospital durations
  hospital <- merge(hospital, durs[population == "hosp", !c("population")],
                    by = c("date", "draw"), all.x = T
  )
  
  # 4.h
  # severe long-term prevalence = severe long-term incidence * duration
  tictoc::tic(msg="apply durations")
  hospital <- .apply_durations(hospital, 'hospital', c(long_seq, 'any'), estimation_years)
  tictoc::toc()
  
  
  # cumulative cases for annual vetting
  tictoc::tic(msg="save rolling prev")
  .save_rolling_prev(hospital, 'hospital', c(long_seq, 'any'), refs, estimation_years)
  tictoc::toc()
  
  hospital_any <- hospital[, c('sex_id', 'draw', 'location_id', 'age_group_id', 
                               'date', 'hospital_risk_num', 'hospital_any_inc', 'hospital_any_prev')]
  
  hospital[, c('hospital_any_inc', 'hospital_any_prev') := NULL]
  
  
  
  cat('Calculating critical incidence and prevalence...\n')
  ## Critical Incidence and Prevalence ----------------------------------- ----
  
  # Read in icu data
  tictoc::tic(msg="reading icu data")
  icu <- covid_counts[, .(location_id, age_group_id, sex_id, draw, date, 
                          icu_inc,
                          icu_deaths)]
  
  tictoc::toc()
  
  # Step 5.c
  # critical at risk number = (ICU admissions - ICU deaths|3 days later) |
  #                            shift forward by {ICU duration if no death + ICU mild moderate duration after discharge}
  icu[, icu_risk_num := icu_inc - icu_deaths]
  icu[icu_risk_num < 0, icu_risk_num := 0]
  
  # Step 5.d
  # By age_group_id merge appropriate proportion
  icu <- icu %>%
    split(.$age_group_id) %>%
    lapply(function(df) {
      
      this_age_group_id <- unique(df$age_group_id)
      kids_age_group_ids <- c(2, 3, 388, 389, 238, 34, 6, 7, 8)
      adults_age_group_ids <- c(9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)
      
      # Identify age group specific proportions to merge
      if (this_age_group_id %in% kids_age_group_ids) {
        props <- any_lc_props[hospital==0 & icu==1 & children==1, !c('hospital', 'icu', 'children')]
      } else if (this_age_group_id %in% adults_age_group_ids) {
        props <- any_lc_props[hospital==0 & icu==1 & children==0, !c('hospital', 'icu', 'children')]
      } else {
        stop("Unaccounted age_group_id in hospital calculation. Please inspect.\nProblematic age_group_id: ", this_age_group_id)
      }
      
      df <- merge(df, props, by = c("sex_id", "draw"), all.x = T)
      
      return(df)
    }) %>%
    rbindlist()
  
  # Adjust risk of long COVID by changing risk over calendar time
  # Merge calendar time risk adjustment draws
  icu <- merge(icu, waning_risk, by = c("draw"), all.x = T)
  icu[, months_since_01012020 := (date - as.IDate('2020-06-30')) / (365/12)]
  #  fwrite(icu[age_group_id == 15 & sex_id == 2], "FILEPATH/TEST_final_beta_calendar_risk_adjustment.csv")
  icu[, c(long_seq, "any") := lapply(.SD, function(x) (log(x / (1 - x)) + (beta_months_since_01012020 * months_since_01012020))),
      .SDcols = c(long_seq, "any")]
  icu[, c(long_seq, "any") := lapply(.SD, function(x) (exp(x) / (1 + exp(x)))),
      .SDcols = c(long_seq, "any")]
  #  fwrite(icu[age_group_id == 15 & sex_id == 2], "FILEPATH/TEST2_final_beta_calendar_risk_adjustment.csv")
  
  
  # Step 5.e
  # icu long_term incidence = icu at risk * proportion outcome among long covid
  tictoc::tic(msg="apply proportions")
  icu <- .apply_proportions(icu, 'icu', c(long_seq, 'any'), definition)
  tictoc::toc()
  
  # Step 5.f
  # Merge on icu durations
  icu <- merge(icu, durs[population == "icu", !c("population")],
               by = c("date", "draw"), all.x = T
  )
  
  # Step 5.g
  # critical long-term prevalence = critical long-term incidence * duration
  tictoc::tic(msg="apply durations")
  icu <- .apply_durations(icu, 'icu', c(long_seq, 'any'), estimation_years)
  tictoc::toc()
  
  # cumulative cases for annual vetting
  tictoc::tic(msg="save rolling prev")
  .save_rolling_prev(icu, 'icu', c(long_seq, 'any'), refs, estimation_years)
  tictoc::toc()
  
  icu_any <- icu[, c('sex_id', 'draw', 'location_id', 'age_group_id', 'date', 
                     'icu_risk_num', 'icu_any_inc', 'icu_any_prev')]
  
  # Remove unneeded cols
  icu[, `:=`(icu_inc = NULL, icu_deaths = NULL, icu_risk_num = NULL, 
             icu_any_inc = NULL, icu_any_prev = NULL)]
  
  cat('Aggregating by year...\n')
  ## Aggregate all by year ----------------------------------------------- ----
  
  # Step 6.a
  outcomes <- list(
    midmod = midmod,
    hospital = hospital,
    icu = icu,
    midmod_any = midmod_any,
    hospital_any = hospital_any,
    icu_any = icu_any
  )
  
  for (outcome in names(outcomes)) {
    
    df <- outcomes[[outcome]]
    
    tictoc::tic(msg=paste0("aggregating ", outcome))
    
    # Subset to estimation years
    tictoc::tic(msg="subsetting date")
    df <- subset_date(df, estimation_years)
    tictoc::toc()
    
    # Aggregate by year
    tictoc::tic(msg=paste0("prepping to save ", outcome))
    if (grepl('any', outcome)) {
      outcome2 <- substr(outcome, 1, nchar(outcome)-4)
      refs_any <- copy(refs)
      refs_any$measures <- "_inc"
      refs_any$short_calc_cols <- "midmod_risk_num"
      refs_any$long_calc_cols <- "any_inc"
      refs_any$long_calc_cols <- c("any_inc", "any_prev")
      df <- .prep_to_save(df, outcome2, 'any', refs_any, definition, estimation_years)
    } else {
      df <- .prep_to_save(df, outcome, long_seq, refs, definition, estimation_years)
    }
    
    tictoc::toc()
    
    if (!grepl('any', outcome)) {
      # Add rollover prevalence
      tictoc::tic(msg="adding rollover")
      df <- .add_rollover(df, outcome, long_seq, estimation_years)
      tictoc::toc()
    } else {
      df <- reshape(df, 
                    idvar = c('location_id', 'age_group_id', 'sex_id', 'draw'),
                    timevar = 'year_id', 
                    times = estimation_years, 
                    direction = 'long', 
                    varying = list(colnames(df)[grepl('prev', colnames(df))], 
                                   colnames(df)[grepl('inc', colnames(df))]),
                    v.names = c(paste0(outcome, '_prev_'), 
                                paste0(outcome, '_inc_')))
      setnames(df, 
               c(paste0(outcome, '_prev_'), paste0(outcome, '_inc_')), 
               c(paste0(outcome, '_prev'), paste0(outcome, '_inc'))
      )
    }
    
    # # Write out data
    # tictoc::tic(msg="writing file")
    # write_feather(df, paste0(temp_dir, outcome, ".feather"))
    # tictoc::toc()
    
    # Save changes to outcomes list
    outcomes[[outcome]] <- df
    
    tictoc::toc()
  }
  
  cat('Aggregating severities...\n')
  ## Aggregate severities ------------------------------------------------ ----
  
  # Step 7
  # Faceted by age: read in all 3 datasets, merge together, aggregate severities
  dt <- lapply(age_groups, function(age_id){
    
    # aggregate severities for any long COVID
    tictoc::tic(msg=paste0("aggregating age group ", age_id))
    
    midmod_agg <- outcomes[["midmod"]][age_group_id == age_id]
    hospital_agg <- outcomes[["hospital"]][age_group_id == age_id]
    
    dt <- merge(midmod_agg, hospital_agg,
                by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"),
                all.x = T
    )
    
    rm(midmod_agg, hospital_agg)
    
    icu_agg <- outcomes[["icu"]][age_group_id == age_id]
    
    dt <- merge(dt, icu_agg,
                by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw'),
                all=T)
    
    rm(icu_agg)
    
    dt <- .aggregate_severities(dt, long_seq)
    
    return(dt)
  }) %>% 
    rbindlist()
  
  
  # Read in midmod_any and hospital_any
  tictoc::tic(msg="reading in midmod_any and hospital_any and icu_any data")
  midmod_any <- outcomes[["midmod_any"]]
  hospital_any <- outcomes[["hospital_any"]]
  icu_any <- outcomes[["icu_any"]]
  tictoc::toc()
  
  
  # Merge and delete datasets
  tictoc::tic(msg="merging midmod_any and hospital_any")
  dt_any <- merge(midmod_any, hospital_any,
                  by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw'),
                  all=T)
  tictoc::toc()
  #  rm(midmod_any, hospital_any)
  
  tictoc::tic(msg="merging on icu_any data")
  dt_any <- merge(dt_any, icu_any,
                  by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw'),
                  all=T)
  tictoc::toc()
  #  rm(icu_any)
  
  
  # Aggregate severities
  tictoc::tic(msg="aggregating severities")
  dt_any <- .aggregate_severities(dt_any, 'any')
  # Merge and delete datasets
  tictoc::tic(msg="merging midmod_any and hospital_any")
  dt_any <- merge(dt_any, midmod_any,
                  by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw'),
                  all=T)
  rm(midmod_any)
  tictoc::toc()
  #  rm(midmod_any, hospital_any)
  tictoc::tic(msg="merging midmod_any and hospital_any")
  dt_any <- merge(dt_any, hospital_any,
                  by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw'),
                  all=T)
  tictoc::toc()
  rm(hospital_any)
  
  tictoc::tic(msg="merging on icu_any data")
  dt_any <- merge(dt_any, icu_any,
                  by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw'),
                  all=T)
  tictoc::toc()
  rm(icu_any)
  tictoc::toc()
  
  cat('Calculating rates for any long COVID...\n')
  ## Calculate rates ----------------------------------------------------- ----
  
  # Step 8
  
  # Pull population
  pop <- get_population(age_group_id = age_groups,
                        single_year_age = FALSE, 
                        location_id = loc_id, 
                        location_set_id = location_set_id, 
                        year_id = unique(dt$year_id), 
                        sex_id = c(1, 2), 
                        release_id = release_id
  )[,c('location_id','year_id','sex_id','population','age_group_id')]
  
  # for FHS runs, we need to use their population numbers because get_population() does not have years beyond GBD estimation years
  if (grepl('fhs', output_version)) {
    pop_fhs <- data.table(read.csv('FILEPATH/summary.csv'))
    pop_fhs <- pop_fhs[scenario == 0]
    pop_fhs[, c('X', 'lower', 'upper', 'scenario') := NULL]
    setnames(pop_fhs, 'mean', 'population')
    # aggregate COVID estimates into FHS age groups and update 'age_groups'
    pop <- copy(pop_fhs)
    rm(pop_fhs)
    
    #    34 = 2-4
    #    238 = 12-23 months
    #    388 = 1-5 months
    #    389 = 6-11 months
    #    4 = 1-11 months (aggregate 388 and 389)
    #    5 = 1-4 (aggregate 238 and 34)
    dt_any[age_group_id %in% c(388, 389), age_group_id := 4]
    dt_any[age_group_id %in% c(238, 34), age_group_id := 5]
    dt_any <- dt_any[,.(sum(any_prev), sum(any_inc), sum(midmod_any_prev), sum(midmod_any_inc), sum(hospital_any_prev),
                        sum(hospital_any_inc), sum(icu_any_prev), sum(icu_any_inc)), 
                     by = c('location_id', 'year_id', 'sex_id', 'age_group_id', 'draw')]
    setnames(dt_any, c('V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8'), c('any_prev', 'any_inc', 'midmod_any_prev', 'midmod_any_inc',
                                                                          'hospital_any_prev', 'hospital_any_inc', 'icu_any_prev', 'icu_any_inc'))
    
    age_groups <- unique(dt_any$age_group_id)
  }
  
  
  # Merge population and data
  dt_any <- merge(dt_any, pop, by=c('location_id', 'year_id', 'age_group_id', 'sex_id'), all.x=T)
  
  # Calculate rates
  tictoc::tic(msg="applying rates to any long COVID")
  dt_any <- .apply_rates(dt_any, c('any', 'midmod_any', 'hospital_any', 'icu_any'))
  tictoc::toc()
  
  cat('\nSaving datasets and running diagnostics...\n')
  ## Save to intermediate location --------------------------------------- ----
  
  # Step 10
  
  tictoc::tic(msg="saving final data for any long COVID")
  dt_any$any_YLD <- 0
  dt_any$midmod_any_YLD <- 0
  dt_any$hospital_any_YLD <- 0
  dt_any$icu_any_YLD <- 0
  .save_dataset_wrapper(dt_any, output_version, loc_id, loc_name, c('any', 'midmod_any', 'hospital_any', 'icu_any'))
  tictoc::toc()
  
  cat('Calculating rates...\n')
  ## Calculate rates ----------------------------------------------------- ----
  
  # Step 8
  
  # for FHS runs, we need to use their population numbers because get_population() does not have years beyond GBD estimation years
  if (grepl('fhs', output_version)) {
    pop_fhs <- data.table(read.csv('FILEPATH/summary.csv'))
    pop_fhs <- pop_fhs[scenario == 0]
    pop_fhs[, c('X', 'lower', 'upper', 'scenario') := NULL]
    setnames(pop_fhs, 'mean', 'population')
    # aggregate COVID estimates into FHS age groups and update 'age_groups'
    pop <- copy(pop_fhs)
    rm(pop_fhs)
    
    #    34 = 2-4
    #    238 = 12-23 months
    #    388 = 1-5 months
    #    389 = 6-11 months
    #    4 = 1-11 months (aggregate 388 and 389)
    #    5 = 1-4 (aggregate 238 and 34)
    dt[age_group_id %in% c(388, 389), age_group_id := 4]
    dt[age_group_id %in% c(238, 34), age_group_id := 5]
    dt <- dt[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw'),
             .SDcols=colnames(dt[, !c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw')])]
    
    age_groups <- unique(dt$age_group_id)
  }
  
  
  # Merge population and data
  dt <- merge(dt, pop, by=c('location_id', 'year_id', 'age_group_id', 'sex_id'), all.x=T)
  rm(pop)
  
  # Calculate rates
  tictoc::tic(msg="applying rates")
  dt <- .apply_rates(dt, long_seq)
  tictoc::toc()
  
  cat('Calculating YLDs...\n')
  ## Calculate YLDs ------------------------------------------------------ ----
  
  # Step 9
  
  # Read in disability weights
  DW <- fread(file.path(roots$disability_weight, 'dw.csv'))
  DW <- DW[hhseqid %in% c(roots$hhseq_ids$long_cognitive, 
                          roots$hhseq_ids$long_fatigue, 
                          roots$hhseq_ids$long_respiratory)]
  
  # Reshape and properly name existing severities
  DW <- melt(DW, measure.vars = roots$draws, value.name = 'dw', variable.name = 'draw')
  DW <- dcast(DW, formula = 'draw ~ healthstate', value.var = 'dw')
  
  colnames(DW) <- c('draw', 'respiratory_mild_dw', 'respiratory_moderate_dw', 
                    'respiratory_severe_dw', 'cognitive_mild_dw', 'cognitive_severe_dw',
                    'fatigue_dw')
  
  # Calculate overlapping severities
  DW[, `:=`(cognitive_mild_fatigue_dw = (1 - ((1 - cognitive_mild_dw) * (1 - fatigue_dw))), 
            cognitive_severe_fatigue_dw = (1 - ((1 - cognitive_severe_dw) * (1 - fatigue_dw))), 
            cognitive_mild_respiratory_mild_dw = (1 - ((1 - cognitive_mild_dw) * (1 - respiratory_mild_dw))), 
            cognitive_mild_respiratory_moderate_dw = (1 - ((1 - cognitive_mild_dw) * (1 - respiratory_moderate_dw))), 
            cognitive_mild_respiratory_severe_dw = (1 - ((1 - cognitive_mild_dw) * (1 - respiratory_severe_dw))), 
            cognitive_severe_respiratory_mild_dw = (1 - ((1 - cognitive_severe_dw) * (1 - respiratory_mild_dw))), 
            cognitive_severe_respiratory_moderate_dw = (1 - ((1 - cognitive_severe_dw) * (1 - respiratory_moderate_dw))), 
            cognitive_severe_respiratory_severe_dw = (1 - ((1 - cognitive_severe_dw) * (1 - respiratory_severe_dw))), 
            fatigue_respiratory_mild_dw = (1 - ((1 - fatigue_dw) * (1 - respiratory_mild_dw))), 
            fatigue_respiratory_moderate_dw = (1 - ((1 - fatigue_dw) * (1 - respiratory_moderate_dw))), 
            fatigue_respiratory_severe_dw = (1 - ((1 - fatigue_dw) * (1 - respiratory_severe_dw))), 
            cognitive_mild_fatigue_respiratory_mild_dw = (1 - ((1 - fatigue_dw) * (1 - cognitive_mild_dw) * (1 - respiratory_mild_dw))), 
            cognitive_mild_fatigue_respiratory_moderate_dw = (1 - ((1 - fatigue_dw) * (1 - cognitive_mild_dw) * (1 - respiratory_moderate_dw))), 
            cognitive_mild_fatigue_respiratory_severe_dw = (1 - ((1 - fatigue_dw) * (1 - cognitive_mild_dw) * (1 - respiratory_severe_dw))), 
            cognitive_severe_fatigue_respiratory_mild_dw = (1 - ((1 - fatigue_dw) * (1 - cognitive_severe_dw) * (1 - respiratory_mild_dw))), 
            cognitive_severe_fatigue_respiratory_moderate_dw = (1 - ((1 - fatigue_dw) * (1 - cognitive_severe_dw) * (1 - respiratory_moderate_dw))), 
            cognitive_severe_fatigue_respiratory_severe_dw = (1 - ((1 - fatigue_dw) * (1 - cognitive_severe_dw) * (1 - respiratory_severe_dw))))]
  
  # fwrite(DW, paste0('FILEPATH/DW.csv'), row.names = FALSE)
  # Merge and calculate all
  dt <- merge(dt, DW, by='draw', all.x=T)
  
  tictoc::tic(msg="applying disability weights")
  dt <- .apply_dws(dt, long_seq)
  tictoc::toc()
  
  
  # Save out mean DWs for reference
  DW <- DW[, lapply(.SD, mean), .SDcols=paste0(long_seq, '_dw')]
  DW <- melt(DW, measure.vars = paste0(long_seq, '_dw'),
             variable.name = 'sequelae', value.name = 'disability_weight',
             variable.factor = F)
  DW[, sequelae := gsub('_dw', '', sequelae)]
  .ensure_dir(paste0(get_core_ref('data_output', 'stage_2'), output_version, 
                     'FILEPATH', loc_name, '_', loc_id, '/'))
  fwrite(DW, paste0(get_core_ref('data_output', 'stage_2'), output_version, 
                    'FILEPATH', loc_name, '_', loc_id, 
                    '/applied_long_covid_disability_weights.csv'))
  
  
  rm(DW)
  ## --------------------------------------------------------------------- ----
  
  
  cat('\nSaving datasets and running diagnostics...\n')
  ## Save to intermediate location --------------------------------------- ----
  
  # Step 10
  
  tictoc::tic(msg="saving final data")
  .save_dataset_wrapper(dt, output_version, loc_id, loc_name, long_seq)
  tictoc::toc()
  
  
  # Step 10.a
  # Delete input files after use
  # unlink(paste0('FILEPATH', 
  #               output_version, 'FILEPATH', loc_name,
  #               '_', loc_id, '_midmod.feather'))
  # unlink(paste0('FILEPATH', 
  #               output_version, 'FILEPATH', loc_name,
  #               '_', loc_id, '_hsp_admit.feather'))
  # unlink(paste0('FILEPATH', 
  #               output_version, 'FILEPATH', loc_name,
  #               '_', loc_id, '_icu_admit.feather'))
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('\nSaving for EPI database...\n')
  ## Save for EPI database ----------------------------------------------- ----
  
  # Step 11
  
  locs <- loc_meta[, c(
    "location_id", "location_ascii_name",
    "region_id", "region_name", "super_region_name",
    "super_region_id", "most_detailed"
  )]
  
  i_base <- file.path(
    get_core_ref('data_output', 'stage_2'), 
    output_version, 
    'stage_2'
  )
  
  
  for (measure in c(long_seq, names(roots$me_ids$any))) {
    #  for (measure in c('any', 'midmod_any', 'hospital_any', 'icu_any')) {
    tictoc::tic(msg=paste0("Finalizing epi data for ", measure))
    
    # Finalize dataset
    tictoc::tic(msg="finalizing data")
    df <- .finalize_data(measure, i_base, loc_name, loc_id)
    tictoc::toc()
    
    
    # Save to final location
    tictoc::tic(msg="writing measure 5")
    save_epi_dataset(dt = df[measure_id == 5, !c('measure_id')],
                     stage = 'final', 
                     output_version = output_version,
                     me_name = measure, 
                     measure_id = 5, 
                     loc_id = loc_id, 
                     loc_name = loc_name, l = locs)
    tictoc::toc()
    
    
    tictoc::tic(msg="writing measure 6")
    save_epi_dataset(dt = df[measure_id == 6, !c('measure_id')],
                     stage = 'final', 
                     output_version = output_version,
                     me_name = measure, measure_id = 6, 
                     loc_id = loc_id, 
                     loc_name = loc_name, l = locs)
    tictoc::toc()
    
    tictoc::toc()
  }
  
  ## Last item - check total memory used --------------------------------- ----
  unlink(temp_dir, recursive = T, force = T)
  
  tot_size <<- tot_size + check_size(obj_list = ls()[ls() %ni% existing_vars], 
                                     env = environment(), print = F)
}

## --------------------------------------------------------------------- ----


## Run All ------------------------------------------------------------- ----

if (!interactive()){
  cat('Beginning execution of:\n')
  begin_time <- Sys.time()
  
  parser <- argparse::ArgumentParser()
  parser$add_argument(
    "--loc_id",
    type = "integer"
  )
  parser$add_argument(
    "--output_version",
    type = "character"
  )
  parser$add_argument(
    "--definition",
    type = "character"
  )
  parser$add_argument(
    "--estimation_years",
    type = "character"
  )
  parser$add_argument(
    "--age_groups",
    type = "character"
  )
  parser$add_argument(
    "--location_set_id",
    type = "integer"
  )
  parser$add_argument(
    "--release_id",
    type = "integer"
  )
  
  args <- parser$parse_args()
  print(args)
  list2env(args, envir = environment())
  
  estimation_years <- as.vector(as.numeric(unlist(strsplit(estimation_years, ","))))
  age_groups <- as.vector(as.numeric(unlist(strsplit(age_groups, ","))))
  
  # Pull location ascii name
  loc_name <- get_location_metadata(
    location_set_id = location_set_id,
    release_id = release_id
  )[location_id == loc_id, location_ascii_name]
  
  
  # Print out args
  cat(paste0('\tloc_id: ', loc_id, '\n\tloc_name: ', loc_name, 
             '\n\toutput_version: ', output_version, '\n'))
  
  
  cat('\n')
  existing_vars <- ls()
  tot_size <- check_size(existing_vars)
  # change definition to "gbd" for GBD estimates (which measure long COVID from end of acute phase)
  #    and to "who" for using WHO definition of long COVID as starting 3 months after
  #    symptom onset and to "12mo" for using 12 months after symptom onset
  #  definition <- "12mo"
  
  
  # Run Data Processing Functions
  main(
    loc_id = loc_id,
    output_version = output_version,
    definition = definition,
    estimation_years = estimation_years,
    age_groups = age_groups,
    location_set_id = location_set_id,
    release_id = release_id
  )
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  cat(paste0('Memory used: ', tot_size, ' GB\n'))
} else {
  cat('Beginning execution of:\n')
  begin_time <- Sys.time()
  
  # Command Line Arguments
  loc_id <- 33  # 33 # 43 # 140 #35507 #34
  # output_version <- "2022-10-17.01fhs"
  output_version <- '2024-10-17.01gbd'
  release_id <- 16
  location_set_id <- 35
  
  # change definition to "gbd" for GBD estimates (which measure long COVID from end of acute phase)
  #    and to "who" for using WHO definition of long COVID as starting 3 months after
  #    symptom onset and to "12mo" for using 12 months after symptom onset
  definition <- "gbd"
  submit <- 0
  
  existing_vars <- ls()
  tot_size <- check_size(existing_vars)
  
  estimation_years <- c(2020, 2021, 2022, 2023, 2024)
  source(paste0(roots$k, 'FILEPATH/get_age_metadata.R'))
  age_groups <- get_age_metadata(release_id = release_id)$age_group_id
  
  locations <- get_location_metadata(location_set_id = location_set_id, 
                                     release_id = release_id)
  
  main(
    loc_id = loc_id,
    output_version = output_version,
    definition = definition,
    estimation_years = estimation_years,
    age_groups = age_groups,
    location_set_id = location_set_id,
    release_id = release_id
  )
  
  #   if (submit == 1) {
  #     locs <- locations[locations$most_detailed==1, location_id]
  # #    locs <- locs[91:length(locs)]
  #     queue <- "long.q"
  #   }
  #   for (loc_id in locs) {
  #     # Pull location ascii name
  #     loc_name <- locations[location_id==loc_id, location_ascii_name]
  #     
  # # Print out args
  # #    cat(paste0('  loc_id: ', loc_id, '\n  loc_name: ', loc_name, 
  # #               '\n  output_version: ', output_version, '\n'))
  # 
  # #    cat('\n')
  #     # Run Data Processing Functions
  #     if (submit == 0) {
  #       main(loc_id, output_version, test, definition, hsp_icu_input_path, estimation_years, 
  #          age_groups, location_set_id, release_id)
  # #    } else if (!file.exists(paste0("FILEPATH", output_version, "FILEPATH", loc_name, "_", loc_id, "_5.csv"))) {
  #     } else {
  #       cat(loc_id)
  #       estimation_years <- "2020,2021,2022,2023"
  #       age_groups <- "2,3,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,34,235,238,388,389"
  #       args <- paste(loc_id, output_version, definition, hsp_icu_input_path, estimation_years, 
  #                     age_groups, location_set_id, release_id)
  #       qsub <- paste0('sbatch FILEPATH', loc_id, '_e.txt ',
  #                      '-o FILEPATH', loc_id, '_o.txt ',
  #                      '-J long_', loc_id, ' -p ', queue, ' -A proj_nfrqe ',
  #                      '-c 10 --mem=280G -t 24:00:00 ',
  #                      'FILEPATH/execRscript.sh ', 
  #                      '-i FILEPATH/latest.img ', 
  #                      '-s FILEPATH/6_long_covid.R ',
  #                      args)
  #       system(qsub)
  #       
  #       rm(loc_id, qsub)
  #     }
  #     
  #   }
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  cat(paste0('Memory used: ', tot_size, ' GB\n'))
}

## --------------------------------------------------------------------- ----
