## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 6_long_covid.R
## Description: Stream short-term outcomes into long-term outcomes.
##              Numbered comments correspond to documentation on this HUB page:
##              ADDRESS/6_long_covid.R
## Contributors: 
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))


.repo_base <- '../'
source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0('ROOT', 'FILEPATH/get_location_metadata.R'))
source(paste0('ROOT', 'FILEPATH/get_population.R'))

## --------------------------------------------------------------------- ----


## Data Processing Functions ------------------------------------------- ----

.apply_proportions <- function(df, df_name, long_seq, definition) {
  #' Convenience function to apply any_lc_proportions for each outcome
  #' @param df [data.table]
  #' @param df_name [str]
  #' @param long_seq [vector]
  
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
  
  
  if (definition == "gbd") {
    ## apply duration to GBS
    
    # adjust duration for end of year when some days spill over into the next year
    tictoc::tic(msg="make_date")
    df[, EOY := make_date(year, 12, 31)]
    df[, EOY := as.IDate(EOY)]
    tictoc::toc()
    
    tictoc::tic(msg="adjusting prevalence")
    df[, `:=`(gbs_annual = roots$defaults$gbs_dur, gbs_dur = roots$defaults$gbs_dur)]
    
    df[(EOY - date) < gbs_dur, gbs_annual := EOY - date]
    
    # insurance
    df[gbs_annual < 0, gbs_annual := 0]
    
    # calculate prev for the infection year
    df[, eval(paste0(df_name, '_gbs_prev')) := get(paste0(df_name, '_gbs_inc')) * gbs_annual]
    df[, gbs_annual := NULL]
    tictoc::toc()
    
    # Calculate prev = inc * dur for years past the infection year
    for (yr in estimation_years) {
      tictoc::tic(msg=paste0("gbs ", yr))
      EOYyr <- as.IDate(paste0(yr, '-12-31'))
      # initialize duration in future years
      df[, eval(paste0('gbs_dur', yr)) := 0]
      
      # assign rollover days to duration
      df[yr == (year + 1) & (EOY - date) < gbs_dur, eval(paste0('gbs_dur', yr)) := gbs_dur - as.integer(EOY - date)]
      
      # calculate prev in future years past infection year
      df[, eval(paste0(df_name, '_gbs_prev_', yr)) := get(paste0(df_name, '_gbs_inc')) * get(paste0('gbs_dur', yr))]
      
      df[, eval(paste0('gbs_dur', yr)) := NULL]
      tictoc::toc()
    }
    
    df[, `:=`(EOY = NULL, gbs_dur = NULL, year = NULL)]
  }
  
  return(df)
}


.prep_to_save <- function(df, df_name, long_seq, refs, definition, estimation_years) {
  df <- subset_date(df, estimation_years)
  df$infections <- NULL
  
  # separate inc and prev
  long_cols_inc <- refs$long_calc_cols[grepl('_inc', refs$long_calc_cols)]
  df_inc <- data.table(copy(data.frame(df)[c('date', 'draw_var', 'sex_id', 'location_id', 'age_group_id',
                                  paste0(df_name, '_', long_cols_inc))]))
  long_cols_prev <- refs$long_calc_cols[grepl('_prev', refs$long_calc_cols)]
  if (!grepl('any', long_seq)) {
    df_prev <- data.table(copy(data.frame(df)[c('date', 'draw_var', 'sex_id', 'location_id', 'age_group_id',
                                    paste0(df_name, '_', long_cols_prev))]))
  }
  
  # shift date of inc so that the incident cases dates are shifted based on the definition, but prevalence
  # already accounts for the lag and so doesn't need to be shifted
  shift <- 0
  if (definition == 'who') {
    if (df_name %in% c('midmod', 'midmod_any')) {
      shift <- 81
    } else if (df_name %in% c('hospital', 'hospital_any')) {
      shift <- 60
    } else if (df_name %in% c('icu', 'icu_any')) {
      shift <- 53
    }
  } else if (definition == '12mo') {
    if (df_name %in% c('midmod', 'midmod_any')) {
      shift <- 365 - 81
    } else if (df_name %in% c('hospital', 'hospital_any')) {
      shift <- 365 - 60
    } else if (df_name %in% c('icu', 'icu_any')) {
      shift <- 365 - 53
    }
  }
  max_date <- max(df_inc$date)
  df_inc[, date := date + shift]
  df_inc <- df_inc[date <= max_date]
  
  # merge back on prev to inc
  if (!grepl('any', long_seq)) {
    df <- merge(df_inc, df_prev, by = c('date', 'draw_var', 'sex_id', 'location_id', 'age_group_id'), all = TRUE)
  } else {
    df <- copy(df_inc)
  }
  df[, year_id := year(date)]
  for (v in long_cols_inc) {
    df[is.na(get(paste0(df_name, '_', v))), eval(paste0(df_name, '_', v)) := 0]
  }
  rm(df_inc, df_prev, long_cols_inc, long_cols_prev, shift)
  
  df_daily <- copy(df)
  df_daily <- df_daily[, lapply(.SD, mean, na.rm=T), by=c('location_id', 'year_id', 'date', 'age_group_id', 'sex_id'),
                       .SDcols=c(paste0(df_name, '_', refs$long_calc_cols))]
  write.csv(df_daily, paste0('FILEPATH/daily_agesex_', df_name, '_', loc_id, '.csv'))
  df_daily <- df_daily[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'year_id', 'date'),
                       .SDcols=c(paste0(df_name, '_', refs$long_calc_cols))]
  write.csv(df_daily, paste0('FILEPATH/daily_', df_name, '_', loc_id, '.csv'))
  rm(df_daily)
  
  df <- df[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'year_id', 'age_group_id', 
                                             'sex_id', 'draw_var'), 
           .SDcols=c(paste0(df_name, '_', refs$long_calc_cols))]
  
  if (long_seq == 'any') {
    df[, eval(paste0(df_name, '_any_prev')) := 0]
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
                                   by=c('location_id', 'draw_var', 'age_group_id', 'sex_id'), 
                                   .SDcols=c(colnames(df_rolled_over)[grepl('prev', colnames(df_rolled_over))])]
  
  
  for (var in c(paste0(df_name, '_', long_seq, '_prev_'))) {
    for (yr in estimation_years) {
      df[, eval(paste0(var, yr)) := NULL]
    }
  }
  # make df of onset-year estimates wide by year
  df <- dcast(df, formula = paste0('location_id + age_group_id + sex_id + draw_var ~ year_id'),
              value.var=c(paste0(df_name, '_', long_seq, '_prev'), paste0(df_name, '_', long_seq, '_inc')))
  
  if (long_seq != 'any') {
    for (var in paste0(df_name, '_', refs$long_calc_cols[grepl('_prev_', refs$long_calc_cols)])) {
      setnames(df_rolled_over, var, paste0('rolled_over_', var))
    }
    df <- merge(df, df_rolled_over, by = c('location_id', 'age_group_id', 'sex_id',
                                           'draw_var'))
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
  
  df <- reshape(df, idvar = c('location_id', 'age_group_id', 'sex_id', 'draw_var'),
                timevar = 'year_id', times = estimation_years, 
                direction = 'long', varying = c(colnames(df)[grepl('prev', colnames(df))], colnames(df)[grepl('inc', colnames(df))]),
                sep = '.')
  
  #  df <- .year_scale_prevalence(df, long_seq)
  
  return(df)
}


.save_rolling_prev <- function(df, df_name, long_seq, refs, estimation_years) {
  
  df[, year := year(date)]
  
  if (df_name == 'midmod') {
    cols_extra <- c('infections', 'midmod_inc', 'hospital_inc', 'deaths_comm', 'midmod_risk_num')
  } else if (df_name == 'hospital') {
    cols_extra <- c('hospital_inc', 'icu_inc', 'hsp_deaths', 'hospital_risk_num')
  } else if (df_name == 'icu') {
    cols_extra <- c('icu_inc', 'icu_deaths', 'icu_risk_num')
  }
  
  
  # Take mean of draws
  save <- subset_date(df, estimation_years)
  save <- save[, lapply(.SD, mean, na.rm=T), by=c('date', 'sex_id', 'location_id', 'age_group_id', 'year'), 
               .SDcols = c(paste0(df_name, '_', refs$long_calc_cols), cols_extra)]
  
  save <- subset_date(save, estimation_years)
  save <- save[, lapply(.SD, sum, na.rm=T), by=c('sex_id', 'location_id', 'age_group_id', 'year'), 
               .SDcols=c(paste0(df_name, '_', refs$long_calc_cols), cols_extra)]
  dir.create(paste0('FILEPATH'), recursive = TRUE)
  write.csv(save, paste0('FILEPATH/', loc_id, df_name, '.csv'))
  rm(save)
  
  # cumulative cases for 2020 vetting
  save <- subset_date(df, estimation_years)
  save <- save[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'draw_var', 'year'), 
               .SDcols=c(paste0(df_name, '_', refs$long_calc_cols), cols_extra)]
  for (yr in estimation_years[2:length(estimation_years)]) {
    save <- save[, eval(paste0(df_name, '_long_covid_prev_', yr)) := get(paste0(df_name, '_cognitive_severe_prev_', yr)) + get(paste0(df_name, '_cognitive_mild_prev_', yr)) + 
                   get(paste0(df_name, '_fatigue_prev_', yr)) + get(paste0(df_name, '_respiratory_mild_prev_', yr)) + get(paste0(df_name, '_respiratory_moderate_prev_', yr)) +
                   get(paste0(df_name, '_respiratory_severe_prev_', yr)) + get(paste0(df_name, '_cognitive_mild_fatigue_prev_', yr)) + get(paste0(df_name, '_cognitive_severe_fatigue_prev_', yr)) + get(paste0(df_name, '_cognitive_mild_respiratory_mild_prev_', yr)) +
                   get(paste0(df_name, '_cognitive_severe_respiratory_mild_prev_', yr)) + get(paste0(df_name, '_cognitive_mild_respiratory_moderate_prev_', yr)) + get(paste0(df_name, '_cognitive_severe_respiratory_moderate_prev_', yr)) +
                   get(paste0(df_name, '_cognitive_mild_respiratory_severe_prev_', yr)) + get(paste0(df_name, '_cognitive_severe_respiratory_severe_prev_', yr)) + get(paste0(df_name, '_fatigue_respiratory_mild_prev_', yr)) +
                   get(paste0(df_name, '_fatigue_respiratory_moderate_prev_', yr)) + get(paste0(df_name, '_fatigue_respiratory_severe_prev_', yr)) + get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_mild_prev_', yr)) +
                   get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_mild_prev_', yr)) + get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_moderate_prev_', yr)) + get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_moderate_prev_', yr)) +
                   get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_severe_prev_', yr)) + get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_severe_prev_', yr))]
    for (outcome in long_seq) {
      save[, eval(paste0(df_name, '_', outcome, '_prev_', yr)) := NULL]
    }
  }
  save <- save[, eval(paste0(df_name, '_long_covid_inc')) := get(paste0(df_name, '_cognitive_severe_inc')) + get(paste0(df_name, '_cognitive_mild_inc')) + 
                 get(paste0(df_name, '_fatigue_inc')) + get(paste0(df_name, '_respiratory_mild_inc')) + get(paste0(df_name, '_respiratory_moderate_inc')) +
                 get(paste0(df_name, '_respiratory_severe_inc')) + get(paste0(df_name, '_cognitive_mild_fatigue_inc')) + get(paste0(df_name, '_cognitive_severe_fatigue_inc')) + 
                 get(paste0(df_name, '_cognitive_mild_respiratory_mild_inc')) + get(paste0(df_name, '_cognitive_severe_respiratory_mild_inc')) + 
                 get(paste0(df_name, '_cognitive_mild_respiratory_moderate_inc')) + get(paste0(df_name, '_cognitive_severe_respiratory_moderate_inc')) +
                 get(paste0(df_name, '_cognitive_mild_respiratory_severe_inc')) + get(paste0(df_name, '_cognitive_severe_respiratory_severe_inc')) + 
                 get(paste0(df_name, '_fatigue_respiratory_mild_inc')) + get(paste0(df_name, '_fatigue_respiratory_moderate_inc')) + 
                 get(paste0(df_name, '_fatigue_respiratory_severe_inc')) + get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_mild_inc')) +
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_mild_inc')) + get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_moderate_inc')) + 
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_moderate_inc')) + get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_severe_inc')) + 
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_severe_inc'))]
  save <- save[, eval(paste0(df_name, '_long_covid_prev')) := get(paste0(df_name, '_cognitive_severe_prev')) + get(paste0(df_name, '_cognitive_mild_prev')) + 
                 get(paste0(df_name, '_fatigue_prev')) + get(paste0(df_name, '_respiratory_mild_prev')) + get(paste0(df_name, '_respiratory_moderate_prev')) +
                 get(paste0(df_name, '_respiratory_severe_prev')) + get(paste0(df_name, '_cognitive_mild_fatigue_prev')) + get(paste0(df_name, '_cognitive_severe_fatigue_prev')) + 
                 get(paste0(df_name, '_cognitive_mild_respiratory_mild_prev')) + get(paste0(df_name, '_cognitive_severe_respiratory_mild_prev')) + 
                 get(paste0(df_name, '_cognitive_mild_respiratory_moderate_prev')) + get(paste0(df_name, '_cognitive_severe_respiratory_moderate_prev')) +
                 get(paste0(df_name, '_cognitive_mild_respiratory_severe_prev')) + get(paste0(df_name, '_cognitive_severe_respiratory_severe_prev')) + 
                 get(paste0(df_name, '_fatigue_respiratory_mild_prev')) + get(paste0(df_name, '_fatigue_respiratory_moderate_prev')) + 
                 get(paste0(df_name, '_fatigue_respiratory_severe_prev')) + get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_mild_prev')) +
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_mild_prev')) + get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_moderate_prev')) + 
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_moderate_prev')) + get(paste0(df_name, '_cognitive_mild_fatigue_respiratory_severe_prev')) + 
                 get(paste0(df_name, '_cognitive_severe_fatigue_respiratory_severe_prev'))]
  # need to update the following line to not hardcode estimation years into the variable names
  if (definition == "gbd") {
    save_cols <- c(paste0(df_name, c("_risk_num", "_long_covid_inc", "_long_covid_prev", "_long_covid_prev_2021",
                                     "_long_covid_prev_2022", "_long_covid_prev_2023", "_gbs_inc", "_gbs_prev")))
  } else {
    save_cols <- c(paste0(df_name, c("_risk_num", "_long_covid_inc", "_long_covid_prev", "_long_covid_prev_2021",
                                     "_long_covid_prev_2022", "_long_covid_prev_2023")))
  }
  if (df_name == 'midmod') {
    if (max(estimation_years) == 2023) {
      save <- save[, c('year', 'location_id', 'draw_var', 'infections', 'midmod_inc', 'hospital_inc', 
                       'deaths_comm', 'midmod_risk_num', 'midmod_long_covid_inc', 'midmod_long_covid_prev', 
                       'midmod_long_covid_prev_2021', 'midmod_long_covid_prev_2022', 'midmod_long_covid_prev_2023')]
    } else {
      save <- save[, c('year', 'location_id', 'draw_var', 'infections', 'midmod_inc', 'hospital_inc', 
                       'deaths_comm', 'midmod_risk_num', 'midmod_long_covid_inc', 'midmod_long_covid_prev', 
                       'midmod_long_covid_prev_2021', 'midmod_long_covid_prev_2022')]
    }
  } else if (df_name == 'hospital') {
    cols_extra <- c('hospital_inc', 'icu_inc', 'hsp_deaths', 'hospital_risk_num')
    if (max(estimation_years) == 2023) {
      save <- save[, c('year', 'location_id', 'draw_var', 'hospital_inc', 'icu_inc', 'hsp_deaths', 'hospital_risk_num', 'hospital_long_covid_inc', 'hospital_long_covid_prev', 
                       'hospital_long_covid_prev_2021', 'hospital_long_covid_prev_2022', 'hospital_long_covid_prev_2023')]
    } else {
      save <- save[, c('year', 'location_id', 'draw_var', 'hospital_inc', 'icu_inc', 'hsp_deaths', 'hospital_risk_num', 'hospital_long_covid_inc', 'hospital_long_covid_prev', 
                       'hospital_long_covid_prev_2021', 'hospital_long_covid_prev_2022')]
    }
  } else if (df_name == 'icu') {
    cols_extra <- c('icu_inc', 'icu_deaths', 'icu_risk_num')
    if (max(estimation_years) == 2023) {
      save <- save[, c('year', 'location_id', 'draw_var', 'icu_inc', 'icu_deaths', 'icu_risk_num', 'icu_long_covid_inc', 'icu_long_covid_prev', 
                       'icu_long_covid_prev_2021', 'icu_long_covid_prev_2022', 'icu_long_covid_prev_2023')]
    } else {
      save <- save[, c('year', 'location_id', 'draw_var', 'icu_inc', 'icu_deaths', 'icu_risk_num', 'icu_long_covid_inc', 'icu_long_covid_prev', 
                       'icu_long_covid_prev_2021', 'icu_long_covid_prev_2022')]
    }
  }
  
  dir.create(paste0('FILEPATH'), recursive = TRUE)
  write.csv(save, paste0('FILEPATH', loc_id, df_name, '.csv'))
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
    
    # Divide prevalence by population
    df[, eval(paste0(outcome, '_prev_rate')) := get(paste0(outcome, '_prev')) / population]
    
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
    out_cols <- c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw_var',
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



main <- function(loc_id, output_version, test, definition, hsp_icu_input_path, estimation_years, 
  age_groups, location_set_id, release_id) {
  ## Base setup for later ------------------------------------------------ ----
  # Pull location ascii name
  loc_name <- as.character(get_location_metadata(location_set_id = location_set_id, 
                                                 release_id = release_id
  )[location_id==loc_id, 'location_ascii_name'])
  
  
  long_seq <- names(roots$me_ids)[6:(length(roots$me_ids)-4)]
  if (definition == "who" | definition == "12mo") {
    long_seq <- names(roots$me_ids)[6:(length(roots$me_ids)-5)]
  }
  # follow_up_days_keep <- ifelse((((roots$estimation_years - 1) %% 4) == 0), (roots$estimation_years-2020) * 366, (roots$estimation_years - 2020) * 365)
  
  # References for later
  .measures <- c('_inc', '_prev', paste0('_prev_', estimation_years[2:length(estimation_years)]))
  refs <- list('measures' = .measures,
               # Columns to calculate
               'short_calc_cols' = c(paste0('asymp', .measures),
                                     paste0('mild', .measures),
                                     paste0('moderate', .measures),
                                     paste0('hospital', .measures),
                                     paste0('icu', .measures)),
               'long_calc_cols' = c(paste0('cognitive_severe', .measures),
                                    paste0('cognitive_mild', .measures),
                                    paste0('fatigue', .measures),
                                    paste0('respiratory_mild', .measures),
                                    paste0('respiratory_moderate', .measures),
                                    paste0('respiratory_severe', .measures),
                                    paste0('cognitive_mild_fatigue', .measures),
                                    paste0('cognitive_severe_fatigue', .measures),
                                    paste0('cognitive_mild_respiratory_mild', .measures),
                                    paste0('cognitive_severe_respiratory_mild', .measures),
                                    paste0('cognitive_mild_respiratory_moderate', .measures),
                                    paste0('cognitive_severe_respiratory_moderate', .measures),
                                    paste0('cognitive_mild_respiratory_severe', .measures),
                                    paste0('cognitive_severe_respiratory_severe', .measures),
                                    paste0('fatigue_respiratory_mild', .measures),
                                    paste0('fatigue_respiratory_moderate', .measures),
                                    paste0('fatigue_respiratory_severe', .measures),
                                    paste0('cognitive_mild_fatigue_respiratory_mild', .measures),
                                    paste0('cognitive_severe_fatigue_respiratory_mild', .measures),
                                    paste0('cognitive_mild_fatigue_respiratory_moderate', .measures),
                                    paste0('cognitive_severe_fatigue_respiratory_moderate', .measures),
                                    paste0('cognitive_mild_fatigue_respiratory_severe', .measures),
                                    paste0('cognitive_severe_fatigue_respiratory_severe', .measures),
                                    paste0('gbs', .measures)))
  if (definition == "who" | definition == "12mo") {
    refs <- list('measures' = .measures,
                 # Columns to calculate
                 'short_calc_cols' = c(paste0('asymp', .measures),
                                       paste0('mild', .measures),
                                       paste0('moderate', .measures),
                                       paste0('hospital', .measures),
                                       paste0('icu', .measures)),
                 'long_calc_cols' = c(paste0('cognitive_severe', .measures),
                                      paste0('cognitive_mild', .measures),
                                      paste0('fatigue', .measures),
                                      paste0('respiratory_mild', .measures),
                                      paste0('respiratory_moderate', .measures),
                                      paste0('respiratory_severe', .measures),
                                      paste0('cognitive_mild_fatigue', .measures),
                                      paste0('cognitive_severe_fatigue', .measures),
                                      paste0('cognitive_mild_respiratory_mild', .measures),
                                      paste0('cognitive_severe_respiratory_mild', .measures),
                                      paste0('cognitive_mild_respiratory_moderate', .measures),
                                      paste0('cognitive_severe_respiratory_moderate', .measures),
                                      paste0('cognitive_mild_respiratory_severe', .measures),
                                      paste0('cognitive_severe_respiratory_severe', .measures),
                                      paste0('fatigue_respiratory_mild', .measures),
                                      paste0('fatigue_respiratory_moderate', .measures),
                                      paste0('fatigue_respiratory_severe', .measures),
                                      paste0('cognitive_mild_fatigue_respiratory_mild', .measures),
                                      paste0('cognitive_severe_fatigue_respiratory_mild', .measures),
                                      paste0('cognitive_mild_fatigue_respiratory_moderate', .measures),
                                      paste0('cognitive_severe_fatigue_respiratory_moderate', .measures),
                                      paste0('cognitive_mild_fatigue_respiratory_severe', .measures),
                                      paste0('cognitive_severe_fatigue_respiratory_severe', .measures)))
  }
  rm(.measures)
  
  # Make temporary output directory
  temp_dir <- paste0('FILEPATH')
  .ensure_dir(temp_dir)
  daily_dir <- paste0('FILEPATH')
  .ensure_dir(daily_dir)
  ## --------------------------------------------------------------------- ----
  
  
  cat('Reading in durations and proportions...\n')
  ## Read in durations and proportions ----------------------------------- ----
  
  # Step 1
  
  # Durations
  durs <- data.table()
  for (file in c('midmod', 'hosp', 'icu')) {
    # Read in and make col for population
    #    d <- fread('FILEPATH')
    #    )
    d <- fread(paste0('FILEPATH', 
                      'duration_draws_', file, '_clean_by_day.csv')
    )
    d$V1 <- NULL
    
    if (definition == "who" | definition == "12mo") {
      d[, c('duration', 'dur1', 'dur2', 'dur3') := NULL]
      setnames(d, c('duration_who_comm', 'dur_who1', 'dur_who2', 'dur_who3'), c('duration', 'dur1', 'dur2', 'dur3'), skip_absent = TRUE)
      setnames(d, c('duration_who_hosp', 'dur_who_hosp1', 'dur_who_hosp2', 'dur_who_hosp3'), c('duration', 'dur1', 'dur2', 'dur3'), skip_absent = TRUE)
      setnames(d, c('duration_who_icu', 'dur_who_icu1', 'dur_who_icu2', 'dur_who_icu3'), c('duration', 'dur1', 'dur2', 'dur3'), skip_absent = TRUE)
    } else {
      d[, c('duration_who_comm', 'duration_who_hosp', 'dur_who1', 'dur_who2', 'dur_who3', 'dur_who_hosp1', 'dur_who_hosp2', 'dur_who_hosp3', 'duration_who_icu', 'dur_who_icu1', 'dur_who_icu2', 'dur_who_icu3') := NULL]
    }
    
    d[, day := day+1]
    
    for (yr in roots$estimation_years) {
      anotheryear <- d[order(c(draw, day))]
      anotheryear <- anotheryear[!is.na(day)]
      if ((yr %% 4)!=0) {
        anotheryear <- anotheryear[day<366]
      } else {
        anotheryear <- anotheryear[day<367]
      }
      anotheryear[duration<0, duration := 0]
      anotheryear[, date := rep(seq(as.Date(paste0(yr, '-01-01')), 
                                    as.Date(paste0(yr, '-12-31')), by="days"), 1000)]
      anotheryear$day <- NULL
      durs <- rbind(durs, anotheryear)
      rm(yr, anotheryear)
    }
    
    rm(file, d)
  }
  setnames(durs, 'draw', 'draw_var')
  setnames(durs, 'duration', 'fat_or_resp_or_cog')
  durs$outcome <- NULL
  
  # "any long_covid" proportions
  #  any_lc_props <- fread(paste0('FILEPATH', 
  #                               'final_proportion_draws.csv')
  #  )[, c('follow_up_days', 'hospital', 'icu', 'female', 'children', 'outcome', 'variable', 'proportion')]
  any_lc_props <- fread(paste0('FILEPATH', 
                               'final_proportion_draws.csv')
  )[, c('follow_up_days', 'hospital', 'icu', 'female', 'children', 'outcome', 'variable', 'proportion')]
  any_lc_props[is.na(proportion), proportion := 0]
  #  any_lc_props_any <- fread(paste0('FILEPATH', 
  #                                   'final_proportion_draws_any.csv')
  #  )[, c('follow_up_days', 'hospital', 'icu', 'female', 'children', 'outcome', 'variable', 'proportion')]
  any_lc_props_any <- fread(paste0('FILEPATH', 
                                   'final_proportion_draws_any.csv')
  )[, c('follow_up_days', 'hospital', 'icu', 'female', 'children', 'outcome', 'variable', 'proportion')]
  
  
  # Guillain-Barré Syndrome proportions
  #  gbs_props <- fread(paste0('FILEPATH', 
  #                            'PRA_proportion.csv')
  #  )[, c('hospital', 'icu', roots$draws), with=F]
  gbs_props <- fread(paste0('FILEPATH', 
                            'PRA_proportion.csv')
  )[, c('hospital', 'icu', roots$draws), with=F]
  gbs_props[, `:=`(sex_id = 1, outcome = 'gbs')]
  temp <- copy(gbs_props)[, sex_id := 2]
  gbs_props <- rbind(gbs_props, temp) %>% melt(measure.vars = roots$draws)
  gbs_props$children <- 0
  temp <- copy(gbs_props)[, children := 1]
  gbs_props <- rbind(gbs_props, temp)
  rm(temp)
  
  # now outcome = 'value_any' is the proportion any long COVID
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
  
  if (definition == "gbd") {
    any_lc_props <- rbind(any_lc_props, gbs_props)
  }
  rm(gbs_props)
  
  any_lc_props <- dcast(any_lc_props, 
                        formula = 'hospital + icu + sex_id + children + variable ~ outcome', 
                        value.var = 'value')
  if (definition == "gbd") {
    any_lc_props <- any_lc_props[, c('hospital', 'icu', 'sex_id', 'children', 'variable', 'c_mild', 'c_mod', 'f', 
                                     'r_mild', 'r_mod', 'r_sev', 'fc_mild', 'fc_mod', 'cr_mild_mild', 'cr_mild_mod', 
                                     'cr_mild_sev', 'cr_mod_mild','cr_mod_mod', 'cr_mod_sev', 
                                     'fr_mild', 'fr_mod', 'fr_sev', 'fcr_mild_mild', 'fcr_mild_mod', 
                                     'fcr_mild_sev', 'fcr_mod_mild', 'fcr_mod_mod', 'fcr_mod_sev', 'gbs', 'value_any')]
    setnames(any_lc_props, 
             c('variable', 'c_mild', 'c_mod', 'f', 'r_mild', 'r_mod', 'r_sev', 'fc_mild', 
               'fc_mod', 'cr_mild_mild', 'cr_mild_mod', 'cr_mild_sev', 'cr_mod_mild',
               'cr_mod_mod', 'cr_mod_sev', 'fr_mild', 'fr_mod', 'fr_sev', 'fcr_mild_mild',
               'fcr_mild_mod', 'fcr_mild_sev', 'fcr_mod_mild', 'fcr_mod_mod', 'fcr_mod_sev', 'gbs', 'value_any'),
             c('draw_var', long_seq, 'any'))
  } else {
    any_lc_props <- any_lc_props[, c('hospital', 'icu', 'sex_id', 'children', 'variable', 'c_mild', 'c_mod', 'f', 
                                     'r_mild', 'r_mod', 'r_sev', 'fc_mild', 'fc_mod', 'cr_mild_mild', 'cr_mild_mod', 
                                     'cr_mild_sev', 'cr_mod_mild','cr_mod_mod', 'cr_mod_sev', 
                                     'fr_mild', 'fr_mod', 'fr_sev', 'fcr_mild_mild', 'fcr_mild_mod', 
                                     'fcr_mild_sev', 'fcr_mod_mild', 'fcr_mod_mod', 'fcr_mod_sev', 'value_any')]
    setnames(any_lc_props, 
             c('variable', 'c_mild', 'c_mod', 'f', 'r_mild', 'r_mod', 'r_sev', 'fc_mild', 
               'fc_mod', 'cr_mild_mild', 'cr_mild_mod', 'cr_mild_sev', 'cr_mod_mild',
               'cr_mod_mod', 'cr_mod_sev', 'fr_mild', 'fr_mod', 'fr_sev', 'fcr_mild_mild',
               'fcr_mild_mod', 'fcr_mild_sev', 'fcr_mod_mild', 'fcr_mod_mod', 'fcr_mod_sev', 'value_any'),
             c('draw_var', long_seq, 'any'))
  }
  any_lc_props <- any_lc_props[, c('sex_id', 'children', 'hospital', 'icu', 'draw_var', long_seq, 'any'), with = F]
  
  test2 <- 0
  if (test2 == 1) {
    cat('   565')
    check_size(ls(), print=TRUE)  
  }
  ## --------------------------------------------------------------------- ----
  
  
  cat('Reading in short-term outputs...\n')
  ## Read in short-term datasets ----------------------------------------- ----
  
  # Step 2
  
  # Mild/moderate
  cat('  mild/moderate... ')
  midmod <- read_feather(paste0('FILEPATH', loc_name,
                                '_', loc_id, '_midmod.feather'))
  midmod[, date := as.IDate(date)]
  
  if (test == 0) {
    midmod <- melt(midmod, measure.vars = roots$draws) %>% 
      setnames(c('variable', 'value'), c('draw_var', 'midmod_inc'))
  } else {
    midmod <- melt(midmod, measure.vars = roots$draws) %>% 
      setnames(c('variable', 'value'), c('draw_var', 'midmod_inc'))
    midmod <- midmod[draw_var %in% c('draw_5', 'draw_6')]
  }
  
  # Hospital
  cat('hospital... ')
  hospital <- read_feather(paste0('FILEPATH', loc_name,
                                  '_', loc_id, '_hsp_admit.feather'))
  setnames(hospital, 'variable', 'msre')
  hospital[, date := as.IDate(date)]
  if (test == 0) {
    hospital <- melt(hospital, measure.vars = roots$draws)
    hospital <- dcast(hospital, formula='location_id + age_group_id + sex_id + date + variable ~ msre', 
                      value.var='value')
  } else {
    hospital <- melt(hospital, measure.vars = roots$draws)
    hospital <- hospital[variable %in% c('draw_5', 'draw_6')]
    hospital <- dcast(hospital, formula='location_id + age_group_id + sex_id + date + variable ~ msre', 
                      value.var='value')
  }
  setnames(hospital, 'variable', 'draw_var')
  
  
  # ICU
  cat('icu...\n')
  icu <- read_feather(paste0('FILEPATH', loc_name,
                             '_', loc_id, '_icu_admit.feather'))
  setnames(icu, 'variable', 'msre')
  icu[, date := as.IDate(date)]
  if (test == 0) {
    icu <- melt(icu, measure.vars = roots$draws)
    icu <- dcast(icu, formula='location_id + age_group_id + sex_id + date + variable ~ msre', 
                 value.var='value')
  } else {
    icu <- melt(icu, measure.vars = roots$draws)
    icu <- icu[variable %in% c('draw_5', 'draw_6')]
    icu <- dcast(icu, formula='location_id + age_group_id + sex_id + date + variable ~ msre', 
                 value.var='value')
  }
  setnames(icu, 'variable', 'draw_var')
  
  if (test2 == 1) {
    cat('   629')
    check_size(obj_list=ls(), print=TRUE)  
  }
  ## --------------------------------------------------------------------- ----
  
  # to use for debugging since this code takes foreeeeever
  if (test==1) {
    durs <- durs[draw_var %in% c('draw_5', 'draw_6')]
    any_lc_props <- any_lc_props[draw_var%in% c('draw_5', 'draw_6')]
    midmod <- midmod[draw_var%in% c('draw_5', 'draw_6')]
    hospital <- hospital[draw_var%in% c('draw_5', 'draw_6')]
    icu <- icu[draw_var%in% c('draw_5', 'draw_6')]
  }
  
  cat('Calculating mild/moderate incidence & prevalence...\n')
  ## Mild/Moderate Incidence and Prevalence ------------------------------ ----
  
  
  tictoc::tic(msg="shifting and writing icu and hospital data")
  # Write icu data and remove it from mem
  write_feather(icu, paste0('FILEPATH', "icu.feather"))
  rm(icu)
  
  
  # Step 3.a
  # Shift hospitalizations 7 days
  lag_hsp <- copy(hospital)[, !c('hsp_deaths')]
  lag_hsp[, date := date + roots$defaults$symp_to_hsp_admit_duration]
  
  
  # Write hospital data and remove it from mem
  write_feather(hospital, paste0('FILEPATH', "hospital.feather"))
  tictoc::toc()
  rm(hospital)
  
  
  # Step 3.b
  # Merge midmod and lag_hsp
  tictoc::tic(msg="merging hospital and deaths data")
  midmod <- merge(midmod, lag_hsp, by=c('location_id', 'age_group_id', 'sex_id', 
                                        'draw_var', 'date'), all.x=T)
  rm(lag_hsp)
  
  # Read in community deaths
  deaths_comm <- read_feather(paste0('FILEPATH', 
                                     'community_deaths_', loc_id, '.feather')
  )[, !c('adj_factor')]
  setnames(deaths_comm, 'variable', 'draw_var')
  deaths_comm[, date := as.IDate(date)]
  
  if (test==1) {
    deaths_comm <- deaths_comm[draw_var%in% c('draw_5', 'draw_6')]
  }
  
  # Shift date to match start of severe symptoms
  deaths_comm[, date := date - (roots$defaults$comm_die_duration_severe + 
                                  roots$defaults$midmod_duration_no_hsp)]
  
  # Merge onto midmod
  midmod <- merge(midmod, deaths_comm, 
                  by=c('location_id', 'age_group_id', 'sex_id', 'date', 'draw_var'))
  tictoc::toc()
  rm(deaths_comm)
  
  
  # Step 3.c
  # mild/moderate at risk number = (mild/moderate incidence - hospital admissions|7 days later) |
  #                                 shift forward by {incubation period + mild/moderate duration|no hospital}
  midmod[is.na(midmod$hospital_inc), hospital_inc := 0]
  midmod[is.na(midmod$midmod_inc), midmod_inc := 0]
  midmod[is.na(midmod$deaths_comm), deaths_comm := 0]
  midmod[, midmod_risk_num := midmod_inc - hospital_inc - deaths_comm]
  midmod[midmod_inc == 0, midmod_risk_num := 0]
  midmod[midmod_risk_num < 0, midmod_risk_num := 0]
  midmod[, date := date + roots$defaults$incubation_period + roots$defaults$midmod_duration_no_hsp]
  
  # Ensure incidence and prevalence aren't negative
  if (test == 0) {
    tictoc::tic(msg="checking negatives")
    midmod <- check_neg(midmod, loc_id, loc_name, output_version, 'long',
                        'midmod_risk_num', return_data = T, years_to_check = estimation_years,
                        check_cols = 'midmod_risk_num')
    tictoc::toc()
  }
  
  
  if (test2 == 1) {
    cat('   696')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  
  tictoc::tic(msg="merging kids and adults")
  
  # Step 3.d
  temp_midmod <- midmod[age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7, 8)]
  temp_props <- any_lc_props[hospital == 0 & icu==0 & children==1, !c('hospital', 'icu', 'children')]
  midmod_kids <- merge(temp_midmod, 
                       temp_props,
                       by=c('sex_id', 'draw_var'), all=T)
  
  
  rm(temp_midmod, temp_props)
  
  
  
  temp_midmod <- midmod[age_group_id %in% c(9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)]
  rm(midmod)
  temp_props <- any_lc_props[hospital == 0 & icu==0 & children==0, !c('hospital', 'icu', 'children')]
  midmod_adults <- merge(temp_midmod, 
                         temp_props, 
                         by=c('sex_id', 'draw_var'), all=T)
  tictoc::toc()
  
  
  rm(temp_midmod, temp_props)
  
  
  tictoc::tic(msg="appending kids and adults")
  midmod <- rbind(midmod_kids, midmod_adults)
  tictoc::toc()
  
  
  rm(midmod_kids, midmod_adults)
  
  
  if (test2 == 1) {
    cat('   706')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  # Step 3.e
  # mild/moderate long_term incidence = mild/moderate at risk * proportion outcome among long covid
  tictoc::tic(msg="apply proportions")
  midmod <- .apply_proportions(midmod, 'midmod', c(long_seq, 'any'), definition)
  tictoc::toc()
  
  
  # Write any long COVID among midmod data and remove it from mem
  tictoc::tic(msg="writing midmod data")
  midmod_any <- midmod[, c('sex_id', 'draw_var', 'location_id', 'age_group_id', 'date', 'midmod_risk_num', 'midmod_any_inc', 'midmod_any_inc')]
  write_feather(midmod_any, paste0('FILEPATH', "midmod_any.feather"))
  tictoc::toc()
  names(midmod_any)
  rm(midmod_any)
  midmod$midmod_any_inc <- NULL
  
  
  if (test2 == 1) {
    cat('   713')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  # Step 3.f
  # Merge on midmod durations
  midmod <- merge(midmod, durs[population=='midmod', !c('population')], 
                  by=c('date', 'draw_var'), all.x=T)
  
  
  if(loc_id==555) {
    dir.create(paste0('FILEPATH'), recursive = TRUE)
    write.csv(midmod[date=='2020-03-29' & sex_id==2], paste0('FILEPATH/5_comm_PASC.csv'))
  }
  if (loc_id >= 523 & loc_id <=573) {
    usa_daily <- midmod[, lapply(.SD, mean, na.rm=T), by=c('location_id', 'date', 'age_group_id', 'sex_id'),
                        .SDcols=c('midmod_risk_num', paste0('midmod_', c("cognitive_severe_inc", "fatigue_inc", "respiratory_mild_inc", 
                                                                         "respiratory_moderate_inc", "respiratory_severe_inc", "cognitive_mild_fatigue_inc", 
                                                                         "cognitive_severe_fatigue_inc", "cognitive_mild_respiratory_mild_inc", "cognitive_mild_respiratory_moderate_inc", 
                                                                         "cognitive_mild_respiratory_severe_inc", "cognitive_severe_respiratory_mild_inc", "cognitive_severe_respiratory_moderate_inc", 
                                                                         "cognitive_severe_respiratory_severe_inc", "fatigue_respiratory_mild_inc", "fatigue_respiratory_moderate_inc", 
                                                                         "fatigue_respiratory_severe_inc", "cognitive_mild_fatigue_respiratory_mild_inc", "cognitive_mild_fatigue_respiratory_moderate_inc", 
                                                                         "cognitive_mild_fatigue_respiratory_severe_inc", "cognitive_severe_fatigue_respiratory_mild_inc", "cognitive_severe_fatigue_respiratory_moderate_inc", 
                                                                         "cognitive_severe_fatigue_respiratory_severe_inc")))]
    usa_daily_22 <- usa_daily[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'date'),
                              .SDcols=c('midmod_risk_num', paste0('midmod_', c("cognitive_severe_inc", "fatigue_inc", "respiratory_mild_inc", 
                                                                               "respiratory_moderate_inc", "respiratory_severe_inc", "cognitive_mild_fatigue_inc", 
                                                                               "cognitive_severe_fatigue_inc", "cognitive_mild_respiratory_mild_inc", "cognitive_mild_respiratory_moderate_inc", 
                                                                               "cognitive_mild_respiratory_severe_inc", "cognitive_severe_respiratory_mild_inc", "cognitive_severe_respiratory_moderate_inc", 
                                                                               "cognitive_severe_respiratory_severe_inc", "fatigue_respiratory_mild_inc", "fatigue_respiratory_moderate_inc", 
                                                                               "fatigue_respiratory_severe_inc", "cognitive_mild_fatigue_respiratory_mild_inc", "cognitive_mild_fatigue_respiratory_moderate_inc", 
                                                                               "cognitive_mild_fatigue_respiratory_severe_inc", "cognitive_severe_fatigue_respiratory_mild_inc", "cognitive_severe_fatigue_respiratory_moderate_inc", 
                                                                               "cognitive_severe_fatigue_respiratory_severe_inc")))]
    usa_daily_22$sex_id <- 3
    usa_daily_22$age_group_id <- 22
    usa_daily_sex <- usa_daily[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'date', 'sex_id'),
                               .SDcols=c('midmod_risk_num', paste0('midmod_', c("cognitive_severe_inc", "fatigue_inc", "respiratory_mild_inc", 
                                                                                "respiratory_moderate_inc", "respiratory_severe_inc", "cognitive_mild_fatigue_inc", 
                                                                                "cognitive_severe_fatigue_inc", "cognitive_mild_respiratory_mild_inc", "cognitive_mild_respiratory_moderate_inc", 
                                                                                "cognitive_mild_respiratory_severe_inc", "cognitive_severe_respiratory_mild_inc", "cognitive_severe_respiratory_moderate_inc", 
                                                                                "cognitive_severe_respiratory_severe_inc", "fatigue_respiratory_mild_inc", "fatigue_respiratory_moderate_inc", 
                                                                                "fatigue_respiratory_severe_inc", "cognitive_mild_fatigue_respiratory_mild_inc", "cognitive_mild_fatigue_respiratory_moderate_inc", 
                                                                                "cognitive_mild_fatigue_respiratory_severe_inc", "cognitive_severe_fatigue_respiratory_mild_inc", "cognitive_severe_fatigue_respiratory_moderate_inc", 
                                                                                "cognitive_severe_fatigue_respiratory_severe_inc")))]
    usa_daily_sex$age_group_id <- 22
    #plot(usa_daily_sex$date, usa_daily_sex$midmod_risk_num)
    usa_daily <- rbind(usa_daily, usa_daily_22, usa_daily_sex)
    dir.create('FILEPATH', recursive = TRUE)
    write.csv(usa_daily, paste0('FILEPATH/daily_midmod_at_risk_', loc_id, '.csv'))
    rm(usa_daily, usa_daily_22, usa_daily_sex)
  }
  
  if (test2 == 1) {
    cat('   727')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  # Step 3.g
  # mild/moderate long-term prevalence = mild/moderate long-term incidence * duration
  tictoc::tic(msg = "apply durations")
  #  midmod_orig <- copy(midmod)
  #  midmod <- copy(midmod_orig)
  midmod <- .apply_durations(midmod, 'midmod', long_seq, estimation_years)
  tictoc::toc()
  
  
  if (test2 == 1) {
    cat('   734')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  # cumulative cases for annual vetting
  tictoc::tic(msg="saving rolling prev")
  .save_rolling_prev(midmod, 'midmod', long_seq, refs, estimation_years)
  tictoc::toc()
  
  
  # Remove unneeded cols
  midmod[, `:=`(midmod_inc = NULL, hospital_inc = NULL, midmod_risk_num = NULL, fat_or_resp_or_cog = NULL)]
  
  
  # Write midmod data and remove it from mem
  tictoc::tic(msg="writing midmod data")
  write_feather(midmod, paste0('FILEPATH', "midmod.feather"))
  tictoc::toc()
  names(midmod)
  rm(midmod)
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Calculating severe incidence and prevalence...\n')
  ## Severe Incidence and Prevalence ------------------------------------- ----
  
  # Read in icu data
  tictoc::tic(msg="reading in and shifting icu data")
  icu <- read_feather(paste0('FILEPATH', "icu.feather"))
  
  # Step 4.a
  # Shift icu admissions
  lag_icu <- copy(icu)[, !c('icu_deaths')]
  lag_icu[, date := date + roots$defaults$icu_to_death_duration]
  
  # Remove icu data from mem
  rm(icu)
  tictoc::toc()
  
  
  # Read in hospital data
  tictoc::tic(msg="reading in and shifting hospital data")
  hospital <- read_feather(paste0('FILEPATH', "hospital.feather"))
  
  # Step 4.b
  # Shift hospital deaths
  lag_hsp <- copy(hospital)[, !c('hospital_inc')]
  lag_hsp[, date := date + roots$defaults$hsp_no_icu_death_duration]
  tictoc::toc()
  
  
  # Step 4.c
  # Merge lagged datasets
  tictoc::tic(msg="merging lagged datasets")
  lag <- merge(lag_icu, lag_hsp, by=c('location_id', 'age_group_id', 'sex_id', 
                                      'draw_var', 'date'), all.x=T)
  tictoc::toc()
  if (test2 == 1) {
    cat('   768')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  rm(lag_icu, lag_hsp)
  tictoc::tic(msg="merging with hospital data")
  hospital <- merge(hospital[, !c('hsp_deaths')], lag, 
                    by=c('location_id', 'age_group_id', 'sex_id', 
                         'draw_var', 'date'), all.x=T)
  tictoc::toc()
  if (test2 == 1) {
    cat('   775')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  rm(lag)
  
  
  # Step 4.d
  # severe at risk number = (hospital admissions - ICU admissions|3 days later - hospital deaths|6 days later) |
  #                          shift forward by {hospital duration if no ICU no death + hospital mild moderate duration after discharge}
  hospital[, hospital_risk_num := hospital_inc - icu_inc - hsp_deaths]
  hospital[hospital_risk_num < 0, hospital_risk_num := 0]
  hospital[, date := date + roots$defaults$hsp_no_icu_no_death_duration + 
             roots$defaults$hsp_midmod_after_discharge_duration]
  
  # Ensure incidence and prevalence aren't negative
  if (test == 0) {
    tictoc::tic(msg="checking negatives")
    hospital <- check_neg(hospital, loc_id, loc_name, output_version, 'long',
                          'hospital_risk_num', return_data = T, years_to_check = estimation_years,
                          check_cols = 'hospital_risk_num')
    tictoc::toc()
  }
  
  
  # Step 4.e
  tictoc::tic(msg="kids and adults props merging")
  temp_hospital <- hospital[age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7, 8)]
  temp_props <- any_lc_props[hospital==1 & icu==0 & children==1, !c('hospital', 'icu', 'children')]
  hospital_kids <- merge(temp_hospital, 
                         temp_props, 
                         by=c('sex_id', 'draw_var'), all=T)
  rm(temp_hospital, temp_props)
  
  
  temp_hospital <- hospital[age_group_id %in% c(9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)]
  rm(hospital)
  temp_props <- any_lc_props[hospital==1 & icu==0 & children==0, !c('hospital', 'icu', 'children')]
  hospital_adults <- merge(temp_hospital, 
                           temp_props, 
                           by=c('sex_id', 'draw_var'), all=T)
  tictoc::toc()
  rm(temp_hospital, temp_props)
  
  
  tictoc::tic(msg="appending kids and adults")
  hospital <- rbind(hospital_kids, hospital_adults)
  tictoc::toc()
  rm(hospital_kids, hospital_adults)
  
  if (test2 == 1) {
    cat('   806')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  # Step 4.f
  # hospital long_term incidence = hospital at risk * proportion outcome among long covid
  tictoc::tic(msg="apply proportions")
  hospital <- .apply_proportions(hospital, 'hospital', c(long_seq, 'any'), definition)
  tictoc::toc()
  
  # Write any long COVID among hospital data and remove it from mem
  tictoc::tic(msg="writing hospital any data")
  hospital_any <- hospital[, c('sex_id', 'draw_var', 'location_id', 'age_group_id', 'date', 'hospital_risk_num', 'hospital_any_inc')]
  write_feather(hospital_any, paste0('FILEPATH', "hospital_any.feather"))
  tictoc::toc()
  names(hospital_any)
  rm(hospital_any)
  hospital$hospital_any_inc <- NULL
  
  
  if (test2 == 1) {
    cat('   815')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  # Step 4.g
  # Merge on hospital durations
  hospital <- merge(hospital, durs[population=='hosp', !c('population')], 
                    by=c('date', 'draw_var'), all.x=T)
  
  if (test2 == 1) {
    cat('   823')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  if(loc_id==555) {
    write.csv(hospital[date=='2020-04-19' & sex_id==2], paste0('FILEPATH/6_hosp_PASC.csv'))
  }
  if (loc_id >= 523 & loc_id <=573) {
    usa_daily <- hospital[, lapply(.SD, mean, na.rm=T), by=c('location_id', 'date', 'age_group_id', 'sex_id'),
                          .SDcols=c('hospital_risk_num', paste0('hospital_', c("cognitive_severe_inc", "fatigue_inc", "respiratory_mild_inc", 
                                                                               "respiratory_moderate_inc", "respiratory_severe_inc", "cognitive_mild_fatigue_inc", 
                                                                               "cognitive_severe_fatigue_inc", "cognitive_mild_respiratory_mild_inc", "cognitive_mild_respiratory_moderate_inc", 
                                                                               "cognitive_mild_respiratory_severe_inc", "cognitive_severe_respiratory_mild_inc", "cognitive_severe_respiratory_moderate_inc", 
                                                                               "cognitive_severe_respiratory_severe_inc", "fatigue_respiratory_mild_inc", "fatigue_respiratory_moderate_inc", 
                                                                               "fatigue_respiratory_severe_inc", "cognitive_mild_fatigue_respiratory_mild_inc", "cognitive_mild_fatigue_respiratory_moderate_inc", 
                                                                               "cognitive_mild_fatigue_respiratory_severe_inc", "cognitive_severe_fatigue_respiratory_mild_inc", "cognitive_severe_fatigue_respiratory_moderate_inc", 
                                                                               "cognitive_severe_fatigue_respiratory_severe_inc")))]
    usa_daily_22 <- usa_daily[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'date'),
                              .SDcols=c('hospital_risk_num', paste0('hospital_', c("cognitive_severe_inc", "fatigue_inc", "respiratory_mild_inc", 
                                                                                   "respiratory_moderate_inc", "respiratory_severe_inc", "cognitive_mild_fatigue_inc", 
                                                                                   "cognitive_severe_fatigue_inc", "cognitive_mild_respiratory_mild_inc", "cognitive_mild_respiratory_moderate_inc", 
                                                                                   "cognitive_mild_respiratory_severe_inc", "cognitive_severe_respiratory_mild_inc", "cognitive_severe_respiratory_moderate_inc", 
                                                                                   "cognitive_severe_respiratory_severe_inc", "fatigue_respiratory_mild_inc", "fatigue_respiratory_moderate_inc", 
                                                                                   "fatigue_respiratory_severe_inc", "cognitive_mild_fatigue_respiratory_mild_inc", "cognitive_mild_fatigue_respiratory_moderate_inc", 
                                                                                   "cognitive_mild_fatigue_respiratory_severe_inc", "cognitive_severe_fatigue_respiratory_mild_inc", "cognitive_severe_fatigue_respiratory_moderate_inc", 
                                                                                   "cognitive_severe_fatigue_respiratory_severe_inc")))]
    usa_daily_22$sex_id <- 3
    usa_daily_22$age_group_id <- 22
    usa_daily_sex <- usa_daily[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'date', 'sex_id'),
                               .SDcols=c('hospital_risk_num', paste0('hospital_', c("cognitive_severe_inc", "fatigue_inc", "respiratory_mild_inc", 
                                                                                    "respiratory_moderate_inc", "respiratory_severe_inc", "cognitive_mild_fatigue_inc", 
                                                                                    "cognitive_severe_fatigue_inc", "cognitive_mild_respiratory_mild_inc", "cognitive_mild_respiratory_moderate_inc", 
                                                                                    "cognitive_mild_respiratory_severe_inc", "cognitive_severe_respiratory_mild_inc", "cognitive_severe_respiratory_moderate_inc", 
                                                                                    "cognitive_severe_respiratory_severe_inc", "fatigue_respiratory_mild_inc", "fatigue_respiratory_moderate_inc", 
                                                                                    "fatigue_respiratory_severe_inc", "cognitive_mild_fatigue_respiratory_mild_inc", "cognitive_mild_fatigue_respiratory_moderate_inc", 
                                                                                    "cognitive_mild_fatigue_respiratory_severe_inc", "cognitive_severe_fatigue_respiratory_mild_inc", "cognitive_severe_fatigue_respiratory_moderate_inc", 
                                                                                    "cognitive_severe_fatigue_respiratory_severe_inc")))]
    usa_daily_sex$age_group_id <- 22
    #    plot(usa_daily_sex$date, usa_daily_sex$hospital_risk_num)
    usa_daily <- rbind(usa_daily, usa_daily_22, usa_daily_sex)
    dir.create(paste0('FILEPATH'), recursive = TRUE)
    write.csv(usa_daily, paste0('FILEPATH/daily_hospital_at_risk_', loc_id, '.csv'))
    rm(usa_daily, usa_daily_22, usa_daily_sex)
  }
  
  
  
  # 4.h
  # severe long-term prevalence = severe long-term incidence * duration
  tictoc::tic(msg="apply durations")
  hospital <- .apply_durations(hospital, 'hospital', long_seq, estimation_years)
  tictoc::toc()
  
  
  # cumulative cases for annual vetting
  tictoc::tic(msg="save rolling prev")
  .save_rolling_prev(hospital, 'hospital', long_seq, refs, estimation_years)
  tictoc::toc()
  
  if (test2 == 1) {
    cat('   839')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  # Remove unneeded cols
  hospital[, `:=`(hospital_inc = NULL, icu_inc = NULL, hsp_deaths = NULL,
                  hospital_risk_num = NULL, fat_or_resp_or_cog = NULL)]
  
  # Write and remove hospital data
  tictoc::tic(msg="writing hospital data")
  write_feather(hospital, paste0('FILEPATH', "hospital.feather"))
  tictoc::toc()
  rm(hospital)
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Calculating critical incidence and prevalence...\n')
  ## Critical Incidence and Prevalence ----------------------------------- ----
  
  # Read in icu data
  tictoc::tic(msg="reading icu data")
  icu <- read_feather(paste0('FILEPATH', "icu.feather"))
  tictoc::toc()
  
  # Step 5.a
  # Shift icu deaths
  tictoc::tic(msg="shifting and merging")
  lag_icu <- copy(icu)[, !c('icu_inc')]
  lag_icu[, date := date + roots$defaults$icu_to_death_duration]
  
  
  # Step 5.b
  # Merge icu and lag_icu
  icu <- merge(icu[, !c('icu_deaths')], lag_icu, 
               by=c('location_id', 'age_group_id', 'sex_id', 'draw_var', 'date'),
               all.x=T)
  tictoc::toc()
  rm(lag_icu)
  
  
  # Step 5.c
  # critical at risk number = (ICU admissions - ICU deaths|3 days later) |
  #                            shift forward by {ICU duration if no death + ICU mild moderate duration after discharge}
  icu[, icu_risk_num := icu_inc - icu_deaths]
  icu[icu_risk_num < 0, icu_risk_num := 0]
  icu[, date := date + roots$defaults$icu_no_death_duration + roots$defaults$icu_midmod_after_discharge_duration]
  
  # Ensure incidence and prevalence aren't negative
  if (test == 0) {
    tictoc::tic(msg="checking negatives")
    icu <- check_neg(icu, loc_id, loc_name, output_version, 'long',
                     'icu_risk_num', return_data = T, years_to_check = estimation_years,
                     check_cols = 'icu_risk_num')
    tictoc::toc()
  }
  
  # Step 5.d
  tictoc::tic(msg="merging kids and adults")
  temp_icu <- icu[age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7, 8)]
  temp_props <- any_lc_props[hospital==0 & icu==1 & children==1, !c('hospital', 'icu', 'children')]
  icu_kids <- merge(temp_icu, 
                    temp_props, 
                    by=c('sex_id', 'draw_var'), all=T)
  rm(temp_icu, temp_props)
  
  
  temp_icu <- icu[age_group_id %in% c(9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)]
  rm(icu)
  temp_props <- any_lc_props[hospital==0 & icu==1 & children==0, !c('hospital', 'icu', 'children')]
  icu_adults <- merge(temp_icu, 
                      temp_props, 
                      by=c('sex_id', 'draw_var'), all=T)
  tictoc::toc()
  rm(temp_icu, temp_props)
  
  tictoc::tic(msg="appending kids and adults")
  icu <- rbind(icu_kids, icu_adults)
  tictoc::toc()
  rm(icu_kids, icu_adults)
  
  if (test2 == 1) {
    cat('   889')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  
  # Step 5.e
  # icu long_term incidence = icu at risk * proportion outcome among long covid
  tictoc::tic(msg="apply proportions")
  icu <- .apply_proportions(icu, 'icu', c(long_seq, 'any'), definition)
  tictoc::toc()
  
  # Write any long COVID among icu data and remove it from mem
  tictoc::tic(msg="writing icu any data")
  icu_any <- icu[, c('sex_id', 'draw_var', 'location_id', 'age_group_id', 'date', 'icu_risk_num', 'icu_any_inc')]
  write_feather(icu_any, paste0('FILEPATH', "icu_any.feather"))
  tictoc::toc()
  names(icu_any)
  rm(icu_any)
  icu$icu_any_inc <- NULL
  
  
  # Step 5.f
  # Merge on icu durations
  icu <- merge(icu, durs[population=='icu', !c('population')], 
               by=c('date', 'draw_var'), all.x=T)
  rm(durs, any_lc_props)
  
  if(loc_id==555) {
    write.csv(icu[date=='2020-04-19' & sex_id==2], paste0('FILEPATH/7_ICU_PASC.csv'))
  }
  if (loc_id >= 523 & loc_id <=573) {
    usa_daily <- icu[, lapply(.SD, mean, na.rm=T), by=c('location_id', 'date', 'age_group_id', 'sex_id'),
                     .SDcols=c('icu_risk_num', paste0('icu_', c("cognitive_severe_inc", "fatigue_inc", "respiratory_mild_inc", 
                                                                "respiratory_moderate_inc", "respiratory_severe_inc", "cognitive_mild_fatigue_inc", 
                                                                "cognitive_severe_fatigue_inc", "cognitive_mild_respiratory_mild_inc", "cognitive_mild_respiratory_moderate_inc", 
                                                                "cognitive_mild_respiratory_severe_inc", "cognitive_severe_respiratory_mild_inc", "cognitive_severe_respiratory_moderate_inc", 
                                                                "cognitive_severe_respiratory_severe_inc", "fatigue_respiratory_mild_inc", "fatigue_respiratory_moderate_inc", 
                                                                "fatigue_respiratory_severe_inc", "cognitive_mild_fatigue_respiratory_mild_inc", "cognitive_mild_fatigue_respiratory_moderate_inc", 
                                                                "cognitive_mild_fatigue_respiratory_severe_inc", "cognitive_severe_fatigue_respiratory_mild_inc", "cognitive_severe_fatigue_respiratory_moderate_inc", 
                                                                "cognitive_severe_fatigue_respiratory_severe_inc")))]
    usa_daily_22 <- usa_daily[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'date'),
                              .SDcols=c('icu_risk_num', paste0('icu_', c("cognitive_severe_inc", "fatigue_inc", "respiratory_mild_inc", 
                                                                         "respiratory_moderate_inc", "respiratory_severe_inc", "cognitive_mild_fatigue_inc", 
                                                                         "cognitive_severe_fatigue_inc", "cognitive_mild_respiratory_mild_inc", "cognitive_mild_respiratory_moderate_inc", 
                                                                         "cognitive_mild_respiratory_severe_inc", "cognitive_severe_respiratory_mild_inc", "cognitive_severe_respiratory_moderate_inc", 
                                                                         "cognitive_severe_respiratory_severe_inc", "fatigue_respiratory_mild_inc", "fatigue_respiratory_moderate_inc", 
                                                                         "fatigue_respiratory_severe_inc", "cognitive_mild_fatigue_respiratory_mild_inc", "cognitive_mild_fatigue_respiratory_moderate_inc", 
                                                                         "cognitive_mild_fatigue_respiratory_severe_inc", "cognitive_severe_fatigue_respiratory_mild_inc", "cognitive_severe_fatigue_respiratory_moderate_inc", 
                                                                         "cognitive_severe_fatigue_respiratory_severe_inc")))]
    usa_daily_22$sex_id <- 3
    usa_daily_22$age_group_id <- 22
    usa_daily_sex <- usa_daily[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'date', 'sex_id'),
                               .SDcols=c('icu_risk_num', paste0('icu_', c("cognitive_severe_inc", "fatigue_inc", "respiratory_mild_inc", 
                                                                          "respiratory_moderate_inc", "respiratory_severe_inc", "cognitive_mild_fatigue_inc", 
                                                                          "cognitive_severe_fatigue_inc", "cognitive_mild_respiratory_mild_inc", "cognitive_mild_respiratory_moderate_inc", 
                                                                          "cognitive_mild_respiratory_severe_inc", "cognitive_severe_respiratory_mild_inc", "cognitive_severe_respiratory_moderate_inc", 
                                                                          "cognitive_severe_respiratory_severe_inc", "fatigue_respiratory_mild_inc", "fatigue_respiratory_moderate_inc", 
                                                                          "fatigue_respiratory_severe_inc", "cognitive_mild_fatigue_respiratory_mild_inc", "cognitive_mild_fatigue_respiratory_moderate_inc", 
                                                                          "cognitive_mild_fatigue_respiratory_severe_inc", "cognitive_severe_fatigue_respiratory_mild_inc", "cognitive_severe_fatigue_respiratory_moderate_inc", 
                                                                          "cognitive_severe_fatigue_respiratory_severe_inc")))]
    usa_daily_sex$age_group_id <- 22
    #    plot(usa_daily_sex$date, usa_daily_sex$icu_risk_num)
    usa_daily <- rbind(usa_daily, usa_daily_22, usa_daily_sex)
    dir.create(paste0('FILEPATH'), recursive = TRUE)
    write.csv(usa_daily, paste0('FILEPATH/daily_icu_at_risk_', loc_id, '.csv'))
    rm(usa_daily, usa_daily_22, usa_daily_sex)
  }
  
  if (test2 == 1) {
    cat('   909')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  # Step 5.g
  # critical long-term prevalence = critical long-term incidence * duration
  tictoc::tic(msg="apply durations")
  icu <- .apply_durations(icu, 'icu', long_seq, estimation_years)
  tictoc::toc()
  
  if (test2 == 1) {
    cat('   916')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  # cumulative cases for annual vetting
  tictoc::tic(msg="save rolling prev")
  .save_rolling_prev(icu, 'icu', long_seq, refs, estimation_years)
  tictoc::toc()
  
  if (test2 == 1) {
    cat('   922')
    check_size(obj_list=ls(), print=TRUE)  
  }
  
  # Remove unneeded cols
  icu[, `:=`(icu_inc = NULL, icu_deaths = NULL, icu_risk_num = NULL, fat_or_resp_or_cog = NULL)]
  
  # Write icu data
  tictoc::tic(msg="writing icu data")
  write_feather(icu, paste0('FILEPATH', "icu.feather"))
  tictoc::toc()
  rm(icu)
  
  ## --------------------------------------------------------------------- ----
  if (1 == 1) {  
    cat('Aggregating by year...\n')
    ## Aggregate all by year ----------------------------------------------- ----
    
    # Step 6.a
    
    for (outcome in c("midmod", "hospital", "icu", "midmod_any", "hospital_any", "icu_any")) {
      #    for (outcome in c("hospital", "icu", "midmod_any", "hospital_any", "icu_any")) {
      tictoc::tic(msg=paste0("aggregating ", outcome))
      
      # Read in data
      tictoc::tic(msg="reading in data")
      df <- read_feather(paste0('FILEPATH', outcome, ".feather"))
      tictoc::toc()
      
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
        df <- reshape(df, idvar = c('location_id', 'age_group_id', 'sex_id', 'draw_var'),
                      timevar = 'year_id', times = estimation_years, 
                      direction = 'long', varying = list(colnames(df)[grepl('prev', colnames(df))], colnames(df)[grepl('inc', colnames(df))]),
                      v.names = c(paste0(outcome, '_prev_'), paste0(outcome, '_inc_')))
        setnames(df, c(paste0(outcome, '_prev_'), paste0(outcome, '_inc_')), c(paste0(outcome, '_prev'), paste0(outcome, '_inc')))
      }
      
      # Write out data
      tictoc::tic(msg="writing file")
      write_feather(df, paste0('FILEPATH', outcome, ".feather"))
      tictoc::toc()
      
      tictoc::toc()
    }
    
    
    # Ensure incidence and prevalence aren't negative
    tictoc::tic(msg="reading midmod and checking negatives")
    if (test == 0) {
      midmod <- read_feather(paste0('FILEPATH', "midmod.feather"))
      
      dt <- check_neg(midmod, loc_id, loc_name, output_version, 'long',
                      c(paste0('midmod_', long_seq, '_inc'), paste0('midmod_', long_seq, '_prev')),
                      return_data = T, years_to_check = estimation_years)
      
      rm(midmod, dt)
    }
    tictoc::toc()
    ## --------------------------------------------------------------------- ----
    
    
    cat('Aggregating severities...\n')
    ## Aggregate severities ------------------------------------------------ ----
    
    # Step 7
    # Faceted by age: read in all 3 datasets, merge together, aggregate severities
    for (age_id in age_groups) {
      tictoc::tic(msg=paste0("aggregating age group ", age_id))
      
      # Read in midmod and hospital
      tictoc::tic(msg="reading in midmod and hospital data")
      midmod <- read_feather(paste0('FILEPATH', "midmod.feather"))[age_group_id == age_id]
      hospital <- read_feather(paste0('FILEPATH', "hospital.feather"))[age_group_id == age_id]
      tictoc::toc()
      
      
      # Merge and delete datasets
      tictoc::tic(msg="merging midmod and hospital")
      dt <- merge(midmod, hospital,
                  by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw_var'),
                  all=T)
      tictoc::toc()
      rm(midmod, hospital)
      
      
      # Read in icu, merge, and delete
      tictoc::tic(msg="reading in icu data")
      icu <- read_feather(paste0('FILEPATH', "icu.feather"))[age_group_id == age_id]
      tictoc::toc()
      
      tictoc::tic(msg="merging on icu data")
      dt <- merge(dt, icu,
                  by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw_var'),
                  all=T)
      tictoc::toc()
      rm(icu)
      
      
      # Aggregate severities
      tictoc::tic(msg="aggregating severities")
      dt <- .aggregate_severities(dt, long_seq)
      tictoc::toc()
      
      
      # Output age-specific data
      tictoc::tic(msg="writing age specific data")
      write_feather(dt, paste0('FILEPATH', "dt_", age_id, ".feather"))
      tictoc::toc()
      
      tictoc::toc()
      rm(dt, age_id)
    }
    
    
    # Read back in datasets and append together
    dt <- data.table()
    for (age_id in age_groups) {
      tictoc::tic(msg=paste0("Appending data for ", age_id))
      
      tictoc::tic(msg="reading in data")
      temp <- read_feather(paste0('FILEPATH', "dt_", age_id, ".feather"))
      tictoc::toc()
      
      
      tictoc::tic(msg="appending")
      dt <- rbind(dt, temp)
      tictoc::toc()
      
      tictoc::toc()
      rm(temp)
      
    }
    
    # aggregate severities for any long COVID
    tictoc::tic(msg=paste0("aggregating age group ", age_id))
    
    # Read in midmod_any and hospital_any
    tictoc::tic(msg="reading in midmod_any and hospital_any and icu_any data")
    midmod_any <- read_feather(paste0('FILEPATH', "midmod_any.feather"))
    hospital_any <- read_feather(paste0('FILEPATH', "hospital_any.feather"))
    icu_any <- read_feather(paste0('FILEPATH', "icu_any.feather"))
    tictoc::toc()
    
    
    # Merge and delete datasets
    tictoc::tic(msg="merging midmod_any and hospital_any")
    dt_any <- merge(midmod_any, hospital_any,
                    by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw_var'),
                    all=T)
    tictoc::toc()
    #  rm(midmod_any, hospital_any)
    
    tictoc::tic(msg="merging on icu_any data")
    dt_any <- merge(dt_any, icu_any,
                    by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw_var'),
                    all=T)
    tictoc::toc()
    #  rm(icu_any)
    
    
    # Aggregate severities
    tictoc::tic(msg="aggregating severities")
    dt_any <- .aggregate_severities(dt_any, 'any')
    # Merge and delete datasets
    tictoc::tic(msg="merging midmod_any and hospital_any")
    dt_any <- merge(dt_any, midmod_any,
                    by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw_var'),
                    all=T)
    rm(midmod_any)
    tictoc::toc()
    #  rm(midmod_any, hospital_any)
    tictoc::tic(msg="merging midmod_any and hospital_any")
    dt_any <- merge(dt_any, hospital_any,
                    by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw_var'),
                    all=T)
    tictoc::toc()
    rm(hospital_any)
    
    tictoc::tic(msg="merging on icu_any data")
    dt_any <- merge(dt_any, icu_any,
                    by=c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw_var'),
                    all=T)
    tictoc::toc()
    rm(icu_any)
    tictoc::toc()
    
    cat('Calculating rates for any long COVID...\n')
    ## Calculate rates ----------------------------------------------------- ----
    
    # Step 8
    
    # Pull population
    pop <- get_population(age_group_id = age_groups,
                          single_year_age = F, location_id = loc_id, 
                          location_set_id = location_set_id, year_id = unique(dt$year_id), 
                          sex_id = c(1,2), release_id = release_id, 
                          status = 'best'
    )[,c('location_id','year_id','sex_id','population','age_group_id')]
    
    
    # Merge population and data
    dt_any <- merge(dt_any, pop, by=c('location_id', 'year_id', 'age_group_id', 'sex_id'), all.x=T)
    rm(pop)
    
    
    # Calculate rates
    tictoc::tic(msg="applying rates to an long COVID")
    dt_any <- .apply_rates(dt_any, c('any', 'midmod_any', 'hospital_any', 'icu_any'))
    tictoc::toc()
    
    
    # Check for negatives again (shouldn't be any, more for prev_rate capping at 1)
    if (test == 0) {
      tictoc::tic(msg="checking negatives")
      dt_any <- check_neg(dt_any, loc_id, loc_name, output_version, 'long',
                          c(paste0(c('any', 'midmod_any', 'hospital_any', 'icu_any'), '_inc_rate'), 
                            paste0(c('any', 'midmod_any', 'hospital_any', 'icu_any'), '_prev_rate')),
                          return_data = T, years_to_check = estimation_years)
      tictoc::toc()
    }
    
    
    cat('\nSaving datasets and running diagnostics...\n')
    ## Save to intermediate location --------------------------------------- ----
    
    # Step 10
    dt_any <- dt_any[year_id<2023]
    tictoc::tic(msg="saving final data for any long COVID")
    dt_any$any_YLD <- 0
    dt_any$midmod_any_YLD <- 0
    dt_any$hospital_any_YLD <- 0
    dt_any$icu_any_YLD <- 0
    .save_dataset_wrapper(dt_any, output_version, loc_id, loc_name, c('any', 'midmod_any', 'hospital_any', 'icu_any'))
    tictoc::toc()
    
    
    dt <- data.table()
    # Read back in data
    for (age_id in age_groups) {
      tictoc::tic(msg=paste0("Appending data for ", age_id))
      
      tictoc::tic(msg="reading in data")
      temp <- read_feather(paste0('FILEPATH', "dt_", age_id, ".feather"))
      tictoc::toc()
      
      
      tictoc::tic(msg="appending")
      dt <- rbind(dt, temp)
      tictoc::toc()
      
      tictoc::toc()
      rm(temp)
      
    }
    
    ## --------------------------------------------------------------------- ----
    
    
    cat('Calculating rates...\n')
    ## Calculate rates ----------------------------------------------------- ----
    
    # Step 8
    
    # Pull population
    pop <- get_population(age_group_id = age_groups,
                          single_year_age = F, location_id = loc_id, 
                          location_set_id = location_set_id, year_id = unique(dt$year_id), 
                          sex_id = c(1,2), release_id = release_id, 
                          status = 'best'
    )[,c('location_id','year_id','sex_id','population','age_group_id')]
    
    
    # Merge population and data
    dt <- merge(dt, pop, by=c('location_id', 'year_id', 'age_group_id', 'sex_id'), all.x=T)
    rm(pop)
    
    
    # Calculate rates
    tictoc::tic(msg="applying rates")
    dt <- .apply_rates(dt, long_seq)
    tictoc::toc()
    
    
    # Check for negatives again (shouldn't be any, more for prev_rate capping at 1)
    if (test == 0) {
      tictoc::tic(msg="checking negatives")
      dt <- check_neg(dt, loc_id, loc_name, output_version, 'long',
                      c(paste0(long_seq, '_inc_rate'), 
                        paste0(long_seq, '_prev_rate')),
                      return_data = T, years_to_check = estimation_years)
      tictoc::toc()
    }
    
    ## --------------------------------------------------------------------- ----
    
    
    cat('Calculating YLDs...\n')
    ## Calculate YLDs ------------------------------------------------------ ----
    
    # Step 9
    
    # Read in disability weights
    DW <- fread(paste0('FILEPATH', 'dw.csv'))
    DW <- DW[hhseqid %in% c(roots$hhseq_ids$long_cognitive, 
                            roots$hhseq_ids$long_fatigue, 
                            roots$hhseq_ids$long_respiratory,
                            roots$hhseq_ids$long_gbs)]
    
    
    # Reshape and properly name existing severities
    DW <- melt(DW, measure.vars = roots$draws)
    DW <- dcast(DW, formula = 'variable ~ healthstate', value.var = 'value')
    colnames(DW) <- c('draw_var', 'respiratory_mild_dw', 'respiratory_moderate_dw', 
                      'respiratory_severe_dw', 'cognitive_mild_dw', 'cognitive_severe_dw',
                      'fatigue_dw', 'gbs_dw')
    
    
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
    
    
    # Merge and calculate all
    dt <- merge(dt, DW, by='draw_var', all.x=T)
    
    tictoc::tic(msg="applying disability weights")
    dt <- .apply_dws(dt, long_seq)
    tictoc::toc()
    dt$gbs_dw <- NULL
    
    
    # Save out mean DWs for reference
    DW <- DW[, lapply(.SD, mean), .SDcols=paste0(long_seq, '_dw')]
    DW <- melt(DW, measure.vars = paste0(long_seq, '_dw'),
               variable.name = 'sequelae', value.name = 'disability_weight',
               variable.factor = F)
    DW[, sequelae := gsub('_dw', '', sequelae)]
    .ensure_dir('FILEPATH')
    fwrite(DW, paste0('FILEPATH', 
                      '/applied_long_covid_disability_weights.csv'))
    
    
    rm(DW)
    ## --------------------------------------------------------------------- ----
    
    
    cat('\nSaving datasets and running diagnostics...\n')
    ## Save to intermediate location --------------------------------------- ----
    
    # Step 10
    dt <- dt[year_id<2023]
    tictoc::tic(msg="saving final data")
    .save_dataset_wrapper(dt, output_version, loc_id, loc_name, long_seq)
    tictoc::toc()
    
    
    # Step 10.a
    # Delete input files after use
    # unlink(paste0('FILEPATH', loc_name,
    #               '_', loc_id, '_midmod.feather'))
    # unlink(paste0('FILEPATH', loc_name,
    #               '_', loc_id, '_hsp_admit.feather'))
    # unlink(paste0('FILEPATH',
    #               '_', loc_id, '_icu_admit.feather'))
    
    ## --------------------------------------------------------------------- ----
    
    
    cat('\nSaving for EPI database...\n')
    ## Save for EPI database ----------------------------------------------- ----
    
    # Step 11
    
    locs <- get_location_metadata(location_set_id=location_set_id, release_id=release_id)[, c('location_id', 'location_ascii_name',
                                                                     'region_id', 'region_name', 'super_region_name',
                                                                     'super_region_id', 'most_detailed')]
    i_base <- paste0('FILEPATH')
    
    
    for (measure in c(long_seq, 'any', 'midmod_any', 'hospital_any', 'icu_any')) {
      #  for (measure in c('any', 'midmod_any', 'hospital_any', 'icu_any')) {
      tictoc::tic(msg=paste0("Finalizing epi data for ", measure))
      
      # Finalize dataset
      tictoc::tic(msg="finalizing data")
      df <- .finalize_data(measure, i_base, loc_name, loc_id)
      tictoc::toc()
      
      
      # Save to final location
      tictoc::tic(msg="writing measure 5")
      save_epi_dataset(dt = df[measure_id == 5, !c('measure_id')],
                       stage = 'final', output_version = output_version,
                       me_name = measure, measure_id = 5, 
                       loc_id = loc_id, 
                       loc_name = loc_name, l = locs)
      tictoc::toc()
      
      
      tictoc::tic(msg="writing measure 6")
      save_epi_dataset(dt = df[measure_id == 6, !c('measure_id')],
                       stage = 'final', output_version = output_version,
                       me_name = measure, measure_id = 6, 
                       loc_id = loc_id, 
                       loc_name = loc_name, l = locs)
      tictoc::toc()
      
      tictoc::toc()
    }
    
    ## --------------------------------------------------------------------- ----
  }  
  
  ## Last item - check total memory used --------------------------------- ----
  unlink('FILEPATH', recursive = T, force = T)
  
  tot_size <<- tot_size + check_size(obj_list = ls()[ls() %ni% existing_vars], 
                                     env = environment(), print = F)
  ## --------------------------------------------------------------------- ----
}

## --------------------------------------------------------------------- ----


## Run All ------------------------------------------------------------- ----

if (!interactive()){
  cat('Beginning execution of:\n')
  begin_time <- Sys.time()
  
  # Command Line Arguments
  loc_id <- as.numeric(commandArgs()[8]) 
  output_version <- as.character(commandArgs()[9])
  definition <- as.character(commandArgs()[10])
  
  hsp_icu_input_path <- as.character(commandArgs()[11])

  estimation_years_str <- as.character(commandArgs()[12])
  estimation_years <- as.vector(as.numeric(unlist(strsplit(estimation_years_str, ","))))

  age_groups_str <- as.character(commandArgs()[13])
  age_groups <- as.vector(as.numeric(unlist(strsplit(age_groups_str, ","))))

  location_set_id <-as.numeric(commandArgs()[14]) 
  release_id <-as.numeric(commandArgs()[15]) 


  
  # Pull location ascii name
  loc_name <- as.character(get_location_metadata(location_set_id = location_set_id, 
                                                 release_id = release_id
  )[location_id==loc_id, 'location_ascii_name'])
  
  
  # Print out args
  cat(paste0('\tloc_id: ', loc_id, '\n\tloc_name: ', loc_name, 
             '\n\toutput_version: ', output_version, '\n'))
  
  
  cat('\n')
  existing_vars <- ls()
  tot_size <- check_size(existing_vars)
  test <- 0
  # change definition to "gbd" for GBD estimates (which measure long COVID from end of acute phase)
  #    and to "who" for using WHO definition of long COVID as starting 3 months after
  #    symptom onset and to "12mo" for using 12 months after symptom onset
#  definition <- "12mo"
  
  
  # Run Data Processing Functions
  main(loc_id, output_version, test, definition, hsp_icu_input_path, estimation_years, 
  age_groups, location_set_id, release_id)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  cat(paste0('Memory used: ', tot_size, ' GB\n'))
} else {
  cat('Beginning execution of:\n')
  begin_time <- Sys.time()
  
  # Command Line Arguments
  loc_id <- 44680  # 33 # 43 # 140 #35507 #34
  # 80 = France
  locs <- loc_id
  output_version <- "2022-03-29.05"
  test <- 1
  if (test==1) {
    roots$estimation_years <- c(2020, 2021, 2022, 2023)  
  }
  
  # change definition to "gbd" for GBD estimates (which measure long COVID from end of acute phase)
  #    and to "who" for using WHO definition of long COVID as starting 3 months after
  #    symptom onset and to "12mo" for using 12 months after symptom onset
  definition <- "12mo"
  
  # locs <- c(53581, 43901, 44710, 44688, 44867, 500, 501, 502, 354, 503, 504, 505, 506, 507, 508, 509, 361, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 4709, 4726, 4717, 4725, 4715, 4730, 4734, 4724, 4728, 4737, 4720, 4713, 4739, 4719, 4740, 4710, 4742, 4718, 4712, 4731, 4714, 4736, 4721, 4729, 4727, 4741, 4738, 4711, 53536, 53568, 43872, 53537, 43909, 53615, 53610, 44716, 44676, 44765, 53591, 53571, 53565, 44649, 43916, 44708, 43918, 44740, 53540, 60135, 25335, 53539, 53613, 44660, 43927, 43892, 43895, 44753, 44879, 44644, 35494, 44792, 43934, 44888, 44733, 44705)
  for (loc_id in locs) {
    #396, 44701
    #for (loc_id in c(43898)) {
    # Pull location ascii name
    loc_name <- as.character(get_location_metadata(location_set_id = location_set_id, 
                                                   release_id = release_id
    )[location_id==loc_id, 'location_ascii_name'])
    
    
    # Print out args
    cat(paste0('  loc_id: ', loc_id, '\n  loc_name: ', loc_name, 
               '\n  output_version: ', output_version, '\n'))
    
    
    cat('\n')
    existing_vars <- ls()
    tot_size <- check_size(existing_vars)
    
    
    # Run Data Processing Functions
    main(loc_id, output_version, test, definition)
  }
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  cat(paste0('Memory used: ', tot_size, ' GB\n'))
}

## --------------------------------------------------------------------- ----