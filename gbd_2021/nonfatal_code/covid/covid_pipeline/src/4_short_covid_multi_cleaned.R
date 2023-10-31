## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 4_short_covid.R
## Description: Primary working code to calculate & produce estimates of 
##              short-term non-fatal burden due to COVID-19. Numbered comments 
##              correspond to documentation on this HUB page:
##              ADDRESS/4_short_covid.R
## Contributors: 
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

.repo_base <- 'FILEPATH'

source(paste0(.repo_base, 'FILEPATH'))



source(paste0(roots$'ROOT', 'FILEPATH/get_ids.R'))
source(paste0(roots$'ROOT', 'FILEPATH/get_location_metadata.R'))
source(paste0(roots$'ROOT', 'FILEPATH/get_population.R'))
source(paste0(roots$'ROOT', 'FILEPATH/get_draws.R'))

## --------------------------------------------------------------------- ----


## Data Processing Functions ------------------------------------------- ----

.quick_mean <- function(df, by_cols, sd_cols, years_to_subset = c(2020)) {
  #' Function to subset date, sum draws by day, and take mean across draws
  
  return (subset_date(df, years_to_subset
                      )[, lapply(.SD, sum, na.rm=T),
                             by=c(by_cols, 'draw_var'),
                             .SDcols=sd_cols
                          ][, lapply(.SD, mean, na.rm=T),
                            by=by_cols,
                            .SDcols=sd_cols])
}

rollover_prev <- function(df, prev_var, inc_var, dur_var, estimation_years) {
  df$EOY <- as.IDate(paste0(substr(df$date, 1, 4), '-12-31'))
  # adjust duration for end of year when some days spill over into the next year
  df[, eval(paste0(dur_var, '_annual')) := get(dur_var)]
  df[(EOY - date) < get(dur_var), eval(paste0(dur_var, '_annual')) := EOY - date]
  # insurance
  df[eval(paste0(dur_var, '_annual')) < 0, eval(paste0(dur_var, '_annual')) := 0]
  # calculate prev for the infection year
  df[, eval(prev_var) := get(inc_var) * get(paste0(dur_var, '_annual'))]
  df[, eval(paste0(dur_var, '_annual')) := NULL]
  
  # Calculate prev = inc * dur for years past the infection year
  for (yr in estimation_years) {
    EOYyr <- as.IDate(paste0(yr, '-12-31'))
    # initialize duration in future years
    df[, eval(paste0(dur_var, yr)) := 0]
    # assign rollover days to duration
    df[yr == (as.numeric(substr(df$date, 1, 4)) + 1) & (EOY - date) < get(dur_var), eval(paste0(dur_var, yr)) := get(dur_var) - (EOY - date)]
    # calculate prev in future years past infection year
    df[, eval(paste0(prev_var, yr)) := get(inc_var) * get(paste0(dur_var, yr))]
    df[, eval(paste0(dur_var, yr)) := NULL]
  }
  df$EOY <- NULL
  return(df)
}


prep_rollover_dur <- function(df, dur_var, estimation_years) {
  df$EOY <- as.IDate(paste0(substr(df$date, 1, 4), '-12-31'))
  # adjust duration for end of year when some days spill over into the next year
  df[, eval(paste0(dur_var, '_annual')) := get(dur_var)]
  df[(EOY - date) < get(dur_var), eval(paste0(dur_var, '_annual')) := EOY - date]
  # insurance
  df[eval(paste0(dur_var, '_annual')) < 0, eval(paste0(dur_var, '_annual')) := 0]

  # Calculate prev = inc * dur for years past the infection year
  for (yr in estimation_years) {
    EOYyr <- as.IDate(paste0(yr, '-12-31'))
    # initialize duration in future years
    df[, eval(paste0(dur_var, yr)) := 0]
    # assign rollover days to duration
    df[yr == (as.numeric(substr(df$date, 1, 4)) + 1) & (EOY - date) < get(dur_var), eval(paste0(dur_var, yr)) := get(dur_var) - (EOY - date)]
  }
  df$EOY <- NULL
  return(df)
}

prep_to_save <- function(df, prev_var, inc_var, estimation_years) {
  df <- subset_date(df, estimation_years)
  df[, year_id := year(date)]
  
  df <- df[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'year_id', 'age_group_id', 
                                             'sex_id', 'draw_var'), 
           .SDcols=c(inc_var, colnames(df)[grepl(prev_var, colnames(df))])]
  
  df[(year_id %% 4) == 0, eval(prev_var) := get(prev_var) / 366] # Dividing by 366 to calculate into year space - otherwise prevalence is > 1
  df[(year_id %% 4) > 0, eval(prev_var) := get(prev_var) / 365] # Dividing by 365 to calculate into year space - otherwise prevalence is > 1
  for (yr in estimation_years) {
    # leap year
    if ((yr %% 4)==0) {
      df[, eval(paste0(prev_var, yr)) := get(paste0(prev_var, yr)) / 366]
    } else {
      df[, eval(paste0(prev_var, yr)) := get(paste0(prev_var, yr)) / 365]
    }
  }

  df_rolled_over <- copy(df)
  df_rolled_over[, eval(prev_var) := NULL]
  df_rolled_over[, eval(inc_var) := NULL]
  df_rolled_over <- df_rolled_over[, lapply(.SD, sum, na.rm=T), 
                                   by=c('location_id', 'draw_var', 'age_group_id', 'sex_id'), 
                                   .SDcols=c(colnames(df_rolled_over)[grepl(prev_var, colnames(df_rolled_over))])]
  df <- dcast(df, formula = paste0('location_id + age_group_id + sex_id + draw_var ~ year_id'),
              value.var=c(prev_var, inc_var))
  for (yr in estimation_years) {
    setnames(df, paste0(prev_var, '_', yr), paste0(prev_var, yr))
    setnames(df_rolled_over, paste0(prev_var, yr), paste0('rolled_over_', prev_var, yr))
  }
  df <- merge(df, df_rolled_over, by = c('location_id', 'age_group_id', 'sex_id',
                                         'draw_var'))
  return(df)
}

save_rolling_prev <- function(df, prev_var) {
  df <- df[, lapply(.SD, sum, na.rm=T), by=c('location_id', 'draw_var'), 
           .SDcols=c(colnames(df)[grepl(prev_var, colnames(df))])]
  write.csv(df, paste0('FILEPATH', output_version, 'FILEPATH', prev_var, '_', loc_id, '.csv'))
}

add_rollover <- function(df, prev_var, inc_var, estimation_years) {
  for (yr in estimation_years) {
    df[, eval(paste0(prev_var, yr)) := get(paste0(prev_var, yr)) + get(paste0('rolled_over_', prev_var, yr))]
    df[, eval(paste0('rolled_over_', prev_var, yr)) := NULL]
  }
  df <- melt(df, id.vars = c('location_id', 'age_group_id', 'sex_id', 'draw_var'), variable.name = 'year_id')
  dfprev <- df[grepl(prev_var, year_id)]
  dfinc <- df[grepl(inc_var, year_id)]
  setnames(dfprev, 'value', prev_var)
  setnames(dfinc, 'value', inc_var)
  dfprev[, year_id := as.numeric(substr(as.character(year_id), nchar(as.character(year_id))-3, nchar(as.character(year_id))))]
  dfinc[, year_id := as.numeric(substr(as.character(year_id), nchar(as.character(year_id))-3, nchar(as.character(year_id))))]
  df <- merge(dfprev, dfinc, by = c('location_id', 'age_group_id', 'sex_id', 'draw_var', 'year_id'))

  return(df)
}


main <- function(loc_id, loc_name, output_version, hsp_icu_input_path,
estimation_years, age_groups, location_set_id, release_id, test) {
  dir.create(paste0('FILEPATH', output_version, 'FILEPATH'), recursive = TRUE)
  dir.create(paste0('FILEPATH', output_version, 'FILEPATH'), recursive = TRUE)
  
  cat('Reading in infections data...\n')
  ## Read in infections data --------------------------------------------- ----
  
  # Read in daily infections, Step 1.a
  infect <- read_feather(paste0(roots$'ROOT', loc_id, 
                                '_infecs.feather'))
  infect[, date := as.IDate(date)]
  
  
  # Reshape long
  infect <- melt(infect, measure.vars = roots$draws)
  setnames(infect, c('variable', 'value'), c('draw_var', 'infections'))
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Reading in hospital admissions data...\n')
  ## Read in hospital admissions data ------------------------------------ ----
  # Step 1.b
  hsp_admit <- read_feather(paste0('FILEPATH', 'hospital_admit_', 
                                   loc_id, '.feather'))
  hsp_admit[, date := as.IDate(date)]
  
  
  # Reshape long
  hsp_admit <- melt(hsp_admit, measure.vars = roots$draws)
  hsp_admit <- dcast(hsp_admit, formula = 'location_id + age_group_id + sex_id + date + variable ~ column_name',
                     value.var='value')
  setnames(hsp_admit, 'variable', 'draw_var')
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Reading in ICU admissions data...\n')
  ## Read in ICU admissions data ----------------------------------------- ----
  # Step 1.c
  icu_admit <- read_feather(paste0(roots$'FILEPATH', 'icu_admit_', 
                                   loc_id, '.feather'))
  icu_admit[, date := as.IDate(date)]
  
  
  # Reshape long
  icu_admit <- melt(icu_admit, measure.vars = roots$draws)
  icu_admit <- dcast(icu_admit, formula = 'location_id + age_group_id + sex_id + date + variable ~ column_name',
                     value.var='value')
  setnames(icu_admit, 'variable', 'draw_var')
  
  ## --------------------------------------------------------------------- ----
  
  
  
  
  cat('Calculating asymptomatic/presymptomatic incidence & prevalence...\n')
  ## Asymptomatic Incidence & Prevalence --------------------------------- ----
  
  # Read in proportion asymp and merge onto infect
  prop_asymp <- read_feather(paste0('FILEPATH',
                                    '_prop_asymp.feather'))
  
  if (test == 1) {
    infect <- infect[draw_var=='draw_5']
    hsp_admit <- hsp_admit[draw_var=='draw_5']
    icu_admit <- icu_admit[draw_var=='draw_5']
    prop_asymp <- prop_asymp[draw_var=='draw_5']
  }
  
  # add raw asymp props onto infections
  
  infect <- merge(infect, prop_asymp,
                  by=c('location_id', 'age_group_id', 'sex_id', 'date', 'draw_var'))
  rm(prop_asymp)
  
  
  # Step 2.a
  # Calculate asymptomatic_incidence = infections_incidence * prop_asymp
  infect[, asymp_inc := infections * prop_asymp]
  
  
  # Step 2.b
  # Calculate asymptomatic_prevalence = asymptomatic_incidence * asymptomatic_duration
  infect[, asymp_duration := roots$defaults$asymp_duration]
  infect <- rollover_prev(infect, 'asymp_prev', 'asymp_inc', 'asymp_duration', estimation_years)
  
  
  # Step 2.c
  # Calculate presymptomatic_incidence = infections_incidence - asymptomatic_incidence
  infect[, `:=`(presymp_inc = infections * (1 - prop_asymp),
                prop_asymp = NULL)]
  
  # Step 2.d
  # Calculate presymptomatic_prevalence = presymptomatic_incidence * incubation_period[within 2020]
  infect[, incubation_period := roots$defaults$incubation_period]
  infect <- rollover_prev(infect, 'presymp_prev', 'presymp_inc', 'incubation_period', estimation_years)
  
  
  if(loc_id==555) {
    write.csv(infect[date=='2020-03-15' & sex_id==1], paste0('FILEPATH/1_infect_asymp_presymp.csv'))
  }
  
  # Step 2.e
  # Sum asymptomatic incidence & prevalence
  infect[, `:=`(asymp_inc = infections,
                asymp_prev = asymp_prev + presymp_prev)]
  for (yr in estimation_years) {
    infect[, eval(paste0('asymp_prev', yr)) := get(paste0('asymp_prev', yr)) + get(paste0('presymp_prev', yr))] 
  }
  asymp <- copy(infect)[, !c('presymp_inc', 'presymp_prev')]
  for (yr in estimation_years) {
    asymp[, eval(paste0('presymp_prev', yr)) := NULL]
  }
  
  
  # Step 2.f
  # Ensure no negative values in incidence and prevalence
  if (test == 0) {
    check_neg(infect, loc_id, loc_name, output_version, 'short',
            c('infections', 'asymp_duration', 'incubation_period', 'asymp_inc', 
              'asymp_prev', 'presymp_inc', 'presymp_prev'),
            c('asymp_inc', 'asymp_prev', 'presymp_inc', 'presymp_prev'),
            years_to_check = estimation_years)
  }
  infect <- infect[, !c('asymp_duration', 'incubation_period')]
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Calculating mild/moderate incidence & prevalence...\n')
  ## Mild/Moderate Incidence & Prevalence -------------------------------- ----
  midmod <- copy(infect)[, !c('asymp_inc', 'asymp_prev', 
                              'infections', 'presymp_prev')]
  for (yr in estimation_years) {
    midmod[, eval(paste0('asymp_prev', yr)) := NULL]
    midmod[, eval(paste0('presymp_prev', yr)) := NULL]
  }
  
  
  # Step 3.a
  # Setup mild/moderate incidence as fast-forwarded presymp_inc [the incubation_period duration]
  midmod[, date := date + roots$defaults$incubation_period]
  setnames(midmod, 'presymp_inc', 'midmod_inc')
  midmod <- merge(midmod, infect[, c('date', 'presymp_inc', 'draw_var', 
                                     'age_group_id', 'sex_id')], 
                  by=c('date', 'draw_var', 'age_group_id', 'sex_id'), all.x=T)
  midmod <- midmod[, c('location_id', 'date', 'age_group_id', 
                       'sex_id', 'draw_var', 'presymp_inc', 'midmod_inc')]
  
  
  # Step 3.b
  # Shift hospitalizations 7 days
  lag_hsp_admit <- copy(hsp_admit[, !c('hsp_deaths', 'icu_deaths')])
  lag_hsp_admit[, symp_to_hsp_admit_duration := roots$defaults$symp_to_hsp_admit_duration]
  lag_hsp_admit[, date := date - symp_to_hsp_admit_duration]
  
  
  # Read in community deaths
  deaths_comm <- read_feather(paste0('FILEPATH', 
                                     'community_deaths_', loc_id, '.feather')
  )[, !c('adj_factor')]
  setnames(deaths_comm, 'variable', 'draw_var')
  deaths_comm[, date := as.IDate(date)]
  
  # Shift date to match start of symptoms
  deaths_comm[, date := date - 
                roots$defaults$symp_to_hsp_admit_duration - 
                roots$defaults$hsp_death_duration]
  
  
  if (test == 1) {
    deaths_comm <- deaths_comm[draw_var=='draw_5']
  }
  
  
  # Step 3.c
  # Merge shifted hospitalizations and midmod
  midmod <- merge(midmod, lag_hsp_admit[, c('date', 'draw_var', 'age_group_id', 
                                            'sex_id', 'hsp_admit')], 
                  by=c('date', 'draw_var', 'age_group_id', 'sex_id'), all.x=T)
  midmod[is.na(hsp_admit), hsp_admit := 0]
  rm(lag_hsp_admit)
  
  # Merge shifted community deaths with midmod
  midmod <- merge(midmod, deaths_comm,
                  by=c('location_id', 'age_group_id', 'sex_id', 'date', 'draw_var'))
  rm(deaths_comm)
  
  
  # Check to see if hsp_admit is ever greater than (midmod_inc - deaths_comm)
  hsp_mean <- .quick_mean(midmod, c('age_group_id', 'sex_id', 'date'), 'hsp_admit', c(2020))
  midmod_mean <- .quick_mean(midmod, c('age_group_id', 'sex_id', 'date'), 'midmod_inc', c(2020))
  deaths_comm_mean <- .quick_mean(midmod, c('age_group_id', 'sex_id', 'date'), 'deaths_comm', c(2020))
  infections_mean <- .quick_mean(infect, c('age_group_id', 'sex_id', 'date'), 'infections', c(2020))
  if (any(hsp_mean$hsp_admit > ((midmod_mean$midmod_inc-deaths_comm_mean$deaths_comm)))) {
    out <- merge(hsp_mean, midmod_mean, by=c('age_group_id', 'sex_id', 'date'))
    out <- merge(out, deaths_comm_mean, by=c('age_group_id', 'sex_id', 'date'))
    out <- merge(out, infections_mean, by=c('age_group_id', 'sex_id', 'date'))
    out[, `:=`(location_id = loc_id, location_name = loc_name)]
    out[hsp_admit > (midmod_inc - deaths_comm), calculates_as_negative := T]
    out <- out[calculates_as_negative==T]
    fwrite(out[, c('location_id', 'location_name', 'age_group_id', 'sex_id', 'date',
                   'midmod_inc', 'hsp_admit', 'deaths_comm')],
           paste0(roots$'ROOT', 'FILEPATH',
                  str_split(output_version, '\\.')[[1]][1], 'FILEPATH',
                  output_version, 'FILEPATH', loc_id,
                  '_mean_midmod_inc_vs_hsp_admit_errors.csv'))
#    stop(paste0('There are some age/sex groups where hospital admissions are greater ',
#                'than the mild/moderate incidence (that do not lead to community deaths), ',
#                'which will lead to negative values calculated in mild/moderate prevalence.'))
    ages <- unique(out$age_group_id)
    sexes <- unique(out$sex_id)
    dates <- unique(out$dates)
    midmod[age_group_id %in% ages & sex_id %in% sexes & date %in% dates, midmod_inc := hsp_admit - deaths_comm]
  }
  rm(hsp_mean, midmod_mean, deaths_comm_mean, infect, infections_mean)
  
  
  
  # Step 3.d
  # Add durations and account for EOY
  midmod[, `:=`(midmod_duration_no_hsp = roots$defaults$midmod_duration_no_hsp,
                symp_to_hsp_admit_duration = roots$defaults$symp_to_hsp_admit_duration,
                midmod_to_severe_no_hsp = roots$defaults$symp_to_hsp_admit_duration)]
  midmod <- prep_rollover_dur(midmod, 'midmod_duration_no_hsp', estimation_years)
  midmod <- prep_rollover_dur(midmod, 'symp_to_hsp_admit_duration', estimation_years)
  midmod <- prep_rollover_dur(midmod, 'midmod_to_severe_no_hsp', estimation_years)
  
  
  # Step 3.e
  # Calculate mild/moderate prevalence = ((midmod_inc - hsp_admit[6 days later] - community deaths[19 days later]) * midmod_duration_no_hsp) + 
  #                                       (hsp_admit[6 days later] * symp_to_hsp_admit_duration) +
  #                                       (community deaths[19 days later] * midmod_to_severe_no_hsp)
  midmod[, midmod_prev := ((midmod_inc - hsp_admit - deaths_comm) * midmod_duration_no_hsp_annual) +
                           (hsp_admit * symp_to_hsp_admit_duration_annual) +
                           (deaths_comm * midmod_to_severe_no_hsp_annual)]
  
  for (yr in estimation_years) {
    # calculate prev in future years past infection year
    midmod[, eval(paste0('midmod_prev', yr)) := ((midmod_inc - hsp_admit - deaths_comm) * get(paste0('midmod_duration_no_hsp', yr))) +
             (hsp_admit * get(paste0('symp_to_hsp_admit_duration', yr))) +
             (deaths_comm * get(paste0('midmod_to_severe_no_hsp', yr)))]
    midmod[, eval(paste0('midmod_duration_no_hsp', yr)) := NULL]
    midmod[, eval(paste0('symp_to_hsp_admit_duration', yr)) := NULL]
    midmod[, eval(paste0('midmod_to_severe_no_hsp', yr)) := NULL]
  }
  
  
  
  # Step 3.f
  # Ensure incidence and prevalence aren't negative
  if (test == 0) {
    check_neg(midmod, loc_id, loc_name, output_version, 'short',
            c('presymp_inc', 'midmod_inc', 'hsp_admit'),
            add_cols = list('midmod_duration_no_hsp' = roots$defaults$midmod_duration_no_hsp,
                            'symp_to_hsp_admit_duration' = roots$defaults$symp_to_hsp_admit_duration),
            years_to_check = estimation_years)
  }
  
  if(loc_id==555) {
    write.csv(midmod[date=='2020-03-20' & sex_id==1], paste0('FILEPATH', output_version, 'FILEPATH/2_mild_mod_sequela.csv'))
  }
  
  # Remove unneeded cols & vars, keep: location, age, sex, date, draw, midmod_inc, midmod_prev*
  midmod <- midmod[, !c('original_prop_asymp', 'presymp_inc', 'hsp_admit', 
                        'infections', 'prop_asymp', 'deaths_comm',
                        'midmod_duration_no_hsp', 'symp_to_hsp_admit_duration', 
                        'midmod_to_severe_no_hsp', 'midmod_duration_no_hsp_annual',
                        'symp_to_hsp_admit_duration_annual', 'midmod_to_severe_no_hsp_annual')]

  ## --------------------------------------------------------------------- ----
  
  
  cat('Calculating hospitalization prevalence...\n')
#  Calculating hospitalization prevalence...
  ## Hospitalization Prevalence ------------------------------------------ ----
  
  # Step 4.a
  # Setup ICU admissions to lag 3 days
  lag_icu_admit <- copy(icu_admit[, !c('icu_deaths')])
  
  
  # Shifting data back: This represents the amount of time spend in hospital before being admitted to ICU
  lag_icu_admit[, date := date - roots$defaults$hsp_icu_death_duration]
  
  
  # Merge datasets - lag_data and hsp_admit
  hsp_admit <- merge(hsp_admit, lag_icu_admit, 
                     by=c('location_id', 'date', 'draw_var', 'age_group_id', 'sex_id'), 
                     all.x=T)
  hsp_admit[is.na(icu_admit), icu_admit := 0]
  rm(lag_icu_admit)
  
  
  # Step 4.d
  # Calculate hospital inc
  hsp_admit[, hospital_inc_no_icu_no_death := hsp_admit - icu_admit - hsp_deaths]
  hsp_admit[, hospital_inc_no_icu_death := hsp_deaths]
  hsp_admit[, hospital_inc_icu_no_death := icu_admit - icu_deaths]
  hsp_admit[, hospital_inc_icu_death := icu_deaths]
  
  hsp_admit[, hospital_inc := hospital_inc_no_icu_no_death + hospital_inc_no_icu_death + 
                              hospital_inc_icu_no_death + hospital_inc_icu_death]
  
  
  # Ensure hsp_admit == hospital_inc. Should be a 1:1 relationship.
  hsp_admit[, equ_check := 1]
  hsp_admit[round(hsp_admit, 3) == round(hospital_inc, 3), equ_check := 0]
  if (length(unique(hsp_admit$equ_check)) > 1) {
    issues <- hsp_admit[equ_check == 1, ]
    issues[, location_name := loc_name]
    fwrite(issues, paste0(roots$'ROOT', 'FILEPATH', loc_id, 
                          '_hsp_admit_to_inc_errors.csv'))
    stop('Original hospital admissions are not equal to calculated hospital incidence.')
  }
  hsp_admit[, equ_check := NULL]
  
  
  # Read in community deaths
  deaths_comm <- read_feather(paste0('FILEPATH', 
                                     'community_deaths_', loc_id, '.feather')
                              )[, !c('adj_factor')]
  setnames(deaths_comm, 'variable', 'draw_var')
  deaths_comm[, date := as.IDate(date)]
  
  
  # Shift date to match start of severe symptoms
  deaths_comm[, date := date - roots$defaults$comm_die_duration_severe]
  
  if (test == 1) {
    deaths_comm <- deaths_comm[draw_var=='draw_5']
  }
  
  
  # Merge with hsp_admit
  hsp_admit <- merge(hsp_admit, deaths_comm,
                     by=c('location_id', 'age_group_id', 'sex_id', 'date', 'draw_var'))
  rm(deaths_comm)
  
  
  # Add community deaths to hospital incidence to account for the incidence of all
  # severe cases (doing this after the error handling above since this is not
  # otherwise accounted for)
  hsp_admit[, hospital_inc := hospital_inc + deaths_comm]
  
  
  
  
  
  
  # Step 4.e
  # Shift durations
  hsp_admit[,`:=`(hsp_no_icu_no_death_duration = roots$defaults$hsp_no_icu_no_death_duration,
                  hsp_no_icu_death_duration = roots$defaults$hsp_no_icu_death_duration,
                  hsp_icu_no_death_duration = roots$defaults$hsp_icu_no_death_duration,
                  hsp_icu_death_duration = roots$defaults$hsp_icu_death_duration)]
  #  hsp_admit <- prep_rollover_dur(hsp_admit, 'hsp_no_icu_no_death_duration', estimation_years)
  #  hsp_admit <- prep_rollover_dur(hsp_admit, 'hsp_no_icu_death_duration', estimation_years)
  #  hsp_admit <- prep_rollover_dur(hsp_admit, 'hsp_icu_no_death_duration', estimation_years)
  #  hsp_admit <- prep_rollover_dur(hsp_admit, 'hsp_icu_death_duration', estimation_years)
  
  # Read in community deaths
  deaths_comm <- read_feather(paste0('FILEPATH', 
                                     'community_deaths_', loc_id, '.feather')
                              )[, !c('adj_factor')]
  setnames(deaths_comm, 'variable', 'draw_var')
  deaths_comm[, date := as.IDate(date)]
  
  # Shift date to match start of severe symptoms
  deaths_comm[, date := date - roots$defaults$comm_die_duration_severe]
  
  if (test == 1) {
    deaths_comm <- deaths_comm[draw_var=='draw_5']
  }
  
  # Add community death duration and EOY scale 
  deaths_comm[, comm_die_duration_severe := roots$defaults$comm_die_duration_severe]
  deaths_comm <- rollover_prev(deaths_comm, prev_var = 'comm_severe_prev', 
                               inc_var = 'deaths_comm', dur_var = 'comm_die_duration_severe', estimation_years = estimation_years)

  
  # Merge onto hsp_admit
  hsp_admit <- merge(hsp_admit, deaths_comm[, !c('deaths_comm', 'comm_die_duration_severe', 'infections', 'prop_asymp')], 
                     by=c('location_id', 'age_group_id', 'sex_id', 'date', 'draw_var'))
  rm(deaths_comm)
  
  
  
  # Step 4.f
  # Calculate hospital_prev = ((hsp_admit - icu_admit[3 days later]) * hsp_no_icu_no_death_duration) +
  #                             (hsp_admit - icu_admit[3 days later])
  #                             ((icu_admit[3 days later] - icu_deaths[6 days later]) * hsp_icu_no_death_duration) +
  #                             (icu_deaths[6 days later] * hsp_icu_death_duration)
  #  hosp_prev = hospital no icu no death + 
  #              hospital no icu death +
  #              hospital icu no death + 
  #              hospital icu death
  hsp_admit <- rollover_prev(hsp_admit, prev_var = 'hospital_prev_no_icu_no_death', 
                               inc_var = 'hospital_inc_no_icu_no_death', dur_var = 'hsp_no_icu_no_death_duration', estimation_years = estimation_years)
  hsp_admit <- rollover_prev(hsp_admit, prev_var = 'hospital_prev_no_icu_death', 
                               inc_var = 'hospital_inc_no_icu_death', dur_var = 'hsp_no_icu_death_duration', estimation_years = estimation_years)
  hsp_admit <- rollover_prev(hsp_admit, prev_var = 'hospital_prev_icu_no_death', 
                               inc_var = 'hospital_inc_icu_no_death', dur_var = 'hsp_icu_no_death_duration', estimation_years = estimation_years)
  hsp_admit <- rollover_prev(hsp_admit, prev_var = 'hospital_prev_icu_death', 
                               inc_var = 'hospital_inc_icu_death', dur_var = 'hsp_icu_death_duration', estimation_years = estimation_years)
  #  hsp_admit[, hospital_prev_no_icu_no_death := hospital_inc_no_icu_no_death * hsp_no_icu_no_death_duration]
  #  hsp_admit[, hospital_prev_no_icu_death := hospital_inc_no_icu_death * hsp_no_icu_death_duration]
  #  hsp_admit[, hospital_prev_icu_no_death := hospital_inc_icu_no_death * hsp_icu_no_death_duration]
  #  hsp_admit[, hospital_prev_icu_death := hospital_inc_icu_death * hsp_icu_death_duration]
  
  hsp_admit[, hospital_prev := hospital_prev_no_icu_no_death + 
              hospital_prev_no_icu_death + 
              hospital_prev_icu_no_death + 
              hospital_prev_icu_death +
              comm_severe_prev]
  
  for (yr in estimation_years) {
    hsp_admit[, eval(paste0('hospital_prev', yr)) := get(paste0('hospital_prev_no_icu_no_death', yr)) + 
                get(paste0('hospital_prev_no_icu_death', yr)) + 
                get(paste0('hospital_prev_icu_no_death', yr)) + 
                get(paste0('hospital_prev_icu_death', yr)) +
                get(paste0('comm_severe_prev', yr))]
  }
  
  # Step 4.g
  # Ensure incidence and prevalence aren't negative
  
  if (test == 0) {
    check_neg(hsp_admit, loc_id, loc_name, output_version, 'short',
            c('hospital_inc', 'hospital_prev', 'hsp_admit', 'icu_admit',
              'hsp_deaths', 'icu_deaths', 'hospital_inc_no_icu_no_death', 
              'hospital_inc_no_icu_death', 'hospital_inc_icu_no_death', 
              'hospital_inc_icu_death', 'hospital_prev_no_icu_no_death',
              'hospital_prev_no_icu_death', 'hospital_prev_icu_no_death',
              'hospital_prev_icu_death', 'comm_severe_prev'),
            c('hospital_inc', 'hospital_prev'),
            list('hsp_no_icu_no_death_duration' = roots$defaults$hsp_no_icu_no_death_duration,
                 'hsp_no_icu_death_duration' = roots$defaults$hsp_no_icu_death_duration,
                 'hsp_icu_no_death_duration' = roots$defaults$hsp_icu_no_death_duration,
                 'hsp_icu_death_duration' = roots$defaults$hsp_icu_death_duration),
            years_to_check = estimation_years)
  }
  
  # Remove unneeded cols & vars: keep location, age, sex, date, draw, all inc*, prev*, dur* vars
  hsp_admit <- hsp_admit[, !c('hospital_inc', 'infections', 'prop_asymp')]
  
  if(loc_id==555) {
    write.csv(hsp_admit[date=='2020-03-26' & sex_id==1], paste0('FILEPATH', output_version, 'FILEPATH/3_severe_sequela.csv'))
  }
  
  # Remove unneeded cols & vars
  hsp_admit <- hsp_admit[, !c('icu_deaths', 'icu_admit', 'hospital_inc_no_icu_no_death', 
                              'hospital_inc_no_icu_death', 'hospital_inc_icu_no_death', 
                              'hospital_inc_icu_death', 'deaths_comm', 
                              'hsp_no_icu_no_death_duration', 'hsp_no_icu_death_duration', 
                              'hsp_icu_no_death_duration', 'hsp_icu_death_duration', 
                              'comm_severe_prev', 'hospital_prev_no_icu_no_death',
                              'hospital_prev_no_icu_death', 'hospital_prev_icu_no_death',
                              'hospital_prev_icu_death')]
  for (yr in estimation_years) {
    hsp_admit[, eval(paste0('comm_severe_prev', yr)) := NULL]
    hsp_admit[, eval(paste0('hospital_prev_no_icu_no_death', yr)) := NULL]
    hsp_admit[, eval(paste0('hospital_prev_no_icu_death', yr)) := NULL]
    hsp_admit[, eval(paste0('hospital_prev_icu_no_death', yr)) := NULL]
    hsp_admit[, eval(paste0('hospital_prev_icu_death', yr)) := NULL]
  }
  setnames(hsp_admit, 'hsp_admit', 'hospital_inc')
  ## --------------------------------------------------------------------- ----
  
  
  cat('Calculating ICU prevalence...\n')
  # Calculating ICU prevalence...
  ## ICU Prevalence ------------------------------------------------------ ----
  
  # Step 5.c
  # Shift durations
  icu_admit[, `:=`(icu_no_death_duration = roots$defaults$icu_no_death_duration,
                   icu_to_death_duration = roots$defaults$icu_to_death_duration)]
  
  # Step 5.d
  # Calculate ICU prevalence = ((icu_admit - icu_deaths[3 days later]) * icu_no_death_duration) +
  #                             (icu_deaths[3 days later] * icu_to_death_duration)
  icu_admit[, icu_inc_no_death := icu_admit - icu_deaths]
  icu_admit <- rollover_prev(icu_admit, prev_var = 'icu_prev_no_death', 
                             inc_var = 'icu_inc_no_death', dur_var = 'icu_no_death_duration',estimation_years = estimation_years)
  icu_admit <- rollover_prev(icu_admit, prev_var = 'icu_prev_death', 
                             inc_var = 'icu_deaths', dur_var = 'icu_to_death_duration', estimation_years = estimation_years)
  
  icu_admit[, icu_prev := icu_prev_no_death + icu_prev_death]
  for (yr in estimation_years) {
    icu_admit[, eval(paste0('icu_prev', yr)) := get(paste0('icu_prev_no_death', yr)) + 
                get(paste0('icu_prev_death', yr))]
    icu_admit[, eval(paste0('icu_prev_no_death', yr)) := NULL]
    icu_admit[, eval(paste0('icu_prev_death', yr)) := NULL]
  }
    
    
    
  # Step 5.e
  # Ensure incidence and prevalence aren't negative
  if (test == 0) {
    check_neg(icu_admit, loc_id, loc_name, output_version, 'short',
            c('icu_admit', 'icu_prev', 'icu_deaths'),
            c('icu_admit', 'icu_prev'),
            list('icu_no_death_duration' = roots$defaults$icu_no_death_duration,
                 'icu_to_death_duration' = roots$defaults$icu_to_death_duration),
            years_to_check = estimation_years)
  }
  
  
  if(loc_id==555) {
    write.csv(icu_admit[date=='2020-03-29' & sex_id==1], paste0('FILEPATH', output_version, 'FILEPATH/4_critical_sequela.csv'))
  }
  
  
  # Remove unneeded cols & vars
  icu_admit <- icu_admit[, !c('icu_no_death_duration', 'icu_to_death_duration', 
                              'icu_prev_death', 'icu_inc_no_death', 'icu_prev_no_death')]
  setnames(icu_admit, 'icu_admit', 'icu_inc')
  ## --------------------------------------------------------------------- ----
  
  
  cat('Calculating post-hospital mild/moderate prevalence...\n')
  # Calculating post-hospital mild/moderate prevalence...
  ## Post-Hospital Mild/Moderate Prevalence ------------------------------ ----
  
  # Step 6.a
  ## Setup ICU admissions to lag 3 days later
  lag_icu <- copy(icu_admit)[, !c('icu_deaths')]
  lag_icu[, hsp_icu_death_duration := roots$defaults$hsp_icu_death_duration]
  lag_icu[, date := date - hsp_icu_death_duration]
  
  
  # Step 6.b
  ## Setup hospital deaths to lag 6 days later and scale down to deaths in hospital
  # no longer needed because we estimate hospital deaths earlier
  
  
  # Step 6.c
  #  lag_deaths[, deaths := deaths * roots$defaults$prop_deaths_icu]
  
  
  # Step 6.d
  ## Merge hospital admissions with icu admissions
  ph <- merge(hsp_admit[, !c('hospital_prev')], 
              lag_icu[, c('location_id', 'age_group_id', 'sex_id', 'date',
                          'draw_var', 'icu_inc')], 
              by=c('location_id', 'date', 'draw_var', 'age_group_id', 'sex_id'), 
              all.x=T)
  rm(lag_icu)
  
  
  # Step 6.e
  # Shift all date to represent hospital discharge
  ph[, date := date + roots$defaults$hsp_no_icu_no_death_duration]
  
  
  # Step 6.f
  # Shift durations
  ph[, hsp_midmod_after_discharge_duration := roots$defaults$hsp_midmod_after_discharge_duration]
  
  
  # Step 6.g
  ## post-hospital mild/moderate prevalence = (hospital_inc - icu_inc|3 days later - hospital deaths|6 days later) * 
  ##                                            hospital mild moderate duration after discharge
  ph[, post_hsp_inc := hospital_inc - icu_inc - hsp_deaths]
  ph <- rollover_prev(ph, prev_var = 'post_hsp_midmod_prev', inc_var = 'post_hsp_inc',
                      dur_var = 'hsp_midmod_after_discharge_duration', estimation_years = estimation_years)
  ph$post_hsp_inc <- NULL
  
  
  # Step 6.h
  ## Ensure prevalence isn't negative
  if (test == 0) {
    check_neg(ph, loc_id, loc_name, output_version, 'short',
            c('post_hsp_midmod_prev', 'hospital_inc', 'icu_inc', 'hsp_deaths'),
            c('post_hsp_midmod_prev'),
            list('hsp_midmod_after_discharge_duration' = roots$defaults$hsp_midmod_after_discharge_duration),
            years_to_check = estimation_years)
  }
  
  
  ## Merge post hospital prevalence with midmod
  midmod <- merge(midmod, ph[, !c('icu_inc', 'hsp_deaths', 'hospital_inc', 
                                  'hsp_midmod_after_discharge_duration')], 
                  by=c('location_id', 'date', 'draw_var', 'age_group_id', 'sex_id'),
                  all.x=T)
  rm(ph)
  
  
  # Step 6.i
  ## mild/moderate prevalence number = mild/moderate prevalence number + post-hospital mild/moderate prevalence
  midmod[, midmod_prev := midmod_prev + post_hsp_midmod_prev]
  midmod[, `:=`(post_hsp_midmod_prev = NULL)]
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Calculating post-icu mild/moderate prevalence...\n')
  # Calculating post-icu mild/moderate prevalence...
  ## Post-ICU Mild/Moderate Prevalence ----------------------------------- ----
  
  # Step 7.a
  ## Setup icu deaths to lag 3 days later
  # not needed anymore
  
  # Step 7.b
  ## Merge icu admissions with icu_deaths
  pi <- icu_admit[, c('location_id', 'age_group_id', 'sex_id', 'date', 'draw_var',
                      'icu_inc', 'icu_deaths')]
  
  
  # Step 7.c
  # Shift all date to represent final hospital discharge for ICU patients
  pi[, date := date + (roots$defaults$hsp_post_icu_duration + 
               roots$defaults$icu_no_death_duration)]
  
  
  # Step 7.d
  # Shift durations
  pi[, icu_midmod_after_discharge_duration := roots$defaults$icu_midmod_after_discharge_duration]
  
  # Step 7.e
  ## post-ICU mild/moderate prevalence = (icu_admit - icu deaths|3 days later) * 
  ##                                      ICU mild moderate duration after discharge
  pi[, post_icu_inc := icu_inc - icu_deaths]
  pi <- rollover_prev(pi, prev_var = 'post_icu_midmod_prev', inc_var = 'post_icu_inc',
                      dur_var = 'icu_midmod_after_discharge_duration', estimation_years = estimation_years)
  
  
  
  # Step 7.f
  ## Ensure prevalence isn't negative
  if (test == 0) {
    check_neg(pi, loc_id, loc_name, output_version, 'short',
            c('post_icu_midmod_prev', 'icu_inc', 'icu_deaths'),
            c('post_icu_midmod_prev'),
            list('icu_midmod_after_discharge_duration' = roots$defaults$icu_midmod_after_discharge_duration),
            years_to_check = estimation_years)
  }
  
  
  ## Merge midmod with post-icu prevalence
  midmod <- merge(midmod, pi[, !c('icu_inc', 'icu_deaths', 
                                  'icu_midmod_after_discharge_duration', 'post_icu_inc')], 
                  by=c('location_id', 'date', 'draw_var', 'age_group_id', 'sex_id'),
                  all.x=T)
  rm(pi)
  
  
  # Step 7.g
  ## mild/moderate prevalence number = mild/moderate prevalence number + post-ICU mild/moderate prevalence
  midmod[, midmod_prev := midmod_prev + post_icu_midmod_prev]
  for (yr in estimation_years) {
    midmod[, eval(paste0('midmod_prev', yr)) := get(paste0('midmod_prev', yr)) + get(paste0('post_icu_midmod_prev', yr))]
    midmod[, eval(paste0('post_icu_midmod_prev', yr)) := NULL]
  }
  midmod[, `:=`(post_icu_midmod_prev = NULL)]
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Splitting into Mild/Moderate outcomes...\n')
  # Splitting into Mild/Moderate outcomes...
  ## Split Mild/Moderate into respective proportions --------------------- ----
  
  # Step 8
  
  # Pull and merge mild/moderate-split draws
#  midmod_split <- fread(paste0(roots$'ROOT', 'FILEPATH', 
#                               'FILEPATH', 
#                               'final_midmod_proportion_draws.csv')
#  )[, c(roots$'ROOT'), with=F]
  midmod_split <- fread(paste0('FILEPATH', 
                               'final_midmod_proportion_draws.csv')
  )[, c(roots$draws), with=F]
  midmod_split <- melt(midmod_split, measure.vars = roots$draws, 
                       variable.name = 'draw_var', value.name = 'prop_mod')
  
  if (test == 1) {
    midmod_split <- midmod_split[draw_var=='draw_5']
  }
  
  midmod <- merge(midmod, midmod_split, by='draw_var')
  rm(midmod_split)
  
  
  # Calculate: mild incidence = midmod_inc * (1 - prop_mod)
  #            moderate incidence = midmod_inc * prop_mod
  #            mild prevalence = midmod_prev * (1 - prop_mod)
  #            moderate prevalence = midmod_prev * prop_mod
  midmod[, `:=`(mild_inc = midmod_inc * (1 - prop_mod),
                moderate_inc = midmod_inc * prop_mod,
                mild_prev = midmod_prev * (1 - prop_mod),
                moderate_prev = midmod_prev * prop_mod)]
  for (yr in estimation_years) {
    midmod[, eval(paste0('mild_prev', yr)) := get(paste0('midmod_prev', yr)) * (1 - prop_mod)]
    midmod[, eval(paste0('moderate_prev', yr)) := get(paste0('midmod_prev', yr)) * prop_mod]
    midmod[, eval(paste0('midmod_prev', yr))]
  }
  midmod[, prop_mod := NULL]
  ## --------------------------------------------------------------------- ----
  
  
  cat('Writing date-specific data for long_covid estimation...\n')
  # Writing date-specific data for long_covid estimation...
  ## Output date-specific data for long_covid ---------------------------- ----
  
  # Step 9
  
  int_o <- paste0('FILEPATH')
  .ensure_dir(int_o)
  
  
  # Mild/Moderate
  write_feather(dcast(midmod[, c('location_id', 'age_group_id', 'sex_id', 
                                 'date', 'draw_var', 'midmod_inc')],
                      formula='location_id + age_group_id + sex_id + date ~ draw_var',
                      value.var='midmod_inc'),
                paste0(int_o, loc_name, '_', loc_id, '_midmod.feather'))
  
  # Hospital Admit
  write_feather(dcast(melt(hsp_admit, measure.vars = c('hospital_inc', 'hsp_deaths')), 
                      formula='location_id + age_group_id + sex_id + date + variable ~ draw_var', 
                      value.var='value'),
                paste0(int_o, loc_name, '_', loc_id, '_hsp_admit.feather'))
  
  # Icu Admit
  write_feather(dcast(melt(icu_admit, measure.vars = c('icu_inc', 'icu_deaths')), 
                      formula='location_id + age_group_id + sex_id + date + variable ~ draw_var', 
                      value.var='value'),
                paste0(int_o, loc_name, '_', loc_id, '_icu_admit.feather'))

## --------------------------------------------------------------------- ----


  cat('Aggregating by year...\n')
  # Aggregating by year...
  ## Aggregate all by year ----------------------------------------------- ----

  # Step 10
  asymp_pre <- asymp
  asymp <- asymp_pre
  # Asymptomatic incidence & prevalence
  asymp <- prep_to_save(asymp, prev_var = 'asymp_prev', inc_var = 'asymp_inc', estimation_years = estimation_years)
  save_rolling_prev(asymp, 'asymp_prev')
  asymp <- add_rollover(asymp, prev_var = 'asymp_prev', inc_var = 'asymp_inc', estimation_years = estimation_years)
  
  
     # Mild incidence and prevalence
  mild <- prep_to_save(midmod, prev_var = 'mild_prev', inc_var = 'mild_inc', estimation_years = estimation_years)
  save_rolling_prev(mild, 'mild_prev')
  mild <- add_rollover(mild, 'mild_prev', inc_var = 'mild_inc', estimation_years = estimation_years)
  
  # Moderate incidence and prevalence
  moderate <- prep_to_save(midmod, prev_var = 'moderate_prev', inc_var = 'moderate_inc', estimation_years = estimation_years)
  save_rolling_prev(moderate, 'moderate_prev')
  rm(midmod)
  moderate <- add_rollover(moderate, 'moderate_prev', inc_var = 'moderate_inc', estimation_years = estimation_years)
  
  # Hospital incidence & prevalence
  hospital <- prep_to_save(hsp_admit, prev_var = 'hospital_prev', inc_var = 'hospital_inc', estimation_years = estimation_years)
  save_rolling_prev(hospital, 'hospital_prev')
  rm(hsp_admit)
  hospital <- add_rollover(hospital, 'hospital_prev', inc_var = 'hospital_inc', estimation_years = estimation_years)
  
  # ICU incidence & prevalence
  icu <- prep_to_save(icu_admit, prev_var = 'icu_prev', inc_var = 'icu_inc', estimation_years = estimation_years)
  save_rolling_prev(icu, 'icu_prev')
  rm(icu_admit)
  icu <- add_rollover(icu, 'icu_prev', inc_var = 'icu_inc', estimation_years = estimation_years)
  

  # Clean up variables here if needed
  ## --------------------------------------------------------------------- ----
  
  
  cat('Calculating rates...\n')
  ## Calculate rates ----------------------------------------------------- ----
  
  # Step 11
  
  pop <- get_population(age_group_id = age_groups,
                        single_year_age = F, location_id = loc_id, 
                        location_set_id = location_set_id, year_id = unique(asymp$year_id), 
                        sex_id = c(1,2), release_id = release_id, 
                        status = 'best'
                        )[,c('location_id','year_id','sex_id','population','age_group_id')]
  
  ages <- get_ids('age_group')
  
  
  # Calculate asymptomatic inc & prev rate
  asymp <- merge(asymp, pop, by=c('location_id','year_id','sex_id','age_group_id'))
  asymp[, asymp_inc_rate := (asymp_inc/population)]
  asymp[, asymp_prev_rate := (asymp_prev/population)]

  
  # Calculate mild inc & prev rate
  mild <- merge(mild, pop, by=c('location_id','year_id','sex_id','age_group_id'))
  mild[, mild_inc_rate:= (mild_inc/population)]
  mild[, mild_prev_rate:= (mild_prev/population)]

  
  # Calculate moderate inc & prev rate
  moderate <- merge(moderate, pop, by=c('location_id','year_id','sex_id','age_group_id'))
  moderate[, moderate_inc_rate:= (moderate_inc/population)]
  moderate[, moderate_prev_rate:= (moderate_prev/population)]

  
  # Calculate hospitalization inc & prev rate
  hospital <- merge(hospital, pop, by=c('location_id','year_id','sex_id','age_group_id'))
  hospital[, hospital_inc_rate := (hospital_inc/population)]
  hospital[, hospital_prev_rate := (hospital_prev/population)]

  
  # Calculate ICU inc & prev rate
  icu <- merge(icu, pop, by=c('location_id','year_id','sex_id','age_group_id'))
  icu[, icu_inc_rate := (icu_inc/population)]
  icu[, icu_prev_rate := (icu_prev/population)]

  rm(pop)
  
  
  ## --------------------------------------------------------------------- ----
  

  cat('Checking negatives again - and capping lower-bounds < 0 if found...\n')
  ## Negatives check again ----------------------------------------------- ----
  
  # Step 12
  
  if (test == 0) {
    asymp <- check_neg(asymp, loc_id, loc_name, output_version, 'short', 
                     c('asymp_inc', 'asymp_prev', 'asymp_inc_rate', 'asymp_prev_rate'),
                     return_data = T, years_to_check = estimation_years)
  }

  
  if (test == 0) {
    mild <- check_neg(mild, loc_id, loc_name, output_version, 'short',
                    c('mild_inc', 'mild_prev', 'mild_inc_rate', 'mild_prev_rate'),
                    return_data = T, years_to_check = estimation_years)
  }

  
  if (test == 0) {
    moderate <- check_neg(moderate, loc_id, loc_name, output_version, 'short',
                        c('moderate_inc', 'moderate_prev', 'moderate_inc_rate', 'moderate_prev_rate'),
                        return_data = T, years_to_check = estimation_years)
  }

  
  if (test == 0) {
    hospital <- check_neg(hospital, loc_id, loc_name, output_version, 'short',
                        c('hospital_inc', 'hospital_prev', 'hospital_inc_rate', 'hospital_prev_rate'),
                        return_data = T, years_to_check = estimation_years)
  }

  
  if (test == 0) {
    icu <- check_neg(icu, loc_id, loc_name, output_version, 'short',
                   c('icu_inc', 'icu_prev', 'icu_inc_rate', 'icu_prev_rate'),
                   return_data = T, years_to_check = estimation_years)
  }

  ## --------------------------------------------------------------------- ----
  
  
  cat('Calculating YLDs...\n')
  ## Calculate YLDs ------------------------------------------------------ ----
  
  # Step 13
  
  # Read in disability weight 
  DW <- fread(paste0(roots$disability_weight, 'dw.csv'))
  
  
  # Calculate asymptomatic YLD
  asymp[, asymp_YLD := asymp_prev_rate*0]
  
  
  # Calculate mild YLD
  mild_DW <- melt(DW[DW$hhseqid == roots$hhseq_ids$short_mild],
                  measure.vars = roots$draws)[,c('variable','value')]
  setnames(mild_DW, c('variable', 'value'), c('draw_var', 'DW'))
  
  if (test == 1) {
    mild_DW <- mild_DW[draw_var=='draw_5']
  }
  
  mild <- merge(mild, mild_DW, by='draw_var')
  mild[, mild_YLD := mild_prev_rate*DW]
  
  
  # Calculate moderate YLD
  moderate_DW <- melt(DW[DW$hhseqid == roots$hhseq_ids$short_moderate],
                      measure.vars = roots$draws)[,c('variable','value')]
  setnames(moderate_DW, c('variable', 'value'), c('draw_var', 'DW'))

  if (test == 1) {
    moderate_DW <- moderate_DW[draw_var=='draw_5']
  }
  moderate <- merge(moderate, moderate_DW, by='draw_var')
  moderate[, moderate_YLD := moderate_prev_rate*DW]
  
  
  # Calculate hospitalization YLD
  hospital_DW <- melt(DW[DW$hhseqid == roots$hhseq_ids$short_severe],
                      measure.vars = roots$draws)[,c('variable','value')]
  setnames(hospital_DW, c('variable', 'value'), c('draw_var', 'DW'))

  if (test == 1) {
    hospital_DW <- hospital_DW[draw_var=='draw_5']
  }
  hospital <- merge(hospital, hospital_DW, by='draw_var')
  hospital[, hospital_YLD := hospital_prev_rate * DW]
  
  
  # Calculate ICU YLD
  icu_DW <- melt(DW[DW$hhseqid == roots$hhseq_ids$short_icu],
                      measure.vars = roots$draws)[,c('variable','value')]
  setnames(icu_DW, c('variable', 'value'), c('draw_var', 'DW'))

  if (test == 1) {
    icu_DW <- icu_DW[draw_var=='draw_5']
  }
  icu <- merge(icu, icu_DW, by='draw_var')
  icu[, icu_YLD := icu_prev_rate * DW]
  
  
  rm(DW, mild_DW, moderate_DW, hospital_DW, icu_DW)
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('\nSaving datasets and running diagnostics...\n')
  ## Save to intermediate location --------------------------------------- ----
  
  # Step 14
  
  save_dataset(dt = asymp,filename = 'asymp', stage = 'stage_1', 
               output_version = output_version, loc_id = loc_id, 
               loc_name = loc_name)
  
  save_dataset(dt = mild, filename = 'mild', stage = 'stage_1', 
               output_version = output_version, loc_id = loc_id, 
               loc_name = loc_name)
  
  save_dataset(dt = moderate, filename = 'moderate', stage = 'stage_1', 
               output_version = output_version, loc_id = loc_id, 
               loc_name = loc_name)
  
  save_dataset(dt = hospital, filename = 'hospital', stage = 'stage_1', 
               output_version = output_version, loc_id = loc_id, 
               loc_name = loc_name)
  
  save_dataset(dt = icu, filename = 'icu', stage = 'stage_1', 
               output_version = output_version, loc_id = loc_id, 
               loc_name = loc_name)
  
  ## --------------------------------------------------------------------- ----
  

  cat('\nSaving for EPI database...\n')
  ## Save for EPI database ----------------------------------------------- ----
  
  # Step 15
  
  locs <- get_location_metadata(location_set_id=location_set_id, release_id=release_id)[, c('location_id', 'location_ascii_name',
                                                                   'region_id', 'region_name', 'super_region_name',
                                                                   'super_region_id', 'most_detailed')]
  i_base <- paste0(get_core_ref('data_output', 'stage_1'), output_version, 
                   'FILEPATH')
  
  
  for (measure in c('asymp', 'mild', 'moderate', 'hospital', 'icu')) {
    # Finalize dataset
    dt <- .finalize_data(measure, i_base, loc_name, loc_id)
    
    
    # Save to final location
    save_epi_dataset(dt = dt[measure_id == 5, !c('measure_id')],
                     stage = 'final', output_version = output_version,
                     me_name = measure, measure_id = 5, 
                     loc_id = loc_id, 
                     loc_name = loc_name, l = locs)
    save_epi_dataset(dt = dt[measure_id == 6, !c('measure_id')],
                     stage = 'final', output_version = output_version,
                     me_name = measure, measure_id = 6, 
                     loc_id = loc_id, 
                     loc_name = loc_name, l = locs)
  }
  
  ## --------------------------------------------------------------------- ----
  
  
  ## Last item - check total memory used --------------------------------- ----
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
  hsp_icu_input_path <- as.character(commandArgs()[10])

  estimation_years_str <- as.character(commandArgs()[11])
  estimation_years <- as.vector(as.numeric(unlist(strsplit(estimation_years_str, ","))))

  age_groups_str <- as.character(commandArgs()[12])
  age_groups <- as.vector(as.numeric(unlist(strsplit(age_groups_str, ","))))

  location_set_id <-as.numeric(commandArgs()[13]) 
  release_id <-as.numeric(commandArgs()[14]) 

  # Pull location ascii name
  loc_name <- as.character(get_location_metadata(location_set_id = location_set_id, 
                                                 release_id = release_id
                                                 )[location_id==loc_id, 'location_ascii_name'])
  
  
  # Print out args
  cat(paste0('  loc_id: ', loc_id, '\n  loc_name: ', loc_name, ' \n location_set_id ', location_set_id,
             '\n  output_version: ', output_version, '\n'))
  
  
  cat('\n')
  existing_vars <- ls()
  tot_size <- check_size(existing_vars)
  test <- 0
  
  
  # Run Data Processing Functions
  main(loc_id, loc_name, output_version, hsp_icu_input_path,
  estimation_years, age_groups, location_set_id, release_id, test)
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  cat(paste0('Memory used: ', tot_size, ' GB\n'))
} else {
  cat('Beginning execution of:\n')
  begin_time <- Sys.time()
  
  # Command Line Arguments
  loc_id <- 35639 # 101 #35507 # 160 # 139 # 140 #35507 #34
#  loc_id <- 106
  output_version <- '2022-03-30.02gbd'
  test <- 1
 
  
  #for (loc_id in c(35452, 396, 44701, 44695, 44693, 44726, 44718, 44745, 44756, 
  #                 44743, 44755, 44733, 44727, 44650, 44651, 44674, 44676, 44658,
  #                 44661, 44675, 44665, 44669, 44762, 44759, 44789, 44792, 44787,
  #                 44714, 44703, 44715, 44711, 44712, 44702, 4657, 43935)) {
    
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
  main(loc_id, loc_name, output_version, test)
  
  #}
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  cat(paste0('Memory used: ', tot_size, ' GB\n'))
}
## --------------------------------------------------------------------- ----