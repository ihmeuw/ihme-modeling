## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: copy_infections_deaths_data_child.R
## Description: Copy and paste COVID infections and deaths datasets into our folders
## Contributors:
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))

## --------------------------------------------------------------------- ----


## Functions ----------------------------------------------------------- ----

.fill_nas_inf <- function(dataset, columns_to_fill) {
  
  # Loop over each column
  for (col in columns_to_fill){
    
    # Fill NAs with 0
    dataset[is.na(get(col)), eval(col) := 0]
    
    # Fill Inf with 0
    dataset[is.infinite(get(col)), eval(col) := 0]
    
  }
  
  # Return
  return(dataset)
  
}


main <- function(version_tag, loc_id) {
  
  # Set input and output directory bases
  cov_dir <- paste0('FILEPATH', version_tag, 'FILEPATH')
  inf_death_out_dir <- paste0('FILEPATH', 
                              'FILEPATH', version_tag, 'FILEPATH')
  hsp_icu_out_dir <- paste0('FILEPATH', 
                            'FILEPATH', version_tag, 'FILEPATH')
  
  
  cat('Reading in data...\n')
  ## Read in data -------------------------------------------------------- ----
  
  # Infections
  inf <- readRDS(paste0(cov_dir, loc_id, '.rds'))[measure_name == 'infections',
                                                  c('location_id', 'age_group_id', 'sex_id', 
                                                    'date', roots$draws), with=F]
  inf <- melt(inf, measure.vars = roots$draws, value.name='infections')
  inf <- unique(inf)
  inf[, location_id := loc_id] # Setting just to be sure
  inf <- setDT(inf)[order(location_id, age_group_id, sex_id, date, infections)]
  n <- nrow(inf)/1000
  setnames(inf, 'variable', 'draw_orig')
  inf$variable <- rep(paste0('draw_', seq(0:999)-1), n)
  inf$draw_orig <- NULL
  inf <- data.table(inf)
  dim(inf)
  inf_test <- subset_date(inf, c(2020))
  inf_test <- inf_test[, lapply(.SD, sum, na.rm=T), 
                       by=c('location_id', 'variable'), 
                       .SDcols=c('infections')]
  inf_test <- inf_test[, lapply(.SD, mean, na.rm=T), 
                       by=c('location_id'), 
                       .SDcols=c('infections')]
  
  
  # Hospitalizations
  hsp <- readRDS(paste0(cov_dir, loc_id, '.rds'))[measure_name == 'hospitalizations',
                                                  c('location_id', 'age_group_id', 'sex_id', 
                                                    'date', roots$draws), with=F]
  hsp <- melt(hsp, measure.vars = roots$draws, value.name='hsp_admit')
  hsp <- unique(hsp)
  hsp[, location_id := loc_id] # Setting just to be sure
  hsp <- setDT(hsp)[order(location_id, age_group_id, sex_id, date, hsp_admit)]
  n <- nrow(hsp)/1000
  setnames(hsp, 'variable', 'draw_orig')
  hsp$variable <- rep(paste0('draw_', seq(0:999)-1), n)
  hsp$draw_orig <- NULL
  hsp <- data.table(hsp)
  dim(hsp)
  
  # ICU Admissions
  icu <- readRDS(paste0(cov_dir, loc_id, '.rds'))[measure_name == 'icu',
                                                  c('location_id', 'age_group_id', 'sex_id', 
                                                    'date', roots$draws), with=F]
  icu <- melt(icu, measure.vars = roots$draws, value.name='icu_admit')
  icu <- unique(icu)
  icu[, location_id := loc_id] # Setting just to be sure
  icu <- setDT(icu)[order(location_id, age_group_id, sex_id, date, icu_admit)]
  n <- nrow(icu)/1000
  setnames(icu, 'variable', 'draw_orig')
  icu$variable <- rep(paste0('draw_', seq(0:999)-1), n)
  icu$draw_orig <- NULL
  icu <- data.table(icu)
  dim(icu)
  
  # Deaths
  dth <- readRDS(paste0(cov_dir, loc_id, '.rds'))[measure_name == 'deaths',
                                                  c('location_id', 'age_group_id', 'sex_id', 
                                                    'date', roots$draws), with=F]
  dth <- melt(dth, measure.vars = roots$draws, value.name='deaths')
  dth <- unique(dth)
  dth[, location_id := loc_id] # Setting just to be sure
  dth <- setDT(dth)[order(location_id, age_group_id, sex_id, date, deaths)]
  n <- nrow(dth)/1000
  setnames(dth, 'variable', 'draw_orig')
  dth$variable <- rep(paste0('draw_', seq(0:999)-1), n)
  dth$draw_orig <- NULL
  dth <- data.table(dth)
  dim(dth)
  
  ## --------------------------------------------------------------------- ----
  
  
  cat("Adjust hospital admissions if they're ever greater than infections...\n")
  ## Adjust hospital admissions if ever greater than infections ---------- ----
  
  # Shift infections to hsp_admit date
  shift_inf <- copy(inf)
  shift_inf[, date := date + roots$defaults$infect_to_hsp_admit_duration]
  
  
  # Merge onto hsp
  hsp <- merge(hsp, shift_inf,
               by=c('location_id', 'age_group_id', 'sex_id', 'date', 'variable'))
  rm(shift_inf)
  
  
  # Check for draws where hsp_admit is greater than infections
  hsp[hsp_admit > infections * 0.642103, hsp_admit_too_large_mask := 1]
  if (length(unique(hsp$hsp_admit_too_large_mask)) > 1) {
    cat(paste0('  NOTE: There are ', nrow(hsp[hsp_admit_too_large_mask==1]),
               ' individual draws where hospital admissions are greater than ',
               '64.2103% of infections (the highest estimates % symptomatic based ',
               'on the % asymptomatic analysis). These hospital admission draws are being scaled ',
               'down to 64.2103% of infections to ensure that we never hit errors ',
               'calculating negative numbers in subtracting outcome groups.\n'))
    cat(paste0('  NOTE: There are ', nrow(hsp[hsp_admit_too_large_mask==1 & infections==0]),
               ' individual draws where hospital admissions are nonzero but ',
               'infections are zero.\n'))
    cat(paste0('  NOTE: There are ', nrow(hsp[hsp_admit_too_large_mask==1 & infections!=0]),
               ' individual draws where hospital admissions are greater than ',
               '64.2103% of infections (the highest estimates % symptomatic based ',
               'on the % asymptomatic analysis). These hospital admission draws are being scaled ',
               'down to 64.2103% of infections to ensure that we never hit errors ',
               'calculating negative numbers in subtracting outcome groups.\n'))
  }
  
  
#    test <- hsp[infections>0]
  
#    test <- copy(test)[, as.list(c(mean(infections, na.rm=TRUE), quantile(infections, c(0.025, 0.975), na.rm=TRUE), 
#                                   mean(hsp_admit, na.rm=TRUE), quantile(hsp_admit, c(0.025, 0.975), na.rm=TRUE))), 
#                       by = c('location_id', 'age_group_id', 'sex_id', 'date')]
#    colnames(test) <- c('location_id', 'age_group_id', 'sex_id', 'date',
#                        'inf_mean', 'inf_lower', 'inf_upper',
#                        'hsp_mean', 'hsp_lower', 'hsp_upper')
  
  
#  inc_plot <- ggplot(data = test) +
#    facet_wrap('sex_id') +
#    scale_x_continuous(labels = seq(0, 32, 5), breaks = seq(0, 32, 5)) +
#    scale_y_continuous(labels = scales::comma) +
#    theme_bw()
  
  
#  inc_plot <- inc_plot + 
#    geom_point(mapping = aes(x=age_group_id, y=ratio_mean),
#               size=0.9) +
#    geom_point(mapping = aes(x=age_group_id, y=ratio_lower),
#               size=0.9) +
#    geom_point(mapping = aes(x=age_group_id, y=ratio_upper),
#               size=0.9) +
#    labs(x='', y='Incident Cases', title=paste0(loc_id, ' Severity Trends by Age Group'),
#         color='Severity')
#  inc_plot
  
  # Where hsp_admit is greater than infections, scale them to 0.642103% of infections
  hsp[hsp_admit_too_large_mask == 1, new_hsp_admit := infections * 0.642103]
  hsp <- .fill_nas_inf(hsp, c('new_hsp_admit'))
  
  
  # Save out file with old and new hsp_admit values
  .ensure_dir(paste0(roots$'ROOT', 'FILEPATH', 
                     version_tag, '/'))
#  fwrite(hsp[hsp_admit_too_large_mask==1, !c('hsp_admit_too_large_mask')],
#         paste0(roots$mnt, 'FILEPATH', 
#                version_tag, 'FILEPATH', loc_id, '.csv'))
  
  
  # Set new_hsp_admit to be hsp_admit where we've made updates
  #  hsp[hsp_admit_too_large_mask == 1, hsp_admit := new_hsp_admit]
  
  
  # Drop vars
  hsp[, `:=`(infections = NULL, hsp_admit_too_large_mask = NULL, 
             new_hsp_admit = NULL)]
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Community death adjustment...\n')
  ## Community death adjustment ------------------------------------------ ----
  
  # Read in community death draws
  comm_dth_draws <- fread(paste0(roots$'ROOT', 'FILEPATH', 
                                 'FILEPATH',
                                 'final_prop_longterm_care_deaths_draws.csv')
  )[, c('one', roots$draws), with=F]
  comm_dth_draws <- melt(comm_dth_draws, measure.vars = roots$draws)
  setnames(comm_dth_draws, 'value', 'prop_deaths_longterm_care')
  
  
  # Merge deaths and long-term care proportions
  adj_factors <- merge(dth, comm_dth_draws, by=c('variable'))
  rm(comm_dth_draws)
  adj_factors$one <- NULL

  # Shift hospitalizations and icu admissions and infections
  shift_inf <- copy(inf)
  shift_hsp <- copy(hsp)
  shift_icu <- copy(icu)
  shift_inf[, date := date + roots$defaults$infect_to_hsp_admit_duration + roots$defaults$hsp_death_duration]
  shift_hsp[, date := date + roots$defaults$hsp_death_duration]
  shift_icu[, date := date + roots$defaults$icu_to_death_duration]

  # Merge hospitalizations and icu admissions and infections
  adj_factors <- merge(adj_factors, shift_inf, 
                       by=c('location_id', 'age_group_id', 'sex_id', 'date', 'variable'))
  rm(shift_inf)
  adj_factors <- merge(adj_factors, shift_hsp, 
                       by=c('location_id', 'age_group_id', 'sex_id', 'date', 'variable'))
  rm(shift_hsp)
  adj_factors <- merge(adj_factors, shift_icu, 
                       by=c('location_id', 'age_group_id', 'sex_id', 'date', 'variable'))
  rm(shift_icu)
  
  # Read in proportion asymp and merge onto infect
  prop_asymp <- fread(paste0(roots$'ROOT', 'FILEPATH', 
                             'final_asymptomatic_proportion_draws_by_age.csv'))
  prop_asymp <- melt(prop_asymp[, c('age_group_id', roots$draws), with=F], 
                     measure.vars = roots$draws)
  setnames(prop_asymp, 'value', 'prop_asymp')
  adj_factors <- merge(adj_factors, prop_asymp, by=c('variable', 'age_group_id'))
  rm(prop_asymp)
  
  
  
  # Workaround for rows where deaths are greater than infections
  
  adj_factors[(infections==0 & deaths>0), help := 1]
  if (length(unique(adj_factors$help)) > 1) {
    cat(paste0('  NOTE: There are ', nrow(adj_factors[help==1]),
               ' individual draws infections are zero and deaths are nonzero. \n'))
    adj_factors[(infections==0 & deaths>0), deaths := 0]
  }    
  adj_factors$help <- NULL

  adj_factors[((infections * (1 - prop_asymp)) - hsp_admit - (deaths * prop_deaths_longterm_care)) < 0, help := 1]
  if (length(unique(adj_factors$help)) > 1) {
    cat(paste0('  NOTE: There are ', nrow(adj_factors[help==1]),
               ' individual draws where community deaths are greater than community cases. \n'))
  } else {
    cat(paste0(' NOTE: There are no draws where deaths need to be adjusted downward to max out at community symptomatic cases.'))
  }
  adj_factors[((infections * (1 - prop_asymp)) - hsp_admit - (deaths * prop_deaths_longterm_care)) < 0, 
              deaths := (infections * (1 - prop_asymp) - hsp_admit)]
  adj_factors$help <- NULL
  
  adj_factors[(deaths < 0.001 * infections) < 0, help := 1]
  if (length(unique(adj_factors$help)) > 1) {
    cat(paste0('  NOTE: There are ', nrow(adj_factors[help==1]),
               ' individual draws infections are zero and deaths are nonzero. \n'))
  } else {
    cat(paste0(' NOTE: There are no draws where deaths need to be adjusted upward to be at least 0.001*infections.'))
  }
  adj_factors[(deaths < 0.001 * infections), 
              deaths := 0.001 * infections]
  adj_factors$help <- NULL
  
#  adj_factors[((infections * (1 - prop_asymp)) - hsp_admit - (deaths * prop_deaths_longterm_care)) < 0, 
#              fix := 1]
  
  
  # Deaths_comm = deaths * (1 – prop_deaths_longterm_care)
  adj_factors[, deaths_comm := deaths * (prop_deaths_longterm_care)]
  adj_factors[, `:=`(prop_deaths_longterm_care = NULL)]
  
  
  # Read in proportions of hsp/icu who die
  die_props <- fread(paste0(roots$'ROOT', 'FILEPATH', 
                            'FILEPATH',
                            'hosp_icu_case_fatality_by_age.csv')
  )[, !c('age_start', 'age_end')]
  
  
  # Merge onto adj_factors by age_group
  adj_factors <- merge(adj_factors, die_props, by='age_group_id')
  rm(die_props)
  
  
  #  adj_factors[, adj_factor := deaths_comm / 
  #                               ((hospital_case_fatality_ratio * hsp_admit) + 
  #                                (icu_case_fatality_ratio * icu_admit))]
  
  adj_factors[, adj_factor := deaths / 
                (deaths_comm + 
                   (hospital_case_fatality_ratio * hsp_admit) + 
                   (icu_case_fatality_ratio * icu_admit))]
  adj_factors <- .fill_nas_inf(adj_factors, c('adj_factor'))
  adj_factors[, `:=`(icu_admit = NULL,
                     deaths = NULL)]
  
  summary(adj_factors$adj_factor)
  
  # Apply adjustment factor to deaths_comm in adj_factors
  adj_factors <- copy(adj_factors)
#  adj_factors[, deaths_comm := deaths_comm * adj_factor]
  # mean(adj_factors$infections * (1 - adj_factors$prop_asymp) - adj_factors$hsp_admit - adj_factors$deaths_comm)
  
  
  adj_factors[, `:=`(hsp_admit = NULL)]
  
  # Apply adjustment factor to hsp_admit at hospital admission date
  shift_adj_factors <- copy(adj_factors)
  # shift back from deaths to hospital admissions
  shift_adj_factors[, date := date - roots$defaults$hsp_death_duration]
  hsp <- merge(hsp, shift_adj_factors, 
               by=c('location_id', 'age_group_id', 'sex_id', 'date', 'variable'))
  rm(shift_adj_factors)
  hsp[, hospital_case_fatality_ratio := hospital_case_fatality_ratio * adj_factor]

  
  hsp[hospital_case_fatality_ratio > 0.6, help := 1]
  if (length(unique(hsp$help)) > 1) {
    cat(paste0('  NOTE: There are ', nrow(hsp[help==1]),
               ' individual draws where hospital case fatality ratio > 0.6. \n'))
  } else {
    cat(paste0(' NOTE: There are no draws where hospital case fatality ratio > 0.6.'))
  }
  hsp[hospital_case_fatality_ratio > 0.6, 
      hospital_case_fatality_ratio := 0.6]
  hsp$help <- NULL
  
  hsp[, hsp_deaths := hsp_admit * hospital_case_fatality_ratio]
  hsp[, `:=`(deaths_comm = NULL, adj_factor = NULL)]
  
  
  
  # Apply adjustment factor to icu_admit
  shift_adj_factors <- copy(adj_factors)
  # shift back from deaths to icu admissions
  shift_adj_factors[, date := date - roots$defaults$icu_to_death_duration]
  icu <- merge(icu, shift_adj_factors, 
               by=c('location_id', 'age_group_id', 'sex_id', 'date', 'variable'))
  rm(shift_adj_factors)
  icu[, icu_case_fatality_ratio := icu_case_fatality_ratio * adj_factor]

  icu[icu_case_fatality_ratio > 0.9, help := 1]
  if (length(unique(icu$help)) > 1) {
    cat(paste0('  NOTE: There are ', nrow(icu[help==1]),
               ' individual draws where icu case fatality ratio > 0.9. \n'))
  } else {
    cat(paste0(' NOTE: There are no draws where icu case fatality ratio > 0.9.'))
  }
  icu[icu_case_fatality_ratio > 0.9, 
      icu_case_fatality_ratio := 0.9]
  icu$help <- NULL
  
  icu[, icu_deaths := icu_admit * icu_case_fatality_ratio]
  icu[, `:=`(deaths_comm = NULL, adj_factor = NULL, infections = NULL, prop_asymp = NULL)]
  
  
  
  # Add icu_deaths to hsp_admit dt
  shift_icu <- copy(icu[, !c('icu_admit')])
  # shift back from icu admissions to hospital admissions
  shift_icu[, date := date - roots$defaults$hsp_icu_death_duration]
  hsp <- hsp[, !c('hospital_case_fatality_ratio', 'icu_case_fatality_ratio', 'infections', 'prop_asymp')]
  hsp <- merge(hsp, shift_icu,
               by=c('location_id', 'age_group_id', 'sex_id', 'date', 'variable'))
  hsp <- hsp[, !c('hospital_case_fatality_ratio', 'icu_case_fatality_ratio')]
  rm(shift_icu)
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Asymptomatic proportion adjustment...\n')
  ## Proportion asymptomatic adjustment ---------------------------------- ----
  
  adj_checks <- copy(adj_factors[, !c('adj_factor')])

  # Calculate asymptomatic_incidence = infections_incidence * prop_asymp
  adj_checks[, asymptomatic := infections * prop_asymp]
  
  # Shift deaths_comm from death to infection space
  adj_checks[, date := date - (roots$defaults$infect_to_hsp_admit_duration + 
                                        roots$defaults$hsp_death_duration)]

  
  # Shift hsp_admit to infection space
  shift_hsp <- copy(hsp[, !c('hsp_deaths', 'icu_deaths')])
  shift_hsp[, date := date - roots$defaults$infect_to_hsp_admit_duration]
  # Merge on
  adj_checks <- merge(adj_checks, shift_hsp,
                      by=c('location_id', 'age_group_id', 'sex_id', 'date', 'variable'))
  rm(shift_hsp)
  
  
  
  # Calculate mask for where to adjust asymp_prop
  adj_checks[(asymptomatic / 
                (infections - hsp_admit - deaths_comm)) > 
                roots$defaults$prop_asymp_comm_no_die_no_hsp, 
             adjust_asymp_prop_mask := 1]
  if (length(unique(adj_checks$adjust_asymp_prop_mask)) > 1) {
    cat(paste0('  NOTE: There are ', nrow(adj_checks[adjust_asymp_prop_mask==1]),
               ' individual draws where prop asymp needs adjusting. \n'))
  } else {
    cat(paste0(' NOTE: There are no draws where prop asymp needs adjusting.'))
  }
  
#  table(adj_checks$adjust_asymp_prop_mask, useNA='always')
  # Adjust prop_asymp for each erroring age
  # Prop_asymp = ((infections – deaths_comm – hosp_admissions) * prop_asymp_comm_no_die) / infections
  adj_checks[, adjusted_prop_asymp := prop_asymp]
  adj_checks[adjust_asymp_prop_mask == 1, 
             adjusted_prop_asymp := (roots$defaults$prop_asymp_comm_no_die_no_hsp * (infections - hsp_admit - deaths_comm)) / infections]


  # Re-save prop_asymp file now that it's adjusted
  prop_asymp <- adj_checks[, c('location_id', 'age_group_id', 'sex_id', 'date', 
                               'variable', 'prop_asymp', 'adjusted_prop_asymp')]
  rm(adj_checks)
  setnames(prop_asymp, c('variable', 'prop_asymp', 'adjusted_prop_asymp'), 
           c('draw_var', 'original_prop_asymp', 'prop_asymp'))
  write_feather(prop_asymp, paste0(inf_death_out_dir, loc_id, '_prop_asymp.feather'))
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Fill NAs with Zero...\n')
  ## Fill NAs with Zero -------------------------------------------------- ----
  
  inf <- .fill_nas_inf(inf, c('infections'))
  hsp <- .fill_nas_inf(hsp, c('hsp_admit', 'hsp_deaths', 'icu_deaths'))
  icu <- .fill_nas_inf(icu, c('icu_admit', 'icu_deaths'))
  dth <- .fill_nas_inf(dth, c('deaths'))
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Reshape and write...\n')
  ## Reshape and write --------------------------------------------------- ----
  
  # Infections
  inf <- dcast(inf, formula = 'location_id + age_group_id + sex_id + date ~ variable',
               value.var='infections')
  write_feather(inf, paste0(inf_death_out_dir, loc_id, '_infecs.feather'))
  rm(inf)
  
  
  # Hospital admissions
  new_hsp <- data.table()
  for (col in c('hsp_admit', 'hsp_deaths', 'icu_deaths')) {
    
    df <- dcast(hsp, formula = 'location_id + age_group_id + sex_id + date ~ variable',
                value.var=col)
    df[, column_name := col]
    new_hsp <- rbind(new_hsp, df)
    rm(df)
    
  }
  rm(hsp)
  write_feather(new_hsp, paste0(hsp_icu_out_dir, 'hospital_admit_', loc_id, '.feather'))
  rm(new_hsp)
  
  
  # ICU admissions
  new_icu <- data.table()
  for (col in c('icu_admit', 'icu_deaths')) {
    
    df <- dcast(icu, formula = 'location_id + age_group_id + sex_id + date ~ variable',
                value.var=col)
    df[, column_name := col]
    new_icu <- rbind(new_icu, df)
    rm(df)
    
  }
  rm(icu)
  write_feather(new_icu, paste0(hsp_icu_out_dir, 'icu_admit_', loc_id, '.feather'))
  rm(new_icu)
  
  
  # Deaths
  dth <- dcast(dth, formula = 'location_id + age_group_id + sex_id + date ~ variable',
               value.var='deaths')
  write_feather(dth, paste0(inf_death_out_dir, loc_id, '_deaths.feather'))
  rm(dth)
  
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Saving community deaths file...\n')
  ## Save deaths_comm ---------------------------------------------------- ----
  
  adj_factors <- adj_factors[,!c('hospital_case_fatality_ratio', 'icu_case_fatality_ratio')]
  write_feather(adj_factors, paste0(hsp_icu_out_dir, 
                                    'community_deaths_', loc_id, '.feather'))
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('Write diagnostic files...\n')
  ## Save diagnostic datasets -------------------------------------------- ----
  
  # Truncate dates
  adj_factors <- subset_date(adj_factors)
  prop_asymp <- subset_date(prop_asymp)
  
  
  # Sum by day
  adj_factors <- adj_factors[, lapply(.SD, sum, na.rm=T), 
                             by=c('location_id', 'age_group_id', 'sex_id', 'variable'), 
                             .SDcols=c('deaths_comm', 'adj_factor')]
  adj_factors[, `:=`(deaths_comm = deaths_comm / 366,
                     adj_factor = adj_factor / 366)]
  prop_asymp <- prop_asymp[, lapply(.SD, sum, na.rm=T), 
                           by=c('location_id', 'age_group_id', 'sex_id', 'draw_var'), 
                           .SDcols=c('prop_asymp', 'original_prop_asymp')]
  prop_asymp[, `:=`(prop_asymp = prop_asymp / 366,
                    original_prop_asymp = original_prop_asymp / 366)]
  
  
  # Calculate deaths_comm mean, upper, lower
  deaths_comm <- copy(adj_factors)[, as.list(c(mean(deaths_comm), quantile(deaths_comm, c(0.025, 0.975)))), 
                                   by = c('location_id', 'age_group_id', 'sex_id')]
  colnames(deaths_comm) <- c('location_id', 'age_group_id', 'sex_id', 
                             'deaths_comm_mean', 'deaths_comm_lower', 'deaths_comm_upper')
  
  
  # Calculate adj_factor mean, upper, lower
  adj_f <- copy(adj_factors)[, as.list(c(mean(adj_factor), quantile(adj_factor, c(0.025, 0.975)))), 
                             by = c('location_id', 'age_group_id', 'sex_id')]
  colnames(adj_f) <- c('location_id', 'age_group_id', 'sex_id', 
                       'adj_factor_mean', 'adj_factor_lower', 'adj_factor_upper')
  rm(adj_factors)
  
  
  # Calculate the new prop_asymp mean, upper, lower
  new_prop_asymp <- copy(prop_asymp)[, as.list(c(mean(prop_asymp), quantile(prop_asymp, c(0.025, 0.975)))), 
                                     by = c('location_id', 'age_group_id', 'sex_id')]
  colnames(new_prop_asymp) <- c('location_id', 'age_group_id', 'sex_id', 
                                'new_prop_asymp_mean', 'new_prop_asymp_lower', 'new_prop_asymp_upper')
  
  
  # Calculate the original prop_asymp mean, upper, lower
  orig_prop_asymp <- copy(prop_asymp)[, as.list(c(mean(original_prop_asymp), quantile(original_prop_asymp, c(0.025, 0.975)))), 
                                      by = c('location_id', 'age_group_id', 'sex_id')]
  colnames(orig_prop_asymp) <- c('location_id', 'age_group_id', 'sex_id', 
                                 'orig_prop_asymp_mean', 'orig_prop_asymp_lower', 'orig_prop_asymp_upper')
  rm(prop_asymp)
  
  
  # Merge together
  df <- merge(deaths_comm, adj_f, by=c('location_id', 'age_group_id', 'sex_id')) %>%
    merge(new_prop_asymp, by=c('location_id', 'age_group_id', 'sex_id')) %>%
    merge(orig_prop_asymp, by=c('location_id', 'age_group_id', 'sex_id'))
  rm(deaths_comm, adj_f, new_prop_asymp, orig_prop_asymp)
  
  
  # Write data
#  fwrite(df, paste0(roots$'ROOT', 'FILEPATH', 
#                    version_tag, 'FILEPATH', loc_id, '.csv'))
  
  ## --------------------------------------------------------------------- ----
  
}

## --------------------------------------------------------------------- ----


## Run all ------------------------------------------------------------- ----
if (!interactive()){
  begin_time <- Sys.time()
  
  
  # Command line arguments
  version_tag <- as.character(commandArgs()[8])
  loc_id <- as.numeric(commandArgs()[9])
  
  
  # Run main function
  main(version_tag, loc_id)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
} else {
  begin_time <- Sys.time()
  
  
  # Command line arguments
  version_tag <- '2021_06_24.01'
  version_tag <- '2021_07_12.09'
  loc_id <- 101
  loc_id <- 60137
  
  # Run main function
  main(version_tag, loc_id)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
}
## --------------------------------------------------------------------- ----