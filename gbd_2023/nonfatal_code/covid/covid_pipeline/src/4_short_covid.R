## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 4_short_covid.R
## Description: Primary working code to calculate & produce estimates of 
##              short-term non-fatal burden due to COVID-19. Numbered comments 
##              correspond to documentation on this HUB page:
##              ADDRESS
## Contributors: NAME, NAME, NAME
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----

# Dynamically set user specific repo base where nf_covid is located
.repo_base <-
  strsplit(
    whereami::whereami(path_expand = TRUE),
    "nf_covid"
  )[[1]][1]

source(file.path(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k, 'FILEPATH/get_ids.R'))
source(paste0(roots$k, 'FILEPATH/get_location_metadata.R'))
source(paste0(roots$k, 'FILEPATH/get_population.R'))
source(paste0(roots$k, 'FILEPATH/get_draws.R'))

## --------------------------------------------------------------------- ----


## Data Processing Functions ------------------------------------------- ----


# input_path <- 'FILEPATH'
# loc_id <- 33

main <- function(loc_id, loc_name, output_version, input_path,
                 estimation_years, age_groups, location_set_id, release_id) {

  input_file_path <- file.path(input_path, paste0(loc_id, "_covid_cases_at_risk.csv"))
  
  cat('Reading in covid counts data...\n')
  ## Read in covid counts data --------------------------------------------- ----
    # Infections
    # Hospital admissions
    # ICU admissions
    # Proportion asymptomatic
    # Asymptomatic incidence
    
  covid_counts <- fread(input_file_path)
  
  # Step 2.b
  # Calculate asymptomatic_prevalence = asymptomatic_incidence * asymptomatic_duration
  covid_counts[, asymp_prev := asymp_inc * roots$defaults$asymp_duration + (infections - asymp_inc) * roots$defaults$incubation_period]
  
  cat('Calculating mild/moderate incidence & prevalence...\n')
  ## Mild/Moderate Incidence & Prevalence -------------------------------- ----
  
  # Step 3.a
  # Setup mild/moderate incidence as fast-forwarded presymp_inc [the incubation_period duration]
  covid_counts[, midmod_inc := infections - asymp_inc]
  # set asymp_inc as infections so that total infections gets saved as incidence for that ME
  covid_counts[, asymp_inc := infections]

  # Step 3.e
  # Calculate mild/moderate prevalence = ((midmod_inc - hsp_admit[6 days later] - community deaths[19 days later]) * midmod_duration_no_hsp) + 
  #                                       (hsp_admit[6 days later] * symp_to_hsp_admit_duration) +
  #                                       (community deaths[19 days later] * midmod_to_severe_no_hsp)
  covid_counts[, midmod_prev := ((midmod_inc - hsp_admit) * roots$defaults$midmod_duration_no_hsp) +
                           (hsp_admit * roots$defaults$symp_to_hsp_admit_duration)]
  
  cat('Calculating hospitalization prevalence...\n')
  ## Hospitalization Prevalence ------------------------------------------ ----

  # Step 4.d
  # Calculate hospital inc
  covid_counts[, hospital_inc_no_icu_no_death := hsp_admit - icu_admit - hsp_deaths]
  covid_counts[, hospital_inc_no_icu_death := hsp_deaths]
  covid_counts[, hospital_inc_icu_no_death := icu_admit - icu_deaths]
  covid_counts[, hospital_inc_icu_death := icu_deaths]
  
  covid_counts[, hospital_inc := hsp_admit]
  
  # Step 4.f hospital prev
  covid_counts[, hospital_prev_no_icu_no_death := hospital_inc_no_icu_no_death * roots$defaults$hsp_no_icu_no_death_duration]
  covid_counts[, hospital_prev_no_icu_death := hospital_inc_no_icu_death * roots$defaults$hsp_no_icu_death_duration]
  covid_counts[, hospital_prev_icu_no_death := hospital_inc_icu_no_death * roots$defaults$hsp_icu_no_death_duration]
  covid_counts[, hospital_prev_icu_death := hospital_inc_icu_death * roots$defaults$hsp_icu_death_duration]
  
  covid_counts[, hospital_prev := hospital_prev_no_icu_no_death + 
              hospital_prev_no_icu_death + 
              hospital_prev_icu_no_death + 
              hospital_prev_icu_death]
  
  cat('Calculating ICU prevalence...\n')
  # Calculating ICU prevalence...
  ## ICU Prevalence ------------------------------------------------------ ----

  # Step 5.d
  # Calculate ICU prevalence = ((icu_admit - icu_deaths[3 days later]) * icu_no_death_duration) +
  #                             (icu_deaths[3 days later] * icu_to_death_duration)
  covid_counts[, icu_inc_no_death := icu_admit - icu_deaths]
  covid_counts[, icu_prev_no_death := icu_inc_no_death * roots$defaults$icu_no_death_duration]
  covid_counts[, icu_prev_death := icu_deaths * roots$defaults$icu_to_death_duration]
  covid_counts[, icu_prev := icu_prev_no_death + icu_prev_death]
  setnames(covid_counts, 'icu_admit', 'icu_inc')
  
  cat('Calculating post-hospital mild/moderate prevalence...\n')
  # Calculating post-hospital mild/moderate prevalence...
  ## Post-Hospital Mild/Moderate Prevalence ------------------------------ ----
  
  # Step 6.f
  # Shift durations
  covid_counts[, post_hsp_midmod_prev := (hospital_inc - icu_inc - hsp_deaths) * roots$defaults$hsp_midmod_after_discharge_duration]
  
  # Step 6.i
  ## mild/moderate prevalence number = mild/moderate prevalence number + post-hospital mild/moderate prevalence
  covid_counts[, midmod_prev := midmod_prev + post_hsp_midmod_prev]
  covid_counts[, post_hsp_midmod_prev := NULL]
  
  cat('Calculating post-icu mild/moderate prevalence...\n')
  # Calculating post-icu mild/moderate prevalence...
  ## Post-ICU Mild/Moderate Prevalence ----------------------------------- ----
  
  # Step 7.d
  # Shift durations
  covid_counts[, post_icu_midmod_prev := (icu_inc - icu_deaths) * roots$defaults$icu_midmod_after_discharge_duration]
  
  
  # Step 7.g
  ## mild/moderate prevalence number = mild/moderate prevalence number + post-ICU mild/moderate prevalence
  covid_counts[, midmod_prev := midmod_prev + post_icu_midmod_prev]
  covid_counts[,  post_icu_midmod_prev := NULL]
  
  cat('Splitting into Mild/Moderate outcomes...\n')
  # Splitting into Mild/Moderate outcomes...
  ## Split Mild/Moderate into respective proportions --------------------- ----
  
  # Step 8
  
  # Pull and merge mild/moderate-split draws

  midmod_split <- fread('FILEPATH/final_midmod_proportion_draws.csv')[, c(roots$draws), with=F]
  
  midmod_split <- melt(midmod_split, measure.vars = roots$draws, 
                       variable.name = 'draw', value.name = 'prop_mod')
  
  covid_counts <- merge(covid_counts, midmod_split, by='draw', all.x = TRUE)
  rm(midmod_split)
  
  
  # Calculate: mild incidence = midmod_inc * (1 - prop_mod)
  #            moderate incidence = midmod_inc * prop_mod
  #            mild prevalence = midmod_prev * (1 - prop_mod)
  #            moderate prevalence = midmod_prev * prop_mod
  covid_counts[, `:=`(mild_inc = midmod_inc * (1 - prop_mod),
                moderate_inc = midmod_inc * prop_mod,
                mild_prev = midmod_prev * (1 - prop_mod),
                moderate_prev = midmod_prev * prop_mod)]
  
  covid_counts[, prop_mod := NULL]
  
  cat('Writing date-specific data for long_covid estimation...\n')
  # Writing date-specific data for long_covid estimation...
  ## Output date-specific data for long_covid ---------------------------- ----
  
  # Step 9
  
  int_o <- paste0('FILEPATH', output_version, 
                  'FILEPATH')
  
  .ensure_dir(int_o)
  
  # Write out single data.frame to csv that includes
    # Mild/moderate
    # Hospital
    # ICU
  fwrite(
    covid_counts[, .(location_id, age_group_id, sex_id, year_id, draw, 
                    infections,
                    midmod_inc,
                    hospital_inc,
                    hsp_deaths,
                    icu_inc,
                    icu_deaths)],
    file = file.path(int_o, paste0(loc_name, '_', loc_id, '.csv'))
  )
  
  cat('Calculating rates...\n')
  ## Calculate rates ----------------------------------------------------- ----
  
  # Step 11
  
  ###################################################################################################################################################################################
  ###################################################################################################################################################################################
  ###### TO DO!!!!!!!!!!!!!!! (also change in 6_long_covid.R)
  ###################################################################################################################################################################################
  ###################################################################################################################################################################################
  # need to pull different population numbers if forecasting past what GBD produces for FHS ... currently will give 0's in final results for 2023 because the following
  #   get_population() call can only pull up to 2022
  pop <- get_population(age_group_id = age_groups,
                        single_year_age = FALSE, 
                        location_id = loc_id, 
                        location_set_id = location_set_id, 
                        year_id = unique(covid_counts$year_id), 
                        sex_id = c(1,2), 
                        release_id = release_id, 
                        status = 'best'
                        )[,c('location_id','year_id','sex_id','population','age_group_id')]
  
  ages <- get_ids('age_group')
  
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
    asymp[age_group_id %in% c(388, 389), age_group_id := 4]
    asymp[age_group_id %in% c(238, 34), age_group_id := 5]
    asymp <- asymp[,.(sum(asymp_prev), sum(asymp_inc)), 
                   by = c('location_id', 'year_id', 'sex_id', 'age_group_id', 'draw')]
    setnames(asymp, c('V1', 'V2'), c('asymp_prev', 'asymp_inc'))
    
    mild[age_group_id %in% c(388, 389), age_group_id := 4]
    mild[age_group_id %in% c(238, 34), age_group_id := 5]
    mild <- mild[,.(sum(mild_prev), sum(mild_inc)), 
                   by = c('location_id', 'year_id', 'sex_id', 'age_group_id', 'draw')]
    setnames(mild, c('V1', 'V2'), c('mild_prev', 'mild_inc'))
    
    moderate[age_group_id %in% c(388, 389), age_group_id := 4]
    moderate[age_group_id %in% c(238, 34), age_group_id := 5]
    moderate <- moderate[,.(sum(moderate_prev), sum(moderate_inc)), 
                   by = c('location_id', 'year_id', 'sex_id', 'age_group_id', 'draw')]
    setnames(moderate, c('V1', 'V2'), c('moderate_prev', 'moderate_inc'))
    
    hospital[age_group_id %in% c(388, 389), age_group_id := 4]
    hospital[age_group_id %in% c(238, 34), age_group_id := 5]
    hospital <- hospital[,.(sum(hospital_prev), sum(hospital_inc)), 
                   by = c('location_id', 'year_id', 'sex_id', 'age_group_id', 'draw')]
    setnames(hospital, c('V1', 'V2'), c('hospital_prev', 'hospital_inc'))
    
    icu[age_group_id %in% c(388, 389), age_group_id := 4]
    icu[age_group_id %in% c(238, 34), age_group_id := 5]
    icu <- icu[,.(sum(icu_prev), sum(icu_inc)), 
                   by = c('location_id', 'year_id', 'sex_id', 'age_group_id', 'draw')]
    setnames(icu, c('V1', 'V2'), c('icu_prev', 'icu_inc'))
    
    age_groups <- unique(asymp$age_group_id)
  }
  
  # Calculate asymptomatic inc & prev rate
  covid_counts <- merge(covid_counts, pop, 
                        by=c('location_id','year_id','sex_id','age_group_id'), 
                        all.x = TRUE)
  
  covid_counts[, asymp_inc_rate := (asymp_inc/population)]
  covid_counts[, asymp_prev_rate := (asymp_prev/population)]
  # Convert from per person day to per person year
  covid_counts[, asymp_prev_rate := asymp_prev_rate / 365]
  
  # Calculate mild inc & prev rate
  covid_counts[, mild_inc_rate:= (mild_inc/population)]
  covid_counts[, mild_prev_rate:= (mild_prev/population)]
  # Convert from per person day to per person year
  covid_counts[, mild_prev_rate := mild_prev_rate / 365]

  
  # Calculate moderate inc & prev rate
  covid_counts[, moderate_inc_rate:= (moderate_inc/population)]
  covid_counts[, moderate_prev_rate:= (moderate_prev/population)]
  # Convert from per person day to per person year
  covid_counts[, moderate_prev_rate := moderate_prev_rate / 365]
  
  # Calculate hospitalization inc & prev rate
  covid_counts[, hospital_inc_rate := (hospital_inc/population)]
  covid_counts[, hospital_prev_rate := (hospital_prev/population)]
  # Convert from per person day to per person year
  covid_counts[, hospital_prev_rate := hospital_prev_rate / 365]

  
  # Calculate ICU inc & prev rate
  covid_counts[, icu_inc_rate := (icu_inc/population)]
  covid_counts[, icu_prev_rate := (icu_prev/population)]
  # Convert from per person day to per person year
  covid_counts[, icu_prev_rate := icu_prev_rate / 365]

  rm(pop)
  
  # Cap prevalence rates to 1
  covid_counts[asymp_prev_rate > 1, asymp_prev_rate := 1]
  covid_counts[mild_prev_rate > 1, mild_prev_rate := 1]
  covid_counts[moderate_prev_rate > 1, moderate_prev_rate := 1]
  covid_counts[hospital_prev_rate > 1, hospital_prev_rate := 1]
  covid_counts[icu_prev_rate > 1, icu_prev_rate := 1]
  
  ## --------------------------------------------------------------------- ----

  cat('Calculating YLDs...\n')
  ## Calculate YLDs ------------------------------------------------------ ----
  
  # Step 13
  
  # Read in disability weight 
  DW <- fread(paste0(roots$disability_weight, 'dw.csv'))
  
  
  # Calculate asymptomatic YLD
  covid_counts[, asymp_YLD := asymp_prev_rate * 0]
  
  # Calculate mild YLD
  mild_DW <- melt(DW[DW$hhseqid == roots$hhseq_ids$short_mild],
                  measure.vars = roots$draws,
                  value.name = 'mild_DW',
                  variable.name = 'draw')[,c('draw','mild_DW')]
  
  covid_counts <- merge(covid_counts, mild_DW, by='draw', all.x = TRUE)
  covid_counts[, mild_YLD := mild_prev_rate*mild_DW]

  # Calculate moderate YLD
  moderate_DW <- melt(DW[DW$hhseqid == roots$hhseq_ids$short_moderate],
                      measure.vars = roots$draws,
                      value.name = 'moderate_DW',
                      variable.name = 'draw')[,c('draw','moderate_DW')]

  covid_counts <- merge(covid_counts, moderate_DW, by='draw', all.x = TRUE)
  covid_counts[, moderate_YLD := moderate_prev_rate*moderate_DW]

  # Calculate hospitalization YLD
  hospital_DW <- melt(DW[DW$hhseqid == roots$hhseq_ids$short_severe],
                      measure.vars = roots$draws,
                      value.name = 'hospital_DW',
                      variable.name = 'draw')[,c('draw','hospital_DW')]

  covid_counts <- merge(covid_counts, hospital_DW, by='draw', all.x = TRUE)
  covid_counts[, hospital_YLD := hospital_prev_rate * hospital_DW]

  # Calculate ICU YLD
  icu_DW <- melt(DW[DW$hhseqid == roots$hhseq_ids$short_icu],
                 measure.vars = roots$draws,
                 value.name = 'icu_DW',
                 variable.name = 'draw')[,c('draw','icu_DW')]

  covid_counts <- merge(covid_counts, icu_DW, by='draw', all.x = TRUE)
  covid_counts[, icu_YLD := icu_prev_rate * icu_DW]

  rm(DW, mild_DW, moderate_DW, hospital_DW, icu_DW)
  
  ## --------------------------------------------------------------------- ----
  
  
  cat('\nSaving datasets and running diagnostics...\n')
  ## Save to intermediate location --------------------------------------- ----
  
  # Step 14
  save_dataset(dt = covid_counts[, .(location_id, year_id, sex_id, age_group_id, draw, 
                                     asymp_prev, asymp_inc, population, asymp_inc_rate, 
                                     asymp_prev_rate, asymp_YLD)],
               filename = 'asymp', 
               stage = 'stage_1', 
               output_version = output_version, 
               loc_id = loc_id, 
               loc_name = loc_name)
  
  save_dataset(dt = covid_counts[, .(location_id, year_id, sex_id, age_group_id, draw, 
                                     mild_prev, mild_inc, population, mild_inc_rate, 
                                     mild_prev_rate, DW = mild_DW, mild_YLD)],
               filename = 'mild', 
               stage = 'stage_1', 
               output_version = output_version, 
               loc_id = loc_id, 
               loc_name = loc_name)
  
  save_dataset(dt = covid_counts[, .(location_id, year_id, sex_id, age_group_id, draw, 
                                     moderate_prev, moderate_inc, population, moderate_inc_rate, 
                                     moderate_prev_rate, DW = moderate_DW, moderate_YLD)],
               filename = 'moderate', 
               stage = 'stage_1', 
               output_version = output_version, 
               loc_id = loc_id, 
               loc_name = loc_name)
  
  save_dataset(dt = covid_counts[, .(location_id, year_id, sex_id, age_group_id, draw, 
                                     hospital_prev, hospital_inc, population, hospital_inc_rate, 
                                     hospital_prev_rate, DW = hospital_DW, hospital_YLD)],
               filename = 'hospital', 
               stage = 'stage_1', 
               output_version = output_version, 
               loc_id = loc_id, 
               loc_name = loc_name)
  
  save_dataset(dt = covid_counts[, .(location_id, year_id, sex_id, age_group_id, draw, 
                                     icu_prev, icu_inc, population, icu_inc_rate, 
                                     icu_prev_rate, DW = icu_DW, icu_YLD)],
               filename = 'icu', 
               stage = 'stage_1', 
               output_version = output_version, 
               loc_id = loc_id, 
               loc_name = loc_name)

  cat('\nSaving for EPI database...\n')
  ## Save for EPI database ----------------------------------------------- ----
  
  # Step 15
  
  locs <- get_location_metadata(
    location_set_id = location_set_id,
    release_id = release_id
  )[, c(
    "location_id", "location_ascii_name",
    "region_id", "region_name", "super_region_name",
    "super_region_id", "most_detailed"
  )]
  
  i_base <- paste0(get_core_ref('data_output', 'stage_1'), output_version, 
                   'FILEPATH')
  
  
  for (measure in c('asymp', 'mild', 'moderate', 'hospital', 'icu')) {
    # Finalize dataset
    # 1. Reads source feather file
    # 2. Subset columns
    # 3. Add years for years in all_gbd_estimation_years that are not in the dataset
      # and set to 0 for inc_rate and pre_rate
    # 4. Reshape
    dt <- .finalize_data(measure, i_base, loc_name, loc_id)
    
    
    # Save to final location
    save_epi_dataset(dt = dt[measure_id == 5, !c('measure_id')],
                     stage = 'final', 
                     output_version = output_version,
                     me_name = measure, 
                     measure_id = 5, 
                     loc_id = loc_id, 
                     loc_name = loc_name, 
                     l = locs)
    
    save_epi_dataset(dt = dt[measure_id == 6, !c('measure_id')],
                     stage = 'final', 
                     output_version = output_version,
                     me_name = measure, 
                     measure_id = 6, 
                     loc_id = loc_id, 
                     loc_name = loc_name, 
                     l = locs)
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
    "--input_path",
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
  list2env(args, envir = environment())
  
  message("Arguments passed were:")
  for (arg in names(args)) {
    message(paste0(arg, ": ", get(arg)))
  }
  
  # Parse estimation_years and age_groups from comma separated character to integer vector
  estimation_years <- as.vector(as.numeric(unlist(strsplit(estimation_years, ","))))
  age_groups <- as.vector(as.numeric(unlist(strsplit(age_groups, ","))))
  
  # Pull location ascii name
  loc_name <- get_location_metadata(location_set_id = location_set_id, 
                                                 release_id = release_id)[location_id==loc_id, location_ascii_name]
  cat('\n')
  existing_vars <- ls()
  tot_size <- check_size(existing_vars)
  test <- 0
  
  # Run Data Processing Functions
  main(
    loc_id = loc_id,
    loc_name = loc_name,
    output_version = output_version,
    input_path = input_path,
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
  loc_id <- 43 
  input_path <- 'FILEPATH'
  output_version <- '2024-08-05-test'
  location_set_id <- 35
  release_id <- 16
  
  # Pull location ascii name
  loc_name <- get_location_metadata(location_set_id = location_set_id, 
                                                 release_id = release_id
                                                 )[location_id==loc_id, location_ascii_name]
  
  
  # Print out args
  cat(paste0('  loc_id: ', loc_id, '\n  loc_name: ', loc_name, 
             '\n  output_version: ', output_version, '\n'))
  
  
  cat('\n')
  existing_vars <- ls()
  tot_size <- check_size(existing_vars)
  
  
  estimation_years <- c(2020, 2021, 2022, 2023)
  source(paste0(roots$k, 'FILEPATH/get_age_metadata.R'))
  age_groups <- get_age_metadata(release_id = release_id)$age_group_id
  
  # Run Data Processing Functions
  main(
    loc_id = loc_id,
    loc_name = loc_name,
    output_version = output_version,
    input_path = input_path,
    estimation_years = estimation_years,
    age_groups = age_groups,
    location_set_id = location_set_id,
    release_id = release_id
  )
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  cat(paste0('Memory used: ', tot_size, ' GB\n'))
}
## --------------------------------------------------------------------- ----