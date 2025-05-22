
# load in cluster params --------------------------------------------------

if (interactive()) {
  task_id <- 0
  params_path <- file.path(getwd(), 'ensemble/params.rds')
} else {
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  command_args <- commandArgs(trailingOnly = TRUE)
  params_path <- command_args[1]
}

params <- readRDS(params_path)

# load in demographics list -----------------------------------------------

demographics_list <- ihme::get_demographics(
  gbd_team = 'epi',
  release_id = params$gbd_rel_id
)

# get total population ----------------------------------------------------

total_pop <- ihme::get_population(
  age_group_id = params$age_group_id,
  location_id = demographics_list$location_id,
  sex_id = params$sex_id,
  year_id = params$year_id,
  population_group_id = 1,
  release_id = params$gbd_rel_id
)

# get pregnant popluation -------------------------------------------------

cc_preg_file_path <- 'FILEPATH/GBD2023_pregnancy_pop_draws_summarized.csv'

WRA_AGE_GROUP_ID <- 7:15
WRA_SEX_ID <- 2

preg_pop <- data.table::fread(
  file = cc_preg_file_path
) |>
  dplyr::filter(
    age_group_id %in% WRA_AGE_GROUP_ID &
      sex_id == WRA_SEX_ID & 
      location_id %in% demographics_list$location_id &
      year_id %in% demographics_list$year_id
  )

# merge total and pregnant populations and get prop pregnant --------------

all_pop <- merge.data.frame(
  x = total_pop,
  y = preg_pop,
  by = c('age_group_id', 'location_id', 'sex_id', 'year_id'),
  suffixes = c('.total', '.pregnant')
) |>
  dplyr::mutate(
    prop_pregnant = population.pregnant / population.total
  )

# save out data -----------------------------------------------------------

fst::write.fst(
  x = all_pop,
  path = params$pregnancy_population_file
)
