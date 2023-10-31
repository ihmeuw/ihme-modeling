rm(list = ls(all.names = T))

## Setup
Sys.umask("0002")
Sys.setenv(MKL_VERBOSE = 0)
suppressMessages({library(data.table); library(dplyr); library(parallel);
  library(lubridate); library(ggplot2); library(yaml); library(ggpubr)})
setDTthreads(1)

## Install ihme.covid
library('ihme.covid', lib.loc='FILEPATH')

## Arguments
user <- Sys.info()["user"]
code_dir <- sprintf('FILEPATH', user)
setwd(code_dir)
run_description <- paste('GBD re-run', Sys.time()) # Describe purpose of run here
rake_hosp <- F # Run for long covid pipeline (rake_hosp = F), else (rake_hosp = T)
fhs <- F # Run for FHS location set & time frame (fhs = T), else (fhs = F) 
apply_scalars <- T #Apply year-specific EM scalars (apply_scalars = T) or used scaled deaths from seir directly (apply_scalars = F)
test <- F # Only runs one location

# Time lags
infec_death_lag <- 26
hosp_duration <- 12 
icu_duration <- 3 # Time from hosp to ICU

# Set input versions

# Age/sex split info
age_rates_version <- '2022_02_11.03' 
mort_age_version <- '2022_03_29.01'
rr_version <- '2022_01_19'

# Covid team inputs (all-age/both-sex estimates)
prod_seir_outputs_version <- '2022_03_23.04'
if (fhs==T){
  gbd_seir_outputs_version <-'2022_04_29.02' #fhs covid seir run (notably, estimates extend through 2023)
} else {
  gbd_seir_outputs_version <- '2022_03_25.04' #gbd covid seir run (full gbd loc set)
}
past_infecs_version <-  '2022_03_02.02'
model_inputs_version <- '2022_03_02.03'

# Demographics team inputs
em_scalar_path <- 'FILEPATH/covid_em_scalars-draw-s3-2022-06-01-16-51.csv'

## Paths
# In
age_rates_dir <- file.path("FILEPATH", age_rates_version)
prod_seir_outputs_dir <- file.path("FILEPATH", prod_seir_outputs_version)
gbd_seir_outputs_dir <- file.path("FILEPATH", gbd_seir_outputs_version)
past_infecs_dir <- file.path("FILEPATH", past_infecs_version)
sero_age_pattern_path <- file.path(age_rates_dir, "seroprev_preds_5yr.csv")
mort_age_pattern_path <- paste0('FILEPATH', mort_age_version, '/mortality_agepattern_preds_byloc_5yr.csv')
ihr_age_pattern_path <- file.path(age_rates_dir, "hir_preds_5yr.csv")
sub_locs_path <- file.path(past_infecs_dir, "sub_locs.csv")
icu_prop_path <- "FILEPATH/final_prop_icu_among_hosp.csv"
path_pop <- "FILEPATH/all_populations.csv"
path_locmeta <- paste0("FILEPATH", model_inputs_version, "/locations/modeling_hierarchy.csv")
deaths_rr_path <- paste0('FILEPATH', rr_version, '/hosps_deaths_global_rr.rds')
hosp_rr_path <- paste0('FILEPATH', rr_version, '/hosps_deaths_global_rr.rds')
                        
# Out
OUTPUT_ROOT <- "FILEPATH"
output_dir <- ihme.covid::get_output_dir(OUTPUT_ROOT, "today")
daily_dir <- file.path(output_dir, "daily")
dir.create(daily_dir)
dir.create(paste0(daily_dir, '/final'))
annual_dir <- file.path(output_dir, "annual")
dir.create(annual_dir)
dir.create(paste0(annual_dir, '/final'))
dir.create(paste0(output_dir, '/props'))
dir.create(paste0(output_dir, '/annual_deaths'))


## Save metadata
yaml_dt <- list(
  age_rates_version =  age_rates_version, 
  prod_seir_outputs_version = prod_seir_outputs_version, 
  gbd_seir_outputs_version = gbd_seir_outputs_version, 
  past_infecs_version = past_infecs_version,
  model_inputs_version = model_inputs_version,
  em_scalar_path = em_scalar_path,
  run_description = run_description, 
  fhs = fhs, 
  rake_hosp = rake_hosp,
  apply_scalars = apply_scalars
) 
yaml::write_yaml(yaml_dt, file.path(output_dir, "metadata.yaml"))


## Functions
source(file.path("FILEPATH/get_population.R"))
source(file.path("FILEPATH/get_age_metadata.R"))
source(file.path("FILEPATH/get_location_metadata.R"))
source(file.path(code_dir, "sex_split.R"))
source(file.path(code_dir, "gbd_age_rates.R"))
source(file.path(code_dir, 'mort_sero_age_pattern.R'))
source(file.path(code_dir, "prep_data.R"))
source(file.path(code_dir, "calc_loc_age_sex_ensembles.R"))
source(file.path(code_dir, "synthesize_measure.R"))
source(file.path(code_dir, "prep_ifr_gbd_locs.R")) 
source(file.path(code_dir, "save_all_props.R"))


## Tables
message("Loading tables...")

covid_hierarchy <- fread(paste0("FILEPATH", model_inputs_version,  "/locations/gbd_analysis_hierarchy.csv")) #union location hierarchy with most detailed covid and gbd locs

if (fhs==T){
  gbd_hierarchy <- get_location_metadata(location_set_id = 39, release_id = 9)
} else {
  gbd_hierarchy <- get_location_metadata(location_set_id = 35, release_id = 9)
}

age_table <- get_age_metadata(gbd_round_id = 7)
pop <- get_population(
  age_group_id = age_table$age_group_id,
  sex_id = c(1, 3),
  location_id = -1,
  year_id = 2020,
  gbd_round_id = 7,
  decomp_step = "iterative",
)

# Locs estimated subnationally for COVID but not GBD (estimates for these parent locs only available from special GBD run)
sub_locs <- c(101, 570, 81, 92) #Canada, Washington State, Germany, Spain


## Prep data
message("Prepping data...")

prep_ifr_gbd_locs(age_rates_dir, gbd_hierarchy, output_dir)
ifr_age_pattern_path <- file.path(output_dir, "ifr_preds_5yr_byloc.csv")

dt <- prep_data(deaths_rr_path, mort_age_pattern_path, sero_age_pattern_path, gbd_seir_outputs_dir,
                prod_seir_outputs_dir, ifr_age_pattern_path, ihr_age_pattern_path,
                path_pop, path_locmeta, pop, age_table, icu_prop_path,
                gbd_hierarchy, covid_hierarchy, sub_locs, hosp_duration, icu_duration, infec_death_lag, fhs, apply_scalars)

saveRDS(dt, file.path(output_dir, "data.rds"))


## Save age-sex specific deaths proportions and sex-specific death, hosp, infec props
message("Saving proportions...")
save_props(dt, output_dir)


## Location-specific preparation of deaths and infections
message("Calculating deaths and infections by location...")
# The location list is saved so the array job knows which locations to run
loc_list <- gbd_hierarchy[most_detailed==1]$location_id
write.csv(data.table(location_id = loc_list), file.path(output_dir, "locs.csv"), row.names = F)

# Calculate infections to deaths
message(' Calculating measures...')
if(test) {
  calc_loc_age_sex(162, dt, output_dir, rake_hosp=FALSE, fhs=TRUE, apply_scalars=TRUE) 
} else {
  # Launch jobs
  user <- Sys.info()[["user"]]
  r_shell <- 'FILEPATH/execRscript.sh'
  queue <- 'all.q'
  script <- paste0(code_dir, 'qsub_loc.r')
  n_jobs <- length(loc_list)
  memory <- '20G'
  threads <- '1'
  time <- '01:00:00'
  name <- 'gbd'
  archive <- T
  error_dir <- paste0("FILEPATH", Sys.info()[['user']], "/errors")
  slurm_output_dir <- paste0("FILEPATH", Sys.info()[['user']], "/output")
  args <- paste0('--outputs-directory ', output_dir, ' --rake_hosp ', rake_hosp, ' --fhs ', fhs, ' --apply_scalars ', apply_scalars)

  command <-  paste("sbatch",
                    "-J", name, 
                    "--mem", memory,
                    "-c", threads,
                    "-t", time,
                    "-A proj_covid", 
                    "-p", queue,
                    "-C archive",
                    "-a", paste0("1-", n_jobs, "%300"),
                    "-e", paste0(error_dir, "/%x.e%j"),
                    "-o", paste0(slurm_output_dir, "/%x.o%j"),
                    "--parsable",
                    sep = " ") 

  ## launch job
  jid <- system(paste(command, r_shell, "-s", script, paste(args, collapse = " ")), intern = T)

}

