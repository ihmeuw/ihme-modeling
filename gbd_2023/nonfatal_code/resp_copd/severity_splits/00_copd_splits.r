### COPD Splits
# DATE
# Part 1: USA conversions - produces a csv of MEPS to GOLD in US that is used in part 2
# Part 2: Run the actual splits: multiply COPD prevalence by severities
# Part 3: Save results

# Part 1 only needs to be run once if GOLD COPD models don't change

### Setup ---------------------------------------------------------------------
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
library(general.utilities, lib.loc = "FILEPATH")
library(tidyverse)


meps <- read.csv("FILEPATH/copd_meps_draws_2019.csv")
dist_data <- meps[, grep("draw", names(meps))]
dist_data <- bind_cols(meps[,1:10], dist_data) # Moving id columns to front

# mapping file to split to appropriate locations
path_to_map <- "FILEPATH/copd_severity_map.csv"
mapping <- read.csv(path_to_map, stringsAsFactors = FALSE)
child_map <- mapping %>% filter(parent == "child")
directory_to_save <- paste0(child_map$directory_to_save)[1]

# cluster details
errors <- "FILEPATH"
user <- "USERNAME"
project = "PROJECT "
q = "CLUSTER"
threads = 10
memory = 20
runtime = "2:00:00"
job_name_1 = "copd_splits"

# Script to split
script_01 <- "FILEPATH/01_copd_splits.R" # Path to script

# Only need to run this once for each GOLD Dismod model update
### Part 1: Convert MEPS to USA GOLD Class ------------------------------------

release_id <- 16
step <- 'iterative'
gbd_ids <- c(3062:3064)
gbd_names <- c("gold1", "gold2", "gold3")
n_draws <- 1000

# get gold class draws for US
gold_copd <- data.frame()
for (i in 1:length(gbd_ids)) {
  df_gold <- get_draws(gbd_id = gbd_ids[i],
                       gbd_id_type = "modelable_entity_id",
                       source = "epi",
                       year_id = 2005, # most data in year 2005
                       location = 102, # united states 
                       sex_id = c(1,2),
                       measure_id = 18,
                       release_id = release_id,
                       n_draws = n_draws,
                       downsample = T)
  df_gold$cause_name <- gbd_names[i]
  gold_copd <- bind_rows(gold_copd, df_gold)
}

### 1a: Convert GOLD Proportions to Severity Proportions ----------------------
df <- reshape_draws(gold_copd, direction = "long")

total_gold <- df %>%
  group_by(age_group_id, location_id, sex_id, year_id, draw) %>%
  summarise(total = sum(value))

df <- df %>% left_join(total_gold)
df$value <- df$value / df$total

dist_data <- reshape_draws(dist_data, draw_name = "draw",
                           draw_value_name = "severity_value",
                           direction = "long")

### 1b: Create conversions from USA 2005 GOLD ---------------------------------

# asymptomatic conversions ----------------------------------------------------
asymptomatic <- df[cause_name == "gold1",] %>% 
  left_join(dist_data[healthstate == "asymptomatic", ]) %>%
  mutate(conversion_value = severity_value / value)

# severe conversions ----------------------------------------------------------
severe <- df[cause_name == "gold3",] %>% 
  left_join(dist_data[healthstate == "copd_sev", ]) %>%
  mutate(conversion_value = severity_value / value)


# Mild severity conversion ----------------------------------------------------
# conversion value is mild severity/sum of mild and moderate healthstates

mild <- dist_data[healthstate == "copd_mild", ] %>%
  mutate(conversion_value = severity_value / 
           (dist_data[healthstate == "copd_mild", severity_value] +
               dist_data[healthstate == "copd_mod", severity_value]) )

# Expand table horizontal to all age and sex ids with inner join
mild <- inner_join(mild[1:n_draws,], 
                   data.frame(expand.grid(age_group_id = unique(severe$age_group_id),
                              sex_id = c(1,2), 
                              healthstate = "copd_mild")))

# Moderate conversions --------------------------------------------------------
# conversion value is moderate severity/sum of mild and moderate healthstates
moderate <- dist_data[healthstate == "copd_mod",] %>%
  mutate(conversion_value = severity_value / 
           (dist_data[healthstate == "copd_mild", severity_value] +
              dist_data[healthstate == "copd_mod", severity_value]) )

# Expand table horizontal to include age and sex id
moderate <- inner_join(moderate[1:n_draws,], 
                   data.frame(expand.grid(age_group_id = unique(severe$age_group_id),
                                          sex_id = c(1,2), 
                                          healthstate = "copd_mod")))

### 1c: Save the conversions --------------------------------------------------
meps_conversion <- bind_rows(asymptomatic, mild, moderate, severe)
write.csv(meps_conversion, 
          "FILEPATHS/meps_conversion_2019.csv",
          row.names = F)

### Part 2: Run the full splits -----------------------------------------------
### Subset locs to 1-5 for testing purposes
locations <- get_location_metadata(location_set_id = 35, gbd_round_id = 7, decomp_step = "step3")
locs <- c(locations$location_id, 20, 189)
locs <- locs[1:5] # for testing

shell <- "FILEPATHS/execRscript.sh"


print('Starting splits -----------------------------------')
# Only need to pass the mapping file to script
arguments_for_script <- c(path_to_map, 
                          "FILEPATHS/meps_conversion_2019.csv")


# Submits jobs parallel by location
source("FILEPATHS/submit_parallel_jobs.R")

job.array.master(tester = F,
                 paramlist = locs,
                 username = user,
                 project = project,
                 threads = threads,
                 mem_free = memory,
                 runtime = runtime,
                 errors = errors,
                 q = q,
                 jobname = job_name_1,
                 childscript = script_01,
                 shell = shell,
                 args = arguments_for_script)


### Part 3: Save results ------------------------------------------------------
# Can run this immediately and will qhold until splits are finished

### Save results --------------------------------------------------------------
source("FILEPATHS/save_results_function.R")

for (i in 1:nrow(child_map)) {
  directory_to_save <- paste0(child_map$directory_to_save[i], child_map$target_me_id[i], "/")
  
  save_results_function(path_to_errors = "FILEPATHS",
                        decomp_step = 'iterative',
                        gbd_round_id = 7,
                        input_directory = directory_to_save,
                        input_file_pattern = "{location_id}.csv",
                        id_to_save = child_map$target_me_id[i],
                        bundle_id = child_map$bundle_id[i],
                        crosswalk_version_id = child_map$crosswalk_version_id[i],
                        description = "Saving_updated_processing",
                        measure_id = "5,6",
                        sex_id = c(1,2),
                        project = "proj_yld",
                        best = F,
                        run_after_jobs = job_name_1,
                        shell = shell,
                        cod_or_epi = "epi")
  
}







