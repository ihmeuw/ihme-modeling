message("Starting to COPD Splits ----------------------------------")
source("FILEPATH/submit_parallel_jobs.R")

### Getting arguments from job array (passed in the function) -----------------
getit <- job.array.child()
args <- commandArgs(trailingOnly=T)
loc <- getit[[1]] # grab the unique PARAMETERS for this task id
path_to_map <- args[2] # starts on argument 7
path_to_usa_copd_conversion_values <- args[3]

message("Path to mapping ", path_to_map)
message("Location ", loc)
message("Path to MEPS ", path_to_usa_copd_conversion_values)

code_dir <- "FILEPATH"
central_dir <- "FILEPATH"

# source(paste0(code_dir, "squeeze_split_save_functions.R"))
source(paste0(central_dir, "get_draws.R"))
source("FILEPATH/reshape_draws.R") # needs to source package when error resolved
# source(paste0(central_dir, "get_model_results.R"))
# source(paste0(central_dir, "get_age_metadata.R"))
library(tidyverse)
library(data.table)
library(ggplot2)

mapping <- read.csv(path_to_map, stringsAsFactors = FALSE)
child_map <- mapping %>% filter(parent == "child")

round <- unique(child_map$gbd_round_id)
step <- unique(child_map$decomp_step)
n_draws <- as.numeric(unique(child_map$n_draws))
downsample <- unique(child_map$downsample)
gbd_ids <- c(3062:3064)
gbd_names <- c("gold1", "gold2", "gold3")

message(round)
message(step)
message(n_draws)
message(gbd_ids)
message(gbd_names)

meps_conversion <- read.csv(path_to_usa_copd_conversion_values) %>% data.table()
# ### Get full gold draws now ---------------------------------------------------
message("Getting gold draws")
gold_copd <- data.frame()
for (i in 1:length(gbd_ids)) {
  message(gbd_ids[i])
  message(gbd_names[i])
  df_gold <- get_draws(gbd_id = gbd_ids[i],
                       gbd_id_type = "modelable_entity_id",
                       source = "epi",
                       location_id = loc,
                       sex_id = c(1,2),
                       measure_id = 18,
                       gbd_round_id = round,
                       decomp_step = step,
                       n_draws = n_draws,
                       downsample = T)
  df_gold$cause_name <- gbd_names[i]
  gold_copd <- bind_rows(gold_copd, df_gold)
}

df2 <- reshape_draws(gold_copd, direction = "long") %>%
  filter(!age_group_id %in% c(27, 33, 164))

total_gold2 <- df2 %>%
  group_by(age_group_id, location_id, sex_id, year_id, draw) %>%
  summarise(total = sum(value))

df2 <- df2 %>% left_join(total_gold2) %>% data.table()
df2$value <- df2$value / df2$total


### line 97 - 109 in the stata code
message("Starting gold to meps conversion")
severe_gold <- df2[cause_name == "gold3", ] %>% 
  left_join(meps_conversion[healthstate == "copd_sev",c("age_group_id", "sex_id", "draw", "conversion_value")]) %>%
  mutate(value_to_multiply_copd_prev = value * conversion_value,
         healthstate = "copd_sev")

asymptomatic_gold <- df2[cause_name == "gold1", ] %>% 
  left_join(meps_conversion[healthstate == "asymptomatic",c("age_group_id", "sex_id", "draw", "conversion_value")]) %>%
  mutate(value_to_multiply_copd_prev = value * conversion_value,
         healthstate = "asymptomatic")

process_mild_moderate <- severe_gold[,c("age_group_id", "sex_id", "year_id", "location_id", "draw", "value_to_multiply_copd_prev")] %>%
  mutate(severe_asymp_total = value_to_multiply_copd_prev + asymptomatic_gold$value_to_multiply_copd_prev)

mild_gold <- process_mild_moderate %>%
  left_join(meps_conversion[healthstate == "copd_mild",c("age_group_id","sex_id", "draw", "conversion_value")])

mild_gold$value_to_multiply_copd_prev <- (1 - mild_gold$severe_asymp_total) * mild_gold$conversion_value
mild_gold$healthstate <- "copd_mild"

moderate_gold <- process_mild_moderate %>%
  left_join(meps_conversion[healthstate == "copd_mod",c("age_group_id","sex_id", "draw", "conversion_value")])

moderate_gold$value_to_multiply_copd_prev <- (1 - moderate_gold$severe_asymp_total) * moderate_gold$conversion_value
moderate_gold$healthstate <- "copd_mod"


# ### Multiply by prevalence of COPD --------------------------------------------
message("Getting prevalence of COPD")
copd_prevalence <- get_draws(gbd_id = 24543,
                             gbd_id_type = "modelable_entity_id",
                             source = "epi",
                             location_id = loc,
                             sex_id = c(1,2),
                             measure_id = c(5,6),
                             gbd_round_id = round,
                             decomp_step = step,
                             n_draws = n_draws,
                             downsample = T)

copd_prevalence_long <- reshape_draws(copd_prevalence, direction = "long") %>%
  filter(!age_group_id %in% c(22, 27, 33, 164))

### Get COPD prevalence by severities -----------------------------------------
message("Multiplying prevalence by severities")
severe_upload <- copd_prevalence_long %>%
  left_join(severe_gold[, c("year_id", "age_group_id", "sex_id", "location_id", "draw",
                            "value_to_multiply_copd_prev")]) %>%
  mutate(value = value * value_to_multiply_copd_prev,
         value = ifelse(measure_id == 6, 0, value), #incidence 0
         value = ifelse(value < 0, 0, value),
         modelable_entity_id = 1875) %>%
  dplyr::select(-value_to_multiply_copd_prev, -model_version_id)

moderate_upload <- copd_prevalence_long %>%
  left_join(moderate_gold[, c("year_id", "age_group_id", "sex_id", "location_id", "draw",
                              "value_to_multiply_copd_prev")]) %>%
  mutate(value = value * value_to_multiply_copd_prev,
         value = ifelse(measure_id == 6, 0, value), # incidence 0
         value = ifelse(value < 0, 0, value),
         modelable_entity_id = 1874) %>%
  dplyr::select(-value_to_multiply_copd_prev, -model_version_id)

mild_upload <- copd_prevalence_long %>%
  left_join(mild_gold[, c("year_id", "age_group_id", "sex_id", "location_id", "draw",
                          "value_to_multiply_copd_prev")]) %>%
  mutate(value = value * value_to_multiply_copd_prev,
         value = ifelse(measure_id == 6, 0, value), #incidence 0
         value = ifelse(value < 0, 0, value),
         modelable_entity_id = 1873) %>%
  dplyr::select(-value_to_multiply_copd_prev, -model_version_id)

# all incidence goes to asymptomatic
asymptomatic_upload <- copd_prevalence_long %>%
  left_join(asymptomatic_gold[, c("year_id", "age_group_id", "sex_id", "location_id", "draw",
                                  "value_to_multiply_copd_prev")]) %>%
  mutate(value = ifelse(measure_id == 5, value * value_to_multiply_copd_prev, value),
         value = ifelse(value < 0, 0, value),
         modelable_entity_id = 3065) %>%
  dplyr::select(-value_to_multiply_copd_prev, -model_version_id)

### Write files to CSV --------------------------------------------------------
message("Saving files")
final_upload <- bind_rows(asymptomatic_upload, mild_upload, moderate_upload, severe_upload) %>%
  reshape_draws(direction = "wide") %>% data.table()

for (i in 1:nrow(child_map)) {
  directory_to_save <- paste0(child_map$directory_to_save[i], 
                              child_map$target_me_id[i], "/")
  message("Directory: ", directory_to_save)
  uploading <- final_upload[modelable_entity_id == child_map$target_me_id[i],]
  write.csv(uploading, paste0(directory_to_save, 
                              unique(uploading$location_id), ".csv"))
}

message("Finished")