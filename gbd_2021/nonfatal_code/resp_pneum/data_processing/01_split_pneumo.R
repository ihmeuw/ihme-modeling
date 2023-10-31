message("Starting to plot pneumos ----------------------------------")



source("FILEPATH/submit_parallel_jobs.R")

### Getting arguments from job array (passed in the function) -----------------
getit <- job.array.child()
args <- commandArgs()
loc <- getit[[1]] # grab the unique PARAMETERS for this task id
path_to_map <- args[7] # starts on argument 7
exclusion_value <- args[8]

# args <- commandArgs(trailingOnly = TRUE)
# path_to_map <- args[1]
# loc <- args[2]
# # loc <- 102 # for testing purposes
# path_to_map <- "/ihme/code/ylds/resp/pneumo_cod_map.csv" # for testing purposes

message("Path to mapping ", path_to_map)
message("Location ", loc)

code_dir <- "FILEPATH"
central_dir <- "FILEPATH"

source("FILEPATH/custom_process_functions_2019.R") #flag problem with package
source(paste0(code_dir, "squeeze_split_save_functions.R")) #flag problem with package

source(paste0(central_dir, "get_draws.R"))
source(paste0(central_dir, "get_model_results.R"))
source(paste0(central_dir, "get_age_metadata.R"))
library(tidyverse)
library(ggplot2)

mapping <- read.csv(path_to_map, stringsAsFactors = FALSE)

### Getting hyperparams -------------------------------------------------------
# This is where errors are most likely to appear
# Causes have different requirements for get draws
message("Getting parameters from mapping")

age_ids <- get_age_metadata(age_group_set_id = mapping$age_group_set[1], 
                            gbd_round_id = mapping$gbd_round_id[1])

locations_meta <- get_location_metadata(location_set_id = 9, 
                                        gbd_round_id = mapping$gbd_round_id[1], 
                                        decomp_step = mapping$decomp_step[1])

locs_meta <- locations_meta %>% dplyr::select(location_id, level, location_ascii_name)
ages_meta <- age_ids %>% dplyr::select(age_group_id, age_group_years_start)

sex_ids <- as.numeric(unlist(strsplit(as.character(mapping$sex_id[1]),',')))

measure_ids <- as.numeric(unlist(strsplit(as.character(mapping$measure_id[1]),',')))

year_ids <- as.numeric(unlist(strsplit(as.character(mapping$year_id[1]),',')))

draws <- mapping$n_draws[1]
downsampling <- mapping$downsample[1]
round <- mapping$gbd_round_id[1]
step <- mapping$decomp_step[1]
directory <- mapping$directory_to_save[1]
target_meids <- mapping$target_me_id

message("Filtering parent and child mapping sections")
child_map <- mapping %>% filter(parent == "child")

### Getting draws for all children and parent ---------------------------------
message("Getting draws")
# custom function to parallelize getting draws by MEID. Should be faster.
draws_list <- get_list_of_draws(gbd_id = child_map$gbd_id,
                                gbd_id_type = child_map$gbd_id_type[1],
                                year_id = year_ids,
                                location = loc, 
                                age_id = age_ids$age_group_id,
                                sex_id = sex_ids,
                                source = child_map$source[1],
                                measure_id = measure_ids,
                                status = child_map$status[1],
                                gbd_round_id = round,
                                decomp_step = step,
                                n_draws = draws,
                                downsample = downsampling)

# custom function
draws_list2 <- scale_draws_to_one_and_return_draws(draws_list, return_wide = FALSE)

# bind and add metadata
for (i in 1:length(draws_list2)) {
  draws_list2[[i]]$cause_name <- mapping$cause_name[i]
  draws_list2[[i]] <- draws_list2[[i]] %>% left_join(ages_meta) %>% left_join(locs_meta)
}

data <- data.frame()
for (i in 1:length(draws_list2)) {
  frame <- draws_list2[[i]]
  data <- bind_rows(data, frame)
}

### Introduce Coal Exclusions -------------------------------------------------
# Currently not in use due to issues with cod data and coal covariate
# Current exclusion criteria:
# In best model, if death rate is lower than lowest US state, mark 0
# message("Getting cod data for coal exclusion")
# cod_data <- get_model_results(gbd_id = 513,
#                               gbd_team = "cod",
#                               age_group_id = 22,
#                               gbd_round_id = mapping$gbd_round_id[1],
#                               decomp_step = mapping$decomp_step[1],
#                               location_id = loc)
# 
# df <- cod_data[,c("year_id", "sex_id", "mean_death_rate")]
# 
# exclusion <- df %>%
#   group_by(sex_id) %>%
#   summarise(cnt = mean(mean_death_rate, na.rm = T))
# 
# # if 1, exclude. If 0, don't exclude
# exclude_value <- exclusion_value 
# exclude_males <- ifelse(exclusion$cnt[exclusion$sex_id == 1] < exclude_value, 1, 0)
# exclude_females <- ifelse(exclusion$cnt[exclusion$sex_id == 2] < exclude_value, 1, 0)
# 
# # if less than 
# if (exclude_males == 1) {
#   data$values[data$sex_id == 1 & data$cause_name == "coal"] <- 0
# }
# 
# if (exclude_females == 1) {
#   data$values[data$sex_id == 2 & data$cause_name == "coal"] <- 0
# }

### Coal Exclusion 2: Match GBD 2019 ------------------------------------------
# last <- get_model_results(gbd_team = "epi",
#                           3052,
#                           location_id = loc,
#                           # age_group_id = 22,
#                           gbd_round_id = 6,
#                           measure_id = 5,
#                           decomp_step = 'step4')
# 
# total_coal_in_location <- sum(last$mean)
# 
# if (total_coal_in_location == 0) {
#   data$values[data$cause_name == "coal"] <- 0
# }


### Data Processing Regression --------------------------------------------------

# summarise data
data <- data %>%
  group_by(cause_name, year_id, age_group_years_start, age_group_id, sex_id, location_id) %>%
  summarize(mean = mean(values),
            lower = quantile(values, .025),
            upper = quantile(values, .975)) %>%
  as_tibble()

# Run polynomial regression to smooth out the jumps in cod data
# uncertainty checks
for (i in 1:4) {
  cause <- unique(data$cause_name)[i]
  model <- lm(mean ~ poly(age_group_years_start, 3) + sex_id + poly(year_id, 3), 
              data = data[data$cause_name == cause,])
  
  final <- predict(model, 
                   new.data = data[data$cause_name == cause, 
                                    c("age_group_years_start", 
                                      "sex_id", "year_id")])
  
  # No negative proportions
  final[final < 0] <- 0
  
  data[data$cause_name == cause, c("predicted")] <- final
  
  
}

# Rescale the proportions to 1 again with the linear regression predictions
sum_total <- data %>%
  group_by(year_id, age_group_years_start, sex_id) %>%
  summarize(total = sum(predicted))

data <- data %>% left_join(sum_total)
data$proportion <- data$predicted / data$total


### Plots for vetting comparisons ---------------------------------------------
data$sex <- ifelse(data$sex_id == 1, "Male", "Female")
# # 
# # # By age
# data %>% filter(year_id %in% c(1990, 2000, 2019)) %>%
#   ggplot(aes(x = as.factor(age_group_id), y = proportion,
#              fill = cause_name)) +
#   geom_bar(stat = "identity", position = 'stack') +
#   facet_grid(year_id~ sex) +
#   labs(title = "United States")
# # 
# data %>% filter(year_id %in% c(1990, 2000, 2019)) %>%
#   ggplot(aes(x = as.factor(age_group_id), y = mean,
#              fill = cause_name)) +
#   geom_bar(stat = "identity", position = 'stack') +
#   facet_grid(year_id~ sex) +
#   labs(title = "China")
# 
# # By year
# data %>% filter(age_group_id %in% c(15, 20, 30)) %>%
#   ggplot(aes(x = as.factor(year_id), y = proportion,
#              fill = cause_name)) +
#   geom_bar(stat = "identity", position = 'stack') +
#   facet_grid(age_group_id ~ sex) +
#   labs(title = "China")
# 
# data %>% filter(age_group_id %in% c(15, 20, 30)) %>%
#   ggplot(aes(x = as.factor(year_id), y = mean,
#              fill = cause_name)) +
#   geom_bar(stat = "identity", position = 'stack') +
#   facet_grid(age_group_id ~ sex) +
#   labs(title = "China")

### Continue Splits -----------------------------------------------------------
data <- data %>%
  dplyr::select(-age_group_years_start, -mean,
                -lower, -upper, -predicted,
                -total)

### Get Pneumo model values ---------------------------------------------------
pneumo <- get_draws(gbd_id_type = "modelable_entity_id",
                    gbd_id = 24988,
                    measure_id = c(5,6),
                    source = "epi",
                    location_id = loc,
                    gbd_round_id = 7,
                    decomp_step = 'iterative',
                    n_draws = draws,
                    downsample = downsampling)

pneumo <- convert_draws_long(pneumo)

test <- pneumo %>% left_join(data)
nonmissing <- test[!is.na(test$proportion),]

# Needed for upload?
missing <- pneumo %>% filter(age_group_id %in% c(2,3,4,5,6,7))
missing$values <- 0
missing <- missing %>% dplyr::select(-model_version_id, -modelable_entity_id, -metric_id)

df <- nonmissing

# Key: get the actual prevalence and incidence values by multiplying by proportion
df$values2 <- df$values * df$proportion

# Reformat dataframe into proper format
df <- df %>%
  dplyr::select(age_group_id, location_id, measure_id, 
                sex_id, year_id, draw, values2, cause_name) %>%
  rename(values = values2)


### Save models ---------------------------------------------------------------

message("Saving csv")

for (i in 1:nrow(mapping)) {

  # A bit more reformating
  df2 <- df %>%
    filter(cause_name == mapping$cause_name[i]) %>%
    dplyr::select(-cause_name)
  df2 <- bind_rows(df2, missing)
  df2 <- convert_draws_wide(df2)

  # Comes from the mapping file. 
  file_name <- paste0(mapping$directory_to_save[1], mapping$target_me_id[i], "/", loc, ".csv")
  message("Saving file to directory: ", file_name)
  
  
  write.csv(df2, file_name, row.names = F)
  
}

message("Finished")





