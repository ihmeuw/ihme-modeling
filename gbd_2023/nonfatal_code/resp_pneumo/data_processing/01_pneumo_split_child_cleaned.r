message("Starting to process pneumos ----------------------------------")



source("/filepath/submit_parallel_jobs.R")

### Getting arguments from job array (passed in the function) -----------------
date<- Sys.Date()

print('passing args')
# Grab the task_id from the SGE environment
args <- commandArgs(trailingOnly = T)
print(paste0("the args are ", args))

task <- Sys.getenv("SLURM_ARRAY_TASK_ID") 
print(paste0("This task_id is ", task))

# Read in the file path that is passed from the parent script
# and save the parameters that correspond to this task id

params <- data.table(fread(args)) 
loc <- params[task_id == task, loc]
print("got params")

path_to_map <- "/filepath/pneumo_cod_map_2023.csv" 

message("Path to mapping ", path_to_map)
message("Location ", loc)

code_dir <- "filepath"
central_dir <- "filepath"

source("/filepath/custom_process_functions_2019.R") 
source("filepath/squeeze_split_save_functions.R")

source(paste0(central_dir, "get_draws.R"))
source(paste0(central_dir, "get_model_results.R"))
source(paste0(central_dir, "get_age_metadata.R"))
source(paste0(central_dir, "get_location_metadata.R"))

library(tidyverse)
library(ggplot2)

mapping <- data.table(read.csv(path_to_map, stringsAsFactors = FALSE))
location_metadata<- get_location_metadata(9, release_id = 16)


print('got to data processing')
### Getting hyperparams -------------------------------------------------------

message("Getting parameters from mapping")

age_ids <- get_age_metadata(age_group_set_id = mapping$age_group_set[1], 
                            release_id = mapping$release_id[1])

locations_meta <- get_location_metadata(location_set_id = 9, 
                                        release_id = mapping$release_id[1])
                                      
locs_meta <- locations_meta %>% dplyr::select(location_id, level, location_ascii_name)

filtered_locs_meta <- locs_meta[locs_meta$location_id %in% loc,]

ages_meta <- age_ids %>% dplyr::select(age_group_id, age_group_years_start)

sex_ids <- as.numeric(unlist(strsplit(as.character(mapping$sex_id[1]),',')))

measure_ids <- as.numeric(unlist(strsplit(as.character(mapping$measure_id[1]),',')))

year_ids <- as.numeric(unlist(strsplit(as.character(mapping$year_id[1]),',')))

draws <- mapping$n_draws[1]
downsampling <- mapping$downsample[1]
round <- mapping$release_id[1]
directory <- paste0(mapping$directory_to_save[1], "/", date)
target_meids <- mapping$target_me_id


message("Filtering parent and child mapping sections")
child_map<- mapping[parent== "child"]

### Getting draws for all children and parent ---------------------------------

get_list_of_draws <- function(gbd_id, 
                              gbd_names = NULL,
                              gbd_id_type, 
                              source, 
                              release_id,
                              version_id = "None",
                              year_id = "None", 
                              location_id = "None",
                              sex_id = "None",
                              age_id = "None", 
                              measure_id = "None", 
                              metric_id = "None",
                              status = "best", 
                              n_draws = 1000, 
                              downsample = F) {
  
  print(paste("Getting draws for", length(gbd_id), "ids"))
  
  list_of_draws <- list()
  for (i in 1:length(gbd_id)) {
    print(paste("Getting draws for id", gbd_id[i]))
    list_of_draws[[i]] <- get_draws(gbd_id = gbd_id[i], 
                                    gbd_id_type = gbd_id_type, 
                                    year_id = year_id,
                                    location_id = location_id, 
                                    age_group_id = age_id, 
                                    sex_id = sex_id,
                                    source = source, 
                                    measure_id = measure_id, 
                                    metric_id = metric_id,
                                    status = status,
                                    version_id = version_id,
                                    release_id = release_id, 
                                    downsample = downsample)
    if(!is.null(gbd_names)) {
      list_of_draws[[i]]$gbd_name <- gbd_names[i]
    }
  }
  
  if (length(list_of_draws) == 1) {
    print("Only passed a single GBD id, returning data table instead of list")
    list_of_draws <- data.table(list_of_draws[[1]])
  } else {
    print("Returning draws in list format. Use list_name[[]] to access")
  }
  
  
  return(list_of_draws)
}

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
                                release_id = round,
                                n_draws = draws,
                                downsample = downsampling)

# custom function
draws_list2 <- scale_draws_to_one_and_return_draws(draws_list, return_wide = FALSE)

# bind and add metadata
for (i in 1:length(draws_list2)) {
  draws_list2[[i]]$cause_name <- mapping$cause_name[i]
  draws_list2[[i]] <- draws_list2[[i]] %>% left_join(ages_meta) %>% left_join(filtered_locs_meta)
}

data <- data.frame()
for (i in 1:length(draws_list2)) {
  frame <- draws_list2[[i]]
  data <- bind_rows(data, frame)
}


### Data Processing Regression --------------------------------------------------

# summarise data
data <- data %>%
  group_by(cause_name, year_id, age_group_years_start, age_group_id, sex_id, location_id) %>%
  summarize(mean = mean(values),
            lower = quantile(values, .025),
            upper = quantile(values, .975)) %>%
  as_tibble()

# Run polynomial regression to smooth out the jumps in cod data

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
data<- merge(data, location_metadata[, c("location_id", "location_name")], by= "location_id")
# #
# # By age
loc_name<- unique(data$location_name)
data %>% filter(year_id %in% c(1990, 2000, 2019, 2024)) %>%
  ggplot(aes(x = as.factor(age_group_id), y = proportion,
             fill = cause_name)) +
  geom_bar(stat = "identity", position = 'stack') +
  facet_grid(year_id~ sex) +
  labs(title = paste0("Proportion values by age, location: ", loc_name))
#
data %>% filter(year_id %in% c(1990, 2000, 2019)) %>%
  ggplot(aes(x = as.factor(age_group_id), y = mean,
             fill = cause_name)) +
  geom_bar(stat = "identity", position = 'stack') +
  facet_grid(year_id~ sex) +
  labs(title = paste0("Mean values by age, location: ", loc_name))


### Continue Splits -----------------------------------------------------------
data <- data %>%
  dplyr::select(-age_group_years_start, -mean,
                -lower, -upper, -predicted,
                -total)

### Get Parent Pneumo model values ---------------------------------------------------
pneumo <- get_draws(gbd_id_type = "modelable_entity_id",
                    gbd_id = 24988,
                    measure_id = c(5,6),
                    source = "epi",
                    location_id = loc,
                    release_id = 16,
                    version_id = version_id,
                    downsample = downsampling)
                   

pneumo <- convert_draws_long(pneumo)

test <- pneumo %>% left_join(data)
nonmissing <- test[!is.na(test$proportion),]

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

# check for true zeros
check<- unique(df, by= c("cause_name", "year_id", "measure_id", "age_group_id", "location_id", "sex_id"))
### Save models ---------------------------------------------------------------

for (i in 1:nrow(mapping)) {
  
  # A bit more reformatting
  df2 <- df %>%
    filter(cause_name == mapping$cause_name[i]) %>%
    dplyr::select(-cause_name)
  df2 <- bind_rows(df2, missing)
  df2 <- convert_draws_wide(df2)
  
  # make a dated directory
  if (!dir.exists(paste0(mapping$directory_to_save[1], date))) {
    dir.create(paste0(mapping$directory_to_save[1], date))
  }
  
  if (!dir.exists(paste0(mapping$directory_to_save[1], date, "/", mapping$target_me_id[i]))) {
    dir.create(paste0(mapping$directory_to_save[1], date, "/", mapping$target_me_id[i]))
  }
  
  # Nested loop for location_id
  for (loc in unique(df2$location_id)) {
    # Comes from the mapping file. 
    file_name <- paste0(mapping$directory_to_save[1], date, "/", mapping$target_me_id[i], "/", loc, ".csv")
    message("Saving file to directory: ", file_name)
    
    # Filter df2 for the specific location_id
    df_loc <- df2[df2$location_id == loc, ]
    
    # Save CSV
    write.csv(df_loc, file_name, row.names = FALSE)
  }
  
}


message("Finished")






