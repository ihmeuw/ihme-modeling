library(reticulate)
library(tidyverse)
reticulate::use_python("FILEPATH")
splitter <- import("pydisagg.ihme.splitter")
source("FILEPATH")
source("FILEPATH")


# Read in data to split. this is the data after sex splitting.
data <- read.csv(paste0(modeling_dir, "FILEPATH",cause_name,".csv"))


### make sure standard error<1
data$standard_error[data$standard_error>1] <- 1


# Step 1: Calculate  the max value in 'upper'
max_upper <- max(data$upper[data$upper <1])
# Step 2: Replace greater than 1 with this value in 'max_upper'
data <- data %>%
  mutate(upper = ifelse(upper >=1 , max_upper, upper))

# Get age metadata
age_mt <- get_age_metadata(release_id = 16)
under_1 <- list(age_group_id = 28, age_group_name = "<1 year", age_group_alternative_name= "<1 year", age_group_name_short = "<1",
                age_group_years_start = 0, age_group_years_end = 1, age_group_days_start=0, age_group_days_end =364, most_detailed = 0 )
age_mt <- rbind(age_mt, under_1, fill=TRUE)
age_mt <- select(age_mt, age_group_years_start, age_group_years_end, age_group_id, age_group_name)

# add age_id to data and mutate age_group_year_start and end to align with age_mt
data_age_end_adjusted <- data %>% mutate(age_end = trunc(age_end) + 1)
# any data that does not have GBD aligned age groups should be split.
## we find rows in data that can't be joined with age_mt_selected based on age_start and age_end
data_to_split <-anti_join(data_age_end_adjusted, age_mt, by = c("age_start" = "age_group_years_start",
                                                                 "age_end" = "age_group_years_end"))
# Data that does not need age splitting, to join with splitted data later:
age_specific_data <-inner_join(data_age_end_adjusted, age_mt %>% select(age_group_years_start, age_group_years_end),
                           by = c("age_start" = "age_group_years_start", "age_end" = "age_group_years_end"))
age_specific_data <- age_specific_data %>% mutate(age_end = age_end - 1)
condition <- ((age_specific_data$age_start == 0) & (age_specific_data$age_end == 0))
age_specific_data$age_end[condition] <- 0.999

# add age_group_years_start&end to data_to_split
data_to_split <- data_to_split %>% mutate(age_group_years_start = age_start,
                                          age_group_years_end = age_end)
data_to_split <- data_to_split %>% mutate(age_end=age_end-1)
condition <- ((data_to_split$age_start == 0) & (data_to_split$age_end == 0))
data_to_split$age_end[condition] <- 0.999


# Mutate age_lwr and age_upr to align with sex split function
data_to_split <- data_to_split %>% mutate(age_lwr = age_group_years_start,
                                          age_upr = age_group_years_end)

# adding uid column to create unique identifiers where there are multiple data points for a set of (nid,loc,sex,year):
data_to_split <- data_to_split %>% mutate(uid=1:nrow(data_to_split))

# when gae_start=age_end, add 1 to age_end:
data_to_split <- data_to_split %>% mutate(age_end = if_else(age_start == age_end, trunc(age_end) + 1, age_end))

# We do not have age_start < 1 in the pattern. Fix it in the data_to_split
data_to_split$age_start[data_to_split$age_start < 1] <- 1

## Select reference data, age specific data and data to split from data ------------------------------------------------------
Poland_locs <- c(53660,53661, 53662, 53663, 53664, 53665, 53666, 53667, 53668, 53669,
                 53670, 53671, 53672, 53673, 53674, 53675)

# Reference data is Clinical data from Poland
ref_df <- data %>% filter(location_id %in% Poland_locs & clinical_data_type=='claims')

# add age_id to ref_df and mutate age_group_year_start and end to align with age_mt
ref_df <- ref_df %>% mutate(age_end = trunc(age_end) + 1)
ref_df <-left_join(ref_df, age_mt, by = c("age_start" = "age_group_years_start",
                                                                 "age_end" = "age_group_years_end"))
ref_df <- ref_df %>% mutate(age_group_years_start = age_start,
                                          age_group_years_end = age_end)
ref_df <- ref_df %>% mutate(age_end=age_end-1)

condition <- ((ref_df$age_start == 0) & (ref_df$age_end == 0))
ref_df$age_end[condition] <- 0.999

## Create the pattern df. ------------------------------------------------------
pattern <- ref_df
#load in draws_df
draws_df <- read.csv(paste0(modeling_dir,"FILEPATH",cause_name,"_draws_df.csv"))

draw_cols <- grep("^draw_", names(draws_df), value = TRUE)
# concatenate pattern and draws_df
pattern <- cbind(pattern, draws_df) %>% select(sex_id, year_id, age_start, age_end, age_group_id, age_group_years_start, age_group_years_end, all_of(draw_cols))

# find average draws over all ages for each sex:
pattern <- pattern %>%
  group_by(age_group_id, sex_id, age_group_years_start, age_group_years_end) %>%
  summarise(across(everything(), mean, na.rm = TRUE), .groups = "drop")
# converting logits to probabilities
pattern <- pattern %>% mutate(across(all_of(draw_cols), ~ 1 / (1 + exp(-.x))))


# target years for splitting
unique(ref_df$year_id)

to_split_years <-sort(unique(data_to_split$year_id))
to_split_years

# Repeat the pattern2 for each year in to_split_years
pattern_over_years <- data.frame()
for(year in to_split_years) {
  # Create a modified version of pattern2 with the current year
  temp_df <- pattern %>% mutate(year_id = year)
  # Concatenate this modified df to the pattern2_years
  pattern_over_years <- bind_rows(pattern_over_years, temp_df)
}

pop_df <- get_population(age_group_id = 'all', year_id=to_split_years, sex_id=1:2,
                          release_id=16,location_id=unique(data_to_split$location_id))

# Age_Splitting: ------------------------------------------------------
age_data_config <- splitter$AgeDataConfig(
  index=c("nid","seq", "location_id", "year_id", "sex_id", "uid"),
  age_lwr="age_start",
  age_upr="age_end",
  val="mean",
  val_sd="standard_error"
)

#Will look for column names with "draw_" at the beginning
draw_cols <- grep("^draw_", names(pattern), value = TRUE)

age_pattern_config <- splitter$AgePatternConfig(
  by=list("sex_id", "year_id"),
  age_key="age_group_id",
  age_lwr="age_group_years_start",
  age_upr="age_group_years_end",
  draws=draw_cols, #Either draw columns OR val & val_sd can be provided
)

age_pop_config <- splitter$AgePopulationConfig(
  index=c("age_group_id", "location_id", "year_id", "sex_id"),
  val="population"
)

age_splitter <- splitter$AgeSplitter(
  data=age_data_config, pattern=age_pattern_config, population=age_pop_config
)

result <- age_splitter$split(
  data=data_to_split,
  pattern=pattern_over_years,
  population=pop_df,
  model="logodds", #model can be "rate" or "logodds"
  output_type="count") #or "count"

#---------------------------------------
result_to_merge <- result %>% select(-age_start, -age_end, -standard_error, -mean, -pat_val, -pat_val_sd, -pop_population, -pat_val_aligned,
                                     -pat_val_sd_aligned, -pop_population_aligned, -pop_population_total, -pop_population_proportion) %>%
  rename(age_start = pat_age_group_years_start, age_end = pat_age_group_years_end,
         mean = age_split_result,standard_error = age_split_result_se)# check the interval of mean:

#calculate lower and upper bounds:
result_to_merge <- result_to_merge %>% mutate(lower = mean - 1.96 * standard_error,
                                              upper = mean + 1.96 * standard_error)

## replacing values in min(lower) and max(upper), since not in rage.
# Step 1: Calculate a fraction of the lowest non-zero value in 'lower'
half_lowest_non_zero_res_lower <- min(result_to_merge$lower[result_to_merge$lower > 0]) / 5
# Step 2: Replace zeros with this value in 'lower'
result_to_merge <- result_to_merge %>%
  mutate(lower = ifelse(lower <= 0, half_lowest_non_zero_res_lower, lower))

# replace upper>=1 with 1- (1-highest_non_zero_upr)/5:
highest_non_zero_upr <- max(result_to_merge$upper[result_to_merge$upper < 1])
new_upper <- 1- (1-highest_non_zero_upr)/5
result_to_merge <- result_to_merge %>%
  mutate(upper = ifelse(upper >= 1, new_upper, upper))

# age_group_years_start and end was added to the data_to_split, so we need to remove them.
## we should also remove pre-split values of age_start&end, mean, standard_error, upper, lower:
data_to_split <- data_to_split %>% select(-age_start, -age_end, -age_group_years_start, -age_group_years_end, -mean, -upper, -lower, -standard_error)

# join result_to_merge and data_to_split on indices to make splitted_data
splitted_data <- inner_join(data_to_split, result_to_merge, by = c("nid","seq", "location_id", "year_id","sex_id", "uid"))

splitted_data <- splitted_data %>%
  mutate(sex = case_when(
    sex_id == 1 ~ "Male",
    sex_id == 2 ~ "Female",
    TRUE ~ "Both" # it should never happen for  sex splitted data
  ))

check <- splitted_data %>% filter(!is.na(crosswalk_parent_seq) & crosswalk_parent_seq != "")

# Move values from seq to crosswalk_parent_seq column, where we do age splitting.

## a few of old crosswalk_parent_seq values are not character from old extractions, we turn all values to integer, since we dont need those old crosswalk_parent_seq values.
splitted_data <- splitted_data %>% mutate(crosswalk_parent_seq = as.integer(crosswalk_parent_seq))

splitted_data <- splitted_data %>% mutate(crosswalk_parent_seq = if_else(!is.na(seq), seq, crosswalk_parent_seq),
                                          seq = if_else(!is.na(seq), NA, seq))


# comparing age_specific_data and splitted_data columns:
setdiff(names(splitted_data),names(age_specific_data))
setdiff(names(age_specific_data),names(splitted_data))

#removing age_lwr, age_upr columns
splitted_data <- splitted_data %>% select(-age_lwr, -age_upr, -age_group_id, -uid)

# merging splitted data with sex specific proportion of data
age_splitted_data <- rbind(splitted_data, age_specific_data)


#saving the age_splitted_data to csv:
write.csv(age_splitted_data, paste0(modeling_dir,"FILEPATH",cause_name, ".csv"), row.names = FALSE)
write.csv(age_splitted_data,paste0(modeling_dir, "FILEPATH",cause_name,".csv"), row.names = FALSE)
