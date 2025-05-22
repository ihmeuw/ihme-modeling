library(dplyr)
library(purrr)
library(tidyr)
library(readxl)
library(reticulate)
reticulate::use_python("FILEPATH")
splitter <- import("pydisagg.ihme.splitter")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

bv <- as.data.table(get_bundle_version(bundle_version_id = bvid))

# add sex_id, year_id to bv
bv$year_id <- floor((bv$year_start + bv$year_end) / 2)
bv <- bv %>%
  mutate(sex_id = case_when(
    sex == "Male" ~ 1,
    sex == "Female" ~ 2,
    TRUE ~ 3 # This acts as the 'else' condition
  ))

# replacing zero values in mean and lower 
## sex_plitting function cannot handle if not(0<mean,upper,lower<=1)

# Step 1: Calculate a farction of the lowest non-zero value in 'mean'
half_lowest_non_zero_mean <- min(bv$mean[bv$mean > 0]) / 5

# Step 2: Replace zeros with this value in 'mean'
bv <- bv %>%
  mutate(mean = ifelse(mean <= 0, half_lowest_non_zero_mean, mean))

# Step 1: Calculate half the lowest non-zero value in 'lower'
half_lowest_non_zero_lwr <- min(bv$lower[bv$lower > 0]) / 5

# Step 2: Replace zeros with this value in 'lower'
bv <- bv %>%
  mutate(lower = ifelse(lower <= 0, half_lowest_non_zero_lwr, lower))

# replace upper>=1 with 1- (1-highest_non_zero_upr)/5:
highest_non_one_upr <- max(bv$upper[bv$upper < 1])
new_upper <- 1- (1-highest_non_one_upr)/5
bv <- bv %>%  mutate(upper = ifelse(upper >= 1, new_upper, upper))


unique(bv$sex_id)
# all sex specif data in the bundle version to concatenate later with the split data
sex_specific_bv <- bv %>% filter(sex_id!=3)

# copying bv and adding val, val_sd to follow sex split func requirement
bv2 <- bv %>% mutate(val=mean, val_sd = standard_error)

age_mt <- get_age_metadata(release_id = 16)
under_1 <- list(age_group_id = 28, age_group_name = "<1 year", age_group_alternative_name= "<1 year", age_group_name_short = "<1",
                age_group_years_start = 0, age_group_years_end = 1, age_group_days_start=0, age_group_days_end =364, most_detailed = 0 )
age_mt <- rbind(age_mt, under_1, fill=TRUE)
age_mt_selected <- select(age_mt, age_group_years_start, age_group_years_end, age_group_id, age_group_name)

## Select reference data, sex specific data and data to split from bv2 ------------------------------------------------------
Poland_locs <- c(53660,53661, 53662, 53663, 53664, 53665, 53666, 53667, 53668, 53669,
                 53670, 53671, 53672, 53673, 53674, 53675)
# Reference data is Clinical data from Polnad
ref_df <- bv2 %>% filter(location_id %in% Poland_locs & clinical_data_type=='claims')
# any data that is not sex specific should be split
data_to_split <- bv2 %>% filter(sex_id==3)

# add age_id to ref_df and mutate age_group_year_start and end to align with age_mt
ref_df_age_id <- ref_df %>% mutate(age_end = trunc(age_end) + 1)
ref_df_age_id <-left_join(ref_df_age_id, age_mt_selected, by = c("age_start" = "age_group_years_start",
                                                                 "age_end" = "age_group_years_end"))
ref_df_age_id <- ref_df_age_id %>% mutate(age_group_years_start = age_start,
                              age_group_years_end = age_end)
ref_df_age_id <- ref_df_age_id %>% mutate(age_end=age_end-1)
condition <- ((ref_df_age_id$age_start == 0) & (ref_df_age_id$age_end == 0))
ref_df_age_id$age_end[condition] <- 0.999

# Mutate age_lwr and age_upr to align with sex split function
data_to_split <- data_to_split %>% mutate(age_lwr = age_start,
                                          age_upr = age_end)

## Create the pattern df. ------------------------------------------------------

pattern <- ref_df_age_id

unique_pairs <- unique(pattern[c(age_start, age_end)])
# Print the result to see the unique pairs
look <- unique_pairs %>% select(age_start, age_end, age_group_id,age_group_name) %>% unique()

#load in draws_df
draws_df <- read.csv(paste0(modeling_dir,"FILEPATH",cause_name,"_draws_df.csv"))

draw_cols <- grep("^draw_", names(draws_df), value = TRUE)
# concatenate pattern and draws_df
pattern <- cbind(pattern, draws_df) %>% select(sex_id, year_id, age_start, age_end, all_of(draw_cols))


# find average draws over all ages for each sex:
pattern2 <- pattern %>%
  group_by(sex_id) %>%
  summarise(across(.cols = everything(), .fns = mean, na.rm = TRUE), .groups = "drop")
  # summarise(across(everything(), mean, na.rm = TRUE), .groups = "drop")
# converting logits to probabilities
pattern2 <- pattern2 %>% mutate(across(all_of(draw_cols), ~ 1 / (1 + exp(-.x))))

# female and male patterns
pattern_F <- pattern2 %>% filter(sex_id==2) %>% select(-sex_id, everything())
pattern_M <- pattern2 %>% filter(sex_id==1) %>% select(-sex_id, everything())

# Pattern ratio for f (f/m)
pattern_ratio <- pattern_F[draw_cols]/pattern_M[draw_cols]

# Calculate mean and sd over all draws
pattern_mean <- apply(pattern_ratio, 1, mean, na.rm = TRUE)
pattern_sd <- apply(pattern_ratio, 1, sd, na.rm = TRUE)

unique(ref_df$year_id)
to_split_years <-sort(unique(data_to_split$year_id))

expanded_pattern <- data.frame(
  year_id = to_split_years,
  value = rep(pattern_mean, length(to_split_years)),
  standard_deviation = rep(pattern_sd, length(to_split_years))
)

pop_df <- get_population( age_group_id = c(22) ,year_id=to_split_years, sex_id=1:2,
                          release_id=16,location_id=unique(data_to_split$location_id))

# Apply sex_split function: ------------------------------------------------------
sex_splitter = splitter$SexSplitter(
  data=splitter$SexDataConfig(
    index=c("nid","seq", "location_id", "year_id", "sex_id","age_lwr","age_upr"),
    val="val",
    val_sd="val_sd"
  ),
  # The SexPattern is anticipating a female/ male ratio for val, val_sd or draws
  pattern=splitter$SexPatternConfig(
    by=list('year_id'),
    val='value', #Either draw columns OR val & val_sd can be provided
    val_sd='standard_deviation'
  ),
  population=splitter$SexPopulationConfig(
    index=c('location_id', 'year_id'),
    sex="sex_id",
    sex_m=1,
    sex_f=2,
    val='population'
  )
)

#Model can only be "rate" for now
result <- sex_splitter$split(
  data=data_to_split,
  pattern=expanded_pattern,
  population=pop_df,
  model="rate", #model can be "rate" or "logodds"
  output_type="rate" #or "count"
)

result_to_merge <- result %>% select(-val_sd, -val, -value, -m_pop, -f_pop, -standard_deviation) %>% rename(mean = sex_split_result,
                                                                                                            standard_error = sex_split_result_se)

result_to_merge <- result_to_merge %>% mutate(lower = mean - 1.96 * standard_error,
                                              upper = mean + 1.96 * standard_error)

### data cleanup and merge ###

# val=mean, val_sd=standard_error was added to the data_to_split df as sex split func requirements, so we need to remove them.
## we should also remove pre-split values of sex_id, mean, standard_error, upper, lower:
data_to_split <- data_to_split %>% select(-val_sd, -val, -mean, -upper, -lower, -standard_error, -sex_id)

# join result_to_merge and data_to_split on indices to make splitted_data
splitted_data <- inner_join(data_to_split, result_to_merge, by = c("nid","seq", "location_id", "year_id", "age_lwr", "age_upr"))

splitted_data <- splitted_data %>%
  mutate(sex = case_when(
    sex_id == 1 ~ "Male",
    sex_id == 2 ~ "Female",
    TRUE ~ "Both" # it should never happen for  sex splitted data
  ))

# assigning splitted data's seq value to parent_seq
splitted_data$crosswalk_parent_seq <- splitted_data$seq

# removing previous seq values
# remove all values in seq column, since we should assign new seq values and bring previous seq values to crosswalk_parent_seq column
splitted_data$seq <- NA

# comparing sex_specific_bv and splitted_data columns:
setdiff(names(splitted_data),names(sex_specific_bv))
setdiff(names(sex_specific_bv),names(splitted_data))

#removing age_lwr, age_upr columns
sex_specific_bv$crosswalk_parent_seq <- ""
splitted_data <- splitted_data %>% select(-age_lwr, -age_upr)

# merging splitted data with sex specific proportion of bv
sex_splitted_bv <- rbind(splitted_data, sex_specific_bv)


### Handling invalid intervals #################################################
# Correction 1: Make sure that the 0 < lower
half_lowest_non_zero_lwr <- min(sex_splitted_bv$lower[sex_splitted_bv$lower > 0]) / 5
sex_splitted_bv$lower[sex_splitted_bv$lower<=0] <- half_lowest_non_zero_lwr


# Correction 2: Replacing standard_error==0:
half_lowest_non_zero_se <- min(sex_splitted_bv$standard_error[sex_splitted_bv$standard_error > 0]) / 5
sex_splitted_bv$standard_error[sex_splitted_bv$standard_error<=0] <- half_lowest_non_zero_se

# Correction 3: assign standard_error = 1 where this value is >1
sex_splitted_bv$standard_error[sex_splitted_bv$standard_error>1] <- 1

# replace upper>=1 with 1- (1-highest_non_zero_upr)/5:
highest_non_one_upr <- max(sex_splitted_bv$upper[sex_splitted_bv$upper < 1])
new_upper <- 1- (1-highest_non_one_upr)/5
sex_splitted_bv$upper[sex_splitted_bv$upper>1] <- new_upper


#saving the sex_splitted_bv to csv:
write.csv(sex_splitted_bv, paste0(modeling_dir, "FILEPATH",cause_name,".csv"), row.names = FALSE)

