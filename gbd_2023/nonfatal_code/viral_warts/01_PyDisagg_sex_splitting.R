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

names(bv)
# drop all columns with cv in their name:
bv<- bv %>% select(-c(starts_with("cv"), bundle_id_, step2_location_year))
names(bv)

# Find columns that contain NaN values
columns_with_nan <- sapply(bv, function(x) any(is.na(x)))
# Print names of columns with NaN values
nan_columns <- names(bv)[columns_with_nan]
print(nan_columns)
# [1] "underlying_nid"

# [13] ""                   "underlying_field_citation_value"
# [15] "clinical_version_id"

unique(bv$underlying_nid)
underlying_nid <- bv %>% filter(is.na(underlying_nid))


# check NA values (smaller_site_unit, measure_adjustment, group, group_review, bundle_id)
#smaller_site_units
unique(bv$smaller_site_unit)
# NA values should be replaced by 0
smaller_site_unit <- bv %>% filter(is.na(smaller_site_unit))
bv$smaller_site_unit[is.na(bv$smaller_site_unit)]<- 0

#measure_adjustment
unique(bv$measure_adjustment)
# Na values should be replaced with 0
measure_adjustment<- bv %>% filter(is.na(measure_adjustment))
bv$measure_adjustment[is.na(bv$measure_adjustment)]<-0

#group
unique(bv$group)
#NA values should be 1
group<- bv %>% filter(is.na(group))
bv$group[is.na(bv$group)]<-1

#group_review
unique(bv$group_review)
# NA values should be replaced with 1
group_review<- bv %>% filter(is.na(group_review))
bv$group_review[is.na(bv$group_review)]<-1

#sex_issue
unique(bv$sex_issue)
# NA values should be replaced with 0
sex_issue<- bv %>% filter(is.na(sex_issue))
bv$sex_issue[is.na(bv$sex_issue)]<-0

#year_issue
unique(bv$year_issue)
# NA values should be replaced with 0
year_issue<- bv %>% filter(is.na(year_issue))
bv$year_issue[is.na(bv$year_issue)]<-0

#age_issue
unique(bv$age_issue)
# NA values should be replaced with 0
age_issue<- bv %>% filter(is.na(age_issue))
bv$age_issue[is.na(bv$age_issue)]<-0

#age_demographer
unique(bv$age_demographer)
unique(bv$age_end[is.na(bv$age_demographer)])
unique(bv$age_start[is.na(bv$age_demographer) & bv$age_end==1])
# NA values should be replaced with 1
age_demographer<- bv %>% filter(is.na(age_demographer))
bv$age_demographer[is.na(bv$age_demographer)]<-1

#measure_issue
unique(bv$measure_issue)
# NA values should be replaced with 0
measure_issue<- bv %>% filter(is.na(measure_issue))
bv$measure_issue[is.na(bv$measure_issue)]<-0


# add sex_id, year_id to bv
bv$year_id <- floor((bv$year_start + bv$year_end) / 2)
bv <- bv %>%
  mutate(sex_id = case_when(
    sex == "Male" ~ 1,
    sex == "Female" ~ 2,
    TRUE ~ 3 # This acts as the 'else' condition
  ))

# replacing zero values in mean and lower 

# Step 1: Calculate a fraction of the lowest non-zero value in 'mean'
half_lowest_non_zero_mean <- min(bv$mean[bv$mean > 0]) / 5

# Step 2: Replace zeros with this value in 'mean'
bv <- bv %>%
  mutate(mean = ifelse(mean <= 0, half_lowest_non_zero_mean, mean))

# Step 1: Calculate a fraction of the lowest non-zero value in 'lower'
half_lowest_non_zero_lwr <- min(bv$lower[bv$lower > 0]) / 5

# Step 2: Replace zeros with this value in 'lower'
bv <- bv %>%
  mutate(lower = ifelse(lower <= 0, half_lowest_non_zero_lwr, lower))

# replace mean>=1 with 1- (1-highest_non_zero_upr)/5:
highest_non_zero_upr <- max(bv$mean[bv$mean < 1])
new_mean <- 1- (1-highest_non_zero_upr)/5
bv <- bv %>%
  mutate(mean = ifelse(mean >= 1, new_mean, mean))

# replace upper>1 with 1- (1-highest_non_zero_upr)/5:
highest_non_zero_upr <- max(bv$upper[bv$upper < 1])
new_upper <- 1- (1-highest_non_zero_upr)/5
bv <- bv %>%
  mutate(upper = ifelse(upper >= 1, new_upper, upper))

### FIX STANDARD ERRORS THAT ARE TOO LARGE###
bv$standard_error[bv$standard_error>1] <- 1

# for when mean>upper, replace upper with mean:
bv$upper[bv$mean > bv$upper] <- bv$mean[bv$mean > bv$upper]

# Replacing upper with mean when mean>upper
bv$upper <- ifelse(bv$mean > bv$upper, bv$mean, bv$upper)

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

data_to_split_2<- data_to_split %>% select(-c( "underlying_nid",
                                               "smaller_site_unit",
                                               "sex_issue", "year_issue",
                                               "age_issue",
                                               "age_demographer",
                                               "measure_issue",
                                               "measure_adjustment",
                                               "recall_type_value",
                                               "group"
                                              ,"group_review",
                                              "design_effect",
                                              "underlying_field_citation_value",
                                              "clinical_version_id",
                                              "table_num",
                                              "bundle_version_id"))
## Create the pattern df. ------------------------------------------------------
pattern <- ref_df_age_id

unique_pairs <- unique(pattern[c(age_start, age_end)])
# Print the result to see the unique pairs
look <- unique_pairs %>% select(age_start, age_end, age_group_id,age_group_name) %>% unique()

#load in draws_df
draws_df <- read.csv(paste0(modeling_dir,"/PyDisagg_age_sex_splitting/",cause_name,"_draws_df.csv"))

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
  data=data_to_split_2,
  pattern=expanded_pattern,
  population=pop_df,
  model="rate", #model can be "rate" or "logodds"
  output_type="rate" #or "count"
)
###################################################
names(result)

result_to_merge <- result %>% select(-val_sd, -val,
                                     -sex_pat_value, -sex_pat_standard_deviation,
                                     -m_pop, -f_pop,
                                     -mean, -standard_error,
                                     -upper, -lower,
                                     -sex_split) %>% dplyr::rename(mean = sex_split_result,
                                                                       standard_error = sex_split_result_se)

setdiff(names(result), names(data_to_split))
setdiff(names(data_to_split), names(result))
setdiff(names(result_to_merge), names(data_to_split))
setdiff(names(data_to_split), names(result_to_merge))

result_to_merge <- result_to_merge %>% mutate(lower = mean - 1.96 * standard_error,
                                              upper = mean + 1.96 * standard_error)

### data cleanup and merge ###

# val=mean, val_sd=standard_error was added to the data_to_split df as sex split func requirements, so we need to remove them.
## we should also remove pre-split values of sex_id, mean, standard_error, upper, lower:
data_to_split <- data_to_split %>% select(-val_sd, -val, -mean, -upper, -lower, -standard_error, -sex_id)

names(result_to_merge)
names(data_to_split)
setdiff(names(data_to_split),names(result_to_merge))
setdiff(names(result_to_merge), names(data_to_split))

result_to_merge <- result_to_merge %>% select(-c("field_citation_value", "page_num", "source_type", "location_name", "ihme_loc_id",
                                                 "year_start", "year_end", "age_start", "age_end", "measure", "cases", "sample_size", "unit_type",
                                                 "representative_name", "urbanicity_type", "recall_type", "sampling_type", "case_name",
                                                 "case_definition", "case_diagnostics", "specificity", "note_modeler", "note_sr", "extractor",
                                                 "effective_sample_size", "input_type", "uncertainty_type",
                                                 "clinical_data_type", "site_memo", "unit_value_as_published", "uncertainty_type_value",
                                                 "bundle_name", "origin_seq", "origin_id", "is_outlier", "sex"
                                                 ))

setdiff(names(result_to_merge), names(data_to_split))
setdiff(names(data_to_split),names(result_to_merge))
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
sex_splitted_bv <- sex_splitted_bv %>%
  mutate(lower = ifelse(lower <= 0, half_lowest_non_zero_lwr, lower))

# Correction 2: Replacing standard_error==0:
half_lowest_non_zero_se <- min(sex_splitted_bv$standard_error[sex_splitted_bv$standard_error > 0]) / 5
sex_splitted_bv <- sex_splitted_bv %>%
  mutate(standard_error = ifelse(standard_error <= 0, half_lowest_non_zero_se, standard_error))

#saving the sex_splitted_bv to csv:
write.csv(sex_splitted_bv, paste0(modeling_dir, "FILEPATH",cause_name,".csv"), row.names = FALSE)

