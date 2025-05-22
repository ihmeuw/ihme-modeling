
# Remove the history and call the GBD functions
rm(list=ls(all.names=T))

library(readxl)
library(data.table)
library(dplyr)
library(tidyr)
library(openxlsx)

## Environment Prep ---------------------------------------------------- ----
version <- 4

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
code_dir <- "FILEPATH"

source(paste0(code_dir, "db_init.R"))
source(paste0(code_dir, "get_age_map.r"))
source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k ,"FILEPATH/get_location_metadata.R"))
source(paste0(roots$k ,"FILEPATH/get_age_metadata.R"))

path1 <- 'FILEPATH/COL_BOGOTA_SABANA_U_CLINIC_LONG_COVID_COHORT_STUDY_2020_2022_MEGA_BASE_LONG_COVID_2_0_Y2023M03D14.XLSX'

path2 <- 'FILEPATH/COL_BOGOTA_SABANA_U_CLINIC_LONG_COVID_COHORT_STUDY_2020_2022_DATA_DICT_280223_Y2023M03D14.XLSX'

outpath <- 'FILEPATH'

# Import the Excel file w/out changing colnames
data <- read_excel(path1, .name_repair = "minimal", na = "N/A")
data_dict <- setDT(read_excel(path2))[0:728,]

# Number of columns w/ duplicated names
length(colnames(data)) - length(unique(colnames(data)))

# Process the column names
# This function appends suffixes to duplicated names based on their order of appearance
append_suffix_to_duplicates <- function(names, suffixes) {
  # Create a new vector to hold the new names
  new_names <- trimws(names)
  temp_suffix = c("_adm", suffixes)
  temp_suffix2 = c("_1","_2")

  # Loop over each unique name
  for (name in unique(names)) {
    # Find all occurrences of the name
    indices <- which(names == name)

    # If there are duplicates, append the corresponding suffix
    if (length(indices) == 4) {
      for (i in seq_along(indices)) {
        new_names[indices[i]] <- paste0(name, temp_suffix[i])
      }
    }
    if (length(indices) == 3) {
      for (i in seq_along(indices)) {
        new_names[indices[i]] <- paste0(name, suffixes[i])
      }
    }
    if (length(indices) == 2) {
      for (i in seq_along(indices)) {
        new_names[indices[i]] <- paste0(name, temp_suffix2[i])
      }
    }
  }

  return(new_names)
}

# Apply the function to your column names with your specified time periods
new_column_names <- append_suffix_to_duplicates(colnames(data), c("_3m", "_6m", "_12m"))

# Rename variables in data dictionary to match later on
new_dict_names <- append_suffix_to_duplicates(data_dict$variable, c("_3m", "_6m", "_12m"))
data_dict[, variable := new_dict_names]

# Update the column names of your data frame
data1 <- copy(data)
colnames(data1) <- new_column_names

# Verify there are no duplicates left
length(colnames(data1)) - length(unique(colnames(data1)))
new_column_names[duplicated(new_column_names)]
new_column_names[new_column_names %like% "pain__"]
grep("_1$|_2$|_adm$", new_column_names, value = TRUE)

# Merge age groups
age_map <- get_age_map(type = "all")
age_map <- age_map[, .(age_group_id, age_group_name)]

data1 <- mutate(data1, age_group_name = ifelse(age < 1, "<1 year",
                                        ifelse(age %in% 1:4, "1 to 4",
                                        ifelse(age %in% 5:9, "5 to 9",
                                        ifelse(age %in% 10:14, "10 to 14",
                                        ifelse(age %in% 15:19, "15 to 19",
                                        ifelse(age %in% 20:24, "20 to 24",
                                        ifelse(age %in% 25:29, "25 to 29",
                                        ifelse(age %in% 30:34, "30 to 34",
                                        ifelse(age %in% 35:39, "35 to 39",
                                        ifelse(age %in% 40:44, "40 to 44",
                                        ifelse(age %in% 45:49, "45 to 49",
                                        ifelse(age %in% 50:54, "50 to 54",
                                        ifelse(age %in% 55:59, "55 to 59",
                                        ifelse(age %in% 60:64, "60 to 64",
                                        ifelse(age %in% 65:69, "65 to 69",
                                        ifelse(age %in% 70:74, "70 to 74",
                                        ifelse(age %in% 75:79, "75 to 79",
                                        ifelse(age %in% 80:84, "80 to 84",
                                        ifelse(age %in% 85:89, "85 to 89",
                                        ifelse(age %in% 90:94, "90 to 94",
                                        ifelse(age >= 95, "95 plus", "Unknown"))))))))))))))))))))))

data1 <- setDT(merge(data1, age_map, by='age_group_name'))

# Apply trimws only to character columns
data1 <- as.data.table(lapply(data1, function(column) {
  if (is.character(column)) {
    return(trimws(column))
  } else {
    return(column)
  }
}))

# Modify some cols
data1[, pneumococcal_vaccination_the_last_5_years_3m := ifelse(pneumococcal_vaccination_the_last_5_years_3m %like% "sure", NA,
                                                               as.numeric(pneumococcal_vaccination_the_last_5_years_3m))]

# Change values of some responses to match dictionary
char_cols1 <- names(data1)[sapply(data1, is.character)]
data1[, (char_cols1) := lapply(.SD, function(x) fifelse(x == "no difficulty", "No - no difficulty", x)), .SDcols = char_cols1]

# Add sex_id
data1[, sex_id := ifelse(male == 1, 1, ifelse(female == 1, 2, 4))]

# Getting the number of patients by age and by sex
sample_n <- data1 %>%
  group_by(age_group_id, age_group_name, sex_id) %>%
  dplyr::summarise(N=n())

# Getting the number of patients by sex
sample_n_bysex <- data1 %>%
  group_by(sex_id) %>%
  dplyr::summarise(N=n())

# Check if any column contains only NA
data1_all_NA <- names(data1)[sapply(data1, function(x) all(is.na(x)))]

# Get list of colnames to convert
char_cols1 <- char_cols1[!char_cols1 %like% c("record_id|age_group_name|gold_copd_class|admitted|occupation|educational|ethnicity")]
cols_dict1 <- data_dict[data_dict$variable %in% char_cols1 & data_dict$when != "admission",]

# Create a list of numerical response mappings for each variable
response_mappings <- setNames(
  lapply(cols_dict1$choices, function(choice_string) {
    choices <- strsplit(choice_string, "|", fixed = TRUE)[1] # Split responses separated by "|"
    choices <- sapply(choices, trimws)  # Trim leading and trailing white spaces
    setNames(seq_along(choices), choices)  # Create named vector with integers
  }),
  cols_dict1$variable
)

# Convert character columns into numerical ones using the response mappings
data2 <- copy(data1)
for (col in cols_dict1$variable) {
  # Get the mapping for the column, if it exists
  mapping <- response_mappings[[col]]
  if (!is.null(mapping)) {
    # Apply the mapping
    data2[, (col) := sapply(get(col), function(x) mapping[x])]
  }
}

# Order by record id
data2 <- data2 %>% arrange(data2$record_id)

check_na_changes <- function(data1, data2) {
  # Ensure the data frames have the same number of columns and the same column names
  if (!all(names(data1) == names(data2))) {
    stop("The data frames do not have the same structure.")
  }

  # Initialize an empty list to store the results
  results <- list()

  # Loop over each column name
  for (col in names(data1)) {
    # Count NAs in each data frame for the current column
    na_count_before <- sum(is.na(data1[[col]]))
    na_count_after <- sum(is.na(data2[[col]]))

    # Check if the number of NAs has changed
    if (na_count_before != na_count_after) {
      # Save the results in a list if there is a change
      results[[col]] <- c(Before = na_count_before, After = na_count_after)
    }
  }

  # If there are changes, return them as a data frame
  if (length(results) > 0) {
    return(do.call(rbind, results))
  } else {
    return("No changes in NA counts across columns.")
  }
}

NA_changes <- check_na_changes(data1, data2)
NA_changes

# Change names of pain_discomfort
setnames(data2, c("pain___discomfort _today_3m","pain___discomfort _today_6m","pain___discomfort _today_12m"),
         c("pain___discomfort_today_3m","pain___discomfort_today_6m","pain___discomfort_today_12m"))

# Calculate eq5d summary score using Dutch Tariff method
eq5d_summary_sc <- function(df, ad, mb, pd, sc, ua, score_name=NULL){

  # Summary score using Dutch Tariff method
  eq5d_summary <- data.frame(
    levels = c(1:5),
    mb_score = c(0, -0.032, -0.056, -0.166, -0.202),
    sc_score = c(0, -0.039, -0.064, -0.18, -0.165),
    ua_score = c(0, -0.04, -0.09, -0.207, -0.181),
    pd_score = c(0, -0.064, -0.089, -0.353, -0.42),
    ad_score = c(0, -0.073, -0.146, -0.36, -0.425)
  )

  eq5d_constant <- 0.956

  df <- merge(df, eq5d_summary[,c('levels','mb_score')], by.x = mb, by.y = 'levels', all.x=T)
  df <- merge(df, eq5d_summary[,c('levels','sc_score')], by.x = sc, by.y = 'levels', all.x=T)
  df <- merge(df, eq5d_summary[,c('levels','ad_score')], by.x = ad, by.y = 'levels', all.x=T)
  df <- merge(df, eq5d_summary[,c('levels','pd_score')], by.x = pd, by.y = 'levels', all.x=T)
  df <- merge(df, eq5d_summary[,c('levels','ua_score')], by.x = ua, by.y = 'levels', all.x=T)

  # Calculate summary score
  df$eq5d_summary <-
    eq5d_constant + df$mb_score + df$sc_score + df$ad_score + df$pd_score + df$ua_score

  if (!is.null(score_name)){
    setnames(df, "eq5d_summary", score_name)
  }

  # Remove the existing '_score' columns
  df <- select(df, -c(mb_score,sc_score,ad_score,pd_score,ua_score))

  return(df)
}

# Summary score pre-covid
data2 <- eq5d_summary_sc(data2,
                         ad='anxiety___depression_3m',
                         mb='mobility_3m',
                         pd='pain___discomfort_3m',
                         sc='self-care_3m',
                         ua='usual_activities_3m',
                         score_name='eq5d_summary_before_covid_3m')

# Summary score post-covid 3m
data2 <- eq5d_summary_sc(data2,
                         ad='anxiety___depression_today_3m',
                         mb='mobility_today_3m',
                         pd='pain___discomfort_today_3m',
                         sc='self-care_today_3m',
                         ua='usual_activities_today_3m',
                         score_name='eq5d_summary_today_3m')

# Summary score post-covid 6m
data2 <- eq5d_summary_sc(data2,
                         ad='anxiety___depression_today_6m',
                         mb='mobility_today_6m',
                         pd='pain___discomfort_today_6m',
                         sc='self-care_today_6m',
                         ua='usual_activities_today_6m',
                         score_name='eq5d_summary_today_6m')

# Summary score post-covid 12m
data2 <- eq5d_summary_sc(data2,
                         ad='anxiety___depression_today_12m',
                         mb='mobility_today_12m',
                         pd='pain___discomfort_today_12m',
                         sc='self-care_today_12m',
                         ua='usual_activities_today_12m',
                         score_name='eq5d_summary_today_12m')

# Modify some character columns to become numerical
data2$admitted_to_intensive_care__icu_itu_3m <- ifelse(data2$admitted_to_intensive_care__icu_itu_3m == "0t applicable", NA,
                                                           as.numeric(data2$admitted_to_intensive_care__icu_itu_3m))
data2$admitted_to_intensive_care__icu_itu_6m <- ifelse(data2$admitted_to_intensive_care__icu_itu_6m == "0t applicable", NA,
                                                           as.numeric(data2$admitted_to_intensive_care__icu_itu_6m))

data2$`re-admitted_to_hospital_or_health_facility_after_covid-19_illness_3m` <-
  ifelse(data2$`re-admitted_to_hospital_or_health_facility_after_covid-19_illness_3m` == "0t sure", NA,
         as.numeric(data2$`re-admitted_to_hospital_or_health_facility_after_covid-19_illness_3m`))

unique(data2$`vaccine_against_sars-cov-2`)

# Getting the number of patients by age groups
sample_n_byAge <- data2 %>%
  group_by(age_group_id, age_group_name) %>%
  dplyr::summarise(N=n()) %>%
  na.omit()

# Getting the number of patients by vaccination status
sample_n_byVaccine <- data2 %>%
  group_by(`vaccine_against_sars-cov-2`) %>%
  dplyr::summarise(N=n()) %>%
  na.omit()

# Getting the number of patients by icu_admission status
sample_n_byICU <- data2 %>%
  group_by(icu_admission) %>%
  dplyr::summarise(N=n())

# Subset data and add new id columns
data_main <- setDT(data2[, c('record_id','age','age_group_id','age_group_name','sex_id',
                             'confusion_disorientation','vaccine_against_sars-cov-2','icu_admission',
                             'eq5d_summary_before_covid_3m','eq5d_summary_today_3m','eq5d_summary_today_6m','eq5d_summary_today_12m',
                             'intensity_fatigue_the_last_24_hours_3m',
                             'intensity_fatigue_the_last_24_hours_6m',
                             'intensity_fatigue_the_last_24_hours_12m',
                             'anxiety___depression_today_3m','anxiety___depression_today_6m','anxiety___depression_today_12m',
                             'anxiety___depression_3m','anxiety___depression_6m','anxiety___depression_12m',
                             'pain___discomfort_today_3m','pain___discomfort_today_6m','pain___discomfort_today_12m',
                             'pain___discomfort_3m','pain___discomfort_6m','pain___discomfort_12m',
                             'usual_activities_today_3m','usual_activities_today_6m','usual_activities_today_12m',
                             'usual_activities_3m','usual_activities_6m','usual_activities_12m',
                             'mobility_today_3m','mobility_today_6m','mobility_today_12m',
                             'mobility_3m','mobility_6m','mobility_12m',
                             'self-care_today_3m','self-care_today_6m','self-care_today_12m',
                             'self-care_3m','self-care_6m','self-care_12m',
                             'difficulty_remembering_or_concentrating_today_3m',
                             'difficulty_remembering_or_concentrating_today_6m',
                             'difficulty_remembering_or_concentrating_today_12m',
                             'difficulty_remembering_or_concentrating_before_covid19_illness_3m',
                             'confusion_lack_of_concentration_3m','confusion_lack_of_concentration_6m','confusion_lack_of_concentration_12m',
                             'shortness_of_breath_breathlessness_3m',
                             'shortness_of_breath_breathlessness_6m',
                             'shortness_of_breath_breathlessness_12m',
                             'persistent_cough_3m','persistent_cough_6m','persistent_cough_12m',
                             'breathlessness_and_fatigue_not_troubled_3m',
                             'breathlessness_and_fatigue_not_troubled_6m',
                             'breathlessness_and_fatigue_not_troubled_12m',
                             'breathlessness_before_covid_19_not_troubled_3m',
                             'difficulty_climbing_steps_today_3m',
                             'difficulty_climbing_steps_today_6m',
                             'difficulty_climbing_steps_today_12m',
                             'difficulty_climbing_steps_before_your_covid19_illness_3m',
                             'breathlessness_short_of_breath_when_hurrying_walking_3m',
                             'breathlessness_short_of_breath_when_hurrying_walking_6m',
                             'breathlessness_short_of_breath_when_hurrying_walking_12m',
                             'breathlessness_before_covid_19_when_hurrying_or_when_walking_3m',
                             'breathlessness_walk_slower_than_most_people_same_age_3m',
                             'breathlessness_walk_slower_than_most_people_same_age_6m',
                             'breathlessness_walk_slower_than_most_people_same_age_12m',
                             'breathlessness_before_covid_19_walk_slower_than_most_people_same_age_3m',
                             'breathlessness_stop_for_breath_after_walking_100_metres_3m',
                             'breathlessness_stop_for_breath_after_walking_100_metres_6m',
                             'breathlessness_stop_for_breath_after_walking_100_metres_12m',
                             'breathlessness_before_covid_19_after_walking_100_metres_3m',
                             'breathlessness_to_leave_the_house_dressing_undressingd_3m',
                             'breathlessness_to_leave_the_house_dressing_undressingd_6m',
                             'breathlessness_to_leave_the_house_dressing_undressingd_12m',
                             'breathlessness_before_covid_19_to_leave_the_house_dressing_undressing_3m',
                             'weakness_in_limbs_muscle_weakness_3m',
                             'weakness_in_limbs_muscle_weakness_6m',
                             'weakness_in_limbs_muscle_weakness_12m',
                             'joint_pain_or_swelling_3m','joint_pain_or_swelling_6m','joint_pain_or_swelling_12m',
                             'pain_on_breathing_3m','pain_on_breathing_6m','pain_on_breathing_12m',
                             'loss_of_taste_3m','loss_of_taste_6m','loss_of_taste_12m',
                             'loss_of_smell_3m','loss_of_smell_6m','loss_of_smell_12m',
                             'problems_sleeping_3m','problems_sleeping_6m','problems_sleeping_12m',
                             'dizziness_light_headedness_3m','dizziness_light_headedness_6m','dizziness_light_headedness_12m',
                             'headache_3m','headache_6m','headache_12m',
                             'fully_recovered_covid-19_illness_3m','fully_recovered_covid-19_illness_6m','fully_recovered_covid-19_illness_12m'
                             )])
# Store survey responses
cols_main <- colnames(data_main)[!colnames(data_main) %in% c('record_id','age','age_group_name','sex_id',
                                                             'vaccine_against_sars-cov-2','icu_admission')]

# Post-acute consequences of infectious disease - fatigue, bodily pain and mood swings: “is always tired and easily upset. The person feels pain all over the body and is depressed”
data_main <- mutate(data_main,
                    post_acute_3m = ifelse(intensity_fatigue_the_last_24_hours_3m >= 30 &
                                             (anxiety___depression_today_3m >2 | pain___discomfort_today_3m >2) &
                                             (anxiety___depression_today_3m > anxiety___depression_3m |
                                                pain___discomfort_today_3m > pain___discomfort_3m |
                                                usual_activities_today_3m > usual_activities_3m), 1, 0),
                    post_acute_6m = ifelse(intensity_fatigue_the_last_24_hours_6m >= 30 &
                                             (anxiety___depression_today_6m >2 | pain___discomfort_today_6m >2) &
                                             (anxiety___depression_today_6m > anxiety___depression_3m |
                                                pain___discomfort_today_6m > pain___discomfort_3m |
                                                usual_activities_today_6m > usual_activities_3m), 1, 0),
                    post_acute_12m = ifelse(intensity_fatigue_the_last_24_hours_12m >= 30 &
                                             (anxiety___depression_today_12m >2 | pain___discomfort_today_12m >2) &
                                             (anxiety___depression_today_12m > anxiety___depression_3m |
                                                pain___discomfort_today_12m > pain___discomfort_3m |
                                                usual_activities_today_12m > usual_activities_3m) &
                                              (post_acute_3m == 1 | post_acute_6m == 1), 1, 0)
                    )


message(paste("Average 3m post-acute fatigue: "), mean(data_main$post_acute_3m, na.rm=T))
message(paste("Average 6m post-acute fatigue: "), mean(data_main$post_acute_6m, na.rm=T))
message(paste("Average 12m post-acute fatigue: "), mean(data_main$post_acute_12m, na.rm=T))

# Cognitive problems
# •	Mild cognitive problems: “has some trouble remembering recent events and finds it hard to concentrate and make decisions and plans”
data_main <- mutate(data_main,
                    cog_mild_3m = ifelse(difficulty_remembering_or_concentrating_today_3m == 2 &
                                           difficulty_remembering_or_concentrating_today_3m >
                                           difficulty_remembering_or_concentrating_before_covid19_illness_3m &
                                           # confusion_lack_of_concentration_3m == 1 &
                                           usual_activities_today_3m %in% c(2,3), 1, 0),
                    cog_mild_6m = ifelse(difficulty_remembering_or_concentrating_today_6m == 2 &
                                           difficulty_remembering_or_concentrating_today_6m >
                                           difficulty_remembering_or_concentrating_before_covid19_illness_3m &
                                           # confusion_lack_of_concentration_6m == 1 &
                                           usual_activities_today_6m %in% c(2,3), 1, 0),
                    cog_mild_12m = ifelse(difficulty_remembering_or_concentrating_today_12m == 2 &
                                           difficulty_remembering_or_concentrating_today_12m >
                                           difficulty_remembering_or_concentrating_before_covid19_illness_3m &
                                           # confusion_lack_of_concentration_12m == 1 &
                                           usual_activities_today_12m %in% c(2,3), 1, 0)
                                          # & (cog_mild_3m == 1 | cog_mild_6m == 1)
                    )

# •	Moderate cognitive problems: “has memory problems and confusion, feels disoriented, at times hears voices that are not real, and needs help with some daily activities”
data_main <- mutate(data_main,
                    cog_moderate_3m = ifelse(difficulty_remembering_or_concentrating_today_3m > 2 &
                                           difficulty_remembering_or_concentrating_today_3m >
                                           difficulty_remembering_or_concentrating_before_covid19_illness_3m &
                                           # confusion_lack_of_concentration_3m == 1 &
                                           usual_activities_today_3m %in% c(3,4,5), 1, 0),
                    cog_moderate_6m = ifelse(difficulty_remembering_or_concentrating_today_6m > 2 &
                                           difficulty_remembering_or_concentrating_today_6m >
                                           difficulty_remembering_or_concentrating_before_covid19_illness_3m &
                                           # confusion_lack_of_concentration_6m == 1 &
                                           usual_activities_today_6m %in% c(3,4,5), 1, 0),
                    cog_moderate_12m = ifelse(difficulty_remembering_or_concentrating_today_12m > 2 &
                                            difficulty_remembering_or_concentrating_today_12m >
                                            difficulty_remembering_or_concentrating_before_covid19_illness_3m &
                                            # confusion_lack_of_concentration_12m == 1 &
                                            usual_activities_today_12m %in% c(3,4,5), 1, 0),
                                           # & (cog_moderate_3m == 1 | cog_moderate_6m == 1)
                    # Cognitive cluster
                    # If symptom criteria is not met at 3m or 6m, don't count in 12m
                    cognitive_3m = ifelse((cog_mild_3m %in% 1 | cog_moderate_3m %in% 1), 1, 0),
                    cognitive_6m = ifelse((cog_mild_6m %in% 1 | cog_moderate_6m %in% 1), 1, 0),
                    cognitive_12m = ifelse((cognitive_3m %in% 1 | cognitive_6m %in% 1) &
                                             (cog_mild_12m %in% 1 | cog_moderate_12m %in% 1), 1, 0)
                    )

data_main[cognitive_12m == 0, cog_mild_12m := 0]
data_main[cognitive_12m == 0, cog_moderate_12m := 0]

# If there is overlap, keep the more severe case
data_main[, cog_overlap_3m := cog_mild_3m + cog_moderate_3m]
data_main[, cog_overlap_6m := cog_mild_6m + cog_moderate_6m]
data_main[, cog_overlap_12m := cog_mild_12m + cog_moderate_12m]

data_main[cog_overlap_3m >1 & cog_mild_3m == cog_moderate_3m, cog_mild_3m := 0]
data_main[cog_overlap_6m >1 & cog_mild_6m == cog_moderate_6m, cog_mild_6m := 0]
data_main[cog_overlap_12m >1 & cog_mild_12m == cog_moderate_12m, cog_mild_12m := 0]

message(paste("Average 3m cognitive problems: "), mean(data_main$cognitive_3m, na.rm=T))
message(paste("Average 6m cognitive problem: "), mean(data_main$cognitive_6m, na.rm=T))
message(paste("Average 12m cognitive problems: "), mean(data_main$cognitive_12m, na.rm=T))

# Respiratory problems
# •	Mild respiratory problems: “has cough and shortness of breath after heavy physical activity, but is able to walk long distances and climb stairs"

data_main <- mutate(data_main,
                    res_mild_3m = ifelse(((shortness_of_breath_breathlessness_3m==1 &
                                           persistent_cough_3m==1 &
                                           usual_activities_today_3m==2) |
                                            (shortness_of_breath_breathlessness_3m==1 &
                                               persistent_cough_3m==0 &
                                               usual_activities_today_3m>2)) &
                                           ((breathlessness_and_fatigue_not_troubled_3m==1 &
                                               breathlessness_and_fatigue_not_troubled_3m >
                                               breathlessness_before_covid_19_not_troubled_3m) |
                                              (difficulty_climbing_steps_today_3m==1 &
                                                 difficulty_climbing_steps_before_your_covid19_illness_3m==1)), 1, 0),
                    res_mild_6m = ifelse(((shortness_of_breath_breathlessness_6m==1 &
                                             persistent_cough_6m==1 &
                                             usual_activities_today_6m==2) |
                                            (shortness_of_breath_breathlessness_6m==1 &
                                               persistent_cough_6m==0 &
                                               usual_activities_today_6m>2)) &
                                           ((breathlessness_and_fatigue_not_troubled_6m==1 &
                                               breathlessness_and_fatigue_not_troubled_6m >
                                               breathlessness_before_covid_19_not_troubled_3m) |
                                              (difficulty_climbing_steps_today_6m==1 &
                                                 difficulty_climbing_steps_before_your_covid19_illness_3m==1)), 1, 0),
                    res_mild_12m = ifelse(((shortness_of_breath_breathlessness_12m==1 &
                                             persistent_cough_12m==1 &
                                             usual_activities_today_12m==2) |
                                            (shortness_of_breath_breathlessness_12m==1 &
                                               persistent_cough_12m==0 &
                                               usual_activities_today_12m>2)) &
                                           ((breathlessness_and_fatigue_not_troubled_12m==1 &
                                               breathlessness_and_fatigue_not_troubled_12m >
                                               breathlessness_before_covid_19_not_troubled_3m) |
                                              (difficulty_climbing_steps_today_12m==1 &
                                                 difficulty_climbing_steps_before_your_covid19_illness_3m==1)), 1, 0)
                                          # & (res_mild_3m ==1 | res_mild_6m ==1)
                    )

# •	Moderate respiratory problems: “has cough, wheezing and shortness of breath, even after light physical activity. The person feels tired and can walk only short distances or climb only a few stairs.”

data_main <- mutate(data_main,
                    res_moderate_3m = ifelse((shortness_of_breath_breathlessness_3m==1 &
                                               persistent_cough_3m==1 &
                                               usual_activities_today_3m>2) &
                                               ((breathlessness_short_of_breath_when_hurrying_walking_3m==1 &
                                                  breathlessness_short_of_breath_when_hurrying_walking_3m >
                                                  breathlessness_before_covid_19_when_hurrying_or_when_walking_3m) |
                                                  (breathlessness_walk_slower_than_most_people_same_age_3m==1 &
                                                     breathlessness_walk_slower_than_most_people_same_age_3m >
                                                     breathlessness_before_covid_19_walk_slower_than_most_people_same_age_3m) |
                                                  (difficulty_climbing_steps_today_3m==2 &
                                                     difficulty_climbing_steps_today_3m >
                                                     difficulty_climbing_steps_before_your_covid19_illness_3m)),1,0),
                    res_moderate_6m = ifelse((shortness_of_breath_breathlessness_6m==1 &
                                                persistent_cough_6m==1 &
                                                usual_activities_today_6m>2) &
                                               ((breathlessness_short_of_breath_when_hurrying_walking_6m==1 &
                                                   breathlessness_short_of_breath_when_hurrying_walking_6m >
                                                   breathlessness_before_covid_19_when_hurrying_or_when_walking_3m) |
                                                  (breathlessness_walk_slower_than_most_people_same_age_6m==1 &
                                                     breathlessness_walk_slower_than_most_people_same_age_6m >
                                                     breathlessness_before_covid_19_walk_slower_than_most_people_same_age_3m) |
                                                  (difficulty_climbing_steps_today_6m==2 &
                                                     difficulty_climbing_steps_today_6m >
                                                     difficulty_climbing_steps_before_your_covid19_illness_3m)),1,0),
                    res_moderate_12m = ifelse((shortness_of_breath_breathlessness_12m==1 &
                                                persistent_cough_12m==1 &
                                                usual_activities_today_12m>2) &
                                               ((breathlessness_short_of_breath_when_hurrying_walking_12m==1 &
                                                   breathlessness_short_of_breath_when_hurrying_walking_12m >
                                                   breathlessness_before_covid_19_when_hurrying_or_when_walking_3m) |
                                                  (breathlessness_walk_slower_than_most_people_same_age_12m==1 &
                                                     breathlessness_walk_slower_than_most_people_same_age_12m >
                                                     breathlessness_before_covid_19_walk_slower_than_most_people_same_age_3m) |
                                                  (difficulty_climbing_steps_today_12m==2 &
                                                     difficulty_climbing_steps_today_12m >
                                                     difficulty_climbing_steps_before_your_covid19_illness_3m)),1,0)
                    )

sum(!is.na(data2$usual_activities_3m))
unique(data2$usual_activities_today_3m)

# •	Severe respiratory problems: “has cough, wheezing and shortness of breath all the time. The person has great difficulty walking even short distances or climbing any stairs, feels tired when at rest, and is anxious.”

data_main <- mutate(data_main,
                    res_severe_3m = ifelse((shortness_of_breath_breathlessness_3m==1 &
                                              persistent_cough_3m==1 &
                                              usual_activities_today_3m>3) &
                                             ((breathlessness_stop_for_breath_after_walking_100_metres_3m==1 &
                                                 breathlessness_stop_for_breath_after_walking_100_metres_3m >
                                                 breathlessness_before_covid_19_after_walking_100_metres_3m) |
                                                (breathlessness_to_leave_the_house_dressing_undressingd_3m==1 &
                                                   breathlessness_to_leave_the_house_dressing_undressingd_3m >
                                                   breathlessness_before_covid_19_to_leave_the_house_dressing_undressing_3m) |
                                                (difficulty_climbing_steps_today_3m==3 &
                                                   difficulty_climbing_steps_today_3m >
                                                   difficulty_climbing_steps_before_your_covid19_illness_3m)),1,0),
                    res_severe_6m = ifelse((shortness_of_breath_breathlessness_6m==1 &
                                              persistent_cough_6m==1 &
                                              usual_activities_today_6m>3) &
                                             ((breathlessness_stop_for_breath_after_walking_100_metres_6m==1 &
                                                 breathlessness_stop_for_breath_after_walking_100_metres_6m >
                                                 breathlessness_before_covid_19_after_walking_100_metres_3m) |
                                                (breathlessness_to_leave_the_house_dressing_undressingd_6m==1 &
                                                   breathlessness_to_leave_the_house_dressing_undressingd_6m >
                                                   breathlessness_before_covid_19_to_leave_the_house_dressing_undressing_3m) |
                                                (difficulty_climbing_steps_today_6m==3 &
                                                   difficulty_climbing_steps_today_6m >
                                                   difficulty_climbing_steps_before_your_covid19_illness_3m)),1,0),
                    res_severe_12m = ifelse((shortness_of_breath_breathlessness_12m==1 &
                                              persistent_cough_12m==1 &
                                              usual_activities_today_12m>3) &
                                             ((breathlessness_stop_for_breath_after_walking_100_metres_12m==1 &
                                                 breathlessness_stop_for_breath_after_walking_100_metres_12m >
                                                 breathlessness_before_covid_19_after_walking_100_metres_3m) |
                                                (breathlessness_to_leave_the_house_dressing_undressingd_12m==1 &
                                                   breathlessness_to_leave_the_house_dressing_undressingd_12m >
                                                   breathlessness_before_covid_19_to_leave_the_house_dressing_undressing_3m) |
                                                (difficulty_climbing_steps_today_12m==3 &
                                                   difficulty_climbing_steps_today_12m >
                                                   difficulty_climbing_steps_before_your_covid19_illness_3m)),1,0),

                    # Respiratory cluster
                    # If symptom criteria is not met at 3m or 6m, don't count in 12m
                    respiratory_3m = ifelse((res_mild_3m %in% 1 | res_moderate_3m %in% 1 | res_severe_3m %in% 1), 1, 0),
                    respiratory_6m = ifelse((res_mild_6m %in% 1 | res_moderate_6m %in% 1 | res_severe_6m %in% 1), 1, 0),
                    respiratory_12m = ifelse((respiratory_3m %in% 1 | respiratory_6m %in% 1) &
                                       (res_mild_12m %in% 1 | res_moderate_12m %in% 1 | res_severe_12m %in% 1), 1, 0)
                    )

data_main[respiratory_12m == 0, res_mild_12m := 0]
data_main[respiratory_12m == 0, res_moderate_12m := 0]
data_main[respiratory_12m == 0, res_severe_12m := 0]

# If there is overlap, keep the more severe case
data_main[, res_overlap_3m := res_mild_3m + res_moderate_3m + res_severe_3m]
data_main[, res_overlap_6m := res_mild_6m + res_moderate_6m + res_severe_6m]
data_main[, res_overlap_12m := res_mild_12m + res_moderate_12m + res_severe_12m]

data_main[res_overlap_3m >1 & res_mild_3m == res_moderate_3m, res_mild_3m := 0]
data_main[res_overlap_3m >1 & res_moderate_3m == res_severe_3m, res_moderate_3m := 0]

data_main[res_overlap_6m >1 & res_mild_6m == res_moderate_6m, res_mild_6m := 0]
data_main[res_overlap_6m >1 & res_moderate_6m == res_severe_6m, res_moderate_6m := 0]

data_main[res_overlap_12m >1 & res_mild_12m == res_moderate_12m, res_mild_12m := 0]
data_main[res_overlap_12m >1 & res_moderate_12m == res_severe_12m, res_moderate_12m := 0]

message(paste("Average 3m respiratory problems: "), mean(data_main$respiratory_3m, na.rm=T))
message(paste("Average 6m respiratory problem: "), mean(data_main$respiratory_6m, na.rm=T))
message(paste("Average 12m respiratory problems: "), mean(data_main$respiratory_12m, na.rm=T))

# Combine the resp, cognitive and fatigue categories
data_main <- mutate(data_main, Cog_Res_3m = ifelse((cognitive_3m %in% 1 & respiratory_3m %in% 1), 1, 0 ))
data_main <- mutate(data_main, Cog_Fat_3m = ifelse((cognitive_3m %in% 1 & post_acute_3m %in% 1), 1, 0 ))
data_main <- mutate(data_main, Res_Fat_3m = ifelse((respiratory_3m %in% 1 & post_acute_3m %in% 1), 1, 0 ))
data_main <- mutate(data_main, Cog_Res_Fat_3m = ifelse((respiratory_3m %in% 1 & post_acute_3m %in% 1 & cognitive_3m %in% 1),
                                                       1, 0 ))

data_main <- mutate(data_main, Cog_Res_6m = ifelse((cognitive_6m %in% 1 & respiratory_6m %in% 1), 1, 0 ))
data_main <- mutate(data_main, Cog_Fat_6m = ifelse((cognitive_6m %in% 1 & post_acute_6m %in% 1), 1, 0 ))
data_main <- mutate(data_main, Res_Fat_6m = ifelse((respiratory_6m %in% 1 & post_acute_6m %in% 1), 1, 0 ))
data_main <- mutate(data_main, Cog_Res_Fat_6m = ifelse((respiratory_6m %in% 1 & post_acute_6m %in% 1 & cognitive_6m %in% 1),
                                                       1, 0 ))

data_main <- mutate(data_main, Cog_Res_12m = ifelse((cognitive_12m %in% 1 & respiratory_12m %in% 1), 1, 0 ))
data_main <- mutate(data_main, Cog_Fat_12m = ifelse((cognitive_12m %in% 1 & post_acute_12m %in% 1), 1, 0 ))
data_main <- mutate(data_main, Res_Fat_12m = ifelse((respiratory_12m %in% 1 & post_acute_12m %in% 1), 1, 0 ))
data_main <- mutate(data_main, Cog_Res_Fat_12m = ifelse((respiratory_12m %in% 1 & post_acute_12m %in% 1 & cognitive_12m %in% 1),
                                                        1, 0 ))

# New Clusters (sub-clinical) -----------------------------------------------------------------

# Fatigue
data_main <- mutate(data_main,
                    fatigue_3m = ifelse((intensity_fatigue_the_last_24_hours_3m >= 30) &
                                          `fully_recovered_covid-19_illness_3m` <= 4 &
                                          eq5d_summary_today_3m <= 0.9 &
                                          eq5d_summary_today_3m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    fatigue_6m = ifelse((intensity_fatigue_the_last_24_hours_6m >= 30) &
                                          `fully_recovered_covid-19_illness_6m` <= 4 &
                                          eq5d_summary_today_6m <= 0.9 &
                                          eq5d_summary_today_6m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    fatigue_12m = ifelse((intensity_fatigue_the_last_24_hours_12m >= 30) &
                                          `fully_recovered_covid-19_illness_12m` <= 4 &
                                          eq5d_summary_today_12m <= 0.9 &
                                          eq5d_summary_today_12m <= eq5d_summary_before_covid_3m - 0.1 &
                                           (fatigue_3m == 1 | fatigue_6m == 1),1,0)
                    )

message(paste("Average 3m fatigue: "), mean(data_main$fatigue_3m, na.rm=T))
message(paste("Average 6m fatigue: "), mean(data_main$fatigue_6m, na.rm=T))
message(paste("Average 12m fatigue: "), mean(data_main$fatigue_12m, na.rm=T))

# Joint/muscle pain/weakness
data_main <- mutate(data_main,
                    jm_pain_weakness_3m = ifelse((weakness_in_limbs_muscle_weakness_3m == 1 | joint_pain_or_swelling_3m == 1) &
                                                   `fully_recovered_covid-19_illness_3m` <= 4 &
                                                   eq5d_summary_today_3m <= 0.9 &
                                                   eq5d_summary_today_3m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    jm_pain_weakness_6m = ifelse((weakness_in_limbs_muscle_weakness_6m == 1 | joint_pain_or_swelling_6m == 1) &
                                                   `fully_recovered_covid-19_illness_6m` <= 4 &
                                                   eq5d_summary_today_6m <= 0.9 &
                                                   eq5d_summary_today_6m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    jm_pain_weakness_12m = ifelse((weakness_in_limbs_muscle_weakness_12m == 1 | joint_pain_or_swelling_12m == 1) &
                                                   `fully_recovered_covid-19_illness_12m` <= 4 &
                                                   eq5d_summary_today_12m <= 0.9 &
                                                   eq5d_summary_today_12m <= eq5d_summary_before_covid_3m - 0.1 &
                                                    (jm_pain_weakness_3m==1 | jm_pain_weakness_6m==1),1,0),
                    )


# Loss of taste/smell
data_main <- mutate(data_main,
                    taste_smell_loss_3m = ifelse((loss_of_taste_3m == 1 |
                                                   loss_of_smell_3m == 1) &
                                                   `fully_recovered_covid-19_illness_3m` <= 4 &
                                                   eq5d_summary_today_3m <= 0.9 &
                                                   eq5d_summary_today_3m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    taste_smell_loss_6m = ifelse((loss_of_taste_6m == 1 |
                                                    loss_of_smell_6m == 1) &
                                                   `fully_recovered_covid-19_illness_6m` <= 4 &
                                                   eq5d_summary_today_6m <= 0.9 &
                                                   eq5d_summary_today_6m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    taste_smell_loss_12m = ifelse((loss_of_taste_12m == 1 |
                                                    loss_of_smell_12m == 1) &
                                                   `fully_recovered_covid-19_illness_12m` <= 4 &
                                                   eq5d_summary_today_12m <= 0.9 &
                                                   eq5d_summary_today_12m <= eq5d_summary_before_covid_3m - 0.1 &
                                                    (taste_smell_loss_3m==1 | taste_smell_loss_6m==1),1,0),
                    )

message(paste("Average 3m taste_smell_loss: "), mean(data_main$taste_smell_loss_3m, na.rm=T))
message(paste("Average 6m taste_smell_loss: "), mean(data_main$taste_smell_loss_6m, na.rm=T))
message(paste("Average 12m taste_smell_loss: "), mean(data_main$taste_smell_loss_12m, na.rm=T))

# Anxiety/depression
data_main <- mutate(data_main,
                    anxiety_depressed_3m = ifelse(anxiety___depression_today_3m > anxiety___depression_3m &
                                                    `fully_recovered_covid-19_illness_3m` <= 4,1,0),
                    anxiety_depressed_6m = ifelse(anxiety___depression_today_6m > anxiety___depression_3m &
                                                    `fully_recovered_covid-19_illness_6m` <= 4,1,0),
                    anxiety_depressed_12m = ifelse(anxiety___depression_today_12m > anxiety___depression_3m &
                                                    `fully_recovered_covid-19_illness_12m` <= 4 &
                                                     (anxiety_depressed_3m==1 | anxiety_depressed_6m==1),1,0),
                    )

message(paste("Average 3m anxiety_depressed: "), mean(data_main$anxiety_depressed_3m, na.rm=T))
message(paste("Average 6m anxiety_depressed: "), mean(data_main$anxiety_depressed_6m, na.rm=T))
message(paste("Average 12m anxiety_depressed: "), mean(data_main$anxiety_depressed_12m, na.rm=T))

# Sleep problems
data_main <- mutate(data_main,
                    sleep_prob_3m = ifelse((problems_sleeping_3m == 1) &
                                          `fully_recovered_covid-19_illness_3m` <= 4 &
                                          eq5d_summary_today_3m <= 0.9 &
                                          eq5d_summary_today_3m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    sleep_prob_6m = ifelse((problems_sleeping_6m == 1) &
                                          `fully_recovered_covid-19_illness_6m` <= 4 &
                                          eq5d_summary_today_6m <= 0.9 &
                                          eq5d_summary_today_6m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    sleep_prob_12m = ifelse((problems_sleeping_12m == 1) &
                                           `fully_recovered_covid-19_illness_12m` <= 4 &
                                           eq5d_summary_today_12m <= 0.9 &
                                           eq5d_summary_today_12m <= eq5d_summary_before_covid_3m - 0.1 &
                                           (sleep_prob_3m == 1 | sleep_prob_6m == 1),1,0)
)

message(paste("Average 3m sleep_prob: "), mean(data_main$sleep_prob_3m, na.rm=T))
message(paste("Average 6m sleep_prob: "), mean(data_main$sleep_prob_6m, na.rm=T))
message(paste("Average 12m sleep_prob: "), mean(data_main$sleep_prob_12m, na.rm=T))

# Dizzy/light-headedness
data_main <- mutate(data_main,
                    dizziness_3m = ifelse((dizziness_light_headedness_3m == 1) &
                                             `fully_recovered_covid-19_illness_3m` <= 4 &
                                             eq5d_summary_today_3m <= 0.9 &
                                             eq5d_summary_today_3m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    dizziness_6m = ifelse((dizziness_light_headedness_6m == 1) &
                                             `fully_recovered_covid-19_illness_6m` <= 4 &
                                             eq5d_summary_today_6m <= 0.9 &
                                             eq5d_summary_today_6m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    dizziness_12m = ifelse((dizziness_light_headedness_12m == 1) &
                                              `fully_recovered_covid-19_illness_12m` <= 4 &
                                              eq5d_summary_today_12m <= 0.9 &
                                              eq5d_summary_today_12m <= eq5d_summary_before_covid_3m - 0.1 &
                                              (dizziness_3m == 1 | dizziness_6m == 1),1,0)
)

message(paste("Average 3m dizziness: "), mean(data_main$dizziness_3m, na.rm=T))
message(paste("Average 6m dizziness: "), mean(data_main$dizziness_6m, na.rm=T))
message(paste("Average 12m dizziness: "), mean(data_main$dizziness_12m, na.rm=T))

# Headache
data_main <- mutate(data_main,
                    headache_prob_3m = ifelse((headache_3m == 1) &
                                            `fully_recovered_covid-19_illness_3m` <= 4 &
                                            eq5d_summary_today_3m <= 0.9 &
                                            eq5d_summary_today_3m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    headache_prob_6m = ifelse((headache_6m == 1) &
                                            `fully_recovered_covid-19_illness_6m` <= 4 &
                                            eq5d_summary_today_6m <= 0.9 &
                                            eq5d_summary_today_6m <= eq5d_summary_before_covid_3m - 0.1,1,0),
                    headache_prob_12m = ifelse((headache_12m == 1) &
                                             `fully_recovered_covid-19_illness_12m` <= 4 &
                                             eq5d_summary_today_12m <= 0.9 &
                                             eq5d_summary_today_12m <= eq5d_summary_before_covid_3m - 0.1 &
                                             (headache_prob_3m == 1 | headache_prob_6m == 1),1,0)
)

message(paste("Average 3m headache: "), mean(data_main$headache_prob_3m, na.rm=T))
message(paste("Average 6m headache: "), mean(data_main$headache_prob_6m, na.rm=T))
message(paste("Average 12m headache: "), mean(data_main$headache_prob_12m, na.rm=T))

# All long term category ----------------------------------------------------------------------
# long term clusters (original 3 only)
data_main <- mutate(data_main, long_term_3m = ifelse((cognitive_3m %in% 1 | respiratory_3m %in% 1 | post_acute_3m %in% 1), 1, 0 ))
data_main <- mutate(data_main, long_term_6m = ifelse((cognitive_6m %in% 1 | respiratory_6m %in% 1 | post_acute_6m %in% 1), 1, 0 ))
data_main <- mutate(data_main, long_term_12m = ifelse((cognitive_12m %in% 1 | respiratory_12m %in% 1 | post_acute_12m %in% 1) &
                                                        (long_term_3m %in% 1 | long_term_6m %in% 1), 1, 0 ))

# If patients already captured in main clusters, don't count in sub-clusters
data_main <- mutate(data_main,
                    fatigue_3m_residual = ifelse(long_term_3m ==1, 0, fatigue_3m),
                    fatigue_6m_residual = ifelse(long_term_6m ==1, 0, fatigue_6m),
                    fatigue_12m_residual = ifelse(long_term_12m ==1, 0, fatigue_12m),
                    jm_pain_weakness_3m_residual = ifelse(long_term_3m ==1, 0, jm_pain_weakness_3m),
                    jm_pain_weakness_6m_residual = ifelse(long_term_6m ==1, 0, jm_pain_weakness_6m),
                    jm_pain_weakness_12m_residual = ifelse(long_term_12m ==1, 0, jm_pain_weakness_12m),
                    taste_smell_loss_3m_residual = ifelse(long_term_3m ==1, 0, taste_smell_loss_3m),
                    taste_smell_loss_6m_residual = ifelse(long_term_6m ==1, 0, taste_smell_loss_6m),
                    taste_smell_loss_12m_residual = ifelse(long_term_12m ==1, 0, taste_smell_loss_12m),
                    anxiety_depressed_3m_residual = ifelse(long_term_3m ==1, 0, anxiety_depressed_3m),
                    anxiety_depressed_6m_residual = ifelse(long_term_6m ==1, 0, anxiety_depressed_6m),
                    anxiety_depressed_12m_residual = ifelse(long_term_12m ==1, 0, anxiety_depressed_12m),
                    sleep_prob_3m_residual = ifelse(long_term_3m ==1, 0, sleep_prob_3m),
                    sleep_prob_6m_residual = ifelse(long_term_6m ==1, 0, sleep_prob_6m),
                    sleep_prob_12m_residual = ifelse(long_term_12m ==1, 0, sleep_prob_12m),
                    dizziness_3m_residual = ifelse(long_term_3m ==1, 0, dizziness_3m),
                    dizziness_6m_residual = ifelse(long_term_6m ==1, 0, dizziness_6m),
                    dizziness_12m_residual = ifelse(long_term_12m ==1, 0, dizziness_12m),
                    headache_prob_3m_residual = ifelse(long_term_3m ==1, 0, headache_prob_3m),
                    headache_prob_6m_residual = ifelse(long_term_6m ==1, 0, headache_prob_6m),
                    headache_prob_12m_residual = ifelse(long_term_12m ==1, 0, headache_prob_12m),

                    # long term clusters (new only)
                    long_term_extended_3m = ifelse(fatigue_3m_residual==1 |
                                                     jm_pain_weakness_3m_residual==1 |
                                                     taste_smell_loss_3m_residual==1 |
                                                     anxiety_depressed_3m_residual==1 |
                                                     sleep_prob_3m_residual==1 |
                                                     dizziness_3m_residual==1 |
                                                     headache_prob_3m_residual==1, 1, 0),
                    long_term_extended_6m = ifelse(fatigue_6m_residual==1 |
                                                     jm_pain_weakness_6m_residual==1 |
                                                     taste_smell_loss_6m_residual==1 |
                                                     anxiety_depressed_6m_residual==1 |
                                                     sleep_prob_6m_residual==1 |
                                                     dizziness_6m_residual==1 |
                                                     headache_prob_6m_residual==1, 1, 0),
                    long_term_extended_12m = ifelse(fatigue_12m_residual==1 |
                                                     jm_pain_weakness_12m_residual==1 |
                                                     taste_smell_loss_12m_residual==1 |
                                                     anxiety_depressed_12m_residual==1 |
                                                     sleep_prob_12m_residual==1 |
                                                     dizziness_12m_residual==1 |
                                                     headache_prob_12m_residual==1, 1, 0),

                    # long term clusters (original + new)
                    long_term_all_3m = ifelse(long_term_3m==1 | long_term_extended_3m==1, 1, 0),
                    long_term_all_6m = ifelse(long_term_6m==1 | long_term_extended_6m==1, 1, 0),
                    long_term_all_12m = ifelse(long_term_12m==1 | long_term_extended_12m==1, 1, 0)
                    )

data_main <- mutate(data_main,
                    long_term = ifelse((long_term_3m %in% 1 |
                                          long_term_6m %in% 1 |
                                          long_term_12m %in% 1), 1, 0 ),
                    long_term_extended = ifelse((long_term_extended_3m %in% 1 |
                                                   long_term_extended_6m %in% 1 |
                                                   long_term_extended_12m %in% 1), 1, 0 ),
                    long_term_all = ifelse((long_term_all_3m %in% 1 |
                                              long_term_all_6m %in% 1 |
                                              long_term_all_12m %in% 1), 1, 0 ),
                    )

# Save the table with all clusters outputs (no survey responses)
data_main2 <- data_main %>%
  select(-all_of(cols_main)) %>%
  select(-all_of(c("long_term","long_term_extended_3m","long_term_extended_6m","long_term_extended_12m",
                   "long_term_extended","long_term_all")))
openxlsx::write.xlsx(data_main2, paste0(outpath,"Colombia_tabulate_all_data_marked_v",version, ".xlsx"), rowNames=F)

# Save the table with all clusters outputs + relevant survey responses
data_main2 <- data_main %>%
  select(-all_of(c("long_term","long_term_extended_3m","long_term_extended_6m","long_term_extended_12m",
                   "long_term_extended","long_term_all")))
openxlsx::write.xlsx(data_main2, paste0(outpath,"Colombia_tabulate_all_clusters_data_v",version, ".xlsx"), rowNames=F)

# Subset data to export
data_inter <- data_main %>%
  select(-which(grepl("record_id|age$|_overlap|long_term$|extended$|_all$", colnames(data_main)))) %>%
  select(-all_of(cols_main))

data_inter <- merge(data_inter, age_map, by='age_group_name')
colnames(data_inter)
# Tabulate
data_long <- tidyr::gather(data_inter , measure, value, c(post_acute_3m:long_term_all_12m))
suffix <- c("_3m","_3m_residual","_6m","_6m_residual","_12m","_12m_residual")
suffix1 <- c("_3m","_6m","_12m")
desired_cols <- c("age_group_id","age_group_name",'icu_admission','vaccine_against_sars-cov-2',"sex_id",'follow_up',
                  'post_acute','cognitive',"cog_mild", "cog_moderate",
                  'respiratory',"res_mild", "res_moderate", "res_severe",'Cog_Res','Cog_Fat','Res_Fat',"Cog_Res_Fat",
                  'fatigue','fatigue_residual','jm_pain_weakness','jm_pain_weakness_residual',
                  'sleep_prob','sleep_prob_residual','taste_smell_loss','taste_smell_loss_residual',
                  'anxiety_depressed','anxiety_depressed_residual',
                  'dizziness','dizziness_residual','headache','headache_residual',
                  paste0("post_acute",suffix),
                  paste0("cognitive",suffix),
                  paste0("cog_mild",suffix1), paste0("cog_moderate",suffix1),
                  paste0("respiratory",suffix),
                  paste0("res_mild",suffix1), paste0("res_moderate",suffix1), paste0("res_severe",suffix1),
                  paste0("Cog_Res",suffix), paste0("Cog_Fat",suffix), paste0("Res_Fat",suffix), paste0("Cog_Res_Fat",suffix),
                  paste0("fatigue",suffix),paste0('fatigue_residual',suffix),
                  paste0("jm_pain_weakness",suffix),paste0("jm_pain_weakness_residual",suffix),
                  paste0("sleep_prob",suffix),paste0("sleep_prob_residual",suffix),
                  paste0("taste_smell_loss",suffix),paste0("taste_smell_loss_residual",suffix),
                  paste0("anxiety_depressed",suffix),paste0("anxiety_depressed_residual",suffix),
                  paste0("dizziness",suffix),paste0("dizziness_residual",suffix),
                  paste0("headache_prob",suffix),paste0("headache_prob_residual",suffix),
                  paste0("long_term",suffix),paste0("long_term_extended",suffix),paste0("long_term_all",suffix),
                  'long_term','long_term_extended','long_term_all',"N")

# Table by sex and age --------------------------------------------------
cocurr_mu <- data_long %>%
  group_by(age_group_id, age_group_name, sex_id, measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  tidyr::spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0
cocurr_mu <- cocurr_mu %>% arrange(age_group_id, sex_id)

# Filter the list of desired columns based on what actually exists in the dataframe
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_mu)]
cocurr_mu <- cocurr_mu[, existing_cols]

# Transforming table
cocurr_mu2 <- cocurr_mu %>%
  pivot_longer(cols = -c(age_group_id, age_group_name, sex_id, N), names_to = "variable", values_to = "value") %>%
  mutate(follow_up = str_extract(variable, "\\d{1,2}m"),
         follow_up = case_when(
           follow_up == "3m" ~ "03m",
           follow_up == "6m" ~ "06m",
           follow_up == "12m" ~ "12m"),
         variable = str_remove(variable, "_\\d{1,2}m")) %>%
  group_by(age_group_id, age_group_name, sex_id, N, follow_up, variable) %>%
  summarise(value = sum(value, na.rm = TRUE)) %>%
  pivot_wider(names_from = variable, values_from = value) %>%
  arrange(follow_up)

cocurr_mu2[is.na(cocurr_mu2)] <- 0
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_mu2)]
cocurr_mu2 <- cocurr_mu2[, existing_cols]

# Table by age groups ---------------------------------------------------
cocurr_age <- data_long %>%
  group_by(age_group_id, age_group_name, measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  tidyr::spread(measure, var)

cocurr_age <- merge(x=cocurr_age, y=sample_n_byAge, all.y=TRUE)
cocurr_age[is.na(cocurr_age)] <- 0
cocurr_age <- cocurr_age %>% arrange(age_group_id)

# Filter the list of desired columns based on what actually exists in the dataframe
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_age)]
cocurr_age <- cocurr_age[, existing_cols]

# Transforming table
cocurr_age2 <- cocurr_age %>%
  pivot_longer(cols = -c(age_group_id, age_group_name, N), names_to = "variable", values_to = "value") %>%
  mutate(follow_up = str_extract(variable, "\\d{1,2}m"),
         follow_up = case_when(
           follow_up == "3m" ~ "03m",
           follow_up == "6m" ~ "06m",
           follow_up == "12m" ~ "12m"),
         variable = str_remove(variable, "_\\d{1,2}m")) %>%
  group_by(age_group_id, age_group_name, N, follow_up, variable) %>%
  summarise(value = sum(value, na.rm = TRUE)) %>%
  pivot_wider(names_from = variable, values_from = value) %>%
  arrange(follow_up)

cocurr_age2[is.na(cocurr_age2)] <- 0
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_age2)]
cocurr_age2 <- cocurr_age2[, existing_cols]

# Table by vaccination status ---------------------------------------------------
cocurr_vaccine <- data_long %>%
  group_by(`vaccine_against_sars-cov-2`, measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  tidyr::spread(measure, var)

cocurr_vaccine <- merge(x=cocurr_vaccine, y=sample_n_byVaccine, all.y=TRUE)
cocurr_vaccine[is.na(cocurr_vaccine)] <- 0
cocurr_vaccine <- cocurr_vaccine %>% arrange(`vaccine_against_sars-cov-2`)

# Filter the list of desired columns based on what actually exists in the dataframe
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_vaccine)]
cocurr_vaccine <- cocurr_vaccine[, existing_cols]

# Transforming table
cocurr_vaccine2 <- cocurr_vaccine %>%
  pivot_longer(cols = -c(`vaccine_against_sars-cov-2`, N), names_to = "variable", values_to = "value") %>%
  mutate(follow_up = str_extract(variable, "\\d{1,2}m"),
         follow_up = case_when(
           follow_up == "3m" ~ "03m",
           follow_up == "6m" ~ "06m",
           follow_up == "12m" ~ "12m"),
         variable = str_remove(variable, "_\\d{1,2}m")) %>%
  group_by(`vaccine_against_sars-cov-2`, N, follow_up, variable) %>%
  summarise(value = sum(value, na.rm = TRUE)) %>%
  pivot_wider(names_from = variable, values_from = value) %>%
  arrange(follow_up)

cocurr_vaccine2[is.na(cocurr_vaccine2)] <- 0
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_vaccine2)]
cocurr_vaccine2 <- cocurr_vaccine2[, existing_cols]

# Table by icu admission status ----------------------------------------------
cocurr_icu <- data_long %>%
  group_by(icu_admission, measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  tidyr::spread(measure, var)

cocurr_icu <- merge(x=cocurr_icu, y=sample_n_byICU, all.y=TRUE)
cocurr_icu[is.na(cocurr_icu)] <- 0
cocurr_icu <- cocurr_icu %>% arrange(icu_admission)

# Filter the list of desired columns based on what actually exists in the dataframe
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_icu)]
cocurr_icu <- cocurr_icu[, existing_cols]

# Transforming table
cocurr_icu2 <- cocurr_icu %>%
  pivot_longer(cols = -c(icu_admission, N), names_to = "variable", values_to = "value") %>%
  mutate(follow_up = str_extract(variable, "\\d{1,2}m"),
         follow_up = case_when(
           follow_up == "3m" ~ "03m",
           follow_up == "6m" ~ "06m",
           follow_up == "12m" ~ "12m"),
         variable = str_remove(variable, "_\\d{1,2}m")) %>%
  group_by(icu_admission, N, follow_up, variable) %>%
  summarise(value = sum(value, na.rm = TRUE)) %>%
  pivot_wider(names_from = variable, values_from = value) %>%
  arrange(follow_up)

cocurr_icu2[is.na(cocurr_icu2)] <- 0
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_icu2)]
cocurr_icu2 <- cocurr_icu2[, existing_cols]

# Table by sex only ----------------------------------------------------------------
cocurr_sex <- data_long %>%
  group_by(sex_id,measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  tidyr::spread(measure, var)

cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0

# Filter the list of desired columns based on what actually exists in the dataframe
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_sex)]
cocurr_sex <- cocurr_sex[, existing_cols]

# Transforming table
cocurr_sex2 <- cocurr_sex %>%
  pivot_longer(cols = -c(sex_id, N), names_to = "variable", values_to = "value") %>%
  mutate(follow_up = str_extract(variable, "\\d{1,2}m"),
         follow_up = case_when(
           follow_up == "3m" ~ "03m",
           follow_up == "6m" ~ "06m",
           follow_up == "12m" ~ "12m"),
         variable = str_remove(variable, "_\\d{1,2}m")) %>%
  group_by(sex_id, N, follow_up, variable) %>%
  summarise(value = sum(value, na.rm = TRUE)) %>%
  pivot_wider(names_from = variable, values_from = value) %>%
  arrange(follow_up)

cocurr_sex2[is.na(cocurr_sex2)] <- 0
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_sex2)]
cocurr_sex2 <- cocurr_sex2[, existing_cols]
# write.xlsx(cocurr_sex2, paste0(outpath,"Colombia_tabulate_clusters_bySex_v",version, ".xlsx"))

# Create a new workbook
wb <- createWorkbook()
addWorksheet(wb, "bySex")
writeData(wb, "bySex", cocurr_sex2)
addWorksheet(wb, "byAge")
writeData(wb, "byAge", cocurr_age2)
addWorksheet(wb, "byICU")
writeData(wb, "byICU", cocurr_icu2)
addWorksheet(wb, "byVaccination")
writeData(wb, "byVaccination", cocurr_vaccine2)
addWorksheet(wb, "byAgeSex")
writeData(wb, "byAgeSex", cocurr_mu2)
saveWorkbook(wb, paste0(outpath,"Colombia_tabulate_clusters_byGroups_v",version, ".xlsx"), overwrite = TRUE)

# --------------------------------------------------------------------
# Add binary variables for fatigue intensity >= 30 (on a 100 scale)
data2 <- mutate(data2,
                intensity_fatigue_24h_30orMore_3m = ifelse(intensity_fatigue_the_last_24_hours_3m >= 30, 1, 0),
                intensity_fatigue_24h_30orMore_6m = ifelse(intensity_fatigue_the_last_24_hours_6m >= 30, 1, 0),
                intensity_fatigue_24h_30orMore_12m = ifelse(intensity_fatigue_the_last_24_hours_12m >= 30, 1, 0)
)

# Merge new columns
new_cols <- c("record_id", colnames(data_main)[!colnames(data_main) %in% colnames(data2)])
all_data <- merge(data2, data_main[, ..new_cols], by="record_id", all.x=T)

# Extract list of symptoms at different followup time
symp_list_3m <- c(colnames(data2)[colnames(data2) %like% "_3m$" & !colnames(data2) %like% "date|difficulty|before_covid|admitted|vaccin"])
symp_list_6m <- c(colnames(data2)[colnames(data2) %like% "_6m$" & !colnames(data2) %like% "date|difficulty|before_covid|admitted|vaccin"])
symp_list_12m <- c(colnames(data2)[colnames(data2) %like% "_12m$" & !colnames(data2) %like% "date|difficulty|before_covid|admitted|vaccin"])

data_symp <- all_data %>%
  select(all_of(c("record_id",symp_list_3m,symp_list_6m,symp_list_12m)))

# Remove columns that contain only NA values or is character format
data_symp <- data_symp %>% select_if(~any(!is.na(.)) & is.numeric(.))

comb_counts <- function(followup) {
  # Initialize an empty data frame to store the combined counts
  combined_counts <- data.frame()
  followup_cols <- setdiff(names(followup), "record_id")

  # Loop through each column and calculate the count of unique values
  for (col in followup_cols) {
    # print(col)
    # Create a table for the unique values
    counts <- table(followup[[col]])

    # Convert the table to a data frame
    counts_df <- as.data.frame(counts)

    # Rename the columns for merging
    colnames(counts_df) <- c("Value", "Count")

    # Add a column with the name of the current column being processed
    counts_df$Variable <- col

    # Bind the current counts data frame to the combined counts data frame
    combined_counts <- rbind(combined_counts, counts_df)
  }

  # Pivot the dataframe to a wider format
  wide_counts <- pivot_wider(combined_counts, names_from = Value, values_from = Count, values_fill = list(Count = 0))

  # Reorder some columns
  if (0 %in% colnames(wide_counts)){
    wide_counts <- wide_counts[c("Variable", "0", setdiff(names(wide_counts), c("Variable", "0")))]

    # Add a new column that is the sum of columns whose values aren't 0 or 1 for each row
    wide_counts <- wide_counts %>% mutate(sum_row = rowSums(select(., -c("Variable","0","1")), na.rm = TRUE))
    # wide_counts <- wide_counts %>% mutate(sum_unknown = rowSums(select(., c("2")), na.rm = TRUE))

    # Subset to yes/no/unknown variables
    wide_counts$binary <- ifelse(wide_counts$sum_row == 0, 1, 0)
    wide_counts <- wide_counts[wide_counts$binary == 1,]
    wide_counts <- wide_counts[,c("Variable","0","1")]
    colnames(wide_counts) <- c("Variable","No","Yes")
  }

  return(as.data.frame(wide_counts))
}

symp_counts <- comb_counts(data_symp)
symp_counts$follow_up <- ifelse(symp_counts$Variable %like% "_3m$", "03-month", ifelse(symp_counts$Variable %like% "_6m$", "06-month", "12-month"))
# symp_counts <- symp_counts[!symp_counts$Variable %like% "before_covid|admitted|vaccin",]

# Calculate frequencies based on symptoms, recovery
bin_vars <- symp_counts$Variable
bin_vars_3m <- bin_vars[bin_vars %like% "_3m$"]
bin_vars_6m <- bin_vars[bin_vars %like% "_6m$"]
bin_vars_12m <- bin_vars[bin_vars %like% "_12m$"]
bin_vars_longCovid_noDur <- paste0(bin_vars, "_longCovid_noDur")
bin_vars_longCovid_noDur_residual <- paste0(bin_vars, "_longCovid_noDur_residual")
bin_vars_longCovid_noDur_residual_ext <- paste0(bin_vars, "_longCovid_noDur_residual_ext")

all_data <- data.table(all_data)

for (var in bin_vars) {
  var_longCovid_noDur <- paste0(var, "_longCovid_noDur")
  var_longCovid_noDur_residual <- paste0(var, "_longCovid_noDur_residual")
  var_longCovid_noDur_residual_ext <- paste0(var, "_longCovid_noDur_residual_ext")

  if (grepl("_3m$", var)) {
    all_data <- all_data %>%
      mutate(
        !!sym(var_longCovid_noDur) := ifelse(
          .data[[var]] == 1 & .data[['fully_recovered_covid-19_illness_3m']] <= 4 &
            eq5d_summary_today_3m <= 0.9 & eq5d_summary_today_3m <= eq5d_summary_before_covid_3m - 0.1, 1, 0),
        !!sym(var_longCovid_noDur_residual) := ifelse(
          long_term_3m == 1, 0, .data[[!!sym(var_longCovid_noDur)]]),
        !!sym(var_longCovid_noDur_residual_ext) := ifelse(
          long_term_all_3m == 1, 0, .data[[!!sym(var_longCovid_noDur)]])
      )
  } else if (grepl("_6m$", var)){
    all_data <- all_data %>%
      mutate(
        !!sym(var_longCovid_noDur) := ifelse(
          .data[[var]] == 1 & .data[['fully_recovered_covid-19_illness_6m']] <= 4 &
            eq5d_summary_today_6m <= 0.9 & eq5d_summary_today_6m <= eq5d_summary_before_covid_3m - 0.1, 1, 0),
        !!sym(var_longCovid_noDur_residual) := ifelse(
          long_term_3m == 1 | long_term_6m == 1, 0, .data[[!!sym(var_longCovid_noDur)]]),
        !!sym(var_longCovid_noDur_residual_ext) := ifelse(
          long_term_all_3m == 1 | long_term_all_6m == 1, 0, .data[[!!sym(var_longCovid_noDur)]])
      )
    } else if (grepl("_12m$", var)){
      var_3m <- sub("_12m$", "_3m", var)
      var_6m <- sub("_12m$", "_6m", var)
      var_3m <- paste0(var_3m, "_longCovid_noDur")
      var_6m <- paste0(var_6m, "_longCovid_noDur")

      all_data <- all_data %>%
        mutate(
          !!sym(var_longCovid_noDur) := ifelse(
            .data[[var]] == 1 & (.data[[var_3m]] == 1 | .data[[var_6m]] == 1) &
              .data[['fully_recovered_covid-19_illness_12m']] <= 4 &
              eq5d_summary_today_12m <= 0.9 & eq5d_summary_today_12m <= eq5d_summary_before_covid_3m - 0.1, 1, 0),
          !!sym(var_longCovid_noDur_residual) := ifelse(
            long_term == 1, 0, .data[[!!sym(var_longCovid_noDur)]]),
          !!sym(var_longCovid_noDur_residual_ext) := ifelse(
            long_term_all == 1, 0, .data[[!!sym(var_longCovid_noDur)]])
        )
      }
}

# Use rowwise() to operate across each row individually
all_data <- all_data %>%
  rowwise() %>%
  # If any symptom == 1, return 1, or else return 0
  mutate(any_symptoms_3m = min(1,max(c_across(all_of(bin_vars_3m)),0, na.rm=T))) %>%
  mutate(any_symptoms_6m = min(1,max(c_across(all_of(bin_vars_6m)),0, na.rm=T))) %>%
  mutate(any_symptoms_12m = min(1,max(c_across(all_of(bin_vars_12m)),0, na.rm=T))) %>%

  mutate(any_longCovid_noDur_3m = min(1,max(c_across(matches("_3m_longCovid_noDur$")),0, na.rm=T))) %>%
  mutate(any_longCovid_noDur_6m = min(1,max(c_across(matches("_6m_longCovid_noDur$")),0, na.rm=T))) %>%
  mutate(any_longCovid_noDur_12m = min(1,max(c_across(matches("_12m_longCovid_noDur$")),0, na.rm=T))) %>%

  mutate(any_longCovid_noDur_residual_3m = min(1,max(c_across(matches("_3m_longCovid_noDur_residual$")),0, na.rm=T))) %>%
  mutate(any_longCovid_noDur_residual_6m = min(1,max(c_across(matches("_6m_longCovid_noDur_residual$")),0, na.rm=T))) %>%
  mutate(any_longCovid_noDur_residual_12m = min(1,max(c_across(matches("_12m_longCovid_noDur_residual$")),0, na.rm=T))) %>%

  mutate(any_longCovid_noDur_residual_ext_3m = min(1,max(c_across(matches("_3m_longCovid_noDur_residual_ext$")),0, na.rm=T))) %>%
  mutate(any_longCovid_noDur_residual_ext_6m = min(1,max(c_across(matches("_6m_longCovid_noDur_residual_ext$")),0, na.rm=T))) %>%
  mutate(any_longCovid_noDur_residual_ext_12m = min(1,max(c_across(matches("_12m_longCovid_noDur_residual_ext$")),0, na.rm=T))) %>%
  ungroup()

# openxlsx::write.xlsx(all_data, paste0(outpath ,"Colombia_tabulate_all_symptoms_v",version, ".xlsx"), rowNames=F)

# Extract only long covid symptoms and results
new_cols2 <- c(bin_vars, new_cols, bin_vars_longCovid_noDur_residual, bin_vars_longCovid_noDur_residual_ext)
all_symp <- all_data[, new_cols2]

# Move record id column to 1st place
all_symp <- all_symp[c("record_id", setdiff(names(all_symp), "record_id"))]
all_symp <- all_symp[, colnames(all_symp)[!colnames(all_symp) %in% c("long_term","long_term_extended","long_term_all")]]
openxlsx::write.xlsx(all_symp, paste0(outpath ,"Colombia_tabulate_all_symptoms_v",version, ".xlsx"), rowNames=F)

# Calculate total patients with long Covid w/out duration criteria
df_all_noDur <- all_data %>% select(all_of(bin_vars_longCovid_noDur))
df_all_noDur <- data.frame(colSums(df_all_noDur, na.rm = TRUE))
colnames(df_all_noDur) <- "yes_longCovid_noDur"
df_all_noDur$Variable_longCovid_noDur <- rownames(df_all_noDur)
rownames(df_all_noDur) <- c(1:nrow(df_all_noDur))
df_all_noDur <- df_all_noDur %>%
  mutate(Variable = stringr::str_replace(Variable_longCovid_noDur, "_longCovid_noDur", ""))

# Calculate total patients with long Covid w/out duration criteria, not captured in original clusters
df_all_noDur_residual <- all_data %>% select(all_of(bin_vars_longCovid_noDur_residual))
df_all_noDur_residual <- data.frame(colSums(df_all_noDur_residual, na.rm = TRUE))
colnames(df_all_noDur_residual) <- "yes_longCovid_noDur_residual"
df_all_noDur_residual$Variable_longCovid_noDur_residual <- rownames(df_all_noDur_residual)
rownames(df_all_noDur_residual) <- c(1:nrow(df_all_noDur_residual))
df_all_noDur_residual <- df_all_noDur_residual %>%
  mutate(Variable = stringr::str_replace(Variable_longCovid_noDur_residual, "_longCovid_noDur_residual", ""))

# Calculate total patients with long Covid w/out duration criteria, not captured in any long covid clusters (both old and new)
df_all_noDur_residual_ext <- all_data %>% select(all_of(bin_vars_longCovid_noDur_residual_ext))
df_all_noDur_residual_ext <- data.frame(colSums(df_all_noDur_residual_ext, na.rm = TRUE))
colnames(df_all_noDur_residual_ext) <- "yes_longCovid_noDur_residual_ext"
df_all_noDur_residual_ext$Variable_longCovid_noDur_residual_ext <- rownames(df_all_noDur_residual_ext)
rownames(df_all_noDur_residual_ext) <- c(1:nrow(df_all_noDur_residual_ext))
df_all_noDur_residual_ext <- df_all_noDur_residual_ext %>%
  mutate(Variable = stringr::str_replace(Variable_longCovid_noDur_residual_ext, "_longCovid_noDur_residual_ext", ""))

# Calculate "any of the above" row
any_cols <- c("any_symptoms_3m","any_symptoms_6m","any_symptoms_12m",
              "any_longCovid_noDur_3m","any_longCovid_noDur_6m","any_longCovid_noDur_12m",
              "any_longCovid_noDur_residual_3m","any_longCovid_noDur_residual_6m","any_longCovid_noDur_residual_12m",
              "any_longCovid_noDur_residual_ext_3m","any_longCovid_noDur_residual_ext_6m","any_longCovid_noDur_residual_ext_12m")

df_all_any_3m <- all_data %>% select(all_of(any_cols[any_cols %like% "_3m$"]))
df_all_any_3m <- data.frame(t(colSums(df_all_any_3m, na.rm = TRUE)))
df_all_any_3m$No <- NA
df_all_any_3m$Variable <- "any_of_the_symptoms_3m"
df_all_any_3m$follow_up <- "03-month"
setnames(df_all_any_3m, "any_symptoms_3m", "Yes")
setnames(df_all_any_3m, "any_longCovid_noDur_3m", "yes_longCovid_noDur")
setnames(df_all_any_3m, "any_longCovid_noDur_residual_3m", "yes_longCovid_noDur_residual")
setnames(df_all_any_3m, "any_longCovid_noDur_residual_ext_3m", "yes_longCovid_noDur_residual_ext")

df_all_any_6m <- all_data %>% select(all_of(any_cols[any_cols %like% "_6m$"]))
df_all_any_6m <- data.frame(t(colSums(df_all_any_6m, na.rm = TRUE)))
df_all_any_6m$No <- NA
df_all_any_6m$follow_up <- "06-month"
df_all_any_6m$Variable <- "any_of_the_symptoms_6m"
setnames(df_all_any_6m, "any_symptoms_6m", "Yes")
setnames(df_all_any_6m, "any_longCovid_noDur_6m", "yes_longCovid_noDur")
setnames(df_all_any_6m, "any_longCovid_noDur_residual_6m", "yes_longCovid_noDur_residual")
setnames(df_all_any_6m, "any_longCovid_noDur_residual_ext_6m", "yes_longCovid_noDur_residual_ext")

df_all_any_12m <- all_data %>% select(all_of(any_cols[any_cols %like% "_12m$"]))
df_all_any_12m <- data.frame(t(colSums(df_all_any_12m, na.rm = TRUE)))
df_all_any_12m$No <- NA
df_all_any_12m$follow_up <- "12-month"
df_all_any_12m$Variable <- "any_of_the_symptoms_12m"
setnames(df_all_any_12m, "any_symptoms_12m", "Yes")
setnames(df_all_any_12m, "any_longCovid_noDur_12m", "yes_longCovid_noDur")
setnames(df_all_any_12m, "any_longCovid_noDur_residual_12m", "yes_longCovid_noDur_residual")
setnames(df_all_any_12m, "any_longCovid_noDur_residual_ext_12m", "yes_longCovid_noDur_residual_ext")

# Merge stats
df_both <- merge(df_all_noDur, df_all_noDur_residual, by="Variable")
df_both <- merge(df_both, df_all_noDur_residual_ext, by="Variable")
df_both <- merge(symp_counts, df_both, by="Variable")
df_both$Variable_longCovid_noDur <- NULL
df_both$Variable_longCovid_noDur_residual <- NULL
df_both$Variable_longCovid_noDur_residual_ext <- NULL
df_both <- rbind(df_both, df_all_any_3m, df_all_any_6m, df_all_any_12m)
df_both <- df_both %>% arrange(follow_up, Variable)

# Move follow up column to last place
df_both <- df_both[c(setdiff(names(df_both), "follow_up"), "follow_up")]
df_both <- df_both[!df_both$Variable %like% "any_",]

# If there are no cases not captured by any of the long-covid clusters, remove the column
if (unique(df_both$yes_longCovid_noDur_residual_ext)==0){
  df_both$yes_longCovid_noDur_residual_ext <- NULL
}

openxlsx::write.xlsx(df_both, paste0(outpath ,"Colombia_tabulate_individual_symptoms_v",version, ".xlsx"),rowNames=F)
