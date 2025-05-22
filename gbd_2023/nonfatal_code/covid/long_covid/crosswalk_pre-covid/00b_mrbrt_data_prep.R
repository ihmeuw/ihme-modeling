#--------------------------------------------------------------
# Name: NAME (USERNAME)
# Date: 27 Feb 2021
# Project: GBD nonfatal COVID
# Purpose: Data categorization of long covid symptom cluster data. Marking
#   covariates, adjust mean/SE.
# Inputs: From 00a - Long covid symptom cluster data crosswalked to reference def
# Outputs: long covid symptom cluster data categorized and adjusted for input
#   into MRBRT.
#--------------------------------------------------------------


# Setup -----------------------------------------------------------------------

# clear workspace
rm(list=ls())
# setwd("FILEPATH")

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH'
  h_root <- '~/'
} else {
  j_root <- 'FILEPATH/'
  h_root <- 'FILEPATH/'
}


# load packages
library(data.table)
library(ggplot2)

output_dir <- "FILEPATH"

datadate <- '101424' #'090924' #'082024' #'070524' #'061124' #'012822'
data_path <- paste0("FILEPATH", datadate, ".csv")

# Load Data -------------------------------------------------------------------
dataset_orig <- read.csv(data_path)
dataset <- setDT(dataset_orig)
str(dataset)

dataset[, follow_up_index := as.character(follow_up_index)]
table(dataset$study_id)

# Data Prep -------------------------------------------------------------------

table(dataset$symptom_cluster, useNA = 'always')
table(dataset$condition, useNA = 'always')
# table(dataset$Group, dataset$study_id)
unique(dataset$condition)
sort(unique(dataset$symptom_cluster))

remove_group <- c("amnesia","claudication","cognitive sub-threshold","hallucination","hyperactive","mental symptoms",
                  "respiratory sub-threshold","gbs")

main_group <- unique(dataset$symptom_cluster)[!unique(dataset$symptom_cluster) %in% remove_group]
sub_group <- unique(dataset[condition == "residuals", symptom_cluster])

# Filter out residual clusters
# dataset <- dataset[!is.na(symptom_cluster)]
# dataset <- dataset[!is.na(symptom_cluster) & symptom_cluster %in% main_group]
dataset <- dataset[!symptom_cluster %in% remove_group]

# Filter out

# For cohorts that have both ref & wo_pre data, remove wo_pre data
dataset <- dataset[!(study_id %in% c("Sechenov StopCOVID", "Colombia CC") & !is.na(mean_wo_pre))]

dataset <- dataset[!(study_id %in% c("Zurich CI - Ticino", "Zurich CI - Zurich") & sample_characteristics == "no baseline sample")]

# Remove studies without number of cases & mean
dataset_rmv <- dataset[(is.na(cases) & (is.na(mean) | mean==0))]
dataset <- dataset[!(is.na(cases) & (is.na(mean) | mean==0))]
unique(dataset[is.na(cases) & (is.na(mean) | mean==0), NID])

## Symptom Clusters --------------------------------------------------------

table(dataset$symptom_cluster, useNA="always")
# dataset[symptom_cluster %in% c("any symptom cluster", "Persistent symtpoms related to illness (new/worsened)"), outcome := "any"]
dataset[symptom_cluster %in% c("any symptom"), outcome := "any"]
dataset[symptom_cluster %in% c("any symptom main cluster"), outcome := "any_main"]
dataset[symptom_cluster %in% c("any symptom new cluster"), outcome := "any_new"]
dataset[symptom_cluster %in% sub_group, outcome := "sub"]

dataset[symptom_cluster %in% c("post-acute fatigue syndrome"), outcome := "fat"]
dataset[symptom_cluster == "cognitive", outcome := "cog"]
dataset[symptom_cluster == "respiratory", outcome := "rsp"]

dataset[symptom_cluster %in% c("post-acute fatigue and cognitive"), outcome := "fat_cog"]
dataset[symptom_cluster %in% c("post-acute fatigue and respiratory"), outcome := "fat_rsp"]
dataset[symptom_cluster %in% c("respiratory and cognitive"), outcome := "cog_rsp"]
dataset[symptom_cluster %in% c("post-acute fatigue and respiratory and cognitive"), outcome := "fat_cog_rsp"]

dataset[symptom_cluster %in% c("mild respiratory among respiratory"), outcome := "mild_rsp"]
dataset[symptom_cluster %in% c("moderate respiratory among respiratory"), outcome := "mod_rsp"]
dataset[symptom_cluster %in% c("severe respiratory among respiratory"), outcome := "sev_rsp"]

dataset[symptom_cluster %in% c("mild cognitive among cognitive"), outcome := "mild_cog"]
dataset[symptom_cluster %in% c("moderate cognitive among cognitive"), outcome := "mod_cog"]

table(dataset$outcome, useNA = 'always')
table(dataset$symptom_cluster[is.na(dataset$outcome)], useNA = 'always')
table(dataset$outcome, dataset$condition)

dataset[, is_outlier := 0]

## Covariates --------------------------------------------------------

table(dataset$outcome_name, useNA = 'always')
table(dataset$symptom_cluster, useNA = 'always')

## Memory -----------------------
dataset[, memory_problems := 0]

# Define the list of outcome names for memory problems
memory_outcomes <-
  c(
    "memory problems", "Memory difficulties", "Mental slowness",
    "Concentration problems", "confusion/lack of concentration",
    "Amnesic complaints", "Memory loss", "Memory problems",
    "New or worsened concentration problem",
    "New or worsened short-term memory problem", "confusion",
    "Cognitive fuzziness/brain fog/difficulty concentrating",
    "cognitive problems", "concentration memory probs",
    "confusion, disorientation, or drowsiness", "memory disorders",
    "memory/concentration impairment", "concentration difficulties",
    "concentration issues", "memory impairment", "cognitive failure"
  )

memory_outcomes

# Set memory_problems to 1 for rows where outcome_name is in the memory_outcomes list
dataset[symptom_cluster %in% c("cognitive") &
          (outcome_name %in% memory_outcomes | lit_nonlit == 'lit'), 
        memory_problems := 1]

dataset[study_id == "Sechenov StopCOVID peds", memory_problems := 0]

table(dataset$outcome_name[dataset$memory_problems != 1 & dataset$symptom_cluster == "cognitive"])

## Fatigue -----------------------------------------
dataset[, fatigue := 0]

# Define the list of outcome names for fatigue
fatigue_outcomes <- c("Fatigue", "fatigue", "Fatigue or muscle weakness",
                      "physical decline/ fatigue",
                      "fatigue ", "tiredness", "fatigue and worse than pre-COVID")

fatigue_outcomes

# Set fatigue to 1 for rows where outcome_name is in the fatigue_outcomes list
dataset[symptom_cluster %in% c("post-acute fatigue syndrome") &
          (outcome_name %in% fatigue_outcomes | lit_nonlit == 'lit'), 
        fatigue := 1]

dataset[study_id == "Sechenov StopCOVID peds", fatigue := 0]

## Cough ------------------------------------------
dataset[, cough := 0]

# Define the list of outcome names for fatigue
cough_outcomes_old <- c("Cough", "cough", "Cough (new/worsened)", "persistent cough")
cough_outcomes <- unique(dataset[outcome_name %like% "cough|Cough" &
                                   !outcome_name %like% "mild|moderate|severe", outcome_name])
cough_outcomes_old[!cough_outcomes_old %in% cough_outcomes]
cough_outcomes[!cough_outcomes %in% cough_outcomes_old]
cough_outcomes

# Set cough to 1 for rows where outcome_name matches "Cough" or variations of it
dataset[symptom_cluster %in% c("respiratory") & outcome_name %in% cough_outcomes, cough := 1]

## Shortness of breath ----------------------------------------
dataset[, shortness_of_breath := 0]

# Define the list of outcome names for fatigue
shortness_of_breath_outcomes_old <- c(
    "shortness of breath", "Shortness of breath", "Dyspnea", "Breathlessness",
    "difficulty breathing, chest tightness", "Dyspnoea",
    "Shortness of breath/chest tightness/ wheezing (new/worsened)",
    "breathing difficulties", "dyspnea", "postactivity polypnoea", "dyspnoea",
    "exertional dyspnoe", "exertional dyspnoea", "respiratory disorders",
    "respiratory symptoms")

shortness_of_breath_outcomes <- unique(dataset[
  outcome_name %like% "breath|Breath|SOB|dysp|Dysp|respiratory|breathing|postactivity polypnoea|wheez" &
    !outcome_name %like% "palpitation|mild|moderate|severe", outcome_name])

shortness_of_breath_outcomes_old[!shortness_of_breath_outcomes_old %in% shortness_of_breath_outcomes]
shortness_of_breath_outcomes[!shortness_of_breath_outcomes %in% shortness_of_breath_outcomes_old]
shortness_of_breath_outcomes

# Set shortness_of_breath to 1 for rows where outcome_name matches variations of "shortness of breath"
dataset[symptom_cluster %in% c("respiratory") & outcome_name %in% shortness_of_breath_outcomes, shortness_of_breath := 1]

dataset[study_id == 'Sechenov StopCOVID peds', shortness_of_breath := 0]

## Administrative -------------------------------------------
dataset[, administrative := 0]
dataset[sample_characteristics == "administrative data" |
          study_id == "Veterans Affairs" |
          study_id == "PRA", administrative := 1]


dataset[outcome_name == "mMRC >= 2" |
          outcome_name == "dyspnea at rest (our severe resp category)" |
          (outcome_name %like% "mild|moderate|severe|unusual fatigue/tiredness|maximal|minimal" & lit_nonlit == "lit"), exclude := 1]

dataset[is.na(exclude), exclude := 0]

table(dataset$exclude, useNA = "always")
unique(dataset[memory_problems == 0 & fatigue == 0 & cough == 0 &
                shortness_of_breath == 0, outcome_name])

## Other Symptom List --------------------------------------------------------
# OTHER SYMPTOM LIST FOR "ANY LONG COVID"

dataset[, other_list := 0]

# Specify the list of outcome names to match (to remove since we now have 'any symptom')
outcome_names_old <- c(
  "from their symptom list",
  "Persistent symtpoms related to illness (new/worsened)",
  "any_symptom",
  "new or persistent symptoms",
  "Post-COVID syndrome, defined as the persistence of at least one clinically relevant symptom, spirometry disturbances or significant radiological alterations",
  "symptomatic",
  "any persistent symptoms",
  "overall"
)

outcome_names <- unique(dataset[outcome == "any", outcome_name])
outcome_names_old[!outcome_names_old %in% outcome_names]
outcome_names[!outcome_names %in% outcome_names_old]

# Select rows where outcome_name matches any of the specified outcome names
# dataset[outcome_name %in% outcome_names, other_list := 1]
dataset[outcome %in% c("any"), other_list := 1]

table(dataset$other_list, dataset$memory_problems)
table(dataset$other_list, dataset$fatigue)
table(dataset$other_list, dataset$cough)
table(dataset$other_list, dataset$shortness_of_breath)

# table(dataset[, outcome_name], dataset[, outcome])
#
# table(dataset[memory_problems == 0 & fatigue == 0 & cough == 0 &
#                 shortness_of_breath == 0, outcome_name],
#       dataset[memory_problems == 0 & fatigue == 0 & cough == 0 &
#                 shortness_of_breath == 0, other_list])

## Mean, Standard Error ----------------------------------------------------
num_cols <- c("mean", "cases", "sample_size", "sample_size_envelope", "lower",
              "upper", "follow_up_value")

dataset[, (num_cols ):= lapply(.SD, as.numeric), .SDcols = c(num_cols)]

# fill in blank means as cases / sample size for main symptom clusters
sum(is.na(dataset$mean))
dataset[is.na(mean) & (outcome %in% c('any', 'any_main', 'any_new', 'cog', 'fat', 'rsp')),
        mean := cases / sample_size]

# fill in blank means as cases / sample size for overlaps where mean_adjusted is blank
# no longer needed since mean_adjusted are filled for all overlaps
table(dataset$outcome,
      is.na(dataset$mean_adjusted) & (!is.na(dataset$sample_size_envelope) | dataset$sample_size_envelope !=0) & dataset$cases !=0)

data_check <- dataset[is.na(dataset$mean_adjusted) &
                        dataset$outcome %in% c("fat_cog","fat_rsp","cog_rsp","fat_cog_rsp") &
                        !(is.na(dataset$sample_size_envelope) | dataset$sample_size_envelope ==0) &
                        dataset$cases !=0]
# dataset[
#   !is.na(sample_size_envelope) &
#     !is.na(mean_adjusted) &
#     (study_id %in% c(
#       "CO-FLOW", "HAARVI", "Faroe", "Iran", "Italy ISARIC",
#       "Sechenov StopCOVID", "Sweden PronMed", "Zurich CC"
#     )) &
#     (outcome %in% c(
#       "cog_rsp", "fat_cog", "fat_cog_rsp", "fat_rsp", "mild_cog",
#       "mild_rsp", "mod_cog", "mod_rsp", "sev_rsp"
#     )),
#   mean := mean_adjusted
# ]
#
# dataset[
#   !is.na(sample_size_envelope) &
#     is.na(mean_adjusted) &
#     (study_id %in% c(
#       "CO-FLOW", "HAARVI", "Faroe", "Iran", "Italy ISARIC",
#       "Sechenov StopCOVID", "Sweden PronMed", "Zurich CC"
#     )) &
#     (outcome %in% c(
#       "cog_rsp", "fat_cog", "fat_cog_rsp", "fat_rsp", "mild_cog",
#       "mild_rsp", "mod_cog", "mod_rsp", "sev_rsp"
#     )),
#   mean := cases / sample_size_envelope
# ]

# standard error for cases>=5:
table(is.na(dataset$standard_error))
dataset[
  cases >= 5 & mean >= 0 &
    !is.na(cases) &
    is.na(standard_error) &
    !(is.na(dataset$sample_size) | dataset$sample_size == 0) &
    (outcome %in% c("any", 'any_main', 'any_new', "cog", "fat", "rsp", "sub")),
  standard_error := sqrt((mean * (1 - mean)) / sample_size)
]

dataset[
  cases >= 5 & mean >= 0 &
    !is.na(cases) &
    is.na(standard_error) &
    !(is.na(dataset$sample_size_envelope) | dataset$sample_size_envelope == 0) &
    (outcome %in% c(
      "cog_rsp", "fat_cog", "fat_cog_rsp", "fat_rsp",
      "mild_cog", "mod_cog", "mild_rsp", "mod_rsp", "sev_rsp"
    )),
  standard_error := sqrt((mean * (1 - mean)) / sample_size_envelope)
]

dataset[
  cases >= 5 & mean >= 0 &
    !is.na(cases) &
    study_id == "PRA" &
    (outcome %in% c("cog_rsp", "fat_cog", "fat_cog_rsp", "fat_rsp")),
  standard_error := sqrt((mean * (1 - mean)) / sample_size)
]

# standard error for cases<5, use Wilson's formula:
dataset[
  cases < 5 &
    !is.na(cases) & mean >= 0 &
    is.na(standard_error) &
    outcome %in% c("any", 'any_main', 'any_new', "cog", "fat", "rsp", "sub"),
  standard_error := sqrt(((mean * (1 - mean)) / sample_size) + ((1.96^2) / (4 * sample_size^2))) / (1 + (1.96^2 / sample_size))
]

dataset[
  cases < 5 &
    !is.na(cases) & mean >= 0 &
    is.na(standard_error) &
    (outcome %in% c('cog_rsp', 'fat_cog', 'fat_cog_rsp', 'fat_rsp', 'mild_cog',
                    'mild_rsp', 'mod_cog', 'mod_rsp', 'sev_rsp')),
  standard_error := sqrt(((mean*(1-mean)) / sample_size_envelope) + ((1.96^2) / (4*sample_size_envelope^2))) / (1 + (1.96^2 / sample_size_envelope))
]

dataset[
  cases < 5 &
    !is.na(cases) & mean >= 0 &
    study_id == "PRA" &
    (outcome %in% c('cog_rsp', 'fat_cog', 'fat_cog_rsp', 'fat_rsp')),
  standard_error := sqrt(((mean * (1 - mean)) / sample_size) + ((1.96^2) / (4 * sample_size^2))) / (1 + (1.96^2 / sample_size))
]

# the only data missing both cases and standard error are VA data, which have lower and upper
dataset[is.na(standard_error), standard_error := (upper - lower) / 3.96]

# Drop rows where standard error is NA and sample size is 0
dataset <- dataset[!(is.na(standard_error) & sample_size_envelope == 0)]

## Community Vs. Hospital Vs. ICU --------------------------------------------
dataset[, hospital := -1]
dataset[, icu := -1]

unique(dataset$sample_population)
table(dataset$sample_population)

hosp_icu_list <- c("general population", "outpatient", "community",
                   "community + hospital + ICU", "Non-hospitalized",
                   "Community", "community + hospital + icu", "comm",
                   "largely comm (10.4% vs 9.3% hospitalised for symptoms in those with pos and neg PCR tests, respectively; see T3)",
                   "community, hospital", "community, hospital, ICU", "hospital, community",
                   "community, hospital, Pre-Delta", "community, hospital, Delta", "community, hospital, Omicron",
                   "hospitalized", "hospitalized and outpatient", "hospitalized + ICU",
                   "hospitalized + icu", "hospital", "hospital + icu",
                   "hospital; receiving oxygen alone", "hosptial + icu", "hospital + ICU",
                   "hospitalized and icu", "hospitalized, icu, and outpatient",
                   "Hospitalized", "hosp", "hosp 48, ICU 10",
                   "hospital (inpatients and outpatients)", "hospitalized (non-icu)",
                   "hospital, ICU", "hospital, Beta", "hospital, Delta", "hospital, Omicron")
hosp_icu_data <- unique(dataset$sample_population)
hosp_icu_data[!hosp_icu_data %in% hosp_icu_list]

# Set hospital to 0 for specified sample population values
dataset[
  sample_population %in% c(
    "general population", "outpatient", "community",
    "community + hospital + ICU", "Non-hospitalized",
    "Community", "community + hospital + icu", "comm",
    "largely comm (10.4% vs 9.3% hospitalised for symptoms in those with pos and neg PCR tests, respectively; see T3)",
    "community, hospital", "community, hospital, ICU", "hospital, community",
    "community, hospital, Pre-Delta", "community, hospital, Delta", "community, hospital, Omicron"),
  hospital := 0
]

# Set hospital to 1 for specified sample population values
dataset[
  sample_population %in% c(
    "hospitalized", "hospitalized and outpatient", "hospitalized + ICU",
    "hospitalized + icu", "hospital", "hospital + icu",
    "hospital; receiving oxygen alone", "hosptial + icu", "hospital + ICU",
    "hospitalized and icu", "hospitalized, icu, and outpatient",
    "Hospitalized", "hosp", "hosp 48, ICU 10",
    "hospital (inpatients and outpatients)", "hospitalized (non-icu)",
    "hospital, ICU", "hospital, Beta", "hospital, Delta", "hospital, Omicron"),
  hospital := 1
]

# Set icu to 0 for specified sample population values or when hospital is 1
dataset[
  sample_population %in% c(
    "general population", "outpatient", "community", "community + hospital + ICU",
    "Non-hospitalized", "Community", "community + hospital + icu", "comm",
    "largely comm (10.4% vs 9.3% hospitalised for symptoms in those with pos and neg PCR tests, respectively; see T3)",
    "community, hospital", "community, hospital, ICU", "hospital, community",
    "community, hospital, Pre-Delta", "community, hospital, Delta", "community, hospital, Omicron") | hospital == 1,
  icu := 0
]

# Set icu to 1 for specified sample population values
dataset[sample_population %in% c("ICU patients", "ICU", "icu"), icu := 1]

# Set hospital to 0 if icu is 1
dataset[icu == 1, hospital := 0]

table(dataset$sample_population, dataset$icu)
table(dataset$sample_population, dataset$hospital)

# Set hospital_or_icu to 1 if icu is 1 or hospital is 1
dataset[, hospital_or_icu := 0]
dataset[hospital == 1 | icu == 1, hospital_or_icu := 1]

# Set hospital_and_icu to 1 for rows where hospital is 1 and sample_population
# contains 'icu' (case-insensitive)
dataset[, hospital_and_icu := 0]
dataset[hospital == 1 & grepl('icu', sample_population, ignore.case = TRUE), hospital_and_icu := 1]
table(dataset$hospital, dataset$icu)

table(dataset[hospital == -1 & icu == -1, sample_population])
table(dataset[icu == 0, sample_population])
table(dataset$sample_population, dataset$hospital_or_icu)
table(dataset$sample_population, dataset$hospital_and_icu)

num_cols <- c("hospital", "icu", "hospital_or_icu", "hospital_and_icu")
dataset[, (num_cols ):= lapply(.SD, as.numeric), .SDcols = c(num_cols)]

dataset[study_id=="Garrigues et al" & sample_population=="hospital + icu", exclude := 1]


## Follow-up Time -----------------------------------------------------------

table(dataset$follow_up_value, useNA = 'always')
table(dataset$follow_up_index, useNA = 'always')
table(dataset$follow_up_units, useNA = 'always')

dataset[, follow_up_value := as.numeric(follow_up_value)]

#dataset$follow_up_units[is.na(dataset$follow_up_value)] <- 0
dataset[follow_up_units == 'days', follow_up_days := follow_up_value]
dataset[follow_up_units == 'weeks', follow_up_days := follow_up_value * 7]
dataset[follow_up_units %in% c('month', 'months'), follow_up_days := follow_up_value * 30]
dataset[follow_up_units == 'year', follow_up_days := follow_up_value * 365]

table(dataset$follow_up_days, useNA='always')
unique(dataset$follow_up_days)
table(dataset$follow_up_index[dataset$hospital == 0 & dataset$icu == 0])
table(dataset$follow_up_index[dataset$hospital == 1 & dataset$icu == 0])
table(dataset$follow_up_index[dataset$hospital == 0 & dataset$icu == 1])
table(dataset$follow_up_index[dataset$hospital == 1 & dataset$icu == 1])

unique(dataset$follow_up_days[dataset$follow_up_index=="COVID diagnosis"])
table(dataset$follow_up_index)
unique(dataset$follow_up_days[dataset$hospital==0 & dataset$icu==0])

# community cases benchmarked from COVID diagnosis = 7 days after infection,
# 14 - 7 = 7 left of acute phase
dataset[
  hospital == 0 & icu == 0 & follow_up_index %like% "COVID diagnosis",
  follow_up_days := follow_up_days - 7
]

# community cases benchmarked from infection = 14 days left of acute phase
dataset[
  hospital == 0 & icu == 0 & !is.na(follow_up_days) &
  follow_up_index == "infection",
  follow_up_days := follow_up_days - 14]

# community cases benchmarked from symptom onset = 5 days after infection,
# 14 - 5 = 9 days left of acute phase
dataset[
  hospital == 0 & icu == 0 & !is.na(follow_up_days) &
    follow_up_index == "symptom onset",
  follow_up_days := follow_up_days - 9
]

# community cases benchmarked from 30 days after COVID diagnosis = (30-7)=
# 22 days after infection, 8 days extra follow-up time
dataset[
  hospital == 0 & icu == 0 &
    !is.na(follow_up_days) &
    follow_up_index == "30 days after COVID diagnosis",
  follow_up_days := follow_up_days + 8
]

# community cases benchmarked from end of acute phase = follow_up_days, no change!

# hospital cases benchmarked from hospital discharge = 26 days after infection,
# 35 - 26 = 9 days left of acute phase
dataset[
  hospital == 1 & icu == 0 &
    (follow_up_index == "hospital discharge" |
      follow_up_index == "Hospital or ICU discharge" |
       follow_up_index == "hospital discharge & negative PCR"),
  follow_up_days := follow_up_days - 9
]

# hospital cases benchmarked from COVID diagnosis = 8 days after infection
# (let's say COVID diagnosis is 3 days after symptom onset), 35 - 8 = 27 days
# left of acute phase
dataset[
  hospital == 1 & !is.na(follow_up_days) & follow_up_index %like% "COVID diagnosis",
  follow_up_days := follow_up_days - 27
]

# hospital cases benchmarked from 30 days after COVID diagnosis = 27 days after
# infection (let's say COVID diagnosis is 3 days after symptom onset),
# 30+8 > 35 by 3 days after acute phase
dataset[
  hospital == 1 & !is.na(follow_up_days) & follow_up_index == "30 days after COVID diagnosis",
  follow_up_days := follow_up_days + 3
]

# hospital cases benchmarked from hospital admission = 12 days after infection,
# 35 - 12 = 23 days left of acute phase
dataset[
  hospital == 1 & !is.na(follow_up_days) & follow_up_index == "hospital admission",
  follow_up_days := follow_up_days - 23
]

# hospital cases benchmarked from symptom onset = 5 days after infection,
# 35 - 5 = 30 days left of acute phase
dataset[
  hospital == 1 & !is.na(follow_up_days) &
    (follow_up_index == "symptom onset" |
      follow_up_index == "disease onset"),
  follow_up_days := follow_up_days - 30
]

# hospital cases benchmarked from end of acute phase = follow_up_days, no change!

# ICU cases benchmarked from COVID diagnosis = 8 days after infection
# (let's say COVID diagnosis is 3 days after symptom onset), 42 - 8 = 34 days
# left of acute phase
dataset[
  icu == 1 & !is.na(follow_up_days) & follow_up_index %like% "COVID diagnosis",
  follow_up_days := follow_up_days - 34
]

# ICU cases benchmarked from ICU discharge = 28 days after infection,
# 42 - 28 = 14 days left of acute phase
dataset[
  icu == 1 & !is.na(follow_up_days) & follow_up_index == "ICU discharge",
  follow_up_days := follow_up_days - 14
]

# ICU cases benchmarked from 30 days after COVID diagnosis = 8 days after
# infection (let's say COVID diagnosis is 3 days after symptom onset),
# 42 - 38 = 4 days left of acute phase
dataset[
  icu == 1 & !is.na(follow_up_days) & follow_up_index == "30 days after COVID diagnosis",
  follow_up_days := follow_up_days - 4
]

# ICU cases benchmarked from hospital admission = 12 days after infection,
# 42 - 12 = 30 days left of acute phase
dataset[
  icu == 1 & !is.na(follow_up_days) & follow_up_index == "hospital admission",
  follow_up_days := follow_up_days - 30
]

# ICU cases benchmarked from hospital discharge = 32 days after infection,
# 42 - 32 = 10 days left of acute phase
dataset[
  icu == 1 & !is.na(follow_up_days) & follow_up_index == "hospital discharge",
  follow_up_days := follow_up_days - 10
]

# ICU cases benchmarked from ICU admission = 14 days after infection,
# 42 - 14 = 28 days left of acute phase
dataset[
  icu == 1 & !is.na(follow_up_days) & follow_up_index == "ICU admission",
  follow_up_days := follow_up_days - 28
]

# ICU cases benchmarked from symptom onset = 5 days after infection,
# 42 - 5 = 37 days left of acute phase
dataset[
  icu == 1 & !is.na(follow_up_days) & (follow_up_index == "symptom onset"),
  follow_up_days := follow_up_days - 37
]

unique(dataset$follow_up_days)
table(dataset$follow_up_index)
table(dataset$study_id, dataset$follow_up_value)
table(dataset$study_id, dataset$follow_up_days)
unique(dataset$follow_up_days)

## Sex ----------------------------------------------------------------------

dataset[, female := 0]
dataset[, male := 0]

table(dataset$sex)
# dataset[sex == "58% male", sex := "Both"]
dataset[sex %in% c("Female", "female", "2", "F"), female := 1]
dataset[sex %in% c("Male", "male", "1", "M"), male := 1]

## Children ------------------------------------------------------------------

dataset[, children := 0]
dataset[age_end < 20, children := 1]

table(dataset$study_id, dataset$children)

## Age ----------------------------------------------------------------------

dataset[, age_start := as.numeric(age_start)]
dataset[, age_end := as.numeric(age_end)]

dataset[, age_range := age_end - age_start]

dataset[, age_specific := 0]
dataset[age_range < 40, age_specific := 1]

# Set age_specific to 0 for rows that meet the combined conditions
table(dataset[study_id %in% c("Sechenov StopCOVID peds", "García‑Abellán J et al","CLoCk", "Asadi-Pooya A et al"), age_range])

dataset[
  age_range >= 40 |
    study_id %in% c("Sechenov StopCOVID peds", "García‑Abellán J et al", "Garcia‑Abellan J et al",
                    "CLoCk", "Asadi-Pooya A et al") |
    (study_id == "Italy ISARIC" & age_start == 0 & age_end == 19),
  age_specific := 0
]

dataset[age_range >= 12 & children == 1, age_specific := 0]
dataset[, age_range := NULL]
table(dataset$study_id, dataset$age_specific)

## Set Min CV -----------------------------------------------------------------

dataset[(standard_error / mean) < 0.1, standard_error := (0.1 * mean)]

## Clean Up -----------------------------------------------------------------

dataset[mean < 0, is_outlier := 1]
dataset[mean == 0 &
          outcome %in%
          c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp", "mild_cog",
            "mod_cog", "mild_rsp", "mod_rsp", "sev_rsp", "sub"),
        is_outlier := 1]

dataset[follow_up_days < 0, is_outlier := 1]

# Russia peds have very low rates, so for respiratory only include the
# shortness of breath rows, and only include follow-up time=1 month
# (ignore the other follow-up times)
# dataset[study_id=="RUS ISARIC peds" & outcome_name=="persistent cough", is_outlier := 1]
# dataset[study_id=="RUS ISARIC peds" & follow_up_value>1, is_outlier := 1]

dataset[(study_id=="PRA"), is_outlier := 0]
table(dataset$study_id, dataset$female)
table(dataset$follow_up_index)
table(dataset$follow_up_days)
table(dataset$study_id, dataset$is_outlier)

sum(is.na(dataset$mean))
dataset <- dataset[!is.na(mean)]

table(dataset$study_id, dataset$outcome)
table(dataset$study_id[dataset$is_outlier == 0], dataset$outcome[dataset$is_outlier == 0])
table(dataset$exclude)

dataset[is.na(exclude), exclude := 0]
table(dataset$exclude, useNA = "always")

dataset[, zero := 0]
dataset[mean == 0, zero := 1]
table(dataset$exclude, dataset$zero)

table(dataset$study_id, dataset$exclude)
table(dataset$outcome, dataset$exclude)
dataset[
  study_id == "Iran ICC",
  exclude := 0
]

dataset[
  study_id == "Italy ISARIC" &
    outcome %in% c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp") &
    age_start == 20,
  exclude := 1
]

# irrelevant age-specific data
# severities cluster (mild, mod, sev) of rsp and cog: where there are no data or no reported cases of rsp or cog
# overlapping cluster: where there are no data or no reported cases of overlaps
# the old "Zurich CC" study
dataset <- dataset[exclude != 1]
dataset[mean >= 1 | mean < 0, is_outlier := 1]

length(unique(dataset$study_id[dataset$is_outlier != 1]))
dim(dataset)



# Remove all Omicron studies
removed_omicron <- dataset[variant == "Omicron"]

# For UK Virus Watch data that have both with and without eq5d data, remove wo_eq5d data
dataset <- dataset[!(NID %in% c(560847, 560848, 560849) & comparison_group == "without eq5d")]
#dataset <- dataset[variant != "Omicron"]

write.csv(dataset, paste0(output_dir, "prepped_data_", datadate, ".csv"))
# write.csv(dataset, paste0(output_dir, "prepped_data_", datadate, "_v2.csv"))

# Save out removed omicron studies
#fwrite(removed_omicron, paste0(output_dir, "prepped_data_omicron_", datadate, ".csv"))
