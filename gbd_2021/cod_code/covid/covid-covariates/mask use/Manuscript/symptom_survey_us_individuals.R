#####################################################################################
## Import Facebook symptom survey individual results in the US, format, keep
## responses at individual level (not aggregated).
## Written by Alice Lazzar-Atwood, modified by Steve Bachmeier and Chris Troeger
#####################################################################################
# source("/home/j/temp/ctroeger/COVID19/Code/symptom_survey_us_individuals.R")
# source("/ihme/code/covid-19/user/ctroeger/covid-beta-inputs/src/covid_beta_inputs/mask_use/Manuscript/symptom_survey_us_individuals.R")

rm(list=ls())

if (Sys.info()["sysname"] %in% c("Linux","Darwin")){
  j <- "FILEPATH"
  h <- paste0("FILEPATH",Sys.info()[7],"/")
  k <- "FILEPATH"
  l <- 'FILEPATH'
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
  l <- 'FILEPATH'
  if (!("pacman" %in% rownames(installed.packages()))){
    install.packages('pacman')
  }
}

# ==============================================================================
# SETUP
# ==============================================================================
pacman::p_load(data.table, spatstat, stringr, dplyr)
dt.shift <- data.table::shift

source("FILEPATH/get_location_metadata.R")

rawdir <- 'FILEPATH'

# Locations
set_info <- fread(paste0(l, 'FILEPATH/fb_best.csv'))
location_set_id <- set_info$value[1]
location_set_version_id <- set_info$value[2]
message(paste0(
  " Location set id: ", location_set_id, "\n",
  " Location set version id: ", location_set_version_id
))

# Load state codes file
state_codes <- read.delim(paste0(rawdir, '/A3b-coding.txt'), header = F, sep = "\t")

# Read in a zipcode-county crosswalk
zip_codes <- fread('https://raw.githubusercontent.com/scpike/us-state-county-zip/master/geo-data.csv')
zip_codes <- zip_codes[, .(state, zipcode, county)]
zip_codes[, state := toupper(state)]

# ==============================================================================
# IMPORT RAW DATA
# ==============================================================================
date_download <- 'latest'
daily_files <- list.files(paste0(rawdir, '/AUTO_EXTRACTED/', date_download, '/'), full.names = T)

# Drop filenames tagged w/ "BAD"
daily_files <- daily_files[!daily_files %like% 'BAD']

# Separate survey data and recordedby date
daily_files <- data.table(daily_files)
setnames(daily_files, old = 'daily_files', new = 'filepath')
daily_files[, recordedby_date := sub('.*recordedby_', '', filepath)]
daily_files[, recordedby_date := sub('.csv', '', recordedby_date)]
daily_files[, date := sub('.*cvid_responses_', '', filepath)]
daily_files[, date := sub('_recordedby_.*', '', date)]

# Keep most recent datasets
daily_files <- daily_files[order(date, recordedby_date)]
daily_files <- daily_files[!duplicated(daily_files[, date], fromLast = T)]
daily_files[, date2 := as.Date(date, '%Y_%m_%d')]
daily_files[, diffdate := date2 - dt.shift(date2)]
if (sum(daily_files$diffdate[2:nrow(daily_files)] != 1) != 0) {stop('There are missing days!')}
daily_files <- list(daily_files$filepath)[[1]]

# only keep daily data files

daily_files <- grep('cvid_responses*',daily_files, value = T)
daily_files <- c(daily_files[daily_files %like% "responses_2021"])

# Read in daily datasets
# Manually read in the problematic columns (C10_x_1) as characters
raw_data_list_inc <- lapply(
  daily_files, 
  fread, colClasses=list(character=c("C10_1_1","C10_2_1","C10_3_1", "C10_4_1", "A2","A2b", "A5_1","A5_3","A4", 'B2b'))) 

# Bind
raw_data_inc <- rbindlist(raw_data_list_inc, use.names = T, fill = T)

rm(raw_data_list_inc)

# ------------------------------------------------------------------------------
# Calculate variable date ranges
raw_data_inc[,start_date := substr(StartDatetime, start = 1, stop = 10)]
raw_data_inc[,start_date := as.Date(start_date)]

variable_dates <- raw_data_inc %>%
  group_by(start_date) %>%
  summarize_all(list(~ sum(!is.na(.)))) %>%
  as.data.table()
variable_dates_l <- melt(variable_dates, id.vars = "start_date")
missing_variable_dates <- dcast(variable_dates_l[value == 0], start_date ~ variable, value.var = 'value')

message(paste0(
  '\n',
  'Survey data date range: ', min(raw_data_inc$start_date), ' - ', max(raw_data_inc$start_date)))
message(paste0(capture.output(missing_variable_dates), collapse = '\n'))
# ------------------------------------------------------------------------------

# Convert class integer64 to integer
# str(raw_data_inc)
names_integer64 <- names(sapply(raw_data_inc, class)[sapply(raw_data_inc, class) == 'integer64'])
raw_data_inc[, (names_integer64) := lapply(.SD, as.integer), .SDcols = names_integer64]

# Convert the problematic string variables to integer
raw_data_inc[, `:=`(
  C10_1_1 = as.integer(C10_1_1), C10_2_1 = as.integer(C10_2_1),
  C10_3_1 = as.integer(C10_3_1), C10_4_1 = as.integer(C10_4_1))]

# Subset to variables of interest
sub <- copy(raw_data_inc)
sub <- sub[,c(
  'start_date', 'EndDatetime', 'weight',
  'A3b', 'A3',
  'D1', 'D1b', 'D2', 'D3', 'D4', 'D5',
  'V1','V3','D8', 'D9',
  'A2b', 'A4',
  'B2', 'B5', 'B6', 'B8', 'B11',
  'C1', 'C3', 'C4','C5','C6','C7', 'C8_1','C8_2','C9',
  'C10_1_1', 'C10_2_1', 'C10_3_1', 'C10_4_1', 'C11', 'C12','C13','C13a',"C14", "C17",
  'E1_1','E1_2',"E1_3","E1_4",
  'Q36')]

sub[,end_date := substr(EndDatetime, start = 1, stop = 10)]

# Clean up
sub[,unique_id := seq.int(nrow(sub))]

# Check for empty weights
if (nrow(sub[is.na(weight)]) != 0) {
  stop('ERROR: there are empty weights')
}

# Merge on states
sub <- merge(sub, state_codes, by.x = 'A3b', by.y = 'V1', all.x=T, all.y=F)
setnames(sub, old = 'V2', new = 'state')
sub[, state := toupper(state)]

# Merge on counties
sub[, A3 := substr(A3, 1, 5)]
sub <- merge(sub, zip_codes[, c('state', 'zipcode', 'county')],
             by.x = c('state', 'A3'), by.y = c('state', 'zipcode'),
             all.x = T, all.y = F)

# Rename, re-order, drop
setnames(sub,
         old = c('start_date', 'A3', 'D1', 'D1b', 'D2'),
         new = c('date', 'zipcode', 'gender', 'pregnant', 'age'))
sub[, `:=`(A3b = NULL, EndDatetime = NULL, end_date = NULL)]
setcolorder(sub, c('unique_id', 'date', 'state', 'zipcode', 'county', 'gender', 'pregnant', 'age', 'weight'))

# Clean up counties
sub[, county := toupper(county)]
sub[!is.na(county) &
      !county %like% ' CENSUS AREA' &
      !county %like% ' BOROUGH' &
      !county %like% ' MUNICIPALITY' &
      !county %like% ' PARISH' &
      !county %like% ' CITY' &
      county != 'DISTRICT OF COLUMBIA',
    county := paste0(county, " COUNTY")]

# Check percent of unmatched zip codes
message(paste0('\n',
               'There are ',
               nrow(sub[is.na(county) & !is.na(zipcode)]),
               ' unmatched zipcode-county locations (',
               round(nrow(sub[is.na(county) & !is.na(zipcode)]) / nrow(sub[!is.na(zipcode)]) * 100, 1), '%)\n'
))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# SURVEY QUESTION NOTES

# A2b How many people are there in your household in total (including yourself)?

# A4 How many additional people in your local community that you know personally are sick (fever, along with at least one other symptom from the above list)?

# B2 In the past 24 hours, have you personally experienced any of the following symptoms? (Select all that apply.)
# 1 - Fever
# 2 - Cough
# 3 - Shortness of breath
# 4 - Difficulty breathing
# 5 - Tiredness or exhaustion
# 6 - Nasal congestion
# 7 - Runny nose
# 8 - Muscle or joint aches
# 9 - Sore throat
# 10 - Persistent pain or pressure in your chest
# 11 - Nausea or vomiting
# 12 - Diarrhea
# 13 - Loss of smell or taste
# 14 - Other (Please specify):
# 15 - None of the above

# B5 Have you been tested for COVID-19 (coronavirus) for your current illness
# (If they select something besides "None of the above" for B2)
# 1 - Yes, I was tested, and received a positive diagnosis for COVID-19
# 2 - Yes, I was tested, but it was negative for COVID-19
# 3 - Yes, I was tested, but have not received the result
# 4 - No, I tried to get tested but could not get a test
# 5 - No, I have not tried to get tested

# C1: Have you ever been told by a health professional that you have:
# 2 Cancer (non-skin)
# 3 Heart disease
# 4 High blood pressure
# 5 Asthma
# 6 COPD or emphysema
# 7 Kidney disease
# 8 Autoimmune
# 9 None of the above
# 10 Type 2 diabetes
# 12 Type 1 diabetes
# 11 Other weakened immune system

# C3 In the past 5 days, have you gone to work outside of your home?
# 1 - Yes
# 2 - No

# C4 In the past 5 days have you worked or volunteered in a hospital, medical office, ambulance service, first responder services, or any other health care settings?
# 1 - Yes
# 2 - No

# C5 In the past 5 days have you worked at or visited a long-term care facility or nursing home?
# 1 - Yes
# 2 - No

# C6 In the past 5 days have you traveled outside of your state
# 1 - Yes
# 2 - No

# C7 - To what extent are you intentionally avoiding contact with other people?
# 1 - All of the time
# 2 - Most of the time; I only leave my home to buy food and other essentials
# 3 - Some of the time; I have reduced the amount of times I am in public spaces, social gatherings, or at work
# 4 - None of the time

# C8_1 - In the past 5 days, how often have you felt anxious
# 1 - None of the time
# 2 - Some of the time
# 3 - Most of the time
# 4 - All of the time

# C8_1 - In the past 5 days, how often have you felt depressed
# 1 - None of the time
# 2 - Some of the time
# 3 - Most of the time
# 4 - All of the time

# C9 - How do you feel about the possibility that you or someone in your immediate family might become seriously ill from COVID-19 (coronavirus disease)?
# 1 - Very worried
# 2 - Somewhat worried
# 3 - Not too worried
# 4 - Not worried at all

# C10 - In the past 24 hours, with how many people have you had direct contact, outside of your household? Your best estimate is fine.
# ["Direct contact" means: a conversation lasting more than 5 minutes with a person who is closer than 6 feet away from you, or physical contact like hand-shaking, hugging, or kissing.]
# 1_1 - Number of people at work
# 2_1 - Number of people shopping for essentials
# 3_1 - Number of people at social gatherings
# 4_1 - Number of people at other places

# C11 In the past 24 hours, have you had a direct contact with anyone who recently tested positive for COVID-19
# 1 - Yes
# 2 - Not to my knowledge

# C12 Was this person a member of your household?
# Asked if C11 is 1 (Yes)
# 1 - Yes
# 2 - No

# C13 During which activities in the past 24 hours? Please select all that apply.
# 1 - Gone to work or school outside the place where you are currently staying
# 2 - Gone to a market, grocery store, or pharmacy
# 3 - Gone to a bar, restaurant, or cafe
# 4 - Spent time with someone who isn't currently staying with you
# 5 - Attended an event with more than 10 people
# 6 - Used public transit
# 8 - None of the above

# C13a During which activities in the past 24 hoursdid you wear a mask? Please select all that apply.
# 1 - Gone to work or school outside the place where you are currently staying
# 2 - Gone to a market, grocery store, or pharmacy
# 3 - Gone to a bar, restaurant, or cafe
# 4 - Spent time with someone who isn't currently staying with you
# 5 - Attended an event with more than 10 people
# 6 - Used public transit
# 7 - None of the above

# C14 In the past 5 days,how often did you wear a mask when in public?
# 1 - All of the time
# 2 - Most of the time
# 3 - Some of the time
# 4 - A little of the time
# 5 - None of the time
# 6 - I have not been in public the past 5 days

# C17 Have you gotten a seasonal flu vaccine since June 2020?
# 1 Yes
# 4 No
# 2 I don't know

# D6 Are you of Hispanic, Latino, or Spanish origin?
# 1 - Yes
# 2 - No

# D7 What is your race?
# 1 - American Indian or Alaskan Native
# 2 - Asian
# 3 - Black or African American
# 4 - Native Hawaiian or Pacific Islander
# 5 - White
# 6 - Other

# D8 What is the highest degree or level of school you have completed?
# 1 - Less than high school
# 2 - High school or GED equivalent
# 3 - Some college
# 4 - 2 year degree
# 5 - 4 year degree
# 6 - Professional degree
# 7 - Doctorate

# E1... Are there children in your household in the following grades?
# E1_1 Pre-kindergarten/kindergarten
# E1_2 Grades 1-5
# E1_3 Grades 6-8
# E1_4 Grades 9-12
# 
# 1 - Yes
# 2 - No
# 5 - Dont know

# Q36 How much of a threat would you say the coronavirus outbreak is to your household’s finances?
# 1 - A substantial threat
# 2 - A moderate threat
# 3 - Not much of a threat
# 4 - Not a threat at all
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ==============================================================================
# PROCESS RESPONSES
# ==============================================================================

# A2b - Number in household

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Calculate outliers
# summary(sub[!is.na(A2b)]$A2b)
# lowerq <- quantile(sub$A2b, na.rm = T)[2]
# upperq <- quantile(sub$A2b, na.rm = T)[4]
# iqr <- upperq - lowerq
# upperth = (iqr * 3) + upperq
# message(paste0('\n Extreme upper threshold: ', upperth, '\n'))
# summary(sub[!is.na(A2b) & A2b <= 10]$A2b)

# NOTES
# * The question asked for the number _including yourself_ and so responses of zero
#   make no sense. Here we force reponses of zero to be 1 instead.
# * The 1st and 3rd quartiles are 2 and 4, respectively, with an extremem outlier
#   calculated to be 3 * (4 - 2) + 4 = 10.
# * ALL RESPONSES GREATHER THAN 12 ARE DROPPED.
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Convert responses of zero to 1
sub[A2b == 0, A2b := 1]

# Drop outliers (A2b > 12)
sub[A2b > 12, A2b := NA]

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# PROCESSING THIS QUESTION IS PAUSED FOR NOW

# NEED TO FIGURE OUT BINS AND PROCESS QUESTION A4 BELOW
# class(sub$A4)
# sub[, A4 := as.integer(A4)]
# unique(sub[!is.na(A4)]$A4)
# min(sub[!is.na(A4)]$A4)
# max(sub[!is.na(A4)]$A4)
# mean(sub[!is.na(A4)]$A4)
# median(sub[!is.na(A4)]$A4)
#
# ggplot(sub[!is.na(A4) & A4 < 20], aes(x = A4)) +
#   # geom_vline(aes(xintercept = mean(A4)), linetype = "dashed", size = 0.6) +
#   geom_histogram(binwidth = 1, color = "black", fill = "gray")
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Process the symptoms as 1 (yes) if any are checked except "None of the above" and 2 (no) if "None of the above" is checked
if (class(sub$B2) != "character") stop("Not character class")
sub[!is.na(B2),B2_yes := ifelse(B2 != '15', 1, 0)]
sub[!is.na(B2),B2_no := ifelse(B2 == '15', 1, 0)]

sub[!is.na(B2),B2_1 :=  ifelse((B2 ==  '1' | B2 %like%  '^1,'), 1, 0)]
sub[!is.na(B2),B2_2 :=  ifelse((B2 ==  '2' | B2 %like%  '^2,'  | B2 %like%  ',2,' | B2 %like%  ',2$'), 1, 0)]
sub[!is.na(B2),B2_3 :=  ifelse((B2 ==  '3' | B2 %like%  '^3,'  | B2 %like%  ',3,' | B2 %like%  ',3$'), 1, 0)]
sub[!is.na(B2),B2_4 :=  ifelse((B2 ==  '4' | B2 %like%  '^4,'  | B2 %like%  ',4,' | B2 %like%  ',4$'), 1, 0)]
sub[!is.na(B2),B2_5 :=  ifelse((B2 ==  '5' | B2 %like%  '^5,'  | B2 %like%  ',5,' | B2 %like%  ',5$'), 1, 0)]
sub[!is.na(B2),B2_6 :=  ifelse((B2 ==  '6' | B2 %like%  '^6,'  | B2 %like%  ',6,' | B2 %like%  ',6$'), 1, 0)]
sub[!is.na(B2),B2_7 :=  ifelse((B2 ==  '7' | B2 %like%  '^7,'  | B2 %like%  ',7,' | B2 %like%  ',7$'), 1, 0)]
sub[!is.na(B2),B2_8 :=  ifelse((B2 ==  '8' | B2 %like%  '^8,'  | B2 %like%  ',8,' | B2 %like%  ',8$'), 1, 0)]
sub[!is.na(B2),B2_9 :=  ifelse((B2 ==  '9' | B2 %like%  '^9,'  | B2 %like%  ',9,' | B2 %like%  ',9$'), 1, 0)]
sub[!is.na(B2),B2_10 := ifelse((B2 == '10' | B2 %like% '^10,'  | B2 %like% ',10,' | B2 %like% ',10$'), 1, 0)]
sub[!is.na(B2),B2_11 := ifelse((B2 == '11' | B2 %like% '^11,'  | B2 %like% ',11,' | B2 %like% ',11$'), 1, 0)]
sub[!is.na(B2),B2_12 := ifelse((B2 == '12' | B2 %like% '^12,'  | B2 %like% ',12,' | B2 %like% ',12$'), 1, 0)]
sub[!is.na(B2),B2_13 := ifelse((B2 == '13' | B2 %like% '^13,'  | B2 %like% ',13,' | B2 %like% ',13$'), 1, 0)]
sub[!is.na(B2),B2_14 := ifelse((B2 == '14' | B2 %like% '^14,'  | B2 %like% ',14,' | B2 %like% ',14$'), 1, 0)]
sub[!is.na(B2),B2_15 := ifelse((B2 == '15' | B2 %like% '^15,'  | B2 %like% ',15,' | B2 %like% ',15$'), 1, 0)]

if (class(sub$B5) != "integer") stop("Not integer class")
sub[!is.na(B5),B5_1 := ifelse(B5 == 1, 1, 0)]
sub[!is.na(B5),B5_2 := ifelse(B5 == 2, 1, 0)]
sub[!is.na(B5),B5_3 := ifelse(B5 == 3, 1, 0)]
sub[!is.na(B5),B5_4 := ifelse(B5 == 4, 1, 0)]
sub[!is.na(B5),B5_5 := ifelse(B5 == 5, 1, 0)]

if (class(sub$C3) != "integer") stop("Not integer class")
sub[!is.na(C3),C3_1 := ifelse(C3 == 1, 1, 0)]
sub[!is.na(C3),C3_2 := ifelse(C3 == 2, 1, 0)]

if (class(sub$C4) != "integer") stop("Not integer class")
sub[!is.na(C4),C4_1 := ifelse(C4 == 1, 1, 0)]
sub[!is.na(C4),C4_2 := ifelse(C4 == 2, 1, 0)]

if (class(sub$C5) != "integer") stop("Not integer class")
sub[!is.na(C5),C5_1 := ifelse(C5 == 1, 1, 0)]
sub[!is.na(C5),C5_2 := ifelse(C5 == 2, 1, 0)]

if (class(sub$C6) != "integer") stop("Not integer class")
sub[!is.na(C6),C6_1 := ifelse(C6 == 1, 1, 0)]
sub[!is.na(C6),C6_2 := ifelse(C6 == 2, 1, 0)]

if (class(sub$C7) != "integer") stop("Not integer class")
sub[!is.na(C7),C7_1 := ifelse(C7 == 1, 1, 0)]
sub[!is.na(C7),C7_2 := ifelse(C7 == 2, 1, 0)]
sub[!is.na(C7),C7_3 := ifelse(C7 == 3, 1, 0)]
sub[!is.na(C7),C7_4 := ifelse(C7 == 4, 1, 0)]

if (class(sub$C8_1) != "integer") stop("Not integer class")
sub[!is.na(C8_1),C8_1_1 := ifelse(C8_1 == 1, 1, 0)]
sub[!is.na(C8_1),C8_1_2 := ifelse(C8_1 == 2, 1, 0)]
sub[!is.na(C8_1),C8_1_3 := ifelse(C8_1 == 3, 1, 0)]
sub[!is.na(C8_1),C8_1_4 := ifelse(C8_1 == 4, 1, 0)]

if (class(sub$C8_2) != "integer") stop("Not integer class")
sub[!is.na(C8_2),C8_2_1 := ifelse(C8_2 == 1, 1, 0)]
sub[!is.na(C8_2),C8_2_2 := ifelse(C8_2 == 2, 1, 0)]
sub[!is.na(C8_2),C8_2_3 := ifelse(C8_2 == 3, 1, 0)]
sub[!is.na(C8_2),C8_2_4 := ifelse(C8_2 == 4, 1, 0)]

if (class(sub$C9) != "integer") stop("Not integer class")
sub[!is.na(C9),C9_1 := ifelse(C9 == 1, 1, 0)]
sub[!is.na(C9),C9_2 := ifelse(C9 == 2, 1, 0)]
sub[!is.na(C9),C9_3 := ifelse(C9 == 3, 1, 0)]
sub[!is.na(C9),C9_4 := ifelse(C9 == 4, 1, 0)]

# Bin the following C10 questions (direct contact) into 0, 1-4, 5-9, 10-19, and 20+
if (class(sub$C10_1_1) != "integer") sub[, C10_1_1 := as.integer(C10_1_1)]
sub[!is.na(C10_1_1),C10_1_zero :=         ifelse(C10_1_1 == 0, 1, 0)]
sub[!is.na(C10_1_1),C10_1_one_to_four :=  ifelse(C10_1_1 >= 1 &  C10_1_1 <= 4, 1, 0)]
sub[!is.na(C10_1_1),C10_1_five_to_nine := ifelse(C10_1_1 >= 5 &  C10_1_1 <= 9, 1, 0)]
sub[!is.na(C10_1_1),C10_1_10_to_19 :=     ifelse(C10_1_1 >= 10 & C10_1_1 <= 19, 1, 0)]
sub[!is.na(C10_1_1),C10_1_20_plus :=      ifelse(C10_1_1 >= 20, 1, 0)]

if (class(sub$C10_2_1) != "integer") sub[, C10_2_1 := as.integer(C10_2_1)]
sub[!is.na(C10_2_1),C10_2_zero :=         ifelse(C10_2_1 == 0, 1, 0)]
sub[!is.na(C10_2_1),C10_2_one_to_four :=  ifelse(C10_2_1 >= 1 &  C10_2_1 <= 4, 1, 0)]
sub[!is.na(C10_2_1),C10_2_five_to_nine := ifelse(C10_2_1 >= 5 &  C10_2_1 <= 9, 1, 0)]
sub[!is.na(C10_2_1),C10_2_10_to_19 :=     ifelse(C10_2_1 >= 10 & C10_2_1 <= 19, 1, 0)]
sub[!is.na(C10_2_1),C10_2_20_plus :=      ifelse(C10_2_1 >= 20, 1, 0)]

if (class(sub$C10_3_1) != "integer") sub[, C10_3_1 := as.integer(C10_3_1)]
sub[!is.na(C10_3_1),C10_3_zero :=         ifelse(C10_3_1 == 0, 1, 0)]
sub[!is.na(C10_3_1),C10_3_one_to_four :=  ifelse(C10_3_1 >= 1 &  C10_3_1 <= 4, 1, 0)]
sub[!is.na(C10_3_1),C10_3_five_to_nine := ifelse(C10_3_1 >= 5 &  C10_3_1 <= 9, 1, 0)]
sub[!is.na(C10_3_1),C10_3_10_to_19 :=     ifelse(C10_3_1 >= 10 & C10_3_1 <= 19, 1, 0)]
sub[!is.na(C10_3_1),C10_3_20_plus :=      ifelse(C10_3_1 >= 20, 1, 0)]

if (class(sub$C10_4_1) != "integer") sub[, C10_4_1 := as.integer(C10_4_1)]
sub[!is.na(C10_4_1),C10_4_zero :=         ifelse(C10_4_1 == 0, 1, 0)]
sub[!is.na(C10_4_1),C10_4_one_to_four :=  ifelse(C10_4_1 >= 1 &  C10_4_1 <= 4, 1, 0)]
sub[!is.na(C10_4_1),C10_4_five_to_nine := ifelse(C10_4_1 >= 5 &  C10_4_1 <= 9, 1, 0)]
sub[!is.na(C10_4_1),C10_4_10_to_19 :=     ifelse(C10_4_1 >= 10 & C10_4_1 <= 19, 1, 0)]
sub[!is.na(C10_4_1),C10_4_20_plus :=      ifelse(C10_4_1 >= 20, 1, 0)]

# Create new C10_5 "all" variable
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# NOTE:
# Question C10 is about direct contacts and there are four options to select:
#   1_1 - Number of people at work
#   2_1 - Number of people shopping for essentials
#   3_1 - Number of people at social gatherings
#   4_1 - Number of people at other places
#
# We calculate the total direct contacts below (C10_5_1). We treat empty (NA)
# responses as zero when creating the total except if all responses are NA, in
# which case C10_5_1 is set to NA as well.
#
# Example:
#   C10_1_1   C10_2_1   C10_3_1   C10_4_1   C10_5
#   1         1         1         1         4
#   10        10        NA        10        3
#   NA        NA        NA        1         1
#   NA        NA        NA        NA        NA
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

sub[, `:=`(
  C10_1_1_zeros = ifelse(is.na(C10_1_1), 0, C10_1_1),
  C10_2_1_zeros = ifelse(is.na(C10_2_1), 0, C10_2_1),
  C10_3_1_zeros = ifelse(is.na(C10_3_1), 0, C10_3_1),
  C10_4_1_zeros = ifelse(is.na(C10_4_1), 0, C10_4_1))]
sub[,C10_5_1 := C10_1_1_zeros + C10_2_1_zeros + C10_3_1_zeros + C10_4_1_zeros]

# Replace all-empty C10 response total as NA
sub[is.na(C10_1_1) & is.na(C10_2_1) & is.na(C10_3_1) & is.na(C10_4_1), C10_5_1 := NA]
# And now bin it
sub[!is.na(C10_5_1),C10_5_zero :=         ifelse(C10_5_1 == 0, 1, 0)]
sub[!is.na(C10_5_1),C10_5_one_to_four :=  ifelse(C10_5_1 >= 1 &  C10_5_1 <= 4, 1, 0)]
sub[!is.na(C10_5_1),C10_5_five_to_nine := ifelse(C10_5_1 >= 5 &  C10_5_1 <= 9, 1, 0)]
sub[!is.na(C10_5_1),C10_5_10_to_19 :=     ifelse(C10_5_1 >= 10 & C10_5_1 <= 19, 1, 0)]
sub[!is.na(C10_5_1),C10_5_20_plus :=      ifelse(C10_5_1 >= 20, 1, 0)]

if (class(sub$C11) != "integer") sub[, C11 := as.integer(C11)]
sub[!is.na(C11),C11_1 := ifelse(C11 == 1, 1, 0)]
sub[!is.na(C11),C11_2 := ifelse(C11 == 2, 1, 0)]

if (class(sub$C12) != "integer") sub[, C12 := as.integer(C12)]
sub[!is.na(C12) & C11 == 1,C12_1 := ifelse(C12 == 1, 1, 0)]
sub[!is.na(C12) & C11 == 1,C12_2 := ifelse(C12 == 2, 1, 0)]

sub[!is.na(C13),C13_1 :=  ifelse((C13 ==  '1' | C13 %like%  '^1,'), 1, 0)]
sub[!is.na(C13),C13_2 :=  ifelse((C13 ==  '2' | C13 %like%  '^2,'  | C13 %like%  ',2,' | C13 %like%  ',2$'), 1, 0)]
sub[!is.na(C13),C13_3 :=  ifelse((C13 ==  '3' | C13 %like%  '^3,'  | C13 %like%  ',3,' | C13 %like%  ',3$'), 1, 0)]
sub[!is.na(C13),C13_4 :=  ifelse((C13 ==  '4' | C13 %like%  '^4,'  | C13 %like%  ',4,' | C13 %like%  ',4$'), 1, 0)]
sub[!is.na(C13),C13_5 :=  ifelse((C13 ==  '5' | C13 %like%  '^5,'  | C13 %like%  ',5,' | C13 %like%  ',5$'), 1, 0)]
sub[!is.na(C13),C13_6 :=  ifelse((C13 ==  '6' | C13 %like%  '^6,'  | C13 %like%  ',6,' | C13 %like%  ',6$'), 1, 0)]
sub[!is.na(C13),C13_7 :=  ifelse((C13 ==  '8' | C13 %like%  '^8,'  | C13 %like%  ',8,' | C13 %like%  ',8$'), 1, 0)]

sub[!is.na(C13a),C13a_1 :=  ifelse((C13a ==  '1' | C13a %like%  '^1,'), 1, 0)]
sub[!is.na(C13a),C13a_2 :=  ifelse((C13a ==  '2' | C13a %like%  '^2,'  | C13a %like%  ',2,' | C13a %like%  ',2$'), 1, 0)]
sub[!is.na(C13a),C13a_3 :=  ifelse((C13a ==  '3' | C13a %like%  '^3,'  | C13a %like%  ',3,' | C13a %like%  ',3$'), 1, 0)]
sub[!is.na(C13a),C13a_4 :=  ifelse((C13a ==  '4' | C13a %like%  '^4,'  | C13a %like%  ',4,' | C13a %like%  ',4$'), 1, 0)]
sub[!is.na(C13a),C13a_5 :=  ifelse((C13a ==  '5' | C13a %like%  '^5,'  | C13a %like%  ',5,' | C13a %like%  ',5$'), 1, 0)]
sub[!is.na(C13a),C13a_6 :=  ifelse((C13a ==  '6' | C13a %like%  '^6,'  | C13a %like%  ',6,' | C13a %like%  ',6$'), 1, 0)]
sub[!is.na(C13a),C13a_7 :=  ifelse((C13a ==  '7' | C13a %like%  '^7,'  | C13a %like%  ',7,' | C13a %like%  ',7$'), 1, 0)]

sub[,C14 := as.integer(C14)]
if (class(sub$C14) != "integer") stop("Not integer class")
sub[!is.na(C14),C14_1 := ifelse(C14 == 1, 1, 0)]
sub[!is.na(C14),C14_2 := ifelse(C14 == 2, 1, 0)]
sub[!is.na(C14),C14_3 := ifelse(C14 == 3, 1, 0)]
sub[!is.na(C14),C14_4 := ifelse(C14 == 4, 1, 0)]
sub[!is.na(C14),C14_5 := ifelse(C14 == 5, 1, 0)]
sub[!is.na(C14),C14_6 := ifelse(C14 == 6, 1, 0)]

# sub[,C13 := as.integer(C13)]
# if (class(sub$C13) != "integer") stop("Not integer class")
# sub[!is.na(C13),C13_1 := ifelse(C13 == 1, 1, 0)]
# sub[!is.na(C13),C13_2 := ifelse(C13 == 2, 1, 0)]
# sub[!is.na(C13),C13_3 := ifelse(C13 == 3, 1, 0)]
# sub[!is.na(C13),C13_4 := ifelse(C13 == 4, 1, 0)]
# sub[!is.na(C13),C13_5 := ifelse(C13 == 5, 1, 0)]
# sub[!is.na(C13),C13_6 := ifelse(C13 == 6, 1, 0)]
# sub[!is.na(C13),C13_7 := ifelse(C13 == 7, 1, 0)]
#
# sub[,C13a := as.integer(C13a)]
# if (class(sub$C13a) != "integer") stop("Not integer class")
# sub[!is.na(C13a),C13a_1 := ifelse(C13a == 1, 1, 0)]
# sub[!is.na(C13a),C13a_2 := ifelse(C13a == 2, 1, 0)]
# sub[!is.na(C13a),C13a_3 := ifelse(C13a == 3, 1, 0)]
# sub[!is.na(C13a),C13a_4 := ifelse(C13a == 4, 1, 0)]
# sub[!is.na(C13a),C13a_5 := ifelse(C13a == 5, 1, 0)]
# sub[!is.na(C13a),C13a_6 := ifelse(C13a == 6, 1, 0)]
# sub[!is.na(C13a),C13a_7 := ifelse(C13a == 8, 1, 0)]

if (class(sub$Q36) != "integer") stop("Not integer class")
sub[!is.na(Q36),Q36_1 := ifelse(Q36 == 1, 1, 0)]
sub[!is.na(Q36),Q36_2 := ifelse(Q36 == 2, 1, 0)]
sub[!is.na(Q36),Q36_3 := ifelse(Q36 == 3, 1, 0)]
sub[!is.na(Q36),Q36_4 := ifelse(Q36 == 4, 1, 0)]

## Race / ethnicity
# sub[!is.na(D6), hispanic := ifelse(D6 == 1, 1, 0)]
# sub[!is.na(D7), race_indigenous_american := ifelse(D7 == 1, 1, 0)]
# sub[!is.na(D7), race_asian := ifelse(D7 == 2, 1, 0)]
# sub[!is.na(D7), race_black := ifelse(D7 == 3, 1, 0)]
# sub[!is.na(D7), race_pacific_islander := ifelse(D7 == 4, 1, 0)]
# sub[!is.na(D7), race_white := ifelse(D7 == 5, 1, 0)]
# sub[!is.na(D7), race_other := ifelse(D7 == 6, 1, 0)]

## Education
sub[!is.na(D8), edu_less_hs := ifelse(D8 == 1, 1, 0)]
sub[!is.na(D8), edu_highschool := ifelse(D8 == 2, 1, 0)]
sub[!is.na(D8), edu_some_college := ifelse(D8 == 3, 1, 0)]
sub[!is.na(D8), edu_associates := ifelse(D8 == 4, 1, 0)]
sub[!is.na(D8), edu_college := ifelse(D8 == 5, 1, 0)]
sub[!is.na(D8), edu_graduate := ifelse(D8 > 5, 1, 0)]

## School kids
sub[!is.na(E1_1), children_kindergarten := ifelse(E1_1 == 1, 1, 0)]
sub[!is.na(E1_2), children_elementary := ifelse(E1_2 == 1, 1, 0)]
sub[!is.na(E1_3), children_middle := ifelse(E1_3 == 1, 1, 0)]
sub[!is.na(E1_4), children_high := ifelse(E1_4 == 1, 1, 0)]

## Health conditions
sub[!is.na(C1), ever_cancer :=  ifelse((C1 ==  '2' | C1 %like%  '^2,'  | C1 %like%  ',2,' | C1 %like%  ',2$'), 1, 0)]
sub[!is.na(C1), ever_chd :=  ifelse((C1 ==  '3' | C1 %like%  '^3,'  | C1 %like%  ',3,' | C1 %like%  ',3$'), 1, 0)]
sub[!is.na(C1), ever_high_bp :=  ifelse((C1 ==  '4' | C1 %like%  '^3,'  | C1 %like%  ',3,' | C1 %like%  ',3$'), 1, 0)]
sub[!is.na(C1), ever_asthma :=  ifelse((C1 ==  '5' | C1 %like%  '^4,'  | C1 %like%  ',4,' | C1 %like%  ',4$'), 1, 0)]
sub[!is.na(C1), ever_copd :=  ifelse((C1 ==  '6' | C1 %like%  '^5,'  | C1 %like%  ',5,' | C1 %like%  ',5$'), 1, 0)]
sub[!is.na(C1), ever_kidney :=  ifelse((C1 ==  '7' | C1 %like%  '^6,'  | C1 %like%  ',6,' | C1 %like%  ',6$'), 1, 0)]
sub[!is.na(C1), ever_diabetes :=  ifelse((C1 %in%  c('10','12') | C1 %like%  '^10,'  | C1 %like%  '^12,'  |
                                            C1 %like%  ',10,' | C1 %like%  ',12,' | C1 %like%  ',10$' | C1 %like%  ',12$'), 1, 0)]

## Flu vaccine
sub[!is.na(C17), flu_vaccine_2020 := ifelse(C17 == 1, 1, 0)]

## Covid vaccine
sub[!is.na(V3), getvaccine_yes := ifelse(V3 == 1, 1, 0)]
sub[!is.na(V3), getvaccine_yesprobably := ifelse(V3 == 2, 1, 0)]
sub[!is.na(V3), getvaccine_noprobably := ifelse(V3 == 3, 1, 0)]
sub[!is.na(V3), getvaccine_no := ifelse(V3 == 4, 1, 0)]

sub[!is.na(V1), received_covid_vaccine := ifelse(V1 == 1, 1, 0)]

## Ever positive test
sub[, ever_positive_test := ifelse(B11 == 1, 1, 0)]
sub[is.na(ever_positive_test), ever_positive_test := 0]

# ==============================================================================
# LOCATIONS
# ==============================================================================
locations <- get_location_metadata(
  location_set_id = location_set_id,
  location_set_version_id = location_set_version_id)
locations_counties <- get_location_metadata(location_set_id = 8)
locations <- rbind(locations, locations_counties)
# Subset locations to USA (102) and Puerto Rico (385)
locations <- locations[
  location_id == 385 |
    parent_id %in% c(385, 102) |
    (level == 5 & path_to_top_parent %like% '1,64,100,102,'),]
locations <- unique(locations[,c('parent_id', 'location_name', 'location_id')])
locations <- merge(locations, locations[, .(location_id, location_name)], by.x = 'parent_id', by.y = 'location_id', all.x = T)
setnames(locations, old=c('location_name.x', 'location_name.y'), new = c('location_name', 'parent_name'))
locations[parent_id == 102, parent_name := 'United States of America']
locations[, `:=`(location_name = toupper(location_name), parent_name = toupper(parent_name))]

sub[, date := format(as.Date(sub$date, format = "%Y-%m-%d"))]

sub <- sub[!is.na(state),]
sub <- sub[state != 'I DO NOT RESIDE IN THE UNITED STATES',]
#sub_locs <- merge(sub, locations, by.x = c('state', 'county'), by.y = c('parent_name', 'location_name'), all.x=T, all.y=F)
sub_locs <- merge(sub, locations[parent_id==102], by.x = 'state', by.y = 'location_name', all.x=T, all.y=F)

sub_locs[, `:=`(state = str_to_title(state))]
#sub_locs[, `:=`(county = str_to_title(county))]

old_names <- c(
  # 'A2b',
  'B2_yes', 'B2_no',
  'B2_1', 'B2_2', 'B2_3', 'B2_4', 'B2_5', 'B2_6', 'B2_7', 'B2_8', 'B2_9',
  'B2_10', 'B2_11', 'B2_12', 'B2_13', 'B2_14', 'B2_15',
  'B5_1', 'B5_2', 'B5_3', 'B5_4', 'B5_5',
  'C3_1', 'C3_2',
  'C4_1', 'C4_2',
  'C5_1', 'C5_2',
  'C6_1', 'C6_2',
  'C7_1', 'C7_2', 'C7_3', 'C7_4',
  'C8_1_1','C8_1_2','C8_1_3','C8_1_4',
  'C8_2_1','C8_2_2','C8_2_3','C8_2_4',
  'C9_1', 'C9_2', 'C9_3', 'C9_4',
  'C10_1', 'C10_2', 'C10_3', 'C10_4', 'C10_5',
  'C11_1', 'C11_2',
  'C12_1', 'C12_2',
  'C13_1', 'C13_2', 'C13_3', 'C13_4', 'C13_5', 'C13_6', 'C13_7',
  'C13a_1', 'C13a_2', 'C13a_3', 'C13a_4', 'C13a_5', 'C13a_6', 'C13a_7',
  'C14_1', 'C14_2', 'C14_3', 'C14_4', 'C14_5', 'C14_6',
  'Q36_1', 'Q36_2', 'Q36_3', 'Q36_4')

new_names <- c(
  # 'mean_household',
  'symptoms_24hrs_yes', 'symptoms_24hrs_no',
  'fever_24hrs', 'cough_24hrs', 'short_breath_24hrs',
  'difficulty_breathing_24hrs', 'tired_24hrs', 'congestion_24hrs',
  'runny_nose_24hrs', 'achy_24hrs', 'sore_throat_24hrs',
  'chest_pain_24hrs', 'nausea_24hrs', 'diarrhea_24hrs',
  'smell_taste_loss_24hrs', 'other_24hrs', 'none_24hrs',
  'symptoms_24hrs_yes_tested_yes_positive', 'symptoms_24hrs_yes_tested_yes_negative',
  'symptoms_24hrs_yes_tested_yes_no_result', 'symptoms_24hrs_yes_tested_no_tried',
  'symptoms_24hrs_yes_tested_no_did_not_try',
  'outside_home_work_yes', 'outside_home_work_no',
  'health_facility_work_yes', 'health_facility_work_no',
  'nursing_home_yes','nursing_home_no',
  'outside_state_yes','outside_state_no',
  'avoid_contact_all', 'avoid_contact_most', 'avoid_contact_some', 'avoid_contact_none',
  'anxious_none', 'anxious_some', 'anxious_most', 'anxious_all',
  'depressed_none', 'depressed_some', 'depressed_most', 'depressed_all',
  'worried_very', 'worried_somewhat', 'worried_not', 'worried_notatall',
  'median_direct_contact_work', 'median_direct_contact_shopping',
  'median_direct_contact_social', 'median_direct_contact_other',
  'median_direct_contact_all',
  'direct_contact_24hrs_yes', 'direct_contact_24hrs_no',
  'direct_contact_24hrs_yes_household_yes', 'direct_contact_24hrs_yes_household_no',
  'yes24hrs_work', 'yes24hrs_grocery', 'yes24hrs_restaurant', 'yes24hrs_otherperson', 'yes24hrs_10pluspeople', 'yes24hrs_publictransit', 'yes24hrs_noneoftheabove',
  'mask24hrs_work', 'mask24hrs_grocery', 'mask24hrs_restaurant', 'mask24hrs_otherperson', 'mask24hrs_10pluspeople', 'mask24hrs_publictransit', 'mask24hrs_noneoftheabove',
  'mask5days_all','mask5days_most','mask5days_some','mask5days_alittle','mask5days_none','mask5days_notbeeninpublic',
  'finance_threat_substantial','finance_threat_moderate', 'finance_threat_little', 'finance_threat_none')

setnames(sub_locs, old = old_names, new = new_names, skip_absent = T)
setnames(sub_locs, c("A2b","A4"), c("n_household","n_known_sick"))
setnames(sub_locs, paste0("C10_1_", c("zero","one_to_four","five_to_nine","10_to_19","20_plus")),
         paste0("contact_work_", c("zero","one_to_four","five_to_nine","10_to_19","20_plus")))
setnames(sub_locs, paste0("C10_2_", c("zero","one_to_four","five_to_nine","10_to_19","20_plus")),
         paste0("contact_essentials_", c("zero","one_to_four","five_to_nine","10_to_19","20_plus")))
setnames(sub_locs, paste0("C10_3_", c("zero","one_to_four","five_to_nine","10_to_19","20_plus")),
         paste0("contact_social_", c("zero","one_to_four","five_to_nine","10_to_19","20_plus")))
setnames(sub_locs, paste0("C10_4_", c("zero","one_to_four","five_to_nine","10_to_19","20_plus")),
         paste0("contact_other_", c("zero","one_to_four","five_to_nine","10_to_19","20_plus")))

## Save the individual results!
sub_locs[, c("county", "unique_id", "zipcode", "D3",
             "D4","D5","B2", paste0("C",3:7), "C8_1", "B11",
             "C8_2","C9","C10_1_1","C10_2_1","C10_3_1","C10_4_1","C11","C12","Q36",
             paste0("C10_5_", c("zero","one_to_four","five_to_nine","10_to_19","20_plus"))) := NULL]

sub_locs[, female := ifelse(gender == 2, 1, 0)]
sub_locs[, age_group := ifelse(age == 1, "18-24",
                               ifelse(age == 2, "24-34",
                                      ifelse(age == 3, "35-44",
                                             ifelse(age == 4, "45-54",
                                                    ifelse(age == 5, "55-64",
                                                           ifelse(age == 6, "65-74", "75+"))))))]

## Race / ethnicity
sub_locs[!is.na(D6), hispanic := ifelse(D6 == 1, 1, 0)]
sub_locs[!is.na(D7), race_indigenous_american := ifelse(D7 == 1, 1, 0)]
sub_locs[!is.na(D7), race_asian := ifelse(D7 == 2, 1, 0)]
sub_locs[!is.na(D7), race_black := ifelse(D7 == 3, 1, 0)]
sub_locs[!is.na(D7), race_pacific_islander := ifelse(D7 == 4, 1, 0)]
sub_locs[!is.na(D7), race_white := ifelse(D7 == 5, 1, 0)]
sub_locs[!is.na(D7), race_other := ifelse(D7 == 6, 1, 0)]

sub_locs[, race := ifelse(D7 == 1, "Indigenous", ifelse(D7 == 2, "Asian",
                                                        ifelse(D7 == 3, "Black", ifelse(D7 == 4, "Pacific Islander",
                                                                                        ifelse(D7 == 5, "White", "Other")))))]

## Education
sub_locs[!is.na(D8), edu_less_hs := ifelse(D8 == 1, 1, 0)]
sub_locs[!is.na(D8), edu_highschool := ifelse(D8 == 2, 1, 0)]
sub_locs[!is.na(D8), edu_some_college := ifelse(D8 == 3, 1, 0)]
sub_locs[!is.na(D8), edu_associates := ifelse(D8 == 4, 1, 0)]
sub_locs[!is.na(D8), edu_college := ifelse(D8 == 5, 1, 0)]
sub_locs[!is.na(D8), edu_graduate := ifelse(D8 > 5, 1, 0)]

sub_locs[, education := ifelse(D8 == 1, "Less HS", ifelse(D8 == 2, "High school", ifelse(D8 < 5, "Some college",
                                                                                         ifelse(D8 == 5, "College", "Graduate"))))]

# Make US feelings more easily aggregated
sub_locs[, nervous := ifelse(anxious_all == 1, "All", ifelse(anxious_most == 1, "Most", ifelse(anxious_some == 1, "Some", ifelse(anxious_none==1,"None",""))))]
sub_locs[, worried := ifelse(worried_very == 1, "All", ifelse(worried_somewhat == 1, "Most", ifelse(worried_not == 1, "Some", ifelse(worried_notatall==1,"None",""))))]
sub_locs[, depressed := ifelse(depressed_all == 1, "All", ifelse(depressed_most == 1, "Most", ifelse(depressed_some == 1, "Some", ifelse(depressed_none==1,"None",""))))]
sub_locs[, finances := ifelse(finance_threat_substantial == 1, "Substantial", ifelse(finance_threat_moderate == 1, "Moderate", ifelse(finance_threat_little==1,"Little","None")))]

#write.csv(sub_locs, "/home/j/Project/covid/data_intake/symptom_survey/us/summary_individual_responses_us.csv", row.names=F)
fwrite(sub_locs, "FILEPATH/summary_individual_responses_us.gz", compress = "auto")

fwrite(sub_locs[!is.na(getvaccine_yes)], "FILEPATH/vaccines_individual_us.csv")

#####################################################################################
## Create another file for mask use during activities
sub_locs <- fread("FILEPATH/summary_individual_responses_us.gz")
sub_locs[, N := 1]
dt <- sub_locs[, lapply(.SD, function(x) sum(x, na.rm=T)), by=c("location_id","state","date"),
              .SDcols=c('yes24hrs_work', 'yes24hrs_grocery', 'yes24hrs_restaurant', 'yes24hrs_otherperson', 'yes24hrs_10pluspeople', 'yes24hrs_publictransit', 'yes24hrs_noneoftheabove',
                        'mask24hrs_work', 'mask24hrs_grocery', 'mask24hrs_restaurant', 'mask24hrs_otherperson', 'mask24hrs_10pluspeople', 'mask24hrs_publictransit', 'mask24hrs_noneoftheabove','N')]

dt <- dt[date >= "2020-09-08"]
dt <- dt[order(state, date)]
dt[, mask_work := mask24hrs_work / yes24hrs_work]
dt[, mask_grocery := mask24hrs_grocery / yes24hrs_grocery]
dt[, mask_restaurant := mask24hrs_restaurant / yes24hrs_restaurant]
dt[, mask_publictransit := mask24hrs_publictransit / yes24hrs_publictransit]
dt[, mask_10pluspeople := mask24hrs_10pluspeople / yes24hrs_10pluspeople]
dt[, mask_otherperson := mask24hrs_otherperson / yes24hrs_otherperson]

dt[, prop_work := yes24hrs_work / N]
dt[, prop_grocery := yes24hrs_grocery / N]
dt[, prop_restaurant := yes24hrs_restaurant / N]
dt[, prop_publictransit := yes24hrs_publictransit / N]
dt[, prop_10pluspeople := yes24hrs_10pluspeople / N]
dt[, prop_otherperson := yes24hrs_otherperson / N]

fwrite(dt, "FILEPATH/facebook_24hrs_mask_activities_usa.csv")
