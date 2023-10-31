#####################################################################################
## Import Facebook symptom survey individual global results, format, keep
## responses at individual level (not aggregated).
## Written by Alice Lazzar-Atwood, modified by Steve Bachmeier and Chris Troeger
## source(paste0("/ihme/code/covid-19/user/", Sys.info()['user'],"/covid-beta-inputs/src/covid_beta_inputs/mask_use/Manuscript/symptom_survey_global_individuals.R"))
#####################################################################################
rm(list=ls())

library(data.table)
setDTthreads(40)
# library(spatstat)
source("FILEPATH/get_location_metadata.R")

# get locations

# read in locs - using a 'merge_name' field, which is still the ISO code for countries, but the country_admin1 for admin1s
gbd_locs <- get_location_metadata(location_set_id = 111, location_set_version_id = 680)
gbd_locs <- gbd_locs[, .(location_id, location_name, ihme_loc_id, parent_id, level)]
parent_locs <- gbd_locs[,c('location_name', 'location_id')]
setnames(parent_locs, old = c('location_name'), new = c('parent_name'))
gbd_locs <- merge(gbd_locs, parent_locs, by.x = 'parent_id', by.y = 'location_id', all.x=T, all.y=F)
#gbd_locs[, gbd := 1]
#gbd_locs <- unique(gbd_locs)
gbd_locs[level==0,merge_test := ihme_loc_id]
gbd_locs[level==1,merge_test := paste0(parent_name, '_', location_name)]

# read in a file that has a bunch of manual matches when the names didn't match
x <- fread('FILEPATH/merge_names.csv')
x <- x[,c('merge_name', 'location_id')]
y <- gbd_locs[,c('location_id', 'location_name', 'ihme_loc_id', 'merge_test')]
x <- merge(x, y, by = 'location_id', all.x=T, all.y=F)
locations <- copy(x)
locations[nchar(merge_test) != 3, merge_test := merge_name]
locations[,merge_name := NULL]
setnames(locations, old = 'merge_test', new = 'merge_name')

# Bind all files together
daily_files <- list.files(
  path = 'FILEPATH',
  full.names = T)
daily_files <- daily_files[!daily_files %like% 'CODEBOOK|iso_country_region_response_map|parta']
raw_data_list <- lapply(daily_files, fread)
raw_data <- rbindlist(raw_data_list, fill = T)

rm(raw_data_list)

# Quick checks
dt <- copy(raw_data)
rm(raw_data)

# New values
# Date
dt[, date1 := as.Date(substr(dt$RecordedDate, 1, 10))]
dt[, date2 := as.Date(substr(dt$StartDate, 1, 10))]

dt[,date := date1]
dt[is.na(date1), date := date2]

dt[,date1 := NULL]
dt[,date2 := NULL]

# ------------------------------------------------------------------------------
# Quick checks
names(dt)[!names(dt) %in% names(fread(daily_files[[1]]))]
names(dt)

# Look for incomplete surveys
names(dt)
unique(dt$Finished)
sum(dt$Finished) / nrow(dt)
nrow(dt[Finished == 0]) / nrow(dt)

# Look for missing countries
nrow(dt[ISO_3 == "" | is.na(ISO_3) | ISO_3 == "NA"])
nrow(dt[country_agg == "" | is.na(country_agg) | country_agg == "NA"])
nrow(dt[region_agg == "" | is.na(region_agg) | region_agg == "NA"])

# Edit UK subnationals so they represent countries instead of admin 1s

dt[country_agg == 'United Kingdom' & NAME_1 == 'England', region_agg := 'England']
dt[country_agg == 'United Kingdom' & NAME_1 == 'Scotland', region_agg := 'Scotland']
dt[country_agg == 'United Kingdom' & NAME_1 == 'Northern Ireland', region_agg := 'Northern Ireland']
dt[country_agg == 'United Kingdom' & NAME_1 == 'Wales', region_agg := 'Wales']
dt[country_agg == 'United Kingdom' & (NAME_1 == ""|is.na(NAME_1)), region_agg := ""]

dt <- dt[!is.na(date),]

# ------------------------------------------------------------------------------
# Helpers
# Re-order
names_desc <- c('ISO_3', 'country_agg', 'region_agg', 'date', 'weight', 'survey_region', 'survey_version',
                'Finished', 'RecordedDate',
                'GID_0', 'GID_1', 'NAME_0', 'NAME_1',
                'country_region_numeric', 'urbanicity', 'gender', 'age_group', 'contacts_24h', 'income')
#'work', 'public_event', 'anxious', 'nervous', 'worried'

#-------------------------------------------------------------------------------
# Make more informative factors for tabulation
dt[, urbanicity := ifelse(E2 == 1, "city", ifelse(E2 == 2, "town", "village/rural"))]
dt[, gender := ifelse(E3 == 1, "male", ifelse(E3 == 2, "female", ifelse(E3 == 3, "self-describe", "no answer")))]
dt[, age_group := ifelse(E4 == 1, "18-24", ifelse(E4 == 2, "25-34",
                                                  ifelse(E4 == 3, "35-44", ifelse(E4 == 4, "45-54",
                                                                                  ifelse(E4 == 5, "55-64", ifelse(E4 == 6, "65-74","> 75"))))))]
# ==============================================================================
# DIRECT CONTACT
# C2	How many people, who are not staying with you, have you had this kind of direct contact with in the last 24 hours?
# "1=1-4 people
# 2=5-9 people
# 3=10-19 people
# 4=20 or more people
# C1_m : If YES, In the last 24 hours, have you had direct contact with anyone who is not
# staying with you? Direct contact means spending longer than one minute within
# two meters of someone or touching, including shaking hands, hugging, or kissing.
# "1=Yes 2=No"
# ==============================================================================
dt[, no_direct_24hrs := ifelse(C1_m == 2, 1, 0)]
dt[, contacts_24h := ifelse(C2 == 1, "1-4", ifelse(C2 == 2, "5-9", ifelse(C2 == 3, "10-19",">20")))]
# ==============================================================================
# C6	In the last 7 days, how many days have you spent time with people who aren’t staying with you?
# 1=0days
# 2=1 day
# 3= 2-4 days
# 4= 5-7 days
# ==============================================================================
dt[, no_contacts_7days := ifelse(C6 == 1, 1, 0)]
dt[, days_contacts := ifelse(C6 == 1, "0", ifelse(C6 == 2, "1", ifelse(C6 == 3, "2-4", "5-7")))]

dt[, worried_income := ifelse(D5 == 1, "Very worried", ifelse(D5 == 2, "Somewhat worried", ifelse(D5 == 3,"Not very","Not worried")))]

# For mask use, which one is better?
  dt[, mask_use := ifelse(C5 == 1, "All", ifelse(C5 == 2, "Most", ifelse(C5 == 3, "Half",ifelse(C5 == 4, "Some",ifelse(C5 == 5, "None","Not in public")))))]

  # For renaming
  old_names <- c('C5_1', 'C5_2', 'C5_3', 'C5_4', 'C5_5', 'C5_6')
  new_names <- c('mask_7days_all', 'mask_7days_most', 'mask_7days_half', 'mask_7days_some', 'mask_7days_none', 'mask_7days_no_public')

  dt[, `:=`(
    C5_1 = ifelse(C5 == 1, 1, 0), C5_2 = ifelse(C5 == 2, 1, 0), C5_3 = ifelse(C5 == 3, 1, 0),
    C5_4 = ifelse(C5 == 4, 1, 0), C5_5 = ifelse(C5 == 5, 1, 0), C5_6 = ifelse(C5 == 6, 1, 0))]
  setnames(dt, old_names, new_names)

# Nervous, depressed
# During the last 7 days, how often did you feel so nervous that nothing could calm you down?
# During the last 7 days, how often did you feel so depressed that nothing could cheer you up?
# How worried are you that you or someone in your immediate family might become seriously ill from coronavirus (COVID-19)?
dt[, nervous := ifelse(D1 == 1, "All", ifelse(D1 == 2, "Most", ifelse(D1 == 3, "Some", ifelse(D1 == 4, "Little","None"))))]
dt[, depressed := ifelse(D2 == 1, "All", ifelse(D2 == 2, "Most", ifelse(D2 == 3, "Some", ifelse(D2 == 4, "Little","None"))))]
dt[, worried := ifelse(D3 == 1, "All", ifelse(D3 == 2, "Most", ifelse(D3 == 3, "Some", ifelse(D3 == 4, "Little","None"))))]

# Movement outside home
# For renaming
old_names <- c('C0_1_1','C0_1_2', 'C0_2_1','C0_2_2', 'C0_3_1','C0_3_2','C0_4_1', 'C0_4_2', 'C0_5_1','C0_5_2', 'C0_6_1', 'C0_6_2')
new_names <- c('yes_24hrs_work', 'no_24hrs_work', 'yes_24hrs_grocery', 'no_24hrs_grocery','yes_24hrs_restaurant', 'no_24hrs_restaurant',
               'yes_24hrs_outsideperson', 'no_24hrs_outsideperson','yes_24hrs_publicevent', 'no_24hrs_publicevent','yes_24hrs_publictransit', 'no_24hrs_publictransit')

# Subset
dt[, `:=`(C0_1_1 = ifelse(C0_1 == 1, 1, 0),
              C0_1_2 = ifelse(C0_1 == 2, 1, 0),
              C0_2_1 = ifelse(C0_2 == 1, 1, 0),
              C0_2_2 = ifelse(C0_2 == 2, 1, 0),
              C0_3_1 = ifelse(C0_3 == 1, 1, 0),
              C0_3_2 = ifelse(C0_3 == 2, 1, 0),
              C0_4_1 = ifelse(C0_4 == 1, 1, 0),
              C0_4_2 = ifelse(C0_4 == 2, 1, 0),
              C0_5_1 = ifelse(C0_5 == 1, 1, 0),
              C0_5_2 = ifelse(C0_5 == 2, 1, 0),
              C0_6_1 = ifelse(C0_6 == 1, 1, 0),
              C0_6_2 = ifelse(C0_6 == 2, 1, 0))]
setnames(dt, old_names, new_names)

# Tested for COVID-19
dt[, ever_tested := ifelse(B6 == 1, 1, 0)]
dt[, tested_2weeks := ifelse(B7 == 1, 1, 0)]
dt[, test_positive := ifelse(B8 == 1, 1, 0)]

# Symptoms
old_names <- c('B1_1_1', 'B1_1_2',
               'B1_2_1', 'B1_2_2',
               'B1_3_1', 'B1_3_2',
               'B1_4_1', 'B1_4_2',
               'B1_5_1', 'B1_5_2',
               'B1_6_1', 'B1_6_2',
               'B1_7_1', 'B1_7_2',
               'B1_8_1', 'B1_8_2',
               'B1_9_1', 'B1_9_2',
               'B1_10_1', 'B1_10_2',
               'B1_11_1', 'B1_11_2',
               'B1_12_1', 'B1_12_2',
               'B1_13_1', 'B1_13_2',
               'B1_14_1', 'B1_14_2'
)
new_names <- c('fever_yes', 'fever_no',
               'cough_yes', 'cough_no',
               'diffbreathing_yes', 'diffbreathing_no',
               'fatigue_yes', 'fatigue_no',
               'runnynose_yes', 'runnynose_no',
               'aches_yes', 'aches_no',
               'sorethroat_yes', 'sorethroat_no',
               'chestpain_yes', 'chestpain_no',
               'nausea_yes', 'nausea_no',
               'nosmelltaste_yes', 'nosmelltaste_no',
               'eyepain_yes', 'eyepain_no',
               'headache_yes', 'headache_no',
               'chills_yes', 'chills_no',
               'anysymptoms_yes', 'anysymptoms_no'
)

dt[, `:=`(B1_1_1 = ifelse(B1_1 == 1, 1, 0),
              B1_1_2 = ifelse(B1_1 == 2, 1, 0),
              B1_2_1 = ifelse(B1_2 == 1, 1, 0),
              B1_2_2 = ifelse(B1_2 == 2, 1, 0),
              B1_3_1 = ifelse(B1_3 == 1, 1, 0),
              B1_3_2 = ifelse(B1_3 == 2, 1, 0),
              B1_4_1 = ifelse(B1_4 == 1, 1, 0),
              B1_4_2 = ifelse(B1_4 == 2, 1, 0),
              B1_5_1 = ifelse(B1_5 == 1, 1, 0),
              B1_5_2 = ifelse(B1_5 == 2, 1, 0),
              B1_6_1 = ifelse(B1_6 == 1, 1, 0),
              B1_6_2 = ifelse(B1_6 == 2, 1, 0),
              B1_7_1 = ifelse(B1_7 == 1, 1, 0),
              B1_7_2 = ifelse(B1_7 == 2, 1, 0),
              B1_8_1 = ifelse(B1_8 == 1, 1, 0),
              B1_8_2 = ifelse(B1_8 == 2, 1, 0),
              B1_9_1 = ifelse(B1_9 == 1, 1, 0),
              B1_9_2 = ifelse(B1_9 == 2, 1, 0),
              B1_10_1 = ifelse(B1_10 == 1, 1, 0),
              B1_10_2 = ifelse(B1_10 == 2, 1, 0),
              B1_11_1 = ifelse(B1_11 == 1, 1, 0),
              B1_11_2 = ifelse(B1_11 == 2, 1, 0),
              B1_12_1 = ifelse(B1_12 == 1, 1, 0),
              B1_12_2 = ifelse(B1_12 == 2, 1, 0),
              B1_13_1 = ifelse(B1_13 == 1, 1, 0),
              B1_13_2 = ifelse(B1_13 == 2, 1, 0),
              B1_14_1 = ifelse(B1_1 == 1 | B1_2 == 1 | B1_3 == 1 |
                                 B1_4 == 1 | B1_5 == 1 | B1_6 == 1 |
                                 B1_7 == 1 | B1_8 == 1 | B1_9 == 1 |
                                 B1_10 == 1 | B1_11 == 1 | B1_12 == 1 |
                                 B1_13 == 1, 1, 0),
              B1_14_2 = ifelse(B1_1 == 2 & B1_2 == 2 & B1_3 == 2 &
                                 B1_4 == 2 & B1_5 == 2 & B1_6 == 2 &
                                 B1_7 == 2 & B1_8 == 2 & B1_9 == 2 &
                                 B1_10 == 2 & B1_11 == 2 & B1_12 == 2 &
                                 B1_13 == 2, 1, 0))]
setnames(dt, old_names, new_names)
setnames(dt, "B2", "days_symptoms")

## Vaccine
# If a vaccine to prevent COVID-19 (coronavirus) were offered to you today, would you choose to get vaccinated?
# Yes definitely, yes probably, no probably, no definitely
dt[, vaccine_yes := ifelse(V3 == 1, 1,0)]
dt[, vaccine_probably := ifelse(V3 == 2, 1,0)]
dt[, vaccine_unlikely := ifelse(V3 == 3, 1,0)]
dt[, vaccine_no := ifelse(V3 == 4, 1,0)]

## Would you be more or less likely to receive the vaccine if recommended by:
setnames(dt, c("V4_1","V4_2","V4_3","V4_4","V4_5"), c("vaccine_friends","vaccine_healthworker","vaccine_who",
                                                      "vaccine_government","vaccine_politicians"))
dt[, vaccine_friends := ifelse(vaccine_friends == 1, "More", ifelse(vaccine_friends == 2, "Same", "Less"))]
dt[, vaccine_healthworker := ifelse(vaccine_healthworker == 1, "More", ifelse(vaccine_healthworker == 2, "Same", "Less"))]
dt[, vaccine_who := ifelse(vaccine_who == 1, "More", ifelse(vaccine_who == 2, "Same", "Less"))]
dt[, vaccine_government := ifelse(vaccine_government == 1, "More", ifelse(vaccine_government == 2, "Same", "Less"))]
dt[, vaccine_politicians := ifelse(vaccine_politicians == 1, "More", ifelse(vaccine_politicians == 2, "Same", "Less"))]

# Do the following reasons describe why you haven't been tested?
dt[, no_test_reason := ifelse(B12_1 == 1, "Unable",
                              ifelse(B12_2 == 1, "Unsure location",
                              ifelse(B12_3 == 1, "Can't afford",
                                     ifelse(B12_4 == 1, "No time",
                                            ifelse(B12_5 == 1, "Unable to travel",
                                                   ifelse(B12_6 == 1, "Worried", NA))))))]

dt[, flu_vaccine_2020 := ifelse(C9 == 1, "Yes", ifelse(C9 == 2, "No", "Unsure"))]
dt[, flu_vaccine_2019 := ifelse(C12 == 1, "Yes", ifelse(C12 == 2, "No", "Unsure"))]
dt[, intend_flu_vaccine := ifelse(C9 == 1, "Already received", ifelse(C10 == 1, "Yes", ifelse(C10 == 2, "No", "Unsure")))]

# E6 - years of education
dt[, education_years := E6]

# dt[, reason_no_flu_vaccine_cost := ifelse(C11_no_1 == 1, 1, 0)]
# dt[, reason_no_flu_vaccine_inconvenient := ifelse(C11_no_2 == 1, 1, 0)]
# dt[, reason_no_flu_vaccine_location := ifelse(C11_no_3 == 1, 1, 0)]
# dt[, reason_no_flu_vaccine_not_important := ifelse(C11_no_4 == 1, 1, 0)]
# dt[, reason_no_flu_vaccine_other := ifelse(C11_no_5 == 1, 1, 0)]

# Worried about enough to eat
dt[, enough_food := ifelse(D4 == 1, "Very worried", ifelse(D4 == 2, "Somewhat worried", ifelse(D4 == 3, "Not very", "Not worried")))]
#How worried are you about your household’s finances in the next month?
dt[, finances := ifelse(D5 == 1, "Very worried", ifelse(D5 == 2, "Somewhat worried", ifelse(D5 == 3, "Not very", "Not worried")))]

##--------------------------------------------------------------------------------------
# Merge with location info, save only columns of interest
dt[,merge_name := ISO_3][,region_agg := ""]
dt_locs <- merge(dt, locations, by.x = 'merge_name', by.y = 'merge_name', all.x=T, all.y=F)

# Great, there are a lot of superfluous columns now
dt_locs[, c(paste0("B1_",1:12),paste0("B",3:8),
            paste0("C0_",1:6),paste0("C",2:6),
            "D1","D2","D3","D4","E2","E3","E4","E5",
            "A1","A2","A2_2_1","A2_2_2",
            "B1_13",paste0("B1b_x",1:13), "B9","B10","B11",
            paste0("B12_",1:6), paste0("B13_",1:7),
            paste0("B14_",1:5),"C7","C8","D5","D6_2","D6_3","D7",
            "D8","D9","D10","E6","E7","F1","F2_1","F2_2","F3_au","F3_de", #"StartDate","RecordedDate",
            "intro1","intro2","C1_m","Q_TotalDuration","GID_0","GID_1","merge_name","survey_region","survey_version",
            "Finished","D6_1","ISO_3","NAME_0","NAME_1","country_agg","country_region_numeric","region_agg") := NULL]
head(dt_locs)

#write.csv(dt_locs, "/home/j/Project/covid/data_intake/symptom_survey/global/summary_individual_responses_global.csv", row.names=F)
fwrite(dt_locs, "FILEPATH/summary_individual_responses_global.gz", compress="auto")

##-----------------------------------------------------------------------------------
# Save vaccine responses
vdt <- dt_locs[, c("location_id","location_name","date","age_group","education_years","ever_tested","mask_7days_all",
                    "yes_24hrs_work","vaccine_yes","vaccine_probably","vaccine_unlikely","vaccine_no",
                    "no_test_reason","flu_vaccine_2020","flu_vaccine_2019","intend_flu_vaccine",
                    "enough_food","finances","gender","urbanicity",
                    "vaccine_friends","vaccine_healthworker","vaccine_who","vaccine_government",
                    "vaccine_politicians","vaccine","weight")]
vdt <- vdt[!is.na(vaccine_yes)]

fwrite(vdt, "FILEPATH/vaccine_individual_responses_global.gz", compress="auto")

