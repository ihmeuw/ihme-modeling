
##
## Purpose: Calculate etiology-specific EMR from linked datasets. 
##          Linkage and dataset prep done in link_italy_data.R
##
## Calculation/method:
##          EMR = Hazard of death, HF-Etiology - Hazard of death, everyone else
##              = deaths/person-time, HF-Etiology - deaths/person-time, everyone else
##          
##          For "everyone else", person-time starts at birth and ends at death or censorship.
##          For "HF-Etiology", person-time starts at first HF diagnosis and ends at death or censorship.
##
##          Keep in mind that "everyone else" for a particular etiology could actually be a HF case, if that
##          person had a different etiology.
##

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, survival, survminer, ggrepel)


###### Paths, args
#################################################################################

central <- "FILEPATH"

italy_data_path <- 'FILEPATH'

## GBD 2017/2019 etiology list for HF
etiologies <-  c("cvd_ihd", "cvd_rhd", "cvd_htn", "cvd_cmp_myocarditis", "cvd_cmp_other", "cvd_endo", "cvd_other", "resp_copd", "resp_pneum_silico", "resp_pneum_asbest",
                 "resp_pneum_coal", "resp_pneum_other", "resp_interstitial", "hemog_thalass", "hemog_g6pd", "hemog_other", "endo", "cong_heart", "cvd_cmp_alcoholic", "cvd_valvu_other")

code_root<- "FILEPATH"
hf_codes <- c("I50|I11|^428|^402|^425")

save_file <- 'FILEPATH'


###### Functions
#################################################################################

source("data_tests.R")
source("model_helper_functions.R")

## Functions written to deal with multiple diagnosis data
source('CI_cleaning_funcs.R')

source(paste0(central, "get_age_metadata.R"))

###### Read in and prep dataset
#################################################################################

message("Reading in data.")
df <- fread(italy_data_path)

## Make sure columns are appropriate data types. check_class is a function in data_tests
df[date_birth == "", date_birth := "1800-01-01"]
lapply(X = c("sex_id", "age_at_time.y", "age_at_time.x"), FUN = check_class, df = df, class = "numeric")
lapply(X = c("start_date", "end_date", "date_death", "date_birth"), FUN = 
         function(x) {
            df[, paste0(x) := as.Date(get(x))]
          })

## Mark if HF is diagnosed, either in the COD or in inpatient encounters
df[unique(unlist(lapply(df[, c("diag1", "diag2", "diag3", "diag4", "diag5", "diag6"), with=F], grep, pattern=hf_codes))), hf_nonfatal := 1]
df[unique(unlist(lapply(df[, c("ucod", "cod1", "cod2", "cod3", "cod4"), with=F], grep, pattern=hf_codes))), hf_fatal := 1]
lapply(X = c("hf_nonfatal", "hf_fatal"), FUN = function(x) df[is.na(get(x)), paste0(x) := 0])

## Turning ICD codes into YLLs. icd_to_yll in CI_cleaning_funcs.R
df[, icd_type := "ICD9"]
for (col in names(df)[grepl("diag|^cod\\d|^ucod$", names(df))]) {
  print(col)
  df <- icd_to_yll(col, df)
}

## Some non-possible pairs (someone matched to a death record that happens before one of their diagnoses)
df[!(is.na(sid)) & date_death < end_date, impossible := 1]
df[is.na(impossible), impossible := 0]
df[, impossible := max(impossible), by="unique_id"]

message(paste0("We got rid of an additional ", length(unique(df[impossible==1, unique_id])), " pairs because the death was supposedly before an inpatient encounter."))

df <- df[impossible==0,]

## Pull out people who have HF. We'll calculate EMR by the mortality hazard of people with HF - mortality hazard of people without HF. 
df[, ever_hf_nonfatal := max(hf_nonfatal), by="unique_id"]
df[, ever_hf_fatal := max(hf_fatal), by="unique_id"]

message(paste0("Of ", length(unique(df$unique_id)), " people in the dataset, ", length(unique(df[ever_hf_nonfatal==1, unique_id])), 
               " (", round(100*length(unique(df[ever_hf_nonfatal==1, unique_id]))/length(unique(df$unique_id)), 2), "%) had a diagnosis of heart failure at any point."))

keep_df <- copy(df)
#df <- copy(keep_df)

###### Tag patients who have each etiology, tag earliest date of HF
#################################################################################

mark_diagnoses <- function(etiology, df) {
  
  # Mark diagnoses of each etiology for each row (hosp encounter). Look through each diagnosis column, mark "dx_insert-etiology-here" if an etiology was found in that encounter
  # Mark if a person has ever been diagnosed with an etiology in all of their encounters
  
  dx_cols <- names(df)[grepl("^diag", names(df)) & grepl("cause$", names(df))]
  
  df[unique(unlist(lapply(df[, dx_cols, with=F], grep, pattern=paste0(etiology)))), paste0("dx_", etiology) := 1]
  df[is.na(get(paste0("dx_", etiology))), paste0("dx_", etiology) := 0]
  
  df[, paste0("ever_diagnosed_", etiology) := sum(get(paste0("dx_", etiology))), by="unique_id"]
  df[get(paste0("ever_diagnosed_", etiology)) > 1, paste0("ever_diagnosed_", etiology) := 1]
  
  df[get(paste0("ever_diagnosed_", etiology)) == 1, keep := 1]
}

find_earliest <- function(etiology, df) {
  
  ## Find the earliest diagnosis of that etiology.
 
  df[get(paste0("dx_", etiology)) == 1, paste0("earliest_", etiology) := min(start_date), by="unique_id"]
  df[get(paste0("ever_diagnosed_", etiology)) == 1, paste0("earliest_", etiology) := unique(get(paste0("earliest_", etiology))[!(is.na(get(paste0("earliest_", etiology))))]), by="unique_id"]
  
}

message("Tagging etiologies.")
lapply(X = etiologies, FUN = mark_diagnoses, df=df)
lapply(X = etiologies, FUN = find_earliest, df=df)

## Find the earliest diagnosis of heart failure, if it's a HF patient.
df[hf_nonfatal==1, earliest_hf := min(start_date), by="unique_id"]
df[, earliest_hf := unique(earliest_hf[!(is.na(earliest_hf))]), by="unique_id"]

fwrite(df, paste0(save_file, 'all_etiologies.csv'))

###### Calculate person-time and deaths for every etiology.
#################################################################################

ages <- get_age_metadata(age_group_set_id = 12)
ages <- ages[age_group_years_start >= 1, .(age_group_years_start, age_group_years_end)]
ages[, k := 1]

message("Beginning to calculate EMR for each etiology:")

emr <- data.table()
for (etiology in etiologies) {
  
  message(etiology)
  
  hf_df <- copy(df[get(paste0("ever_diagnosed_", etiology)) == 1 & ever_hf_nonfatal == 1,])
  hf_df <- hf_df[(earliest_hf - get(paste0("earliest_", etiology)) >= -30),]
  hf_df_ids <- hf_df[, unique(unique_id)]
  
  no_hf_df <- copy(df[!(unique_id %in% hf_df_ids),])
                
  ############# WORK ON HF ###################
  
  ## Mark last admission, or death
  hf_df[died == 1, end_time := date_death]
  hf_df[died == 0, end_time := max(start_date), by="unique_id"]
  
  ## Mark age at time of each encounter
  hf_df[is.na(age_at_time.x), age_at_time.x := age_at_time.y]
  hf_df[is.na(age_at_time.x), age_at_time.x := age]
  
  ## Mark age at earliest HF
  hf_df[earliest_hf == start_date, age_at_hf := age_at_time.x]
  hf_df[is.na(age_at_hf), age_at_hf := 1000]
  hf_df[, age_at_hf := min(age_at_hf), by="unique_id"]
  
  ## Mark oldest age seen in data
  hf_df[, oldest_age := max(age_at_time.x), by="unique_id"]
  
  ## Strip the dt into ID, youngest age, oldest age. We'll merge this onto age groups.
  stripped_df <- unique(hf_df[, .(unique_id, age_at_hf, oldest_age, died)])
  stripped_df[, k := 1] # Make a key to merge in ages later
  
  ## Merge in age groups
  expanded_df <- merge(stripped_df, ages, allow.cartesian = T)
  expanded_df[, k := NULL]
  for (i in c("age_at_hf", "oldest_age")) expanded_df[, paste0(i) := as.numeric(get(i))]
  
  ## Restrict to age groups where the person has overlapped
  expanded_df[, in_age_group := ifelse((age_group_years_start >= age_at_hf & age_group_years_start <= oldest_age) | (age_group_years_end >= age_at_hf & age_group_years_end <= oldest_age), 1, 0)]
  expanded_df <- expanded_df[in_age_group == 1,]
  
  ## Figure out how many person-years the person contributed to the age group
  expanded_df[age_at_hf <= age_group_years_start & oldest_age >= age_group_years_end, years_contributed := 5]
  expanded_df[age_at_hf >= age_group_years_start & oldest_age >= age_group_years_end, years_contributed := age_group_years_end - age_at_hf]
  expanded_df[age_at_hf <= age_group_years_start & oldest_age <= age_group_years_end, years_contributed := oldest_age - age_group_years_start]
  expanded_df[age_at_hf >= age_group_years_start & oldest_age <= age_group_years_end, years_contributed := oldest_age - age_at_hf]
  
  ## Figure out when the person died in the age group
  expanded_df[died == 1, age_died := max(oldest_age), by = "unique_id"]
  expanded_df[died == 1 & (age_group_years_start <= age_died & age_group_years_end >= age_died), age_group_death := 1]
  expanded_df[is.na(age_group_death), age_group_death := 0]
  
  ## Strip the df to age groups, person-years contributed, and deaths
  calc_df <- expanded_df[, .(age_group_years_start, age_group_years_end, years_contributed, age_group_death)]

  ## Calculate age-specific hazard of dying
  calc_df[, deaths := sum(age_group_death), by=c("age_group_years_start", "age_group_years_end")]
  calc_df[, person_years := sum(years_contributed), by=c("age_group_years_start", "age_group_years_end")]
  calc_df <- unique(calc_df[, .(age_group_years_start, age_group_years_end, deaths, person_years)])
  calc_df[, hazard := deaths/person_years]
  
  ## Save for HF:
  hf_stripped <- copy(calc_df)
  hf_stripped[, demographics := "heart_failure"]
  
  
  ############# WORK ON EVERYONE ELSE ###################
  
  ## Mark last admission, or death
  no_hf_df[died == 1, end_time := date_death]
  no_hf_df[died == 0, end_time := max(start_date), by="unique_id"]
  
  ## Mark age at time of each encounter
  no_hf_df[is.na(age_at_time.x), age_at_time.x := age_at_time.y]
  no_hf_df[is.na(age_at_time.x), age_at_time.x := age]
  
  ## Mark age at death or latest encounter
  no_hf_df[end_time == start_date | end_time == date_death, age_at_last_point := age_at_time.x]
  no_hf_df[is.na(age_at_last_point), age_at_last_point := 1000]
  no_hf_df[, age_at_last_point := min(age_at_last_point), by="unique_id"]
  no_hf_df <- no_hf_df[age_at_last_point < 900,] ## Not sure why but there are ~5 people with ages of 999
  
  ## Mark oldest age seen in data
  no_hf_df[, oldest_age := max(age_at_last_point), by="unique_id"]
  
  ## Strip the dt into ID, youngest age, oldest age. We'll merge this onto age groups.
  stripped_df <- unique(no_hf_df[, .(unique_id, oldest_age, died)])
  stripped_df[, k := 1] # Make a key to merge in ages later
  
  ## Merge in age groups
  expanded_df <- merge(stripped_df, ages, allow.cartesian = T)
  expanded_df[, k := NULL]
  for (i in c("oldest_age")) expanded_df[, paste0(i) := as.numeric(get(i))]
  
  ## Restrict to age groups where the person has overlapped
  expanded_df[, in_age_group := ifelse(oldest_age >= age_group_years_start, 1, 0)]
  expanded_df <- expanded_df[in_age_group == 1,]
  
  ## Figure out how many person-years the person contributed to the age group
  expanded_df[oldest_age >= age_group_years_end, years_contributed := 5]
  expanded_df[oldest_age <= age_group_years_end, years_contributed := oldest_age - age_group_years_start]
  
  ## Figure out when the person died in the age group
  expanded_df[died == 1, age_died := max(oldest_age), by = "unique_id"]
  expanded_df[died == 1 & (age_group_years_start <= age_died & age_group_years_end >= age_died), age_group_death := 1]
  expanded_df[is.na(age_group_death), age_group_death := 0]
  
  ## Strip the df to age groups, person-years contributed, and deaths
  calc_df <- expanded_df[, .(age_group_years_start, age_group_years_end, years_contributed, age_group_death)]
  
  ## Calculate age-specific hazard of dying
  calc_df[, deaths := sum(age_group_death), by=c("age_group_years_start", "age_group_years_end")]
  calc_df[, person_years := sum(years_contributed), by=c("age_group_years_start", "age_group_years_end")]
  calc_df <- unique(calc_df[, .(age_group_years_start, age_group_years_end, deaths, person_years)])
  calc_df[, hazard := deaths/person_years]
  
  ## Save for everyone else
  no_hf_stripped <- copy(calc_df)
  no_hf_stripped[, demographics := "never_heart_failure"]
  
  ################ MERGE EVERYONE #########################
  
  etiology_emr <- rbind(hf_stripped, no_hf_stripped, fill=T)
  etiology_emr[, etiology := paste0(etiology)]
  
  fwrite(etiology_emr, paste0(save_file, etiology, '.csv'))
  
  emr <- rbind(emr, etiology_emr, fill=T)
  
}


## Final adjustments, writing, making PDFs of raw

emr[person_years == 0, hazard := NA]
emr[, dem := ifelse(demographics=="heart_failure", "Diagnosed with HF and etiology", "Never diagnosed with HF and etiology")]
emr <- emr[!(is.na(hazard))]
emr[, small := ifelse(person_years < 500, "< 500", "> 500")]

fwrite(emr, paste0(save_file, "all_etiologies.csv"))

