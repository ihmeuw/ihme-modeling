
##
## Purpose: Get the MCOD proportion from Brazil, USA, Taiwan, Mexico, and Colombia. 
##          This is the proportion of deaths from a given etiology that have heart failure associated.
##


date<-gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, doBy)

###### Paths, args
############################################################################################################

## Central functions
central <- "CENTRAL"

## These are paths to year-specific cleaned data
paths <- data.table(
  usa = paste0("FILEPATH"),
  twn = paste0("FILEPATH"),
  bra = paste0("FILEPATH"),
  col = paste0("FILEPATH"),
  mex = paste0("FILEPATH")
)

## File name roots in the above folders
names <- data.table(
  usa = "NAME",
  bra = "NAME",
  twn = "NAME",
  col = "NAME",
  mex = "NAME"
)

write_path <- 'FILEPATH'

## GBD 2017/2019 etiology list for HF
etiologies <-  c("cvd_ihd", "cvd_rhd", "cvd_htn", "cvd_cmp_myocarditis", "cvd_cmp_other", "cvd_endo", "cvd_other",
                 "resp_copd", "resp_pneum_silico", "resp_pneum_asbest", "resp_pneum_coal", "resp_pneum_other",
                 "resp_interstitial", "hemog_thalass", "hemog_g6pd", "hemog_other", "endo_other", "cong_heart",
                 "cvd_cmp_alcoholic", "cvd_valvu_other")
hund_attribution <- c("cvd_htn", "cvd_cmp_other", "cvd_cmp_alcoholic")

## ICD codes for HF 
hf_codes <- "I50|I11|^428|^402|^425" # HF, HHD, cardiomyopathies

gbd_round_id <- 6

###### Functions
############################################################################################################

## Age metadata
source(paste0(central, "get_age_metadata.R"))
age_groups <- get_age_metadata(age_group_set_id = 12, gbd_round_id = gbd_round_id)

## Functions written to deal with multiple diagnosis data
source('CI_cleaning_funcs.R')

source('convenient_funcs.R')

## For Colombia data - not fully prepped by the MCOD team
age_dt <- data.table(
  old_age = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26"),
  new_age = c("0",    # Under 1 day
              "0",    # 1-6 days
              "0.01917808",     # 7-27 days
              "0.07671233",     # 28-29 days
              "0.07671233",     # 1-5 months
              "0.07671233",     # 6-11 months
              "1",            # 1 year
              "1",            # 2-4 years
              "5",            # 5-9 years
              "10",          # 10-14 years
              "15",          # 15-19 years
              "20",          # 20-24 years
              "25",          # 25-29 years
              "30",          # 30-34 years
              "35",          # Listed as 35-99 years
              "40",          # 40-44 years
              "45",          # 45-49 years
              "50",          # 50-54 years
              "55",          # 55-59 years
              "60",          # 60-64 years
              "65",          # 65-69 years
              "70",          # 70-74 years
              "75",          # 75-79 years
              "80",          # 80-84 years
              "85")            # 85+
)
age_dt[, old_age := as.numeric(old_age)]
age_dt[, new_age := as.numeric(new_age)]


###### Read in data, and count the number of deaths that had HF associated. 
###### For USA, Mex, Bra, save by state.
############################################################################################################

country <- "usa"
usa_dt <- data.table()

for (year in 1992:2016) {

  print(year)

  ## Pull in data
  data <- fread(paste0(paths[,get(country)], names[,get(country)], year, "_cleaned.csv"))

  ## Make sure diagnosis codes ("multiple_cause_1", "multiple_cause_2", etc.) are characters
  dx_cols <- c(names(data)[grepl("multiple_cause", names(data))], "cause")
  lapply(X = dx_cols, FUN = check_class, df=data, class="character")

  ## Noting when someone has a HF code anywhere in their chain
  data[unique(unlist(lapply(data[, dx_cols, with=F], grep, pattern=hf_codes))), hf_chain := 1]
  data[is.na(hf_chain), hf_chain := 0]

  ## Cleaning of diagnosis columns:
  ##  - remove the ICD dot ("I50.0" to "I500") for consistency with the map
  ##  - turning ICD codes into yll causes ("I50" to "garbage_code")
  data <- clean_cols(data)
  lapply(X = dx_cols, FUN = remove_ICD_dot, df=data)
  data <- icd_to_yll("cause", data)

  ## Restrict to the causes we care about
  data <- data[cause_yll_cause %in% etiologies,]
  data <- data[cause_yll_cause %in% hund_attribution, hf_chain := 1]

  ## Count and append
  data[, sum_deaths := sum(deaths), by=c("cause_yll_cause", "age", "sex", "state_occ_alpha")]
  data[, sum_hf_deaths := sum(hf_chain), by=c("cause_yll_cause", "age", "sex", "state_occ_alpha")]

  data <- data[, .(sum_deaths, sum_hf_deaths, cause_yll_cause, cause_yll_cause_name, age, sex, year, state_occ_alpha)]
  data <- unique(data)

  data[age == "0d", age := as.character(0)]
  data[age == "7d", age := as.character(0.01917808)]
  data[age == "28d", age := as.character(0.07671233)]
  data$age <- as.numeric(data$age)
  
  ## Pull in age group IDs
  data <- merge(data, age_groups[, .(age_group_id, age_group_years_start)], by.x="age", by.y="age_group_years_start")
  setnames(data, old=c("sex", "year"), new=c("sex_id", "year_id"))
  
  usa_dt <- rbind(usa_dt, data, fill=T)

}

fwrite(usa_dt, file = paste0("FILEPATH"))

country <- "twn"
twn_dt <- data.table()

for (year in 2008:2016) {

  print(year)

  ## Pull in data
  data <- as.data.table(readxl::read_excel(paste0(paths[,get(country)], names[,get(country)], year, ".xlsx")))

  ## Make sure diagnosis codes ("multiple_cause_1", "multiple_cause_2", etc.) are characters
  dx_cols <- c(names(data)[grepl("multiple_cause", names(data))], "cause")
  lapply(X = dx_cols, FUN = check_class, df=data, class="character")

  ## Noting when someone has a HF code anywhere in their chain
  data[unique(unlist(lapply(data[, dx_cols, with=F], grep, pattern=hf_codes))), hf_chain := 1]
  data[is.na(hf_chain), hf_chain := 0]

  ## Cleaning of diagnosis columns:
  ##  - remove the ICD dot ("I50.0" to "I500") for consistency with the map
  ##  - turning ICD codes into yll causes ("I50" to "garbage_code")
  data <- clean_cols(data)
  lapply(X = dx_cols, FUN = remove_ICD_dot, df=data)
  data <- icd_to_yll("cause", data)

  ## Restrict to the causes we care about
  data <- data[cause_yll_cause %in% etiologies,]
  data <- data[cause_yll_cause %in% hund_attribution, hf_chain := 1]

  ## Count and append
  data[, sum_deaths := sum(deaths), by=c("cause_yll_cause", "age_group_id", "sex_id")]
  data[, sum_hf_deaths := sum(hf_chain), by=c("cause_yll_cause", "age_group_id", "sex_id")]

  data <- data[, .(sum_deaths, sum_hf_deaths, cause_yll_cause, cause_yll_cause_name, age_group_id, sex_id, year_id)]
  data <- unique(data)

  twn_dt <- rbind(twn_dt, data, fill=T)

}

fwrite(twn_dt, file = paste0("FILEPATH"))

country <- "bra"
bra_dt <- data.table()

for (year in 1999:2016) {

  print(year)

  ## Pull in data
  data <- fread(paste0(paths[,get(country)], names[,get(country)], year, ".csv"))

  ## Make sure diagnosis codes ("multiple_cause_1", "multiple_cause_2", etc.) are characters
  dx_cols <- c(names(data)[grepl("multiple_cause", names(data))], "cause")
  lapply(X = dx_cols, FUN = check_class, df=data, class="character")

  ## Noting when someone has a HF code anywhere in their chain
  data[unique(unlist(lapply(data[, dx_cols, with=F], grep, pattern=hf_codes))), hf_chain := 1]
  data[is.na(hf_chain), hf_chain := 0]

  ## Cleaning of diagnosis columns:
  ##  - remove the ICD dot ("I50.0" to "I500") for consistency with the map
  ##  - turning ICD codes into yll causes ("I50" to "garbage_code")
  data <- clean_cols(data)
  lapply(X = dx_cols, FUN = remove_ICD_dot, df=data)
  data <- icd_to_yll("cause", data)

  ## Restrict to the causes we care about
  data <- data[cause_yll_cause %in% etiologies,]
  data <- data[cause_yll_cause %in% hund_attribution, hf_chain := 1]

  ## Count and append
  data[, sum_deaths := sum(deaths), by=c("cause_yll_cause", "age_group_id", "sex_id", "location_id")]
  data[, sum_hf_deaths := sum(hf_chain), by=c("cause_yll_cause", "age_group_id", "sex_id", "location_id")]

  data <- data[, .(sum_deaths, sum_hf_deaths, cause_yll_cause, cause_yll_cause_name, age_group_id, sex_id, year_id, location_id)]
  data <- unique(data)

  bra_dt <- rbind(bra_dt, data, fill=T)

}

fwrite(bra_dt, file = paste0("FILEPATH"))


country <- "col"
col_dt <- data.table()


for (year in 1998:2017) {
  
  print(year)
  
  ## Pull in data
  data <- fread(paste0(paths[,get(country)], names[,get(country)], year, ".csv"))
  
  if (year %in% 1998:2007) {
    setnames(data, old=c("c_dir1", "c_ant1", "c_ant2", "c_ant3", "c_bas1"), new=c("multiple_cause_1", "multiple_cause_2", "multiple_cause_3", "multiple_cause_5", "cause"))
  } else {
    setnames(data, old=c("c_dir1", "c_dir12", "c_ant1", "c_ant12", "c_ant2", "c_ant22", "c_ant3", "c_ant32", "c_bas1"),
             new = c("multiple_cause_1", "multiple_cause_2", "multiple_cause_3", "multiple_cause_4", "multiple_cause_5", 
                     "multiple_cause_6", "multiple_cause_7", "multiple_cause_8", "cause"))
  }
  
  data <- data[!(gru_ed1 == 26)] # Unknown age
  
  data$gru_ed1 <- as.numeric(data$gru_ed1)
  data <- merge(data, age_dt, by.x="gru_ed1", by.y="old_age")
  
  data <- merge(data, age_groups[, .(age_group_years_start, age_group_id)], by.x="new_age", by.y="age_group_years_start")

  ## Make sure diagnosis codes ("multiple_cause_1", "multiple_cause_2", etc.) are characters
  dx_cols <- c(names(data)[grepl("multiple_cause", names(data))], "cause")
  lapply(X = dx_cols, FUN = check_class, df=data, class="character")
  
  ## Noting when someone has a HF code anywhere in their chain
  data[unique(unlist(lapply(data[, dx_cols, with=F], grep, pattern=hf_codes))), hf_chain := 1]
  data[is.na(hf_chain), hf_chain := 0]
  
  ## Cleaning of diagnosis columns:
  ##  - remove the ICD dot ("I50.0" to "I500") for consistency with the map
  ##  - turning ICD codes into yll causes ("I50" to "garbage_code")
  data <- clean_cols(data)
  lapply(X = dx_cols, FUN = remove_ICD_dot, df=data)
  data <- icd_to_yll("cause", data)
  
  ## Restrict to the causes we care about
  data <- data[cause_yll_cause %in% etiologies,]
  data <- data[cause_yll_cause %in% hund_attribution, hf_chain := 1]
  
  ## Count and append
  data[, sum_deaths := sum(deaths), by=c("cause_yll_cause", "age_group_id", "sex_id")]
  data[, sum_hf_deaths := sum(hf_chain), by=c("cause_yll_cause", "age_group_id", "sex_id")]
  
  data <- data[, .(sum_deaths, sum_hf_deaths, cause_yll_cause, cause_yll_cause_name, age_group_id, sex_id, year_id)]
  data <- unique(data)
  
  col_dt <- rbind(col_dt, data, fill=T)
  
}
  
fwrite(col_dt, paste0("FILEPATH"))


country <- "mex"
mex_dt <- data.table()
for (year in 2009:2016) {
  
  print(year)
  
  ## Pull in data
  data <- fread(paste0(paths[,get(country)], names[,get(country)], year, ".csv"))
  data[, deaths := 1]
  
  ## Make sure diagnosis codes ("multiple_cause_1", "multiple_cause_2", etc.) are characters
  dx_cols <- c(names(data)[grepl("multiple_cause", names(data))], "cause")
  lapply(X = dx_cols, FUN = check_class, df=data, class="character")
  
  ## Noting when someone has a HF code anywhere in their chain
  data[unique(unlist(lapply(data[, dx_cols, with=F], grep, pattern=hf_codes))), hf_chain := 1]
  data[is.na(hf_chain), hf_chain := 0]
  
  ## Cleaning of diagnosis columns:
  ##  - remove the ICD dot ("I50.0" to "I500") for consistency with the map
  ##  - turning ICD codes into yll causes ("I50" to "garbage_code")
  data <- clean_cols(data)
  data[, icd_type := "ICD10"]
  #lapply(X = dx_cols, FUN = remove_ICD_dot, df=data)
  data <- icd_to_yll("cause", data)
  
  ## Restrict to the causes we care about
  data <- data[cause_yll_cause %in% etiologies,]
  data <- data[cause_yll_cause %in% hund_attribution, hf_chain := 1]
  
  ## Count and append
  data[, sum_deaths := sum(deaths), by=c("cause_yll_cause", "age_group_id", "sex_id", "location_id")]
  data[, sum_hf_deaths := sum(hf_chain), by=c("cause_yll_cause", "age_group_id", "sex_id", "location_id")]
  
  data <- data[, .(sum_deaths, sum_hf_deaths, cause_yll_cause, cause_yll_cause_name, age_group_id, sex_id, year_id, location_id)]
  data <- unique(data)
  
  mex_dt <- rbind(mex_dt, data, fill=T)
  
}

fwrite(mex_dt, file = paste0("FILEPATH"))

