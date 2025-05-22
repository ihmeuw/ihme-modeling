
##
## Author: USER
## Date: DATE
##
## Purpose: Get the MCOD proportion from several countries.
##          This is the proportion of deaths from a given etiology that have heart failure associated.
##
##
## source('FILEPATH')
##
rm(list=ls())
os <- .Platform$OS.type

date<-gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, doBy)


###### Paths, args
############################################################################################################

## Get args from sbatch
args <-commandArgs(trailingOnly = TRUE)
task_id <- ifelse(is.na(as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))), 1, as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID")))

country_path <- args[1]
countries <- fread(country_path)
country <- countries[task_id, .(country_ident)]$country_ident

## Central functions
central <- "FILEPATH"

## These are paths to year-specific cleaned data
paths <- data.table(
  usa = "FILEPATH",
  twn = "FILEPATH", 
  bra = "FILEPATH",
  col = "FILEPATH",  
  mex = "FILEPATH"
)

## File name roots in the above folders
names <- data.table(
  usa = "usa_gbd2020_ages_",
  bra = "bra_gbd2020_ages_",
  twn = "clean_twn_vr_",
  col = "col_",
  mex = "mex_"
)

write_path <- 'FILEPATH'

## Etiology list for HF
etiologies <-  c("cvd_ihd", "cvd_rhd", "cvd_htn", "cvd_cmp_myocarditis", "cvd_cmp_other", "cvd_endo", "cvd_other",
                 "resp_copd", "resp_pneum_silico", "resp_pneum_asbest", "resp_pneum_coal", "resp_pneum_other",
                 "resp_interstitial", "hemog_thalass", "hemog_g6pd", "hemog_other", "endo_other", "cong_heart",
                 "cvd_cmp_alcoholic", "cvd_valvu_other", "cvd_pah", 
                 "mental_drug_cocaine", 
                 "mental_drug_amphet",
                 "cvd_afib",
                 "ckd",
                 "cirrhosis",
                 "cvd_stroke_cerhem", "cvd_stroke_isch", "cvd_stroke_subhem", 
                 "cvd_stroke",
                 "cvd_ihd_chronic", "cvd_ihd_acute","cvd_ihd_angina",
                 "endo_thyroid", "endo_hypothyroid", "endo_hyperthyroid") 

hund_attribution <- c("cvd_htn", "cvd_cmp_other", "cvd_cmp_alcoholic")

## ICD codes for HF 
hf_codes <- "I50|I11|^428|^402|^425" # HF, HHD, cardiomyopathies

gbd_round_id <- VALUE
decomp_step <- "VALUE"


###### Functions
############################################################################################################

## Age metadata
source(paste0(central, "get_age_metadata.R"))
age_groups <- get_age_metadata(age_group_set_id = VALUE, gbd_round_id = gbd_round_id)

## Functions written to deal with multiple diagnosis data
source('FILEPATH')

## Helper functions written by USER and USER
source('FILEPATH')

###### Read in data, and count the number of deaths that had HF associated. 
###### For USA, Mex, Bra, save by state.
############################################################################################################

if (country == "usa") {
  
  usa_dt <- data.table()
  
  for (year in 1992:2016) {
    
    print(year)
    
    ## Pull in data
    data <- fread(paste0(paths[,get(country)], names[,get(country)], year, ".csv"))
    
    ## Make sure diagnosis codes ("multiple_cause_1", "multiple_cause_2", etc.) are characters
    dx_cols <- c(names(data)[grepl("multiple_cause", names(data))], "cause")
    lapply(X = dx_cols, FUN = check_class, df=data, class="character")
    
    ## Noting when someone has a HF code anywhere in their chain
    data[unique(unlist(lapply(data[, dx_cols, with=F], grep, pattern=hf_codes, perl=T))), hf_chain := 1]
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
    
    # Rename sex to sex_id 
    data[,sex_id:=sex]
    
    ## Count and append
    data[, sum_deaths := sum(deaths), by=c("cause_yll_cause", "age", "sex_id", "state_occ_alpha")]
    data[, sum_hf_deaths := sum(hf_chain), by=c("cause_yll_cause", "age", "sex_id", "state_occ_alpha")]
    
    data <- data[, .(sum_deaths, sum_hf_deaths, cause_yll_cause, cause_yll_cause_name, age, sex_id, year, state_occ_alpha)]
    data <- unique(data)
    
    data[age == "0d", age := as.character(0)]
    data[age == "7d", age := as.character(0.01917808)]
    data[age == "1-5mo", age := as.character(0.07671233)]
    data[age == "6-11mo", age := as.character(0.50136986)]
    data[age == "12-23mo", age := as.character(1)]
    data[age == "2-4", age := as.character(2)]
    data$age <- as.numeric(data$age)
    
    ## Pull in age group IDs
    data <- merge(data, age_groups[, .(age_group_id, age_group_years_start)], by.x="age", by.y="age_group_years_start")
    setnames(data, old=c("year"), new=c("year_id"))
    
    usa_dt <- rbind(usa_dt, data, fill=T)
    
  }
  
  fwrite(usa_dt, file = paste0(write_path, "usa_mcod_counts", gbd_round_id, decomp_step, "_", date, ".csv"))
  
} else if (country == "twn") {
  
  twn_dt <- data.table()
  
  for (year in 2008:2016) {
    
    print(year)
    
    ## Pull in data
    data <- as.data.table(readxl::read_excel(paste0(paths[,get(country)], names[,get(country)], year, ".xlsx")))
    
    ## Make sure diagnosis codes ("multiple_cause_1", "multiple_cause_2", etc.) are characters
    dx_cols <- c(names(data)[grepl("multiple_cause", names(data))], "cause")
    lapply(X = dx_cols, FUN = check_class, df=data, class="character")
    
    ## Noting when someone has a HF code anywhere in their chain
    data[unique(unlist(lapply(data[, dx_cols, with=F], grep, pattern=hf_codes, perl=T))), hf_chain := 1]
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
  
  fwrite(twn_dt, file = paste0(write_path, "twn_mcod_counts", gbd_round_id, decomp_step, "_", date, ".csv"))
  
} else if (country == "bra") {
  
  bra_dt <- data.table()
  
  for (year in 1999:2016) {
    
    print(year)
    
    ## Pull in data
    data <- fread(paste0(paths[,get(country)], names[,get(country)], year, ".csv"))
    
    ## Make sure diagnosis codes ("multiple_cause_1", "multiple_cause_2", etc.) are characters
    dx_cols <- c(names(data)[grepl("multiple_cause", names(data))], "cause")
    lapply(X = dx_cols, FUN = check_class, df=data, class="character")
    
    ## Noting when someone has a HF code anywhere in their chain
    data[unique(unlist(lapply(data[, dx_cols, with=F], grep, pattern=hf_codes, perl=T))), hf_chain := 1]
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
  
  fwrite(bra_dt, file = paste0(write_path, "bra_mcod_counts", gbd_round_id, decomp_step, "_", date, ".csv"))
  
} else if (country == "col") {
  
  col_dt <- data.table()
  
  for (year in 1998:2017) {
    
    print(year)
    
    ## Pull in data
    data <- fread(paste0(paths[,get(country)], names[,get(country)], year, ".csv"))
    
    ## Make sure diagnosis codes ("multiple_cause_1", "multiple_cause_2", etc.) are characters
    dx_cols <- c(names(data)[grepl("multiple_cause", names(data))], "cause")
    lapply(X = dx_cols, FUN = check_class, df=data, class="character")
    
    ## Noting when someone has a HF code anywhere in their chain
    data[unique(unlist(lapply(data[, dx_cols, with=F], grep, pattern=hf_codes, perl=T))), hf_chain := 1]
    data[is.na(hf_chain), hf_chain := 0]
    
    # Remove once the extract_col.R reruns
    data[,code_system_id:=2]
    
    ## Cleaning of diagnosis columns:
    ##  - remove the ICD dot ("I50.0" to "I500") for consistency with the map
    ##  - turning ICD codes into yll causes ("I50" to "garbage_code")
    data <- clean_cols(data)
    lapply(X = dx_cols, FUN = remove_ICD_dot, df=data)
    data <- icd_to_yll("cause", data)
    
    ## Restrict to the causes we care about
    data <- data[cause_yll_cause %in% etiologies,]
    data <- data[cause_yll_cause %in% hund_attribution, hf_chain := 1]
    
    data[, year_id:=year]
    ## Count and append
    data[, sum_deaths := sum(deaths), by=c("cause_yll_cause", "age_group_id", "sex_id")]
    data[, sum_hf_deaths := sum(hf_chain), by=c("cause_yll_cause", "age_group_id", "sex_id")]
    
    data <- data[, .(sum_deaths, sum_hf_deaths, cause_yll_cause, cause_yll_cause_name, age_group_id, sex_id, year_id)]
    data <- unique(data)
    
    col_dt <- rbind(col_dt, data, fill=T)
    
  }
  
  fwrite(col_dt, paste0(write_path, "col_mcod_counts", gbd_round_id, decomp_step, "_", date, ".csv"))
  
} else if (country == "mex") {
  
  mex_dt <- data.table()
  for (year in 2009:2016) {
    
    print(year)
    
    ## Pull in data
    data <- fread(paste0(paths[,get(country)], names[,get(country)], year, ".csv"))

    ## Make sure diagnosis codes ("multiple_cause_1", "multiple_cause_2", etc.) are characters
    dx_cols <- c(names(data)[grepl("multiple_cause", names(data))], "cause")
    lapply(X = dx_cols, FUN = check_class, df=data, class="character")
    
    ## Noting when someone has a HF code anywhere in their chain
    data[unique(unlist(lapply(data[, dx_cols, with=F], grep, pattern=hf_codes, perl=T))), hf_chain := 1]
    data[is.na(hf_chain), hf_chain := 0]
    
    ## To remove post rerunning extract
    data[,':='(icd_type="ICD10", year_id=year)]
    
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
    
    mex_dt <- rbind(mex_dt, data, fill=T)
    
  }
  
  fwrite(mex_dt, file = paste0(write_path, "mex_mcod_counts", gbd_round_id, decomp_step, "_", date, ".csv"))
  
}
