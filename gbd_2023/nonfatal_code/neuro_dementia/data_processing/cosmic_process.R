### Process COSMIC studies for upload to bundle

rm(list=ls())

# Set directories and parameters

j_root <- "FILEPATH" 
h_root <- "FILEPATH" 
l_root <- "FILEPATH" 
functions_dir <- "FILEPATH" 

dir_cosmic <- "FILEPATH" 
upload_dir <- "FILEPATH" 

# load packages and source functions
pacman::p_load(data.table, openxlsx, ggplot2, plyr, parallel, dplyr, RMySQL, stringr, msm, dbplyr, Hmisc, writexl)

functs <- c("save_bundle_version.R", "get_bundle_version.R", "get_crosswalk_version.R", "get_bundle_data.R",
            "save_crosswalk_version.R", "upload_bundle_data.R", "get_location_metadata.R", "get_age_metadata.R",
            "get_draws.R", "get_population.R", "get_ids.R", "get_elmo_ids.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))


################################################################################
# Get COSMIC results and process


# Get a list of filenames in the directory

filenames <- list.files(path = dir_cosmic, full.names = TRUE, recursive = TRUE)

# Print the filenames
for (filename in filenames) {
  print(filename)
}

keep_vars <- c("seq", "underlying_nid","nid", "age_start", "age_end", "sex", "measure", "mean", "lower", "upper", "cases", "sample_size", "type", "study",
               "year_start", "year_end", "is_outlier", "source_type", "representative_name", "urbanicity_type", "recall_type", "unit_type",
               "unit_value_as_published", "input_type", "design_effect", "recall_type_value", "uncertainty_type","uncertainty_type_value",
               "standard_error", "sampling_type", "effective_sample_size", "location_name")

# Function to standardized results .rds
process1 <- function(dt, keep=keep_vars, nid, year_start, year_end, location_name, urbanicity_type="Mixed/both"){
  study <- deparse(substitute(dt))
  dt <- copy(dt)
  dt[measure == "incidence", `:=` (age_start = min_age, age_end = max_age)]
  if("sex_id" %in% colnames(dt)){
    dt[sex_id == 1, sex := "Male"]
    dt[sex_id == 2, sex := "Female"]    
  }
  dt[, `:=` (cases = ncases, sample_size = nsample)]
  dt$nid <- nid
  dt$study <- study
  dt$year_start <- year_start
  dt$year_end <- year_end
  dt$is_outlier <- 0
  dt$location_name <- location_name
  dt$urbanicity_type <- urbanicity_type
  dt[measure == "incidence", unit_type := "Person*year"]
  dt[measure == "prevalence", unit_type := "Person"]
  dt$unit_value_as_published <- 1
  dt$recall_type <- "Not Set"
  dt$representative_name <- "Unknown"
  dt$source_type <- "Survey - longitudinal"
  dt$uncertainty_type <- "Confidence interval"
  dt$uncertainty_type_value <- 95
  additional_cols <-c("seq", "underlying_nid","input_type","design_effect","recall_type_value",
                      "standard_error", "sampling_type","effective_sample_size")
  dt[, additional_cols] <- NA
  dt <- dt[, keep, with=F]
  return(dt)
}


#### STUDIES WITH NIDS

HELIAD <- readRDS(paste0(dir_cosmic, "heliad_incprev_output_full_model_2022_05_09.rds"))
HELIAD_prev <- HELIAD$prev_dt
HELIAD_inci <- HELIAD$inc_dt %>% 
  mutate(measure = "incidence")
HELIAD <- rbind(HELIAD_prev, HELIAD_inci, fill=TRUE) %>% 
  mutate(min_age = age_min, max_age = age_max)
HELIAD <-process1(HELIAD,
                  nid=501594,
                  year_start = 2009, year_end=2019,
                  location_name = "Greece")

KLOSCAD <- readRDS(paste0(dir_cosmic, "kloscad_incprev_2022_03_22.rds"))
KLOSCAD <- KLOSCAD %>% 
  mutate(age_start = min_age,
         age_end = max_age)
KLOSCAD <- process1(KLOSCAD, 
                    nid = 559729, 
                    year_start = 2010, year_end = 2018,
                    location_name = "Republic of Korea")

MAS <- read_rds(paste0(dir_cosmic, "MAS_incprev_2024_04_18.rds"))
MAS <- MAS %>% 
  mutate(age_start = min_age,
         age_end = max_age)
MAS <- process1(MAS, 
                nid = 559748,
                year_start = 2005, year_end = 2018,
                location_name = "Australia")

SAS <- readRDS(paste0(dir_cosmic, "sas_incprev_2024_12_23.rds"))
SAS_prev <- SAS$prev_dt
SAS_inci <- SAS$inc_dt %>% 
  mutate(measure = "incidence")
SAS <- rbind(SAS_prev, SAS_inci, fill=TRUE) %>% 
  mutate(min_age = age_min, 
         max_age = age_max)
SAS <- SAS[age_group == "95+", `:=` (min_age = 95, max_age = 99)]
SAS <- process1(SAS,
                nid=559747,
                year_start = 2011, year_end = 2017,
                urbanicity_type = "Urban",
                location_name = "Shanghai")

ESPIRIT <- read_rds(paste0(dir_cosmic, "espirit_incprev_2022_03_22.rds"))
ESPIRIT <- ESPIRIT %>% 
  mutate(age_start = min_age, age_end = max_age)
ESPIRIT <- process1(ESPIRIT, 
                    nid = 559910, 
                    year_start = 1999, year_end = 2016,
                    location_name = "France")

MMAP <- read_rds(paste0(dir_cosmic, "MMAP_incprev_2024_04_29.rds"))
MMAP <- MMAP %>% 
  mutate(age_start = min_age, age_end = max_age)
MMAP <- process1(MMAP, 
                 nid = 543395, 
                 year_start = 2011, year_end = 2015,
                 location_name = "Philippines")

LRGS <- readRDS(paste0(dir_cosmic, "lrgs_incprev_output_full_model_2022_05_10.rds"))
LRGS_prev <- LRGS$prev_dt
LRGS_inci <- LRGS$inc_dt %>% 
  mutate(measure = "incidence")
LRGS <- rbind(LRGS_prev, LRGS_inci, fill=TRUE) %>% 
  mutate(min_age = age_min, 
         max_age = age_max,
         ncases = NA,
         nsample = NA)
LRGS <- process1(LRGS,
                 nid=559927,
                 year_start = 2013, year_end = 2014,
                 location_name = "Malaysia")

LEILA <- read_rds(paste0(dir_cosmic, "LEILA_incprev_2022_12_18.rds"))
LEILA <- LEILA %>% 
  mutate(age_start = min_age, age_end = max_age)
LEILA <- process1(LEILA, 
                  nid = 559926,
                  year_start = 1996, year_end = 2014,
                  location_name = "Germany")

SLAS <- read_rds(paste0(dir_cosmic, "SLAS_incprev_2023_12_18.rds"))
SLAS <- SLAS %>% 
  mutate(age_start = min_age, age_end = max_age)
SLAS <- process1(SLAS, 
                 nid = 497828, 
                 year_start = 2008, year_end = 2013,
                 location_name = "Singapore")


INVECE <- readRDS(paste0(dir_cosmic, "inceve_incprev_2021_12_07.rds"))
INVECE <- process1(INVECE, 
                   nid = 559925, 
                   year_start = 2009, year_end = 2011,
                   location_name = "Italy")

EAS <- readRDS(paste0(dir_cosmic, "eas_incprev_output_2024_11_20.rds"))
EAS_prev <- EAS$prev_dt
EAS_inci <- EAS$inc_dt %>% 
  mutate(measure = "incidence")
EAS <- rbind(EAS_prev, EAS_inci, fill=TRUE) %>% 
  mutate(min_age = age_min, max_age = age_max)
EAS <- process1(EAS,
                nid=559702,
                year_start = 1993, year_end = 2010,
                urbanicity_type = "Urban",
                location_name = "New York")

SALSA <- read_rds(paste0(dir_cosmic, "SALSA_incprev_2024_04_18.rds"))
SALSA <- SALSA %>% 
  mutate(age_start = min_age, age_end = max_age)
SALSA <- process1(SALSA, 
                  nid = 559742, 
                  year_start = 1998, year_end = 2007,
                  urbanicity_type = "Urban",
                  location_name = "California")

CHAS <- readRDS(paste0(dir_cosmic, "chas_incprev_2022_04_05full_model.rds"))
CHAS <- process1(CHAS, 
                 nid=559890, 
                 year_start = 2002, year_end = 2006,
                 location_name= "Cuba")

MAAS <- read_rds(paste0(dir_cosmic, "MAAS_incprev_2024_03_19.rds"))
MAAS <- MAAS %>% 
  mutate(age_start = min_age, age_end = max_age)
MAAS <- process1(MAAS, 
                 nid = 508295, 
                 year_start = 1993, year_end = 2005,
                 location_name = "Netherlands")

MYNAH <- read_rds(paste0(dir_cosmic, "mynah_prev_output_2024_01_10.rds"))
MYNAH <- MYNAH %>% 
  mutate(type="")
MYNAH <- process1(MYNAH,
                  nid=559743,
                  year_start = 1993, year_end = 2003,
                  location_name = "Karnataka")

ZARADEMP <- readRDS(paste0(dir_cosmic, "zarademp_incprev_2021_12_07.rds"))
ZARADEMP <- ZARADEMP %>% 
  mutate(age_start = min_age, age_end = max_age)
ZARADEMP <- process1(ZARADEMP,
                     nid = 559749,
                     year_start = 1995, year_end = 2001,
                     location_name = "Spain")

FAROE <- read_rds(paste0(dir_cosmic, "FAROE_inc_2022_12_18.rds"))
FAROE <- process1(FAROE, 
                  nid = 560143 , 
                  year_start = 2007, year_end = 2024,
                  location_name = "Denmark")

GOTHENBURG <- read_rds(paste0(dir_cosmic, "GOTHENBURG_inc_2022_12_18.rds"))
GOTHENBURG <- process1(GOTHENBURG, 
                       nid = 560144, 
                       year_start = 1971, year_end = 2024,
                       location_name = "Sweden")

MYHAT <- read_rds(paste0(dir_cosmic, "MYHAT_incprev_2022_12_18.rds"))
MYHAT <- MYHAT %>% 
  mutate(age_start = min_age, age_end = max_age)
MYHAT <- process1(MYHAT, 
                  nid = 560141, 
                  year_start = 2006, year_end = 2024,
                  location_name = "Pennsylvania")

PATH <- read_rds(paste0(dir_cosmic, "PATH_inc_2022_12_18.rds"))
PATH <- process1(PATH, 
                 nid = 560138, 
                 year_start = 2001, year_end = 2021,
                 location_name = "Australia")

SPAH <- read_rds(paste0(dir_cosmic, "SPAH_incprev_2024_04_10.rds"))
SPAH <- SPAH %>% 
  mutate(age_start = min_age, age_end = max_age)
SPAH <- process1(SPAH,
                 nid = 560139,
                 year_start = 2003, year_end = 2008,
                 location_name = "São Paulo")

VP <- read_rds(paste0(dir_cosmic, "VP_incprev_2024_03_20.rds"))
VP <- VP %>% 
  mutate(age_start = min_age, age_end = max_age)
VP <- process1(VP,
               nid = 560142 ,
               year_start = 2011, year_end = 2024,
               location_name = "Spain")

CLAS <- read_rds(paste0(dir_cosmic, "CLAS_incprev_2024_03_26.rds"))
CLAS <- CLAS %>% 
  mutate(age_start = min_age, age_end = max_age)
CLAS <- process1(CLAS, 
                 nid = 560140,
                 year_start = 2011 , year_end = 2024,
                 location_name = "China")

EPIDEMCA <- read_rds(paste0(dir_cosmic, "EPIDEMCA_incprev_2022_12_18.rds")) # Note - Central African Republic and Republic of Congo
EPIDEMCA <- EPIDEMCA %>% 
  mutate(age_start = min_age, age_end = max_age)
EPIDEMCA <- process1(EPIDEMCA,
                     nid = 560137,
                     year_start = 2011, year_end = 2014,
                     location_name = "Central Sub-Saharan Africa")

## Combine all studies and add location_id
locations <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
locations <- locations %>% 
  select(location_id, location_name)

COSMIC_ALL <- rbind(HELIAD, KLOSCAD, MAS, SAS, ESPIRIT, MMAP, LRGS, LEILA, SLAS, INVECE, EAS, SALSA, CHAS, MAAS, MYNAH, ZARADEMP,
                    FAROE, GOTHENBURG, MYHAT, PATH, SPAH, VP, CLAS, EPIDEMCA)
COSMIC_ALL[measure == "prevalence", type := ""]
COSMIC_ALL[type == "unweighted", `:=` (is_outlier = 1, note_modeler = "outliered unweighted cosmic incidence estimates")]
COSMIC_ALL[, extractor := "USERNAME"]
COSMIC_ALL <- COSMIC_ALL %>% 
  left_join(locations, by="location_name") %>% 
  mutate(lower = ifelse(lower < 0, 0, lower)) %>% 
  mutate_at(vars(mean, lower, upper, uncertainty_type, uncertainty_type_value), ~ ifelse(mean<lower, NA, .))


write_xlsx(COSMIC_ALL, path = paste0(upload_dir, "cosmic_results.xlsx"))
