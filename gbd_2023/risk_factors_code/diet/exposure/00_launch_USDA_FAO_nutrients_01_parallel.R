rm(list = ls())

# Libraries
library(data.table)
library(tidyverse)
library(dplyr)
library(haven) # for reading in dta files
library(argparse)
library(stringr)
library(FAOSTAT, lib.loc = "FILEPATH") 
source("FILEPATH/r/get_ids.R")
source("FILEPATH/r/get_location_metadata.R")


# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"

##############################################################
# CODE BLOCKS RUN BASED ON THESE FLAGS!!!  (T = run, F = skip)

prep_sua <- F # IF SUA PREP NOT NEEDED, SET TO FALSE
run_01 <- T # IF NUTRIENT CALCULATIONS FOR LOCATIONS NOT NEEDED, SET TO FALSE
run_jobs <- T

##############################################################

# Paths
sge.output.dir <- paste0("FILEPATH") # output and error logs

code_path <- "FILEPATH/diet_estimation/1_exposure/fao_data/"

# Arguments (These should carry over into the launched scripts) 
parser <- ArgumentParser()
parser$add_argument("--gbd_round", help = "GBD round/cycle",
                    default = "gbd2022", type = "character")
parser$add_argument("--release_id", help = "GBD release id",
                    default = '16', type = "integer")
parser$add_argument("--version", help = "Version (?)",
                    default = "2022", type = "character")

args <- parser$parse_args()
args_flat <- paste(args[1], args[2], args[3]) 

list2env(args, environment());# rm(args)


data_path <- paste0("FILEPATH/exposure/", gbd_round, "/data/to_be_compiled/") # 

#### SUA PREP CODE ####

# Note: Intentionally keeping all rows from left dataframe in each merge operation.
if (prep_sua) {
  
  sua2015 <- read_csv('FILEPATH') # input data
  sua_data_public <- fread("FILEPATH") 
  reshape_josef_fbs <- read_dta(paste0(j, "FILEPATH")) # used to "borrow populations from the old FBS (one used in 2015) version since this one doesn't have values for South Sudan, Eritrea, and Sudan" 
  new_fbs <- read_csv('FILEPATH')
  refuse_usda <- read_csv(paste0("FILEPATH"))
  
  # this gets FAO information we need using the FAOSTATpackage
  # Hong Kong and Macao have different codes at IHME, need to adjust manually...
  FAOSTAT_countries <- FAOcountryProfile
  FAOSTAT_countries <- FAOSTAT_countries %>% 
    dplyr::select(c(FAOST_CODE, ISO3_CODE, FAO_TABLE_NAME, OFFICIAL_FAO_NAME, SHORT_NAME)) %>%
    mutate(ISO3_CODE = replace(ISO3_CODE, ISO3_CODE == "HKG", "CHN_354"),
           ISO3_CODE = replace(ISO3_CODE, ISO3_CODE == "MAC", "CHN_361"))
  
  # South Sudan doesn't have a FAO code, need to add manually...
  FAOSTAT_countries$ISO3_CODE[is.na(FAOSTAT_countries$ISO3_CODE) & FAOSTAT_countries$FAOST_CODE == 277] <- 'SSD'
  
  # this gets IHME location information we need
  locs <- get_location_metadata(location_set_id = 35, release_id=release_id) %>% 
    dplyr::select(c(location_id, location_name, ihme_loc_id))
  
  ## SUA2015 (old data) processing
  
  sua2017_reshape <- sua2015 %>% 
    pivot_longer(cols = -c("countries", "product_codes", "products", "ELE_codes", "Country_codes", "elements"),
                 names_to = "year", names_prefix = '', values_to = "data")

  sua2017_reshape$year <- as.numeric(substr(sua2017_reshape$year, 2, 7)) # removes X_ prefix on year values
  
  setnames(sua2017_reshape, old = c('ELE_codes', 'Country_codes'),  # rename column (not sure why this is done)
           new = c('ele_codes', 'country_codes'))
  
  sua2017_reshape <- filter(sua2017_reshape, ele_codes == 11 | ele_codes == 141)
  
  sua2017_reshape <- sua2017_reshape %>% # create ele_name column and insert MT/year into this column for ELE_codes == 141
    mutate(ele_name = case_when(ele_codes == 141 ~ "MT/year"))
  
  # filter out data for years 2010-2013 (we decided to keep the current 2010-2013 data since its more recently updated)
  sua2017_reshape <- filter(sua2017_reshape, year < 2010)
  
  sua2017_reshape$source <- 'old' # tracking differences between old and new
  
  ## Current SUA data processing
  
  sua_data_public_reshape <- sua_data_public %>% 
    dplyr::select(-c('Y2010F','Y2011F','Y2012F','Y2013F','Y2014F', 'Y2015F','Y2016F','Y2017F','Y2018F','Y2019F', 'Y2020F', 'Y2021F', "Area Code (M49)", "Item Code (CPC)", "Unit")) %>%
    pivot_longer(cols = -c("Area Code", "Area", "Item Code", "Item", "Element Code", "Element"),
                 names_to = "year", names_prefix = '', values_to = "data")
  
  sua_data_public_reshape$year <- as.numeric(substr(sua_data_public_reshape$year, 2, 7)) # removes Y prefix on year values
  
  setnames(sua_data_public_reshape, old = c('Element Code', 'Area Code', 'Area', 'Item Code', 'Item', 'Element'),
           new = c('ele_codes', 'country_codes', 'countries', 'product_codes', 'products', 'elements'))
  
  sua_data_public_reshape <- filter(sua_data_public_reshape, ele_codes == 511 | ele_codes == 5141) # | ele_codes == 664 | ele_codes == 665) # not sure if 664 and 665 should be kept
  
  sua_data_public_reshape <- sua_data_public_reshape %>% 
    mutate(ele_name = case_when(ele_codes == 5141 ~ "MT/year"))
  
  sua_data_public_reshape$source <- 'current' # tracking differences between old and new
  
  ## Current FBS data processing (for fish data)
  
  new_fbs_fish <- new_fbs %>%
    filter(Item %in% c("Population", "Freshwater Fish", "Demersal Fish", "Pelagic Fish", "Marine Fish, Other", "Cephalopods", "Crustaceans", "Mollusks, Other", "Aquatic Animals, Other"))
  
  new_fbs_fish <- new_fbs_fish %>% 
    dplyr::select(-c('Y2010F','Y2011F','Y2012F','Y2013F','Y2014F', 'Y2015F','Y2016F','Y2017F','Y2018F','Y2019F', 'Y2020F', 'Y2021F', "Area Code (M49)", "Item Code (FBS)", "Unit")) %>%
    pivot_longer(cols = -c("Area Code", "Area", "Item Code", "Item", "Element Code", "Element"),
                 names_to = "year", names_prefix = '', values_to = "data")
  
  new_fbs_fish$year <- as.numeric(substr(new_fbs_fish$year, 2, 7)) # removes Y prefix on year values
  
  setnames(new_fbs_fish, old = c('Element Code', 'Area Code', 'Area', 'Item Code', 'Item', 'Element'),
           new = c('ele_codes', 'country_codes', 'countries', 'product_codes', 'products', 'elements'))
  
  new_fbs_fish <- filter(new_fbs_fish, ele_codes %in% c(511,645)) # kg/capita/yr
  
  new_fbs_fish <- new_fbs_fish %>%
    mutate(ele_name = case_when(ele_codes == 5141 ~ "MT/year")) 
  
  new_fbs_fish$source <- 'current_fbs' # tracking differences between old and new
  
  # Combine current FBS data with current SUA data
  current_combined <- rbind(sua_data_public_reshape, new_fbs_fish)
  
  # Combine both dataframes and continue processing
  sua_combined <- rbind(sua2017_reshape, current_combined)
  sua_combined$countries <- str_replace(sua_combined$countries, "Sudan, former", "Sudan")
  store_pops_sua2017 <- filter(sua_combined, ele_codes %in% c(11, 511) & product_codes %in% c(1, 2501)) # also includes 2501 for fbs pops
  store_pops_sua2017 <- store_pops_sua2017 %>% 
    mutate(population = store_pops_sua2017$data) %>%
    dplyr::select(countries, year, population, source) 
  reshape_josef_fbs$countries <- gsub("Sudan \\(former\\)", "Sudan", reshape_josef_fbs$countries)  # replace 'Sudan (former)' to Sudan to match IHME
  rjfbs_filt <- filter(reshape_josef_fbs, ele_codes %in% c(11, 511) & product_codes == 1) # getting pops for Eritrea and Sudan (updated with new data)
  rjfbs_er_sf_filt <- rjfbs_filt[rjfbs_filt$countries %in% c("Eritrea", "Sudan"), ]
  
  pops_append <- bind_rows(store_pops_sua2017,rjfbs_er_sf_filt)
  merged_data <- merge(sua_combined, pops_append, by = c("countries", "year", "source"), all.x = TRUE, all.y = TRUE) 
  merged_data <- merged_data %>% mutate(population = ifelse(population %in% NA, data.y, population))
  
  # converting data to g/person/day
  # since SUA and FBS are in different starting units, need to handle them individually
  merged_data <- merged_data %>%
    mutate(population = population * 1000, 
           data.x = case_when(
             !grepl("current_fbs", source) ~ 1000000 * data.x / (population * 365), # Tonnes to g/person/day conversion
             grepl("current_fbs", source) ~ 1000 * data.x / (365) # kg/capita/year to g/person/day conversion
           ),
           ele_name = "g/person/day"
    )  
  merged_data <- merged_data %>% mutate(country_codes.x = ifelse(country_codes.x %in% NA, country_codes.y, country_codes.x))
  
  corrected_sua = merged_data[0:11]
  setnames(corrected_sua, old = colnames(corrected_sua),
           # new = c('countries', 'year', 'country_codes', 'product_codes', 'products', 'ele_codes', 'elements', 'data', 'ele_name', 'source', 'population'))
           new = c('countries', 'year', 'source', 'country_codes', 'product_codes', 'products', 'ele_codes', 'elements', 'data', 'ele_name', 'population')) # reordered when merging on source
  setnames(refuse_usda, old = 'com', new = 'product_codes')
  
  fao_to_usda_codes <- merge(corrected_sua, refuse_usda, by = "product_codes", all.x = TRUE) # merge fao refuse data with sua data
  
  fao_to_usda_codes$refuse_fao <- NULL # remove refuse_fao
  
  fao_to_usda_codes$Refuse <- fao_to_usda_codes$Refuse / 100
  
  setnames(fao_to_usda_codes, old = 'data', new = 'old_data')
  
  fao_to_usda_codes <- fao_to_usda_codes %>%
    mutate(data = old_data * (1-Refuse))
  
  fao_to_usda_codes$data <- fao_to_usda_codes$data / 100
  
  fao_to_usda_codes <- fao_to_usda_codes %>%
    mutate(ele_name = "100g/person/day")
  
  # merge FAO information to SUA data
  simple_sua_final <- merge(fao_to_usda_codes, FAOSTAT_countries, by.x = "country_codes", by.y = "FAOST_CODE", all.x = TRUE)
  
  # merge IHME location information to SUA data
  simple_sua_final <- merge(simple_sua_final,  locs, by.x = "ISO3_CODE", by.y = "ihme_loc_id", all.x = TRUE) 
  
  simple_sua_final <- simple_sua_final[complete.cases(simple_sua_final$location_id), ]
  
  loc_ids <- unique(simple_sua_final$location_id)
  job_ids <- 1:length(loc_ids) # for running 01 arrayed on cluster
  simple_sua_locid_unique <- data.frame(loc_ids, job_ids)
  
  write_csv(simple_sua_locid_unique, paste0(data_path, "simple_sua_locid_unique.csv"))
  write_csv(simple_sua_final, paste0(data_path, "simple_sua_nadim.csv"))
  
}

#### END SUA PREP CODE ####

#### BEGIN JOB RUN CODE ####

if (run_jobs) {
  
  ### 01_USDA_FAO_nutrient_calc_parallel.R ###
  
  if (run_01 == T) {
    
    map_path <- paste0(data_path, "simple_sua_locid_unique.csv")
    
    rw <- length(count.fields(map_path, sep = ","))-1
    
    sys.sub_01 <- paste0("sbatch -J ", "fao01 -c 20 --array=0-", rw, "%10 --mem=12G -t 02:00:00 -A proj_diet -p all.q ", sge.output.dir)

    script_01 <- paste0(code_path, "01_fortified_USDA_FAO_nutrient_calc_parallel.R")
    jid_01 <- system(paste(sys.sub_01, "FILEPATH/execRscript.sh", "-s", script_01, args_flat, map_path), intern = T)
    job_id_01 <- stringr::str_extract(jid_01, "[:digit:]+") 
    sys.sub_02 <- paste0("sbatch -J fao02 -c 6 -d ", job_id_01, " --mem=12G -t 02:00:00 -A proj_erf -p all.q ", sge.output.dir)
    
  } else {
    
    job_id_01 <- "None. USDA + FAO nutrient calcuation step skipped."
    sys.sub_02 <- paste0("sbatch -J fao02 -c 6 --mem=12G -t 02:00:00 -A proj_diet -p all.q ", sge.output.dir)
    
  }
  
  ### 02_compile_nutrients_locs.R ###
  finished_jobs <- list.files(paste0("FIELPATH/fortification/01_output_parallelized/"), full.names = F)

  script_02 <- paste0(code_path, "02_compile_nutrients_locs.R")
  jid_02 <- system(paste(sys.sub_02, "FILEPATH/execRscript.sh", "-s", script_02, args_flat), intern = T)
  job_id_02 <- str_extract(jid_02, "[:digit:]+")
  
  ### 03_USDA_defs_to_FAO.R ###
  
  sys.sub_03 <- paste0("sbatch -J fao03 -c 6 -d ", job_id_02, " --mem=12G -t 02:00:00 -A proj_diet -p all.q ", sge.output.dir)
  script_03 <- paste0(code_path, "03_USDA_defs_to_FAO.R")
  jid_03 <- system(paste(sys.sub_03, "FILEPATH/execRscript.sh", "-s", script_03, args_flat), intern = T)
  job_id_03 <- str_extract(jid_03, "[:digit:]+")
  
  ### 04_SUA_prep_food_groups_compile.R ###
  
  sys.sub_04 <- paste0("sbatch -J fao04 -c 6 -d ", job_id_03, " --mem=12G -t 02:00:00 -A proj_diet -p all.q ", sge.output.dir)
  script_04 <- paste0(code_path, "04_SUA_prep_food_groups_compile.R")
  jid_04 <- system(paste(sys.sub_04, "FILEPATH/execRscript.sh", "-s", script_04, args_flat), intern = T)
  job_id_04 <- str_extract(jid_04, "[:digit:]+")
  
  message(paste("Job ID for script 01:", job_id_01))
  message(paste("Job ID for script 02:", job_id_02))
  message(paste("Job ID for script 03:", job_id_03))
  message(paste("Job ID for script 04:", job_id_04))
  
}