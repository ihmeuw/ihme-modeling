rm(list = ls())

library("dplyr")
library("tidyr")
library("data.table")
library("readxl")
library("feather")


if (Sys.info()["sysname"]=="Windows") {
  h <- "H:"
  j <- "J:"
} else {
  h <- "~"
  j <- "/snfs1"
  
}


# ESTABLISH PATHS AND FILENAMES ----------------------------------------------------------------------------------

this.dir <- FILEPATH  # add path to this repo, which contains the following mapping files.



# ISO3 code for country
iso3 <- ISOCODE HERE # Insert the ISO3 for the country that you want to run this on
description <- DESCRIPTION HERE # Give this run a description to be used in naming

# Column names
varnames <- data.frame(
  zonecode    = 'county_fips', # The name of the variable that contains the 8-digit code for the location of death
  location_id = 'location_id', # The IHME location id
  date        = 'date',        # The name of the variable that contains the date of death (as a character, formatted as "YYY-MM-DD")
  age         = 'age',         # The name of the variable that contains the age of the deceased (in years)
  sex         = 'sex',         # The name of the variable that contains the sex/gender of the deceased (coded as 1 = male, 2 = female)
  value       = 'cause',       # The name of the variable that contains ICD10 codes (underlying cause of death)
  stringsAsFactors = F)


paths <- data.frame(
  icdmapPath   = paste0(this.dir, 'inputs/package_map_icd9.csv'),
  restrictPath = paste0(this.dir, 'inputs/restrictions.csv'),
  rdPath       = paste0(this.dir, 'outputs/', iso3, "/", description, '/compiled/'),
  acPath       = paste0(this.dir, 'inputs/cause_meta.csv'),
  tempPath     = paste0(this.dir, 'temperature/', iso3, '/'),
  cmetaPath    = paste0(this.dir, 'inputs/'),
  stringsAsFactors = F)


source(paste0(this.dir, "code/clean_vr.R"))
source(paste0(this.dir, "code/apply_rd.R"))
source(paste0(this.dir, "code/temp_collapse.R"))


for (year in 1980:1988) {

  filename_in <- paste0(this.dir, "datasets/", iso3, "/", iso3, "_", year, "_vr.csv")   # path/filename of raw data
  filename_out <- paste0(this.dir, "outputs/", iso3, "/", iso3, "_", year, ".csv")      # path/filename to write out to
  
# APPLY VR DATA CLEANING & CAUSE CORRECTION FUNCTION ---------------------------------------------------------------

  # this following lines of code reads in the VR data file (excel format) and converts it to a data table.
  # If the data are stored in a different format you can change this to use the appropriate import function (e.g. fread or read.csv for a csv file)
  #vr_raw <- read_excel(filename_in)
  vr_raw <- fread(filename_in)
  setDT(vr_raw)

  vr_clean <- clean_vr(vr_raw, paths, varnames, iso3)
  if(exists("vr_clean")==T) vr_raw <- NULL
  
  
  # PULL OUT GARBAGE CODE ROWS AND REDISTRIBUTE ----------------------------------------------------------------------
  
  vr_clean_rd <- apply_rd(vr_clean, paths, iso3)
  if(exists("vr_clean_rd")==T) vr_clean <- NULL
  
  
  
  # MERGE VR AND TEMPERAUTRE DATA ------------------------------------------------------------------------------------
  
  vr_clean_rd_temp <- temp_collapse(vr_clean_rd, paths, iso3, collapse = F)
  if(exists("vr_clean_rd_temp")==T) vr_clean_rd <- NULL
  
  
# SAVE FILE --------------------------------------------------------------------------------------------------------

  filename_out <- paste0(this.dir, "outputs/", iso3, "/", description, "/", iso3, "_", year, ".feather")     # path/filename to write out to
  write_feather(vr_clean_rd_temp, filename_out)
}

