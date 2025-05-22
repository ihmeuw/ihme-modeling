
##
## Author: USER
## Date: DATE, updated DATE for final CoD-Correct
## 
## Purpose: Child script to get CSMR for etiologies of heart failure as input into proportion models.
##          Adapted from code written by USER and USER
##

rm(list=ls())

pacman::p_load(data.table, ggplot2, parallel, plyr)

args <-commandArgs(trailingOnly = TRUE)

task_id <- ifelse(is.na(as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))), 1, as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID")))

datetime <- gsub("-", "_", Sys.Date())

loc_path <- args[1]
print(loc_path)
print(paste0("Loc path is ", loc_path))
outputDir <- args[2]
print(paste0("OutputDir is ", outputDir))
single_yr <- args[3]

loc_path <- fread(loc_path)
loc_id <- loc_path[task_id, location_id]

print(paste0('Getting codcorrect draws for location ', loc_id))


###### Paths, args
#################################################################################

central <- "FILEPATH"

## CVD output folder
cvd_path = "FILEPATH"

## Parameters to change each reiteration: 
gbd_round_id <- VALUE
gbd_team <- VALUE
decomp_step <- VALUE
faux_correct <- F
compare_version_id <- VALUE


###### Functions
#################################################################################

source(paste0(central, 'get_outputs.R'))
source(paste0(central, 'get_demographics.R'))
source(paste0(central, 'get_population.R'))
source(paste0(central, "get_age_metadata.R"))
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_ids.R"))
source(paste0(central, "get_model_results.R"))

###### Pull in demographic data
#################################################################################

## Pull in age, sex, year IDs
metadata <- get_demographics(gbd_team=gbd_team, gbd_round_id=gbd_round_id)

# Tryin to pull in all and collapse onto prediction years
pred_year_ids <- c(1990, 1995, 2000, 2005, 2010, 2015, 2020)
if (single_yr){
  all_year_ids <- pred_year_ids
}else{
  all_year_ids <- 1990:2022
}

sex_ids  <- unique(metadata$sex_id)
age_group_ids = unique(metadata$age_group_id)

## Pull in HF etiology map and merge with HF IDs
composite <- fread(paste0(cvd_path, '/composite_cause_list_2020.csv'))
composite[prev_folder%like%"stroke",pull_csmr := 1]

age_groups <- get_age_metadata(VALUE, gbd_round_id = gbd_round_id)
locations <- get_location_metadata(VALUE, gbd_round_id = gbd_round_id)

###### Pull in CSMR
#################################################################################

## Necessary columns
group_cols = c('cause_id', 'sex_id', 'age_group_id', 'location_id', 'year_end')
measure_cols = c('mean_death', 'upper_death', 'lower_death')

## Get CSMR for all 
etiologies_csmr <- data.table(acause=composite[pull_csmr==1,acause])
etiologies_csmr[, acause := gsub("acute\\_|chronic\\_", "", acause)]
ids <- get_ids("cause")
COD_CAUSE_IDS <- merge(etiologies_csmr, ids, by = "acause")
COD_CAUSE_IDS <- COD_CAUSE_IDS$cause_id
COD_CAUSE_IDS <- unique(COD_CAUSE_IDS)

if (faux_correct) { 
  
  df <- get_outputs(process_version_id = VALUE, #most recent fauxcorrect run
                    topic = 'cause', 
                    measure_id = VALUE,
                    metric_id = VALUE,
                    cause_id = COD_CAUSE_IDS,
                    gbd_round_id = VALUE,
                    decomp_step = decomp_step, 
                    location_id = loc_id, 
                    year_id = year_ids, 
                    age_group_id = age_group_ids,
                    sex_id = sex_ids)
  
} else {
  
  df <- get_outputs(compare_version_id = compare_version_id, #most recent COD-Correct run
                    topic = 'cause', 
                    measure_id = VALUE,
                    metric_id = VALUE,
                    cause_id = COD_CAUSE_IDS,
                    gbd_round_id = gbd_round_id,
                    decomp_step = "step3", 
                    location_id = loc_id, 
                    year_id = all_year_ids, 
                    age_group_id = age_group_ids,
                    sex_id = sex_ids)
}

df[, c("expected", "location_type", "measure_name", "metric_name", "age_group_name") := NULL]
df[is.na(location_name), location_name := unique(df[!is.na(location_name),location_name])]

## Remove NA values (age restrictions)
df <- df[!(is.na(val)) & year_id %in% all_year_ids & measure_id==VALUE]

## Numeric columns should be 0 if empty
for (col in c("val", "upper", "lower")) df[is.na(get(paste0(col))), paste0(col) := 0]

## Collapse into 1990-last estimation year
pop <- get_population(location_id = loc_id, year_id = all_year_ids, age_group_id = age_group_ids, sex_id = sex_ids, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
df <- merge(df, pop, by=c("age_group_id", "year_id", "location_id", "sex_id"))

# function collapses to nearest 5 year pred year if they are adequately spaced out otherwise defaults to nearest input year
collapse_yrs <- function(input_yr, pred_yrs=pred_year_ids){
  
  dif <- input_yr - pred_yrs
  # check if there are > 2 nearby years, if there are take the smaller one
  if (sum(abs(dif)<=2)>1){
    return(pred_yrs[min(which(abs(dif) == min(abs(dif))))])
  }
  # otherwise take the closer of the 5 year intervals
  else{
    return(pred_yrs[which(abs(dif)<=2)])
  }
}

df[,collapse_yr := sapply(year_id, collapse_yrs)]

# Calc CSMR
df[, `:=` (cs_deaths = val*population, cs_deaths_upper = upper*population, cs_deaths_lower = lower*population)]
df[, `:=` (sum_cs_deaths = sum(cs_deaths), sum_cs_deaths_upper = sum(cs_deaths_upper), sum_cs_deaths_lower = sum(cs_deaths_lower)), by=c("age_group_id", "sex_id", "location_id", "cause_id", "collapse_yr")]
df[, sum_population := sum(population), by=c("age_group_id", "location_id", "sex_id", "cause_id", "collapse_yr")]
df[, `:=` (csmr_sum = sum_cs_deaths/sum_population, csmr_sum_high = sum_cs_deaths_upper/sum_population, csmr_sum_low = sum_cs_deaths_lower/sum_population)]
df[, csmr_sum_se := (csmr_sum_high - csmr_sum_low)/(2*1.96)]


df[, year_start := 1990]
df[, year_end := max(pred_year_ids)]

df <- merge(df, composite, by=c("cause_id", "cause_name"), all.x=T, all.y=F)
setnames(df, "acause.x", "acause")
df$acause.y<-NULL
df <- merge(df, age_groups, by=c("age_group_id"), all.x=T, all.y=F)
df <- merge(df, locations, by=c("location_id", "location_name"), all.x=T, all.y=F)

df <- unique(df[, .(age_group_id, location_id, sex_id, cause_id, acause, location_name, sex, csmr_sum, csmr_sum_se, year_start, year_end, collapse_yr)])

# Fill in missing values for any age-sex-cause-location-year combination with zeros
square <- expand.grid(age_group_id=unique(df$age_group_id), sex_id=unique(df$sex_id), cause_id=unique(df$cause_id),
                      location_id=unique(df$location_id), year_start = unique(df$year_start), year_end=unique(df$year_end), collapse_yr = unique(df$collapse_yr))
square <- merge(square, ids, by = c("cause_id"), all.x=T, all.y=F)
square <- merge(square, age_groups, by = "age_group_id", all.x=T, all.y=F)
square <- merge(square, locations, by = "location_id", all.x=T, all.y=F)

df <- as.data.table(merge(df, square, by=c("age_group_id", "sex_id", "cause_id", "acause", "location_name", "location_id", "year_end", "year_start", "collapse_yr"), all=T))
df <- unique(df[, .(age_group_id, location_id, sex_id, cause_id, acause, location_name, sex, csmr_sum, csmr_sum_se, year_start, year_end, collapse_yr)])

for (col in c("csmr_sum", "csmr_sum_se")) df[is.na(get(paste0(col))), paste0(col) := 0]
df[, sex := ifelse(sex_id==VALUE, "Male", "Female")]
df <- df[!is.na(cause_id)]

#######################################
## 4. Save output to FILEPATHs folder to be read back in the main script
#######################################

file <- paste0(outputDir, '/codcorrect_', loc_id, '.rds')
saveRDS(object = df, file = file)
print("Done!")

fileConn<-file(paste0(outputDir, "/log.txt"))
writeLines(c(paste0("Date ran: ", datetime),
             paste0("fauxcorrect: ", faux_correct),
             paste0("COD compare version ID: ", compare_version_id),
             paste0("Causes: ", COD_CAUSE_IDS),
             paste0("GBD Round ID: ", gbd_round_id)), fileConn)
close(fileConn)
