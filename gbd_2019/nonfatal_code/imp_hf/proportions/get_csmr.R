
## 
## Purpose: Child script to get CSMR for etiologies of heart failure as input into proportion models.
##
pacman::p_load(data.table, ggplot2, parallel, plyr)

args <-commandArgs(trailingOnly = TRUE)
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))

datetime <- gsub("-", "_", Sys.Date())

loc_path <- args[1]
print(loc_path)
print(paste0("Loc path is ", loc_path))
outputDir <- args[2]
print(paste0("OutputDir is ", outputDir))

loc_path <- fread(loc_path)
loc_id <- loc_path[task_id, location_id]

print(paste0('Getting codcorrect draws for location ', loc_id))


###### Paths, args
#################################################################################

central <- "FILEPATH"

## CVD output folder
cvd_path = "FILEPATH"

## Parameters to change each reiteration: 
gbd_round_id <- 6 
gbd_team <- "epi"
decomp_step <- "step4"
faux_correct <- F

## IDs with step 4 HF proportion model bundles, NIDs, and cause_ids
IDs <-data.table(name = c("VALUES"), 
                 bundle_id = c("VALUES"),
                 modelable_entity_id = c("VALUES"),
                 nid = c("VALUES"),
                 cause_id = c("VALUES")
)

###### Functions
#################################################################################

source(paste0(central, 'get_outputs.R'))
source(paste0(central, 'get_demographics.R'))
source(paste0(central, 'get_population.R'))
source(paste0(central, "get_age_metadata.R"))
source(paste0(central, "get_location_metadata.R"))


###### Pull in demographic data
#################################################################################

## Pull in age, sex, year IDs
metadata <- get_demographics(gbd_team="epi", gbd_round_id=6)
year_ids <- unique(metadata$year_id)
sex_ids  <- unique(metadata$sex_id)
age_group_ids = unique(metadata$age_group_id)

## Pull in HF etiology map and merge with HF IDs
composite <- fread(paste0(cvd_path, '/composite_cause_list.csv'))
composite[is.na(composite_id), composite_id := cause_id]
composite <- merge(composite, IDs, by.x="composite_id", by.y="cause_id", all=T)

age_groups <- get_age_metadata(12, gbd_round_id = gbd_round_id)
locations <- get_location_metadata(9, gbd_round_id = gbd_round_id)

###### Pull in CSMR
#################################################################################

## Necessary columns
group_cols = c('cause_id', 'sex_id', 'age_group_id', 'location_id', 'year_end')
measure_cols = c('mean_death', 'upper_death', 'lower_death')

## Get CSMR for all non-composite cause
COD_CAUSE_IDS <- composite[!(is.na(cause_id)), unique(cause_id)]

if (faux_correct) { 
  
  df <- get_outputs(process_version_id = "VALUE", 
                    topic = 'cause', 
                    measure_id = 1,
                    metric_id = 3,
                    cause_id = COD_CAUSE_IDS,
                    gbd_round_id = 6,
                    decomp_step = decomp_step, 
                    location_id = loc_id, 
                    year_id = year_ids, 
                    age_group_id = age_group_ids,
                    sex_id = sex_ids)
  
} else {
  
  df <- get_outputs(process_version_id = "VALUE", 
                    topic = 'cause', 
                    measure_id = 1,
                    metric_id = 3,
                    cause_id = COD_CAUSE_IDS,
                    gbd_round_id = 6,
                    decomp_step = decomp_step, 
                    location_id = loc_id, 
                    year_id = year_ids, 
                    age_group_id = age_group_ids,
                    sex_id = sex_ids)
}

## Remove NA values (age restrictions)
df <- df[!(is.na(val)) & year_id %in% year_ids & measure_id==1]

## Numeric columns should be 0 if empty
for (col in c("val", "upper", "lower")) df[is.na(get(paste0(col))), paste0(col) := 0]

## Collapse into 1990-last estimation year
pop <- get_population(location_id = loc_id, year_id = year_ids, age_group_id = age_group_ids, sex_id = sex_ids, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
df <- merge(df, pop, by=c("age_group_id", "year_id", "location_id", "sex_id"))

df[, `:=` (cs_deaths = val*population, cs_deaths_upper = upper*population, cs_deaths_lower = lower*population)]
df[, `:=` (sum_cs_deaths = sum(cs_deaths), sum_cs_deaths_upper = sum(cs_deaths_upper), sum_cs_deaths_lower = sum(cs_deaths_lower)), by=c("age_group_id", "sex_id", "location_id", "cause_id")]
df[, sum_population := sum(population), by=c("age_group_id", "location_id", "sex_id", "cause_id")]
df[, `:=` (csmr_sum = sum_cs_deaths/sum_population, csmr_sum_high = sum_cs_deaths_upper/sum_population, csmr_sum_low = sum_cs_deaths_lower/sum_population)]
df[, csmr_sum_se := (csmr_sum_high - csmr_sum_low)/(2*1.96)]

df <- merge(df, composite, by=c("cause_id", "cause_name", "acause"), all.x=T, all.y=F)
df <- merge(df, age_groups, by=c("age_group_id", "age_group_name"), all.x=T, all.y=F)
df <- merge(df, locations, by=c("location_id", "location_name"), all.x=T, all.y=F)

df[, sex := ifelse(sex_id==1, "Male", "Female")]

df <- unique(df[, .(age_group_id, location_id, sex_id, cause_id, acause, cause_name, location_name, sex, csmr_sum, csmr_sum_se)])

df[, year_start := 1990]
df[, year_end := max(year_ids)]

# Fill in missing values for any age-sex-cause-location-year combination with zeros
square <- expand.grid(age_group_id=unique(df$age_group_id), sex_id=unique(df$sex_id), cause_id=unique(df$cause_id),
                      location_id=unique(df$location_id), year_end=unique(df$year_end), year_start=unique(df$year_start))

df <- as.data.table(merge(df, square, by=c("age_group_id", "sex_id", "cause_id", "location_id", "year_end"), all=T))

for (col in c("csmr_sum", "csmr_sum_se")) df[is.na(get(paste0(col))), paste0(col) := 0]

#######################################
## 4. Save output to 0_codcorrected_deaths folder to be read back in the main script
#######################################

file <- paste0(outputDir, '/codcorrect_', loc_id, '.rds')
saveRDS(object = df, file = file)
print("Done!")


