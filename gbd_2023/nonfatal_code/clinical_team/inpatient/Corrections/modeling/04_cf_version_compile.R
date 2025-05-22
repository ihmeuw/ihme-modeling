USER = Sys.info()[7]
library(ggplot2)
library(stats)
library(data.table)

source('~/db_utilities.R')
source("~/db_queries.R")

## STEP ONE: Create metadata for version set ####
cf_version_set_id = 6

write_folder = paste0('FILEPATH',cf_version_set_id,'/')
if(!file.exists(write_folder)){
  dir.create(write_folder, showWarnings = FALSE, recursive = TRUE)
}
### Metadata for cf version set id ####
version_metadata = data.table(cf_version_set_id = c(cf_version_set_id), 
                              cf_version_set_name= c("GBD results 2023"), 
                              cf_version_set_description=c("All data the same as last version set with the exception of bundle 6716"))
write.csv(version_metadata, paste0(write_folder, 'cf_version_set_db_update.csv'), row.names = FALSE)

active_bundles_cfs = copy(all_cf_bundles)
datetime_default = 'MODEL_ID1'
datetime_sub = 'MODEL_ID2'

version_metadata_bundle = data.table(
  cf_version_set_id = cf_version_set_id,
  bundle_id = unique(active_bundles_cfs$bundle_id),
  cf_version_id = NA,
  cf_version_date = datetime_default
)

write.csv(version_metadata_bundle, paste0(write_folder, 'cf_version_set_bundle_db_update.csv'), row.names = FALSE)

## STEP TWO: 
# If the datetime (model version) varies by bundle, we will need to use the above as the reference for which version to pull 
# results from and then update the code below to pull results differentially by bundle

# read in scalar results for the version set
read_dates = unique(version_metadata_bundle$cf_version_date)
scalar_results = rbindlist(lapply(read_dates, function (x) {
  df = readRDS(paste0('FILEPATH', x, '/scalar_results_draws.rds'))
  df[, cf_version_date := x]
  return(df)
}))

# merge in the metadata to keep only the correct version dates
scalar_results = merge(scalar_results, version_metadata_bundle[, .(bundle_id, cf_version_date)], by = c('bundle_id', 'cf_version_date'))

## VALIDATIONS ####
#### Create a validation_df with all available bundle_ids, age_group_ids, and sex_ids ####
ages = get_age_metadata(release_id = 16) %>%
  setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
age_1 = data.table(age_group_id = 28, age_start = 0, age_end = 1)
ages = ages %>% rbind(age_1, fill = TRUE)

validation_df = as.data.table(expand.grid(age_group_id = ages$age_group_id, bundle_id = unique(active_bundles_cfs$bundle_id), sex_id = c(1,2)))
validation_df = merge(validation_df, ages[,.(age_group_id, age_start, age_end)], by = 'age_group_id')

### Get age sex restrictions ####
rests = get_bundle_restrictions() %>% as.data.table()
rests = melt(rests, id.vars = c('bundle_id','yld_age_start','yld_age_end','map_version'), variable.name = 'sex', value.name = 'sex_present')
rests[sex == 'male', sex_id := 1][sex == 'female', sex_id := 2]

### Apply age restrictions to this table ####
validation_df = merge(validation_df, rests, by = c('bundle_id','sex_id'))
validation_df = validation_df[sex_present != 0]
validation_df = validation_df[age_start >= yld_age_start,]

validation_df[, rm := 0]
validation_df[(yld_age_end >= 1) & (age_start > yld_age_end), rm := 1]  # this appears to be applying the upper bound age restriction 
validation_df = validation_df[rm != 1]
validation_df[(yld_age_end < 1) & age_start >= 1, rm := 1] 
validation_df = validation_df[rm != 1]
validation_df[, rm := NULL]
validation_df[, present := 1]

all_buns_ages_sexes_in_data = unique(scalar_results[,.(bundle_id, age_group_id, sex_id)])

check = merge(unique(all_buns_ages_sexes_in_data), unique(validation_df[, .(bundle_id, age_group_id, sex_id, present)]), all = TRUE, by = c('bundle_id', 'age_group_id', 'sex_id'))
if(nrow(check[present == 1,]) != nrow(all_buns_ages_sexes_in_data)) stop ('Missing bundles, ages, or sexes!')

# get the rows that are missing above
all_buns_ages_sexes_in_data[, check := 1]
check2 = merge(check, all_buns_ages_sexes_in_data, all=TRUE)
check3 = check2[is.na(check)]

# split out by age group id and sex id 
results_draws_sex = split(scalar_results, scalar_results$sex_id) 
results_draws_sex_age = lapply(results_draws_sex, function (x) {split(x, x$age_group_id)}) 

write_folder = paste0('FILEPATH', cf_version_set_id,'/draws/')
if(!file.exists(write_folder)){
  dir.create(write_folder, showWarnings = FALSE, recursive = TRUE)
}

lapply(results_draws_sex_age, function(results_by_sex){
  lapply(results_by_sex, function(x){
    sex = unique(x$sex_id)
    age_group = unique(x$age_group_id)
    fwrite(x, paste0(write_folder,age_group, '_', sex,'.csv'))
  })
})
