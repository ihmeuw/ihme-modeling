# team: GBD Injuries
# project: Crosswalking for GBD 2020
# script: getting clinical data for outpatient crosswalk

# load shared functions
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')

decomp_step <- 'iterative'
gbd_round <- 7

# these IDs are for the bundles in which the newly audited data are stored
bundle_df <- read.csv('FILEPATHS/bundle_versions.csv')
bundle_df <- bundle_df[!is.na(bundle_df$save),]
bundles <- bundle_df$bundle_id

for (i in 1:length(bundles)) {
  # save a bundle version
  result <- save_bundle_version(bundles[i], decomp_step, gbd_round, include_clinical=c('inpatient', 'outpatient'))
    
  # note down the version
  print(paste0('saved bundle version for ', bundles[i]))
  version <- result$bundle_version_id
  print(version)
  
  # pull bundle data plus clinical data
  data <- get_bundle_version(bundle_version_id = version, export = FALSE, fetch = 'all')
  
  # get and save only clinical data
  ci_data <-
    subset(data,
           clinical_data_type %in% c('inpatient', 'outpatient'))

  write.csv(
    ci_data,
    paste0(
      'FILEPATH/bundle_',
      as.character(bundles[i]),
      '.csv'
    ),
    row.names = FALSE
  )
  print(paste0('saved clinical data for ', bundles[i]))
}