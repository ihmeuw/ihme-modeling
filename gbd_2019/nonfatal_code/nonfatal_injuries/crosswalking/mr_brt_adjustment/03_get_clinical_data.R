# load shared functions
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')

decomp_step <- 'step4'

# these IDs are for the bundles in which the newly audited data are stored
bundle_df <- read.csv('FILEPATH')
bundles <- bundle_df$bundle_id
names <- bundle_df$acause_name

df <- data.frame(bundle_id = numeric(),
                 bundle_version = numeric(),
                 acause_name = character())

for (i in 1:length(bundles)) {
  if (bundles[i] == 640) {
    # save a bundle version
    result <- save_bundle_version(bundles[i], decomp_step, include_clinical = FALSE)
  } else {
    # save a bundle version
    result <- save_bundle_version(bundles[i], decomp_step, include_clinical = TRUE)
  }
    
  print(paste0('saved bundle version for ', bundles[i]))
  version <- result$bundle_version_id
    
  # Add the bundle and it's version to our df of bundles and versions
  newdf <- data.frame(bundle_id = bundles[i], version = as.numeric(version), acause_name = names[i])
  df <- rbind(df, newdf)
  
  if (bundles[i] != 640) {
    # pull bundle data plus clinical data
    data <- get_bundle_version(bundle_version_id = version, export = TRUE)
    # get and save only clinical data
    ci_data <-
      subset(data,
             source_type %in% c('Facility - inpatient', 'Facility - outpatient'))
  
    write.csv(
      ci_data,
      paste0(
        'FILEPATH',
        as.character(bundles[i]),
        '.csv'
      )
    )
    print(paste0('saved clinical data for ', bundles[i]))
  }
}

write.csv(df,
         'FILEPATH', row.names = FALSE)
