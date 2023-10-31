# team: GBD Injuries
# script: save a snapshot of the xwalked data so you can launch a DisMod model
library(openxlsx)

source('FILEPATH/save_crosswalk_version.R')

bundle <- commandArgs()[6]
version <- commandArgs()[7]
name <- commandArgs()[8]
description <- commandArgs()[9]
repo <- commandArgs()[10]

print(paste(bundle, version, name, description, repo))

# read in clinical outlier file
clinical_outliers <- read.xlsx('FILEPATH')
clinical_outliers <- clinical_outliers[(clinical_outliers$bundle_id==bundle) & (clinical_outliers$is_outlier==TRUE),]
colnames(clinical_outliers)[colnames(clinical_outliers) == 'is_outlier'] <- 'to_outlier'
clinical_outliers$sex <- ifelse(clinical_outliers$sex_id==1, 'Male',
                                ifelse(clinical_outliers$sex_id==2, 'Female', 'Both'))
clinical_outliers <- clinical_outliers[, c('sex', 'location_id', 'year_start', 'year_end', 'nid', 'age_start', 'to_outlier')]

# read in crosswalk sheet
data <- read.xlsx(
  paste0(
    repo,
    'adjusted_data/',
    as.character(bundle),
    '_',
    name,
    '_adjusted.xlsx'
  )
)

# rows that are merged get outliered
data <- merge(data, clinical_outliers, by = c('sex', 'location_id', 'year_start', 'year_end', 'nid', 'age_start'), all.x = TRUE)
data[!is.na(data$to_outlier), 'is_outlier'] <- 1
data$to_outlier <- NULL

# save a new crosswalk sheet with outliers
filepath <- paste0(repo,
                  'outliered_data/',
                  as.character(bundle),
                  '_',
                  name,
                  '_outliered.xlsx')
write.xlsx(data, filepath, sheetName = 'extraction', row.names = FALSE)

# save crosswalk version to epi database
result <- save_crosswalk_version(
  bundle_version_id = version,
  data_filepath = filepath,
  description = description
)

