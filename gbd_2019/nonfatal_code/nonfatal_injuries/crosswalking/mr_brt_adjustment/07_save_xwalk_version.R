source('FILEPATH/save_crosswalk_version.R')

bundle <- commandArgs()[6]
version <- commandArgs()[7]
name <- commandArgs()[8]

print('starting script')

bundle_version_id <- version

data_filepath <-
  paste0(
    'FILEPATH',
    as.character(bundle),
    '_',
    name,
    '_adjusted.xlsx'
  )

description <- 'step 4 crosswalks'

result <- save_crosswalk_version(
  bundle_version_id = bundle_version_id,
  data_filepath = data_filepath,
  description = description
)

