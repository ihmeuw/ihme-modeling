################################################################################
## Purpose: Process UN Demographic Yearbook 2022 (DYB) Data on
##          birth prevalence for use in nonfirstborn modeling for
##          neonatal hemolytic disease. Updated to include mother's age as variable.
## Input:   UN DYB 2022 Tabulated Data from data.un.org
##          last downloaded on September 25, 2023
## Output:  A prepared dataframe uploading to bundle 7667
################################################################################

# PROCESSING SECTION ###########################################################
# SETUP ------------------------------------------------------------------------
library(data.table)
library(reshape2)
library(openxlsx)
library(dplyr)

# PREP LOCATIONS ---------------------------------------------------------------
locations <- ihme::get_location_metadata(location_set_id = 22, release_id = 16)
locations <- locations[,.(location_name,location_id,ihme_loc_id)]
locations <- locations[ihme_loc_id != 'USA_533'] # removing the US state of Georgia so that it doesn't get mapped to the country of Georgia

# UPLOAD PROCESSING FUNCTION ---------------------------------------------------
process_data <- function(i) {
  
  # reading in the files from the bundle directory
  dt <- fread(paste0(input_dir, dir_list[i]), fill = TRUE)
  dt[,V1:=NULL] # delete extra column
  dt[,Area:=NULL] # delete Area column - values are all "Total"
  
  # separating out footnotes section
  footnotes_key <- dt[is.na(Value)]
  footnotes_key <- footnotes_key[3:nrow(footnotes_key), c('Country or Area', 'Year')]
  setnames(footnotes_key, c('Country or Area','Year'), c('key','note_text'))
  footnotes_key$key <- as.integer(footnotes_key$key)
  
  # removing footnotes section from dt and renaming "Value Footnotes" to "key"
  dt <- dt[!is.na(Value)]
  setnames(dt, 'Value Footnotes', 'key')
  
  # label unique combos of location and year - this we can use for underlying NIDs
  labels <- dt[, .N, by = .(`Country or Area`, Year)]
  labels[, source_id := .I]
  dt <- merge(dt, labels, by = c('Year','Country or Area'), all.x = TRUE)
  
  # Change column names to GBD names
  setnames(dt, 'Country or Area', 'location_name')
  setnames(dt, 'Birth order', 'bord')
  
  # group this data into the maternal age groups of 10 to 54
  dt[Age %in% c('10 - 14','14-Oct','10','11','12','13','14','0 - 14','0 - 15', '13 - 14', '15-Oct'), age_group_id := 7]
  dt[Age %in% c('15 - 19', '16 - 19','15','16','17','18','19'), age_group_id := 8]
  dt[Age %in% c('20 - 24','20','21','22','23','24'), age_group_id := 9]
  dt[Age %in% c('25 - 29','25','26','27','28','29'), age_group_id := 10]
  dt[Age %in% c('30 - 34','30','31','32','33','34'), age_group_id := 11]
  dt[Age %in% c('35 - 39','35','36','37','38','39'), age_group_id := 12]
  dt[Age %in% c('40 - 44','40','41','42','43','44'), age_group_id := 13]
  dt[Age %in% c('45 - 49','45','46','47','48','49'), age_group_id := 14]
  dt[Age %in% c('50 +', '50', '50 - 54'), age_group_id := 15]
  dt[Age %in% c('40 +', '45 +', '35 +', '10 +', '0 - 19', '0 - 24', '16 +', '15 - 49'), age_group_id := 8888] # this is for the wide age ranges
  dt[Age %in% c('Unknown'), age_group_id := 9999] # grouping unknowns
  
  # removing the wide age ranges because it is not usable
  dt <- dt[(age_group_id %in% c(8888)) == FALSE]
  
  # removing the 'Total" rows for Age
  dt <- dt[(Age %in% c('Total')) == FALSE]
  
  # check to see every row has an age_group_id assignment
  testthat::test_that("All ages have an age group ID", {
    testthat::expect_equal(
      nrow(dt[is.na(age_group_id), .N, by = .(Age)]),
      0
    )
  })
  testthat::test_that("All sources have an age group ID", {
    testthat::expect_equal(
      nrow(dt[is.na(age_group_id), .N, by = .(source_id)]),
      0
    )
  })
  
  # collapse by age_group_id because some 'Age' columns are split within the age_group_id
  dt[, `:=`(Value = sum(.SD$Value)),
     by=c('Year', 'location_name', 'age_group_id', 'bord', 'Source Year', 'source_id')] #sums the Value column for each age group id
  dt_age_bins <- unique(dt[, c('Year', 'location_name', 'age_group_id', 'bord', 'Record Type', 'Reliability', 'Source Year', 'source_id','Value','key')])
  dt_age_bins$Value <- as.integer(dt_age_bins$Value)
  
  # create notfirst_types column
  bord_types <- unique(dt$bord)
  notfirst_types <- bord_types[bord_types != 'First' & bord_types != 'Total']
  
  dt_wide <- tidyr::pivot_wider(
    dt_age_bins,
    names_from = "bord",
    values_from = "Value"
  ) |>
    data.table::setDT()
  
  #validate that the sum of the nonfirstborns equals total - firstborns
  dt_wide[, not_first := rowSums(.SD, na.rm=T), .SDcols = notfirst_types]
  dt_wide[, total_check := rowSums(.SD, na.rm=T), .SDcols = c('First', 'not_first')]
  
  # drop rows where the total of the first and not firstborns are not equal to the Total
  dt_wide <- dt_wide[total_check == Total]
  
  # keep rows where unknown is less than 0.1% of the number of firstborns
  dt_wide[is.na(Unknown), Unknown := 0]
  dt_wide[, unknown_percent := (Unknown / Total) * 100]
  source_count <- dt_wide[unknown_percent > 1, .N, by = .(location_name, Year)]
  nrow(source_count)
  source_count <- dt_wide[, .N, by = .(location_name, Year)]
  nrow(source_count)
  dt_wide <- dt_wide[is.na(Unknown) | Unknown == 0 | unknown_percent < 1]
  
  # calculate proportion not firstborn section
  dt_wide[, not_firstborn := not_first / Total]
  # including 0s if both Total and not_first are 0. Otherwise, the returned values are NaN.
  dt_wide$not_firstborn <- ifelse(dt_wide$Total == 0 & dt_wide$not_first == 0, 0, dt_wide$not_firstborn)
  
  # test
  testthat::test_that("All rows have a not_firstborn proportion", {
    testthat::expect_equal(
      nrow(dt_wide[is.na(not_firstborn)]),
      0
    )
  })
  
  # setting the sample size
  dt_wide <- dt_wide %>%
    group_by(location_name, Year) %>%
    mutate(sample_size = sum(Total))
  setDT(dt_wide)
  # drop the rows where we have a zero sample size
  dt_wide <- dt_wide[sample_size>0]
  
  # test
  testthat::test_that("All rows have a sample size value that is not NA and greater than zero", {
    testthat::expect_equal(
      nrow(dt_wide[is.na(sample_size) | sample_size <= 0]),
      0
    )
  })
  
  # match location_names to location_ids
  setDT(dt_wide)
  dt_wide[location_name == 'China, Macao SAR', location_name := 'Macao Special Administrative Region of China']
  dt_wide[location_name == 'China, Hong Kong SAR', location_name := 'Hong Kong Special Administrative Region of China']
  dt_wide[location_name == 'United Kingdom of Great Britain and Northern Ireland' | location_name == 'Falkland Islands (Malvinas)',
          location_name := 'United Kingdom']
  
  dt_wide <- merge(dt_wide, locations[,.(location_id, location_name, ihme_loc_id)], by = 'location_name', all.x = TRUE)
  
  # dropping locations we don't extract
  territories_we_dont_extract <- c(
    'Faroe Islands',
    'French Guiana',
    'Martinique',
    'Reunion',
    'Montserrat',
    'Åland Islands',
    'Jersey',
    'Guadeloupe',
    'British Virgin Islands',
    'Gibraltar',
    'New Caledonia',
    'Norfolk Island',
    'Cayman Islands',
    'Curaçao',
    'French Polynesia',
    'Anguilla',
    'Liechtenstein',
    'Saint Barthélemy',
    'Saint Pierre and Miquelon',
    'Saint-Martin (French part)'
  )
  
  dt_wide <- dt_wide[(location_name %in% territories_we_dont_extract)==FALSE]
  
  # check locations that didn't get assigned an ihme_loc_id
  dt_wide[is.na(ihme_loc_id)]
  
  dt_wide <- dt_wide[,.(location_name, Year, age_group_id, source_id, Total, not_firstborn, location_id, ihme_loc_id, sample_size)]
  
  return(dt_wide)
}

# RUN --------------------------------------------------------------------------
input_dir <- ('FILEPATH')
dir_list <- list.files(input_dir)

dt1 <- process_data(i = 1)
dt2 <- process_data(i = 2)
dt3 <- process_data(i = 3)

# binding
dt <- rbind(dt1, dt2, dt3, use.names = T)

# ADDING NIDS SECTION ##########################################################
# add NID ----------------------------------------------------------------------
dt$nid <- 534531

# PREPARING FOR STGPR BUNDLE UPLOAD ############################################
# Adding age_start and age_end columns
age_df <- ihme::get_age_metadata(release_id = 16)
dt <- merge.data.table(dt,age_df,by.x = "age_group_id",by.y = "age_group_id")

# renaming columns to be able to bind with collapsed files in post_collapse_processing.R
setnames(dt, 'not_firstborn', 'val')
setnames(dt, 'Year', 'year_id')
dt$year_id <- as.integer(dt$year_id)
dt$year_start <- dt$year_id
dt$year_end <- dt$year_id
dt[, underlying_nid := NA] #TEMP update with underlying nids
dt[, seq := NA]
dt$is_outlier <- 0
dt$sex <- 'Both'
dt$measure <- 'proportion'

dt[, standard_error := nch::wilson_score_interval_se(val, sample_size)]
dt[, variance := standard_error^2]
dt$standard_error <- NULL

# SAVING FILE FOR BUNDLE UPLOAD ################################################
upload_filepath <- 'FILEPATH'
write.xlsx(dt, file = upload_filepath, sheetName = 'extraction')

# OUTLIERING ###################################################################
# outlier terminal age groups
dt <- openxlsx::read.xlsx("FILEPATH")
data.table::setDT(dt)
dt <- dt[age_group_id == 15 & val < 0.25, is_outlier := 1]
dt <- dt[age_group_id == 7 & val > 0.75, is_outlier := 1]
# outlier small sample sizes
dt <- dt[sample_size < 30, is_outlier := 1]

# add underlying NIDs ##########################################################
# process underlying nids csv file downloaded from GHDx NID 534531 Reference tab
underlying_nids <- data.table::fread(
  "FILEPATH"
)
underlying_nids <- underlying_nids %>%
  dplyr::mutate(
    location_name = stringr::str_extract(`To Title`, "^(.*?)(?=\\sVital)"),
    year_id = stringr::str_extract(`To Title`, "\\d{4}$"),
    field_citation_value = "United Nations Statistics Division (UNSD). UNSD Demographic Statistics - Live Births by Birth Order and Age of Mother. New York City, United States of America: United Nations Statistics Division (UNSD)."
  ) %>%
  dplyr::rename(
    underlying_nid = "To Source NID",
    nid = "From Dataset NID",
    underlying_field_citation_value = "To Title",
  ) %>%
  dplyr::mutate(
    underlying_field_citation_value = paste0(underlying_field_citation_value, ".")
  ) %>%
  dplyr::select(!c("From Title"))

# change to ihme location names
underlying_nids[location_name == 'Macao', location_name := 'Macao Special Administrative Region of China']
underlying_nids[location_name == 'Hong Kong', location_name := 'Hong Kong Special Administrative Region of China']
underlying_nids[location_name == 'United Kingdom - Northern Ireland', location_name := 'Northern Ireland']
underlying_nids[location_name == 'Falkland Islands', location_name := 'Northern Ireland']

# additional underlying nids
file2 <- data.table::fread(
  "FILEPATH"
)
file2 <- file2 %>%
  dplyr::mutate(
    location_name = stringr::str_extract(`Title`, "^(.*?)(?=\\sVital)"),
    year_id = stringr::str_extract(`Title`, "\\d{4}$"),
    field_citation_value = "United Nations Statistics Division (UNSD). UNSD Demographic Statistics - Live Births by Birth Order and Age of Mother. New York City, United States of America: United Nations Statistics Division (UNSD).",
    nid = 534531
  ) %>%
  dplyr::rename(
    underlying_field_citation_value = "Suggested citation",
    underlying_nid = "underlying nid"
  ) %>%
  dplyr::select(c("location_name", "year_id", "field_citation_value", "underlying_field_citation_value", "nid", "underlying_nid"))
setDT(file2)
# change to ihme location names
file2[location_name == 'Republic of San Marino', location_name := 'San Marino']

underlying_nids_fulllist <- rbind(
  underlying_nids,
  file2
)
underlying_nids_fulllist <- underlying_nids_fulllist %>%
  dplyr::mutate(year_id = as.integer(year_id))

# merge
dt <- dt %>%
  dplyr::select(!underlying_nid)
dt <- merge(
  dt,
  underlying_nids_fulllist,
  by = c("nid", "location_name", "year_id"),
  all.x = TRUE,
  all.y = FALSE
)
setDT(dt)

# check to see every row has an underlying nid
testthat::test_that("All rows have an underlying nid", {
  testthat::expect_equal(
    nrow(dt[is.na(underlying_nid)]),
    0
  )
})

# save file ####################################################################
upload_filepath <- 'FILEPATH'
openxlsx::write.xlsx(dt, file = upload_filepath, sheetName = 'extraction')