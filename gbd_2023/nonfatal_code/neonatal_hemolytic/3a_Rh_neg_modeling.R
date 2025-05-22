################################################################################
## Purpose:   Prep Rh negativity prevalence bundle for dismod
## Input:     Adding new data (mostly blood bank website data) to bundle 389,
##            including data in bundle version 23615
## Output:    A prepared crosswalk version for dismod MEID:24625
################################################################################

# setup ------------------------------------------------------------------------

library(openxlsx)
library(data.table)
`%>%` <- magrittr::`%>%`
bundle_id = 389

# clear bundle -----------------------------------------------------------------

bundle389 <- ihme::get_bundle_data(bundle_id = 389)
seq <- bundle389[,.(seq)]
path_to_clear_data <- 'FILEPATH'
openxlsx::write.xlsx(sheetName = "extraction", x = seq, file = path_to_clear_data)
result <- ihme::upload_bundle_data(bundle_id, filepath=path_to_clear_data)

# prep new data for upload ---------------------------------------------------------

#note: discussed inputting age start age end as 0 and 99; assuming sample size of 10000 for blood bank sources
rhneg <- openxlsx::read.xlsx(
  "FILEPATH",
  sheet = "extraction"
)
rhneg <- rhneg[-1, ]
rhneg$mean <- as.numeric(rhneg$mean)
rhneg <- rhneg %>%
  filter(!is.na(mean))
rhneg$year_start <- 2023
rhneg$year_end <- 2023
rhneg$representative_name <- "Unknown"
rhneg$urbanicity_type <- "Unknown"
rhneg$recall_type <- "Point"
rhneg$unit_type <- "Person"
rhneg$unit_value_as_published <- 1
rhneg$location_name <- sub("\\|.*", "", rhneg$location_name)
rhneg$seq <-
  seq(from = 1,
      by = 1,
      length.out = nrow(rhneg)) #reassigning the seq values for bundle so it starts from 1

# upload new data to bundle ----------------------------------------------------

bundle_id <- 389
upload_filepath <-
  'FILEPATH'
openxlsx::write.xlsx(rhneg, file = upload_filepath, sheetName = 'extraction')
result <- ihme::upload_bundle_data(bundle_id, filepath=upload_filepath)

# adjustments to last round bundle version -------------------------------------

# discussed we're changing the age start and age end to 0 and 99, respectively for all sources
bv <- ihme::get_bundle_version(bundle_version_id = 23615)
setDT(bv)
bv$age_start <- 0
bv$age_end <- 99
bv$note_SR[bv$nid == 402213] <- "age start and age end entered as 0 to 99"
bv$note_SR[bv$nid == 402243] <- "age start and age end entered as 0 to 99"
bv$sex <- "Both"
bv$is_outlier[bv$nid == 402224] <- 1 # outlier saudi arabia (too high)
bv$is_outlier[bv$nid == 145738] <- 1 # outlier cameroon (too high)
bv <- bv %>%
  filter(!(location_name == "Trinidad and Tobago")) # remove Trinidad and Tobago - data is too old for dismod
bv$seq <-
  seq(from = 25,
      by = 1,
      length.out = nrow(bv)) #just to start from the next seq number in the bundle

# upload adjusted data to bundle -----------------------------------------------

upload_filepath <-
  'FILEPATH'
openxlsx::write.xlsx(bv, file = upload_filepath, sheetName = 'extraction')
result <- ihme::upload_bundle_data(bundle_id, filepath=upload_filepath)

# save bundle version and automatic crosswalk ----------------------------------
result <- ihme::save_bundle_version(bundle_id, automatic_crosswalk = TRUE)