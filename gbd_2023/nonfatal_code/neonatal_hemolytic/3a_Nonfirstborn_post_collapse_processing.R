##############################################################################
## Purpose:   Processing nonfirstborn prevalence data for uploading to bundle
##            per stgpr validations
## Input:     Collapsed survey microdata created from collapse_nonfirstborn.R
## Output:    Extraction sheet to be used for upload bundle data.R
##############################################################################

# Setup ------------------------------------------------------------------------
'%>%' <- magrittr::'%>%'
age_df <- ihme::get_age_metadata(release_id = 16)
locations <- ihme::get_location_metadata(location_set_id = 22, release_id = 16) 
locations <- locations[,.(ihme_loc_id, location_id)]

# read in files ----------------------------------------------------------------
collapsed_jdrive <- data.table::fread(file = 'FILEPATH')
collapsed_jdrive2 <- data.table::fread(file = 'FILEPATH')
collapsed_limiteduse <- data.table::fread(file = 'FILEPATH')
collapsed_limiteduse2 <- data.table::fread(file = 'FILEPATH')

# binding files ----------------------------------------------------------------
collapsed <- data.table::rbindlist(list(collapsed_jdrive, collapsed_limiteduse, collapsed_jdrive2, collapsed_limiteduse2),
                                   use.names=TRUE)

# Reshape collapsed file -------------------------------------------------------
collapsed <- collapsed[mean >= 0] # dropping proportions with -1 mean. This indicates the code parsed through the module for usable variables and did not find any.
wra_age_groups <- c(7:15) # all of the age groups for women of reproductive age 10-54 years old
collapsed <- collapsed[mom_age_group_id %in% wra_age_groups] # dropping age groups for women not of reproductive age
collapsed <- collapsed[child_birth_year > 0] # drop where child birth year couldn't be identified

# Processing -------------------------------------------------------------------
#adding age_start and age_end columns
collapsed <- dplyr::left_join(collapsed,
                              age_df,
                              by = c("mom_age_group_id" = "age_group_id")
)

#cleaning the ihme_loc_id column before merge
collapsed$ihme_loc_id <- gsub("[\n ]", "", collapsed$ihme_loc_id)
collapsed <- dplyr::left_join(collapsed,
                              locations
)

#removing Turks and Caicos, Phl (Garbage_PHL in extraction), old Ethiopia loc (44858),
#and problematic India locs (from the same NIDs the detailed locs are being included)
collapsed <- collapsed[!is.na(location_id)]

#removing DHS Malaria Indicator Surveys - raw data is not capturing birth order plausibly
collapsed <- collapsed %>%
  dplyr::filter(!nid %in% c(
    157059, #malawi
    77387, #malawi
    413934, #mozambique
    69806, #madagascar
    108080, #burundi
    350836, #rwanda
    77391, #rwanda
    188785, #burkina faso
    56828, #liberia
    218587, #mali
    11516 #senegal
  ))

#removing Special DHS Dominican Republic Surveys as they are not nationally representative
collapsed <- collapsed %>%
  dplyr::filter(!nid %in% c(
    165645, #DR Special DHS 2013
    21198 #DR Special DHS 2007
  ))

# remove duplicate rows
collapsed <- collapsed %>%
  dplyr::distinct()

# validations for STGPR --------------------------------------------------------
# setting year_id same as year_start and year_end because we're looking at the births of that year. FYI VR data already does this.
data.table::setnames(collapsed, 'child_birth_year', 'year_id')
collapsed$year_id <- as.integer(collapsed$year_id)
collapsed$year_start <- collapsed$year_id
collapsed$year_end <- collapsed$year_id

data.table::setnames(collapsed, 'mom_age_group_id', 'age_group_id')
data.table::setnames(collapsed, 'mean', 'val')
collapsed[, measure := 'proportion']
collapsed[, variance := ((val * (1 - val)) / sample_size) + ((1.96 ^ 2) /
                                                               (4 * (sample_size ^ 2)))]
collapsed[, sex := 'Both']
collapsed[, underlying_nid := NA]
collapsed[, seq := NA]
collapsed$is_outlier <- 0

# save bundle data to snfs -----------------------------------------------------
upload_filepath <-
  'FILEPATH'
openxlsx::write.xlsx(collapsed, file = upload_filepath, sheetName = 'extraction')

# OUTLIERING AND SAVE ##########################################################
collapsed <- openxlsx::read.xlsx('FILEPATH')
# outliering end age groups
collapsed[age_group_id == 15 & val < 0.25, is_outlier := 1]
collapsed[age_group_id == 7 & val > 0.75, is_outlier := 1]

# save bundle data to snfs -----------------------------------------------------
upload_filepath <-
  'FILEPATH'
openxlsx::write.xlsx(collapsed, file = upload_filepath, sheetName = 'extraction')