
rm(list = ls())

code_root <-"FILEPATH"
data_root <- "FILEPATH"

library(data.table)
library(openxlsx)
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source(paste0(code_root, "FILEPATH/processing.R")) # function to make zero draw files
source(paste0(code_root, "FILEPATH/utils.R")) #custom ebola functions
 
bundle_version <- save_bundle_version(bundle_id = 'ADDRESS', release_id = 'ADDRESS')
bundle_version_data <- get_bundle_version(bundle_version_id = 'ADDRESS', fetch = 'all')
bundle_version_data[, crosswalk_parent_seq := NA]

bundle_version_data[sex == 'Male', sex_id := 1]
bundle_version_data[sex == 'Female', sex_id := 2]
bundle_version_data[sex == 'Both', sex_id := 3]

agesplit_data <- copy(bundle_version_data[value_age_spec == 1])
ssa_props <- calc_as_proportion(data = agesplit_data)
sfa_props <- calc_ss_proportion(data = agesplit_data, s_id = 2)
sma_props <- calc_ss_proportion(data = agesplit_data, s_id = 1)

all_aa       <- copy(bundle_version_data[sex_id == 3 & value_age_group_id == 22])
as_split     <- as_age_split(all_age_data = all_aa, all_age_proportions = ssa_props)
as_split[, sex := ifelse(sex_id == 1, 'Male', 'Female')]

male_aa      <- copy(bundle_version_data[sex_id == 1 & value_age_group_id == 22])
male_split   <- ss_age_split(ss_all_age_data = male_aa, ss_age_proportions = sma_props)

female_aa    <- copy(bundle_version_data[sex_id == 2 & value_age_group_id == 22])
female_split <- ss_age_split(ss_all_age_data = female_aa, ss_age_proportions = sfa_props)  

bundle_version_data[, proportions := NA]

#combine all splits
ebola_surv_split  <- rbind(female_split, as_split, male_split, bundle_version_data, fill = TRUE)
ebola_surv_split <-  ebola_surv_split[value_age_group_id != 22] 

ebola_surv_split[, crosswalk_parent_seq := seq]
ebola_surv_split[, seq := NA]

# save crosswalk
write.xlsx(ebola_surv_split, 'FILEPATH', sheetName = 'extraction')

save_crosswalk_version(bundle_version_id = ADDRESS, 
                       data_filepath = 'FILEPATH', 
                       description = 'XXX'
                       )
