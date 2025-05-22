## ----------------------- SAVE BUNDLE VERIONS (OPTIONAL) ------------------------------

source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/get_bundle_data.R")

#main cases bundle (both all age and age specific data).
bundle_data<- get_bundle_data(bundle_id = ADDRESS , decomp_step = 'ADDRESS', export = TRUE)

NCC_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step='ADDRESS', include_clinical = FALSE) # returns a bundle_version_id



#age specific bundle 
age_spec_bundle_data<- get_bundle_data(bundle_id = ADDRESS, decomp_step = 'ADDRESS', export = TRUE)

NCC_age_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step='ADDRESS', include_clinical = FALSE) # returns a bundle_version_id

