## ----------------------- SAVE BUNDLE VERIONS (OPTIONAL) ------------------------------

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

#main cases bundle (both all age and age specific data). This is used in DISMOD NCC model
bundle_data<- get_bundle_data(bundle_id = ADDRESS , decomp_step = 'iterative', export = TRUE)

NCC_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step='iterative', include_clinical = FALSE) # returns a bundle_version_id

#age specific bundle 
age_spec_bundle_data<- get_bundle_data(bundle_id = ADDRESS, decomp_step = 'iterative', export = TRUE)

NCC_age_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step='iterative', include_clinical = FALSE) # returns a bundle_version_id
