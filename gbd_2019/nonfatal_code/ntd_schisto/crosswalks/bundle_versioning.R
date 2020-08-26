

### ----------------------- LOAD DATA ------------------------------

# Bundle All Age
# Bundle Age Specific
# Diagnostic Coefficients

### ----------------------- SAVE BUNDLE VERIONS (OPTIONAL) ------------------------------

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

# 
gw_bundle<- get_bundle_data(bundle_id = ADDRESS, decomp_step = 'iterative', export = TRUE)


###all age/age specific - mix - bundle ####

bv_all_age<- save_bundle_version(bundle_id=ADDRESS, decomp_step="step2", include_clinical = FALSE) # returns a bundle_version_id

step_2_bundle<- get_bundle_version(bundle_version_id = ADDRESS, export=TRUE)

description <- "FILEPATH"

ADDRESS <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)





readxl::excel_sheets("FILEPATH")
df <- readxl::read_xlsx("FILEPATH", sheet = 'extraction')

df_post1990<- subset(df, year_start>=1990 & year_end>=1990)

write.xlsx(df_post1990, "/FILEPATH", sheet = 'extraction')

openxlsx::write.xlsx(df_post1990, sheetName = "extraction", file = "FILEPATH")

description2 <- "Schisto_sexsplit_dxcw_agesplit_aggregated_co-infection_decomp2_forDISMOD-ADDRESS_droppre1990"

step2_bundle890_cw_version_drop1990 <- save_crosswalk_version(bundle_version_id = 2489, data_filepath = "FILEPATH", description = description2)

readxl::excel_sheets("FILEPATH")
df <- readxl::read_xlsx("FILEPATH", sheet = 'extraction')

df_post1980<- subset(df, year_start>=1980)

write.xlsx(df_post1980, "FILEPAT", sheet = 'extraction')

openxlsx::write.xlsx(df_post1980, sheetName = "extraction", file = "FILEPATH")

description3 <- "Schisto_sexsplit_dxcw_agesplit_aggregated_co-infection_decomp2_forDISMOD-ADDRESS_droppre1980"

ADDRESS <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description3)





##### STEP 4 CROSSWALK UPLOADS -- currently in iterative space#####

bv_all_age<- save_bundle_version(bundle_id=ADDRESS, decomp_step="iterative", include_clinical = FALSE) # returns a bundle_version_id
#request_id bundle_version_id request_status


description <- "step4test_DISMOD-ADDRESS_post1980_casesgt1_boundedCI"

ADDRESS <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)



#testing dismod model with restricting mean to 0.75
description <- "step4test_mean_lt_0.75_DISMOD-ADDRESS_post1980_casesgt1_boundedCI"

ADDRESS <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)



###STEP 4 - FINAL ITERATION ########


bv_all_age<- save_bundle_version(bundle_id=ADDRESS, decomp_step="step4", include_clinical = FALSE) # returns a bundle_version_id
#request_id bundle_version_id request_status

description <- "step4_mean_lt_0.75_gt_0.05_post1980_casesgt1_boundedCI"
ADDRESS <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)

## dropping all data points below 10% and retaining >0.75 mean 

description <- "step4_mean_gt_0.1"
ADDRESS <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)


description <- "step4_mean_gt_0.1_locscorrected"
ADDRESS <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)



description <- "step4_younger_age_high_prev_drop"
ADDRESS <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)





age_spec_schisto<- get_bundle_data(bundle_id = ADDRESS, decomp_step = 'iterative', export = TRUE)


age_specific_bundle <- get_bundle_version(bundle_version_id = ADDRESS)


#25 year age bins
age_specific_bundle <- age_specific_bundle[, age_width := age_end - age_start]
age_specific_bundle <- age_specific_bundle[age_width <= 25, ]


age_specific_bundle[, crosswalk_parent_seq := seq]




 
# #removing sex-specific data as DISMOD only allows for one or the other.
 
age_specific_bundle_both_sex_only<- subset(age_specific_bundle, sex=="Both")
 
#removing group review =0 as the epi uplaoder wont allow for it
age_specific_bundle_both_sex_only_group_review<- subset(age_specific_bundle_both_sex_only, group_review !=0 | is.na(group_review))
 
 
# #re-tagging a saudi subnational to saudi national
 age_specific_bundle_both_sex_only_group_review$location_id[age_specific_bundle_both_sex_only_group_review$location_id==44550] <- 152
 age_specific_bundle_both_sex_only_group_review$ihme_loc_id[age_specific_bundle_both_sex_only_group_review$ihme_loc_id=="SAU_44550"] <- "SAU"
age_specific_bundle_both_sex_only_group_review$location_name[age_specific_bundle_both_sex_only_group_review$location_name=="Jizan"] <- "Saudi Arabia"
 
 
 
 openxlsx::write.xlsx(age_specific_bundle_both_sex_only_group_review, "FILEPATH",  sheetName = "extraction")
 description <- "Schisto_age_specific_data_decomp2_cw_to_kk3_iha_filt"

cw_age_specific_bundle_both_sex_only <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)
 


######################################
##saving crosswalk versions for stage specific age specific bundles 
#####################################

stage_1_schisto_age_d<- get_bundle_data(bundle_id = ADDRESS, decomp_step = 'step2', export = TRUE)

stage_1_schisto_age_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step='step2', include_clinical = FALSE) # returns a bundle_version_id

stage_1_schisto_age<- get_bundle_version(bundle_version_id = ADDRESS, export=FALSE)

stage_1_schisto_age[, crosswalk_parent_seq := seq]

stage_1_schisto_age<- subset(stage_1_schisto_age, sex=="Both")
#8887

stage_1_schisto_age <- subset(stage_1_schisto_age, location_id!=4618 & location_id!=4619 & location_id!=4620 & location_id!=4621 & location_id!=4622 & location_id!=4623 & location_id!=4624 & location_id!=4625 & location_id!=4626 & location_id!=4636 & location_id!=4749)
#8590

openxlsx::write.xlsx(stage_1_schisto_age, "FILEPATH",  sheetName = "extraction")
description <- "ADDRESS"

stage_1_schisto_age_cw_v <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)

###stage 2####

stage_2_schisto_age_d<- get_bundle_data(bundle_id = ADDRESS, decomp_step = 'step2', export = TRUE)

stage_2_schisto_age_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step='step2', include_clinical = FALSE) # returns a bundle_version_id

stage_2_schisto_age<- get_bundle_version(bundle_version_id = ADDRESS, export=FALSE)

stage_2_schisto_age[, crosswalk_parent_seq := seq]

stage_2_schisto_age<- subset(stage_2_schisto_age, sex=="Both")

stage_2_schisto_age <- subset(stage_2_schisto_age, location_id!=4618 & location_id!=4619 & location_id!=4620 & location_id!=4621 & location_id!=4622 & location_id!=4623 & location_id!=4624 & location_id!=4625 & location_id!=4626 & location_id!=4636 & location_id!=4749)

openxlsx::write.xlsx(stage_2_schisto_age, "FILEPATH",  sheetName = "extraction")
description <- "ADDRESS"

stage_2_schisto_age_cw_v <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)


####stage 3

stage_3_schisto_age_d<- get_bundle_data(bundle_id = ADDRESS, decomp_step = 'step2', export = TRUE)

stage_3_schisto_age_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step='step2', include_clinical = FALSE) # returns a bundle_version_id
#request_id bundle_version_id request_status

stage_3_schisto_age<- get_bundle_version(bundle_version_id = ADDRESS, export=FALSE)
#8840

stage_3_schisto_age[, crosswalk_parent_seq := seq]

stage_3_schisto_age<- subset(stage_3_schisto_age, sex=="Both")

stage_3_schisto_age <- subset(stage_3_schisto_age, location_id!=4618 & location_id!=4619 & location_id!=4620 & location_id!=4621 & location_id!=4622 & location_id!=4623 & location_id!=4624 & location_id!=4625 & location_id!=4626 & location_id!=4636 & location_id!=4749)

openxlsx::write.xlsx(stage_3_schisto_age, "FILEPATH",  sheetName = "extraction")
description <- "ADDRESS"

stage_3_schisto_age_cw_v <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)


