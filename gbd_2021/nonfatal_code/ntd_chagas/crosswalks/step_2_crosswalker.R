
# NTDs - Chagas disease 
#Description: Aply Sex and age -crosswalk- MR-BRT Model Fit and DisMod age pattern
# 


## SET UP FOCAL DRIVES

rm(list = ls())

os <- .Platform$OS.type
if (os=="windows") {
} else {
}


library(data.table)
library(ggplot2)
library(openxlsx)
source("FILEPATH/")
source("FILEPATH/")
source("FILEPATH/")
source("FILEPATH")
source('FILEPATH/')
source('FILEPATH/')
source('FILEPATH/')
source('FILEPATH')
source("FILEPATH/")

# MR-BRT

repo_dir <- paste0("FILEPATH")
source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))

# Custom

source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))

#############################################################################################
###                                      Set-Up                                           ###
#############################################################################################

#' [Set-up run directory / tracker]

run_file <- fread(paste0(FILEPATH))
run_dir <- run_file[nrow(FILEPATH), FILEPATH]
crosswalks_dir    <- paste0(FILEPATH, "/FILEPATH/")

#'[Set Status]


#############################################################################################
###                                      Crosswalks                                       ###
#############################################################################################

fit1 <- readRDS(paste0(FILEPATH))

#' [Set-up Age-Split Bundle]
agesplit_bv_data   <- get_bundle_version(bundle_version_id = ADDRESS)

# clean
agesplit_bv_data[, age_start := round(age_start)]
agesplit_bv_data[, age_end := round(age_end)]

agesplit_bv_data_ss   <- apply_sex_crosswalk(mr_brt_fit_obj = fit1, all_data = agesplit_bv_data, decomp_step = "ADDRESS")

agesplit_out_file <- paste0(crosswalks_dir, FILEPATH)
openxlsx::write.xlsx(FILEPATH)
agesplit_description <- "New Sex Split from binomial distribution 10% trim, no surveys dropped"
 
agesplit_cw_md  <- save_crosswalk_version(bundle_version_id = ADDRESS, 
                                         data_filepath =  FILEPATH,
                                         description = FILEPATH)

########### input bv

input_data_bv_data <- get_bundle_version(bundle_version_id = ADDRESS)

# # clean
input_data_bv_data[, age_start := round(age_start)]
input_data_bv_data[, age_end := round(age_end)]

#'[Sex Crosswalk]
input_data_bv_data_ss <- apply_sex_crosswalk(mr_brt_fit_obj = fit1, all_data = input_data_bv_data, decomp_step = "ADDRESS")

#'[Diagnostic Crosswalk]


#'[Age Crosswalk] 

input_data_bv_data_ss[, year_id := year_start]
input_data_bv_data_ss[, location_id := NULL]
locs <- get_location_metadata(ADDRESS)
locs <- locs[, .(location_name, location_id)]

input_data_bv_data_ss <- merge(input_data_bv_data_ss, locs, by = "location_name")
input_data_bv_data_ss[is.na(sample_size), sample_size := effective_sample_size]
input_data_bv_data_ss[is.na(cases), cases := mean * effective_sample_size ]

input_data_bv_data_ss[,measure_id := 5]

  
#issue with Emma's code
input_data_bv_data_as <- apply_age_split(data = input_data_bv_data_ss,
                                dismod_meid = ADDRESS,
                                dismod_mvid = ADDRESS,
                                loc_pattern = ADDRESS,
                                decomp_step_meid = "ADDRESS",
                                decomp_step_pop = "ADDRESS")



#############################################################################################
###                                      Save Crosswalk Version                           ###
#############################################################################################

input_data_bv_data_as[age_start == 1 & age_end == 0, age_end := 1]
input_data_bv_data_as[, age_diff := NULL]
input_data_bv_data_as[, upper := mean + (1.96 * standard_error)]
input_data_bv_data_as[, lower := mean - (1.96 * standard_error)]
input_data_bv_data_as[, uncertainty_type_value := 95]
input_data_bv_data_as[lower < 0, lower := 0]

input_data_bv_data_as <- input_data_bv_data_as[upper <= 1]

# take out pseudo 0's not in location hierarchy for dismod
locs <- get_location_metadata(location_set_id = ADDRESS)
input_data_bv_data_as <- input_data_bv_data_as[(location_id %in% locs[,location_id]),]


input_data_out_file <- paste0(crosswalks_dir, "FILEPATH")
openxlsx::write.xlsx(FILEPATH)

input_data_description <- "ADDRESS"

input_data_cw_md <- save_crosswalk_version(bundle_version_id = ADDRESS, 
                                          data_filepath =  FILEPATH,
                                          description = FILEPATH)



nnote <- "ADDRESS"

update_tracker(ninput_data = ADDRESS,
               ninput_data_bv = ADDRESS,
               ninput_data_cw = input_data_cw_md$ADDRESS,
               nagesplit = ADDRESS,
               nagesplit_bv = ADDRESS,
               nagesplit_cw = ADDRESS,
               nnote = nnote
               )



#'[Sex Crosswalk]

input_data_bv_data <- get_bundle_version(bundle_version_id = ADDRESS)
# splice in new birth pregnancy 1
input_data_bv_data_no_bp <- input_data_bv_data[field_citation_value != "ADDRESS"]
new_bp <- fread(FILEPATH) # does not have 2019 as gbd 2017 did not have 2017


old_bp <- input_data_bv_data[field_citation_value == "ADDRESS"]
# format new_bp for binding
setnames(new_bp, names(new_bp)[names(new_bp) %like% "cv_"], c( "value_diag_mixed" ,  "value_passive" , "value_blood_donor" ,"value_subn_endemic"))
new_bp[, c("acause", "data_type", "response_rate") := NULL]

new_bp[, ":="(field_citation_value = "ADDRESS",
              table_num = NA,
              page_num = NA,
              site_memo = "",
              case_definition = "",
              note_modeler = "", 
              bundle_id = ADDRESS,
              origin_seq = ADDRESS,
              origin_id = ADDRESS,
              cause_id = ADDRESS,
              underlying_field_citation_value = NA,
              clinical_data_type = NA,
              extractor = "ADDRESS")]
input_data_bv_data <- rbind(input_data_bv_data_no_bp, new_bp)

# clean
input_data_bv_data[, age_start := round(age_start)]
input_data_bv_data[, age_end := round(age_end)]
input_data_bv_data[extractor == "ADDRESS", age_end := 0]

non_modeled_data <- input_data_bv_data[extractor != "ADDRESS",]
modeled_data     <- input_data_bv_data[extractor == "ADDRESS",]

# add sample size for sex crosswalk

modeled_data[, sample_size := NULL]
locs <- unique(input_data_bv_data[extractor == "ADDRESS", location_id])
years <- unique(input_data_bv_data[extractor == "ADDRESS", year_start])
 
pop <- get_population(age_group_id = 28,
                       gbd_round_id = ADDRESS,
                       decomp_step = "ADDRESS",
                       location_id = locs,
                       year_id = years)
pop[, c("year_start", "year_end") := year_id]
pop[, sample_size := population]
pop[, c("age_start", "age_end") := .(0, 1)]
pop <- pop[, .(location_id, year_start, year_end, sample_size, age_start, age_end)]
modeled_data[, c("age_start", "age_end") := .(0, 1)]
modeled_data <- merge(modeled_data, pop, by = c("location_id", "year_start", "year_end", "age_start", "age_end"))
modeled_data[, cases := mean * sample_size]
input_data_bv_data <- rbind(modeled_data, non_modeled_data)



input_data_bv_data_ss <- apply_sex_crosswalk(mr_brt_fit_obj = fit1, all_data = input_data_bv_data, decomp_step = "ADDRESS")

#'[Age Crosswalk]

input_data_bv_data_as <- apply_age_split(data = input_data_bv_data_ss,
                                         dismod_meid = ADDRESS,
                                         dismod_mvid = ADDRESS,
                                         loc_pattern = ADDRESS,
                                         decomp_step_meid = "ADDRESS",
                                         decomp_step_pop = "ADDRESS")


input_data_bv_data_as[, crosswalk_parent_seq := ADDRESS]
#
input_data_bv_data_as[age_end < age_start, age_end := 1]
input_data_bv_data_as[, age_diff := NULL]

# calculate upper
input_data_bv_data_as[, uncertainty_type_value := 95]
input_data_bv_data_as[, upper := mean + (1.96 * standard_error)]
input_data_bv_data_as[, lower := mean - (1.96 * standard_error)]
input_data_bv_data_as[lower < 0, lower := 0]
input_data_bv_data_as <- input_data_bv_data_as[upper <= 1]

input_data_bv_data_as[field_citation_value %like% 'Southern Cone' & location_name %like% 'Bolivia', is_outlier := 1]
input_data_bv_data_as[field_citation_value %like% 'Gürtler RE, Chuit R' & location_name %like% 'Argentina', is_outlier := 1]
input_data_bv_data_as[location_name %like% 'Minas' & field_citation_value %like% 'Lima', is_outlier := 1]
input_data_bv_data_as[location_name %like% 'Ecuador', is_outlier := 1]
input_data_bv_data_as[location_name %like% 'Vene' & field_citation_value %like% 'Ac', is_outlier := 1]
input_data_bv_data_as[location_name %like% 'Mex' & field_citation_value %like% 'Licona', is_outlier := 1]
input_data_bv_data_as[location_name %like% 'Campeche' & field_citation_value %like% 'Mont', is_outlier := 1]

input_data_out_file <- paste0(FILEPATH)
locs <- get_location_metadata(location_set_id = ADDRESS)[,location_id]
input_data_bv_data_as <- input_data_bv_data_as[location_id %in% locs]
openxlsx::write.xlsx(FILEPATH)
input_data_description <- "ADDRESS"
input_data_cw_md <- save_crosswalk_version(bundle_version_id = ADDRESS, 
                                           data_filepath =  FILEPATH,
                                           description = ADDRESS)
                                           
