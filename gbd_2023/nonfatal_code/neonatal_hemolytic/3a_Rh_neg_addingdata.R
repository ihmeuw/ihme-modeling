################################################################################
## adding data to rh negativity ################################################
################################################################################

rhneg_bundle <- ihme::get_bundle_version(bundle_version_id = 46234)

# adding data
# Discussed age start 0 and age end 99. Assuming no age variation, sex-specificity, or time trend for model.
rhneg_bundle_withnewdata <- rhneg_bundle %>%
  add_row(location_id = 122,
          location_name = "Ecuador",
          mean = 0.0206,
          sample_size = 1015,
          is_outlier = 0,
          nid = 564766,
          field_citation_value = "Nunez Cifuentes IS. [Prevalence of ABO and Rh Blood groups in the city of Quito-Ecuador]. Revista San Gregorio. 2022; 1(52): 102-114.",
          sex = "Both",
          year_start = 2020,
          year_end = 2021,
          age_start = 0,
          age_end = 99,
          measure = "prevalence",
          urbanicity_type = "Unknown",
          recall_type = "Point",
          unit_type = "Person",
          unit_value_as_published = 1,
          source_type = "Survey - other/unknown",
          seq = 108,
          underlying_nid = NA,
          input_type = "extracted",
          design_effect = NA,
          recall_type_value = NA,
          uncertainty_type = "Sample size",
          sampling_type = NA,
          effective_sample_size = NA,
          representative_name = "Representative for subnational location only"
  ) %>% #http://scielo.senescyt.gob.ec/pdf/rsan/v1n52/2528-7907-rsan-1-52-00102.pdf
  add_row(location_id = 351,
          location_name = "Guam",
          mean = 0.015,
          sample_size = 343,
          is_outlier = 0,
          nid = 564760,
          field_citation_value = "Plato CC, Cruz M. Blood group and haptoglobin frequencies of the Chamorros of Guam. Am J Hum Genet. 1967; 19(6): 722-31.",
          sex = "Both",
          year_start = 1964,
          year_end = 1966,
          age_start = 0,
          age_end = 99,
          measure = "prevalence",
          urbanicity_type = "Unknown",
          recall_type = "Point",
          unit_type = "Person",
          unit_value_as_published = 1,
          source_type = "Survey - other/unknown",
          seq = 109,
          underlying_nid = NA,
          input_type = "extracted",
          design_effect = NA,
          recall_type_value = NA,
          uncertainty_type = "Sample size",
          sampling_type = NA,
          effective_sample_size = NA,
          representative_name = "Unknown",
          note_SR = "blood group among the Chamorro people of Guam"
  ) %>% #https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1706299/pdf/ajhg00397-0038.pdf
  add_row(location_id = 170,
          location_name = "Congo",
          mean = .024,
          sample_size = 266055,
          is_outlier = 0,
          nid = 564764,
          field_citation_value = "Angounda BM, Mokono SO, Boukatou GB, Bakoua BS, Nanitelamio EPLC. Distribution of ABO and Rhesus (RHD) blood groups among blood donors in Republic of Congo. Int J Blood Transfus Immunohematol. 2023; 13(1): 9-14.",
          sex = "Both",
          year_start = 2011,
          year_end = 2015,
          age_start = 0,
          age_end = 99,
          measure = "prevalence",
          urbanicity_type = "Unknown",
          recall_type = "Point",
          unit_type = "Person",
          unit_value_as_published = 1,
          source_type = "Survey - other/unknown",
          seq = 110,
          underlying_nid = NA,
          input_type = "extracted",
          design_effect = NA,
          recall_type_value = NA,
          uncertainty_type = "Sample size",
          sampling_type = NA,
          effective_sample_size = NA,
          representative_name = "Unknown",
          note_SR = "blood donor data"
  ) %>% #https://www.ijbti.com/archive/2023/pdf/100077Z02BA2023.pdf
  add_row(location_id = 173,
          location_name = "Gabon",
          mean = .023,
          sample_size = 4744,
          is_outlier = 0,
          nid = 564762,
          field_citation_value = "Ngassaki-Yoka CD, Ndong JMN, Bisseye C. ABO, Rhesus blood groups and transfusion-transmitted infections among blood donors in Gabon. SJMS. 2018; 13(1): 12-21.",
          sex = "Both",
          year_start = 2015,
          year_end = 2015,
          age_start = 0,
          age_end = 99,
          measure = "prevalence",
          urbanicity_type = "Unknown",
          recall_type = "Point",
          unit_type = "Person",
          unit_value_as_published = 1,
          source_type = "Survey - other/unknown",
          seq = 111,
          underlying_nid = NA,
          input_type = "extracted",
          design_effect = NA,
          recall_type_value = NA,
          uncertainty_type = "Sample size",
          sampling_type = NA,
          effective_sample_size = NA,
          representative_name = "Unknown",
          note_SR = "blood donor data"
  ) #DOI 10.18502/sjms.v13i1.1685

# drop nids that we aren't using
rhneg_bundle_withnewdata <- rhneg_bundle_withnewdata %>%
  filter(!nid %in% c(145738, 402224, 145775, 145776))

openxlsx::write.xlsx(rhneg_bundle_withnewdata,
                     file = "FILEPATH",
                     sheetName = "extraction")

# bundle remove and upload -----------------------------------------------------
bundle_id <- 389
upload <- TRUE
remove <- TRUE

if (remove) {
  remove_rows <- function(bundle_id) {
    bundle_data <- ihme::get_bundle_data(bundle_id = bundle_id)
    seqs_to_delete <- bundle_data[measure == "prevalence", list(seq)]
    path <- withr::local_tempfile(fileext = ".xlsx")
    openxlsx::write.xlsx(seqs_to_delete, file = path, sheetName = "extraction")
    ihme::upload_bundle_data(bundle_id = bundle_id, filepath = path)
  }
  remove_rows(bundle_id = bundle_id)
}

if (upload) {
  ihme::upload_bundle_data(
    bundle_id = bundle_id,
    filepath = "FILEPATH"
  )
}

# saving bundle version and automatic crosswalk
result <- ihme::save_bundle_version(bundle_id, automatic_crosswalk = TRUE)