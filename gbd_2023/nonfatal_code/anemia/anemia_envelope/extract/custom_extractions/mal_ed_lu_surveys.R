
# load in surveys ---------------------------------------------------------

basic_survey <- data.table::fread(
  'FILEPATH/FILE.CSV'
) |>
  dplyr::select(pid, gender) |>
  unique() |>
  data.table::setnames(
    old = 'gender', new = 'sex_id'
  )

micronutrient_survey <- data.table::fread(
  'FILEPATH/FILE.CSV'
) |>
  dplyr::filter(hb_adj != 'NA')

# aggregate hemoglobin values given there are multiple readings child --------

# ONLY USE THE FIRST SAMPLE FOR EACH PERSON
aggregated_mn_survey <- micronutrient_survey |>
  dplyr::mutate(
    hemoglobin_raw = as.numeric(BCHHB) * 10,
    hemoglobin_alt_adj = as.numeric(hb_adj) * 10,
    hgb_unit = 'g/L'
  ) |>
  dplyr::group_by(Pid, Country_ID) |>
  dplyr::mutate(
    min_age = min(agedays)
  ) |>
  dplyr::filter(
    agedays == min_age
  ) |>
  dplyr::ungroup()

# merge surveys together --------------------------------------------------

full_survey <- dplyr::inner_join(
  x = aggregated_mn_survey,
  y = basic_survey,
  by = c('Pid' = 'pid')
) |>
  dplyr::mutate(
    Pid = NULL
  )

# map location ID ---------------------------------------------------------

location_map <- data.frame(
  Country_ID = c("BG", "BR", "IN", "NP", "PE", "PK", "SA", "TZ"),
  ihme_loc_id = c('BGD', 'BRA_4755', 'IND_43901', 'PER', 'NPL', 'PAK', 'ZAF_486', 'TZA')
)

loc_df <- ihme::get_location_metadata(location_set_id = 35, release_id = 16)

full_survey <- dplyr::inner_join(
  x = full_survey,
  y = location_map,
  by = 'Country_ID'
) |>
  dplyr::inner_join(
    y = loc_df[,.(location_id, ihme_loc_id, location_name)],
    by = 'ihme_loc_id'
  )

# update age_year ---------------------------------------------------------

full_survey$age_year <- full_survey$agedays / 365.25
full_survey$cv_pregnant <- 0
full_survey$year_start <- 2010
full_survey$year_end <- 2013

# write out extracted surveys ---------------------------------------------

anemia_cb <- read.csv(file.path(getwd(), 'extract/anemia_codebook.csv')) |>
  dplyr::filter(survey_name == 'MAL_ED')

extraction_dir <- 'FILEPATH'

# match extracted file to codebook entry ----------------------------------

save_extracted_file <- function(cb, extracted_data) {
  
  for(r in seq_len(nrow(cb))) {
    file_name <- paste(
      cb$survey_name[r],
      cb$nid[r],
      cb$survey_module[r],
      cb$ihme_loc_id[r],
      cb$year_start[r],
      cb$year_end[r],
      sep = "_"
    )
    
    curr_extraction <- extracted_data |>
      dplyr::filter(ihme_loc_id == cb$ihme_loc_id[r])
    curr_extraction$nid <- cb$nid[r]
    
    haven::write_dta(
      data = curr_extraction,
      path = file.path(
        extraction_dir,
        paste0(file_name, '.dta')
      )
    )
  }
}

save_extracted_file(
  cb = anemia_cb,
  extracted_data = full_survey
)
