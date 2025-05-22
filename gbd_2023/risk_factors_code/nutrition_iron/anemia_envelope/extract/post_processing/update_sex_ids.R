md_file_list <- list.files(
  path = 'FILEPATH',
  full.names = TRUE
)

# identify the bad extractions --------------------------------------------

missing_sex_id <- c()
bad_sex_id <- c()
for (f in md_file_list) {
  dat <- haven::read_dta(f)
  if(!('sex_id' %in% colnames(dat))) {
    print(paste('sex_id not in:', f))
    missing_sex_id <- append(missing_sex_id, f)
  } else if(any(is.na(dat$sex_id)) || any(!(unique(dat$sex_id %in% 1:2)))) {
    print(paste('bad sex_ids:', f))
    bad_sex_id <- append(bad_sex_id, f)
  }
}

# set 2 for all female only extractions -----------------------------------

for (f in missing_sex_id) {
  dat <- haven::read_dta(f)
  dat$sex_id <- 2
  haven::write_dta(
    data = dat,
    path = f
  )
}

# flag the remaining surveys for 3 or match with basic --------------------

basic_cb <- read.csv(file.path(getwd(), 'extract/post_processing/winnower_db_vw_cb_basic.csv'))

basic_cb$extract_file <- paste(
  basic_cb$survey_name,
  basic_cb$nid,
  basic_cb$survey_module,
  basic_cb$ihme_loc_id,
  basic_cb$year_start,
  basic_cb$year_end,
  sep = "_"
) |>
  paste0('.dta') |>
  stringr::str_replace_all(
    pattern = '/',
    replacement = '_'
  )

file_index <- 9

dat <- haven::read_dta(bad_sex_id[file_index])
any(is.na(dat$sex_id))
length(which(is.na(dat$sex_id)))
any(!(dat$sex_id %in% 1:2))
length(which(!(dat$sex_id %in% 1:2)))
dat <- dat |> dplyr::filter(!(is.na(sex_id))) |> 
  dplyr::mutate(
    sex_id = dplyr::case_when(
      sex_id == 3 ~ 2,
      .default = 1
    )
  )
haven::write_dta(
  data = dat,
  path = bad_sex_id[file_index]
)
