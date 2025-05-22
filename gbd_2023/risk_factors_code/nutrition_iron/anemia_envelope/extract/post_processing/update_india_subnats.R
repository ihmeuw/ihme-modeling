anemia_cb <- read.csv(file.path(getwd(), 'extract/anemia_codebook.csv'))

nid_india <- anemia_cb |>
  dplyr::filter(grepl('IND', ihme_loc_id)) |>
  purrr::chuck('nid')

india_files <- list.files(
  path = 'FILEPATH',
  pattern = paste(nid_india, collapse = '|'),
  full.names = TRUE
)

loc_columns <- c(
  'ihme_loc_id', 'admin_1', 'urban', 'admin_1_id', 'admin_1_mapped',
  'admin_1_urban_id', 'admin_1_urban_mapped', 'nid'
)

loc_df <- ihme::get_location_metadata(location_set_id = 35, release_id = 16)

clean_ihme_loc_id <- function(vec){
  vec <- stringr::str_remove_all(string = vec, pattern = "\\n")
  vec <- stringr::str_remove_all(string = vec, pattern = "\\r")
  vec <- stringr::str_remove_all(string = vec, pattern = "\\t")
  return(vec)
}

india_loc_df <- lapply(india_files, \(f) {
  dat <- haven::read_dta(f, n_max = 1)
  good_cols <- intersect(colnames(dat), loc_columns)
  haven::read_dta(
    file = f,
    col_select = tidyselect::all_of(good_cols)
  ) |>
    unique() |>
    dplyr::mutate(
      extract_file_path = f
    )
}) |> data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::mutate(
    og_ihme_loc_id = ihme_loc_id,
    ihme_loc_id = dplyr::case_when(
      !(is.na(admin_1_id)) & admin_1_id != "" ~ admin_1_id,
      .default = ihme_loc_id
    ),
    ihme_loc_id = clean_ihme_loc_id(ihme_loc_id)
  ) |> 
  data.table::merge.data.table(
    y = loc_df[,.(ihme_loc_id, location_id)],
    by = 'ihme_loc_id',
    all.x = TRUE
  )

loc_df <- loc_df |>
  dplyr::mutate(
    urban = dplyr::case_when(
      grepl('IND_', ihme_loc_id) & grepl('urban', location_name, ignore.case = TRUE) ~ 1,
      grepl('IND_', ihme_loc_id) & grepl('rural', location_name, ignore.case = TRUE) ~ 0,
      .default = NA_integer_
    )
  ) |>
  data.table::setnames(
    old = 'ihme_loc_id',
    new = 'admin_1_id'
  )

india_loc_df <- data.table::merge.data.table(
  x = india_loc_df,
  y = loc_df[,.(parent_id, admin_1_id, urban)],
  by.x = c('location_id', 'urban'),
  by.y = c('parent_id', 'urban'),
  all.x = TRUE,
  suffixes = c('.winnower', '.mapped')
)

india_loc_df <- india_loc_df |>
  dplyr::mutate(
    new_admin_1_id = dplyr::case_when(
      !(is.na(admin_1_id.mapped)) ~ admin_1_id.mapped,
      .default = admin_1_id.winnower
    ),
    admin_1_id = admin_1_id.winnower
  )

data.table::fwrite(
  x = india_loc_df,
  file = file.path(getwd(), 'extract/post_processing/india_subnat_map.csv')
)

for(f in india_files) {
  print(f)
  india_df <- haven::read_dta(f) |>
    data.table::setDT()
  temp_map <- india_loc_df |>
    dplyr::filter(extract_file_path == f) |>
    dplyr::select(urban, admin_1_id, new_admin_1_id) |>
    unique()
  
  if(!(all(is.na(temp_map$urban))) && !(all(is.na(temp_map$admin_1_id))) &&
     all(c('urban', 'admin_1_id') %in% colnames(india_df))) {
    print('updating...')
    india_df$admin_1_id <- clean_ihme_loc_id(india_df$admin_1_id)
    temp_map$admin_1_id <- clean_ihme_loc_id(temp_map$admin_1_id)
    temp_map$new_admin_1_id <- clean_ihme_loc_id(temp_map$new_admin_1_id)
    
    india_df <- data.table::merge.data.table(
      x = india_df,
      y = temp_map,
      by = c('urban', 'admin_1_id'),
      all.x = TRUE
    ) |>
      dplyr::mutate(
        admin_1_id = dplyr::case_when(
          !(is.na(new_admin_1_id)) ~ new_admin_1_id,
          .default = admin_1_id
        )
      )
    
    update_cols <- c('admin_1_mapped', 'admin_1_urban_id', 'admin_1_urban_mapped')
    for(c in update_cols) {
      if(c %in% colnames(india_df)) {
        data.table::setnames(
          x = india_df,
          old = c,
          new = paste0('og_', c)
        )
      }
    }
    
    haven::write_dta(
      data = india_df,
      path = f
    )
  }
}

new_output_path <- 'FILEPATH'
for(f in india_files) {
  stmt <- paste('cp', f, new_output_path)
  system(stmt)
}