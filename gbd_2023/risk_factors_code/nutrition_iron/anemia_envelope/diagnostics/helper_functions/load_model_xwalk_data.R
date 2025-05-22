
# get all plotting data ---------------------------------------------------

load_model_xwalk_data <- function(me_list, custom_id_list = NULL, sex_id){
  df_list <- list()
  
  merge_cols <- c(
    'age_group_id',
    'sex_id',
    'location_id',
    'year_id'
  )
  
  for(i in names(me_list)){
    current_me_data <- load_model_data(
      me_id = me_list[[i]][['current']][['modelable_entity_id']],
      mv_id = me_list[[i]][['current']][['model_version_id']],
      me_type = me_list[[i]][['current']][['id_type']],
      sex_id = sex_id,
      release_id = me_list[[i]][['current']][['release_id']],
      custom_id_list = custom_id_list
    ) |>
      data.table::setnames(
        old = c('mean', 'upper', 'lower'),
        new = c('mean_current', 'upper_current', 'lower_current')
      )
    
    xwalk_data <- load_xwalk_data(
      crosswalk_version_id = me_list[[i]][['current']][['crosswalk_version_id']]
    ) 
    
    previous_me_data <- load_model_data(
      me_id = me_list[[i]][['previous']][['modelable_entity_id']],
      mv_id = me_list[[i]][['previous']][['model_version_id']],
      me_type = me_list[[i]][['previous']][['id_type']],
      sex_id = sex_id,
      release_id = me_list[[i]][['previous']][['release_id']],
      custom_id_list = custom_id_list
    ) |>
      data.table::setnames(
        old = c('mean', 'upper', 'lower'),
        new = c('mean_previous', 'upper_previous', 'lower_previous')
      )
    
    me_data <- data.table::merge.data.table(
      x = current_me_data,
      y = xwalk_data,
      by = merge_cols,
      all.x = TRUE,
      suffixes = c('.current_me', '.xwalk')
    ) |>
      data.table::merge.data.table(
        y = previous_me_data,
        by = merge_cols,
        all.x = TRUE,
        suffixes = c('', '.previous_me')
      )
    
    me_data <- format_merged_data(
      input_df = me_data,
      gbd_rel_id = me_list[[i]][['current']][['release_id']]
    )
    
    df_list <- append(df_list, list(me_data))
  }
  names(df_list) <- names(me_list)
  return(df_list)
}

# load in model data ------------------------------------------------------

load_model_data <- function(me_id, mv_id, me_type, sex_id, release_id, custom_id_list = NULL){
  message(paste(me_id, mv_id, me_type, release_id))
  
  estimation_id_list <- get_demographics_data(
    release_id = release_id,
    custom_id_list = custom_id_list
  )
  
  me_results <- ihme::get_model_results(
    gbd_team = me_type,
    gbd_id = as.integer(me_id),
    model_version_id = as.integer(mv_id),
    age_group_id = as.integer(estimation_id_list$age_group_id),
    location_id = as.integer(estimation_id_list$location_id),
    year_id = as.integer(estimation_id_list$year_id),
    sex_id = sex_id,
    release_id = as.integer(release_id)
  )
  
  return(me_results)
}

# get demographics data ---------------------------------------------------

get_demographics_data <- function(release_id, custom_id_list = NULL){
  estimation_id_list <- ihme::get_demographics(
    gbd_team = 'epi', 
    release_id = release_id
  )
  estimation_id_list$sex_id <- NULL
  
  loc_df <- ihme::get_location_metadata(
    location_set_id = 35,
    release_id = release_id
  )
  
  estimation_id_list$location_id <- loc_df$location_id
  
  estimation_id_list$year_id <- c(1980, 1985, estimation_id_list$year_id)
  
  if(!(is.null(custom_id_list))){
    if(!(any(names(custom_id_list) %in% names(estimation_id_list)))){
      stop(
        'Can only supply custom IDs for the following parameters: ',
        paste(names(estimation_id_list), collapse = ", ")
      )
    }else{
      for (i in names(custom_id_list)) {
        estimation_id_list[[i]] <- custom_id_list[[i]]
      }
    }
  }
  
  return(estimation_id_list)
}

# get xwalk data ----------------------------------------------------------

load_xwalk_data <- function(crosswalk_version_id){
  keep_cols <- c(
    'nid', 'sex_id', 'age_group_id', 'year_id', 'location_id', 'val', 'upper',
    'lower', 'to_train', 'to_split', 'vmnis', 'from_2021', 'imputed_measure_row', 
    'imputed_elevation_adj_row', 'from_xwalk', 'is_outlier', 'og_year_id'
  )
  
  df <- ihme::get_crosswalk_version(
    crosswalk_version_id = crosswalk_version_id
  )|>
    dplyr::mutate(
      sex_id = dplyr::case_when(
        sex == 'Male' ~ 1,
        sex == 'Female' ~ 2,
        .default = NA_integer_
      ),
      from_xwalk = 1,
      og_year_id = year_id,
      year_id = agesexsplit::get_estimation_year_ids(year_id, 16)
    ) |>
    dplyr::select(dplyr::all_of(keep_cols)) |>
    dplyr::filter(og_year_id >= 1980) |>
    data.table::setDT() |>
    data.table::setnames(
      old = c('val', 'upper', 'lower'),
      new = c('mean_xwalk', 'upper_xwalk', 'lower_xwalk')
    )
  
  return(df)
}

# format merged data set --------------------------------------------------

format_merged_data <- function(input_df, gbd_rel_id){
  df <- data.table::copy(input_df)
  
  df$Sex <- ifelse(
    df$sex_id == 1,
    'Male',
    'Female'
  )
  
  loc_df <- ihme::get_location_metadata(
    location_set_id = 35,
    release_id = gbd_rel_id
  ) |>
    data.table::setnames(
      old = 'location_name',
      new = 'Location'
    )
  
  age_df <- ihme::get_age_metadata(
    release_id = gbd_rel_id
  ) |>
    data.table::setnames(
      old = c('age_group_years_start', 'age_group_years_end'),
      new = c('age_start', 'age_end')
    )
  age_df <- age_df[order(age_start)]
  age_df$age_group_name <- factor(age_df$age_group_name, levels = age_df$age_group_name)
  
  df <- data.table::merge.data.table(
    x = df,
    y = loc_df[, c('location_id', 'Location'), with = FALSE],
    by = 'location_id'
  ) |>
    data.table::merge.data.table(
      y = age_df[, c('age_group_id', 'age_start', 'age_end', 'age_group_name'), with = FALSE],
      by = 'age_group_id'
    )
  
  df <- df[from_xwalk == 1, `Adjustment Type` := 'Original/Unadjusted']
  df <- df[from_xwalk == 1 & to_split == 1, `Adjustment Type` := 'Age/Sex Split']
  df <- df[from_xwalk == 1 & imputed_measure_row == 1, `Adjustment Type` := 'Measure Imputed']
  df <- df[from_xwalk == 1 & imputed_elevation_adj_row == 1, `Adjustment Type` := 'Elevation Adjusted']
  
  return(df)
}
