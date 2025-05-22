
# source libraries --------------------------------------------------------

library(data.table)
set.seed(123)

# create mod_sev anemia prevalence ----------------------------------------

create_mod_sev_anemia_prev <- function(input_df){
  df <- copy(input_df)
  
  df$temp_row_id <- seq_len(nrow(df))
  
  #create a data table with all of the potential combos
  id_cols <- c(
    'nid', 'year_id', 'sex_id', 'orig_age_start', 'orig_age_end', 'location_id',
    'cv_pregnant', 'site_memo', 'var', 'temp_row_id'
  )
  
  value_cols <- c(
    'val', 'variance', 'upper', 'lower', 'standard_error', 'sample_size', 'cases'
  )
  
  to_adjust_df <- subset_mod_sev_rows(
    input_df = df,
    id_cols = id_cols
  )
  
  to_adjust_df <- merge.data.table(
    x = to_adjust_df,
    y = df,
    by = id_cols
  ) |> unique()
  
  mod_sev_df <- calculate_mod_sev_prev(
    input_df = to_adjust_df,
    id_cols = id_cols,
    value_cols = value_cols
  )
  
  mod_sev_df <- update_mod_sev_info(
    og_df = df,
    input_mod_sev_df = mod_sev_df,
    id_cols = id_cols,
    value_cols = value_cols
  )
  
  df <- rbindlist(
    list(df, mod_sev_df),
    use.names = TRUE, fill = TRUE
  )
  
  df$temp_row_id <- NULL
  
  return(df)
}

# get the rows where we only have moderate and severe anemia --------------

subset_mod_sev_rows <- function(input_df, id_cols){
  df <- copy(input_df)
  
  keep_vars <- paste('anemia', c('moderate', 'severe', 'mod_sev'), sep = "_")
  keep_var_str <- paste(keep_vars, collapse = "|")
  keep_var_str <- paste0("^(", keep_var_str, ").*")
  i_vec <- which(grepl(keep_var_str, df$var))
  mod_sev_df <- df[i_vec, ]
  
  unique_df <- mod_sev_df |>
    dplyr::select(dplyr::all_of(id_cols)) |>
    setDT() |>
    unique()
  
  adj_types <- c('raw', 'adj', 'who', 'brinda')
  unique_df$elevation_adj_type <- NA_character_
  for(i in adj_types){
    search_str <- paste0("*._", i, "$")
    i_vec <- which(grepl(search_str, unique_df$var))
    unique_df$elevation_adj_type[i_vec] <- i
  }
  
  group_cols <- setdiff(id_cols, c('var', 'temp_row_id'))
  group_cols <- c(group_cols, 'elevation_adj_type')
  
  to_adjust_df <- unique_df |>
    dplyr::group_by_at(group_cols) |>
    dplyr::mutate(num_rows = dplyr::n()) |>
    dplyr::ungroup() |>
    dplyr::slice(which(.data$num_rows == 2)) |>
    setDT()
  
  return(to_adjust_df)
}

# calculate mod+severe anemia prevalence ----------------------------------

calculate_mod_sev_prev <- function(input_df, id_cols, value_cols){
  df <- copy(input_df)
  
  keep_cols <- c(id_cols, value_cols, 'elevation_adj_type')
  
  merge_cols <- setdiff(
    c(id_cols, 'elevation_adj_type'), c('var', 'temp_row_id')
  )
  
  severe_df <- df |> 
    dplyr::slice(which(
      grepl('severe', .data$var)
    )) |>
    dplyr::select(dplyr::all_of(keep_cols))
  
  moderate_df <- df |> 
    dplyr::slice(which(
      grepl('moderate', .data$var)
    )) |>
    dplyr::select(dplyr::all_of(keep_cols))
  
  merge_df <- dplyr::inner_join(
    x = severe_df,
    y = moderate_df,
    by = merge_cols,
    suffix = c('.severe', '.moderate')
  ) |> unique()
  
  for(c in value_cols) merge_df[[c]] <- NA_real_
  
  merge_df$sample_size <- merge_df$sample_size.severe + merge_df$sample_size.moderate
  
  num_draws <- 1000
  for(r in seq_len(nrow(merge_df))){
    severe_draws <- rnorm(
      n = num_draws,
      mean = merge_df$val.severe[r],
      sd = merge_df$standard_error.severe[r]
    )
    severe_draws <- severe_draws * merge_df$sample_size.severe[r]
    
    moderate_draws <- rnorm(
      n = num_draws,
      mean = merge_df$val.moderate[r],
      sd = merge_df$standard_error.moderate[r]
    )
    moderate_draws <- moderate_draws * merge_df$sample_size.moderate[r]
    
    mod_sev_draws <- (severe_draws + moderate_draws) / merge_df$sample_size[r]
    
    merge_df$val[r] <- mean(mod_sev_draws)
    merge_df$upper[r] <- quantile(mod_sev_draws, 0.975)
    merge_df$lower[r] <- quantile(mod_sev_draws, 0.025)
    if(merge_df$val[r] < 0) {
      merge_df$val[r] <- 0
      merge_df$lower[r] <- 0
    }
    merge_df$standard_error[r] <- (merge_df$upper[r] - merge_df$lower[r]) /
      (2 * qnorm(0.975))
    merge_df$variance[r] <- var(mod_sev_draws)
    merge_df$cases[r] <- merge_df$val[r] * merge_df$sample_size[r]
  }
  
  merge_df$temp_row_id <- merge_df$temp_row_id.severe
  merge_df$var <- paste0("anemia_mod_sev_", merge_df$elevation_adj_type)
  merge_df$imputed_mod_sev <- TRUE
  
  drop_cols <- colnames(merge_df)[grepl("\\.", colnames(merge_df))]
  drop_cols <- append(drop_cols, 'elevation_adj_type')
  for(c in drop_cols) merge_df[[c]] <- NULL
  
  return(merge_df)
}

# append on all other data associated with mod_sev data -------------------

update_mod_sev_info <- function(og_df, input_mod_sev_df, id_cols, value_cols){
  df <- copy(og_df)
  mod_sev_df <- copy(input_mod_sev_df)
  
  keep_cols <- setdiff(colnames(df), c(id_cols, value_cols))
  keep_cols <- append(keep_cols, 'temp_row_id')
  
  df <- df |>
    dplyr::select(dplyr::all_of(keep_cols)) |>
    setDT()
  
  mod_sev_df <- merge.data.table(
    x = mod_sev_df, 
    y = df,
    by = 'temp_row_id'
  )
  
  return(mod_sev_df)
}
