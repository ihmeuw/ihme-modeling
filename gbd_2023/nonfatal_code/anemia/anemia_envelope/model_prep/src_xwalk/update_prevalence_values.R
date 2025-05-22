# load in command args ----------------------------------------------------

if(interactive()){
  task_id <- 8
  map_file_name <- file.path(getwd(), "model_prep/param_maps/measure_preg_df.csv")
  output_dir <- "FILEPATH"
}else{
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  command_args <- commandArgs(trailingOnly = TRUE)
  map_file_name <- command_args[1]
  output_dir <- command_args[2]
}

measure_preg_df <- read.csv(map_file_name)

current_elevation_adjust_type <- measure_preg_df$elevation_adj_type[task_id]
current_cv_pregnant <- measure_preg_df$cv_pregnant[task_id]

bundle_map <- read.csv(file.path(getwd(), "model_prep/param_maps/bundle_map.csv")) |>
  dplyr::filter(
    elevation_adj_type == current_elevation_adjust_type &
      cv_pregnant == current_cv_pregnant
  )

# merge all measures together within same elevation type ------------------

envelope_df <- data.table::data.table()

keep_cols <- c(
  'nid', 'year_id', 'age_group_id', 'sex_id', 'location_id', 'val', 'row_id'
)

for(r in seq_len(nrow(bundle_map))) {
  file_name <- file.path(
    output_dir,
    bundle_map$id[r],
    'post_dedup.fst'
  )
  temp_df <- fst::read.fst(file_name) |>
    dplyr::select(tidyselect::all_of(keep_cols)) |>
    data.table::setDT() |>
    data.table::setnames(
      old = c('val', 'row_id'), 
      new = paste0(c('val.', 'row_id.'), bundle_map$var[r])
    )
  if(r == 1) {
    envelope_df <- temp_df
  } else {
    envelope_df <- data.table::merge.data.table(
      x = envelope_df,
      y = temp_df, 
      by = c('nid', 'year_id', 'age_group_id', 'sex_id', 'location_id'),
      all = TRUE
    )
  }
}

# get current anemia column names -----------------------------------------

anemia_col_names <- c('anemic', 'mod_sev', 'severe')
for(i in seq_len(length(anemia_col_names))) {
  new_col <- grep(
    pattern = paste0('val.*', anemia_col_names[i]),
    x = colnames(envelope_df),
    value = TRUE
  )
  anemia_col_names[i] <- new_col
}

# assign status of prevalence values that violate bounds ------------------

envelope_df$bad_mod_sev <- 0
envelope_df$bad_severe <- 0

i_vec <- which(envelope_df[[anemia_col_names[1]]] < envelope_df[[anemia_col_names[2]]])
envelope_df$bad_mod_sev[i_vec] <- 1

i_vec <- which(envelope_df[[anemia_col_names[2]]] < envelope_df[[anemia_col_names[3]]])
envelope_df$bad_severe[i_vec] <- 1

to_fix <- envelope_df |>
  dplyr::filter(bad_mod_sev == 1 | bad_severe == 1)

# get ratios between different anemia thresholds --------------------------

good_df <- envelope_df |>
  dplyr::filter(bad_mod_sev == 0 & bad_severe == 0)

good_df$mod_sev_to_total_ratio <- good_df[[anemia_col_names[2]]] / good_df[[anemia_col_names[1]]]
good_df$severe_to_mod_sev_ratio <- good_df[[anemia_col_names[3]]] / good_df[[anemia_col_names[2]]]

mod_sev_ratio <- median(good_df$mod_sev_to_total_ratio, na.rm = TRUE)
severe_ratio <- median(good_df$severe_to_mod_sev_ratio, na.rm = TRUE)

# use ratios to re-calculate prevalence values ----------------------------

to_fix$new_mod_sev <- to_fix[[anemia_col_names[2]]]
i_vec <- which(to_fix$bad_mod_sev == 1)
to_fix$new_mod_sev[i_vec] <- to_fix[[anemia_col_names[1]]][i_vec] * mod_sev_ratio

to_fix$new_severe <- to_fix[[anemia_col_names[3]]]
i_vec <- which(to_fix$bad_severe == 1)
to_fix$new_severe[i_vec] <- to_fix[[anemia_col_names[2]]][i_vec] * severe_ratio

# one more check to ensure prevalence values work -------------------------

to_fix$bad_prevalence_row <- 0
i_vec <- which(
  to_fix[[anemia_col_names[1]]] < to_fix$new_mod_sev |
    to_fix[[anemia_col_names[1]]] < to_fix$new_severe |
    to_fix$new_mod_sev < to_fix$new_severe
)
to_fix$bad_prevalence_row[i_vec] <- 1

# load back in xwalk data and update prevalence values --------------------

update_vars <- c('mod_sev', 'severe')

for(i in seq_len(length(update_vars))) {
  bun_id <- bundle_map |>
    dplyr::filter(grepl(update_vars[i], var)) |>
    purrr::chuck('id')
  
  file_name <- file.path(
    output_dir,
    bun_id,
    'post_dedup.fst'
  )
  
  dis_df <- fst::read.fst(path = file_name)
  
  flag_col_name <- paste0('bad_', update_vars[i])
  new_col_name <- paste0('new_', update_vars[i])
  i_vec <- which(to_fix[[flag_col_name]] == 1)
  curr_fix_df <- to_fix |>
    dplyr::slice(i_vec)
  
  dis_df$bad_prevalence_row <- 0
  dis_df$post_impute_prev <- 0
  
  for(r in seq_len(nrow(curr_fix_df))) {
    xwalk_row_index <- which(
      dis_df$age_group_id == curr_fix_df$age_group_id[r] &
        dis_df$sex_id == curr_fix_df$sex_id[r] &
        dis_df$location_id == curr_fix_df$location_id[r] &
        dis_df$year_id == curr_fix_df$year_id[r] &
        dis_df$nid == curr_fix_df$nid[r]
    )
    dis_df$val[xwalk_row_index] <- curr_fix_df[[new_col_name]][r]
    dis_df$bad_prevalence_row[xwalk_row_index] <- curr_fix_df$bad_prevalence_row[r]
    dis_df$post_impute_prev[xwalk_row_index] <- 1
  }
  
  fst::write.fst(
    x = dis_df,
    path = file_name
  )
  
}
