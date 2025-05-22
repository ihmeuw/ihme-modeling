
# load in command line info -----------------------------------------------

source(file.path(getwd(), 'model_prep/src_xwalk/hb_model_ensemble_draws.R'))

if (interactive()) {
  task_id <- 9
} else {
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
}

map_file_name <- file.path(getwd(), 'model_prep/param_maps/bundle_map.csv')
bundle_map <- read.csv(map_file_name)

# aggregate all points that are duplicates for a aslyn --------------------

find_duplicates <- function(dat) {
  dat |>
    dplyr::group_by(nid, age_group_id, sex_id, location_id, year_id) |>
    dplyr::summarise(num_duplicate_rows = dplyr::n()) |>
    dplyr::ungroup() |>
    dplyr::filter(num_duplicate_rows > 1) |>
    unique() |>
    dplyr::mutate(
      has_duplicate = 1,
      duplicate_group_number = seq_len(dplyr::n())
    )
}

filter_duplicates <- function(dat, s_dat) {
  m_dat <- merge.data.frame(
    x = dat,
    y = s_dat,
    by = c('nid', 'age_group_id', 'sex_id', 'location_id', 'year_id'),
    all = TRUE
  )
  
  list(
    good_df = m_dat |> dplyr::filter(is.na(has_duplicate)),
    dup_df = m_dat |> dplyr::filter(!(is.na(has_duplicate) & has_duplicate == 1))
  )
}

sample_draws <- function(measure_type, mn, vr, row_sample_size) {
  NUM_DRAWS <- 1000
  draw_rows <- paste('draw', seq_len(NUM_DRAWS), sep = '_')
  draw_df <- lapply(seq_len(length(row_sample_size)), \(r) {
    draw_vec <- switch (
      measure_type,
      'continuous' = hb_me_ensemble_draws(NUM_DRAWS, mn[r], vr[r]),
      'proportion' = rnorm(n = NUM_DRAWS, mean = mn[r], sd = sqrt(vr[r]))
    )
    matrix(data = draw_vec, nrow = NUM_DRAWS, ncol = 1) |>
      as.data.frame() |>
      dplyr::mutate(
        sample_size = row_sample_size[r],
        draw_id = draw_rows
      )
  }) |> 
    data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
    dplyr::group_by(draw_id) |>
    dplyr::summarise(
      new_mean = weighted.mean(x = V1, w = sample_size)
    ) |>
    dplyr::mutate(new_sample_size = sum(row_sample_size))
  
  if(interactive()) assign('temp_draw_df', draw_df, envir = .GlobalEnv)
  
  upper_val <- quantile(draw_df$new_mean, 0.975)
  lower_val <- quantile(draw_df$new_mean, 0.025)
  se_val <- (upper_val - lower_val) / (2 * qnorm(0.975))
  
  return(
    list(
      mean_val = mean(draw_df$new_mean), 
      variance_val = se_val ^ 2,
      se_val = se_val,
      ss_val = mean(draw_df$new_sample_size),
      upper = upper_val,
      lower = lower_val
    )
  )
}

aggregate_duplicates <- function(dat) {
  lapply(unique(dat$duplicate_group_number), \(x) {
    x_df <- dat |> 
      dplyr::filter(duplicate_group_number == x)
    
    new_value_list <- sample_draws(
      measure_type = unique(x_df$measure),
      mn = x_df$val,
      vr = x_df$variance,
      row_sample_size = x_df$sample_size
    )
    x_df |>
      dplyr::slice(1) |>
      dplyr::mutate(
        row_id = paste0(row_id, '_1'),
        val = new_value_list$mean_val,
        variance = new_value_list$variance_val,
        sample_size = new_value_list$ss_val,
        standard_error = new_value_list$se_val,
        upper = new_value_list$upper,
        lower = new_value_list$lower,
        site_memo = location_name
      )
  }) |> 
    data.table::rbindlist(use.names = TRUE, fill = TRUE)
}

filter_group_review <- function(dat){
  i_vec <- which(dat$group_review == 1 | is.na(dat$group_review))
  
  dat |>
    dplyr::slice(i_vec)
}

main_aggregate_function <- function(bun_id) {
  root_dir <- file.path(
    'FILEPATH', bun_id
  )
  input_file_name <- 'elevation_adj_measure2.csv'
  output_file_name <- 'post_dedup.fst'
  dat <- data.table::fread(
    file = file.path(root_dir, input_file_name)
  ) |>
    filter_group_review()
  
  duplicate_df <- find_duplicates(dat)
  if(interactive()) assign('duplicate_df', duplicate_df, envir = .GlobalEnv)
  df_list <- filter_duplicates(dat, duplicate_df)
  if(interactive()) assign('df_list', df_list, envir = .GlobalEnv)
  dedup_df <- aggregate_duplicates(df_list$dup_df)
  if(interactive()) assign('x_dedup_df', dedup_df, envir = .GlobalEnv)
  final_df <- data.table::rbindlist(
    list(df_list$good_df, dedup_df),
    use.names = TRUE,
    fill = TRUE
  )
  
  if(interactive()) {
    assign('final_df', final_df, envir = .GlobalEnv)
  } 
  fst::write.fst(
    x = final_df,
    path = file.path(root_dir, output_file_name)
  ) 
}

# run function ------------------------------------------------------------

main_aggregate_function(bun_id = bundle_map$id[task_id])

