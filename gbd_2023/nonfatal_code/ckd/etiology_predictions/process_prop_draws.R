# Functions for script get_prop_draws.R:

get_me_draws <- function(me, age_ids, loc_id, sex_ids, release_id, measure) {
  draws <- get_draws(
    gbd_id_type  = "modelable_entity_id",
    source       = "epi",
    gbd_id       = me,
    age_group_id = age_ids,
    location_id  = loc_id,
    sex_id       = sex_ids,
    release_id   = release_id,
    measure_id   = measure, 
    metric_id    = 3 #rate
  )
  
  draws[, c("model_version_id", "metric_id") := NULL]
  return(draws)
}

get_columns <- function(df, type = "draw") {
  if (type == "draw") {
    cols <- names(df)[grep("draw_", names(df))]
  } else if (type == "non_draw") {
    cols <- names(df)[!grepl("draw_", names(df))]
  } else {
    stop("Invalid type. Choose 'draw' or 'non-draw'.")
  }
  return(cols)
}

get_long_draws <- function(dt, new_col_name) {
  message('Converting dt from wide to long..')
  
  draw_cols <- get_columns(dt, type = "draw")
  non_draw_cols <- get_columns(dt, type = "non_draw")
  
  dt_long <- data.table::melt(dt,
                              id.vars = non_draw_cols, #columns to keep
                              measure.vars = draw_cols, #columns to collapse (or combine) together.
                              variable.name = 'draw', #column name that became long
                              value.name = new_col_name) #column name that holds values
  return(dt_long)
}

get_wide_draws <- function(long_dt, cols_to_keep, col_name_to_widen) {
  message('Converting dt from long to wide..')
  
  # Create a formula string dynamically based on cols_to_keep and draw
  formula_str <- paste(paste(cols_to_keep, collapse = " + "), "~ draw", sep = " ")
  
  # Convert from long to wide using the dynamic formula
  wide_dt <- data.table::dcast(long_dt, 
                               formula = as.formula(formula_str), 
                               value.var = col_name_to_widen)
  
  return(wide_dt)
}

check_columns <- function(df, n) {
  # Check columns size of df
  if (ncol(df) != n) {
    stop("df has the wrong number of columns")
  }
}

get_dm_draws_and_reshape <- function(dm_me, dm_name) {
  
  message(paste('Getting draws for', dm_name))
  # dm models are in prevalence space 
  df <- get_me_draws(dm_me, age_ids, loc_id, sex_ids, release_id, 5)
  df[, c("modelable_entity_id", "measure_id") := NULL]
  
  df_long <- get_long_draws(df, dm_name)
  
  return(df_long)
}

save_results <- function (dt, outdir, stage_name, etio_name, loc_id) {
  dir <- paste0(outdir, stage_name, "/", etio_name, "/")
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }
  write.csv(dt, paste0(dir, loc_id, ".csv"), row.names = F)
}
