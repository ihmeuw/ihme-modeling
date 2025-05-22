source(file.path(getwd(), "ensemble/ensemble_functions.R"))
future::plan(future::multisession)

if (interactive()) {
  task_id <- 1
  model_loc <- read.csv(file.path(getwd(), 'ensemble/locs.csv')) |> 
    dplyr::slice(task_id) |> 
    purrr::chuck('location_id')
  params <- readRDS(file.path(getwd(), 'ensemble/params.rds'))
  out_dir <- 'FILEPATH'
} else {
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  args <- commandArgs(trailingOnly = TRUE)
  model_loc <- read.csv(args[1]) |> 
    dplyr::slice(task_id) |> 
    purrr::chuck('location_id')
  params <- readRDS(args[2])
  out_dir <- args[3]
}

message(task_id)

# set up output dir -------------------------------------------------------

if(task_id == 1) {
  if(dir.exists(out_dir)) unlink(x = out_dir, recursive = TRUE)
  dir.create(
    path = out_dir,
    mode = '0775'
  )
}

# load in draws and get parallelizing GBD IDs -----------------------------

cli::cli_progress_step("Getting draws...", msg_done = "Got draws.")
# Get draws for all meids and reshape long
draws <- lapply(names(params$me_id_list), function(me_type){
  
  me_id <- params$me_id_list[[me_type]][['modelable_entity_id']] 
  
  draws_file_path <- file.path(
    params$draws_path, 
    me_id, 
    paste0('draws_loc_', model_loc, '.fst')
  )
  
  current_draws <- fst::read.fst(
    path = draws_file_path,
    as.data.table = TRUE
  )
  
  return(current_draws)
})

combos <- unique(
  draws[[1]] |>
    dplyr::select(age_group_id, sex_id)
)

year_ids <- draws[[1]] |>
  purrr::chuck('year_id') |>
  unique() |>
  sort()

# load in anemia threshold and distribution data --------------------------

dist_weights <- read.csv(file.path(getwd(), 'ensemble/weights.csv'))

anemia_thresholds <- read.csv(
  file.path(getwd(), 'ensemble/new_who_thresholds_w_age_group_ids.csv')
)

# find SD for all age/sex/years -------------------------------------------

cli::cli_progress_step("Integrating draws...", msg_done = "Draws integrated.")
output <- lapply(year_ids, \(yr) {
  year_draws <- lapply(draws, \(draw) {
    draw |> dplyr::filter(year_id == yr)
  })
  furrr::future_map(seq_len(nrow(combos)), function(combo_row) {
    draw_rows <- lapply(year_draws, function(draw) {
      vctrs::vec_slice(
        x = draw, 
        i = draw$sex_id == combos$sex_id[combo_row] &
              draw$age_group_id == combos$age_group_id[combo_row]
      )
    })
    
    combo_thresholds <- anemia_thresholds |>
      dplyr::filter(
        age_group_id == combos$age_group_id[combo_row] & 
          sex_id == combos$sex_id[combo_row] & pregnant == 0
      ) |>
      dplyr::select(hgb_upper_mild, hgb_upper_moderate, hgb_upper_severe)
    
    indices <- list(
      which(params$x <= combo_thresholds$hgb_upper_mild),
      which(params$x <= combo_thresholds$hgb_upper_moderate),
      which(params$x <= combo_thresholds$hgb_upper_severe)
    )
    names(indices) <- names(params$me_id_list[2:4])
    
    out <- furrr::future_map(0:999, function(draw_index) {
      draw_name <- paste0("draw_", draw_index)
      draw <- lapply(draw_rows, `[[`, draw_name)
      names(draw) <- names(params$me_id_list)
      x <- get_ensemble_draw(
        draw = draw,
        weights = dist_weights,
        xmin = purrr::chuck(params, "xmin"),
        xmax = purrr::chuck(params, "xmax"),
        x = purrr::chuck(params, "x"),
        min_sd = params$min_sd,
        max_sd = params$max_sd,
        threshold_weights = params$threshold_weights,
        indices = indices
      )
      x$draw <- draw_name
      return(x)
    }) |>
      data.table::rbindlist()
    out$location_id <- model_loc
    out$year_id <- yr
    out$age_group_id <- combos$age_group_id[combo_row]
    out$sex_id <- combos$sex_id[combo_row]
    return(out)
  }) |>
    data.table::rbindlist()
}) |>
  data.table::rbindlist()

# save out data -----------------------------------------------------------

cli::cli_progress_step("Saving...", msg_done = "Saved. Finished")

out_file_name <- file.path(
  out_dir,
  paste0('loc_', model_loc, '.fst')
)
fst::write.fst(
  x = output,
  path = out_file_name
)

cli::cli_progress_done()