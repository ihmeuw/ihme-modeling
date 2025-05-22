source("FILEPATH")
future::plan(future::multisession)

args <- commandArgs(trailingOnly = TRUE)
location_id <- as.integer(args[1])
params <- readRDS(args[2])
cause_params <- readRDS(args[3])

cli::cli_progress_step("Getting draws...", msg_done = "Got draws.")
draws <- furrr::future_map(purrr::chuck(cause_params, "me_mv_ids"), function(x) {
  ihme::get_draws(
    gbd_id_type = "modelable_entity_id",
    gbd_id = purrr::chuck(x, "me_id"),
    version_id = purrr::chuck(x, "mv_id"),
    location_id = location_id,
    age_group_id = purrr::chuck(params, "age_group_id"),
    year_id = 1990:2024,
    sex_id = 1:2,
    source = "epi",
    release_id = 16
  ) |> tibble::new_tibble()
})


cols <- c("year_id", "sex_id", "age_group_id")
combos <- purrr::chuck(draws, "mean")[cols]
cli::cli_progress_step("Integrating draws...", msg_done = "Draws integrated.")
output <- furrr::future_pmap(combos, function(year_id, sex_id, age_group_id) {
  draw_rows <- lapply(draws, function(draw) {
    vctrs::vec_slice(draw, draw$year_id == year_id & draw$sex_id == sex_id &
                       draw$age_group_id == age_group_id)
  })
  out <- furrr::future_map(0:999, function(draw_index) {
    draw_name <- paste0("draw_", draw_index)
    draw <- lapply(draw_rows, `[[`, draw_name)
    x <- get_ensemble_draw(
      draw,
      purrr::chuck(cause_params, "weights"),
      purrr::chuck(params, "xmin"),
      purrr::chuck(params, "xmax"),
      purrr::chuck(params, "x"),
      purrr::chuck(params, "indices")
    )
    x$draw <- draw_name
    return(x)
  }) |>
    data.table::rbindlist()
  out$location_id <- location_id
  out$year_id <- year_id
  out$age_group_id <- age_group_id
  out$sex_id <- sex_id
  return(out)
}) |>
  data.table::rbindlist()

purrr::walk(c("prev_mild", "prev_mod", "prev_sev", "prev_ex"), function(nm) {
  data.table::set(output, which(output[[nm]] < 0 | is.na(output[[nm]])),
                  nm, value = 0)
  data.table::set(output, which(output[[nm]] > 1), nm, value = 1)
})

output[, prev1_2 := prev_mild - prev_mod]
output[, prev2_3 := prev_mod - prev_sev]
output[, prev3_4 := prev_sev - prev_ex]

var_nms <- c(
  "prev_mild",
  "prev_mod",
  "prev_sev",
  "prev_ex",
  "prev1_2",
  "prev2_3",
  "prev3_4",
  "meanval",
  "sdev"
)
outputs <- purrr::map(var_nms, function(x) {
  data.table::dcast(output,
    location_id + year_id + sex_id + age_group_id ~ draw,
    value.var = x
  )
})

cli::cli_progress_step("Saving...", msg_done = "Saved. Finished")
dirs <- fs::path(purrr::chuck(cause_params, "dir_out"), var_nms)
paths <- fs::path(dirs,  paste0(location_id, ".csv"))
purrr::walk2(outputs, paths, function(out, path) data.table::fwrite(out, path))

cli::cli_progress_done()