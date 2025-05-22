source("4b_kern_impair/funs.R")

params_global <- readr::read_rds("params_global.rds")
dir_mnt <- purrr::chuck(params_global, "dir_mnt")
path_csv <- fs::path(dir_mnt, "4b_Kernicterus_Impairment") |>
  fs::dir_create() |>
  fs::path("total_kern_prev.csv")
params <- list(
  path_csv = path_csv,
  path_xlsx = fs::path_ext_set(path_csv, "xlsx")
)
readr::write_rds(params, "4b_kern_impair/params.rds")

draws <- read_prev_draws(
  dir_mnt = dir_mnt,
  nms = c("rh_disease", "g6pd", "preterm", "other"),
  type = "kern",
  by = c(
    "age_group_id",
    "sex_id",
    "location_id",
    "year_id",
    "measure_id",
    "metric_id",
    "haqi"
  )
) |>
  sum_draws()

draws |>
  nch::pivot_draws_longer() |>
  nch::summarize_draws() |>
  data.table::fwrite(file = purrr::chuck(params, "path_csv"))
