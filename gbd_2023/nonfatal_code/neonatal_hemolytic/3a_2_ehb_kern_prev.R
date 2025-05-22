`%>%` <- magrittr::`%>%`
source("est_prevs.R")
source("get_prev_draws.R")
params_global <- readr::read_rds("params_global.rds")
name <- "rh_disease"
dir_rhesus <- fs::path("FILEPATH")
draws_path <- fs::path("FILEPATH")
draws <- data.table::fread(draws_path) |>
  hemo::remove_draw_prefix() |>
  dplyr::mutate(age_group_id = nch::id_for("age_group", "Birth"),
                measure_id = nch::id_for("measure", "prevalence"),
                metric_id = nch::id_for("metric", "Rate")) %>%
  hemo::apply_each_draw(function(x) x / .$births) |>
  dplyr::select(tidyselect::all_of(c(
    "age_group_id",
    "sex_id",
    "location_id",
    "year_id",
    "measure_id",
    "metric_id"
  )), tidyselect::starts_with("draw_"))
checkmate::assert_data_frame(draws, any.missing = FALSE)
data.table::fwrite(draws, fs::path("FILEPATH"))

est_prevs(
  name = name,
  directory = dir_rhesus,
  dir_old =
    "FILEPATH",
  # The values below are derived from Zipursky et. al. (1), which cited the
  # Clark study (2) on trials for anti-D gammaglobulin before widespread
  # availability of Rhogam. They are also cited in Bhutani et. al. (3).
  # 1. Zipursky A. The universal prevention of Rh immunization. Clin Obstet
  #    Gynecol 1971;14:869–84.
  # 2. Clarke CA: Prevention of Rhesus iso-immunization.
  #    Seminars in Hematology 6:201, 1969.
  # 3. Bhutani VK, Zipursky A, Blencowe H, et al. Neonatal hyperbilirubinemia
  #    and Rhesus disease of the newborn: incidence and impairment estimates for
  #    2010 at regional and global levels. Pediatr Res 2013; 74 Suppl 1: 86–100.
  mean = 0.15,
  lower = 0.133,
  upper = 0.191
)