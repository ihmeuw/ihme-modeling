read_prev_draws <- function(dir_mnt, nms, type, by) {
  result <- fs::path(
    dir_mnt,
    c("3a_Rhesus", "3b_G6PD", "3c_Preterm", "3d_Other"),
    paste0(nms, "_", type, "_prev_draws.csv")
  ) |>
    purrr::map(data.table::fread) |>
    purrr::map2(paste0(nms, "_"), hemo::add_draw_prefix) |>
    purrr::reduce(dplyr::full_join, by = by)
  checkmate::assert_data_frame(result, any.missing = FALSE)
  result
}

sum_draws <- function(draws, draw_ids = 0:999) {
  for (draw_id in draw_ids) {
    data.table::set(
      x = draws,
      j = paste0("rh_disease_draw_", draw_id),
      value = purrr::chuck(draws, paste0("rh_disease_draw_", draw_id)) +
        purrr::chuck(draws, paste0("g6pd_draw_", draw_id)) +
        purrr::chuck(draws, paste0("preterm_draw_", draw_id)) +
        purrr::chuck(draws, paste0("other_draw_", draw_id))
    )
  }
  draws |>
    dplyr::select(
      !(tidyselect::starts_with("g6pd_draw_") |
          tidyselect::starts_with("preterm_draw_") |
          tidyselect::starts_with("other_draw_"))
    ) |>
    hemo::remove_draw_prefix()
}

add_age_groups <- function(draws) {
  dplyr::bind_rows(
    draws,
    dplyr::mutate(
      draws, age_group_id = nch::id_for("age_group", "Birth")
    ),
    dplyr::mutate(
      draws, age_group_id = nch::id_for("age_group", "Late Neonatal")
    ),
  )
}
