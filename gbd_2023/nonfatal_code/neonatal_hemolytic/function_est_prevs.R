est_prevs <- function(name, directory, dir_old, mean, lower, upper) {
  ehb_prev_draws <- hemo::ehb_prev(
    draws = data.table::fread(
      fs::path(directory, paste0(name, "_draws.csv"))
    ),
    mean = mean,
    lower = lower,
    upper = upper
  )
  checkmate::assert_data_frame(ehb_prev_draws, any.missing = FALSE)

  data.table::fwrite(
    ehb_prev_draws,
    fs::path(directory, paste0(name, "_ehb_prev_draws.csv"))
  )

  kern_prev_draws <- hemo::kern_prev(
    ehb_prev_draws,
    kern_factor_draws = hemo::prep_kern_factor_draws(data.table::fread(fs::path(
      fs::path_abs(fs::path(directory, "..")),
      "4a_Kernicterus_prevalence/draws_final.csv"
    )))
  )
  checkmate::assert_data_frame(kern_prev_draws, any.missing = FALSE)

  data.table::fwrite(
    kern_prev_draws,
    fs::path(directory, paste0(name, "_kern_prev_draws.csv"))
  )

  purrr::pmap(
    list(
      new_data = list(ehb_prev_draws, kern_prev_draws),
      old_data = fs::path(
        dir_old,
        c(paste0(name, "_ehb_summary_stats.csv"),
          paste0(name, "_kernicterus_summary_stats.csv"))
      ) |>
        purrr::map(data.table::fread) |>
        purrr::map(hemo::prep_old) |>
        purrr::map(hemo::impute_for_2022),
      path_plot = fs::path(
        directory, paste0(name, "_", c("ehb", "kern"), "_comp_plot.pdf")
      ),
      old_year = 2021,
      new_year = 2022,
      title = paste("Prevalence of", c("EHB", "kernicterus"), "due to", name)
    ),
    hemo::plot_vs_old
  )
}
