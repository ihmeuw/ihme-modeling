library(ggplot2)
library(data.table)
#' Make a dot plot of the relative difference between the data and the location-specific median
#' @param data_path Path to testing data
#' @param out_path Path to write the plot to
dot_plot <- function(data_path, out_path) {
  dt <- fread(data_path)
  dt[, date := as.Date(date)]
  dt[shift(daily_total) != 0 & daily_total != 0, pct_diff := (daily_total - shift(daily_total)) / median(daily_total) * 100, by = "location_id"]
  pdf(out_path, width = 13, height = 50)
  gg <- ggplot(dt, aes(x = pct_diff, y = location_name, alpha = date)) +
    geom_point(na.rm=TRUE) +
    geom_vline(xintercept = 0) +
    theme_bw()

  print(gg)
  invisible(dev.off())
}

#' Make location-specific time-series plots of raw and smoothed data
#' @param data_path Path to testing data
#' @param out_path Path to write the plot to
#' @param hierarchy Location hierarchy to use for sorting
loc_time_series <- function(data_path, out_path, hierarchy) {
  dt <- fread(data_path)
  dt[, date := as.Date(date)]
  high_risk_locs <- dt[daily_total_reported < 0]$location_id
  sorted <- ihme.covid::sort_hierarchy(hierarchy[location_id %in% dt$location_id])$location_id
  pdf(out_path, width = 13, height = 8)
  for (loc_id in sorted) {
    gg1 <- ggplot(dt[location_id == loc_id & !is.na(daily_total) & !is.na(daily_total_reported)], aes(x = date)) +
      geom_col(aes(y = daily_total_reported / pop * 1e5), alpha = .5) +
      geom_line(aes(y = daily_total / pop * 1e5), linetype = "dashed") +
      geom_line(aes(y = daily_total_smoothed/ pop * 1e5)) + 
      theme_bw() +
      labs(title = paste(unique(dt[location_id == loc_id]$location_name), "Total Tests")) +
      xlab("Date") + ylab("Daily tests per 100k population")

    print(gg1)
  }
  invisible(dev.off())
}