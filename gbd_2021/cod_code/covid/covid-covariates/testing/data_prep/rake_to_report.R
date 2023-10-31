rake_to_report <- function(testing_data, path) {
  cumul_dt <- testing_data[, .(
    cumul_report = sum(daily_total_reported, na.rm = T),
    cumul_smoothed = sum(daily_total, na.rm = T)
  ),
  by = .(location_name, location_id)
  ]
  cumul_dt[, rake_ratio := cumul_report / cumul_smoothed]

  # create plots to check data:
  hist(cumul_dt$rake_ratio, xlab = "Cumulative reported/cumulative smoothed", main = "")
  outliers <- cumul_dt[rake_ratio > 1.2]$location_name
  testing_data[, cumul_report := cumsum(ifelse(is.na(daily_total_reported), 0, daily_total_reported)),
    by = "location_id"
  ]
  testing_data[, cumul_smooth := cumsum(ifelse(is.na(daily_total), 0, daily_total)), by = "location_id"]
  pdf(file.path(path, "ratio_outliers.pdf"), width = 13, height = 8)
  for (this_loc in outliers) {
    gg1 <- ggplot(testing_data[location_name == this_loc], aes(x = date)) +
      geom_col(aes(y = daily_total_reported), alpha = .5) +
      geom_line(aes(y = daily_total)) +
      theme_bw() +
      labs(title = paste(this_loc, "Total Tests"))

    print(gg1)

    gg2 <- ggplot(testing_data[location_name == this_loc], aes(x = date)) +
      geom_col(aes(y = cumul_report), alpha = .5) +
      geom_line(aes(y = cumul_smooth)) +
      theme_bw() +
      labs(title = paste(this_loc, "Total Tests"))

    print(gg2)
  }
  dev.off()

  # Merge on ratio and scale
  testing_data <- merge(testing_data, cumul_dt[, .(location_id, rake_ratio)], by = "location_id")
  testing_data[, daily_total := daily_total * rake_ratio]
  testing_data[, rake_ratio := NULL]

  return(testing_data)
}
