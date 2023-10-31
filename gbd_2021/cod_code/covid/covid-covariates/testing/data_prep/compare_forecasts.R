outputs_dir <- "FILEPATH"
release1 <- "2020_05_14.05"
release2 <- "2020_05_14.04"

simple_data_path1 <- file.path(outputs_dir, release1, "forecast_raked_test_pc_simple.csv")
simple_data_path2 <- file.path(outputs_dir, release2, "forecast_raked_test_pc_simple.csv")
raw_data_path <- file.path(outputs_dir, release1, "data_smooth.csv")
out_path <- file.path(outputs_dir, release1, "smooth_comp_plot.pdf")
plot_comp <- function(simple_data_path1, simple_data_path2, raw_data_path, out_path) {
  simple_dt1 <- fread(simple_data_path1)
  simple_dt1[, version := "Non-smoothed"]
  simple_dt2 <- fread(simple_data_path2)
  simple_dt2[, version := "Smoothed"]
  dt <- rbind(simple_dt1, simple_dt2)
  dt[, date := as.Date(date)]
  raw_dt <- fread(raw_data_path)
  raw_dt[, date := as.Date(date)]
  dt[, plot_name := paste0(location_id, " - ", location_name)]
  pdf(out_path, width = 12, height = 8)
  for (i in seq(ceiling(length(sort(unique(dt$location_id))) / 12))) {
    loc_list <- sort(unique(dt$location_id))[((i - 1) * 12 + 1):(min(i * 12, length(sort(unique(dt$location_id)))))]
    plot_dt <- dt[location_id %in% loc_list]
    gg <- ggplot() +
      geom_line(data = plot_dt, aes(x = date, y = test_pc * 1e5, color = version, linetype = observed == 1)) +
      geom_point(data = raw_dt[location_id %in% loc_list], aes(x = date, y = daily_total_reported / pop * 1e5), color = "black", size = 0.1) +
      theme_bw() +
      theme(legend.position = "bottom") +
      xlab("Date") +
      ylab("Tests per 100k") +
      facet_wrap(~location_name, scales = "free_y") +
      guides(linetype = F)
    print(gg)
  }
  dev.off()
}

plot_comp(simple_data_path1, simple_data_path2, raw_data_path, out_path)