## PLOTS MEAN VS ADJUSTED MEAN AFTER XWALKING AND SAVES PDF

plot_mean_mean_adj <- function(plot_dt, acause, out_dir) {
  model_plot_dir <- paste0(out_dir, "/plots/")
  dir.create(model_plot_dir)
  plot_dt <- plot_dt[def != ""]
  plot_dt <- plot_dt[measure == "incidence"]
  plot_dt <- plot_dt[is_outlier == 0]
  plot_dt[is.na(mean_adj), mean_adj := mean]
  plot_dt[is.na(se_adj), se_adj := standard_error]
  g <- ggplot(plot_dt, aes(x = mean, y = mean_adj)) + geom_point(aes(color = def)) +
    geom_errorbar(aes(ymin = max(mean_adj - 1.96 * se_adj, 0), 
                      ymax = mean_adj + 1.96 * se_adj, color = def), width = 0.0000001) +
    geom_errorbarh(aes(xmin = max(mean - 1.96 * standard_error, 0), 
                       xmax = mean + 1.96 * standard_error, color = def), height =0.0000001) +
    geom_abline(color = "red", alpha = 0.2) + 
    theme_bw() + xlab("Mean") + ylab("Adjusted mean") + ggtitle("Incidence xwalk")
  saveRDS(g, paste0(model_plot_dir, date, "_", acause, "_mean_vs_mean_adj_plot_object.RDS"))
  # remove points with large SE and adjusted SE to view better
  g2 <- ggplot(plot_dt[standard_error < 0.2 & se_adj < 0.2], 
               aes(x = mean, y = mean_adj)) + geom_point(aes(color = def)) +
    geom_errorbar(aes(ymin = max(mean_adj - 1.96 * se_adj, 0), 
                      ymax = mean_adj + 1.96 * se_adj, color = def), width = 0.0000001) +
    geom_errorbarh(aes(xmin = max(mean - 1.96 * standard_error, 0), 
                       xmax = mean + 1.96 * standard_error, color = def), height =0.0000001) +
    geom_abline(color = "red", alpha = 0.2) + 
    theme_bw() + xlab("Mean") + ylab("Adjusted mean") + ggtitle("Incidence xwalk")
  pdf(paste0(model_plot_dir, date, "_", acause, "_mean_vs_mean_adj_plot_object.pdf"))
  print(g2)
  dev.off()
  print(paste0("Plot saved in", model_plot_dir, date, "_", acause, "_mean_vs_mean_adj_plot_object.pdf"))
}

