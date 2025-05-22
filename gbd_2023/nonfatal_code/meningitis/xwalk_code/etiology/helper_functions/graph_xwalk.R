#' @description graph crosswalked data
#' @function graph_xwalk
#' @param dt output of adjust_orig_vals from crosswalk package
#' @param out_dir output directory
#' @param model_name name of the model
#' @return nothing returned, a plot is saved in the output directory
#' @note 

# MAKE A GRAPH ---------------------------------------------------------
graph_xwalk <- function(dt, out_dir, model_name) {
  # set mean_adj to mean for unadjusted data
  dt[is.na(mean_adj), mean_adj := mean]
  dt[is.na(se_adjusted), se_adjusted := standard_error]
  # plot with NO error bars
  pdf(paste0(out_dir, model_name, "_mean_mean_adj_error_bars.pdf"))
  g <- ggplot(dt, aes(x = mean, y = mean_adj)) + geom_point(aes(color = definition)) +
    geom_errorbar(aes(ymin = max(mean_adj - 1.96 * se_adjusted, 0),
                      ymax = mean_adj + 1.96 * se_adjusted, color = definition, alpha = 0.2), width = 0.0000001) +
    geom_errorbarh(aes(xmin = max(mean - 1.96 * standard_error, 0),
                       xmax = mean + 1.96 * standard_error, color = definition, alpha = 0.2), height =0.0000001) +
    geom_abline(color = "red", alpha = 0.2) + 
    theme(legend.position = "bottom") + xlab("Mean") + ylab("Adjusted mean") 
  print(g)
  dev.off()
  pdf(paste0(out_dir, model_name, "_mean_mean_adj.pdf"))
  g <- ggplot(dt, aes(x = mean, y = mean_adj, color = definition)) + geom_point() +
    geom_abline(color = "black", alpha = 0.2) + 
    theme(legend.position = "bottom") + xlab("Mean") + ylab("Adjusted mean") + ggtitle(model_name)
  print(g)
  dev.off()
}