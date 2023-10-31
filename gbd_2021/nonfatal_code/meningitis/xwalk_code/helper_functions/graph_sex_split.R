#' @author
#' @date 2020/06/22
#' @description graph sex split data
#' @function graph_sex_split
#' @param dt sex-split data
#' @param out_dir output directory
#' @param model_name name of the model
#' @return nothing returned, a plot is saved in the output directory
#' @note 

# MAKE A GRAPH ---------------------------------------------------------
graph_sex_split <- function(dt, out_dir, model_name) {
  pdf(paste0(out_dir, model_name, "_sex_split_graph.pdf"))
  gg_sex <- ggplot(dt) + 
    geom_point(aes(x = mean, y = m_mean, color = 'M')) +
    geom_point(aes(x = mean, y = f_mean, color = 'F')) +
    labs(x = 'Both Sex Mean', y = 'Sex Split Means', color = 'Sex') +
    geom_abline(slope = 1, intercept = 0) +
    scale_color_manual(values =c('M' = 'midnightblue', 'F' = 'purple')) +
    ggtitle('Sex Split Means Compared to Both Sex Mean') 
  print(gg_sex)
  dev.off()
}