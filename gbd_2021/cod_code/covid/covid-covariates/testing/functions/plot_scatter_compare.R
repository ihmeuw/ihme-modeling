
#' Scatterplots comparing testing per capita 
#' 
# Compares new model to a prior best reference, for both daily and cumulative
# testing quantities (test_pc, cumulative_test_pc)
#'
#' @param DATASET [data.table] data.table with a reference (last production) and newly modeled testing covariate outputs
#' Require columns: date, location_name, test_pc, cumulative_test_pc
#' @param PERCENT_DIFF [numeric] what % differences from reference to label in plots? 
#' @param VARIABLE [character] quoted "variable name" you want to plot (one, quoted)
#'
#' @return [plot]
.plot_scatter_compare <- function(DATASET, PERCENT_DIFF, VARIABLE) {
  
  require(ggplot2)
  require(glue)

  NEW_VAR <- paste0(VARIABLE, "_new")
  REF_VAR <- paste0(VARIABLE, "_ref")
  
  p <- DATASET %>% 
    ggplot(aes(x = get(REF_VAR), y = get(NEW_VAR))) + 
    geom_point() + 
    facet_wrap(~date) +
    geom_abline(intercept=0, slope=1, col="red") + 
    scale_x_log10("Reference model - Last Production") +
    scale_y_log10("New model") + 
    geom_text_repel(data = DATASET[abs((get(NEW_VAR) - get(REF_VAR))/get(REF_VAR)) > PERCENT_DIFF], 
                    aes(label = location_name),
                    max.overlaps = 50) +
    ggtitle(glue(str_extract(NEW_VAR, "^.*_"), ": Locations with > {PERCENT_DIFF*100}% change")) +
    theme_bw()  
  
  print(p)
  
}

