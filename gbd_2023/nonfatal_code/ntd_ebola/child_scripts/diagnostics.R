####################################################################
#' [Diagnostics]
####################################################################

#' [Single Parameter Draws]
# Histogram. Specify outfile or prints to console


single_param_hist <- function("FILEPATH", title = "", out_file = NA, bin_width = NA){
  
  library(data.table)
  library(ggplot2)
  library(stringr)
  
  data  <- fread("FILEPATH")
  data  <- data.table("draws" = as.numeric(t(data)[2:1001]))

  if (!is.na(out_file)){
    pdf(out_file)
  }
  
  if (is.na(bin_width)){
    range   <- max(data[, draws]) - min(data[,draws])
    bin_width <- range/30
  }
   
  p <- ggplot(data, aes(x = draws)) +
    geom_histogram(fill = "#F67250", binwidth = bin_width) +
    labs(x = "Draws", y = "Count", title = title) +
    theme_bw()+
    theme(plot.title = element_text(hjust = 0.5))
  
  print(p)
  
  if (!is.na(out_file)){
    dev.off()
  }
  
}
  
#' [Top 15 Countries by Measure by Year]

library(ggplot2)
library(data.table)
library(RColorBrewer)
library(stringr)

##
