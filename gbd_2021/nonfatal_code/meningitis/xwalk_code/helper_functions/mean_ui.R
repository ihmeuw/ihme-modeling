mean_ui <- function(x){
  library(dplyr)
  y <- x[ , grepl( "draw_" , names(x) ), with = F ]
  y$mean <- apply(y, 1, mean)
  y$lower <- apply(y, 1, function(x) quantile(x, c(.025)))
  y$upper <- apply(y, 1, function(x) quantile(x, c(.975)))
  w <- y[, c('mean', 'lower', 'upper')]
  z <- x[ , !grepl( "draw_" , names(x) ), with = F ]
  v <- cbind(z, w)
  v <- as.data.table(v)
  return(v)
}
