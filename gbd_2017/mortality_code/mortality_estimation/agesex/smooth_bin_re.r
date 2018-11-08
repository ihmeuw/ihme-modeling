###########################################################################
## 2 May 2011
## Description: creates a smoothed version of the 'bin' random effect from
##              the sex model (fit in stata)
###########################################################################

## load data
  rm(list=ls())
  library(foreign)

  input_file <- commandArgs()[2]
  output_file <- commandArgs()[3]

  print(input_file)
  print(output_file)

  stata <- read.dta(input_file, convert.underscore=F, convert.factors=F)
  model <- unique(stata$model)
  var.name <- unique(stata$var)
  names(stata)[names(stata)==var.name] <- "idv"

## fit sex model loess
if (model == "sex_model") {

  alpha  <- 0.4
  loess.model  <- loess(re_bin ~ idv, data=stata, span=alpha, control=loess.control(surface=c("direct")))
  range <- seq(0, 1, by=0.001)
  fitted <- predict(loess.model, newdata=range, se=TRUE)
  fitted <- data.frame(idv=range, re_bin_pred=fitted$fit, re_bin_pred_se=fitted$se.fit)

  ## make trend flat once it starts increasing again
  increase <- c(0, fitted$re_bin_pred[2:nrow(fitted)] - fitted$re_bin_pred[1:(nrow(fitted)-1)])
  constant <- fitted$re_bin_pred[increase>0 & c(0, increase[1:nrow(fitted)-1])<0]
  constant <- constant[constant<0]
  fitted$re_bin_pred[which(fitted$re_bin_pred %in% constant)[1]:nrow(fitted)] <- constant[1]

## fit age model loess
} else {

  alpha  <- 0.3
  loess.model  <- loess(re_bin ~ idv, data=stata, span=alpha, control=loess.control(surface=c("direct")))
  range <- seq(-9,0,by=0.01)
  fitted <- predict(loess.model, newdata=range, se=TRUE)
  fitted <- data.frame(idv=range, re_bin_pred=fitted$fit, re_bin_pred_se=fitted$se.fit)

  ## level off at highest on left
  if (grepl("lnn", model)) {
  min.data <- max(stata$idv[stata$bin==1])
  max.est.left <- max(fitted$re_bin_pred[fitted$idv<min.data])
  edge <- min(fitted$idv[fitted$re_bin_pred == max.est.left])
  fitted$re_bin_pred[fitted$idv < edge] <- max.est.left
  }

  ## level off at lowest point on left
  if (grepl("pnn|ch|enn", model)) {
  min.data <- max(stata$idv[stata$bin==1])
  min.est.left <- min(fitted$re_bin_pred[fitted$idv<min.data])
  edge <- min(fitted$idv[fitted$re_bin_pred == min.est.left])
  fitted$re_bin_pred[fitted$idv < edge] <- min.est.left
  }

  ## do nothing on left: inf

  ## level off at highest point on right
  if (grepl("lnn|ch", model)) {
    max.data <- min(stata$idv[stata$bin==20])
    max.est.right <- max(fitted$re_bin_pred[fitted$idv>max.data])
    edge <- max(fitted$idv[fitted$re_bin_pred == max.est.right])
    fitted$re_bin_pred[fitted$idv > edge] <- max.est.right
  }

  ## level off at lowest point on right
  if (grepl("enn|pnn|inf", model)) {
    max.data <- min(stata$idv[stata$bin==20])
    min.est.right <- min(fitted$re_bin_pred[fitted$idv>max.data])
    edge <- max(fitted$idv[fitted$re_bin_pred == min.est.right])
    fitted$re_bin_pred[fitted$idv > edge] <- min.est.right
  }
}
  names(fitted)[names(fitted)=="idv"] <- var.name

## output to read into stata when actually apply model
  write.dta(fitted, file=output_file)
