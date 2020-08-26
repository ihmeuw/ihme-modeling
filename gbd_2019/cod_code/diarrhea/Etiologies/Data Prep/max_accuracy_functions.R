## This file contains some of the functions requred for 
## calculating the sensitivity and specificity of 
## diagnostics. 
########################################################

## Function for cutoff values ##
cutoff <- function(value, data=subset(keep.tac, !is.na(case))){
  tac <- data
  x <- value
  path <- pathogen
  cut <- ifelse(tac[,path]<x,1,0)
}

## Calculate maximum accuracy ##
accuracy.fun <- function(b=35, data=subset(keep.tac, !is.na(case))){
  r <- predictive(cutoff(b), data[,"case"])
  accuracy <- r #r$accuracy
  return(accuracy)
}

## Cross table function ## 
predictive <- function(x, y){
  r <- table(x, y)
  prevalence <- r[2,2]/colSums(r)[2]
  positive <- r[2,2]/rowSums(r)[2]
  negative <- r[1,1]/rowSums(r)[1]
  accuracy <- (r[1,1]+r[2,2])/length(y)
  odds <- r[2,2]*r[1,1]/r[1,2]/r[2,1]
  paf <- prevalence*(1-1/odds)
  
  out <- data.frame(prevalence, positive, negative,accuracy,odds, paf)
}

cross_table <- function(value, data, pathogen){
  case <- data$case
  positive <- ifelse(data[,pathogen] < value, 1, 0)
  r <- table(positive, case)
  prevalence_case <- r[2,2]/colSums(r)[2]
  prevalence_control <- r[2,1]/colSums(r)[1]
  prevalence_total <- r[2,2] / length(case)
  positive <- r[2,2]/rowSums(r)[2]
  negative <- r[1,1]/rowSums(r)[1]
  sensitivity <- r[2,2]/(r[1,2]+r[2,2])
  specificity <- r[1,1]/(r[1,1]+r[2,1])
  accuracy <- (r[1,1]+r[2,2])/length(case)
  odds <- r[2,2]*r[1,1]/r[1,2]/r[2,1]
  paf <- prevalence_case*(1-1/odds)
  
  out <- data.frame(prevalence_case, prevalence_control, prevalence_total, positive, negative, sensitivity, specificity, accuracy, odds, paf)
}