### Maximizes accuracy (correctly assigns case-control status) for GEMS reanalysis using TAC data ###
## Note that internal IHME filepaths and directories have been renamed "FILEPATH" for public viewing ##
## Thanks for reading! ##

library(ggplot2)
library(survival)
library(reshape2)

gems <- read.csv("FILEPATH/gems_final.csv")
tac <- subset(gems, !is.na(case.control))

## Function for cutoff values ##
cutoff <- function(value, data=tac){
  tac <- data
  x <- value
  path <- pathogen
  cut <- ifelse(tac[,path]<x,1,0)
}

## Calculate maximum accuracy ##
accuracy.fun <- function(b=35){
  r <- predictive(cutoff(b), tac$case.control)
  accuracy <- r$accuracy
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

######### Primary analysis! ###############
## Where is accuracy maximized?
pathogens <- c("tac_adenovirus","tac_aeromonas","tac_campylobacter","tac_cryptosporidium","tac_entamoeba", "tac_EAEC",
               "tac_TEPEC", "tac_norovirus", "tac_rotavirus","tac_salmonella", "tac_shigella_eiec","tac_ETEC","tac_v_cholerae")

#### Create loess curves for each, find loess maximum #####
n <- 1
span <- 0.3
name <- span*10
loess.out <- data.frame()
pdf(paste0("FILEPATH/loess_accuracy",name,".pdf"))
for(p in pathogens){
  pathogen <- p
  output <- data.frame()
  if(p == "tac_aeromonas"){
    min <- 25
  } else {
     min <- floor(min(tac[,pathogen]))+1
  }
  
  for(i in seq(min,35,.02)){
    a <- accuracy.fun(i)
    ad <- data.frame(accuracy= a, ct=i)
    output <- rbind(output, ad)
  }
  
  l <- loess(accuracy ~ ct,  data=output, span=span)
  output$p <- predict(l, data.frame(ct=seq(min,35,.02)))
  infl <- c(FALSE, diff(diff(output$p)>0)!=0)
  output$diff <- c(NA, diff(output$p))
  output <- output[!is.na(output$diff),]
  test <- subset(output, diff > 0)
  t2 <- subset(test, ct<34 & ct>15)
  q <- output$ct[which.min(output$diff)]
  o <- test$ct[which.max(test$p)]
  infl <- c(FALSE, diff(diff(test$p)>0)!=0)
  if(length(infl[infl==T])==0){
    r <- o
  } else {
    r <- test$ct[test$p==max(test$p[infl==T])]
  }
  
  max <- output$ct[which.max(output$p)]
  min.diff1 <- output$p - (max(output$p)-0.005)
  intercept1 <- output$ct[which.min(abs(min.diff1))]

  g <- ggplot(data=output, aes(x=ct, y=accuracy)) + geom_point() +
    geom_line(aes(x=ct, y=p), col="blue") + 
    geom_vline(xintercept=accuracy$ct[n], col="red") +
    geom_vline(xintercept=intercept1, col="purple") + geom_hline(yintercept=max(output$p)-0.005, col="purple", lty=3) +
    geom_vline(xintercept=r, col="blue") +
     ylab("Accuracy of Diagnostic") + xlab("Cycle Threshold") +
    ggtitle(paste(p,"Span =",span)) 
  print(g)
  r <- ifelse(length(r)>0,r,NA)
  result <- data.frame(pathogen = p, span=span, max.loess=max(output$p), ct.max=output$ct[which.max(output$accuracy)], ct.inflection=r, ct.noimprove=intercept1) 
  loess.out <- rbind.data.frame(loess.out, result)
  n <- n+1
}
dev.off()
write.csv(loess.out, "FILEPATH/min_loess_accuracy.csv")

######### END Main Function #############
