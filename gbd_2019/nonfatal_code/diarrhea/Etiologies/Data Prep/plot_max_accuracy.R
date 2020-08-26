## Plot maximum TAC accuracy in MAL-ED ##
max_acc <- read.csv("filepath")
combined <- read.csv("filepath")
source('filepath/max_accuracy_functions.R')
loess.out <- data.frame(loess.out)
for(p in tac.names[tac.names!="c_difficile"]){
  pathogen <- p
  
  if(type=="maled"){
    gbd_ct <- max_acc$ct.inflection[max_acc$tac.names==p]
  } else {
    gbd_ct <- max_acc$ct.inflection[max_acc$pathogen==p]
  }
  
  output <- data.frame()
  if(p == "aeromonas"){
    min <- 25
  } else {
    min <- floor(min(keep.tac[,pathogen], na.rm=T))+1
  }
  if(min(keep.tac[,pathogen], na.rm=T)<10){
    min <- 10
  } else {
    min <- floor(min(keep.tac[,pathogen], na.rm=T)) + 1
  }
  is_case <- subset(keep.tac, !is.na(case))$case
  prev_control <- c()
  for(i in seq(min,35,interval)){
    a <- accuracy.fun(i, data=subset(keep.tac, !is.na(case)))
    ad <- data.frame(a, ct=i)
    prev_control <- c(prev_control, mean(cutoff(i)[is_case==0], na.rm=T))
    output <- rbind(output, ad)
  }
  output$prevalence_controls <- prev_control
  output <- subset(output, prevalence > 0)
  min <- min(output$ct)
  
  l <- loess(accuracy ~ ct,  data=output, span=span)
  output$p <- predict(l, data.frame(ct=seq(min,35,interval)))
  infl <- c(FALSE, diff(diff(output$p)>0)!=0)
  output$diff <- c(NA, diff(output$p))
  output <- output[!is.na(output$diff),]
  test <- subset(output, diff > 0)
  t2 <- subset(test, ct<34 & ct>15)
  q <- output$ct[which.min(output$diff)]
  o <- test$ct[which.max(test$p)]
  infl <- c(FALSE, diff(diff(output$p)>0)!=0)
  if(length(infl[infl==T])==0){
    r <- min
  } else {
    r <- output$ct[output$p==max(output$p[infl==T])]
  }
  
  output$loess_odds <- predict(loess(odds ~ ct, data=subset(output, odds!="Inf")), newdata=output)
  cut_odds <- output$ct[which.min(abs(output$loess_odds-2))]
  max <- output$ct[which.max(output$p)]
  g <- ggplot(data=output, aes(x=ct, y=accuracy)) + geom_point() + theme_bw() + 
    geom_line(aes(x=ct, y=p), col="blue") +
    geom_vline(xintercept=cut_odds, col="#663399") + annotate("text", x = cut_odds, y=min(output$accuracy)+0.02, label = "Diarrhea associated") +
    geom_vline(xintercept=r, col="#D81159") + annotate("text", x = r, y=min(output$accuracy)+0.01, label = "Max accuracy") +
    geom_vline(xintercept=gbd_ct, col="#FFBC42") + annotate("text", x = gbd_ct, y=min(output$accuracy), label = "GBD Cut-Point") +
    ylab("Accuracy of Diagnostic") + scale_x_continuous(limits=c(15,35), "Cycle Threshold") +
    ggtitle(p) 
  print(g)
  g <- ggplot(data=output, aes(x=ct, y=odds)) + geom_point() + stat_smooth(method="loess", se=F) + ggtitle(paste("Odds ratio: ",p)) + theme_bw() + 
    scale_x_continuous(limits=c(15,35), "Ct") + ylab("Odds ratio") +
    geom_hline(yintercept=1, lty=2) + geom_hline(yintercept=2, lty=2) + 
    geom_vline(xintercept=cut_odds, col="#663399") + annotate("text", x=cut_odds, y=1.75, label="Diarrhea associated") +
    geom_vline(xintercept=r, col="#D81159") + annotate("text", x = r, y=1.5, label = "Max accuracy") +
    geom_vline(xintercept=gbd_ct, col="#FFBC42") + annotate("text", x = gbd_ct, y=1.25, label = "GBD Cut-Point") 
  print(g)
  g <- ggplot(data=output, aes(x=ct, y=prevalence)) + geom_point() + geom_point(aes(y=prevalence_controls), pch=17, col="darkgray") + 
    stat_smooth(method="loess", se=F) + ggtitle(paste("Prevalence: ",p)) + theme_bw() + 
    scale_x_continuous(limits=c(15,35), "Ct") + ylab("Prevalence") +
    geom_vline(xintercept=r, col="#D81159") + annotate("text", x = r, y=0.02, label = "Max accuracy") +
    geom_vline(xintercept=cut_odds, col="#663399") + annotate("text", x=cut_odds, y=0.075, label="Diarrhea associated") +
    geom_vline(xintercept=gbd_ct, col="#FFBC42") + annotate("text", x = gbd_ct, y=0.05, label = "GBD Cut-Point") 
  print(g)
  g <- ggplot(data=output, aes(x=ct, y=paf)) + geom_line() + stat_smooth(method="loess", se=F) + ggtitle(paste("Attributable Fraction: ",p)) + theme_bw() + 
    scale_x_continuous(limits=c(15,35), "Ct") + ylab("Attributable Fraction") +
    geom_vline(xintercept=r, col="#D81159") + annotate("text", x = r, y=0.01, label = "Max accuracy") +
    geom_vline(xintercept=cut_odds, col="#663399") + annotate("text", x=cut_odds, y=0.03, label="Diarrhea associated") +
    geom_vline(xintercept=gbd_ct, col="#FFBC42") + annotate("text", x = gbd_ct, y=0.02, label = "GBD Cut-Point") 
  print(g)
  r <- ifelse(length(r)>0,r,NA)
  result <- data.frame(pathogen = p, span=span, max.loess=max(output$p), ct_inflection=r, ct_gbd_gems=gbd_ct, ct_or_2 = cut_odds) 
  loess.out <- rbind.data.frame(loess.out, result)
  n <- n+1
}