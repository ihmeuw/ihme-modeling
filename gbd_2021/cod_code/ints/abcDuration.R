library(dplyr)
library(tidyr)
library(readxl)

duration <- read_excel(FILEPATH)
duration <- duration[duration$exclude==0 & is.na(duration$exclude)==FALSE,]

duration$median[!is.na(duration$mean)] <- median(rnbinom(1000000, size = 0.87, mu = 4.83877551020408))

refMedian <- weighted.mean(duration$median, duration$sample_size, na.rm = TRUE)
ref25 <- weighted.mean(as.numeric(duration$lower[grepl("IQR", duration$note_SR)==1]), 
                       as.numeric(duration$sample_size[grepl("IQR", duration$note_SR)==1]), na.rm = TRUE)
ref75 <- weighted.mean(as.numeric(duration$upper[grepl("IQR", duration$note_SR)==1]), 
                       as.numeric(duration$sample_size[grepl("IQR", duration$note_SR)==1]), na.rm = TRUE)
refMin <- weighted.mean(as.numeric(duration$lower[grepl("IQR", duration$note_SR)==0]), 
                       as.numeric(duration$sample_size[grepl("IQR", duration$note_SR)==0]), na.rm = TRUE)
refMax <- weighted.mean(as.numeric(duration$upper[grepl("IQR", duration$note_SR)==0]), 
                       as.numeric(duration$sample_size[grepl("IQR", duration$note_SR)==0]), na.rm = TRUE)



ecdf_fun <- function(x,perc) ecdf(x)(perc)


paramTest = function(a,b, R) {

  r1 <- replicate(R, rnbinom(n = 39,  size = a, prob = b)+1) 
  r2 <- replicate(R, rnbinom(n = 21,  size = a, prob = b)+1) 
  r3 <- replicate(R, rnbinom(n = 146, size = a, prob = b)+1) 
  r4 <- replicate(R, rnbinom(n = 622, size = a, prob = b)+1) 
  r5 <- replicate(R, rnbinom(n = 98,  size = a, prob = b)+1) 

  deviations <- c(rep(c(ecdf_fun(r1, 1)-0, ecdf_fun(r1, 6.5)-0.5, ecdf_fun(r1, 50)-1), times = 39),
           rep(c(ecdf_fun(r2, 1)-0, ecdf_fun(r2, 7.7)-0.5, ecdf_fun(r2, 30)-1), times = 21),
           rep(c(ecdf_fun(r3, 4)-0.25, ecdf_fun(r3, 7)-0.5, ecdf_fun(r3, 10)-0.75), times = 146),
           rep(c(ecdf_fun(r4, 4)-0.25, ecdf_fun(r4, 7)-0.5, ecdf_fun(r4, 11)-0.75), times = 622),
           rep(c(ecdf_fun(r5, 3)-0.5), times = 98))
           
  data.frame(size = a, prob = b, deviation = mean(deviations), absDev = mean(abs(deviations)), rmse = sqrt(mean(deviations^2)))
  }


paramTestOut <- data.frame(size=numeric(), prob=numeric(), deviation=numeric(), absDev=numeric(), rmse=numeric())

for (x in seq(from = 0.5, to = 100, by = 0.5)) { 
   print(x)
   for (y in seq(from = 0.01, to = 1, by = 0.01)) { 
    paramTestOut <-rbind(paramTestOut, paramTest(a = x, b = y, R = 1000))
    }
  }

rmse <- paramTestOut[,c(1, 2, 5)]
rmse2 <- reshape(rmse, timevar = "size", idvar = "prob", direction = "wide")
rmse2 <- rmse2[,-1]
rmse3 <- as.matrix(rmse2)


persp( seq(from = 0.01, to = 1, by = 0.01), seq(from = 0.5, to = 100, by = 0.5), rmse3)

best <- paramTestOut[paramTestOut$rmse<0.1,]
paramTestOut2 <- data.frame(size=numeric(), prob=numeric(), deviation=numeric(), absDev=numeric(), rmse=numeric())

for (i in 1:length(best$size)) { 
  print(i)
  paramTestOut2 <-rbind(paramTestOut2, paramTest(a = best$size[i], b = best$prob[i], R = 10000))
  }


paramTestOut3 <- data.frame(size=numeric(), prob=numeric(), deviation=numeric(), absDev=numeric(), rmse=numeric())

for (x in seq(from = 1.5, to = 2.5, by = 0.1)) { 
  for (y in seq(from = 0.14, to = 0.26, by = 0.01)) { 
    print(c(x,y))
    paramTestOut3 <-rbind(paramTestOut3, paramTest(a = x, b = y, R = 10000))
  }
}


paramTestOut4 <- data.frame(size=numeric(), prob=numeric(), deviation=numeric(), absDev=numeric(), rmse=numeric())

for (x in seq(from = 1.5, to = 2.5, by = 0.1)) { 
  for (y in seq(from = 0.14, to = 0.26, by = 0.01)) { 
    print(c(x,y))
    paramTestOut4 <-rbind(paramTestOut3, paramTest(a = x, b = y, R = 100000))
  }
}




doOnce = function(a,b, R) {
  getStats = function(N,size,prob, Q=c(0,0.5, 1)) {
    rnbinom(N, size, prob)+1
  }
  
  replicate(R, getStats(39,a,b)) 
  
  replicate(R, getStats(21,a,b)) 
  replicate(R,getStats(146, a, b, Q=c(0.25, .5, .75)))  
  replicate(R,getStats(622, a, b, Q=c(0.25, .5, .75))) 
  replicate(R,getStats(98, a, b, Q=c(0.5))) 
}

doOnce(a = 0.1, b = 0.1, R = 10)

ecdf_fun <- function(x,perc) ecdf(x)(perc)