#Post Ebola sequelae decay curve

#load in data
evd_data<-read.csv(FILEPATH)


#using means
dataset<-data.frame(y=NA, x=NA)
for (i in 1:nrow(evd_data)){
  input_expanded<-cbind(y=mean(c(evd_data$value_min[i], evd_data$value_max[i])),x=log(mean(c(evd_data$time_start[i], evd_data$time_end[i]))))
  dataset<-rbind(dataset, input_expanded)
}


dataset<-na.omit(dataset)
model<-lm(y~x, data=dataset)


#assess duration
years<-seq(0, 20, 0.05)
#for each year, calculate the value with CI
new_data<-data.frame(x=log(years))

intercept<-coef(summary(model))[1,1]
intercept_ste<-coef(summary(model))[1,2]
mean_ste<-coef(summary(model))[2,2]
mean<-coef(summary(model))[2,1]

mean_draws<-runif(1000, mean-1.96*mean_ste, mean+1.96*mean_ste)
intercept_draws<-runif(1000, intercept-1.96*intercept_ste, intercept+1.96*intercept_ste)

results<-list()  
result_draw<-data.frame(x=years,y=rep(NA,length(years) ))

for (i in 1:1000){
  
for(j in 1:nrow(result_draw)){
 result_draw$y[j]<-(mean_draws[i]*log(result_draw$x[j]))+intercept_draws[i]
 result_draw$y[1]<-1
}
  results[[i]]<-result_draw
}

for (i in 1:1000){
  for (j in 1:nrow(results[[i]]-1)){
    
    results[[i]]$value[j]<-((results[[i]]$x[j+1]-results[[i]]$x[1])*(results[[i]]$y[j]-results[[i]]$y[j+1]))/2
    
  }
  results[[i]]$value[nrow(results[[i]])]<-0
}
duration<-NA

for (i in 1:1000){
  duration[i]<-sum(results[[i]]$value)
}

mean(duration)
quantile(duration, probs=c(0.05, 0.5,0.95))

central_tendency<-result_draw
for(j in 1:nrow(central_tendency)){
  central_tendency$y[j]<-(mean*log(central_tendency$x[j]))+intercept
  central_tendency$y[1]<-1
}
