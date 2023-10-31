
### ESTABLISH RUNNING CONDITIONS ###
makeFile <- "yes"	# options are yes, no
tempVar <- "dailyTemp"	# options are dailyTemp, tempTmrelDeviation, dailyTempDeviation, dailyHI, dailyWBT
catVar <- "meanTempDegree" # options are loc, meanTempCat, rangeCat, meanTempDegree,meanHIDegree, meanWBTDegree
catVar2 <- "adm1"
ref <- "zoneMean" # default is 0 for deviations & 15_25 for dailyTemp (zoneMean); alternative is maxMort or zoneMean; added zoneMeanHI and zoneMeanWBT
version <- "withZaf" #rrShift" # name to give folder
#version <- "gbd2019" #rrShift" # name to give folder
zoneLimits<-data.frame(lower=6,upper=28)

data<-fread("FILEPATH/.csv")



data1<-fread(paste0("FILEPATH", catVar, "_", catVar2, "_", tempVar, "_", zoneLimits$lower, "-", zoneLimits$upper, ".csv"))
data2<-fread(paste0("FILEPATH/", "/data/mrBrt_R_", catVar, "_", catVar2, "_", tempVar, "_ref", ref, "_", zoneLimits$lower, "-", zoneLimits$upper, ".csv"))


str(data1)
dim(data)
dim(data2)
table(is.na(data))
table(is.na(data2$lnRr_ckd))

str(data)

nrow(data1)
nrow(data2)
table(is.na(data1))
table(is.na(data2))
names(data1)
names(data2)
dim(data1)
dim(data2)




############# Add extrapolation matrix 
data<-fread(paste0("FILEPATH", "/data/mrBrt_R_", catVar, "_", catVar2, "_", tempVar, "_ref", ref, "_", zoneLimits$lower, "-", zoneLimits$upper, ".csv"))
range(data$dailyTempCat,na.rm=TRUE)
DD<-NULL

i<-6


for (i in 6:35){
meanTempDegree<-rep(i,75)
dailyTempDegree<-seq(-24,50,by=1)

dd<-cbind(meanTempDegree,dailyTempDegree)
DD<-rbind(DD,dd)

print(i)
}
dim(DD)
dim(data)
cc<-data[1,6:569]

CC<-rbind(cc,cc)
str(cc)

CC <- cc[rep(seq_len(nrow(cc)), each = 2250), ] 
CC[,]<-NA
View(CC)
dim(CC)
dim(DD)
rbind()

DATA<-cbind(DD,CC)

write.csv(DATA, paste0("FILEPATH", "/data/mrBrt_R_", catVar, "_", catVar2, "_", tempVar, "_ref", ref, "_", "extrapolMatrix.csv"), row.names = FALSE)


View(DATA)
View(CC)
View(cc)
View(cc)
dd[,1:569]<-NA
str(dd)

DailyTempDegree<-1

DD<-data[meanTempDegree==MeanTempDegree]

range(DD$dailyTempCat)
max<-max(DD$dailyTempCat)

dd$meanTempDegree[1]<-6
range
dd$dailyTempCat[1]<-29


range(data$meanTempDegree,na.rm = TRUE)
range(data$dailyTempCat,na.rm = TRUE)
str(data$meanTempDegree)

str(dd$m)


DD<-rbind(DD,dd)


str(dd[,1:3])

DATA<-rbind(dd,data)
str(DATA)
dy<-replace(df, 2,'blueberry')

str(dd)
dim(data)
str(data)
range(data$meanTempDegree,na.rm = TRUE)
range(data$dailyTempCat,na.rm = TRUE)
unique(data$adm1)
unique(data$loc)
