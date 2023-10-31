############################################################################################################################
## DESCRIPTION: This script reads in redistributed data and merges it to meteorological data (heat index, wbt and temperature) 
## INPUTS: Redistributed individual mortality data; daily meteorological data for admin 2 
## OUTPUTS: One file with Redistributed mortality data and meteorological data including heat indices 
## AUTHOR: EOD
## DATE CREATED: 
#############################################################################################################################
### Note admin2 is finer than admin1

rm(list=ls())

require(data.table)
require(feather)


##### get location metadata 
source("/FILEPATH/get_location_metadata.R")
locations <- get_location_metadata(location_set_id = 35, gbd_round_id = 6, decomp_step = "step4")


#data<-fread("/FILEPATH/.csv")

mort<-fread("/FILEPATH/.csv")
mort<-mort[,-c("dewp","temp","heat_index","population","mean_temp")]

MET<-NULL
years<-ts(1980:2020)
isos<-c("BRA","CAN","CHL","COL","GTM","MEX","NZL","USA")

for(j in 1:length(isos)){

  for (i in 1:length(years)){
  met<-fread(paste0("/FILEPATH/",isos[j],"/",isos[j],"_temperature_",years[i],".csv"))
  met[,loc:=tolower(isos[j])]
  MET<-rbind(MET,met)
  print(paste0("i equals ", i))
  }
  print(j)
}

ZONE<-NULL
for (j in 1:length(isos)){
zone<-fread(paste0("/FILEPATH/",isos[j],"/",isos[j],"_meanAllTemps.csv"))
ZONE<-rbind(ZONE,zone)
print(j)
}


mm<-merge(MET,ZONE,by=c("zonecode","loc"),all.x=TRUE)


MET<-as.data.table(read_feather("/FILEPATH/.feather"))
quantile(MET$mean_temp,probs=c(0.05,0.95))
names(MET)
data<-merge(mort,MET,by=c("zonecode","loc","date"),all.x = TRUE, all.y = FALSE)

names(data)


write_feather(data,"/FILEPATH/.feather")
