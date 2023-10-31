
### script creates daily rasters from hourlyfor spread
### can be paralallized by year but if only individual years are added it can be run unparalallized

# runtime configuration

if (Sys.info()["sysname"] == "Linux") {
  
  j <- "/FILEPATH/" 
  h <- "/FILEPATH/"
  
} else { 
  
  j <- "J:"
  h <- "H:"
  
}

require(ncdf4)
require(raster)
require(data.table)
require(rgdal)
require(feather)

################### Define variables ################################################################
YEAR<-2020

arg <- commandArgs()[-(1:5)]  # First args are for unix use only
print(arg)

YEAR=arg[1]
print(YEAR)

end<-ifelse(YEAR==1992|YEAR==1996|YEAR==2000|YEAR==2004|YEAR==2008|YEAR==2012|YEAR==2016,2928,2920)
end_day<-ifelse(YEAR==1992|YEAR==1996|YEAR==2000|YEAR==2004|YEAR==2008|YEAR==2012|YEAR==2016,366,365)

#################### Read in raster ###############################################################
BRICK<-brick(paste0(j,"FILEPATH/era5_spread_",YEAR,".nc"))


for (i in 1:end){
  
  file_name<-paste0("spread_",i)
  file<-rotate(BRICK[[i]])
  assign(file_name,file)
  print(i)
}


BRICK<-stack()

for(i in 1:end_day){  
  
  a<-((i-1)*8)+1
  
  STACK<-stack(get(paste0("spread_",a)),get(paste0("spread_",a+1)),get(paste0("spread_",a+2)),get(paste0("spread_",a+3)),
               get(paste0("spread_",a+4)),get(paste0("spread_",a+5)),get(paste0("spread_",a+6)),get(paste0("spread_",a+7)))
  
  STACK_dis<-disaggregate(STACK,fact=2)
  mean_name<-paste0("mean_",i)
  mean <- calc(STACK_dis, fun = sum)/8
  assign(mean_name,mean)
  BRICK<-stack(BRICK,mean)
  
  print(i)
  
}


writeRaster(BRICK,filename=file.path(paste0(j,"FILEPATH/spread_daily_",YEAR,".nc")), overwrite=TRUE, format="CDF")

print("end")
