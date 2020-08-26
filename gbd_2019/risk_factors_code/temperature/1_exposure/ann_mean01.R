
rm(list=ls())

### script creates daily rasters from hourly

# runtime configuration

if (Sys.info()["sysname"] == "Linux") {

  j <- "ADDRESS"
  h <- "ADDRESS/USERNAME/"

} else {

  j <- "ADDRESS"
  h <- "ADDRESS"

}

require(ncdf4, lib.loc="FILEPATH")
require(raster)
require(data.table)
require(rgdal)
require(feather)


YEARS<-seq(1980,2018)

end_day<-ifelse(YEAR==1992|YEAR==1996|YEAR==2000|YEAR==2004|YEAR==2008|YEAR==2012|YEAR==2016,366,365)

STACK<-stack()

for (i in 1:length(YEARS)){
  YEAR<-YEARS[i]
  temp<-brick(paste0(j,"FILEPATH",YEARS[i],".grd"))

  end_day<-ifelse(YEAR==1992|YEAR==1996|YEAR==2000|YEAR==2004|YEAR==2008|YEAR==2012|YEAR==2016,366,365)

  file_name<-paste0("mean_",YEARS[i])
  ann_mean <- calc(temp, fun = sum)/end_day
  assign(file_name,ann_mean)

  STACK<-stack(STACK,ann_mean)

  print(i)

  }

mean<-calc(STACK,fun=mean)
plot(mean,main="mean")

writeRaster(mean,filename=file.path(paste0("FILEPATH/annual_mean.grd")), overwrite=TRUE, format="raster")


