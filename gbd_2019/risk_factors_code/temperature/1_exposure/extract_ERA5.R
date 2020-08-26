#-------------------Header------------------------------------------------
# Author: USERNAME
# Date: 2019
# Purpose: Script extarcts era % for each day of each year and GBD location (subnationals);
#          This is done so that code can be run in parallel
#          The script also creades 100 draws of each daily temperature pixel using the sread provided in the ERA5 data set
#***************************************************************************


# runtime configuration

if (Sys.info()["sysname"] == "Linux") {

  j <- "/home/j/"
  h <- "/homes/USERNAME/"

} else {

  j <- "ADDRESS:"
  h <- "ADDRESS:"

}


require(ncdf4, lib.loc="/ADDRESS/USERNAME/R/")
require(raster)
require(data.table)
require(rgdal)
require(feather)


arg <- commandArgs()[-(1:5)]  # First args are for unix use only
print(arg)

YEAR=arg[1]
print(YEAR)

LOCATION_ID<-arg[2]
print(LOCATION_ID)

end_day<-ifelse(YEAR==1992|YEAR==1996|YEAR==2000|YEAR==2004|YEAR==2008|YEAR==2012|YEAR==2016,366,365)
print(end_day)

#################### Read in shapefile ###############################################################
shapefile.dir <- file.path(j, "FILEPATH")
shapefile.version <- "GBD2019_analysis_final"
shape <- readOGR(dsn = shapefile.dir, layer = shapefile.version)
sub_shape<-shape[shape@data$loc_id == LOCATION_ID,]


BRICK_temp<-brick(paste0(j,"FILEPATH",YEAR,".grd"))
BRICK_spread<-brick(paste0(j,"FILEPATH",YEAR,".grd"))


DATA<-NULL

for (i in 1:end_day){

  day_temp<-BRICK_temp[[i]]
  day_spread<-BRICK_spread[[i]]
  plot(day_temp)

  crs(day_temp)<-"+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
  crs(day_spread)<-"+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"

  temp_crop<-crop(day_temp,sub_shape)
  temp_sub<-mask(temp_crop,sub_shape)

  spread_crop<-crop(day_spread,sub_shape)
  spread_sub<-mask(spread_crop,sub_shape)
  plot(spread_crop)

  test<-is.na(mean(temp_sub@data@values,na.rm=TRUE))

  if(test==TRUE){
    temp_sub<-temp_crop
    spread_sub<-temp_crop
  }

  data_temp <- as.data.table(rasterToPoints(temp_sub, spatial = TRUE))
  colnames(data_temp)<-c("temp","x","y")
  data_spread <- as.data.table(rasterToPoints(spread_sub, spatial = TRUE))
  colnames(data_spread)<-c("spread","x","y")
  data_spread[,":="(x=x-0.125,y=y-0.125)]
  data<-merge(data_temp,data_spread,all.x=TRUE)
  data[,loc_id:=LOCATION_ID]
  data[,":="(day=i,year=YEAR)]
  DATA<-as.data.table(rbind(DATA,data))
  print(i)

}


DATA[,date:=as.Date(day-1,origin=paste0(YEAR,"-01-01"))]
DATA<-DATA[, .(x, y,temp ,spread, loc_id, date)]
DATA$spread[is.na(DATA$spread)] <- mean(data_spread$spread,na.rm=TRUE)


dir.create(file.path(paste0(j,"FILEPATH",YEAR)))
write_feather(DATA,paste0(j,"FILEPATH",YEAR,"/era5_",YEAR,"_",LOCATION_ID,".feather"))


##############################################################################################################################################
### Create draws
##############################################################################################################################################

temp<-as.data.table(read_feather(paste0(j, "FILEPATH",YEAR,"FILEPATH",YEAR,"_",LOCATION_ID,".feather")))
temp[,dailyTempCat:=as.integer(round(temp-273.15,digits = 0))]
temp[,temp_C:=temp-273.15]

ann_mean<-as.data.table(read_feather(paste0(j, "FILEPATH",LOCATION_ID,".feather")))[,1:3]
colnames(ann_mean)<-c("meanTempCat","x","y")
ann_mean[,meanTempCat:=as.integer(round(meanTempCat-273.15,digits = 0))]
temp<-merge(temp,ann_mean,by=c("x","y"),all.x=TRUE)

#### create a 1000 draws

temp[ ,c(paste0("temp_",0:999)):=lapply(.SD,function(x){rnorm(1,temp_C,spread)}),by=.(x,y,date)]

dir.create(paste0(j, "FILEPATH",YEAR,"/draws"))
write_feather(temp,paste0(j, "FILEPATH",YEAR,"FILEPATH",YEAR,"_",LOCATION_ID,".feather"))

print("end2")
