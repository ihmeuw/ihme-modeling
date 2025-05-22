#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: RF: air_ozone
# Purpose: Create meaningful plots to explore data and compare to other versions
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "/home/j" 
  h_root <- "/homes/kcausey"
  
} else { 
  
  j_root <- "J:"
  h_root <- "H:"
  
}

# define parameters
exp.versions <- c(6,14) #versions to compare
location_set_version_id <- 367
location_set_id <- 35

# load packages, install if missing
pacman::p_load(data.table, ggplot2, grid, gridExtra, magrittr, stringr,rworldmap,plotly,ggthemes)


# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)
#***********************************************************************************************************************

#----IN/OUT-------------------------------------------------------------------------------------------------------------
##in##
data.dir <- file.path(home.dir, 'products/exp')

##out##
graphs.dir <- file.path(home.dir, 'diagnostics')

dir.create(paste0(graphs.dir,"/",exp.versions[1],"v",exp.versions[2]))

#***********************************************************************************************************************
#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
##function lib##
# GBD Map Function
map.function.dir <- file.path(j_root,"FILEPATH")
file.path(map.function.dir,"GBD_WITH_INSETS_MAPPING_FUNCTION.R") %>% source

#ubcov functions#
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "utilitybelt/db_tools.r") %>% source()

#Slower central location function
source(file.path(j_root,"tFILEPATH/get_location_metadata.R"))

#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#***********************************************************************************************************************

#----PREP---------------------------------------------------------------------------------------------------------------
#read in the appended datasets
data_1 <- file.path(data.dir,exp.versions[1],"summary/all_ozone_exposure.csv")%>% fread
data_2 <- file.path(data.dir,exp.versions[2],"summary/all.csv")%>% fread

#create columns for location and year
data_2[,c("location_id","year_id"):=tstrsplit(file,"_",keep=2:3)]
data_2$year_id <- as.numeric(data_2$year_id)
#create columns for location and year
#data_1[,c("location_id","year_id"):=tstrsplit(file,"_",keep=2:3)]
data_1$year_id <- as.numeric(data_1$year_id)
#delete unnecessary columns made by BASH
data_2$file <- NULL
data_1$file <- NULL
data_1$unofficial_location_name <- NULL

#Add in ihme_loc_id
locs <- get_location_metadata(location_set_id)
locs$location_id <- as.character(locs$location_id)

#merge
keycols <- c("location_id","year_id")
setkeyv(data_1,keycols)
setkeyv(data_2,keycols)
all_data <- merge(data_1, data_2, all=TRUE)
all_data <- merge(all_data,locs[,.(location_id,ihme_loc_id,super_region_name,region_name,parent_id)],by=c("location_id"),all.x=TRUE)


#check for missing countries:
est<-locs[is_estimate==1]
for(i in est$location_id){
  if(i %ni% data_2$location_id){print(i)}
}


#plots against last year's estimates
gg<-ggplot(data=all_data[!is.na(region_name)&region_name!=""],
       aes(x=exposure_mean.x,
           y=exposure_mean.y,
           label=ihme_loc_id,
           group=year_id,
           color=region_name)) +
  geom_point()+
  #geom_text(size=2) +
  geom_abline(slope=1) +
  #facet_wrap(~super_region_name,2)+
  labs(color="Region",x="GBD 2016 Exposure (ppb)",y="GBD 2017 Exposure (ppb)")+
  theme_bw()

gg_plotly <- ggplotly(gg)  
htmlwidgets::saveWidget(as_widget(gg_plotly), paste0(graphs.dir,"/",exp.versions[1],"v",exp.versions[2],"/ozone_plotly.html"))

#plots most recent estimates on a map
#mapping function is different location set than results
load(paste(j_root,"/DATA/SHAPE_FILES/GBD_geographies/master/GBD_2017/inset_maps/allSubs/GBD_WITH_INSETS_ALLSUBS_PREPPED.RData",sep="")) 
map.locs <- map@data[,c("iso3", "ID")]

#make a list of missing locations in data
missing <- NULL
for (l in unique(as.character(map.locs$iso3))){
  if(nrow(all_data[!is.na(exposure_mean.y) & ihme_loc_id==l])<1){
    missing <- rbind(missing, l)
  }
}
missing <- as.data.table(missing)
setnames(missing, "V1","ihme_loc_id")
missing <- merge(missing, locs[,.(ihme_loc_id,location_id)])

#drop the nationals
missing <- missing[location_id<500]

#for missing loc ids, assign them that of their parent
par<-all_data[year_id==2017 & !is.na(exposure_mean.y) & parent_id %in% missing$location_id]
length(unique(par$parent_id))



map_prep <- all_data[year_id==2017,c("ihme_loc_id","exposure_mean.y"),with=F]
names(map_prep) <- c("ihme_loc_id","mapvar")
limit <- quantile(map_prep$mapvar,probs=(seq(0,1,0.05)),na.rm=T)

gbd_map(data=map_prep,
        limits=limit,
        labels=round(limit,digits=1)[2:21],
        legend.title="Ozone (ppb)",
        col.reverse=T,
        title="GBD 2017 Ozone Exposure, 2017")

#plot GBD 2016
map_prep <- all_data[year_id==2016,c("ihme_loc_id","exposure_mean.x"),with=F]
names(map_prep) <- c("ihme_loc_id","mapvar")

gbd_map(data=map_prep,
        limits=limit,
        labels=round(limit,digits=1)[2:21],
        legend.title="Ozone (ppb)",
        col.reverse=T,
        title="GBD 2016 Ozone Exposure, 2016")

#plots only new locations
ggplot(data=all_data[is.na(exposure_mean.x)],
       aes(x=year_id,
           y=exposure_mean.y,
           by=location_id)) +
  geom_line() +
  theme_bw(base_size=6)

#map of differences
map_prep2 <- all_data[year_id==1990,list(ihme_loc_id,100*(exposure_mean.y-exposure_mean.x)/exposure_mean.x)]
names(map_prep2) <- c("ihme_loc_id","mapvar")
limit <- quantile(map_prep2$mapvar,probs=(seq(0,1,0.05)),na.rm=T)

gbd_map(data=map_prep2,
        limits=limit,
        labels=lapply(quantile(map_prep2$mapvar,probs=(seq(0.05,1,0.05)),na.rm=T),round,digits=3),
        legend.title="% change",
        col.reverse=T,
        legend.cex=.9,
        legend.col=1,
        title="% Change in GBD 1990 ozone Exposure")

#map of differences (2010)
map_prep2 <- all_data[year_id==2010,list(ihme_loc_id,100*(exposure_mean.y-exposure_mean.x)/exposure_mean.x)]
names(map_prep2) <- c("ihme_loc_id","mapvar")
limit <- quantile(map_prep2$mapvar,probs=(seq(0,1,0.05)),na.rm=T)

gbd_map(data=map_prep2,
        limits=limit,
        labels=lapply(quantile(map_prep2$mapvar,probs=(seq(0.05,1,0.05)),na.rm=T),round,digits=3),
        legend.title="% change",
        col.reverse=T,
        #legend.cex=.9,
        legend.col=1,
        title="% Change in GBD 2010 ozone Exposure")

#absolute change
map_prep3 <- all_data[year_id==1990,list(ihme_loc_id,(exposure_mean.y-exposure_mean.x))]
names(map_prep3) <- c("ihme_loc_id","mapvar")
limit <- quantile(map_prep3$mapvar,probs=(seq(0,1,0.05)),na.rm=T)

gbd_map(data=map_prep3,
        limits=limit,
        labels=lapply(quantile(map_prep3$mapvar,probs=(seq(0.05,1,0.05)),na.rm=T),round,digits=3),
        legend.title="% change",
        col.reverse=T,
        legend.cex=.9,
        legend.col=1,
        title="Absolute change in GBD 1990 ozone Exposure")

#compares a small selection of samples
ggplot(data=all_data[ihme_loc_id %in% c("DJI","PRI")],
       aes(x=year_id,
           y=exposure_mean.y,
           color=ihme_loc_id)) +
  geom_line() +
  theme_bw()

#creates pdf of trends
pdf(paste0(graphs.dir,"/",exp.versions[1],"v",exp.versions[2],"/trends.pdf"))

all_data[, c("parent","child") := tstrsplit(ihme_loc_id, "_", fixed=TRUE)]

for(region in unique(all_data$region_name)){

  gg <- ggplot(data=all_data[region_name==region & is.na(child)],aes(x=year_id,y=exposure_mean.y,ymin=exposure_lower.y,ymax=exposure_upper.y,color=ihme_loc_id,fill=ihme_loc_id))+
    geom_line()+
    geom_point()+
    geom_ribbon(alpha=0.2)+
    ggtitle(region)
  
  print(gg)  

}

for(par in unique(all_data[!is.na(child),parent])){
  
  gg <- ggplot(data=all_data[grep(par,ihme_loc_id)], aes(x=year_id,y=exposure_mean.y,ymin=exposure_lower.y,ymax=exposure_upper.y,color=ihme_loc_id,fill=ihme_loc_id))+
    geom_line()+
    geom_point()+
    geom_ribbon(alpha=0.2)+
    ggtitle(par)
  
  print(gg)
}

dev.off()

