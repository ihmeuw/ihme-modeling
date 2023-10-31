
library(data.table)
library(plyr)
library(dplyr)
library(magrittr)

source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')  
source('FILEPATH')  

input_dir <- 'FILEPATH'
max_year <- 2019

locs <- get_location_metadata(22)

plot <- T

#Load datasets and reduce to required columns. Reshape long.

# st-gpr output for LPC
alc_lpc <- fread(paste0('FILEPATH'))
alc_lpc[,c("V1","age_group_id", "sex_id")] <- NULL
alc_lpc_melt <- melt.data.table(alc_lpc, id.vars=c("location_id", "year_id"), 
                                measure.vars=paste0("draw_", 0:999), value.name='alc_lpc', variable.name="draw")
alc_lpc_melt <- alc_lpc_melt[year_id >= 1980] # limit years for the covariate

# read in the net tourism adjustments:
if (F){
  net_tourism_files <- list.files('FILEPATH', full.names = T)
  net_tourism <- lapply(net_tourism_files, fread)
  net_tourism_data <- rbindlist( net_tourism )
  net_tourism_data$location_name <- NULL
  
} 

####################### tourism adjustment ######################

net_tourism_data_pre <- fread('FILEPATH')
net_tourism_data_pre$location_name <- NULL

# this is a full dataset of all the places/years we will need adjustment factors for
net_tourism_data_pre_missing <- as.data.table(expand.grid(location_id = unique(net_tourism_data_pre$location_id), year_id = c(seq(1980,1994,1),seq(2015,max_year,1)), draw = paste0("draw_", 0:999)))
net_tourism_data_pre_missing$net_tourism <- 0

net_tourism_data <- rbind(net_tourism_data_pre,net_tourism_data_pre_missing)

# copy out the adjustment factor for tourism in 1995 to the prvious years, since 1995 is the earlier year we have tourism data for
info_1995 <- net_tourism_data_pre[year_id == 1995]
info_1995$year_id <- NULL
setnames(info_1995,old=c("net_tourism"),new=c("tourism_1995"))

net_tourism_data <- merge(net_tourism_data,info_1995,by=c("location_id","draw"))
net_tourism_data[year_id <= 1994, net_tourism := tourism_1995]

# do the same for 2014 (replicating out for more recent years)
info_2014 <- net_tourism_data_pre[year_id == 2014]
info_2014$year_id <- NULL
setnames(info_2014,old=c("net_tourism"),new=c("tourism_2014"))
net_tourism_data <- merge(net_tourism_data,info_2014,by=c("location_id","draw"))
net_tourism_data[year_id > 2014, net_tourism := tourism_2014]

# adjust LPC when tourism values are present. Otherwise, assume that there is no tourism adjustment
tourism_alc <- merge(alc_lpc_melt,net_tourism_data,by=c("location_id","year_id","draw"),all=T)

# if there is no tourism value, then set it to 0
tourism_alc[is.na(net_tourism), net_tourism:=0]

# add the lpc and the tourism total together
tourism_alc[,alc_lpc_new := alc_lpc + net_tourism]

# if the net LPC value is less than 0, then don't adjust at all
tourism_alc[alc_lpc_new <= 0, alc_lpc_new := alc_lpc]

# and write it out 
write.csv(tourism_alc,'FILEPATH')

