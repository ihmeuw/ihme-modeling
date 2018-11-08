# set up parallelized portion from qsub

jobnum <- as.numeric(commandArgs()[3])
root_path <-  commandArgs()[4]
outpath <- commandArgs()[5]

if (Sys.info()[1] == "Linux"){
  j <- "FILEPATH"
  h <- paste0("FILEPATH",Sys.info()[6])
  package_lib <- paste0(j,'FILEPATH')
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  package_lib <- paste0(j,'FILEPATH')
}
.libPaths(package_lib)
#load necessary packages
library(dplyr)
library(maptools)
library(raster)
library(rgeos)
library(rgdal)
library(data.table)
library(seegSDM)
library(stringr)
library(sp)

repo <- paste0(j, 'FILEPATH')
source(paste0(repo, 'FILEPATH/functions.R'))
source(paste0(repo, 'FILEPATH/econiche_qsub.R'))
source(paste0(repo, 'FILEPATH/check_loc_results.R'))

# set i
i <- jobnum

# set up filepaths and directories

##############
## pull in mean map
nichemap_mansoni <- raster(paste0(root_path, 'FILEPATH/preds_', i, '.tif'), band=1)
nichemap_haematobium <- raster(paste0(root_path, 'FILEPATH/preds_', i, '.tif'), band=1)
urbanicity <- raster(paste0(j, 'FILEPATH/ghslurbanicity_mean_1y_2005_00_00.tif'))
urbanicity[urbanicity>0 & urbanicity<=100000] <- 3
urbanicity[is.na(urbanicity) | urbanicity == 0] <- 1
urbanicity[urbanicity == 3] <- 0
# # get world shapefile
# worldshp = readOGR(paste0(j, 'FILEPATH'), 'GBD2016_analysis_final')
# # get rid of the factor class
# worldshp$loc_id = as.numeric(levels(worldshp$loc_id))[as.integer(worldshp$loc_id)]
# # pull in geographic restrictions
# excl_list = data.table(read.csv(paste0(j, "FILEPATH/species_specific_exclusions.csv"), stringsAsFactors = F))
# # keep if a location is endemic
# excl_list = excl_list[japonicum == "" | mansoni == "" | haematobium =="" | other == "",]
# excl_list_mansoni = excl_list[mansoni == "" | location_id == 172,]
# excl_list_haematobium = excl_list[haematobium =="" | location_id == 215,]
# excl_list_japonicum = excl_list[japonicum == "" | location_id == 10 | location_id == 12,]
# #subset world shp to be only locations with da shishto
# worldshp = worldshp[worldshp$loc_id %in% excl_list[,location_id],]
# worldshp_mansoni = worldshp[worldshp$loc_id %in% excl_list_mansoni[,location_id],]
# worldshp_haematobium = worldshp[worldshp$loc_id %in% excl_list_haematobium[,location_id],]
# worldshp_japonicum = worldshp[worldshp$loc_id %in% excl_list_japonicum[,location_id],]

#load world pop
pop = raster(paste0(j, 'FILEPATH/worldpop_total_5y_2005_00_00.tif'))
# mask our urban areas
pop <- pop * urbanicity
ras_shp <- raster(paste0(j, "FILEPATH/ras_shp.tif"))
ras_shp_mansoni <- raster(paste0(j, "FILEPATH/ras_shp_mansoni.tif"))
ras_shp_haematobium <- raster(paste0(j, "FILEPATH/ras_shp_haematobium.tif"))
setExtent(pop, ras_shp)


#load in occurrence reference csv
occ_raw_mansoni<-read.csv(paste0(root_path, 'FILEPATH/dat_all.csv'))
occ_raw_haematobium<-read.csv(paste0(root_path, 'FILEPATH/dat_all.csv'))

#define threshold value stringexte
threshold<-seq(0,1,0.001)
threshold<-threshold[order(threshold, decreasing = TRUE)]

#generate independent subset of the data using a subset of the occurrence probabilities
sensitivity<-rep(NA, length(threshold))

ROC_mega_table<-list(NA)
optimal_thresholds<-NA
optimal_thresholds_m<-NA
optimal_thresholds_h<-NA
#for each map extract values for all occurrences and pseudo 0s
occ_mansoni <- occ_raw_mansoni[occ_raw_mansoni$PA == 1,]
occ_mansoni$X <- seq.int(nrow(occ_mansoni))
occ_haematobium <- occ_raw_haematobium[occ_raw_haematobium$PA == 1,]
occ_haematobium$X <- seq.int(nrow(occ_haematobium))
occ_coords_mansoni <- occ_mansoni[c('longitude', 'latitude')]
occ_coords_haematobium <- occ_haematobium[c('longitude', 'latitude')]

pseudoz_mansoni <- occ_raw_mansoni[occ_raw_mansoni$PA == 0,]
pseudoz_haematobium <- occ_raw_haematobium[occ_raw_haematobium$PA == 0,]

# now do this for pseudo 0s
pseudoz_coords_mansoni <- pseudoz_mansoni[c('longitude', 'latitude')]
pseudoz_coords_haematobium <- pseudoz_haematobium[c('longitude', 'latitude')]

##############################
##### TEST WITH MEAN MAP #####
##############################

# extract these values from the raster
occ_preds_mansoni <- raster::extract(nichemap_mansoni, occ_coords_mansoni, df = T, method = 'simple')
occ_preds_haematobium <- raster::extract(nichemap_haematobium, occ_coords_haematobium, df = T, method = 'simple')
# setup for merge
colnames(occ_preds_mansoni) <- c('X', 'prob')
colnames(occ_preds_haematobium) <- c('X', 'prob')

occ_mansoni <- merge(occ_mansoni, occ_preds_mansoni, by = "X")
occ_haematobium <- merge(occ_haematobium, occ_preds_haematobium, by = "X")
#generate independent subset of the data using a subset of the occurrence probabilities
sensitivity_mansoni<-rep(NA, length(threshold))
sensitivity_haematobium<-rep(NA, length(threshold))
# extract raster values for true occurence data only
# set sensitivities
for (k in 1:length(threshold)){
  sensitivity_mansoni[k]<-length(occ_mansoni$prob[occ_mansoni$prob>threshold[k]])/length(occ_mansoni$prob)
  sensitivity_haematobium[k]<-length(occ_haematobium$prob[occ_haematobium$prob>threshold[k]])/length(occ_haematobium$prob)
}
## now for pseudo 0s
## extract these values from the raster
pseudoz_preds_mansoni <- raster::extract(nichemap_mansoni, pseudoz_coords_mansoni, df = T, method = 'simple')
pseudoz_preds_haematobium <- raster::extract(nichemap_haematobium, pseudoz_coords_haematobium, df = T, method = 'simple')

# setup for merge
colnames(pseudoz_preds_mansoni) <- c('X', 'prob')
colnames(pseudoz_preds_haematobium) <- c('X', 'prob')
# setup pseudoz
pseudoz_mansoni$X <- (1:nrow(pseudoz_mansoni))
pseudoz_haematobium$X <- (1:nrow(pseudoz_haematobium))

# merge together
pseudoz_mansoni <- merge(pseudoz_mansoni, pseudoz_preds_mansoni, by = "X")
pseudoz_haematobium <- merge(pseudoz_haematobium, pseudoz_preds_haematobium, by = "X")

fpr_mansoni<-rep(NA, length(threshold))
fpr_haematobium<-rep(NA, length(threshold))

for (a in 1:length(threshold)){
  fpr_mansoni[a]<-length(pseudoz_mansoni$prob[pseudoz_mansoni$prob>threshold[a]])/length(pseudoz_mansoni$prob)
  fpr_haematobium[a]<-length(pseudoz_haematobium$prob[pseudoz_haematobium$prob>threshold[a]])/length(pseudoz_haematobium$prob)
}

ROC_table<-data.frame(threshold=threshold, sensitivity_m=sensitivity_mansoni, sensitivity_h=sensitivity_haematobium, fp_m=fpr_mansoni, fp_h=fpr_haematobium)

# plot(x=ROC_table$false_positive, y=ROC_table$sensitivity, type='l', main=paste(i))

for(a in 1:nrow(ROC_table)){
  ROC_table$hypotenuse_m[a]<-sqrt(((1-ROC_table$sensitivity_m[a])^2)+(ROC_table$fp_m[a]^2))
  ROC_table$hypotenuse_h[a]<-sqrt(((1-ROC_table$sensitivity_h[a])^2)+(ROC_table$fp_h[a]^2))
}
ROC_mega_table[[1]]<-ROC_table
optimal_thresholds_m[1]<-ROC_table$threshold[which(ROC_table$hypotenuse_m==min(ROC_table$hypotenuse_m))]
optimal_thresholds_h[1]<-ROC_table$threshold[which(ROC_table$hypotenuse_h==min(ROC_table$hypotenuse_h))]


###############
## generate PAR per district per map
###############
zonal_PAR<-list()
PAR<-list()

# setup raster shapes for stamping out areas
ras_shp_mansoni[ras_shp_mansoni>0 & ras_shp_mansoni<=100000]<- 1
ras_shp_mansoni[is.na(ras_shp_mansoni)]<-0
ras_shp_haematobium[ras_shp_haematobium>0 & ras_shp_haematobium<=100000]<- 1
ras_shp_haematobium[is.na(ras_shp_haematobium)]<-0

threshold_map_m<-nichemap_mansoni>optimal_thresholds_m[1]
threshold_map_m<-threshold_map_m*ras_shp_mansoni
threshold_map_m[is.na(threshold_map_m)]<- 0
threshold_map_h<-nichemap_haematobium>optimal_thresholds_h[1]
threshold_map_h<-threshold_map_h*ras_shp_haematobium
threshold_map_h[is.na(threshold_map_h)]<- 0

threshold_map <- do.call("sum", list(threshold_map_m, threshold_map_h))
threshold_map[threshold_map>=1 & threshold_map <= 2]<-1

PAR[[1]]<-threshold_map*pop
zonal_PAR[[1]]<-zonal(PAR[[1]],
                      ras_shp,
                      fun='sum',
                      na.rm=TRUE)


zonal_PAR_clean<-zonal_PAR[[1]]
for (i in 1:nrow(zonal_PAR_clean)){
  if (zonal_PAR_clean[i,2]<1){
    zonal_PAR_clean[i,2]<-0
  }
}

zonal_PAR[[1]]<-zonal_PAR_clean
colnames(zonal_PAR[[1]]) <- c('zone', 'par')


#calculate zonal totals
total_sums<-zonal(pop,
                  ras_shp,
                  fun = 'sum',
                  na.rm = TRUE)
for (i in 1:nrow(total_sums)){
  if (total_sums[i,2]<=0){
    total_sums[i,2]<-NA
  }
}

# generate the total population to get proportion
prop_par <- cbind(total_sums, zonal_PAR[[1]])
prop_par <- as.data.table(prop_par)

prop_par[, prop:=par/sum]
prop_par <- prop_par[, c("zone", "prop")]
colnames(prop_par)[2] <- c(paste0("prop_", jobnum))
write.csv(prop_par, file = paste0(outpath, "FILEPATH/prop_", jobnum, ".csv"))

# save optimal thresholds
optimal_thresholds[1] <- optimal_thresholds_m[1]
optimal_thresholds[2] <- optimal_thresholds_h[1]
write.csv(optimal_thresholds, file = paste0(outpath, "FILEPATH/threshold_", jobnum, ".csv"))
