# set up parallelized portion from qsub

jobnum <- as.numeric(commandArgs()[2])
data_dir <-  commandArgs()[3]
map_dir <- commandArgs()[4]
outpath <- commandArgs()[5]
package_lib <- commandArgs()[6] 
date_mansoni <- commandArgs()[7]
date_haematobium <- commandArgs()[8]
date_japonicum <- commandArgs()[9]


if (Sys.info()[1] == "Linux"){
  ADDRESS <- "FILEPATH"
} else {
  ADDRESS <- "FILEPATH"
}

##load necessary packages
# .libPaths(package_lib)
# library(dplyr)
# library(maptools)
# library(raster)
# library(rgeos)
# library(rgdal)
# library(data.table)
# library(seegSDM)
# library(stringr)
# library(sp)

source('FILEPATH')
package_list <- c("dplyr", "maptools", "raster", "rgeos", "rgdal", "data.table", "seegSDM", "stringr", "sp")
load_R_packages(package_list)

model_path <- 'FILEPATH/ntd_schisto/extract_PAR_non_urban_mask'
params_path <- 'FILEPATH/ntd_schisto/params/extract_PAR_non_urban_mask'

source('FILEPATH')    ## eco-niche functions
source('FILEPATH')    ## eco-niche locations check

# set i
i <- jobnum

##############
## pull in mean maps (tif files)
nichemap_mansoni <- raster('FILEPATH', band=1)

nichemap_haematobium <- raster('FILEPATH'), band=1)

nichemap_japonicum <- raster('FILEPATH'), band=1)


#load world pop
pop = raster(paste0(j, '/WORK/11_geospatial/01_covariates/00_MBG_STANDARD/worldpop/total/5y/worldpop_total_5y_2005_00_00.tif'))


#outputs of rasterizing final predictions and world GBD shape file from extract_PAR species code
ras_shp <- raster("FILEPATH"))
ras_shp_mansoni <- raster("FILEPATH"))
ras_shp_haematobium <- raster("FILEPATH"))
ras_shp_japonicum <- raster("FILEPATH"))


#setExtent sets the extent of a Raster* object. 
#Either by providing a new Extent object or by setting the extreme coordinates one by one.
setExtent(pop, ras_shp)


#load in occurrence reference csv

occ_raw_mansoni<-read.csv('FILEPATH')    # mansoni
occ_raw_haematobium<-read.csv('FILEPATH')    # haematobium
occ_raw_japonicum<-read.csv('FILEPATH')    # japonicum

#define threshold value stringexte
threshold<-seq(0,1,0.001)
threshold<-threshold[order(threshold, decreasing = TRUE)]

#generate independent subset of the data using a subset of the occurrence probabilities
sensitivity<-rep(NA, length(threshold))

ROC_mega_table<-list(NA)
optimal_thresholds<-NA
optimal_thresholds_m<-NA
optimal_thresholds_h<-NA
optimal_thresholds_j<-NA

#for each map extract values for all occurrences and pseudo 0s
occ_mansoni <- occ_raw_mansoni[occ_raw_mansoni$PA == 1,]
occ_haematobium <- occ_raw_haematobium[occ_raw_haematobium$PA == 1,]
occ_japonicum <- occ_raw_japonicum[occ_raw_japonicum$PA == 1,]
occ_coords_mansoni <- occ_mansoni[c('longitude', 'latitude')]
occ_coords_haematobium <- occ_haematobium[c('longitude', 'latitude')]
occ_coords_japonicum <- occ_japonicum[c('longitude', 'latitude')]

pseudoz_mansoni <- occ_raw_mansoni[occ_raw_mansoni$PA == 0,]
pseudoz_haematobium <- occ_raw_haematobium[occ_raw_haematobium$PA == 0,]
pseudoz_japonicum <- occ_raw_japonicum[occ_raw_japonicum$PA == 0,]

# now do this for pseudo 0s
pseudoz_coords_mansoni <- pseudoz_mansoni[c('longitude', 'latitude')]
pseudoz_coords_haematobium <- pseudoz_haematobium[c('longitude', 'latitude')]
pseudoz_coords_japonicum <- pseudoz_japonicum[c('longitude', 'latitude')]


##############################
##### TEST WITH MEAN MAP #####
##############################

##niche map is the mean map  -- extract from the raster
occ_preds_mansoni <- extract(nichemap_mansoni, occ_coords_mansoni, df = T, method = 'simple')
occ_preds_haematobium <- extract(nichemap_haematobium, occ_coords_haematobium, df = T, method = 'simple')
occ_preds_japonicum <- extract(nichemap_japonicum, occ_coords_japonicum, df = T, method = 'simple')

#setup for merge
colnames(occ_preds_mansoni) <- c('X', 'prob')
colnames(occ_preds_haematobium) <- c('X', 'prob')
colnames(occ_preds_japonicum) <- c('X', 'prob')

#merging extraction of above with the psudeo 0 file (where PA=1)
occ_mansoni <- merge(occ_mansoni, occ_preds_mansoni, by = "X")
occ_haematobium <- merge(occ_haematobium, occ_preds_haematobium, by = "X")
occ_japonicum <- merge(occ_japonicum, occ_preds_japonicum, by = "X")

#generate independent subset of the data using a subset of the occurrence probabilities
sensitivity_mansoni<-rep(NA, length(threshold))
sensitivity_haematobium<-rep(NA, length(threshold))
sensitivity_japonicum<-rep(NA, length(threshold))

# extract raster values for true occurence data only
# set sensitivities
for (k in 1:length(threshold)){
  sensitivity_mansoni[k]<-length(occ_mansoni$prob[occ_mansoni$prob>threshold[k]])/length(occ_mansoni$prob)
  sensitivity_haematobium[k]<-length(occ_haematobium$prob[occ_haematobium$prob>threshold[k]])/length(occ_haematobium$prob)
  sensitivity_japonicum[k]<-length(occ_japonicum$prob[occ_japonicum$prob>threshold[k]])/length(occ_japonicum$prob)
}
## now for pseudo 0s
## extract these values from the raster
pseudoz_preds_mansoni <- extract(nichemap_mansoni, pseudoz_coords_mansoni, df = T, method = 'simple')
pseudoz_preds_haematobium <- extract(nichemap_haematobium, pseudoz_coords_haematobium, df = T, method = 'simple')
pseudoz_preds_japonicum <- extract(nichemap_japonicum, pseudoz_coords_japonicum, df = T, method = 'simple')

# setup for merge
colnames(pseudoz_preds_mansoni) <- c('X', 'prob')
colnames(pseudoz_preds_haematobium) <- c('X', 'prob')
colnames(pseudoz_preds_japonicum) <- c('X', 'prob')

# setup pseudoz
pseudoz_mansoni$X <- (1:nrow(pseudoz_mansoni))
pseudoz_haematobium$X <- (1:nrow(pseudoz_haematobium))
pseudoz_japonicum$X <- (1:nrow(pseudoz_japonicum))

# merge together
pseudoz_mansoni <- merge(pseudoz_mansoni, pseudoz_preds_mansoni, by = "X")
pseudoz_haematobium <- merge(pseudoz_haematobium, pseudoz_preds_haematobium, by = "X")
pseudoz_japonicum <- merge(pseudoz_japonicum, pseudoz_preds_japonicum, by = "X")

fpr_mansoni<-rep(NA, length(threshold))
fpr_haematobium<-rep(NA, length(threshold))
fpr_japonicum<-rep(NA, length(threshold))

for (a in 1:length(threshold)){
  fpr_mansoni[a]<-length(pseudoz_mansoni$prob[pseudoz_mansoni$prob>threshold[a]])/length(pseudoz_mansoni$prob)
  fpr_haematobium[a]<-length(pseudoz_haematobium$prob[pseudoz_haematobium$prob>threshold[a]])/length(pseudoz_haematobium$prob)
  fpr_japonicum[a]<-length(pseudoz_japonicum$prob[pseudoz_japonicum$prob>threshold[a]])/length(pseudoz_japonicum$prob)
}

ROC_table<-data.frame(threshold=threshold, sensitivity_m=sensitivity_mansoni, sensitivity_h=sensitivity_haematobium, sensitivity_j=sensitivity_japonicum, fp_m=fpr_mansoni, fp_h=fpr_haematobium, fp_j=fpr_japonicum)

for(a in 1:nrow(ROC_table)){
  ROC_table$hypotenuse_m[a]<-sqrt(((1-ROC_table$sensitivity_m[a])^2)+(ROC_table$fp_m[a]^2))
  ROC_table$hypotenuse_h[a]<-sqrt(((1-ROC_table$sensitivity_h[a])^2)+(ROC_table$fp_h[a]^2))
  ROC_table$hypotenuse_j[a]<-sqrt(((1-ROC_table$sensitivity_j[a])^2)+(ROC_table$fp_j[a]^2))
}

ROC_mega_table[[1]]<-ROC_table
optimal_thresholds_m[1]<-ROC_table$threshold[which(ROC_table$hypotenuse_m==min(ROC_table$hypotenuse_m))]
optimal_thresholds_h[1]<-ROC_table$threshold[which(ROC_table$hypotenuse_h==min(ROC_table$hypotenuse_h))]
optimal_thresholds_j[1]<-ROC_table$threshold[which(ROC_table$hypotenuse_j==min(ROC_table$hypotenuse_j))]


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
ras_shp_japonicum[ras_shp_japonicum>0 & ras_shp_japonicum<=100000]<- 1
ras_shp_japonicum[is.na(ras_shp_japonicum)]<-0

## this would need to go inside of loop
threshold_map_m<-nichemap_mansoni>optimal_thresholds_m[1]
threshold_map_m<-threshold_map_m*ras_shp_mansoni
threshold_map_m[is.na(threshold_map_m)]<- 0
threshold_map_h<-nichemap_haematobium>optimal_thresholds_h[1]
threshold_map_h<-threshold_map_h*ras_shp_haematobium
threshold_map_h[is.na(threshold_map_h)]<- 0
threshold_map_j<-nichemap_japonicum>optimal_thresholds_j[1]
threshold_map_j<-threshold_map_j*ras_shp_japonicum
threshold_map_j[is.na(threshold_map_j)]<- 0

threshold_map <- do.call("sum", list(threshold_map_m, threshold_map_h, threshold_map_j))
threshold_map[threshold_map>=1 & threshold_map <= 3]<-1

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

write.csv(prop_par, file = "FILEPATH")


# save optimal thresholds
optimal_thresholds[1] <- optimal_thresholds_m[1]
optimal_thresholds[2] <- optimal_thresholds_h[1]
optimal_thresholds[3] <- optimal_thresholds_j[1]


write.csv(optimal_thresholds, file = "FILEPATH")
