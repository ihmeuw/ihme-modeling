# set up parallelized portion from sub

jobnum <- as.numeric(commandArgs()[2])
data_dir <-  commandArgs()[3]
map_dir <- commandArgs()[4]
outpath <- commandArgs()[5]
package_lib <- commandArgs()[6] 
date_mansoni <- commandArgs()[7]
date_haematobium <- commandArgs()[8]
date_japonicum <- commandArgs()[9]


if (Sys.info()[1] == "Linux"){
  drive1 <- "FILEPATH"
  drive2 <- paste0("FILEPATH",Sys.info()[7])
} else {
  drive1 <- "FILEPATH"
  code_root <-  "FILEPATH"
}


source('FILEPATH/setup.R')
package_list <- c("dplyr", "maptools", "raster", "rgeos", "rgdal", "data.table", "seegSDM", "stringr", "sp")
load_R_packages(package_list)

model_path <- 'FILEPATH'
params_path <- 'FILEPATH'

\repo <- paste0(drive1, 'FILEPATH/code')
source(paste0(repo, 'FILEPATH/central/functions.R'))                   
source(paste0(repo, 'FILEPATH/check_loc_results.R'))  

# set i
i <- jobnum

##############

#Change file path (acording to the map dir file path stated in the parent code file)

#mansoni_file <- paste0(map_dir, '/', date_mansoni,'/model_output/model_', date_mansoni, i, '.tif')
#editing the above with a new run of extract par for decomp
mansoni_file <- paste0(map_dir, 'FILEPATH','/preds_', i, '.tif')

print(mansoni_file)

nichemap_mansoni <- raster(mansoni_file, band=1)

#Change file path (acording to the map dir file path stated in the parent code file)
#nichemap_haematobium <- raster(paste0(map_dir, '/', date_haematobium,'/model_output/model_', date_haematobium, i, '.tif'), band=1)
#editing the above with a new run of extract par for decomp
nichemap_haematobium <- raster(paste0(map_dir, 'FILEPATH','/preds_', i, '.tif'), band=1)


#Change file path (acording to the map dir file path stated in the parent code file)
#nichemap_japonicum <- raster(paste0(map_dir, '/', date_japonicum,'/model_output/model_', date_japonicum, i, '.tif'), band=1)
#editing the above with a new run of extract par for decomp 
nichemap_japonicum <- raster(paste0(map_dir, 'FILEPATH','/preds_', i, '.tif'), band=1)


#load world pop
#input file(s) provided 
pop = raster(paste0(j, 'FILEPATH'))

#GBD 2019: these are outputs of ratsrizing  pred final and world GBD shape file in extract par species code
#These filepaths will change based on where you save these in the earlier parent code
#input file(s) provided 
ras_shp <- raster(paste0(params_path, "FILEPATH"))
ras_shp_mansoni <- raster(paste0(params_path, "FILEPATH"))
ras_shp_haematobium <- raster(paste0(params_path, "FILEPATH"))
ras_shp_japonicum <- raster(paste0(params_path, "FILEPATH"))


#setExtent sets the extent of a Raster* object. 
#Either by providing a new Extent object or by setting the extreme coordinates one by one.
setExtent(pop, ras_shp)

#load in occurrence reference csv
#The file paths may have to change for decomp 
#input files provided 

occ_raw_mansoni<-read.csv(paste0(data_dir, 'FILEPATH'))
occ_raw_haematobium<-read.csv(paste0(data_dir, 'FILEPATH'))
occ_raw_japonicum<-read.csv(paste0(data_dir, 'FILEPATH'))

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

# extract these values from the raster
##niche map is the mean map
#what are you getting here?
occ_preds_mansoni <- extract(nichemap_mansoni, occ_coords_mansoni, df = T, method = 'simple')
occ_preds_haematobium <- extract(nichemap_haematobium, occ_coords_haematobium, df = T, method = 'simple')
occ_preds_japonicum <- extract(nichemap_japonicum, occ_coords_japonicum, df = T, method = 'simple')
# setup for merge
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
## this needs to go before loop
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

#this will change based on the new outpath folder
write.csv(prop_par, file = paste0(outpath, "FILEPATH", jobnum, ".csv"))

# save optimal thresholds
optimal_thresholds[1] <- optimal_thresholds_m[1]
optimal_thresholds[2] <- optimal_thresholds_h[1]
optimal_thresholds[3] <- optimal_thresholds_j[1]

#this will change based on the new outpath folder
write.csv(optimal_thresholds, file = paste0(outpath, "FILEPATH", jobnum, ".csv"))




