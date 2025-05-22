#################
#
# Average elevation by admin2 level using population as weights
#
#################


# source libraries --------------------------------------------------------

library(data.table)
library(stringr)
library(raster)
library(terra)
library(sp)
library(rgdal)

# set up working environment ----

task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
year_vec <- c(1980:2021)
year_id <- year_vec[task_id]

command_args <- commandArgs(trailingOnly = TRUE)
output_directory <- command_args[1]

Sys.sleep(task_id / 5)

load(file.path(output_directory, "elevation_map_data.RData"))

# load in population for the given year ----
pop_raster <- NULL
if(year_id < 2000){
  pop_raster <- raster(paste0("FILEPATH/worldpop_total_1y_",year_id,"_00_00.tif"))
}else{
  pop_raster <- raster(paste0("FILEPATH/worldpop_total_1y_",year_id,"_00_00.tif"))
}

# for each admin location, calculate the weighted mean elevation ----------

admin_df <- admin_df[level >= 4]
for(i in admin_df$loc_id){
  geogTemp <- world[world$loc_id==i,] #subset the location of the admin2
  
  # crop the raster files to be of the admin area being mapped
  crop_elevation <- crop(elevation_map,extent(geogTemp))
  crop_pop <- crop(pop_raster,extent(geogTemp))
  
  intersect_flag <- intersect(extent(crop_elevation), extent(crop_pop))
  
  if (!(is.null(intersect_flag))){
    
    #extract the data within each subset raster region (results are returned in the same order for each)
    elevation_list <-  raster::extract(crop_elevation,geogTemp,df=T,na.rm=F)
    pop_list <- raster::extract(crop_pop,geogTemp,df=T,na.rm=F)
    
    # get the total population, median population, average population, median elevation, unwegithed average elevation, weighted elevation mean by population
    pop_mean <- mean(pop_list[,2],na.rm=T)
    pop_sum <- sum(pop_list[,2],na.rm = T)
    pop_med <- median(pop_list[,2],na.rm = T)
    
    med_elevation <- median(elevation_list$elevation_mean_synoptic,na.rm=T)
    unweighted_elevation_mean <- mean(elevation_list$elevation_mean_synoptic,na.rm=T)
    
    pop_elevation_df <- data.table(elevation = elevation_list$elevation_mean_synoptic, population = pop_list[,2])
    pop_elevation_df <- pop_elevation_df[!(is.na(elevation)) & !(is.na(population))]
    weighted_elevation_mean <- weighted.mean(x = pop_elevation_df$elevation,w = pop_elevation_df$population,na.rm=T)
    weighted_sd <- sqrt(Hmisc::wtd.var(x = pop_elevation_df$elevation, weights = pop_elevation_df$population, na.rm = T))
    
    admin_df <- admin_df[loc_id == i, `:=`(total_population = pop_sum,
                                           mean_population = pop_mean,
                                           median_population = pop_med,
                                           
                                           median_elevation = med_elevation,
                                           unweighted_mean_elevation = unweighted_elevation_mean,
                                           weighted_mean_elevation = weighted_elevation_mean,
                                           weighted_elevation_sd = weighted_sd,
                                           num_tiles = nrow(pop_elevation_df))]
  }
}

# write out data ----------------------------------------------------------

output_file_name <- file.path(output_directory, paste0("ihme_subnat_elevation_means_",year_id,".csv"))

write.csv(
  x = admin_df,
  file = output_file_name,
  row.names = F
)



