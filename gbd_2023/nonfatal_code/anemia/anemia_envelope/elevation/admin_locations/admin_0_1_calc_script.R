#################
#
# Average elevation by admin2 level using population as weights
#
#################

# source libraries --------------------------------------------------------

library(data.table)

# read in slurm args ------------------------------------------------------

command_args <- commandArgs(trailingOnly = TRUE)
output_directory <- command_args[1]

# set up working environment ---- 
year_vec <- c(1980:2021)

# apply elevation adjustments for admin 1 and admin 0 units ---------------

for(i in year_vec){
  print(i)
  df <- fread(file.path(output_directory, paste0("admin2_elevation_means_", i, ".csv")))
  
  #calculate admin 1 elevation data points
  admin1 <- df[!(is.na(weighted_mean_elevation)),
               .(total_population = sum(total_population, na.rm = T),
                  mean_population = mean(total_population, na.rm = T),
                  median_population = median(total_population, na.rm = T),
                  weighted_mean_elevation = weighted.mean(x = weighted_mean_elevation, w = total_population, na.rm = T)),
               .(ADM0_NAME,ADM0_CODE,ADM1_NAME,ADM1_CODE)]
  
  #calculate admin 0 elevation data points
  admin0 <- df[!(is.na(weighted_mean_elevation)),
               .(total_population = sum(total_population, na.rm = T),
                 mean_population = mean(total_population, na.rm = T),
                 median_population = median(total_population, na.rm = T),
                 weighted_mean_elevation = weighted.mean(x = weighted_mean_elevation, w = total_population, na.rm = T)),
               .(ADM0_NAME,ADM0_CODE)]
  
  #write out csv files
  write.csv(admin1,file.path(output_directory, paste0("admin1_elevation_means_",i,".csv")),row.names = F)
  write.csv(admin0,file.path(output_directory, paste0("admin0_elevation_means_",i,".csv")),row.names = F)
}
