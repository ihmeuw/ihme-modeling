rm(list=ls())

print(paste("Working dir:", getwd()))
j <- "FILEPATH"
my_libs = "FILEPATH"

library(data.table)
library(magrittr)
source(paste0(j, "FILEPATH/get_draws.R"))
source(paste0(j, "FILEPATH/get_location_metadata.R"))

key <- data.table(me_id = 1594, acause = "neonatal_sepsis")
loc_list <- get_location_metadata(location_set_id = 9, gbd_round_id = 6)
loc_list <- loc_list[is_estimate == 1]
year_list <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
sex_list <- c(1, 2)

for(me in key$me_id){
  
  acause <- key[me_id == me, acause]
  data_dir <- paste0("FILEPATH/")
  
  for(year in year_list){
    for(sex in sex_list){
      for(loc in loc_list[["location_id"]]) {
        
        file = paste0(data_dir, "5_", loc, "_", year, "_", sex, ".csv")
        
        if(file.exists(file) == F){
          
          shell <- "FILEPATH/stata_shell.sh"
          script <- "FILEPATH/04_interpolate_sepsis_parallel_parallel.do"
          
          job_name <- paste0("interpolate_", me, "_", loc, "_", year, "_", sex)
          print(job_name)
          
          ooo_eee = paste0("-e FILEPATH -o FILEPATH")
          
          qsub_call = paste("qsub -l fthread=1 -N", job_name,
                            "-P proj_neonatal", ooo_eee, 
                            "-l m_mem_free=25G -l fthread=4 -l h_rt=00:05:00 -l archive -q all.q",
                            "FILEPATH/stata_shell.sh", script, shQuote(paste(me, acause, loc, year, sex)))
          system(qsub_call)
        }    
      }
    }
  }
}
