
#clear memory
rm(list=ls())

library("data.table")
library("dplyr")

#define args:
iso3 <- ISO # Add the ISO3 code for the country that you want to redistribute 
description <- DESCRIPTION # Give this run a description to be used in naming

min_year <- 1980
max_year <- 2020

ages <- c(0, 0.01, 0.1, 1, seq(5, 95, 5))
sexes <- 1:2

gc_source <- "all"

user <- USENAME


# set up directories
j_root <- "/snfs1"
h_root <- "~" 
rdir <- FILEPATH 
odir <- FILEPATH 

# set up run environment
project <- "proj_erf"
sge.output.dir <- paste0(" -o FILEPATH", user, "/output -e FILEPATH", user, "/errors ")
r.shell <- "FILEPATH/health_fin_forecasting_shell_singularity.sh"
slots <- 8
save.script <- paste0(rdir, "/code/jsonExtractor.R")



cod <- fread(paste0(FILEPATH, "/datasets/", iso3, "_split_codes.csv"))[year_id %in% min_year:max_year, ]




for (loc_id in unique(cod[, location_id])) {
  
  ihme_loc_id <- paste0(iso3, "_", loc_id)
  
  for (code_system_id in unique(cod[,code_system_id])) {
    
    pathExtension <- paste("outputs", iso3, description, ihme_loc_id, code_system_id, sep = "/")
    
    dir.create(paste(rdir, pathExtension, sep = "/"), recursive = T)
    dir.create(paste(FILEPATH, "redist", pathExtension, sep = "/"), recursive = T)
  }
  
  
  for (year in unique(cod[location_id==loc_id, year_id])) {
    code_system_id <- unique(cod[year_id==year & location_id==loc_id, code_system_id])
    
    # set script and memory based on which type of redistribution to run (by code or by package)
    if (length(code_system_id)==1 & code_system_id %in% c(1,6)) {
      
      if (code_system_id==6) {
        mem <- "250G"
      } else if (code_system_id==1) {
        mem <- "160G"
      } 
    
      for (age in ages)  {
        for (sex in sexes) {
    
            args <- paste(year, age, sex, loc_id, code_system_id, gc_source, description) 
            
            jname <- paste("rd", loc_id, year, age, sex, sep = "_")
            
            # Create submission call
            sys.sub <- paste0("qsub -l m_mem_free=", mem, " -l fthread=", slots, " -P ", project, " -q all.q", sge.output.dir, " -N ", jname)

            # Run
            system(paste(sys.sub, r.shell, save.script, args))
            Sys.sleep(0.01)
            
        }
      }
    } else {
      print(paste0("Code system for location ", loc_id, " and year ", year, "is not 1 or 6 (could be different code system, missing, or a mix)"))
    }
  }
}

