##############################################################################################################
#
# Description: Prepare incidence draws for HIV PAF code
#
##############################################################################################################

### Setup
library(data.table)
library(parallel)
library(future, lib.loc = paste0("FILEPATH", Sys.getenv(x='USER'), "/rlibs/"))
library(pbmcapply, lib.loc = paste0("FILEPATH", Sys.getenv(x='USER'), "/rlibs/"))
source("FILEPATH/get_location_metadata.R")

### Function
hiv_draws <- function(spectrum.name,main_dir) {
  
  ### Input Paths
  in.dir <- paste0("FILEPATH/", spectrum.name, "/")
  suffix <- "_ART_data.csv"
  
  ### Output Directory
  output.dir <- paste0(main_dir, "/hiv_inc_draws_", spectrum.name)
  dir.create(output.dir, showWarnings=F)
  
  ## Get location list with lowest GBD level locations
  loc.table <- get_location_metadata(location_set_id=22,release_id = 16)
  loc.table <- loc.table[loc.table$is_estimate == 1 & loc.table$most_detailed == 1,]
  loc.list <- unique(loc.table$ihme_loc_id)
  
  ### List of files in output directory
  file.list.out <- list.files(output.dir)
  
  ### Function to prepare HIV incidence draws
  hiv_prep <- function(loc) {
    loc.id <- loc.table[ihme_loc_id == loc, location_id]
    
    filename <- paste0(loc.id, "_hiv_incidence_draws.csv")
    
    if(!(filename %in% file.list.out)) {
      
      #create file to read
      in.path <- paste0(in.dir, loc, suffix)
      data.id <- fread(in.path)
      data.id[, location_id := loc.id]
      data.id[, run_num := paste0("inc", run_num)]
      cast.data <- dcast.data.table(data.id, location_id + year_id + age_group_id + sex_id ~ run_num, value.var="new_hiv")
      
      #copy 2022 for 2023 because missing
      temp <- cast.data[year_id == 2022]
      temp[, year_id := 2023]
      cast.data <- rbind(cast.data, temp)
      
      #save
      output.path <- paste0(output.dir, "/",loc.id, "_hiv_incidence_draws.csv")
      write.csv(cast.data, file=output.path, row.names=F)
    }
  }
  pbmclapply(loc.list,hiv_prep,mc.cores=40)
  
  
}

