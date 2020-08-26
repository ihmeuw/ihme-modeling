###############################################################################
# Author: 
# Date: 23 May 2016, updated 4/24/17 for GBD 2016
# Description: Prepare incidence draws for HIV PAF code
# Update Instructions: change spectrum.name and output.dir, double check shared
#     function and in.dir
###############################################################################

### Setup
library(data.table)
library(parallel)
library(future, lib.loc="FILEPATH")
library(pbmcapply, lib.loc="FILEPATH")
source("FILEPATH/get_location_metadata.R")

hiv_draws <- function(spectrum.name,main_dir){
  
  ### Paths
  in.dir <- paste0("FILEPATH", spectrum.name, "/")
  suffix <- "_ART_data.csv"
  
  # output directory
  output.dir <- paste0(main_dir,"/hiv_inc_draws_",spectrum.name)
  dir.create(output.dir, showWarnings=F)
  
  ## get location list with lowest GBD level locations
  loc.table <- get_location_metadata(location_set_id=22)
  loc.table <- loc.table[loc.table$is_estimate == 1 & loc.table$most_detailed == 1,]
  loc.list <- unique(loc.table$ihme_loc_id)

  ### Code
  file.list.out <- list.files(output.dir)
  hiv_prep <- function(loc){
    loc.id <- loc.table[ihme_loc_id == loc, location_id]
    filename <- paste0(loc.id, "_hiv_incidence_draws.csv")

      in.path <- paste0(in.dir, loc, suffix)
      data.id <- fread(in.path)
      data.id[, location_id := loc.id]
      data.id[, run_num := paste0("inc", run_num)]
      cast.data <- dcast.data.table(data.id, location_id + year_id + age_group_id + sex_id ~ run_num, value.var="new_hiv")
      output.path <- paste0(output.dir, "/",loc.id, "_hiv_incidence_draws.csv")
      write.csv(cast.data, file=output.path, row.names=F)

  }
  pbmclapply(loc.list,hiv_prep,mc.cores=40)
  
  
}

