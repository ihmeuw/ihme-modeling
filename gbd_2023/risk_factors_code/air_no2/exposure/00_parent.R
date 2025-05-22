#-------------------Header------------------------------------------------
# Author: 
# Purpose: Launcher script for NO2 exposure


#------------------Set-up--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}

project <- "-A proj_diet " # "-A proj_erf "
sge.output.dir <- "-e FILEPATH -o FILEPATH "
#sge.output.dir <- "" # toggle to run with no output files


rshell <- "FILEPATH"

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

'%ni%' <- Negate("%in%")

run_all <- T
estimate_islands <- F

# ----------------------------------------- Directories -------------------------------------------------------------
# load in locations
source(file.path(central_lib,"current/r/get_location_metadata.R"))
# locs <- get_location_metadata(35, gbd_round_id = 7, decomp = "iterative") %>% as.data.table
locs <- get_location_metadata(35, gbd_round_id = 9, release_id = 16) %>% as.data.table
locs <- locs[most_detailed==1]

# versioning


grid.version <- 12 # Same as above but with updated inputs (rescaled GBD 2021 inputs for 1990-2019, LUR for 2020-2023)

# ----------------------------------------- Launch prep script ----------------------------------------------------

draws.dir <- paste0("FILEPATH/air_no2/exp/gridded/",grid.version,"/draws/")
dir.create(draws.dir,recursive=T,showWarnings = T)

finished <- list.files(draws.dir)

years <- c(1990,1995,2000,2005:2023) # FULL RUN

draws_required <- 250 # 100
gbd_round_id <- 9
release_id <- 16

memory <- 50
memory_large <- 100
cores.provided <- 5

script <- file.path("FILEPATH/air_no2/exp/01_prep_collapse.R")

# make a tracker to track large locs split into chunks (only do this once!)
tracker_dir <- paste0("FILEPATH/air_no2/exp/gridded/",grid.version)
tracker <- data.table("location_id"=numeric(),
                      "year_id"=numeric(),
                      "nfiles"=numeric())
write.csv(tracker,paste0(tracker_dir,"/split_locs.csv"),row.names = F)

# do this only once before you estimate the islands!
if(run_all){
  
  # for (loc in unique(c(locs$location_id))){
  for (loc in unique(c(349))){
      
    for(year in years){
      
    # args <- paste(grid.version, loc, year)
    args <- paste(loc,year,draws_required,gbd_round_id,grid.version,release_id)
    mem <- paste0("--mem=",memory,"G")
    if(loc%in%c(62,71,101,102,135,44979)){
      mem <- paste0("--mem=",memory_large,"G")
    }
    fthread <- paste0("-c " ,cores.provided)
    runtime <- "-t 3:00:00" # "-t 18:00:00"
    jname <- paste0("-J no2_exp_",loc,"_",year)
    archive <- "-C archive"
    # browser() ### DEBUG
    
    if(TRUE%ni%grepl(glob2rx(paste0(loc,"_*",year,".csv")),finished)){ # check to make sure the job has not already completed
      system(paste("sbatch",jname,mem,fthread,runtime,project,"-p long.q",archive,sge.output.dir,rshell,script,args))
      }
    }
  }
}


# check to see which locations have no gridcells
# this is how we will decide which locations need to have papua new guinea's exposure
# we are using Papua New Guinea's exposure because it is the closest to the islands and is also below the TMREL anyways,
# so these locations will still have no burden
### TODO: CHECK IF MISSING FILES ARE FOR ALL YEARS FOR EACH LOCATION
### IF LOCATION IS COMPLETELY MISSING DATA (SO NO FILES FOR ANY YEAR), THEN COPY OVER FROM NEAREST LOCATION
### IF LOCATION ONLY MISSING A FEW YEARS, MAYBE USE NEAREST YEAR WITH DATA TO FILL IN????
if(estimate_islands){
  zero_gridcells <- c()
  for (loc in unique(locs$location_id)){
    if(!file.exists(paste0(draws.dir,loc,"_2023.csv"))){
      zero_gridcells <- c(zero_gridcells,loc)
    } else if(is.na(file.size(paste0(draws.dir,loc,"_2023.csv")))){
      zero_gridcells <- c(zero_gridcells,loc)
    } else if(file.size(paste0(draws.dir,loc,"_2023.csv"))<100){
      zero_gridcells <- c(zero_gridcells,loc)
    }
  }
  zero_gridcells <- data.table(location_id = zero_gridcells)
  zero_gridcells <- merge(zero_gridcells,locs[,c("location_id","ihme_loc_id","location_name","region_name","super_region_name")],by="location_id")
  View(zero_gridcells) # just to double-check they are the right ones

  for (loc in zero_gridcells$location_id) {
    print(paste0("Working on ", loc, "!"))
    if (zero_gridcells[zero_gridcells$location_id == loc, ]$region_name == 'Oceania'){
      zero_gridcells_oceania <- filter(zero_gridcells, region_name == 'Oceania')
      print(paste0("Copying over Papua New Guinea for ",loc))
      for(year in years){
        temp <- fread(paste0(draws.dir,"26_",year,".csv")) # copying over Papua New Guniea for Oceania islands
        lapply(zero_gridcells_oceania$location_id,function(x){fwrite(temp,paste0(draws.dir,x,"_",year,".csv"))})
        print(paste0("Done with ", year, "!"))
      }
    } else if (zero_gridcells[zero_gridcells$location_id == loc, ]$region_name == 'Southeast Asia') {
      zero_gridcells_maldives <- filter(zero_gridcells, region_name == 'Southeast Asia')
      print(paste0("Copying over Seychelles for ",loc))
      for(year in years){
        temp <- fread(paste0(draws.dir,"186_",year,".csv")) # copying over Seychelles for Maldives and Mauritius
        lapply(zero_gridcells$location_id,function(x){fwrite(temp,paste0(draws.dir,x,"_",year,".csv"))})
        print(paste0("Done with ", year, "!"))
      }
    }
      else if (zero_gridcells[zero_gridcells$location_id == loc, ]$location_name == 'Bermuda') {
        zero_gridcells_bermuda <- filter(zero_gridcells, location_name == 'Bermuda')
        print(paste0("Copying over Bahamas for ",loc))
        for(year in years){
          temp <- fread(paste0(draws.dir,"106_",year,".csv")) # copying over Seychelles for Mauritius
          lapply(zero_gridcells$location_id,function(x){fwrite(temp,paste0(draws.dir,x,"_",year,".csv"))})
          print(paste0("Done with ", year, "!"))
        }
      }
    else {
      warning(paste0("No data specified to copy over for ", loc, "!"))
      next
    }
    print(paste0("Done with ", loc, "!"))
  }
}



