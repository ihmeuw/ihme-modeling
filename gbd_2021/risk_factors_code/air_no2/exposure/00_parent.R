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

project <- "-P PROJECT "
sge.output.dir <- " -o FILEPATH -e FILEPATH "
#sge.output.dir <- "" # toggle to run with no output files

rshell <- "FILEPATH.sh"

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
source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))
locs <- get_location_metadata(35) %>% as.data.table
locs <- locs[most_detailed==1]

# versioning
grid.version <- VERSION

# ----------------------------------------- Launch prep script ----------------------------------------------------

draws.dir <- paste0("FILEPATH",grid.version,"/FILEPATH/")
dir.create(draws.dir,recursive=T,showWarnings = F)

finished <- list.files(draws.dir)

years <- c(1990,1995,2000,2005:2020)
draws_required <- 1000
gbd_round_id <- 7

memory <- 50
cores.provided <- 5

script <- file.path("FILEPATH/01_prep.R")

# make a tracker to track large locs split into chunks (only do this once)
tracker_dir <- paste0("FILEPATH",grid.version)
tracker <- data.table("location_id"=numeric(),
                      "year_id"=numeric(),
                      "nfiles"=numeric())
write.csv(tracker,paste0(tracker_dir,"/split_locs.csv"),row.names = F)

# do this only once before you estimate the islandS
if(run_all){
  
  for (loc in unique(c(locs$location_id))){

    for(year in years){
      
    # args <- paste(grid.version, loc, year)
    args <- paste(loc,year,draws_required,gbd_round_id,grid.version)
    mem <- paste0("-l m_mem_free=",memory,"G")
    fthread <- paste0("-l fthread=",cores.provided)
    runtime <- "-l h_rt=06:00:00"
    jname <- paste0("-N no2_exp_",loc,"_",year)
    
    if(TRUE%ni%grepl(glob2rx(paste0(loc,"_*",year,".csv")),finished)){ # check to make sure the job has not already completed
      system(paste("qsub",jname,mem,fthread,runtime,project,"-q long.q", "-l archive=TRUE" ,sge.output.dir,rshell,script,args))
      }
    }
  }
}


# check to see which locations have no gridcells (the really small islands in Oceania)
if(estimate_islands){
  zero_gridcells <- c()
  for (loc in unique(locs$location_id)){
    if(is.na(file.size(paste0(draws.dir,loc,"_2020.csv")))){
      next
    } else if(file.size(paste0(draws.dir,loc,"_2020.csv"))<100){
      zero_gridcells <- c(zero_gridcells,loc)
    }
  }
  zero_gridcells <- data.table(location_id = zero_gridcells)
  zero_gridcells <- merge(zero_gridcells,locs[,c("location_id","ihme_loc_id","location_name","region_name","super_region_name")],by="location_id")
  View(zero_gridcells) # just to double-check they are the right ones
  
  zero_gridcells <- zero_gridcells[location_id!=367] # we are handling Monaco separately
  
  for(year in years){
    temp <- fread(paste0(draws.dir,"26_1_",year,".csv"))
    lapply(zero_gridcells$location_id,function(x){write.csv(temp,paste0(draws.dir,x,"_",year,".csv"),row.names = F)})
    print(paste0("Done with ", year, "!"))
  }
  
}


# estimate Monaco my using the 02_estimate_small_locs.R script
