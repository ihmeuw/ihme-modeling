
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 6/13/2019
# Purpose: Launcher for paf calculation and bwga shifts
#
# source("FILEPATH.R", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  } else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  }


project <- "-P ADDRESS" # -p must be set on the production cluster in order to get slots and not be in trouble
sge.output.dir <- "-o FILEPATH -e FILEPATH"

# load packages, install if missing

lib.loc <- paste0(h_root,"FILEPATH",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

output.version <- 51 #interpolating PAFs for all estimation years, GBD 2019
years <- c(1990,2000,2017,1995,2005,2010,2015,2019)

hap.map.date <-  "092419"
hap.stgpr.runid <- 102800
draws.required <- 1000
exposure.grid.version <- "36"
bwga.ier.version <- 38
ier.version <- "33power2_simsd_source_priors"

home.dir <- "FILEPATH"

code.dir <- file.path(h_root, 'FILEPATH')
cataract.calc.script <- file.path(h_root,"FILEPATH.R")
bwga.script <- file.path(h_root,"FILEPATH.R")
paf.script <- file.path(h_root,"FILEPATH.R")
rshell <- "FILEPATH.sh"

# Get the list of most detailed GBD locations
source(file.path(central_lib,"FILEPATH.R"))
locs <- get_location_metadata(35)
locations <- locs[most_detailed==1, location_id] %>% unique %>% sort

# Launch Cataracts --------------------------------------------------------

for(year in years){

   args <- paste(output.version,
                draws.required,
                year,
                hap.stgpr.runid)

  mem <- "-l m_mem_free=2G"
  fthread <- "-l fthread=5"
  runtime <- "-l h_rt=01:00:00"
  archive <- "" # no j-drive access needed
  jname <- paste0("-N air_paf_cataract_",year)

  system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,cataract.calc.script,args))

}

# Launch BWGA shifts -------------------------------------------------------------

out.bwga.dir <-  file.path("FILEPATH",output.version)

#see if files have already been saved
bwga.files <- list.files(out.bwga.dir, pattern=".csv", full.names=T)

for(year in years){
  for(loc in locations){

      if (length(grep(paste0("/",loc,"_",year), bwga.files))<1 &
          !(loc==44979 & year==2015)) {

        args <- paste(loc,
                      year,
                      exposure.grid.version,
                      bwga.ier.version,
                      output.version,
                      draws.required,
                      hap.stgpr.runid,
                      hap.map.date)

        size <- file.size(paste0("FILEPATH",exposure.grid.version,"FILEPATH",loc,"_",year,".fst"))

        mem <- .1*size/1e6 + 4
        threads <- 2
        time <- size/1e6 + 20

        mem <- paste0("-l m_mem_free=",mem,"G")
        fthread <- paste0("-l fthread=",threads)
        runtime <- paste0("-l h_rt=00:",time,":00")
        archive <- "-l archive=TRUE"
        jname <- paste0("-N air_bwga_", loc, "_", year, "_",formatC(size, format = "e", digits = 1))

        system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,bwga.script,args))
      }
    }
  }


# Calc RR and PAF ---------------------------------------------------------

outcomes <- fread(file.path(home.dir,"FILEPATH.csv"))

status.dir <- file.path(home.dir,"FILEPATH",output.version)
dir.create(status.dir,showWarnings = F)

# This section pulls info from qustat on running and jobs in queue (helpful if some jobs fail and you need to relaunch)

running <- system("qstat -r | grep \"Full jobname\" -B1", intern=T) #pull job info from cluster on jobs running/in queue
running <- running[seq(2,length(running),3)] # only take rows that contain job names
if(sum(grepl("air_paf",running))>0){
  running <- data.table(running)
  running[,c("location_id","year_id","i"):=tstrsplit(running,"_",keep=c(3,4,5))] #pull out relevant info from jobs
  running[,job:=paste(location_id,year_id,i,sep="_")] #create a column we can use to test to see if our job is still running (or in queue)
  running <- running$job
}else{running <- list()}

# Each job writes an empty csv when it finishes; This line checks which jobs are finished
finished <- list.files(status.dir)

for(year in years){
  for(i in 1:nrow(outcomes)){
    print(paste("Working on year",year, "row",i,"of",nrow(outcomes),"outcomes",Sys.time()))
    
    for (loc in locations) {
  
      if (!(paste0(loc,"_",year,"_",i,".csv") %in% finished) &
          !(paste0(loc,"_",year,"_",i) %in% running)) {  #checks to make sure the job is not already running
  
        #estimate threads and memory based on file size
  
        size <- file.size(paste0("FILEPATH",exposure.grid.version,"FILEPATH",loc,"_",year,".fst"))
  
        mem <- .35 * size/1e6 + 4
        threads <- 2
        time <- 1400 #making this really high. When lots of jobs are running at the same time they slow eachother down a lot
  
        args <- paste(loc,
                      year,
                      exposure.grid.version,
                      ier.version,
                      output.version,
                      draws.required,
                      hap.stgpr.runid,
                      threads,
                      hap.map.date,
                      i)
  
        mem <- paste0("-l m_mem_free=",mem,"G")
        fthread <- paste0("-l fthread=",threads)
        runtime <- paste0("-l h_rt=00:",time,":00")
        archive <- "-l archive=TRUE"
        jname <- paste0("-N air_paf_", loc, "_", year, "_", i, "_",formatC(size, format = "e", digits = 1))
  
        system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,paf.script,args))
        
        Sys.sleep(1)
      }
    }
  }
}