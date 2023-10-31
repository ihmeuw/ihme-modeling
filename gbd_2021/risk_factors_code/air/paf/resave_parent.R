# clear memory
rm(list=ls())
user <- "USERNAME"

# disable scientific notation
options(scipen = 999)

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


# load packages, install if missing
pacman::p_load(data.table, magrittr, ini)

#define args:
paf.version <- VERSION
risks <- data.table(risk=c("air_pmhap","air_pm","air_hap"), me_id=c(20260,8746,8747), rei_id=c(380,86,87))


project <- "-P PROJECT "

sge.output.dir <- paste0(" -o FILEPATH", user, "FILEPATH -e FILEPATH", user, "FILEPATH ")


save.script <- "FILEPATH/resave.R"
r.shell <- "FILEPATH.sh"



#----------------------------Functions---------------------------------------------------


#Get locations
source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))

locations <- get_location_metadata(location_set_id=35, gbd_round_id=7)

home.dir <- "FILEPATH/"

for(risk in risks$risk){
  paf.dir.new <- file.path(home.dir,risk,"FILEPATH",paf.version)
  dir.create(paf.dir.new,showWarnings = F)
}

out.mediation.dir <- file.path(home.dir,"FILEPATH",paf.version)
dir.create(out.mediation.dir,showWarnings = F)



#--------------------SCRIPT-----------------------------------------------------------------
locs <- locations[most_detailed==1,location_id]

pm.hap.dir <- file.path(home.dir,"FILEPATH",paf.version)
pm.hap.files <- list.files(pm.hap.dir)

pm.dir <- file.path(home.dir,"FILEPATH",paf.version)
pm.files <- list.files(pm.dir)

hap.dir <- file.path(home.dir,"FILEPATH",paf.version)
hap.files <- list.files(hap.dir)


  for(lid in locs){
    if(!(paste0(lid,".csv") %in% pm.hap.files)|
       !(paste0(lid,".csv") %in% pm.files)|
       !(paste0(lid,".csv") %in% hap.files)){

      args <- paste(lid,paf.version)
      mem <- "-l m_mem_free=2G"
      fthread <- "-l fthread=1"
      runtime <- "-l h_rt=04:00:00"
      archive <- ""
      jname <- paste0("-N ","resave_",lid)
      
      system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q long.q",sge.output.dir,r.shell,save.script,args))
    }
  }
