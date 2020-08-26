
rm(list=ls())


# runtime configuration
if (Sys.info()["sysname"] == "Linux") {

  j <- "ADDRESS"
  h <- "/ADDRESS/USERNAME/"

} else {

  j <- "ADDRESS"
  h <- "ADDRESS"

}

script <- file.path("ADDRESS/extract_ERA5.R")
sge.output.dir<-"-j y -o ADDRESS"
rshell<-file.path("ADDRESS/health_fin_forecasting_shell_singularity.sh")

#############################################################################################################################

source("ADDRESS/get_location_metadata.R")
reporting_hierarchy<- get_location_metadata(location_set_id=35)
rhierarchy<-reporting_hierarchy[is_estimate==1&most_detailed==1]
LOC<-as.vector(rhierarchy[,c("location_id")])
LOC<-rhierarchy$location_id

LOCATION_ID<-LOC
YEARS<-rep(2019,990)

mem <- "-l m_mem_free=100G"
fthread <- "-l fthread=4"
runtime <- "-l h_rt=48:00:00"
archive <- "-l archive=TRUE" # or "" if no jdrive access needed
project<-"-P proj_custom_models"


for (i in 1:length(YEARS)){
  year<-YEARS[i]
  loc<-as.numeric((LOCATION_ID[i]))
  args <- paste(year,loc)
  jname <- paste0("-N paf",year,loc)

  #NOTE: project, sge.output.dir, & rshell MUST be defined elsewhere in script
  system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,script,args))
  print(i)
}


