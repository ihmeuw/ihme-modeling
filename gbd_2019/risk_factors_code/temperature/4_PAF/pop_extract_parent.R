
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {

  j <- "ADDRESS"
  h <- "/ADDRESS/USERNAME/"

} else {

  j <- "ADDRESS"
  h <- "ADDRESS"

}


script <- file.path("FILEPATH/pop_extract.R")
sge.output.dir<-"-j y -o FILEPATH"
rshell<-file.path("FILEPATH/health_fin_forecasting_shell_singularity.sh")

#############################################################################################################################

YEARS<-seq(1990,2018,1)

mem <- "-l m_mem_free=40G"
fthread <- "-l fthread=4"
runtime <- "-l h_rt=24:00:00"
archive <- "-l archive=TRUE" # or "" if no jdrive access needed
project<-"-P proj_custom_models"

for (i in 1:length(YEARS)){

  year<-YEARS[i]
  args <- paste(year)
  jname <- paste0("-N ","pop",year)

  #NOTE: project, sge.output.dir, & rshell MUST be defined elsewhere in script
  system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,script,args))

}

