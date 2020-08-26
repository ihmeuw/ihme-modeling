###########################################################
### Purpose: Collapse ubcov extraction output
### DOCUMENTATION: ADDRESS
###########################################################
### Purpose: Update code to run in parallel on the fair cluster (buster).
###########################################################
# source("FILEPATH", echo=T)
###################
### Setting up ####
###################

##install.packages('RMySQL')

rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j       <- "ADDRESS"
  h       <- "ADDRESS"
  erf     <- "ADDRESS"

} else {
  j       <- "ADDRESS"
  user    <- Sys.info()[["user"]]
  h       <- paste0("ADDRESS", user)
  erf     <- "ADDRESS"
}

#Packages:
lib.loc <- paste0(h,"/R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","haven","dplyr","survey","RMySQL")


for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

## Load Functions
ubcov_central <- paste0(j, "ADDRESS")
setwd(ubcov_central)
source("ADDRESS")

######################################################################################################################

## Settings

topic <- "hap" 
config.path <- paste0(erf, "ADDRESS") ## Path to config.csv
census <- F ## are you collapsing censuses that are in the "census" folder?
parallel <- T ## Run in parallel?
fthreads <- 3 ## How many cores per job? (used in mclapply) | Set to 1 if running on desktop or serially, 1 is usually enough here
m_mem_free <- 35 ## How much GB of RAM per job?
h_rt <- "10:00:00" ## How long should your job run? (hh:mm:ss)
logs <- paste0(erf, "ADDRESS") ## Path to logs
cluster_proj <- 'erf'

## Launch collapse
df <- collapse.launch(topic=topic, config.path=config.path, cluster_project=cluster_proj, parallel=parallel, fthreads=fthreads, m_mem_free=m_mem_free, h_rt=h_rt, logs=logs)


#If collapsing censuses, clear out census folder for new extractions
if(census){
  system("mv -v ADDRESS* ADDRESS")
}
