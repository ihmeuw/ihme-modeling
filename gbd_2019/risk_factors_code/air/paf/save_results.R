#----HEADER----------------------------------------------------------------------------------------------------------------------
# Author: NAME
# Date: 4/19/2018
# Purpose: child script for save results for air_pm, air_hap and air, new proportional pafs method
# source("FILEPATH.R", echo=T)
# qsub -N save_air_pm -pe multi_slot 40 -P ADDRESS -o FILEPATH -e FILEPATH FILEPATH.sh FILEPATH.R 
#*********************************************************************************************************************************
#------------------------Setup -------------------------------------------------------
# clear memory
rm(list=ls())
user <- "USERNAME"

# disable scientific notation
options(scipen = 999)

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

  arg <- commandArgs(trailingOnly=T)

  if (length(arg)==0) {
    #toggle for targeted run on cluster
    arg <- c("air_pmhap", #risk
             20260, #me_id
             380, #rei_id
             44, #paf version
             "step4") #decomp_step
  }
  
# load packages, install if missing
pacman::p_load(data.table, magrittr, ini)

#define args:
risk <- arg[1]
me <- arg[2]
rei <- arg[3] %>% as.numeric
paf.version <- arg[4]
decomp <- arg[5]
years <- 1990:2019

#draw directories
home.dir <- file.path("FILEPATH")
paf.dir.new <- file.path(home.dir,risk,"FILEPATH",paf.version)

#output directories
paf.scatter <- file.path(home.dir,risk,"FILEPATH",paf.version)
dir.create(paf.scatter, showWarnings=F, recursive=T)

#----------------------------Functions---------------------------------------------------
#Save Results Risk (RR, PAF)
source(file.path(central_lib,"FILEPATH.R"))
#PAF scatters
source("FILEPATH.R")

#--------------------Save and plot PAFs-----------------------------------------------------

save_results_risk(input_dir = paf.dir.new,
                  input_file_pattern = "{location_id}.csv",
                  modelable_entity_id = me,
                  year_id = years,
                  description = paste0("PAFs version: ",paf.version," estimation years"),
                  risk_type = "paf",
                  mark_best=T,
                  decomp_step=decomp,
                  gbd_round_id=6)

#PAF Scatters
 
paf_scatter(rei_id= rei,
               measure_id= 4,
               file_path= file.path(paste0(paf.scatter,"/",risk,"FILEPATH",paf.version,"FILEPATH.pdf")),
               year_id= 1990,
               decomp_step=decomp)
 
paf_scatter(rei_id= rei,
               measure_id= 3,
               file_path= file.path(paste0(paf.scatter,"/",risk,"FILEPATH",paf.version,"FILEPATH.pdf")),
               year_id= 1990,
             decomp_step=decomp)
 
paf_scatter(rei_id= rei,
             measure_id= 4,
             file_path= file.path(paste0(paf.scatter,"/",risk,"FILEPATH",paf.version,"FILEPATH.pdf")),
             year_id= 2000,
             decomp_step=decomp)
 
paf_scatter(rei_id= rei,
             measure_id= 3,
             file_path= file.path(paste0(paf.scatter,"/",risk,"FILEPATH",paf.version,"FILEPATH.pdf")),
             year_id= 2000,
             decomp_step=decomp)
 
paf_scatter(rei_id= rei,
             measure_id= 4,
             file_path= file.path(paste0(paf.scatter,"/",risk,"FILEPATH",paf.version,"FILEPATH.pdf")),
             year_id= 2017,
             decomp_step=decomp)
 
paf_scatter(rei_id= rei,
             measure_id= 3,
             file_path= file.path(paste0(paf.scatter,"/",risk,"FILEPATH",paf.version,"FILEPATH.pdf")),
             year_id= 2017,
             decomp_step=decomp)

paf_scatter(rei_id= rei,
            measure_id= 4,
            file_path= file.path(paste0(paf.scatter,"/",risk,"FILEPATH",paf.version,"FILEPATH.pdf")),
            year_id= 2019,
            decomp_step=decomp)

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"FILEPATH",paf.version,"FILEPATH.pdf")),
            year_id= 2019,
            decomp_step=decomp)