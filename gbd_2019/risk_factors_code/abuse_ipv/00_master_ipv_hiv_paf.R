##################################################################################
## Intimate Partner Violence - HIV PAF calculation MASTER
## AUTHOR: USERNAME
## DATE: June 2018
## PROJECT: Intimate partner violence, GBD risk factors
## PURPOSE: (1) get HIV incidence draws
##          (2) prepare proportion HIV transmission by sex but not by CSW
##          (3) meta-analysis for relative-risk
##          (4) launch jobs to calculate PAF by location
##          (5) save results
## NOTE: only works on the cluster
##################################################################################

# SET-UP ------------------------------------------------------------------

rm(list=ls())

# library(pacman)
# p_load(data.table,dplyr)
library(data.table)
library(dplyr)

# toggles for whichever combination of steps you would like to run
hiv <- F
csw <- F
rr <- F #always set to False, because 2019 revision to how RR draws created that doesn't use the script called in here
paf_calc <- F
save_results <- T

# decomp step -- CHANGE AS NECESSARY
decomp <- 'step4'

# modeler (who's sgeoutput dir should errors and logs be read to?) -- CHANGE AS NECESSARY
modeler <- 'USERNAME'

# spectrum name -- CHANGE HERE IF MODEL CHANGES, CAN GET NAME FROM HIV TEAM 
if(decomp=='step3'){
  spectrum.name <- '190730_quetzal'
} else if(decomp=='step4_deprecated'){
  spectrum.name <- '190630_rhino'
} else if(decomp=='step4'){
  spectrum.name <- '190630_rhino'
  #note that the bested version uses spectrum.name <- '190630_rhino_combined', but when this
  #was estimated, during step4, they were using '190630_rhino'
} else {
  warning("need to get correct spectrum name")
}

# shared functions
shared_functions_dir <- 'FILEPATH'
source(paste0(shared_functions_dir,'/get_location_metadata.R'))

# directories
main_dir <- 'FILEPATH'
code_dir <- 'FILEPATH'
shell <- 'FILEPATH'

# get locations
loc_table <- get_location_metadata(35, decomp_step = 'step4', gbd_round_id = 6)
loc_table <- loc_table[is_estimate==1& most_detailed==1,]
locations <- unique(loc_table$location_id)

# set version
date <- Sys.Date()
date <- "2019_10_22"
ver <- gsub("-", "_", date)

# HIV INCIDENCE -----------------------------------------------------------

if(hiv==T){
  print(paste0('Using spectrum draws from model ',spectrum.name, '. Please confirm that this is the best model.'))
  source(paste0(code_dir,"/01_hiv_inc_draws.R")) # source hiv_draws function
  hiv_draws(spectrum.name,main_dir)
}

# CSW CALCULATION ---------------------------------------------------------

if(csw==T) source(paste0(code_dir,"/02_csw.R"))

# RELATIVE RISK META-ANALYSIS ---------------------------------------------

if(rr==T) source(paste0(code_dir,"/03_rr_draws.R"))

# PAF CALCULATION ---------------------------------------------------------

mem <- '20G'
threads <- 10
rt <- "5:00:00"
if(paf_calc==T){
  if(dir.exists(paste0(main_dir,"/paf_draws_",ver))==F) dir.create(paste0(main_dir,"/paf_draws_",ver))
  hold_list <- c()
  
  # function to build qsub, and append to hold_list jobs submitted
  submit_qsub <- function(loc_id){
    print(loc_id)
    child_name <- paste0("ipv_hiv_",loc_id)
    updated_hold_list <- c(hold_list, child_name)
    errors <- "-o FILEPATH"
    sys.sub <- paste0("qsub -N ", child_name, " -P proj_paf -l m_mem_free=", mem, " -l fthread=", threads, 
                      " -q all.q -l archive=TRUE -l h_rt=", rt, " ", errors)
    
    args <- c(loc_id, main_dir, ver, spectrum.name, decomp)
    args <- list(paste(args, collapse = " "))
    child_script <- paste0(code_dir,"/04_hiv_paf_calc.R")
    
    qsub <- paste(sys.sub, shell," -s ", child_script," ", args)
    system(paste(qsub))
    return(updated_hold_list)
  }
  
  #log jobs submitted, launch jobs over all locations
  hold_list <- sapply(locations, submit_qsub)
  
  ## run this portion to see if there are any missing locations and run the following loop if so!
  ## run for missing locations
  missing <- c()
  files <- list.files(paste0(main_dir,'/paf_draws_',ver),full.names=F)
  for(loc in locations){
    if(!paste0(loc,"_2.csv")%in%files){
      missing <- c(missing, loc)
    }
  }
  print("missing these locations:")
  print(missing)

  # locations <- c() # use this line to only use a subset of locations
  hold_list <- sapply(missing, submit_qsub)
  hold_list <- paste(hold_list,sep=",",collapse=",")
}

# SAVE RESULTS ------------------------------------------------------------

if(save_results==T){
  
  print("Launching save_results")
  
  if(paf_calc==T) sys.sub <- paste0("qsub -N ipv_hiv_save_results -P proj_team -pe multi_slot 40 -hold_jid ", hold_list)
  if(paf_calc==F) sys.sub <- paste0("qsub -N save_ipvhiv_",decomp," -P proj_team -l m_mem_free=40G -l fthread=20 -q all.q -l h_rt=5:00:00 -l archive=TRUE -o FILEPATH")
  args <- c(spectrum.name,main_dir,ver,decomp)
  args <- list(paste(args, collapse = " "))
  script <- paste0(code_dir,"/05_save_results_paf.R")
  
  qsub <- paste(sys.sub, shell," -s ",script, " ", args)
  qsub_sarah <- paste0("qsub -N save_ipvhiv -P proj_paf -l m_mem_free=40G -l fthread=20 -q all.q -l h_rt=5:00:00 -o FILEPATH ", shell, " -s ", script, " ", args)
  system(paste(qsub))
  
}

# END