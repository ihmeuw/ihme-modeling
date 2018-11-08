##################################################################################
## Intimate Partner Violence - HIV PAF calculation MASTER
## PURPOSE: (1) get HIV incidence draws
##          (2) prepare proportion HIV transmission by sex but not by CSW
##          (3) meta-analysis for relative-risk
##          (4) launch jobs to calculate PAF by location
##          (5) save results
##################################################################################

# SET-UP ------------------------------------------------------------------

rm(list=ls())

library(pacman)
p_load(data.table,dplyr)

# toggles for different steps
hiv <- F
csw <- F
rr <- F
paf_calc <- F
save_results <- T

# spectrum name
spectrum.name <- 'FILENAME'

# shared functions
shared_functions_dir <- 'FILEPATH'
source(paste0(shared_functions_dir,'/get_location_metadata.R'))

# directories
main_dir <- 'FILEPATH'
code_dir <- 'FILEPATH'

# get locations
loc_table <- get_location_metadata(35)
loc_table <- loc_table[is_estimate==1& most_detailed==1,]
locations <- unique(loc_table$location_id)

# set version
date <- Sys.Date()
ver <- gsub("-", "_", date)

# HIV INCIDENCE -----------------------------------------------------------

if(hiv==T) source(paste0(code_dir,"/01_hiv_inc_draws.R"))

# CSW CALCULATION ---------------------------------------------------------

if(csw==T) source(paste0(code_dir,"/02_csw.R"))

# RELATIVE RISK META-ANALYSIS ---------------------------------------------

if(rr==T) source(paste0(code_dir,"/03_rr_draws.R"))

# PAF CALCULATION ---------------------------------------------------------

if(paf_calc==T){
  if(dir.exists(paste0(main_dir,"/paf_draws_",ver))==F) dir.create(paste0(main_dir,"/paf_draws_",ver))
  hold_list <- c()
  for(loc_id in locations){
    
    child_name <- paste0("ipv_hiv_",loc_id)
    hold_list <- c(hold_list, child_name)
    sys.sub <- paste0("qsub -N ", child_name, " -P proj_custom_models -pe multi_slot 10")
    args <- c(loc_id,main_dir,ver)
    args <- list(paste(args, collapse = " "))
    r_shell <- "FILEPATH/r_shell.sh"
    child_script <- paste0(code_dir,"/04_hiv_paf_calc.R")
    
    qsub <- paste(sys.sub, r_shell, child_script, "\"", args, "\"")
    system(paste(qsub))
    
  }
}

# SAVE RESULTS ------------------------------------------------------------

if(save_results==T){
  if(paf_calc==T){
    
    sys.sub <- paste0("qsub -N ipv_hiv_save_results -P proj_custom_models -pe multi_slot 40 -hold_jid " , hold_list)
    args <- c(spectrum.name,main_dir,ver)
    args <- list(paste(args, collapse = " "))
    r_shell <-"FILEPATH/r_shell.sh"
    script <- paste0(code_dir,"/05_save_results_paf.R")
    
    qsub <- paste(sys.sub, r_shell, script, "\"", args, "\"")
    system(paste(qsub))
    
   } else {
     source(paste0(code_dir,"/05_save_results_paf.R"))
   }
 }