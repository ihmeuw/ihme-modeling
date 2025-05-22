#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Save results for envir_radon
#------------------------Setup -------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/"
  h_root <- "~/"
  central_lib <- "/ihme/cc_resources/libraries/"
} else {
  j_root <- "J:/"
  h_root <- "H:/"
  central_lib <- "K:/libraries/"
}

project <- "proj_erf "
sge.output.dir <- " -o /ADDRESS -e /ADDRESS "


# load packages, install if missing
pacman::p_load(data.table, magrittr,ini)

years <- 1990:2022
decomp <- "iterative"

#----------------------------Functions---------------------------------------------------

#Save Results Epi (EXP)
source(file.path(central_lib,"current/r/save_results_epi.R"))

#Save Results Risk (RR, PAF)
source(file.path(central_lib,"current/r/save_results_risk.R"))

#get demographics
source(file.path(central_lib,"current/r/get_demographics.R"))

#get draws
source(file.path(central_lib,"current/r/get_draws.R"))


#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#--------------------SCRIPT-----------------------------------------------------------------


  #PAF_calculator code:
  source("/ihme/code/risk/paf/launch_paf.R")
  launch_paf(90, year_id=years, decomp_step=decomp, cluster_proj=project)

  #PAF scatters
  source("/ihme/code/risk/diagnostics/paf_scatter.R")
  
  #hold for 1 hour until save results finishes
  Sys.sleep(60*60*1)
  # 
  
  for (year in years){
    paf_scatter(rei_id = 90,
                file_path = paste0("/FILEPATH/paf_scatter_yld_",year,"_",decomp,".pdf"),
                measure_id = 3,
                year_id = year,
                gbd_round_id = 7,
                decomp_step = decomp,
                add_isos = F)

    paf_scatter(rei_id = 90,
                file_path = paste0("/FILEPATH/paf_scatter_yll_",year,"_",decomp,".pdf"),
                measure_id = 4,
                year_id = year,
                gbd_round_id = 7,
                decomp_step = decomp,
                add_isos = F)
    # version_id=)
  }


