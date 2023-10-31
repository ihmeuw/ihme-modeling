#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: RF: Lead Exposure
# Purpose: Launch the parallelized backcasting of lead exposure
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {

  j_root <- "FILEPATH"
  h_root <- "FILEPATH"

} else {

  j_root <- "FILEPATH"
  h_root <- "FILEPATH"

}

project <- "-P proj_erf "
sge.output.dir <- "-o FILEPATH -e FILEPATH"

#load packages, install if missing
library(data.table)
library(magrittr)
library(plyr)
library(parallel)

# set working directories
coeff.dir <- file.path(j_root, "FILEPATH")  # where the draws of the blood to bone lead conversion factor is
setwd(coeff.dir)

# Run settings
run_id <- 168590 # envir_lead_exp st-gpr run-id; update as needed
input_dir <- file.path("FILEPATH",run_id,"FILEPATH")
draws.required <- 1000
threads <- 10
runtime <- "10:00:00"
prep.data1 <- F # whether draws need to be taken for bone lead conversion factor or if they are already made and stored in coeff.dir
prep.data2 <- F # whether st-gpr output needs to be merged together and separated by draw, or if that's already been done
testing <- F

# version history
output_version <- 14 # GBD 2020 final

#list out draws
if (testing == T) {
  draws <- c("draw_429")
} else {
  draws <- paste0("draw_", 0:(draws.required-1))
}

# code.dir <- file.path(j_root, 'WORK/05_risk/risks/envir_lead/code/') # old
code.dir <- "FILEPATH"
calc.script <- file.path(code.dir, "lead_backcast_calc.R")
r.shell <- "FILEPATH/execRscript.sh -s"
data.path <- file.path("FILEPATH",run_id)
if(!dir.exists(data.path)) dir.create(data.path, recursive = T)

#********************************************************************************************************************************
#----PREP--------------------------------------------------------------------------------------------------------
if (prep.data1 == TRUE) {

  clean.envir <- file.path(coeff.dir, "bone_coeffs.Rdata") #this file will be read in by each parallelized run in order to preserve draw covariance
  #objects exported:
  #coeff.draws = draws of the conversion factor to estimate bone lead from CBLI

  #create 1000 draws of the conversion factor
  #currently using the .05 conversion factor from Howard Hu's paper
  #TODO investigate other options incl sepideh paper
  coeff.mean <- 0.05
  coeff.sd <- (0.055-0.046)/(2*1.96)
  coeff.draws <- rnorm(draws.required, mean=coeff.mean, sd=coeff.sd)

  save(coeff.draws,
       file=clean.envir)
}

#----LAUNCH CALC--------------------------------------------------------------------------------------------------------

if (prep.data2 == TRUE) {
  # read in all locations with draws into one data table, for envir_lead_exp
  files <- list.files(input_dir)
  data <- rbindlist(mclapply(file.path(input_dir,files), fread, mc.cores = 4), use.names = TRUE)
  
  for (draw in draws) {
    this_data <- data[,c("location_id","year_id","sex_id","age_group_id",draw),with=F]
    setnames(this_data,names(this_data)[5],"data")

    write.csv(this_data,file.path(data.path,paste0(draw,".csv")),row.names=F)

    # Launch jobs
    jname <- paste0("lead_backcast_v", output_version, "_runid_", run_id, "_", draw)
    sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -l fthread=", threads, " -l m_mem_free=50G -l h_rt=", runtime, " -l archive=TRUE -q all.q")
    args <- paste(draw,
                  run_id,
                  output_version,
                  draws.required,
                  threads)
    system(paste(sys.sub, r.shell, calc.script, args))
  }

} else {
  for (draw in draws){
    # Launch jobs
    jname <- paste0("lead_backcast_v", output_version, "_runid_", run_id, "_", draw)
    sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -l fthread=", threads, " -l m_mem_free=50G -l h_rt=", runtime, " -l archive=TRUE -q all.q")
    args <- paste(draw,
                  run_id,
                  output_version,
                  draws.required,
                  threads)
    system(paste(sys.sub, r.shell, calc.script, args))

  }
}

