#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: RF: Lead Exposure
# Purpose: Launch the parallelized backcasting of lead exposure
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

project <- "-P ADDRESS "
sge.output.dir <- "FILEPATH"

#load packages, install if missing
library(data.table)
library(magrittr)
library(plyr)

# set working directories
coeff.dir <- file.path("FILEPATH")  # where the draws of the blood to bone lead conversion factor is
setwd(coeff.dir)

# Run settings
run_id <- 83492 # envir_lead_exp st-gpr run-id with draws; update as needed
input_dir <- file.path("FILEPATH")
draws.required <- 1000
threads <- 20
runtime <- "5:00:00"
prep.data1 <- F # whether draws need to be taken for bone lead conversion factor or if they are already made and stored in coeff.dir
prep.data2 <- T # whether st-gpr output needs to be merged together and separated by draw, or if that's already been done
testing <- F

#list out draws
if (testing == T) {
  draws <- c("draw_429")
} else {
  draws <- paste0("draw_", 0:(draws.required-1))
}

##in##
# version history
output.version <- 1 # gbd17 first versions; initial 2019 run stored here too (oops)
output.version <- 2 # gbd17
output.version <- 3 # 2019 decomp step 1
output.version <- 4 # 2019 decomp step 2
output.version <- 5 # 2019 decomp step 3 (edit 1/3/2020: no changes made in step 4, so this is the final output.version for GBD 2019)

code.dir <- "FILEPATH"
calc.script <- "SCRIPT"
r.shell <- "SHELL"
data.path <- file.path("FILEPATH")
if(!dir.exists(data.path)) dir.create(data.path, recursive = T)

#********************************************************************************************************************************
#----PREP--------------------------------------------------------------------------------------------------------
if (prep.data1 == TRUE) {

  clean.envir <- file.path(coeff.dir, "bone_coeffs.Rdata") #this file will be read in by each parallelized run in order to preserve draw covariance
  #objects exported:
  #coeff.draws = draws of the conversion factor to estimate bone lead from CBLI

  #create 1000 draws of the conversion factor
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
  data <- rbindlist(lapply(file.path(input_dir,files),fread), use.names = TRUE)

  all_data <- copy(data)

  for (draw in draws){
    this_data <- all_data[,c("location_id","year_id","sex_id","age_group_id",draw),with=F]
    setnames(this_data,names(this_data)[5],"data")

    write.csv(this_data,file.path(data.path,paste0(draw,".csv")),row.names=F)

    # Launch jobs
    jname <- paste0("lead_backcast_v", output.version, "_runid_", run_id, "_", draw)
    sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -l fthread=", threads, " -l m_mem_free=30G -l h_rt=", runtime, " -l archive=TRUE -q all.q")
    args <- paste(draw,
                  run_id,
                  output.version,
                  draws.required,
                  threads)
    system(paste(sys.sub, r.shell, calc.script, args))
  }

} else {
  for (draw in draws){
    # Launch jobs
    jname <- paste0("lead_backcast_v", output.version, "_runid_", run_id, "_", draw)
    sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -l fthread=", threads, " -l m_mem_free=30G -l h_rt=", runtime, " -l archive=TRUE -q all.q")
    args <- paste(draw,
                  run_id,
                  output.version,
                  draws.required,
                  threads)
    system(paste(sys.sub, r.shell, calc.script, args))

  }
}

## END
