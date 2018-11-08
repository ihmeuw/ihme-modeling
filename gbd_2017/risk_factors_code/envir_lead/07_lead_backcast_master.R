# Project: RF: Lead Exposure
# Purpose: Launch the parallelized backcasting of lead exposure 

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

  project <- "-P proj_crossval " 
  sge.output.dir <- "FILEPATH"


#load packages, install if missing
library(data.table)
library(magrittr)
library(plyr)

# set working directories
coeff.dir <- file.path("FILEPATH")  # where the draws of the blood to bone lead conversion factor is
setwd(coeff.dir)

# Run settings
run_id <- 17702 
input_dir <- file.path("FILEPATH")
draws.required <- 1000
cores.provided <- 4
prep.data1 <- F # whether st-gpr output needs to be merged together and separated by draw, or if that's already been done
prep.data2 <- F # whether draws need to be taken for bone lead conversion factor or if they are already made and stored in coeff.dir
testing <- T

#list out draws
if (testing == T) {
  draws <- c("draw_39")
} else {
  draws <- paste0("draw_",seq(0,draws.required-1,1))
}

##in##
code.dir <- file.path("FILEPATH")
calc.script <- file.path("FILEPATH")
r.shell <- file.path("FILEPATH")
data.path <- file.path("FILEPATH")
dir.create(data.path, recursive = T, showWarnings = FALSE)

# version history
output.version <- 1 #first version

#********************************************************************************************************************************	
#----PREP--------------------------------------------------------------------------------------------------------
if (prep.data2 == TRUE) {
  
  clean.envir <- file.path("FILEPATH") 
  
  #create 1000 draws of the conversion factor
  #currently using the .05 conversion factor 
  coeff.mean <- 0.05
  coeff.sd <- (0.055-0.046)/(2*1.96)
  coeff.draws <- rnorm(draws.required, mean=coeff.mean, sd=coeff.sd)
  
  save(coeff.draws,
       file=clean.envir)
}

#----LAUNCH CALC--------------------------------------------------------------------------------------------------------

if (prep.data1 == TRUE) {
  # read in all locations with draws into one data table
  files <- list.files(input_dir)
  all_data <- rbindlist(lapply(file.path(input_dir,files),fread))
  
  for (draw in draws){
    this_data <- all_data[,c("location_id","year_id","sex_id","age_group_id",draw),with=F]
    setnames(this_data,names(this_data)[5],"data")
    
    write.csv(this_data,file.path(data.path,paste0(draw,".csv")),row.names=F)
    
    # Launch jobs
    jname <- paste0("calc_exp_v", output.version, "_runid_", run_id, "_", draw)
    sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", cores.provided*2, " -l mem_free=", cores.provided*4, "G")
    args <- paste(draw,
                  run_id,
                  output.version,
                  draws.required,
                  cores.provided)
    system(paste(sys.sub, r.shell, calc.script, args))
  }
  
} else {
  for (draw in draws){
    # Launch jobs
    jname <- paste0("calc_exp_v", output.version, "_runid_", run_id, "_", draw)
    sys.sub <- paste0("qsub ",project, sge.output.dir, " -N ", jname, " -pe multi_slot ", cores.provided*2, " -l mem_free=", cores.provided*4, "G")
    args <- paste(draw,
                  run_id,
                  output.version,
                  draws.required,
                  cores.provided)
    system(paste(sys.sub, r.shell, calc.script, args))
    
  }
}
