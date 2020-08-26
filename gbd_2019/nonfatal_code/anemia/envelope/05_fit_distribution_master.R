###########################################################
### Project: Anemia
### Purpose: Fit Distribution to Modeled Mean & Standard Deviation. Launches parallelization of disriubtion type (ensemble or weibull)
###########################################################

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

###########################################################
#
# CHOOSE DISTRIBUTION TYPE
distribution <- "ensemblemv2p"
#
###########################################################


os <- .Platform$OS.type
j <- "FILEPATH"
h <- "FILEPATH"


# load packages, install if missing
library(data.table)

# Settings
project <- "-P proj_anemia " 
draws.required <- 1000 
resume <- F 

###Specify Directories###
  code.dir <- paste0("FILEPATH")
  sge.output.dir <- "-o FILEPATH -e FILEPATH "
  output_root <- paste0("FILEPATH", distribution)

# create directories
  dir.create(file.path(output_root), showWarnings = FALSE)
  dir.create(file.path("FILEPATH"), showWarnings = FALSE)
  dir.create(file.path("FILEPATH"), showWarnings = FALSE)
  dir.create(file.path("FILEPATH"), showWarnings = FALSE)

#********************************************************************************************************************************	

#----ENSEMBLE FIT-----------------------------------------------------------------------------------------------------------------
rerun_weights = FALSE 

if(rerun_weights){ 
  NOISY = FALSE
  runTest = FALSE 
  
  ihmeDistSelect.R <- file.path(code.dir, "FILEPATH/ihmeDistSelect.R")
  setwd(file.path(code.dir, "FILEPATH"))
  source(ihmeDistSelect.R)
  
  load(paste0(output_root,"FILEPATH/data.child.rda"))
  load(paste0(output_root,"FILEPATH/data.men.rda"))
  load(paste0(output_root,"FILEPATH/data.women.rda"))
  thresh.child <- c(110,100,70)
  thresh.men <- c(130,110,80)
  thresh.women <- c(120,110,80)

  modSelect.child <- list()
  modSelect.men <- list()
  modSelect.women <- list()

  weights.child <- list()
  weights.men <- list()
  weights.women <- list()

  for(i in 1:3){
    print(paste0("FITTING SEVERITY ", i))
    print("child")
    modSelect.child[[i]] <- ihmeDistSelect(data.child, classA, thresh.child[[i]], "lower")
    weights.child[[i]] <- modSelect.child[[1]]$MoMcompare$full
    print("men")
    modSelect.men[[i]] = ihmeDistSelect(data.men, classA, thresh.men[[i]], "lower")
    weights.men[[i]] <- modSelect.men[[1]]$MoMcompare$full
    print("women")
    modSelect.women[[i]] = ihmeDistSelect(data.women, classA, thresh.women[[i]], "lower")
    weights.women[[i]] <- modSelect.women[[1]]$MoMcompare$full

  }
  
  weightdir <- paste0(output_root, "/weights")
  dir.create(file.path(weightdir), showWarnings = FALSE)
  
  save(weights.child, file = paste0(weightdir, "/weights.child.rda"))
  save(weights.men, file = paste0(weightdir, "/weights.men.rda"))
  save(weights.women, file = paste0(weightdir, "/weights.women.rda"))
}

#********************************************************************************************************************************

if (resume==FALSE){  
  for(meid in c(10489, 10490, 10491, 10507)) {
    print(paste("DROPPING OLD RESULTS FOR MEID", meid))
    dir.create(paste0(output_root, "/", meid), showWarnings = FALSE)
    setwd(paste0(output_root, "/", meid))
    old_files <- list.files()
    file.remove(old_files)
  } 
}


#----LAUNCH JOBS-----------------------------------------------------------------------------------------------------------------
  
  source(paste0("FILEPATH/get_location_metadata.R"))
  loc_meta <- get_location_metadata(location_set_id=9,gbd_round_id=6)
  loc_meta <- loc_meta[is_estimate == 1 & most_detailed == 1]
  locs <- unique(loc_meta$location_id)
  
  for(loc in locs){
  # Launch jobs
  jname <- paste0("anemia_distfit_loc_",loc)
  model.script <- paste0(code.dir, "/fit_", distribution, "_parallel.R")
  r.shell <- file.path(code.dir, "rshell.sh")
  sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -l m_mem_free=3G -l fthread=2 -l h_rt=00:20:00 -q all.q")
  args <- paste(code.dir, output_root, loc, draws.required, distribution)
  
     system(paste(sys.sub, r.shell, model.script, args))
  }
#*******************************************************************************************************************************

  
if(T) {
  print("ALL JOBS LAUNCHED - FEEL FREE TO QUIT R")
  print("TIMER TO SEE WHEN ALL JOBS ARE DONE - every 60s")
  finished <- list.files(paste0(output_root, "/10507"))
  while(length(finished) < length(locs)) {
    finished <- list.files(paste0(output_root, "/10507"))
    print(paste(length(finished), "of", length(locs), "jobs finished"))
    Sys.sleep(60)
  }
  print("CONGRATS - ALL JOBS FINISHED")
  }