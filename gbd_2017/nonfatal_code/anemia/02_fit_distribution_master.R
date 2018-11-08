#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

###########################################################
#
distribution <- "ensemblemv2p"
#
###########################################################

# load packages, install if missing
library(data.table)

# Settings
project <- "-P proj_anemia " 
cores.provided <- 1 
draws.required <- 1000 


###Specify Directories###
  code.dir <- FILEPATH
  sge.output.dir <- "-o FILEPATH -e FILEPATH"
  output_root <- paste0(FILEPATH, distribution)

# create directories
  dir.create(file.path(output_root), showWarnings = FALSE)
  dir.create(file.path(FILEPATH), showWarnings = FALSE)
  dir.create(file.path(FILEPATH), showWarnings = FALSE)
  dir.create(file.path(FILEPATH), showWarnings = FALSE)


#********************************************************************************************************************************	

#----ENSEMBLE FIT-----------------------------------------------------------------------------------------------------------------



rerun_weights = FALSE 

if(rerun_weights){ 
  NOISY = FALSE
  runTest = FALSE 
  
  ihmeDistSelect.R <- file.path(code.dir, FILEPATH)
  setwd(file.path(code.dir, FILEPATH))
  source(ihmeDistSelect.R)
  
  load(paste0(output_root,FILEPATH))
  load(paste0(output_root,FILEPATH))
  load(paste0(output_root,FILEPATH))

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
  
  weightdir <- paste0(output_root, FILEPATH)
  dir.create(file.path(weightdir), showWarnings = FALSE)
  
  save(weights.child, file = paste0(weightdir, FILEPATH))
  save(weights.men, file = paste0(weightdir, FILEPATH))
  save(weights.women, file = paste0(weightdir, FILEPATH))
}

#********************************************************************************************************************************

#----LAUNCH JOBS-----------------------------------------------------------------------------------------------------------------
  
  source(FILEPATH/get_location_metadata.R)
  loc_meta <- get_location_metadata(location_set_id=9)
  loc_meta <- loc_meta[is_estimate == 1 & most_detailed == 1]
  locs <- unique(loc_meta$location_id)

  source(FILEPATH/get_covariate_estimates.R)
  cov <- get_covariate_estimates(covariate_id=13)
  write.csv(cov, FILEPATH, row.names = F)
  
  for(loc in locs){
  # Launch jobs
  jname <- paste0("anemia_distfit_loc_",loc)
  model.script <- paste0(code.dir, "/02_fit_", distribution, "_parallel.R")
  r.shell <- file.path(code.dir, "rshell.sh")
  sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", cores.provided*2, " -l mem_free=", cores.provided*4)
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
  
