
#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

###########################################################
#
# CHOOSE DISTRIBUTION TYPE  
distribution <- "ensemblemv2p"
#
###########################################################


library(data.table)

###Specify Directories###
  code.dir <- FILEPATH
  sge.output.dir <- FILEPATH
  #sge.output.dir <- "" # toggle to run with no output files
  output_root <- paste0(j, FILEPATH, distribution)

# create directories
  dir.create(file.path(output_root), showWarnings = FALSE)
 

#********************************************************************************************************************************	

#----ENSEMBLE FIT-----------------------------------------------------------------------------------------------------------------


#----LAUNCH JOBS-----------------------------------------------------------------------------------------------------------------
  
  source(paste0(j,FILEPATH))
  loc_meta <- get_location_metadata(location_set_id=9)
  loc_meta <- loc_meta[is_estimate == 1 & most_detailed == 1]
  locs <- unique(loc_meta$location_id)

  for(loc in locs){
  # Launch jobs
  jname <- paste0("anemia_distfit_loc_",loc)
  model.script <- paste0(code.dir, "/02_fit_", distribution, "_parallel.R")
  r.shell <- file.path(code.dir, "rshell.sh")
  sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", cores.provided*2, " -l mem_free=", cores.provided*4)
  args <- paste(code.dir, output_root, loc, draws.required, distribution)
  
   #print(paste(sys.sub, r.shell, model.script, args))
   system(paste(sys.sub, r.shell, model.script, args))
  }
#*******************************************************************************************************************************

