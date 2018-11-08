#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Launch the parallelized calculation of injury PAFs
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

if (Sys.getenv('SGE_CLUSTER_NAME') == "prod" ) {

  project <- "-P PROJECT " # -p must be set on the production cluster in order to get slots and not be in trouble
  sge.output.dir <- "-o FILEPATH -e FILEPATH "
  #sge.output.dir <- "" # toggle to run with no output files

} else {

  project <- "-P PROJECT " # dev cluster has project classes now too
  sge.output.dir <- "-o FILEPATH -e FILEPATH "
  #sge.output.dir <- "" # toggle to run with no output files

}

# load packages, install if missing
pacman::p_load(data.table, magrittr)


# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)


# set project values
location_set_version_id <- 397
year_end <- 2017
retry.toggle <- F #toggle if doing a second run because of job failures

# Settings
squeeze.version <- 30
draws.required <- 1000
cores.provided <- 5

# version history
output.version <- 1 # first version of occ PAFs
output.version <- 2 #second version should match #1
output.version <- 3 #updated codcorrect run (#56)
output.version <- 4 #updated injuries exposure after finding fwrite problems
output.version <- 5 #new injuries exposure with workers v10
output.version <- 6 #new injuries exposure with workers v11
output.version <- 7 #new cap at 85% after pafs were exceeding 1 in some cases
output.version <- 8 #GBD 17 first
output.version <- 9 # GBD 17 Final

###in/out###
##in##
code.dir <- file.path(j_root, 'FILEPATH')
calc.script <- file.path(code.dir, "calc_inj.R")
r.shell <- "FILEPATH"

##out##
paf.dir <- file.path('FILEPATH', output.version)
#********************************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
#ubcov functions#
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source()

#job launcher
launchCountry <- function(country,
                          name,
                          script,
                          args,
                          cores=5,
                          retry=F,
                          out.dir=NA,
                          out.file=NA) {

  # Prepare job settings
  slots <- cores
  mem <- cores * 4
  jname <- paste0(name, "_loc_", country)

  #add the looping country as 1st arg and the cores as last arg
  out.args <- paste(country,
                    args,
                    cores)

  # Create submission call
  sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")


  if(retry==TRUE) { #add capacity for second run if memory or cluster issues are causing failure

    if (length(grep(paste0('paf_yld_', country, '_', year_end, '_2.csv'),
                    list.files(path=out.dir, pattern=out.file, full.names=F)))==0) {

      out <- paste0('retrying...', country)
      paste(sys.sub, r.shell, script, out.args) %>% system

    } else {

      out <- paste0(country, " is finished...skipping")

    }

  } else {

    out <- paste0('launching...', jname)
    paste(sys.sub, r.shell, script, out.args) %>% system
  }

  return(out)

}
#********************************************************************************************************************************

#----LAUNCH CALC--------------------------------------------------------------------------------------------------------
# Get the list of most detailed GBD locations
source(file.path(j_root,"FILEPATH"))
locs <- get_location_metadata(gbd_round_id = 5, location_set_id = 22)
locations <- unique(locs[is_estimate==1, location_id]) %>% sort

status <- lapply(locations,
                 launchCountry,
                 name="inj_paf",
                 script=calc.script,
                 args=paste(squeeze.version, output.version, draws.required),
                 retry=retry.toggle,
                 out.dir=paf.dir,
                 out.file=".csv",
                 cores=cores.provided)

retry.locs <- locations[!lapply(status, function(x) grepl('is finished', x)) %>% unlist]

if (length(retry.locs)!=0) {
  message('retrying for: ', paste0(retry.locs, "|"))
} else message('inj exposure = all done!')
