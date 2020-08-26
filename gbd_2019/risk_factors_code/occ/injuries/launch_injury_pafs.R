#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Launch the parallelized calculation of PAF calculation for occ injuries
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

project <- " -P proj_erf "
sge.output.dir <- "-o FILEPATH -e FILEPATH "

# load packages, install if missing
pacman::p_load(data.table, magrittr)

# set project values
decomp <- "step4"
year_end <- 2019 # this is used to check if files need to be rerun or not; set it to the highest year specified in calc script
retry.toggle <- T # toggle if doing a second run because of job failures

# Settings
squeeze.version <- 40
draws.required <- 1000
cores.provided <- 5

# version history
output.version <- 16 # GBD 2019 final

###in/out###
##in##
code.dir <- "FILEPATH"
calc.script <- file.path(code.dir, "FILEPATH")
r.shell <- "SHELL"

##out##
paf.dir <- file.path("FILEPATH", output.version)
#********************************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "FUNCTION") %>% source

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
  threads <- 5
  mem <- 25
  jname <- paste0(name, "_loc_", country)

  #add the looping country as 1st arg and the cores as last arg
  out.args <- paste(country,
                    args,
                    cores)

  # Create submission call
  sys.sub <- paste0("qsub -l m_mem_free=", mem, "G -l fthread=", threads, project, "-q long.q -l h_rt=6:00:00 -l archive=TRUE ", sge.output.dir, "-N ", jname)

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

  message(out)

}
#********************************************************************************************************************************

#----LAUNCH CALC--------------------------------------------------------------------------------------------------------
# Get the list of most detailed GBD locations
source("FUNCTION")
locs <- get_location_metadata(gbd_round_id = 6, location_set_id = 22, decomp_step = "step4")
locations <- unique(locs[is_estimate==1, location_id]) %>% sort

status <- lapply(locations,
                 launchCountry,
                 name="inj_paf_calc",
                 script=calc.script,
                 args=paste(squeeze.version, output.version, draws.required, decomp),
                 retry=retry.toggle,
                 out.dir=paf.dir,
                 out.file=".csv",
                 cores=cores.provided)

retry.locs <- locations[!lapply(status, function(x) grepl('is finished', x)) %>% unlist]

if (length(retry.locs)!=0) {
  message('retrying for: ', paste0(retry.locs, "|"))
} else message('inj exposure = all done!')

#see if files have already been saved
files <- list.files(path=out.dir, pattern=".csv", full.names=F)

for (country in locations) {

  mem <- cores.provided * 4
  slots <- cores.provided * 2

  if(retry==TRUE) { #add capacity for second run if memory or cluster issues are causing failure

    if (length(grep(paste0("paf_yll_", country, "_"), files))==0) {

      cores.provided <- 10
      mem <- 40
      slots <- 20 #try running again with a lot of memory but few slots. will be slow but should get it done

    } else next

  }

  message("launching PAF calc for loc ", country, "\n --using ", slots, " slots and ", mem, "GB of mem")

  # Launch jobs
  jname <- paste0("calc_paf_v", output.version, "_loc_", country)
  sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")
  args <- paste(country,
                squeeze.version,
                output.version,
                draws.required,
                cores.provided)

  system(paste(sys.sub, r.shell, conda.path, env.name, calc.script, args))

}
