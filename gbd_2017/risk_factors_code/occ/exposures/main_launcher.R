#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Launch the worker calculations or exposure calculations
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

# load packages
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr)
# library(DBI, lib.loc = file.path(h_root,"FILEPATH"))

#version history#
#workers
worker.version <- 3 #routed employment ratios into industry proportions
worker.version <- 4 #new run in updated parallel machinery
worker.version <- 5 #new run in updated functions & adding occupations
worker.version <- 6 
worker.version <- 7 #adding microdata and making some changes to allow for backcasting
worker.version <- 8 #adding new microdata and setting up extrapolation for ages 70-85
worker.version <- 9 #fixed the older age groups, was setting some to 0
worker.version <- 10 #rerunning with a fix for the missing industries <1980 (assuming 1980 value)
worker.version <- 11 #updated microdata
worker.version <- 12 #GBD 2017 review week
worker.version <- 13
worker.version <- 14 #GBD 2017 Final

#exposures
output.version <- 1 # GBD2015
output.version <- 2 #first run of GBD2016
output.version <- 3 #fixing some issues with carc files
output.version <- 4 #fixing some issues with ages
output.version <- 5 #updated industry proportions
output.version <- 6 #adding noise and particulates
output.version <- 7 #adding asthmagens and ergonomics
output.version <- 8 
output.version <- 9 #adding ETS to the carex regression/rerunning asthmagens problem with service
output.version <- 10 #rerunning asthmagens problem with key | rerunning carc problem with smoke/trichlo
output.version <- 11 #rerunning all carc files arent saving properly | rerunning injuries
output.version <- 12 #rerunning carc to test the high/low assumptions (using gbd2015 assumption now)
output.version <- 13 #rerunning carc with updates to CAREX reg and predictions
output.version <- 14 #more updates to the CAREX regression
output.version <- 15 #changes to the turnover calculation
output.version <- 16 #update of 15 after saving issues
output.version <- 17 #fixing some carc issues with age trend
output.version <- 18 #added fixes to CAREX, carcinogen turnover, back-casting for noise/copd, rerunning occ exps with worker v8, rerunning asb/inj with new codcorrect
output.version <- 19 #same changes as version 18 but the worker v8 had problems with the upper age groups (0s)/older years that are fixed
output.version <- 20 #updating asbestos high/low RR // missing some years in select countries on carc exposure
output.version <- 21 #trying to fix the missing years for carc issue // another update to asbestos high/low
output.version <- 22 #update to asbestos, calculating PAF
output.version <- 23 #rerunning with worker v11
output.version <- 24 #should match 23 but retrying because of failed locs in the industry exposures
#output.version <- 25 #adding interpolation for high/low exposure across SDI
#output.version <- 26 #matches version 25, but there were some issues with the new file saving for carcs
output.version <- 27 #post GBD2016, rerunning carcs to fix 80+ issue
#output.version <- 28 #27 same code but enforcing GBD round id == 4
output.version <- 28 #GBD 2017 first runs
output.version <- 29 #Fix to carcinogen exposures
output.version <- 30 ## GBD 2017 Final

#injury paf versions
inj.version <- 8 #GBD 2017 first runs
inj.version <- 9 #GBD 2017 Final

#scatters
scatter.version <- 1
scatter.version <- 2 #penultimate burdenator
scatter.version <- 3 #wills stuff
scatter.version <- 4 #ok actually penultimate 
scatter.version <- 5
scatter.version <- 6 #GBD 2017 first runs
scatter.version <- 7 #GBD 2017 Final

#set toggles
calc.workers <- F
workers.type <- 1 ## 0 for all workers, 1 for industry- and occupation-specific workers
calc.carc <- F
calc.ind <- F
calc.inj <- F
calc.occ <- F
calc.abs <- F
save <- F
save.paf <- T
scatter <- F
retry.toggle <- F
save.descrip <- "final_exposures_hopefully"
paf.save.descrip <- "fixed_empty_year_issue"

#set values for project
location_set_version_id <- 397
year_start <- 1970
year_end <- 2017
save.years <- c(seq(1990,2005,5),2007,2010,2015,2016,2017)
save.years <- seq(1990,2017)
scatter.years <- c(1990,2000,2010)
draws.required <- 1000

paf.scatter.name <- 'GBD17_Final'
paf.scatter.comp <- 0 # 0 if comparing to 2016 best, number if comparing against specific PAF version
nonmeso.version <- 345086
meso.version <- 345617
paf.scatter.iso <- T # set to False if want to omit ihme_loc_id from paf scatter


save.list <- c("occ_carcino_acid",
               "occ_carcino_asbestos",
               "occ_carcino_arsenic",
               "occ_carcino_benzene",
               "occ_carcino_beryllium",
               "occ_carcino_cadmium",
               "occ_carcino_chromium",
               "occ_carcino_diesel",
               "occ_carcino_formaldehyde",
               "occ_carcino_nickel",
               "occ_carcino_pah",
               "occ_carcino_silica",
               "occ_carcino_trichloroethylene",
               "occ_hearing",
               "occ_particulates",
               "occ_backpain",
               "occ_asthmagens")

save.paf.list <- c("occ_carcino_asbestos",
                   "occ_injuries")

paf.scatter.list <- c("occ_carcino_acid",
                      "occ_carcino_asbestos",
                      "occ_carcino_arsenic",
                      "occ_carcino_benzene",
                      "occ_carcino_beryllium",
                      "occ_carcino_cadmium",
                      "occ_carcino_chromium",
                      "occ_carcino_diesel",
                      "occ_carcino_formaldehyde",
                      "occ_carcino_nickel",
                      "occ_carcino_pah",
                      "occ_carcino_silica",
                      "occ_carcino_trichloroethylene",
                      "occ_hearing",
                      "occ_particulates",
                      "occ_backpain",
                      "occ_asthmagens",
                      'occ_injuries')

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

###in/out###
##in##
#dirs#
code.dir <- file.path(j_root, 'FILEPATH')
me.dir <- file.path(home.dir, "FILEPATH")
  me.names <- file.path(me.dir, 'modelable_entity_names.csv') %>% fread

#scripts#
worker.script <- file.path(code.dir, "4_workers.R")
carc.script <- file.path(code.dir, "5_carc_exposure.R")
ind.script <- file.path(code.dir, "5_ind_exposure.R")
inj.script <- file.path(code.dir, "5_inj_exposure.R")
occ.script <- file.path(code.dir, "5_occ_exposure.R")
abs.script <- file.path(code.dir, "6_asbestos_exposure.R")
scatter.script <- file.path(code.dir,"paf_scatter_wrapper.R")

#shells/envs#
r.shell <- "FILEPATH"
stata.shell <- paste0("FILEPATH -q do ")

##out##
#note except for save dir, these are just used to verify that files have been saved, else relaunch
#hence they use the last step in each process
draw.dir <- file.path('FILEPATH')
abs.dir <- file.path(draw.dir, 'FILEPATH', output.version)
carc.dir <- file.path(draw.dir, 'FILEPATH', output.version)
ind.dir <- file.path(draw.dir, 'FILEPATH', output.version)
inj.dir <-  file.path(draw.dir, 'FILEPATH', output.version)
occ.dir <- file.path(draw.dir, 'FILEPATH', output.version)
worker.dir <- file.path(draw.dir, 'FILEPATH', worker.version)
  lapply(c(abs.dir, ind.dir, draw.dir, inj.dir, occ.dir, worker.dir), dir.create, recursive=T)
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source
# central functions
file.path(j_root, 'FILEPATH') %>% source
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

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

  #having issues with memory leak on ind, boost the mem requirements and decrease slots until you can solve
  if (name %like% 'carc') mem <- cores * 25

  #add the looping country as 1st arg and the cores as last arg
  out.args <- paste(country,
                    args,
                    cores)

  # Create submission call
  sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")


  if(retry==TRUE) { #add capacity for second run if memory or cluster issues are causing failure

    if (length(grep(paste0("^", country, out.file), #carot adds an start anchor to verify that the country ID matches exactly
                    list.files(path=out.dir, full.names=F)))==0) {

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

#create PAF scatters
occGraph <- function(entity, filename, version, add_isos, cores=5) {

  # Prepare job settings
  slots <- cores
  mem <- cores * 4

  ## determine rei_id
  rei <- me.names[risk %like% entity, unique(rei_id)]

  for (year in scatter.years){
    for (measure in c(3,4)){

      measure_name <- ifelse(measure == 3,"yld","yll")
      jname <- paste0(gsub("occ_","",entity),"_",measure_name,"_",year,"_scatter")
      file_name <- paste0(gsub("occ_","",entity),"_",measure_name,"_",year,"_",filename,".pdf")

      # Create submission call
      sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")

      job <- paste0(sys.sub,
                    " FILEPATH ", scatter.script,
                    " --args ", rei, " ", measure, " ", year, " ", file_name, " ", version, " ", add_isos)

      out <- paste0('launching scatter for ...', jname)

      system(job)
      print(out)
      print(job)
    }
  }
}

occSave <- function(entity,
                    this.element,
                    best=FALSE,
                    save_results_description="blank") {

  cores.provided <- 10
  slots <- cores.provided * 2
  mem <- cores.provided * 4

  message("launching exposure save for ", entity, "\n --using ", slots, " slots and ", mem, "GB of mem")

  dt <- me.names[risk %like% entity & element==this.element]

  element <- this.element
  types <- dt[, unique(group)]
  for (type in types) {

    # Launch jobs
    jname <- paste0("save_", entity, "_", type)
    if (element == "paf") jname <- paste0("save_", entity, "_", element)
    sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")

    me_id <- dt[group==type, modelable_entity_id]
    parent_folder <-dt[group==type, risk]
    results.dir <- file.path(draw.dir, element, parent_folder, type, output.version)
    if (entity == "occ_injuries") results.dir <- file.path(draw.dir, element, parent_folder, type, inj.version)

    if (element == "exp"){
      job <- paste0(sys.sub,
                    " FILEPATH FILEPATH",
                    " --args",
                    " --type epi",
                    " --year_ids ", paste(save.years,collapse=","),
                    " --me_id ", me_id,
                    " --input_directory ", results.dir,
                    " --descript ", save_results_description,
                    " --best ", best,
                    " --measure_epi 18")
    } else {
      job <- paste0(sys.sub,
                    " FILEPATH FILEPATH",
                    " --args",
                    " --type risk",
                    " --id_type paf",
                    " --file_pattern paf_{location_id}_{year_id}.csv",
                    " --year_ids ", paste(save.years,collapse=","),
                    " --me_id ", me_id,
                    " --input_directory ", results.dir,
                    " --descript ", save_results_description,
                    " --best ", best)
    }

    system(job)

  }

  return(paste0(entity, '=saving..'))

}

#***********************************************************************************************************************

#----LAUNCH CALC--------------------------------------------------------------------------------------------------------
# Get the list of most detailed GBD locations (if necessary)
if (any(c(calc.workers,calc.abs,calc.carc,calc.ind,calc.inj,calc.occ))){
  source(file.path(j_root,"FILEPATH"))
  locs <- get_location_metadata(gbd_round_id = 5, location_set_id = 22)
  locations <- unique(locs[is_estimate==1, location_id]) %>% sort
}

if (calc.workers == TRUE) {

  status <- lapply(locations,
                   launchCountry,
                   name="workers",
                   script=worker.script,
                   args=paste(worker.version, draws.required, workers.type),
                   retry=retry.toggle,
                   out.dir=worker.dir,
                   out.file="occ_workers.fst",
                   cores=5)

  retry.locs <- locations[!lapply(status, function(x) grepl('is finished', x)) %>% unlist]

  if (length(retry.locs)!=0) {
    message('retrying for :', retry.locs)
  } else message('workers = all done!')

} else if (calc.abs==TRUE) {

  status <- lapply(locations,
                   launchCountry,
                   name="abs_exp",
                   script=abs.script,
                   args=paste(output.version, draws.required),
                   retry=retry.toggle,
                   out.dir=abs.dir,
                   out.file=".csv",
                   cores=5)

  retry.locs <- locations[!lapply(status, function(x) grepl('is finished', x)) %>% unlist]

  if (length(retry.locs)!=0) {
    message('retrying for: ', paste0(retry.locs, "|"))
  } else message('carc exposure = all done!')

} else if (calc.carc==TRUE) {

  status <- lapply(locations,
                   launchCountry,
                   name="carc_exp",
                   script=carc.script,
                   args=paste(worker.version, output.version, draws.required),
                   retry=retry.toggle,
                   out.dir=carc.dir,
                   out.file=".csv",
                   cores=10)

  retry.locs <- locations[!lapply(status, function(x) grepl('is finished', x)) %>% unlist]

  if (length(retry.locs)!=0) {
    message('retrying for: ', paste0(retry.locs, "|"))
  } else message('carc exposure = all done!')

} else if (calc.ind==TRUE) {

  status <- lapply(locations,
                   launchCountry,
                   name="ind_exp",
                   script=ind.script,
                   args=paste(worker.version, output.version, draws.required),
                   retry=retry.toggle,
                   out.dir=ind.dir,
                   out.file=".csv",
                   cores=5)

  retry.locs <- locations[!lapply(status, function(x) grepl('is finished', x)) %>% unlist]

  if (length(retry.locs)!=0) {
    message('retrying for: ', paste0(retry.locs, "|"))
  } else message('carc exposure = all done!')

} else if (calc.inj==TRUE) {

  status <- lapply(locations,
                   launchCountry,
                   name="inj_exp",
                   script=inj.script,
                   args=paste(worker.version, output.version, draws.required),
                   retry=retry.toggle,
                   out.dir=inj.dir,
                   out.file="_inj.fst",
                   cores=5)

  retry.locs <- locations[!lapply(status, function(x) grepl('is finished', x)) %>% unlist]

  if (length(retry.locs)!=0) {
    message('retrying for: ', paste0(retry.locs, "|"))
  } else message('inj exposure = all done!')

} else if (calc.occ==TRUE) {

  status <- lapply(locations,
                   launchCountry,
                   name="occ_exp",
                   script=occ.script,
                   args=paste(worker.version, output.version, draws.required),
                   retry=retry.toggle,
                   out.dir=occ.dir,
                   out.file=".csv",
                   cores=5)

  retry.locs <- locations[!lapply(status, function(x) grepl('is finished', x)) %>% unlist]

  if (length(retry.locs)!=0) {
    message('retrying for: ', paste0(retry.locs, "|"))
  } else message('occ exposure = all done!')

} else if (save==TRUE) {

  lapply(save.list, occSave, best=TRUE, this.element="exp",
         save_results_description=save.descrip)

} else if (save.paf==TRUE) {

  lapply(save.paf.list, occSave, best=TRUE, this.element="paf",
         save_results_description=paf.save.descrip)

} else if (scatter==TRUE) {

  lapply(paf.scatter.list[!grepl("asbestos",paf.scatter.list)], occGraph,
         filename=paf.scatter.name, version = paf.scatter.comp, add_isos = paf.scatter.iso)

  if (any(grepl("asbestos",paf.scatter.list))) {
    lapply("occ_carcino_asbestos", occGraph, filename=paf.scatter.name, version = nonmeso.version,
           add_isos = paf.scatter.iso)

    lapply("occ_carcino_asbestos", occGraph, filename=paste0("meso_",paf.scatter.name), version = meso.version,
           add_isos = paf.scatter.iso)
  }
}

#********************************************************************************************************************************
