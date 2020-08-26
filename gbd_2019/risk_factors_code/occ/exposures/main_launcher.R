#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Launch the parallelized calculation of occ exposures
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

project <- " -P proj_erf "
sge.output.dir <- "-o FILEPATH -e FILEPATH "

# load packages
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr)

#version history#
#workers
worker.version <- 20 # GBD 2019 final

#exposures
output.version <- 41 # GBD 2019 final

# injury paf versions
inj.version <- 16 # GBD 2019 final

#scatters
scatter.version <- 12 # GBD 2019 final

#set toggles
calc.workers <- F
workers.type <- 0 ## 0 for all workers, 1 for industry- and occupation-specific workers
calc.carc <- F
calc.ind <- F
calc.inj <- F
calc.occ <- F
calc.abs <- F
save.exp <- T
save.paf <- F
scatter <- F
retry.toggle <- F
save.descrip <- "resubmission, all years, codcorrect v101"
save.descrip <- gsub(" ", "_", save.descrip) # needs to be underscore-delimited or it will break
paf.save.descrip <- "resubmission, all years"
paf.save.descrip <- gsub(" ", "_", paf.save.descrip) # ditto

#set values for project
location_set_version_id <- 443
year_start <- 1970
year_end <- 2019
save.years <- 1990:2019
scatter.years <- 2017
draws.required <- 1000
decomp_step <- "step4"

paf.scatter.name <- "GBD19step3"
paf.scatter.comp <- 0 # 0 if comparing to 2017 best, number if comparing against specific PAF version
nonmeso.version <- 345086
meso.version <- 345617
paf.scatter.iso <- F # set to False if want to omit ihme_loc_id from paf scatter


save.list <- c("occ_carcino_acid",
               "occ_carcino_arsenic",
               "occ_carcino_asbestos",
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
                      "occ_carcino_arsenic",
                      "occ_carcino_asbestos",
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
                      "occ_injuries")


# set working directories
home.dir <- "FILEPATH"
setwd(home.dir)

###in/out###
code.dir <- "FILEPATH"
me.dir <- "FILEPATH"
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
r.shell <- "SHELL"

##out##
draw.dir <- "FILEPATH"
abs.dir <- file.path(draw.dir, "FILEPATH", output.version)
carc.dir <- file.path(draw.dir, "FILEPATH", output.version)
ind.dir <- file.path(draw.dir, "FILEPATH", output.version)
inj.dir <-  file.path(draw.dir, "FILEPATH", output.version)
occ.dir <- file.path(draw.dir, "FILEPATH", output.version)
worker.dir <- file.path(draw.dir, "FILEPATH", worker.version)
lapply(c(abs.dir, ind.dir, draw.dir, inj.dir, occ.dir, worker.dir), dir.create, recursive=T)
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "FUNCTION") %>% source
# central functions
file.path("FUNCTION") %>% source
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
  threads <- cores
  mem <- cores * 4
  jname <- paste0(name, "_loc_", country)
  
  # workers needs ~3G mem, requesting 5G to be safe
  if (name %like% 'workers') mem <- cores
    
  # inj needs ~4G mem, requesting 7.5G to be safe
  if (name %like% 'inj') mem <- cores * 1.5

  # asb needs <1 G mem, requesting 2.5G to be safe
  if (name %like% 'abs') mem <- cores / 2

  # occ needs ~2.5G mem, requesting 5G to be safe
  if (name %like% 'occ') mem <- cores

  # ind needs ~5G mem, requesting 10G to be safe
  if (name %like% 'ind') mem <- cores * 2

  # carc needs more mem (65-75G - requesting 80G to be safe) otherwise the cluster will break
  if (name %like% 'carc') mem <- cores * 16

  #add the looping country as 1st arg and the cores as last arg
  out.args <- paste(country,
                    args,
                    cores)

  # Create submission call
  sys.sub <- paste0("qsub -l m_mem_free=", mem, "G -l fthread=", threads, project, "-q long.q -l h_rt=2:00:00 -l archive=TRUE ", sge.output.dir, "-N ", jname)


  if(retry==TRUE) { #add capacity for second run if memory or cluster issues are causing failure

    if (length(grep(paste0("^", country, out.file), #caret adds an start anchor to verify that the country ID matches exactly
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
  threads <- cores
  mem <- cores

  ## determine rei_id
  rei <- me.names[risk %like% entity, unique(rei_id)]

  for (year in scatter.years){
    for (measure in c(3,4)){

      measure_name <- ifelse(measure == 3,"yld","yll")
      jname <- paste0(gsub("occ_","",entity),"_",measure_name,"_",year,"_scatter")
      file_name <- paste0(gsub("occ_","",entity),"_",measure_name,"_",year,"_",filename,".pdf")

      # Create submission call
      sys.sub <- paste0("qsub -l m_mem_free=", mem, "G -l fthread=", threads, project, "-q all.q -l h_rt=00:20:00 -l archive=TRUE ", sge.output.dir, "-N ", jname)

      job <- paste0(sys.sub,
                    " ", r.shell, " ", scatter.script, " ",
                    rei, " ", measure, " ", year, " ", file_name, " ", version, " ", add_isos, " ", decomp_step)

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

  threads <- 20
  mem <- 80

  message("launching ", this.element, " save for ", entity, "\n --using ", threads, " threads and ", mem, "GB of mem")

  dt <- me.names[risk %like% entity & element==this.element]

  element <- this.element
  types <- dt[, unique(group)]
  #types <- "low" #toggle for change to specific types
  for (type in types) {

    # Launch jobs
    jname <- paste0("save_", entity, "_", type)
    if (element == "paf") jname <- paste0("save_", entity, "_", element)
    sys.sub <- paste0("qsub -l m_mem_free=", mem, "G -l fthread=", threads, project, "-q long.q -l h_rt=168:00:00 -l archive=TRUE ", sge.output.dir, "-N ", jname)

    me_id <- dt[group==type, modelable_entity_id]
    parent_folder <- dt[group==type, risk]
    results.dir <- file.path(draw.dir, element, parent_folder, type, output.version)
    if (entity == "occ_injuries") results.dir <- file.path(draw.dir, element, parent_folder, type, inj.version)
    print(results.dir)

    if (element == "exp") {
      job <- paste0(sys.sub,
                    " SHELL FUNCTION",
                    " --args",
                    " --type epi",
                    " --year_ids ", paste(save.years,collapse=","),
                    " --me_id ", me_id,
                    " --input_directory ", results.dir,
                    " --draws ", draws.required,
                    " --descript ", save_results_description,
                    " --best ", best,
                    " --decomp_step ", decomp_step,
                    " --measure_epi 18")
    } else if (element == "paf") {
        if (entity == "occ_carcino_asbestos") {
          job <- paste0(sys.sub,
                        " SHELL FUNCTION",
                        " --args",
                        " --type risk",
                        " --id_type paf",
                        " --file_pattern paf_{location_id}_{year_id}.csv",
                        " --year_ids ", paste(save.years,collapse=","),
                        " --me_id ", me_id,
                        " --input_directory ", results.dir,
                        " --draws ", draws.required,
                        " --descript ", save_results_description,
                        " --decomp_step ", decomp_step,
                        " --best ", best)
        } else {
          job <- paste0(sys.sub,
                        " SHELL FUNCTION",
                        " --args",
                        " --type risk",
                        " --id_type paf",
                        " --file_pattern paf_{measure}_{location_id}_{year_id}_{sex_id}.csv",
                        " --year_ids ", paste(save.years,collapse=","),
                        " --me_id ", me_id,
                        " --input_directory ", results.dir,
                        " --draws ", draws.required,
                        " --descript ", save_results_description,
                        " --decomp_step ", decomp_step,
                        " --best ", best)
        }

    }

    system(job)
    print(job)

  }

  return(paste0(entity, '=saving...'))

}

#***********************************************************************************************************************

#----LAUNCH CALC--------------------------------------------------------------------------------------------------------
# Get the list of most detailed GBD locations (if necessary)
if (any(c(calc.workers,calc.abs,calc.carc,calc.ind,calc.inj,calc.occ))){
  source(file.path("FUNCTION"))
  locs <- get_location_metadata(gbd_round_id = 6, location_set_id = 22)
  locations <- unique(locs[is_estimate==1, location_id]) %>% sort
}

if (calc.workers == TRUE) {
  outfile <- ifelse(workers.type == 0,"_emp_workers.fst","_occ_workers.fst")
  status <- lapply(locations,
                   launchCountry,
                   name="workers",
                   script=worker.script,
                   args=paste(worker.version, draws.required, workers.type, decomp_step),
                   retry=retry.toggle,
                   out.dir=worker.dir,
                   out.file=outfile,
                   cores=5)
  
  retry.locs <- locations[!lapply(status, function(x) grepl('is finished', x)) %>% unlist]
  
  if (length(retry.locs)!=0) {
    message('retrying for: ', retry.locs)
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
  } else message('asb exposure = all done!')

} else if (calc.carc==TRUE) {

  status <- lapply(locations,
                   launchCountry,
                   name="carc_exp",
                   script=carc.script,
                   args=paste(worker.version, output.version, draws.required),
                   retry=retry.toggle,
                   out.dir=carc.dir,
                   out.file=".csv",
                   cores=5)

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
  } else message('ind exposure = all done!')

} else if (calc.inj==TRUE) {

  status <- lapply(locations,
                   launchCountry,
                   name="inj_exp",
                   script=inj.script,
                   args=paste(worker.version, output.version, draws.required, decomp_step),
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

} else if (save.exp==TRUE) {

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
