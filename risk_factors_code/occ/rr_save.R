#----HEADER-------------------------------------------------------------------------------------------------------------

# Date: 04/05/2017
# Purpose: Update relative risks for OCC

#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
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
  project <- "" # 
  sge.output.dir <- "-o FILEPATH -e FILEPATH "
  #sge.output.dir <- "" # toggle to run with no output files
  
}

# load packages
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, readxl)


# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

#version history#
#exposures
output.version <- 1 # GBD2015
output.version <- 2 #first run of GBD2016
output.version <- 3 #updating age groups for silica/adding single RR to asbestos
output.version <- 4 #removing meso RR form asbestos
output.version <- 5 #adding low RRs for arsenic, acid, benzene, nickel

#set toggles
calc.rr <- T
save <- T

#set values for project
location_set_version_id <- 149
year_start <- 1970
year_end <- 2016
all.ages <- c(2:20, 30, 31, 32, 235)
relevant.ages <- all.ages[all.ages >= 8] #only ages 15+
draws.required <- 1000
rr.cols <- paste0("rr_", 0:(draws.required-1))

save.list <- c("occ_carcino_acid",
               "occ_carcino_asbestos",
               "occ_carcino_arsenic",
               "occ_carcino_benzene",
               "occ_carcino_beryllium",
               "occ_carcino_cadmium",
               "occ_carcino_chromium",
               "occ_carcino_diesel",
               "occ_carcino_smoke",
               "occ_carcino_formaldehyde",
               "occ_carcino_nickel",
               "occ_carcino_pah",
               "occ_carcino_silica",
               "occ_carcino_trichloroethylene",
               "occ_hearing",
               "occ_particulates",
               "occ_backpain",
               "occ_asthmagens")

###in/out###
##in##
#dirs#
code.dir <- file.path(h_root, 'FILEPATH')
data.dir <-  file.path(home.dir, "FILEPATH")
rr.dt <- file.path(data.dir, 'gbd_2016_rr_update.xlsx') %>% read_xlsx %>% as.data.table
me.dir <- file.path(home.dir, "FILEPATH")
me.names <- file.path(me.dir, 'modelable_entity_names.csv') %>% fread

#scripts#
worker.script <- file.path(code.dir, "4_workers.R")

#shells/envs#
conda.path <- "FILEPATH"
env.name <- "R_CONDA"
r.shell <- file.path(h_root, "FILEPATH")
stata.shell <- paste0("FILEPATH -q do ")

##out##
#note except for save dir, these are just used to verify that files have been saved, else relaunch
#hence they use the last step in each process
draw.dir <- file.path('FILEPATH')
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db (personal version)
file.path(central.function.dir, "db_tools.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source
# central functions
file.path(j_root, 'FILEPATH') %>% source
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

calcRR <- function(entity,
                   mort.option=NA,
                   morb.option=NA,
                   upload=FALSE,
                   best=FALSE,
                   save_results_description="blank") {
  
  #subset the me.dt/rr.dt based on the risk you are looking at
  this.me <- me.names[risk %like% entity]
  
  this.rr <- rr.dt[rei %like% entity]
  types <- this.rr[, unique(cat_label)]
  entity.id <- this.rr$me_id %>% unique
  
  #create a data table with all the required information
  #note other variables relative to this job
  out.rr <- data.table(iso3 = "G", location_id = 1, risk = entity)
  
  #specify the current
  out.rr[, cause_id := this.rr$cause_id %>% unique]
  
  #specify mortality/morbidity as needed
  out.rr[, mortality := mort.option %>% as.numeric]
  out.rr[, morbidity := morb.option %>% as.numeric]
  
  catLoop <- function(this.cat, dt) {
    
    out <- copy(dt)
    
    if (this.rr[cat_label==this.cat, ci_type]==.95) {
      
      z.val <- 1.96 * 2
      
    } else if (this.rr[cat_label==this.cat, ci_type]==.99) {
      
      z.val <- 2.58 * 2
      
    }
    
    #generate draws based on reported values
    #the distribution is lognormal for the asbestos+meso / acid+larynx RR
    if (entity != "occ_carcino_asbestos" & entity != "occ_carcino_acid" & entity != "occ_carcino_nickel") {
      
      out[, (rr.cols) := rnorm(draws.required,
                               mean=this.rr[cat_label==this.cat, mean],
                               sd=(this.rr[cat_label==this.cat, upper] - this.rr[cat_label==this.cat, lower]) / z.val)
          %>% as.list]
      
    } else {
      
      out[, (rr.cols) := rnorm(draws.required,
                               mean=this.rr[cat_label==this.cat, mean] %>% log,
                               sd=(this.rr[cat_label==this.cat, upper] %>% log -
                                     this.rr[cat_label==this.cat, lower] %>% log) / z.val) %>% exp
          %>% as.list]
      
    }
    
    #specify the correct category using parameter variable
    out[, parameter := this.rr[cat_label==this.cat, cat]]
    
    #calculate mean/CI to verify
    out[, mean := apply(.SD, 1, mean), .SDcols = rr.cols]
    out[, lower := apply(.SD, 1, quantile, c(.025)), .SDcols = rr.cols]
    out[, upper := apply(.SD, 1, quantile, c(.975)), .SDcols = rr.cols]
    
    return(out)
    
  }
  
  out.rr <- lapply(types, catLoop, dt=out.rr) %>% rbindlist
  
  #duplicate for all relevant age groups
  dupeVar <- function(value, var, dt) {
    
    message('duplicating for ', var, '=', value)
    out <- copy(dt)
    out[, (var) := value]
    return(out)
    
  }
  
  out.rr <- lapply(relevant.ages, dupeVar, var="age_group_id", dt=out.rr) %>% rbindlist
  out.rr[, modelable_entity_id := entity.id]
  
  #specify save location
  parent_folder <- this.me$risk %>% unique
  
  out.dir <- file.path(draw.dir, parent_folder, output.version)
  dir.create(out.dir, recursive = T)
  
  
  #save one file for each sex/year
  writeFiles <- function(this.sex, this.year, dt) {
    
    out <- copy(dt)
    
    out[, sex_id := this.sex]
    out[, year_id := this.year]
    
    write.csv(out, paste0(out.dir, "/rr_1_", this.year, "_", this.sex, ".csv"))
    
  }
  
  files <- expand.grid(sex=c(1,2), year=c(1990, 1995, 2000, 2005, 2010, 2013, 2015, 2016))
  mapply(writeFiles, this.sex=files$sex, this.year=files$year, MoreArgs = list(dt=out.rr))
  
  message("RRs drawn and written to csv in ", out.dir)
  
  if (upload==TRUE) {
    
    #update to submit the stata job (save_results for RR is only in stata) using the entity id and filepath
    
  }
  
  return(paste0(entity, '=saving..'))
  
}
#***********************************************************************************************************************

#----RUN----------------------------------------------------------------------------------------------------------------
#use your function to calculate draws of the RR, save csvs, and then run save results to upload
calcRR("occ_carcino_nickel", mort.option = T, morb.option = T, upload=F, best=F, save_results_description = "adding low RRs")
#***********************************************************************************************************************

#----ASBESTOS-----------------------------------------------------------------------------------------------------------
best.id <- 102193
best.dir <- file.path('FILEPATH', best.id ,'draws')
files <- list.files(best.dir)

removeCat <- function(file) {
  
  message('working on ', file)
  
  dt <- fread(file.path(best.dir, file))
  
  dt <- dt[parameter!="cat2"] #remove the LOW exposure category
  dt[parameter=="cat3", parameter := "cat2"] #reset the TMREL to the second category
  dt <- dt[cause_id != 483] #remove the mesothelioma RR (using a different approach now)
  
  out.dir <- file.path(draw.dir, 'occ_carcino', 'occ_carcino_asbestos', output.version)
  dir.create(out.dir, recursive = T)
  
  write.csv(dt, file.path(out.dir, file))
  
}

#lapply(files, removeCat)
