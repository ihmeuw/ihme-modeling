###27 Feb 2018
###Child script for split severity and subtract; these can be parallelized over cause and location; draws must then be stored to use in another script, because split_epi_model has to have access to all locations and output for all locations

#source("FILEPATH/pud_and_gastritis_adj_child.R")

# set up environment
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
} else {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
}

# source central functions
source(paste0(j, "FILEPATH/get_draws.R"))

# define objects

# objects for causes, locations, directories to send draws to
# passed from parent
cause<-commandArgs()[3]
location<-commandArgs()[4]
adj_child_files<-commandArgs()[5]

asymp_files<-commandArgs()[6]
mild_files<-commandArgs()[7]
mod_files<-commandArgs()[8]
adj_acute_files<-commandArgs()[9]
adj_complic_files<-commandArgs()[10]

## Decomp Step
step <- "step4"

## list of ME defs 
pud_total <- 24675
pud_acute <- 19692
pud_complic <- 19691 

pud_asymp <-  9314
mild_pud <- 20401
almod_pud <- 20402
mod_pud <- 20403

adj_acute_pud <- 20399
adj_complic_pud <- 20400

#
gastritis_total <- 24676
gastritis_acute <- 19696
gastritis_complic <- 19695

gastritis_asymp <-9528
mild_gastritis <- 20404
almod_gastritis <- 20405
mod_gastritis <- 20406

adj_acute_gastritis <- 20408
adj_complic_gastritis <- 20409

## objects for getting draws
age_groups <- c(2:20, 30:32, 235)
measure <- "5"

draws <- paste0("draw_", 0:999)

## severity splits from MEPS analysis
if(cause=="gastritis") {
  asymp_mean <- 0.320128838451268
  asymp_lower <- 0.31041388518024
  asymp_upper <- 0.32977303070761
  
  mild_mean <- 0.359060747663551
  mild_lower <- 0.287049399198932
  mild_upper <- 0.436598798397864
  
  almod_mean <- 0.32081041388518
  almod_lower <- 0.243658210947931
  almod_upper <- 0.395861148197597
}

if(cause=="pud") {
  asymp_mean <- 0.347690627843494
  asymp_lower <- 0.334849863512284
  asymp_upper <- 0.358507734303913
  
  mild_mean <- 0.279345768880801
  mild_lower <- 0.213808007279345
  mild_upper <- 0.349408553230209
  
  almod_mean <- 0.372963603275705
  almod_lower <- 0.303912647861692
  almod_upper <- 0.437693357597816
}

# Adjustment step from old modeling approach was dropped, but still need to copy to the downstream MEIDs
## get draws
total_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_total")), source="epi", location_id=location, measure_id=measure, age_group_id=age_groups, gbd_round_id=6, decomp_step=step)
unadj_acute_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_acute")), source="epi", location_id=location, measure_id=measure, age_group_id=age_groups, gbd_round_id=6, decomp_step=step)
unadj_complic_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_complic")), source="epi", location_id=location, measure_id=measure, age_group_id=age_groups, gbd_round_id=6, decomp_step=step)

## make copies of the draws
adj_acute_draws <- copy(unadj_acute_draws)
adj_complic_draws <- copy(unadj_complic_draws)

# split total by MEPS proportions

## calc SD from upper and lower limits
asymp_sd <- (asymp_upper-asymp_lower)/(2*1.96)
mild_sd <- (mild_upper-mild_lower)/(2*1.96)
almod_sd <- (almod_upper-almod_lower)/(2*1.96)

##generate vector of 1000 draws of distribution centered around proportion w/ standard errors (for all three proportions)
asymp_prop_draws <-rnorm(1000,asymp_mean, asymp_sd)
mild_prop_draws <-rnorm(1000,mild_mean, mild_sd)
almod_prop_draws <-rnorm(1000, almod_mean, almod_sd)
 
## copy total draws to new objects called asymp_draws, mild_draws, almod_draws
asymp_draws <- copy(total_draws)
mild_draws <- copy(total_draws)
almod_draws <- copy(total_draws)

## multiply the adjusted total draws by the proportion draws 
for(i in 0:999) {
  draw <- paste0("draw_", i)
  asymp_draws[, draw := asymp_draws[, draw,with=F]*asymp_prop_draws[i+1], with=F]
  mild_draws[, draw := mild_draws[, draw,with=F]*mild_prop_draws[i+1], with=F]
  almod_draws[, draw := almod_draws[, draw,with=F]*almod_prop_draws[i+1], with=F]
}

# subtract adj_complic and adj_acute draws off of the almod draws to get mod draws
mod_draws <- copy(almod_draws)

## this is syntax for a test to make sure the draws you manipulated from a copy still have same identifiers
# all(adj_acute_draws[,c("measure_id","location_id","year_id","age_group_id","sex_id"),with=F]==
#     mod_draws[,c("measure_id","location_id","year_id","age_group_id","sex_id"),with=F]
# )

mod_draws <- mod_draws[,draws,with=FALSE]-adj_acute_draws[,draws,with=FALSE]-adj_complic_draws[,draws,with=FALSE]
mod_draws[mod_draws<0] <- 0
## add columns with identifiers to draws just produced for moderate severity
mod_draws <- cbind(adj_acute_draws[,c("measure_id","location_id","year_id","age_group_id","sex_id"),with=F], mod_draws)

## drop model_version_id	and replace modelable_entity_id, which were originally copied from total_draws and don't apply to these sets
asymp_draws[ , model_version_id := NULL]
asymp_draws[ , modelable_entity_id := get(paste0(cause, "_asymp"))]

mild_draws[ , model_version_id := NULL]
mild_draws[ , modelable_entity_id := get(paste0("mild_", cause))]

mod_draws[ , model_version_id := NULL]
mod_draws[ , modelable_entity_id := get(paste0("mod_", cause))]

adj_acute_draws[ , model_version_id := NULL]
adj_acute_draws[ , modelable_entity_id := get(paste0("adj_acute_", cause))]

adj_complic_draws[ , model_version_id := NULL]
adj_complic_draws[ , modelable_entity_id := get(paste0("adj_complic_", cause))]

## write draws to files so anemia can use them, all locations for a given cause and child model need to go to a single, unique folder
write.csv(asymp_draws, file.path(asymp_files, paste0(measure,"_", location, ".csv")), row.names = F)
write.csv(mild_draws, file.path(mild_files, paste0(measure,"_", location, ".csv")), row.names = F)
write.csv(mod_draws, file.path(mod_files, paste0(measure,"_", location, ".csv")), row.names = F)
write.csv(adj_acute_draws, file.path(adj_acute_files, paste0(measure,"_", location, ".csv")), row.names = F)
write.csv(adj_complic_draws, file.path(adj_complic_files, paste0(measure,"_", location, ".csv")), row.names = F)
