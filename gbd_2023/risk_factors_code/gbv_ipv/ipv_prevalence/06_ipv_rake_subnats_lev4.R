######################################################################################################################################################################################################################################
#
# Purpose: Logit rake post-stgpr processed draws (subnat to nat); level 4
#
#####################################################################################################################################################################################################################################

#parameters that change annually: root and release_id
root <- 'FILEPATH'
release <- 16


# LBD fxns ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Set repo location and indicator group
core_repo          <- paste0('FILEPATH')
indic_repo         <- paste0('FILEPATH')
remote             <- 'origin'
branch             <- 'develop'
pullgit            <- FALSE

## sort some directory stuff
commondir      <- sprintf('FILEPATH')
package_list <- c(t(read.csv(sprintf('%s/package_list.csv',commondir),header=FALSE)))
package_list <- package_list[package_list!= "tictoc"]

# Load MBG packages and functions
lapply(package_list, library, character.only=TRUE)
setwd(paste0(core_repo, '/FILEPATH'))
for (ii in (list.files(pattern = "functions.R"))) {print(ii); source(ii)}

#custom load indicator-specific functions
source(paste0(indic_repo,'FILEPATH/misc_vaccine_functions.R'))
source('FILEPATH/helper_functions.R')
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/utility.r")
source("FILEPATH/get_population.R")
source(paste0(root, 'FILEPATH/logit_raking_function.R'))

#load packages
list.of.packages <- c("data.table","stringr","ggplot2","gridExtra","parallel", "boot", "openxlsx")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
lapply(list.of.packages, require, character.only = TRUE)

#load demographics
locs <- get_location_metadata(22, release_id = release)
ages <- get_age_metadata(24, release_id = release)
age21 <- data.table(age_group_id=21, age_group_name='80 plus')
ages <- rbind(ages, age21, fill=T)

#function to make draws long
draws_to_long <- function(dt, value_name = "value"){
  out <- melt.data.table(dt,
                         measure.vars = patterns("draw_"),
                         variable.name = "draw",
                         value.name = value_name)
  
  out[, draw := tstrsplit(draw, "_", keep = 2)]
  out[, draw := as.integer(draw)]
}

#create one large unraked file
read_draw <- function(c.file){
  print(c.file)
  fread(c.file) -> dat
  if ('V1' %in% colnames(dat)) {
    dat$V1 <- NULL
  }
  return(dat)
}

#raking settings:
zero_heuristic <- T
iterate <- T
approx_0_1 <- T
MaxJump = 11
MaxIter = 80
FunTol = 1e-5
if_no_gbd <- "return_na"

#arguments
if (interactive()) {
  
} else {
  run_id <- commandArgs(trailingOnly = T)[1]
  version <- commandArgs(trailingOnly = T)[2]
  l <- as.numeric(commandArgs(trailingOnly=T)[3])
}

id <- 2

#read in level 4 children for this parent id
file.list <- paste0(root, "FILEPATH/", run_id, '/', version, '/', c(l, as.character(unique(locs[level==4 & parent_id==l]$location_id))), '.csv')
file.list %>% lapply(read_draw) %>% rbindlist() -> unraked_data
print('data loaded')

#clean up
unraked_data[,c('version', 'upper', 'lower') := NULL]
if ('V1' %in% colnames(unraked_data)) {unraked_data$V1 <- NULL}
setnames(unraked_data, 'mean', 'value')
unraked_data[, sex_id:=id]
head(unraked_data)


#LBD METHOD --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#rake level 4 to 3 first (this script)

#set up inputs to function
lvl3 <- unraked_data[location_id==l]
lvl3 <- lvl3[, .(value = mean(value)), by=.(location_id, age_group_id, year_id, sex_id)]
lvl3[, sex_id:=NULL]
setcolorder(lvl3, neworder=c('location_id', 'year_id', 'age_group_id', 'value'))
colnames(lvl3) <- c("name", "year", "ages", "value")
lvl4 <- unraked_data[location_id!=l]
lvl4 <- dcast(lvl4, location_id+year_id+age_group_id~draw, value.var='value')
nyears <- length(unique(unraked_data$year_id))
year_list <- unique(unraked_data$year_id)

#load pops and merge onto lvl4
pops <- get_population(release_id = release, age_group_id = 'all', location_id = unique(unraked_data$location_id), sex_id = 2, year_id = year_list)
pops$run_id <- NULL
lvl4 <- merge(lvl4, pops, by=c('year_id', 'location_id', 'age_group_id'), all.x=T)

#get unique ages in data
age_group_ids <- unique(lvl4$age_group_id)
gbd_loc_id <- l

#rake function -------------------------------------------------------------------------------------------------------------------------------------------------------
raked <- gbd_raking_level4(rake_targets=lvl3, gbd_loc_id=l, cell_pred=lvl4, nyears=nyears, year_list=c(year_list), age_group_ids=age_group_ids)

#save by location
dir.create(paste0(root,"FILEPATH/",run_id,"/", version, "/"), recursive=T)
for(c.loc in unique(raked$location_id)){
  print(c.loc)
  write.csv(raked[location_id == c.loc], paste0(root,"FILEPATH/",run_id,"/", version, "/", c.loc,".csv") )
}


