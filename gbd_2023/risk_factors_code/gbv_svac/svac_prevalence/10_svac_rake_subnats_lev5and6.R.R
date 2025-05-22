######################################################################################################################################################################################################################################
#
# Author: USERNAME
# Purpose: Rake post-stgpr processed draws (subnat to nat) using LBD logit-raking function; level 5
#
#####################################################################################################################################################################################################################################

#directory
root <- 'FILEPATH'

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
message('Loading in required R packages and MBG functions')
lapply(package_list, library, character.only=TRUE)
setwd(paste0(core_repo, '/mbg_central'))
for (ii in (list.files(pattern = "functions.R"))) {print(ii); source(ii)}

# Custom load indicator-specific functions
source(paste0(indic_repo,'FILEPATH/misc_vaccine_functions.R'))
source('FILEPATH/helper_functions.R')
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/utility.r")
source("FILEPATH/get_population.R")
source(paste0(root, 'FILEPATH/logit_raking_function.R'))

list.of.packages <- c("data.table","stringr","ggplot2","gridExtra","parallel", "boot", "openxlsx")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
lapply(list.of.packages, require, character.only = TRUE)

#load demographics
locs <- get_location_metadata(22, release_id = 16)
ages <- get_age_metadata(24, release_id = 16)
age21 <- data.table(age_group_id=21, age_group_name='80 plus')
ages <- rbind(ages, age21, fill=T)

# function to make draws long
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
  if ('V1' %in% colnames(dat)) {dat$V1 <- NULL}
  return(dat)
}

#### raking settings:
zero_heuristic <- T
iterate <- T
approx_0_1 <- T
MaxJump = 11
MaxIter = 80
FunTol = 1e-5
if_no_gbd <- "return_na"

#Arguments
if (interactive()) {
  
} else {
  run_id <- commandArgs(trailingOnly = T)[1]
  version <- commandArgs(trailingOnly = T)[2]
  l <- as.numeric(commandArgs(trailingOnly=T)[3])
  risk <- commandArgs(trailingOnly = T)[4]
}

# Now start the script ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

id <- ifelse(risk=='fem_csa', 2, 1)

#read in children for this parent id
file.list <- paste0(root, "FILEPATH/", risk, '/', run_id, '/', version, '/', c(as.character(unique(locs[parent_id==l]$location_id))), '.csv')
file.list %>% lapply(read_draw) %>% rbindlist() -> unraked_data
print('data loaded')

#clean up
unraked_data[, c('version', 'upper', 'lower'):=NULL]
if ('V1' %in% colnames(unraked_data)) {unraked_data$V1 <- NULL}
setnames(unraked_data, 'mean', 'value')
unraked_data[, sex_id:=id]
head(unraked_data)

#LBD METHOD --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#rake level 5 and 6 after 4 is done

#set up inputs to function
lvl4 <- fread(paste0(root, "FILEPATH/", risk, '/', run_id, '/', version, '/', l, '.csv'))
lvl4 <- lvl4[, .(value = mean(raked_value)), by=.(location_id, age_group_id, year_id)]
setcolorder(lvl4, neworder=c('location_id', 'year_id', 'age_group_id', 'value'))
colnames(lvl4) <- c("name", "year", "ages", "value")
lvl5 <- unraked_data[location_id!=l]
lvl5 <- dcast(lvl5, location_id+year_id+age_group_id~draw, value.var='value')
nyears <- length(unique(unraked_data$year_id))
year_list <- unique(unraked_data$year_id)

#load pops and merge onto lvl4
pops <- get_population(release_id = 16, age_group_id = 'all', location_id = 'all', sex_id = id, year_id = 'all')
pops$run_id <- NULL
lvl5 <- merge(lvl5, pops, by=c('year_id', 'location_id', 'age_group_id'), all.x=T)

#get unique ages in data
age_group_ids <- unique(lvl5$age_group_id)
gbd_loc_id <- l

#rake function -------------------------------------------------------------------------------------------------------------------------------------------------------
raked <- gbd_raking_level4(rake_targets=lvl4, gbd_loc_id=l, cell_pred=lvl5, nyears=nyears, year_list=c(year_list), age_group_ids=age_group_ids)

#save by location
print('final dt created, beginning saving draws by subnat id')

dir.create(paste0(root,"FILEPATH/",risk, '/', run_id,"/", version, "/"), recursive=T)
for(c.loc in unique(raked$location_id)){
  print(c.loc)
  write.csv(raked[location_id == c.loc], paste0(root,"FILEPATH/", risk, '/', run_id,"/", version, "/", c.loc,".csv"), row.names = F)
}

print('saving raked draws completed')
