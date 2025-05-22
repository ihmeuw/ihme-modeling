# STGPR China-specific script, adjust China mortality estimates to match a trusted source
## ---------------------------

## Used in basically every script
Sys.umask(mode = "0002")
windows <- Sys.info()[1][["sysname"]]=="Windows"
user <- 'USER'
lib.loc <- 'LIB_LOC'
.libPaths(c(lib.loc,.libPaths()))
packages <- c('fastmatch')
for(p in packages){
if(p %in% rownames(installed.packages())==FALSE){
install.packages(p)
}
library(p, character.only = T)
}

library(data.table)

code_dir <- 'FILEPATH'

args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
  run.name <- args[1]
} else {
  run.name = 'RUN.NAME'
}


locmap <- fread('FILEPATH/get_locations_2020.csv')
locmap <- locmap[,.(location_id, location_name, ihme_loc_id)]
locmap2 <- copy(locmap)
setnames(locmap2, 'location_id', 'parent_id')
setnames(locmap2, 'ihme_loc_id', 'parent_loc_id')

locs <- locmap

lowest <- fread(paste0('FILEPATH', run.name, '/get_locations_2020_lowest.csv'))
lowest <- lowest[level >= 4,]
lowest <- lowest[level != 4 & ihme_loc_id != 'CHN',]
lowest[,parent_loc_id := unlist(lapply(strsplit(lowest$ihme_loc_id, split = '_'), function(x) x[1]))]


subnat_locs <- lowest[parent_loc_id == 'CHN', parent_loc_id := 'CHN_44533']
subnat_locs[,parent_id := NULL]
subnat_locs <- merge(subnat_locs, locmap2, by = 'parent_loc_id')
subnat_locs <- subnat_locs[,.(ihme_loc_id,parent_loc_id, parent_id)]

parent_locs <- subnat_locs[,parent_loc_id := NULL]
setnames(parent_locs, 'ihme_loc_id', 'parent_loc_id')

st_locs <- fread('FILEPATH/gen_countries_final.csv')
setnames(st_locs, "AGO", "ihme_loc_id")
st_locs <- rbind(st_locs, data.table(ihme_loc_id="AGO"))
st_locs <- st_locs[!grepl('IND',ihme_loc_id)]
st_locs <- merge(st_locs, locs)

#######
#start of the use_scaled_chn == 1
#######

cod_data <- fread(paste0('FILEPATH', run.name, '/cod_data_2020.csv'))
cod_data[,V1 := NULL]
master <- merge(cod_data, locs, by = 'location_id')

chn_cod <- master
chn_cod <- chn_cod[!ihme_loc_id %in% c("CHN_354","CHN_361","CHN_44533"),] # remove Hong Kong, Macao, China minus Hong Kong + Macao 
chn_cod <- chn_cod[grepl('CHN',ihme_loc_id),] 
write.csv(chn_cod, paste0("FILEPATH/", run.name, "/cod_chn.csv"))

infect <- fread("FILEPATH/prepped_hiv_data.csv")
setnames(infect, "year", "year_id", skip_absent = T)
infect[,infect_deaths := hiv_deaths + aids_deaths]
infect[,c('hiv_deaths', 'aids_deaths'):=NULL]


chn_cod <- chn_cod[cod_source_label == 'China_2004_2012',]
write.csv(chn_cod, paste0('FILEPATH', run.name, '/deaths_test_new.csv'))

chn_cod <- chn_cod[!age_group_id %in% c(22,27),] # remove all-ages from China + China subnats 
chn_cod[,deaths := sum(deaths), by = c('year_id', 'location_name')]
chn_cod <- chn_cod[year_id > 2012,] #subset by only >2012 years, but only includes 2013-2017
setnames(chn_cod, 'deaths', 'vr_deaths')


chn_cod <- fread(paste0('FILEPATH', run.name, '/cod_chn.csv'))
chn_cod <- chn_cod[cod_source_label != 'China_Infectious',]
chn_scalars <- fread(paste0("FILEPATH", run.name, "/GBD2023_scalar.csv"))
setnames(chn_scalars, "scalar_gbd2023", "scalar")
chn_scalars[scalar < 1, scalar := 1]
chn_scalars[,scalar := max(scalar), by = 'location_name']
if(any(colnames(chn_cod) == 'V1')){
  chn_cod[,V1 := NULL]
}
chn_cod <- merge(unique(chn_scalars[location_id %in% chn_cod[,location_id],.(location_id, scalar)]), chn_cod, by = 'location_id')
chn_cod[,deaths := deaths * scalar]
chn_cod[,sample_size := sample_size * scalar]
write.csv(chn_cod, paste0('FILEPATH', run.name, '/test_china_scalars.csv'))
chn_cod[,scalar := NULL]
chn_cod[,c('nid','cf','rate'):= '.']

master <- master[!ihme_loc_id %in% c("CHN_354","CHN_361"),]
master <- master[!ihme_loc_id %like% 'CHN',]
cod_data <- rbind(chn_cod, master, fill=T)


##drop VR sources that we can't use
cod_data[,exception := 0]
cod_data[grepl('IND',ihme_loc_id), exception := 1]
cod_data[,exception := NULL]

##no HIV deaths before 1981
cod_data <- cod_data[year_id >= 1981,]

write.csv(cod_data, paste0('FILEPATH', run.name, '/cod_data_2020.csv'))

