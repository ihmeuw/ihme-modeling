# create cw indicators for locations that LBD doesn't have (e.g. europe)
# This file is used to run write_cw_prep.R

# CONFIG #######################################################################

#Clean out list
rm(list=ls())

library(pbapply)

## prep extractions for write_cw
source("FILEPATHt/write_cw_prep.R")

# locs
source("FILEPATH/get_location_metadata.R")
locs<-get_location_metadata(location_set_id = 22, release_id = 9)

# List of unique surveys #####################################################
## ADD NEW DIRECTORIES HERE ###############

# FIRST GO TO WRITE_CW_PREP.R AND ADD ANY NEW DIRECTORIES BEFORE RUNNING THIS. ADD THE NEW DIRECTORIES TO THE LIST BELOW

#List the directories
# MAKE SURE TO ADD THE NEWEST DIRECTORIES TO THE BEGINNING OF THE LIST
#   If you don't do that, then it will not keep the newest survey extraction (which is the one that we want to keep)
dirs<-c(dir_22b,dir_22a,dir_21,dir_20,dir_19, # non-limited surveys
        l.dir_22b,l.dir_22a,l.dir_21,l.dir_20) # limited use surveys

#Make empty lists for the for-loop
files_check<-list()
final_files<-list()

# For loop to determine unique surveys and make a list of all the 
for (n in 1:length(dirs)) {
  
  #list the files names in the dir
  filepaths<-list.files(dirs[[n]])
  
  #Determine which ones are already on the list
  exclude<-intersect(files_check,filepaths)
  
  #exclude the files that are dups
  filepaths_nondup<-filepaths[!(filepaths %in% exclude)]
  
  #Add the non-dup files to the list of files
  files_check<-append(files_check,filepaths_nondup)
  
  #load in the full filepaths
  full_filepaths<-list.files(dirs[[n]], full.names = T)
  
  #Now pull in the full filepaths of the non-dup files
  full_filepaths<-full_filepaths[full_filepaths %like% paste(filepaths_nondup, collapse = "|")]
  
  #Add in the full filepath to the list of files to load (this should only keep the newest file, and not load in dups in older folders)
  final_files<-append(final_files,full_filepaths)
  
  print(paste0("Finished ",dirs[[n]]))
  
}


#exclude the files
final_files<-final_files[!(final_files %in% exclude)]

# CW Function ##############################################
# This takes ~3 hrs to run

#run CW function
to_cw <- rbindlist(pblapply(final_files, prep_cw), fill = T)

#write to CSV
write.csv(to_cw, "FILEPATH/to_cw.csv", row.names = F)

#If we have already created above just run this ####################
## combine ########
to_cw <- fread("FILEPATH/to_cw.csv")

# water #############
w.cw <- to_cw[!is.na(w.indicator) & missing_w_source_drink <= 0.15] # exclude sources with greater than 15% missingness
w.cw[, iso3 := substr(ihme_loc_id, 1, 3)]

Ahhh# fix some typos
w.cw[w.indicator == "suface", w.indicator := "surface"]
w.cw[w.indicator == "public_im", w.indicator := "piped_imp"]

# LBD regions
regions <- read.csv('FILEPATH/geoid_stg2.csv')
regions <- regions[,c('iso3', 'region')]
regions$iso3 <- as.character(regions$iso3)
regions$region <- as.character(regions$region)
regions <- regions[complete.cases(regions),]
setDT(regions)

w.cw <- merge(w.cw, regions, by = "iso3", all.x = T)
w.cw <- merge(w.cw, locs[, .(ihme_loc_id, region_name)], by = "ihme_loc_id")
w.cw[region == "excluded", region := NA]

# fix locs w/o regions
w.cw[is.na(region) & region_name == "Central Europe", region := "central_eur"]
w.cw[is.na(region) & region_name == "Central Asia", region := "central_asia"]
w.cw[is.na(region) & region_name == "Eastern Europe", region := "east_eur"]
w.cw[is.na(region) & region_name == "Caribbean", region := "mcacaf"]
w.cw[is.na(region) & region_name == "Western Europe", region := "west_eur"]
w.cw[is.na(region) & region_name == "Southeast Asia", region := "se_asia"]
w.cw[is.na(region) & region_name == "Southern Latin America", region := "s_america"]
w.cw[ihme_loc_id %in% c("PSE","TUR"), region := "mid_east"] 

# summarize indicators
w.cw[, N := .N, by = c("iso3","region")] # sample size by country
for (loc in unique(w.cw$iso3)) {
  unique_sources <- length(w.cw[iso3 == loc, unique(nid)])
  w.cw[iso3 == loc, sources := unique_sources]
}

w.totals <- dcast(w.cw, iso3 + region + N + sources ~ w.indicator, value.var = "w.indicator", fun.aggregate = length)

# save
setnames(w.totals, "region", "reg")
write.csv(w.totals, "FILEPATHk/cw_water_all.csv", row.names = F) # everything

water.cw <- fread("FILEPATH/cw_water_2.csv") # LBD's
new_iso3 <- setdiff(unique(w.totals$iso3), unique(water.cw$iso3)) # iso3's that are not in LBD's
w.totals.new <- w.totals[iso3 %in% new_iso3]
write.csv(w.totals.new, "FILEPATH/cw_water_new_iso3.csv", row.names = F)

############ sanitation #############
s.cw <- to_cw[!is.na(s.indicator) & missing_t_type <= 0.15]
s.cw[, iso3 := substr(ihme_loc_id, 1, 3)]

# fix some typos
s.cw[s.indicator == "im", s.indicator := "imp"]
s.cw[s.indicator == "latrine", s.indicator := "latrine_imp"]
s.cw[s.indicator == "flush_imp_Sewer", s.indicator := "flush_imp_sewer"]

# LBD regions
regions <- read.csv('FILEPATH/geoid_stg2.csv')
regions <- regions[,c('iso3', 'region')]
regions$iso3 <- as.character(regions$iso3)
regions$region <- as.character(regions$region)
regions <- regions[complete.cases(regions),]
setDT(regions)

s.cw <- merge(s.cw, regions, by = "iso3", all.x = T)
s.cw <- merge(s.cw, locs[, .(ihme_loc_id, region_name)], by = "ihme_loc_id")
s.cw[region == "excluded", region := NA]

# fix locs w/o regions
s.cw[is.na(region) & region_name == "Central Europe", region := "central_eur"]
s.cw[is.na(region) & region_name == "Central Asia", region := "central_asia"]
s.cw[is.na(region) & region_name == "Eastern Europe", region := "east_eur"]
s.cw[is.na(region) & region_name == "Caribbean", region := "mcacaf"]
s.cw[is.na(region) & region_name == "Western Europe", region := "west_eur"]
s.cw[is.na(region) & region_name == "Southeast Asia", region := "se_asia"]
s.cw[is.na(region) & region_name == "Southern Latin America", region := "s_america"]
s.cw[ihme_loc_id %in% c("ARE","PSE","TUR"), region := "mid_east"]

# summarize indicators
s.cw[, N := .N, by = c("iso3","region")] # sample size by country
for (loc in unique(s.cw$iso3)) {
  unique_sources <- length(s.cw[iso3 == loc, unique(nid)])
  s.cw[iso3 == loc, sources := unique_sources]
}

s.totals <- dcast(s.cw, iso3 + region + N + sources ~ s.indicator, value.var = "s.indicator", fun.aggregate = length)

# save
setnames(s.totals, c("region","open"), c("reg","od"))
s.totals[, flush_imp := flush_imp + flush_imp_septic + flush_imp_sewer]
s.totals[, c("flush_imp_septic","flush_imp_sewer") := NULL]
write.csv(s.totals, "FILEPATH/cw_sani_all.csv", row.names = F) # everything

sani.cw <- fread("FILEPATH/cw_sani_2.csv") # LBD's
new_iso3 <- setdiff(unique(s.totals$iso3), unique(sani.cw$iso3)) # iso3's that are not in LBD's
s.totals.new <- s.totals[iso3 %in% new_iso3]
write.csv(s.totals.new, "FILEPATH/cw_sani_new_iso3.csv", row.names = F) # only the iso3's not already in LBD's dataset
