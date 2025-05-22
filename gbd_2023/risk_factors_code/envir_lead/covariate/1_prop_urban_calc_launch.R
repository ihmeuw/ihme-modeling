# run prop_urban_calc

rm(list=ls())

source("FILEPATH/get_location_metadata.R")
library(arrow)
library(readr)

# args ##############################
release<-16
loc_set<-22 #location_set_id, covariate computation
gbd_cycle<-"GBD_2023"
gbdcycle<-"GBD2023" 
out_cycle<-"GBD2022" #GBD cycle for out directory
urb_date<-"2024_05_01" #folder date for the urbanity binary directory
pop_date<-"20240506" #folder date for the population binary directory
out.dir<-paste0("FILEPATH/",out_cycle,"/FILEPATH")
grid_yrs<-c(1975,1980,1985,1990,1995,2000,2005,2010,2015,2020) #years that are in both the urbanicity and population files

#folder creation
for (yr in grid_yrs) {
  if(!file.exists(paste0(out.dir,"/FILEPATH",yr))){
    dir.create(paste0(out.dir,"/FILEPATH",yr))
  }
}

if (!file.exists(paste0(out.dir,"FILEPATH"))) {
  dir.create(paste0(out.dir,"/FILEPATH"))
}

# Run Rasters ################################################################
#first run the jobs so that we can get all of the rasters into a data.table
for (yr in grid_yrs) {
  job <- paste0("sbatch -J urb_r_", yr,
                " --mem 200G -c 20 -p long.q -t 120 ",
                "-A USERNAME -o FILEPATH -e FILEPATH ",
                "FILEPATH -s ",
                "FILEPATH/prop_urban_raster_edit_2cat.R ", 
                paste0(c(release, loc_set, gbd_cycle, gbdcycle, urb_date, pop_date, out.dir, yr), collapse = " "))
  
  system(job)
  print(job)
}


# Borders and locs ###########################################################
#global shapefile directory
shapefile.dir <- paste0("FILEPATH/",gbd_cycle,"/FILEPATH")
shapefile.version <- paste0(gbdcycle,"FILEPATH") # loc_set_22 is covariates location set

# load in the borders for the countries
borders<-sf::read_sf(shapefile.dir, layer = shapefile.version)
borders$location_id <- borders$loc_id # this variable is misnamed, fix it here
borders$ihme_loc_id <- borders$ihme_lc_id # ditto

borders_locs<-unique(borders$location_id)

#get location metadata
locs<-get_location_metadata(release_id = release, location_set_id = loc_set)

#make a list of locations we are looking at. We only want locations that we estimate for
locs<-locs[is_estimate==1 & location_id %in% borders_locs,c("location_id")]
locs<-locs[[1]]

# run urban calc #############################################
for (yr in grid_yrs) {
  for(loc in locs){
    
  job <- paste0("sbatch -J urb_", yr,"_",loc,
                " --mem 150G -c 20 -p long.q -t 2880 ",
                "-A USERNAME -o FILEPATH -e FILEPATH ",
                "FILEPATHh -s ",
                "FILEPATH/prop_urban_calc.R ", 
                paste0(c(release, loc_set, gbd_cycle, gbdcycle, urb_date, pop_date, out.dir, yr, loc), collapse = " "))
  
  system(job)
  print(job)
  }
}


# Determine which locations didn't run #####################################
#make a function to check if the file exist in each folder
file_exist<-function(yr,loc){
  filepath<-paste0(out.dir,"/yr_",yr,"/urban_center_",yr,"_loc_",loc,".feather")
  file.exists(filepath)
}

#check for file existence
to_run<-data.table()

for(yr in grid_yrs){
  for (loc in locs){
    complete<-file_exist(yr,loc)
    to_run<-rbindlist(list(to_run,data.table(year_id=yr, location_id=loc, complete = complete)),use.names = T)
  }
}

#now only keep the ones that are false
to_run<-to_run[complete==F,]


#rerun the job with a longer time
for (n in 1:nrow(to_run)) {
  yr<-to_run[n,year_id]
  loc<-to_run[n,location_id]
  
  job <- paste0("sbatch -J urb_", yr,"_",loc,
                " --mem 150G -c 20 -p long.q -t 2880 ",
                "-A USERNAME -o FILEPATH -e FILEPATH ",
                "FILEPATH -s ",
                "FILEPATH/prop_urban_calc.R ", 
                paste0(c(release, loc_set, gbd_cycle, gbdcycle, urb_date, pop_date, out.dir, yr, loc), collapse = " "))
  
  system(job)
  print(job)
}

#final table ###############################
#Once all of the jobs finish, load them all in and rbind them together
sub_outdir<-grep("^yr_", basename(list.dirs(path = out.dir, full.names = TRUE, recursive = FALSE)), value = TRUE)

#list the files
files<- lapply(sub_outdir, function(subdir) {
  list.files(path = file.path(out.dir, subdir), full.names = TRUE)
})

files_list<-unlist(files,recursive = T, use.names = F)

#read in and combine the files
final<-rbindlist(lapply(files_list, read_feather), use.names = TRUE, fill = TRUE)

#save
write_excel_csv(final,file.path(out.dir,"urban_combo.csv"))
