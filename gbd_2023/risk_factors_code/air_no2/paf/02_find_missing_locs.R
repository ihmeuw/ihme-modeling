
#-------------------Header------------------------------------------------
# Purpose: Diagnose missing locations after running PAFs
#          
#------------------Set-up--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}


# load packages, install if missing
lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","fst","parallel","raster","pbapply","tictoc")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# set working directories
home.dir <- file.path("FILEPATH")
setwd(home.dir)

# set project values
location_set_id <- 35

source(file.path(central_lib,"current/r/get_location_metadata.R"))
locs <- get_location_metadata(35)

grid.version <- 7
output.version <- 12

years <- c(1990,1995,2000,2005,2010,2015,2019,2020,2021,2022) # estimation years only

# Directories & functions -------------------------------------------------------------

no2.grid.in <- file.path("FILEPATH")
status.dir <- file.path("FILEPATH")

locations <- list.files(no2.grid.in)
locations <- grep(paste(years,collapse="|"), 
                        locations, value=TRUE)
finished <- list.files(status.dir)

# check to see which location years we are missing

missing <- c()
for(l in locations){
  if(!l%in%finished){
    missing <- c(missing,l)
  }
}

#------------------------Make NO2 tracker retroactively-------------------------------

grid.version <- 7
draws.dir <- paste0("FILEPATH")
files <- list.files(draws.dir)

source(file.path("FILEPATH/get_location_metadata.R"))
locations <- get_location_metadata(35)
locations <- locations[most_detailed==1,location_id]

tracker_dir <- paste0("FILEPATH")


for(year in years){
  for(loc in locations){

    if(length(files[grepl(glob2rx(paste0(loc,"_*",year,".csv")),files)])>1){
      n <- files[grepl(glob2rx(paste0(loc,"_*",year,".csv")),files)] %>% strsplit( "_" ) %>% sapply( "[", 2 ) %>% as.numeric
      nfiles <- max(n)

      tracker <- fread(paste0(tracker_dir,"/split_locs.csv"))
      new_row <- data.table("location_id" = loc,
                            "year_id" = year,
                            "nfiles" = nfiles)
      tracker <- rbind(tracker,new_row)
      write.csv(tracker,paste0(tracker_dir,"/split_locs.csv"),row.names = F)
    }
  }

  print(paste0("Done with ", year))
}

# 
