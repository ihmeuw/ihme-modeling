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

project <- "-P PROJECT "
sge.output.dir <- " -o FILEPATH -e FILEPATH "
#sge.output.dir <- "" # toggle to run with no output files

# load packages, install if missing
lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","ggplot2","openxlsx","metafor","pbapply")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

'%ni%' <- Negate("%in%")

tmrel.version <- VERSION

#------------------Directories and shared functions-----------------------------
# load current locations list
source("FILEPATH/get_location_metadata.R")
location_set_version <- 35
locations <- get_location_metadata(location_set_version, gbd_round_id=7)

out_dir <- paste0("FILEPATH", tmrel.version)
dir.create(out_dir,recursive = T)

#------------------Prep RR data-------------------------------------------------
# read in the NO2 RR data
data <- read.xlsx(file.path(h_root,"FILEPATH.xlsm")) %>% as.data.table

# remove observations with no NID
data <- data[!is.na(nid)]
data <- data[is.na(exclude)|exclude==0]

# drop observations with an undefined NO2_conc_increment for now
data <- data[nid!=416839]
# drop observations with a negative NO2_conc_increment for now
data <- data[nid!=436414]

#------------------Convert exposures to ppb-------------------------------------
# convert all NO2 concentration increments to ppb
# using US EPA conversion factor of 0.5319 * micrograms/m^3 = ppb
# 1 ppb = 1.88 micrograms/m^3
data <- data[,NO2_conc_increment:=as.numeric(NO2_conc_increment)]
data[NO2_conc_units=="microgram/m^3", NO2_conc_increment:=NO2_conc_increment*.5319]

# convert all NO2 exposure means to ppb
data[NO2_conc_units=="microgram/m^3", NO2_conc_min:=NO2_conc_min*.5319]
data[NO2_conc_units=="microgram/m^3", NO2_conc_2.5:=NO2_conc_2.5*.5319]
data[NO2_conc_units=="microgram/m^3", NO2_conc_97.5:=NO2_conc_97.5*.5319]
data[NO2_conc_units=="microgram/m^3", NO2_conc_sd:=NO2_conc_sd*.5319]
data[NO2_conc_units=="microgram/m^3", NO2_conc_median:=NO2_conc_median*.5319]

# convert IQR to numeric value
convert_to_numeric <- function(x){
  iqr <- strsplit(x, "-")
  iqr <- as.numeric(iqr[[1]])
  numeric <- iqr[2]-iqr[1]
  return(numeric)
}

data <- data[grepl("-",NO2_conc_iqr), NO2_conc_iqr:=convert_to_numeric(NO2_conc_iqr)]
data[,NO2_conc_iqr:=as.numeric(NO2_conc_iqr)]
data[NO2_conc_units=="microgram/m^3", NO2_conc_iqr:=NO2_conc_iqr*.5319]

data[,NO2_conc_units:="ppb"]

#------------------Estimate min concentrations----------------------
temp <- data[!is.na(NO2_conc_median) & !is.na(NO2_conc_sd)]
temp <- temp[,c("nid","ihme_loc_id","location_name","NO2_conc_mean","NO2_conc_min","NO2_conc_median","NO2_conc_sd")]
temp <- unique(temp)

temp <- temp[order(NO2_conc_median)]
temp <- temp[1:5] # we only take the lowest 5

# estimate 0.1 percentile because we're missing the min for one study
temp[,NO2_conc_0.1 := NO2_conc_median - NO2_conc_sd*3.090]
temp[NO2_conc_0.1<0,NO2_conc_0.1:=0]
temp[is.na(NO2_conc_min),NO2_conc_min:=NO2_conc_0.1]

# estimate 5th percentile concentrations (use mean, sd, & z-score)
temp[,NO2_conc_5 := NO2_conc_median - NO2_conc_sd * 1.645]
temp[NO2_conc_5 < NO2_conc_min, NO2_conc_5 := NO2_conc_min]
temp[NO2_conc_5<0, NO2_conc_5:= 0]

tmrel <- data.table(min = mean(temp$NO2_conc_min, na.rm=T),
                       conc_5 = mean(temp$NO2_conc_5, na.rm=T))
