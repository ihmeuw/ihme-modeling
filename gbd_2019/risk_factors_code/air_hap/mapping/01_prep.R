
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 02/14/2019
# Purpose: Read in WHO HAP mapping database and transform for modeling
#          
# source("FILEPATH.R", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
} else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
}

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

date <- format(Sys.Date(), "%m%d%y")

#------------------DIRECTORIES--------------------------------------------------

home_dir <- file.path("FILEPATH")

in_dataset <- file.path(home_dir,"FILEPATH.xlsx")
out_dataset <- paste0(home_dir,"FILEPATH",date,".csv")

# central functions
source(file.path(central_lib,"FILEPATH.R"))
locs <- get_location_metadata(35)
source(file.path(central_lib,"FILEPATH.R"))

#------------------FORMAT DATA FOR MODELING------------------------------------

dt <- read.xlsx(in_dataset,sheet="Sheet1",startRow=2) %>% as.data.table

# drop exclusions
dt <- dt[is.na(exclude)]

# drop rows that are general descriptives
dt <- dt[!(grepl("General",`Study.Label-Tag`))]

# isolate to PM2.5 studies
dt <- dt[PM.size.fraction %in% c("PM2.5","PM 2.5","< PM2.5","2.5")]

# set outcome value to be arithmetic mean, geometric mean, or median in that order; label
dt[,c("pm_measure","measure_type") := list(PM.Arithmetic.Mean,"arithmetic mean")]
dt[(is.na(pm_measure)), c("pm_measure","measure_type") := list(as.numeric(PM.Geometric.Mean),"geometric mean")]
dt[(is.na(pm_measure)), c("pm_measure","measure_type") := list(PM.Median,"median")]

# drop rows without measure of central tendency
dt <- dt[!is.na(pm_measure)]

# scale PM2.5 based on unit
dt <- dt[PM.Unit.of.Measurement %in% c("µg/m3","μg/m3","ug/m3","mg/m3")]
dt[PM.Unit.of.Measurement == "mg/m3", pm_measure := pm_measure * 1000]

# make variable for standard v. non-standard measurement period. 
# Standard measurement is 24 hours or more. 
# There are 2 studies with no data, I assumed they were standard. 
dt[,measure_24hr:=1]
dt[PM.Averaging.Period %in% c("1hour","Meal Duration","1 hr","Cooking Period (1hr)","6 hours","60 min cooking + 20 min pre-cooking","cook time","Cooking time (TWA): 10-15 min","at least 4 hrs (avg: 11.8 hrs)","at least 4 hrs ",
                              "9 hours", "1 hour", "8 hrs", "Peak concentration", "Cooking duration (1.5 hours)", "8 hours", "6 h", "at least 4 hrs (avg. 10.4 hrs)"),
   measure_24hr:=0]

# personal or area
dt[grepl("ersonal",PM.Type.of.Measurement),measure_loc:="personal"]
dt[grepl("ersonal",PM.Location.of.Sampler) & is.na(measure_loc),measure_loc:="personal"]
dt[Type.of.Measurements %in% c("Personal/Annual average") & is.na(measure_loc), measure_loc:="personal"]
dt[grepl("utdoor|mbient",PM.Type.of.Measurement) & is.na(measure_loc), measure_loc:="outdoor"]
dt[grepl("rea|itchen|ndoor|itting",PM.Type.of.Measurement) & is.na(measure_loc), measure_loc:="indoor"]
dt[grepl("Yard|Patio",PM.Location.of.Sampler) & is.na(measure_loc), measure_loc:="outdoor"]
dt[!is.na(PM.Location.of.Sampler) & is.na(measure_loc), measure_loc:="indoor"]
dt[grepl("Area",Type.of.Measurements) & is.na(measure_loc),measure_loc:="indoor"]
dt[is.na(measure_loc), measure_loc:="indoor"]

# drop outdoor studies
dt <- dt[measure_loc!="outdoor"]

# when in doubt, assume kitchen for the rest
dt[is.na(measure_loc), measure_loc:="indoor"]

#--------------------------MALE/FEMALE/CHILD------------------------------------

# man/woman/child
# for S no 137 in China, only keep aggregate for women and male/female for children
dt <- dt[S.No!=137 | PM.Type.of.Measurement %in% 
     c("Personal Exposures (Women)", 
       "Personal Exposures (Women)",
       "Personal Exposures (Children Male 5-8)",
       "Personal Exposures (Children Female 5-8)",
       "Personal Exposures (Children Male 9-11)",
       "Personal Exposures (Children Female 9-11)",
       "Personal Exposures (Children-Male-12-14)",
       "Personal Exposures (Children Female 12-14)",
       "Area (Kitchen)") ]

dt[grepl("Male",PM.Type.of.Measurement) & measure_loc=="personal", measure_group:="male"]
dt[grepl("Women|Female", PM.Type.of.Measurement) & measure_loc=="personal" & is.na(measure_group), measure_group:="female"]
dt[grepl("(Child)|(Infant)", PM.Type.of.Measurement) & measure_loc=="personal" & is.na(measure_group), measure_group:="child"]

dt[grepl("omen|emale", `Gender./Age.groups.of.study.participants`) & measure_loc=="personal" & is.na(measure_group), measure_group:="female"]
dt[grepl("hild", `Gender./Age.groups.of.study.participants`) & measure_loc=="personal" & is.na(measure_group), measure_group:="child"]

# assume the rest are female
dt[measure_loc=="personal" & is.na(measure_group), measure_group:="female"]

dt[measure_loc=="indoor",measure_group:="indoor"]

#------------------FUEL TYPES---------------------------------------------------

# fuel types
setnames(dt,"PM.Measurement.Categorical.Descriptives.(Rural/Urban;.Location;.Season;.Fuel.Type;.Kitchen.Type;Stove.Type;Others)","this_measure")

dt[,c("clean","kerosene","coal","wood","crop","dung"):=0]

# assign fuel types
dt[grepl("elec|gas|lpg|methane|clean fuel",this_measure,ignore.case=T),clean:=1]
dt[grepl("kerosene",this_measure,ignore.case=T),kerosene:=1]
dt[grepl("coal|\"mixed\" fuel|solid fuel",this_measure,ignore.case=T),coal:=1]
dt[grepl("wood|biomass|\"mixed\" fuel|solid fuel",this_measure,ignore.case=T),wood:=1]
dt[grepl("crop|biomass|agricult|plant|\"mixed\" fuel|peat|solid fuel|husks",this_measure,ignore.case=T),crop:=1]
dt[grepl("dung|biomass|solid fuel",this_measure,ignore.case=T),dung:=1]

# fix those without labels
dt[clean==0 & kerosene==0 & coal==0 & wood==0 & crop==0 & dung==0 & `Cooking.Fuel(s)`=="Gas, Wood",c("clean","wood"):=1]
dt[clean==0 & kerosene==0 & coal==0 & wood==0 & crop==0 & dung==0 & `Cooking.Fuel(s)`=="Solid fuel",c("coal","wood","crop","dung"):=1]
dt[clean==0 & kerosene==0 & coal==0 & wood==0 & crop==0 & dung==0 & `Cooking.Fuel(s)`=="LPG, Wood, Kerosene, Cowdung",c("clean", "wood", "kerosene", "dung"):=1]

# fix specific study exceptions
# 182B doesn't specify, but I looked at Matt Schupler's database and discovered this
dt[S.No=="182B" & PM.Median==14,clean:=1]
dt[S.No=="182B" & PM.Median==280,c("wood","dung","crop"):=1]

# 139 only includes wood and dung in biomass
dt[S.No=="139", "crop":=0]

# 92 only cooking fuel is wood, problem caused by heating
dt[S.No=="192", c("clean","kerosene","coal","crop","dung"):=0]

# 162C biomass is only wood
dt[S.No=="162C", c("clean","kerosene","coal","crop","dung"):=0]

# 185 only cooking fuel is wood, problem caused by "biomass stove"
dt[S.No=="185", c("clean","kerosene","coal","crop","dung"):=0]

# 193 has a row for charcoal and a row for wood
dt[S.No=="193" & grepl("primary fuel - wood",this_measure), coal:=0 ]
dt[S.No=="193" & grepl("primary fuel - charcoal",this_measure), wood:=0 ]

# drop studies on kerosene only
dt <- dt[!(clean==0 & kerosene==1 & coal==0 & wood==0 & crop==0 & dung==0)]

setnames(dt,"kerosene","includes_kerosene")

dt[,id:=1:.N]

dt[,n_fuels:=sum(clean,coal,wood,crop,dung),by="id"]
dt[,clean:=clean/n_fuels]
dt[,coal:=coal/n_fuels]
dt[,crop:=crop/n_fuels]
dt[,wood:=wood/n_fuels]
dt[,dung:=dung/n_fuels]


# create indicator for solid
dt[,solid:=sum(coal,crop,wood,dung),by="id"]
# for this we don't need to control for total number of fuels
dt[solid<1 & solid>0,c("solid"):=0.5]

dt[,sample_size:=PM.Number.of.Measurements]
dt[is.na(sample_size),sample_size:=get("Number.of.households/.participants") %>% as.numeric()]
dt[is.na(sample_size) & Title %in% 
     c("Particulate matter and carbon monoxide in highland Guatemala: indoor and outdoor levels from traditional and improved wood stoves and gas stoves",
       "Carbon monoxide as a tracer for assessing exposures to particulate matter in wood and gas cookstove households of highland Guatemala"),
   sample_size:=9] #from cleaned database

dt<-dt[,c("S.No","Author","Title","Reference","Year","location_name",
      "pm_measure","measure_type","sample_size","measure_24hr","measure_loc","measure_group","includes_kerosene","clean","coal","wood","crop",
      "dung","solid","n_fuels","id")]

setnames(dt,"Year","year_id")

dt <- merge(dt,locs[,.(location_name,location_id,ihme_loc_id,super_region_name,region_name)],by="location_name",all.x=T)


# Read in GBD extractions -------------------------------------------------

gbd <- fread(file.path(home_dir,"FILEPATH"))

dt <- rbind(dt,gbd,fill=T,use.names=T)


# Add ambient PM2.5 and SDI -------------------------------------------------------------

air_pm <- get_covariate_estimates(106,decomp_step = "iterative", location_id=c(unique(dt$location_id)),year_id=c(unique(dt$year_id)))
setnames(air_pm,"mean_value","air_pm")
dt <- merge(dt,air_pm[,.(location_id,year_id,air_pm)],by=c("location_id","year_id"),all.x=T)

# subtract off ambient exposure
dt[,pm_excess:=pm_measure-air_pm]

# replace negatives with 1 (we are going to transform this into log space)
dt[pm_excess<1, pm_excess:=1]

# merge on sdi

sdi <- get_covariate_estimates(881,decomp_step = "step4", location_id=c(unique(dt$location_id)),year_id=c(unique(dt$year_id)))
setnames(sdi,"mean_value","sdi")
dt <- merge(dt,sdi[,.(location_id,year_id,sdi)],by=c("location_id","year_id"),all.x=T)

dt[,n:=.N,by=c("location_id","year_id","S.No","measure_group","clean","includes_kerosene","coal","wood","crop","dung")]

# save for modelling

write.csv(dt,out_dataset,row.names=F)