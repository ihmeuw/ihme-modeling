
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 11/20/19
# Purpose: RR max calculation for ambient and HAP
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

packages <- c("data.table","magrittr","fst","Hmisc","stringr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

#------------------DIRECTORIES AND FUNCTIONS-------------------------------
# age expansion function
source("FILEPATH.R")
# general misc helper functions
central.function.dir <- file.path(h_root, "FILEPATH")
file.path(central.function.dir, "FILEPATH.R") %>% source

source(paste0(central_lib,"FILEPATH.R"))
source(paste0(central_lib,"FILEPATH.R"))
source(paste0(central_lib,"FILEPATH.R"))

locs <- get_location_metadata(35)
ages <- get_age_metadata(12)

years <- c(1990,2010,2019)

pops <- get_population(age_group_id=ages$age_group_id,
                       location_id=locs[most_detailed==1,location_id],
                       year_id=years,
                       sex_id=c(1,2),
                       gbd_round_id=6,
                       decomp_step="step4")

pops[sex_id==1,grouping:="male"]
pops[sex_id==2,grouping:="female"]
pops[age_group_id<=5, grouping:="child"]

pops <- pops[,.(population=sum(population)),by=c("year_id","location_id","grouping")]


# Load and format data ---------------------------------------------------------------
files <- list.files("FILEPATH",full.names=T)

exposure <- data.table()
for(year in years){
  dt <- lapply(grep(paste0(year,".csv"),files,value=T),fread) %>% rbindlist()
  exposure <- rbind(exposure,dt)
}

exposure <- exposure[variable %in% c("ambient","hap_excess")]

# make wide by RF exposure
exposure <- dcast.data.table(exposure,location_id + year_id + grouping ~ variable, value.var="mean")

# recalculate total hap
exposure[,hap_total:=ambient+hap_excess]

exposure <- merge(exposure,pops,by=c("location_id","year_id","grouping"))

max <- exposure[,.(air_pm=wtd.quantile(ambient,0.99,weights=population),
                   air_hap=wtd.quantile(hap_excess,0.99,weights=population),
                   air_pmhap=wtd.quantile(hap_total,0.99,weights=population)),by=c("grouping")]

# make max long
max <- melt(max, id.vars="grouping", variable.name = "rei")
max[,value:=signif(value,digits=3)]

# read in risk curves
labels <- c(paste0("cvd_ihd_",seq(25,95,5)),paste0("cvd_stroke_",seq(25,95,5)),
            "lri","resp_copd","t2_dm","neo_lung")

curves <- data.table()
for(label in labels){
  dt <- fread(paste0("FILEPATH",label,"/",label,"FILEPATH.csv"))
  dt[,exposure_spline:=signif(exposure_spline,digits=3)]
  
  setnames(dt,"exposure_spline","value")
  
  dt <- merge(max,dt,all.x=T,by="value")
  
  dt[,label:=label]
  
  if(label!="lri"){
    dt <- dt[grouping!="child"]
  }
  
  dt[,cause:=gsub("_[0-9][0-9]","",label)]
  dt[,age:=as.integer(str_sub(label,start=-2))]
  dt[is.na(age),age:=99]
  
  dt <- expandAges(dt,cause.code=unique(dt$cause))
  
  curves <- rbind(curves,dt)
  
  print(paste("Done with ",label))
}

# make long by draw
curves <- melt.data.table(curves,
                          id.vars = c("value","grouping","rei","cause","age_group_id","sex_id"),
                          measure.vars = patterns("draw_"),
                          variable.name = "draw",
                          value.name = "rr")

curves[,draw:=tstrsplit(draw,"_",keep=2)]
curves[,draw:=as.integer(draw)]


# Expand cvd_stroke ------------------------------------------------------------------
# expand cvd_stroke to include relevant subcauses in order to prep for merge to YLDs, using custom find/replace function
# supply the values you want to find/replace as vectors
old.causes <- c('cvd_stroke')
replacement.causes <- c('cvd_stroke_isch',
                        "cvd_stroke_intracerebral", 
                        "cvd_stroke_subarachnoid")
curves[,acause:=cause]

# pass to custom function
curves <- findAndReplace(curves,
                      old.causes,
                      replacement.causes,
                      "acause",
                      "acause",
                      TRUE) #set this option to be true so that rows can be duplicated in the table join (expanding the rows)

# replace each cause with cause ID
curves[, cause_id := acause] #create the variable
# supply the values you want to find/replace as vectors
cause.codes <- c('cvd_ihd',
                 "cvd_stroke_isch",
                 "cvd_stroke_intracerebral",
                 "cvd_stroke_subarachnoid",
                 "lri",
                 'neo_lung',
                 'resp_copd',
                 't2_dm')

cause.ids <- c(493,
               495,
               496,
               497,
               322,
               426,
               509,
               976)

# pass to custom function
curves <- findAndReplace(curves,
                      cause.codes,
                      cause.ids,
                      "cause_id",
                      "cause_id")


# Save and plot ----------------------------------------------------------------------
for(this.rei in unique(curves$rei)){
  out <- curves[rei==this.rei, .(cause_id,rei,draw,rr,age_group_id,sex_id)]
  
  write.csv(out,paste0("FILEPATH",this.rei,"FILEPATH.csv"),row.names=F)
}


ggplot(curves[,mean(rr),by=c("acause","age_group_id","sex_id","rei")],
       aes(x=as.factor(age_group_id),y=V1,color=as.factor(sex_id)))+
  geom_point()+
  facet_grid(acause~rei,scales="free_y")