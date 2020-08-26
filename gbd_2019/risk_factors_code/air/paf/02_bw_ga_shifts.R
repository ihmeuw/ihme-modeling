
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 6/12/2019
# Purpose: Generate bw/ga shifts from IER to feed into bw/ga paf calculation
#          
# source("FILEPATH.R", echo=T)
# qsub -N shift_calc_bwga -l fthread=5 -l m_mem_free=10G -l h_rt=2:00:00 -l archive -q all.q -P ADDRESS -o FILEPATH -e FILEPATH FILEPATH.sh FILEPATH.R
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

arg <- commandArgs()[-(1:5)]  # First args are for unix use only

if (length(arg)==0) {
  #toggle for targeted run on cluster
  arg <- c("543", #location
           2017, #year
           35, #exp grid version
           38, #ier version
           45, #output version
           1000, #draws required
           58934, #stgpr HAP run_id
           "092419")  #hap map date
}

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","fst")

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
location_set_id <- 22

# Set parameters from input args
this.country <- arg[1]
this.year <- arg[2]
exp.grid.version <- arg[3]
ier.version <- arg[4]
output.version <- arg[5]
draws.required <- as.numeric(arg[6])
run_id <- arg[7]
hap.map.date <- arg[8]

draw.cols <- paste0("draw_", 1:draws.required)
pred.cols <- paste0("pred_",1:draws.required)
format.cols <- paste0("draw_", 0:(draws.required-1)) # formatting
hap.map.cols <- paste0("hap_map_", 1:draws.required)
hap.prop.cols <- paste0("hap_prop_", 1:draws.required)
hap.ratio.cols <- paste0("hap_ratio_",1:draws.required)
ambient.cols <- paste0("ambient_",1:draws.required)
paf.cols <- paste0("draw_", 0:(draws.required-1)) # must be saved in 0-999 format
mr.brt.cols <- paste0("mrbrt_",1:draws.required)

exp_cats <- c("tmrel","ambient","hap")

# set project values
location_set_id <- 22

#Generate tmrel distribution
set.seed(143)
tmrel.draws <- runif(draws.required,2.4,5.9)

# Directories -------------------------------------------------------------

hap.prop.in <- file.path("FILEPATH",run_id,"FILEPATH")
hap.map.in <- file.path(home.dir,"FILEPATH",hap.map.date)
ambient.grid.in <- file.path(home.dir,"FILEPATH",exp.grid.version,"FILEPATH")
out.dir <- file.path(home.dir,"FILEPATH",output.version)
out.sum.dir <- file.path(home.dir,"FILEPATH",output.version)
out.prop.dir <- file.path(home.dir,"FILEPATH",output.version)
ier.dir <- file.path(j_root,"FILEPATH",ier.version)

mr.brt.dir <- file.path(j_root,"FILEPATH")

dir.create(out.dir,showWarnings=F)
dir.create(out.sum.dir,showWarnings=F)
dir.create(out.prop.dir,showWarnings=F)

# Load/format data ----------------------------------------------------------------

hap.prop <- fread(paste0(hap.prop.in,"/",this.country,".csv"))[year_id==this.year]
setnames(hap.prop,format.cols,hap.prop.cols)
hap.prop <- hap.prop[,c(hap.prop.cols),with=F]

hap.map <- fread(paste0(hap.map.in,"FILEPATH",hap.map.date,".csv"))[location_id==this.country & year_id==this.year & grouping!="indoor"]
setnames(hap.map,draw.cols,hap.map.cols)
hap.map <- hap.map[,c("grouping",hap.map.cols),with=F]

# read in ambient grid
ambient <- read.fst(paste0(ambient.grid.in,"/",this.country,"_",this.year,".fst")) %>% as.data.table
names(ambient) <- tolower(names(ambient))
setnames(ambient,pred.cols,ambient.cols)
ambient <- ambient[,c("latitude","longitude","weight","pop",ambient.cols),with=F]

if(exp.grid.version==35){
  #drop gridcells of zero to speed up computation
  if(ambient[,mean(pop)]>0.1){
    ambient <- ambient[pop>0.1]
  }
}

# make data long by draw for easier manipulation
hap.prop <- melt(hap.prop,measure.vars=hap.prop.cols,value.name="hap_prop",variable.name="draw",value.factor=T)
hap.prop[,draw:=as.numeric(draw)]

hap.map <- melt(hap.map,id.vars="grouping",measure.vars=hap.map.cols,value.name="hap_map",variable.name="draw",value.factor=T)
hap.map[,draw:=as.numeric(draw)]

ambient <- melt(ambient,measure.vars=ambient.cols,value.name="ambient",variable.name="draw",value.factor=T)
ambient[,draw:=as.numeric(draw)]

# merge hap datasets together
exp <- merge(hap.prop,hap.map,by="draw")

exp <- exp[grouping=="female"] #only need female for bw_ga

# calculate total exposure for those exposed to hap
exp[,hap_excess:=hap_map] #hap only exposure (above ambient levels)

# merge on grid cell ambient
exp <- merge(exp,ambient,by="draw",allow.cartesian=T)

# calculate total exposures
exp[,hap:=hap_excess+ambient] # total exposure for those exposed to HAP, used to get value from exposure-response curve

# Add in tmrel
exp[,tmrel:=tmrel.draws[draw]]

# Calculate shifts ----------------------------------------------------------------

calculate_shift <- function(cause,dt){
  # Read in mrbrt estimates
  mrbrt <- fread(paste0(mr.brt.dir,"/",cause,"FILEPATH",cause,"FILEPATH.csv"))
  
  # duplicate draws to ensure 1000 estimates
  n_draws <- ncol(mrbrt)-1
  
  if(n_draws<draws.required){
    for(d in n_draws:(draws.required-1)){
      mrbrt[,paste0("draw_",d):=get(paste0("draw_",d-n_draws))]  # This repeats the first couple sets of draws out to ensure a complete 1000
    }
  }
  
  setnames(mrbrt,format.cols,mr.brt.cols)
  
  # make long by draw
  mrbrt <- melt.data.table(mrbrt,id.vars="exposure_spline", measure.vars=patterns("mrbrt_"), variable.name="draw", variable.factor=T, value.name="mrbrt")
  mrbrt[,draw:=as.numeric(draw)]
  
  mrbrt_exposures <- unique(mrbrt$exposure_spline)
  
  setkeyv(mrbrt,c("draw","exposure_spline"))
  
  # Calculate RR  
  for(exp_cat in exp_cats){
    
    # merge on mrbrt predictions based on closest available exposure datapoint predicted for mr_brt & draw
    dt[,paste0("round_",exp_cat):=mrbrt_exposures[which.min(abs(mrbrt_exposures - get(exp_cat)))],by=1:nrow(dt)]
    
    setkeyv(dt,c("draw",paste0("round_",exp_cat)))
    
    # get mrbrt estimate
    dt <- merge(dt,mrbrt,by.x=c("draw",paste0("round_",exp_cat)),by.y=c("draw","exposure_spline"))
    # change name
    setnames(dt,"mrbrt",paste0(cause,"_",exp_cat))
  }
  
  rm(mrbrt)
  
  # scale estimate shifts based on draw of tmrel
  for(exp_cat in setdiff(exp_cats,"tmrel")){
    dt[,paste0(cause,"_",exp_cat):=min(get(paste0(cause,"_",exp_cat))-get(paste0(cause,"_tmrel")),0),by=1:nrow(dt)] # if exposure is less than tmrel, set to zero
  }
  
  return(dt)
  
}

bw_dt <- calculate_shift("bw",dt=exp)
ga_dt <- calculate_shift("ga",dt=exp)

exp <- merge(bw_dt,ga_dt,by=intersect(names(bw_dt),names(ga_dt)))

# calculate average shift based on proportion of pop exposed to hap
exp[,bw:=bw_hap*hap_prop+bw_ambient*(1-hap_prop)]
exp[,ga:=ga_hap*hap_prop+ga_ambient*(1-hap_prop)]

# population weight shift, ambient exposure, RR
exp <- merge(exp[,lapply(.SD,weighted.mean,w=pop*weight),.SDcols=c("bw","ga","ambient"), by=c("grouping","draw")], #pop_weighted PM shift to calculate PM pafs, exposure for proportional split
            exp[,c("hap_prop","hap_excess","grouping","draw"),with=F] %>% unique, #HAP doesn't differ by gridcell, only by group
            by=c("grouping","draw"))

exp[,pop_average_pm:=ambient+hap_prop*hap_excess]  #population average exposure (denominator of proportion for splitting pafs)
exp[,hap_paf_ratio:=(hap_prop)*(hap_excess)/pop_average_pm] # proportion of paf attributable to hap
exp[,ambient_paf_ratio:=ambient/pop_average_pm] # proportion of paf attributable to ambient

exp[,location_id:=this.country]
exp[,year_id:=this.year]

# Write summary file & save --------------------------------------------------------------

lower <- function(x){quantile(x,p=.025)}
upper <- function(x){quantile(x,p=.975)}

summary <- melt(exp,id.vars=c("grouping","location_id","year_id","draw"))
summary <- summary[,.(mean=mean(value),lower=lower(value),upper=upper(value)),by=c("grouping","location_id","year_id","variable")]

write.csv(summary,paste0(out.sum.dir,"/",this.country,"_",this.year,".csv"),row.names=F)

# save bw ga shifts
write.csv(exp[,.(location_id,year_id,draw,bw,ga)],paste0(out.dir,"/",this.country,"_",this.year,".csv"),row.names=F)

# save draws of proportional PAF splits for later
write.csv(exp[,.(location_id,year_id,grouping,draw,hap_paf_ratio,ambient_paf_ratio)],paste0(out.prop.dir,"/",this.country,"_",this.year,".csv"),row.names=F)