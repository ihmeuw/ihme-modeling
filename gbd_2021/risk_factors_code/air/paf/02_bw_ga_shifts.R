
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

arg <- commandArgs()[8:length(commandArgs())]


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
location_set_id <- ID

# Set parameters from input args
this.country <- arg[1]
this.year <- arg[2]
exp.grid.version <- arg[3]
rr.version <- arg[4]
output.version <- arg[5]
draws.required <- as.numeric(arg[6])
run_id <- arg[7] # this is the version for overall HAP proportion (solid only)
hap.map.date <- arg[8]
squeeze.version <- arg[9] # this is the version for disaggregated draws


draw.cols <- paste0("draw_", 1:draws.required)
pred.cols <- paste0("pred_",1:draws.required)
old.cols <- paste0("draw_", 0:(draws.required-1)) # come saved in 0-999 format
hap.map.cols <- paste0("hap_map_", 1:draws.required)
hap.prop.cols <- paste0("hap_prop_", 1:draws.required)
hap.ratio.cols <- paste0("hap_ratio_",1:draws.required)
ambient.cols <- paste0("ambient_",1:draws.required)
paf.cols <- paste0("draw_", 0:(draws.required-1)) # must be saved in 0-999 format
mr.brt.cols <- paste0("mrbrt_",1:draws.required)

fuels <- c("crop","coal","dung","wood")

exp_cats <- c("tmrel","ambient","hap_crop","hap_coal","hap_dung","hap_wood")

# Generate tmrel distribution
set.seed(143)
tmrel.draws <- runif(draws.required,2.4,5.9)


# Directories -------------------------------------------------------------

hap.prop.in <- file.path("FILEPATH",run_id,"FILEPATH")
hap.fuel.in <- file.path(home.dir,"FILEPATH",squeeze.version)
hap.map.in <- file.path(home.dir,"FILEPATH",hap.map.date)
ambient.grid.in <- file.path(home.dir,"FILEPATH",exp.grid.version,"FILEPATH")
out.dir <- file.path(home.dir,"FILEPATH",output.version)
out.sum.dir <- file.path(home.dir,"FILEPATH",output.version)
out.prop.dir <- file.path(home.dir,"FILEPATH/",output.version)
ier.dir <- file.path(j_root,"FILEPATH",rr.version)

mr.brt.dir <- paste0("FILEPATH",rr.version,"FILEPATH")

dir.create(out.dir,recursive = T,showWarnings=F)
dir.create(out.sum.dir,recursive = T,showWarnings=F)
dir.create(out.prop.dir,recursive = T,showWarnings=F)


# Read in data ----------------------------------------------------------------

# format HAP data (HAP disaggregation)
prep_fuel <- function(this.fuel,this.country){
  # load prop data
  hap.prop <- fread(paste0(hap.fuel.in,"/",this.fuel,"/",this.country,".csv"))[year_id==this.year]
  setnames(hap.prop,old.cols,hap.prop.cols)
  hap.prop <- hap.prop[,c(hap.prop.cols),with=F]
  
  # load map data
  hap.map <- fread(paste0(hap.map.in,"/lm_pred_",this.fuel,"_",hap.map.date,".csv"))[location_id==this.country & year_id==this.year & grouping!="indoor"]
  setnames(hap.map,draw.cols,hap.map.cols)
  hap.map <- hap.map[,c("grouping",hap.map.cols),with=F]
  hap.map <- hap.map[,(hap.map.cols):=lapply(.SD,as.numeric), .SDcols=hap.map.cols]
  
  # make data long by draw for easier manipulation
  hap.prop <- melt(hap.prop,measure.vars=hap.prop.cols,value.name=paste0("hap_prop_",this.fuel),variable.name="draw",value.factor=T)
  hap.prop[,draw:=as.numeric(draw)]
  
  hap.map <- melt(hap.map,id.vars="grouping",measure.vars=hap.map.cols,value.name=paste0("hap_map_",this.fuel),variable.name="draw",value.factor=T)
  hap.map[,draw:=as.numeric(draw)]
  
  # merge hap datasets together
  exp <- merge(hap.prop,hap.map,by="draw")
  
  exp <- exp[grouping=="female"] # only need female for bw_ga
  
  # calculate total exposure for those exposed to hap
  exp[,paste0("hap_excess_",this.fuel):=get(paste0("hap_map_",this.fuel))] # hap only exposure (above ambient levels)
  
  return(exp)
  
}

# read in each of the fuel types and merge
crop <- prep_fuel("crop",this.country)
coal <- prep_fuel("coal",this.country)
exp <- merge(crop,coal,by=c("draw","grouping"))
dung <- prep_fuel("dung",this.country)
exp <- merge(exp,dung,by=c("draw","grouping"))
wood <- prep_fuel("wood",this.country)
exp <- merge(exp,wood,by=c("draw","grouping"))

# read in ambient grid 
ambient <- read.fst(paste0(ambient.grid.in,"/",this.country,"_",this.year,".fst")) %>% as.data.table
  
names(ambient) <- tolower(names(ambient))
setnames(ambient,pred.cols,ambient.cols)
ambient <- ambient[,c("latitude","longitude","weight","pop",ambient.cols),with=F]

ambient <- melt(ambient,measure.vars=ambient.cols,value.name="ambient",variable.name="draw",value.factor=T)
ambient[,draw:=as.numeric(draw)]

# merge on grid cell ambient
exp <- merge(exp,ambient,by="draw",allow.cartesian=T)

# calculate total exposures for each fuel type
# total exposure for those exposed to hap, used to get value from RR curve
exp[, `:=` (hap_crop = hap_excess_crop + ambient,
            hap_coal = hap_excess_coal + ambient,
            hap_dung = hap_excess_dung + ambient,
            hap_wood = hap_excess_wood + ambient)]

# Add in tmrel
exp[,tmrel:=tmrel.draws[draw]]


# Calculate shift--------------------------------------------------------------

calculate_shift <- function(cause,dt){
  # Read in mrbrt estimates
  
  mrbrt <- fread(paste0(mr.brt.dir,cause,".csv"))
  mrbrt[,exposure:=NULL]
  mrbrt <- mrbrt[,1:draws.required]
  setnames(mrbrt,mr.brt.cols)
  mrbrt[,exposure_spline:=seq(0,1000,length.out = 1000)]
  
  # make long by draw
  mrbrt <- melt.data.table(mrbrt,id.vars="exposure_spline", measure.vars=patterns("mrbrt_"), variable.name="draw", variable.factor=T, value.name="mrbrt")
  mrbrt[,draw:=as.numeric(draw)]
  
  setkeyv(mrbrt,c("draw","exposure_spline"))
  
  # Calculate RR  
  
  for(exp_cat in exp_cats){
    
    # merge on mrbrt predictions based on closest available exposure datapoint predicted for mr_brt & draw
    # make a join column because exp loses its exposure column (& we want to keep exp and tmrel)
    mrbrt[,join_exp:=exposure_spline]
    dt[,join_exp:=get(exp_cat)]
    
    # set the key variables on which to join (the roll is performed on the last one)
    setkeyv(mrbrt,c("draw","join_exp"))
    setkeyv(dt,c("draw","join_exp"))
    
    # rolling join by nearest exposure
    dt <- mrbrt[dt, roll = "nearest"]
    
    # change name
    setnames(dt,"mrbrt",paste0(cause,"_",exp_cat))
    
    # remove the unnecessary columns
    dt[,exposure_spline:=NULL]
    dt[,join_exp:=NULL]
    
  }
  
  rm(mrbrt)
  
  # scale estimate shifts based on draw of tmrel
  # if exposure is less than tmrel, set to zero

  for(exp_cat in setdiff(exp_cats,"tmrel")){
    dt[,paste0(cause,"_",exp_cat):=min(get(paste0(cause,"_",exp_cat))-get(paste0(cause,"_tmrel")),0),by=1:nrow(dt)]
  }
  
  return(dt)
  
}

bw_dt <- calculate_shift("bw",dt=exp)
ga_dt <- calculate_shift("ga",dt=exp)

exp <- merge(bw_dt,ga_dt,by=intersect(names(bw_dt),names(ga_dt)))

# create overall HAP prop/exposure (solid) for proportional splitting
exp[,"hap_prop":=hap_prop_crop+hap_prop_coal+hap_prop_dung+hap_prop_wood]

# calculate overall hap shift (weighted mean of fuel types)
exp[,"bw_hap":=((bw_hap_coal*hap_prop_coal)+
             (bw_hap_crop*hap_prop_crop)+
             (bw_hap_dung*hap_prop_dung)+
             (bw_hap_wood*hap_prop_wood))/hap_prop]


# calculate average shift based on proportion of pop exposed to hap
exp[,"bw":= bw_hap*hap_prop + bw_ambient*(1-hap_prop)]

# do the same thing for ga
exp[,"ga_hap":=((ga_hap_coal*hap_prop_coal)+
      (ga_hap_crop*hap_prop_crop)+
      (ga_hap_dung*hap_prop_dung)+
      (ga_hap_wood*hap_prop_wood))/hap_prop]
      

exp[,"ga":=ga_hap*hap_prop + ga_ambient*(1-hap_prop)]


# population weight shift, ambient exposure, RR

exp <- merge(exp[,lapply(.SD,weighted.mean,w=pop*weight),.SDcols=c("bw","ga","ambient"), by=c("grouping","draw")], # pop_weighted PM shift to calculate PM pafs, exposure for proportional split
            exp[,c("hap_prop","hap_prop_crop","hap_prop_coal","hap_prop_dung","hap_prop_wood",
                   "hap_excess_crop","hap_excess_coal","hap_excess_dung","hap_excess_wood",
                   "grouping","draw"),with=F] %>% unique, #HAP doesn't differ by gridcell, only by group
            by=c("grouping","draw"))

# generate hap excess through weighted mean
exp[,"hap_excess":=((hap_prop_crop*hap_excess_crop)+
      (hap_prop_coal*hap_excess_coal)+
      (hap_prop_dung*hap_excess_dung)+
      (hap_prop_wood*hap_excess_wood))/hap_prop]

exp[,"pop_average_pm":=ambient+(hap_prop*hap_excess)]  #population average exposure (denominator of proportion for splitting pafs)
exp[,"hap_paf_ratio":=(hap_prop*hap_excess)/pop_average_pm] #proportion of paf attributable to hap
exp[,"ambient_paf_ratio":=ambient/pop_average_pm] #proportion of paf attributable to ambient

exp[,"location_id":=this.country]
exp[,"year_id":=this.year]


#summary file

lower <- function(x){quantile(x,p=.025)}
upper <- function(x){quantile(x,p=.975)}

summary <- melt(exp,id.vars=c("grouping","location_id","year_id","draw"))
summary <- summary[,.(mean=mean(value),lower=lower(value),upper=upper(value)),by=c("grouping","location_id","year_id","variable")]

write.csv(summary,paste0(out.sum.dir,"/",this.country,"_",this.year,".csv"),row.names=F)

# save bw ga shifts
write.csv(exp[,.(location_id,year_id,draw,bw,ga)],paste0(out.dir,"/",this.country,"_",this.year,".csv"),row.names=F)

# save draws of proportional PAF splits for later
write.csv(exp[,.(location_id,year_id,grouping,draw,hap_paf_ratio,ambient_paf_ratio)],paste0(out.prop.dir,"/",this.country,"_",this.year,".csv"),row.names=F)






