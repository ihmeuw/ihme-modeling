
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 8/19/2019
# Purpose: Generate RR and PAF from IER
#          
# source("FILEPATH.R", echo=T)
# qsub -N paf_calc_new -l fthread=5 -l m_mem_free=10G -l h_rt=2:00:00 -l archive -q all.q -P ADDRESS -o FILEPATH -e FILEPATH FILEPATH.sh FILEPATH.R
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
  arg <- c("367", #location
           2017, #year
           36, #exp grid version
           "FILEPATH", #ier version
           47, #output version
           1000, #draws required
           102800, #stgpr HAP run_id
           5, # cores
           "092419", #hap map date
           1)  # row of outcome table
}

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","fst","parallel")

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

# set parameters from input args
this.country <- arg[1]
this.year <- arg[2]
exp.grid.version <- arg[3]
ier.version <- arg[4]
output.version <- arg[5]
draws.required <- as.numeric(arg[6])
run_id <- arg[7]
cores <- arg[8]
hap.map.date <- arg[9]
i <- as.numeric(arg[10])

# generate tmrel distribution
set.seed(143)
tmrel.draws <- runif(draws.required,2.4,5.9)

pred.cols <- paste0("pred_",1:draws.required)
draw.cols <- paste0("draw_", 1:draws.required)
format.cols <- paste0("draw_", 0:(draws.required-1)) #formatting
hap.map.cols <- paste0("hap_map_", 1:draws.required)
hap.prop.cols <- paste0("hap_prop_", 1:draws.required)
hap.ratio.cols <- paste0("hap_ratio_",1:draws.required)
ambient.cols <- paste0("ambient_",1:draws.required)
paf.cols <- paste0("draw_", 0:(draws.required-1)) # must be saved in 0-999 format
mr.brt.cols <- paste0("mrbrt_",1:draws.required)

exp_cats <- c("tmrel","ambient","hap")

# #Toggle for running on 3 sample ages
# full_ages <- F
# 
# if(full_ages==F){
#   cvd_ages <- c(25,50,80)
# }else{
#   cvd_ages <- c(seq(25,95,5))
# }

outcomes <- fread(file.path(home.dir,"FILEPATH.csv"))

# Directories -------------------------------------------------------------

# Air PM functions
air.function.dir <- file.path(h_root, 'FILEPATH')
# this pulls the miscellaneous helper functions for air pollution
file.path(air.function.dir, "misc.R") %>% source

# general functions
central.function.dir <- file.path(h_root, "code/tools/")
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source

#mr.brt.dir <- file.path(j_root,"temp/rmbarber/evidence_score/air_pollution_10_02_looser_deriv_prior")
hap.prop.in <- file.path("FILEPATH",run_id,"FILEPATH")
hap.map.in <- file.path(home.dir,"FILEPATH",hap.map.date)
ambient.grid.in <- file.path(home.dir,"FILEPATH",exp.grid.version,"FILEPATH")
pm.sum.out <- file.path(home.dir,"FILEPATH",output.version)
rr.sum.out <- file.path(home.dir,"FILEPATH",output.version)
paf.sum.out <- file.path(home.dir,"FILEPATH",output.version)

status.dir <- file.path(home.dir,"FILEPATH",output.version)

dir.create(pm.sum.out, showWarnings = F)
dir.create(rr.sum.out, showWarnings = F)
dir.create(paf.sum.out, showWarnings = F)

reis <- c("air_pmhap","air_pm","air_hap")

for(rei in reis){
  
  dir.create(file.path(home.dir,rei,"FILEPATH",output.version), recursive = T, showWarnings = F)
    dir.create(file.path(home.dir,rei,"FILEPATH",output.version), recursive = T, showWarnings = F)
}

# Read in data ----------------------------------------------------------------

hap.prop <- fread(paste0(hap.prop.in,"/",this.country,".csv"))[year_id==this.year]
setnames(hap.prop,format.cols,hap.prop.cols)
hap.prop <- hap.prop[,c(hap.prop.cols),with=F]

hap.map <- fread(paste0(hap.map.in,"/lm_pred_",hap.map.date,".csv"))[location_id==this.country & year_id==this.year & grouping!="indoor"]
setnames(hap.map,draw.cols,hap.map.cols)
hap.map <- hap.map[,c("grouping",hap.map.cols),with=F]

# # Ratio now applied earlier and saved in lm_pred file
# hap.ratio <- fread(paste0(hap.map.in,"/crosswalk_",hap.map.date,".csv"))
# setnames(hap.ratio,draw.cols,hap.ratio.cols)
# hap.ratio <- hap.ratio[,c("grouping",hap.ratio.cols),with=F]

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

# hap.ratio <- melt(hap.ratio,id.vars="grouping",measure.vars=hap.ratio.cols,value.name="hap_ratio",variable.name="draw",value.factor=T)
# hap.ratio[,draw:=as.numeric(draw)]

ambient <- melt(ambient,measure.vars=ambient.cols,value.name="ambient",variable.name="draw",value.factor=T)
ambient[,draw:=as.numeric(draw)]

# merge hap datasets together
exp <- merge(hap.prop,hap.map,by="draw")

# calculate total exposure for those exposed to hap
exp[,hap_excess:=hap_map] # hap only exposure for each group (men,women,children) (above ambient levels)

# merge on grid cell ambient
exp <- merge(exp,ambient,by="draw",allow.cartesian=T)

# calculate total exposures
exp[,hap:=hap_excess+ambient] # total exposure for those exposed to hap, used to get value from RR curve

# Add in tmrel
exp[,tmrel:=tmrel.draws[draw]]

  row <- outcomes[i,]
  
  # subset to relevant groups (LRI is the only outcome evaluated in children)
  if(row$group=="adults"){
    exp <- exp[grouping %in% c("female","male")]
  }else if(row$group=="all"){
    exp <- exp[grouping %in% c("female","male","child")]
  }
  
    # mrbrt <- fread(paste0(mr.brt.dir,"/",row$cause,
    #                       ifelse(row$age==99,"",paste0("_",row$age)),
    #                       "_no_prior_mono_cvcv/",row$cause,
    #                       ifelse(row$age==99,"",paste0("_",row$age)),
    #                       "_y_samples_fe.csv"))
  
    mrbrt <- fread(row$filepath)
 
    setnames(mrbrt,format.cols,mr.brt.cols)
  
  # make long by draw
  mrbrt <- melt.data.table(mrbrt,id.vars="exposure_spline", measure.vars=patterns("mrbrt_"), variable.name="draw", variable.factor=T, value.name="mrbrt")
  mrbrt[,draw:=as.numeric(draw)]
  
  mrbrt_exposures <- unique(mrbrt$exposure_spline)
  
  setkeyv(mrbrt,c("draw","exposure_spline"))
  
  # Calculate RR  
  
  for(exp_cat in exp_cats){
    
    # merge on mrbrt predictions based on closest available exposure datapoint predicted for mr_brt & draw
    exp[,paste0("round_",exp_cat):=mrbrt_exposures[which.min(abs(mrbrt_exposures - get(exp_cat)))],by=1:nrow(exp)]
    setkeyv(exp,c("draw",paste0("round_",exp_cat)))
    
    # get mrbrt estimate
    exp <- merge(exp,mrbrt,by.x=c("draw",paste0("round_",exp_cat)),by.y=c("draw","exposure_spline"))
    # change name
    setnames(exp,"mrbrt",paste0("rr_",exp_cat))
  }
  
  rm(mrbrt)
    
    # incidence and mortality, for CVD outcomes scale using predefined ratios. For others, no difference, duplicate draws.
    #
    # we are not doing this before because we included an indicator in MRBRT and there was no significant difference. I'm leaving 
    # the code for posterity
    if(row$est_incidence==1){
      
      if(row$cause=="cvd_ihd"){
        ratio <- 0.141
      }else if(row$cause=="cvd_stroke"){
        ratio <- 0.553
      }
      
      yll <- copy(exp)
      yll[,type:="yll"]
      yld <- copy(exp)
      yld[,type:="yld"]
      
      for(exp_cat in exp_cats){
        yld[,paste0("rr_",exp_cat):=get(paste0("rr_",exp_cat) * ratio - ratio + 1)]
      }
      
      exp <- rbind(yll,yld)
      
    }else if(row$est_incidence==0){
      
      #Assumption that YLLs and YLD RRs are the same
      
      yll <- copy(exp)
      yll[,type:="yll"]
      yld <- copy(exp)
      yld[,type:="yld"]
      
      exp <- rbind(yll,yld)
    }
    
  rm(yll,yld)
  
  # scale RRs based on predicted TMREL RR
  for(exp_cat in setdiff(exp_cats,"tmrel")){
    exp[,paste0("rr_",exp_cat):=max(get(paste0("rr_",exp_cat))/rr_tmrel,1),by=1:nrow(exp)] # dont let it go less than 1, this would happen if exposure < tmrel
  }
  
  #calculate summary PM rr as weighted mean
  exp[,"rr_pm":=rr_hap*hap_prop+rr_ambient*(1-hap_prop)]
  
  #Uses equation given by Steve to recalculate hap RR. Excess risk for the combined exposure minus the excess risk due to ambient plus 1
  #This will be used in RR_max calculation for SEVs
  exp[,"rr_hap":= (rr_hap-1) - (rr_ambient-1) + 1]
  
  # population weight shift, ambient exposure, RR
  exp <- merge(exp[,lapply(.SD,weighted.mean,w=pop*weight),.SDcols=c("rr_hap","rr_ambient","rr_pm","ambient"), by=c("grouping","draw","type")], #pop_weighted PM rr to calculate PM pafs, ambient and hap rrs for SEVs, exposure for proportional split
               exp[,c("hap_prop","hap_excess","grouping","draw"),with=F] %>% unique, #HAP doesn't differ by gridcell, only by group
               by=c("grouping","draw"))
    
  exp[,pop_average_pm:=ambient+hap_prop*hap_excess]  #population average exposure (denominator of proportion for splitting pafs)
  exp[,hap_paf_ratio:=(hap_prop)*(hap_excess)/pop_average_pm] #proportion of paf attributable to hap
  exp[,ambient_paf_ratio:=ambient/pop_average_pm] #proportion of paf attributable to ambient
  
  exp[,paf_pm:=(rr_pm-1)/rr_pm] #calculate PM paf based on weighted PM RR
  exp[,paf_hap:=paf_pm*hap_paf_ratio] #proprotionally split PM paf to Hap and ambient
  exp[,paf_ambient:=paf_pm*ambient_paf_ratio]
  
  exp[,location_id:=this.country]
  exp[,year_id:=this.year]
  exp[,cause:=row$cause]
  exp[,age:=row$age]
  
# if(full_ages==FALSE){
#   #Duplicate ages for ihd and stroke
#   for(i in setdiff(seq(25,95,5),cvd_ages)){
#     nearest_age <- cvd_ages[which.min(abs(cvd_ages-i))]
#     dt <- copy(out[age==nearest_age])
#     dt[,age:=i]
#     out <- rbind(out,dt)
#   }
# }
    
# Create summary files ----------------------------------------------------
  
  #summary file of exposures
  
  lower <- function(x){quantile(x,p=.025)}
  upper <- function(x){quantile(x,p=.975)}
  
  summary <- melt(exp,id.vars=c("grouping","location_id","year_id","draw","type","cause","age"))
  summary <- summary[,.(mean=mean(value),lower=lower(value),upper=upper(value)),by=c("grouping","cause","age","location_id","year_id","type","variable")]
  
  # only do this for one age/cause group
  if(row$cause=="lri"){
    write.csv(summary[variable %in% c("ambient","hap_prop","hap_excess","pop_average_pm","hap_paf_ratio","ambient_paf_ratio"),.(grouping,variable,location_id,year_id,mean,lower,upper)] %>% unique,
              paste0(pm.sum.out,"/",this.country,"_",this.year,".csv"),row.names=F)
  }

  # summary file of RR
  
  write.csv(summary[variable %in% c("rr_hap","rr_ambient","rr_pm")],
            paste0(rr.sum.out,"/",row$cause,"_",row$age,"_",this.country,"_",this.year,".csv"),row.names=F)
  
  # summary file of PAF
  
  write.csv(summary[variable %in% c("paf_pm","paf_hap","paf_ambient")],
            paste0(paf.sum.out,"/",row$cause,"_",row$age,"_",this.country,"_",this.year,".csv"),row.names=F)
  

# Save Draws --------------------------------------------------------------

exp <- exp[,.(location_id,year_id,cause,age,grouping,draw,type,rr_hap,rr_pm,rr_ambient,paf_pm,paf_hap,paf_ambient)]  
  
# reshape long by risk
exp <- melt.data.table(exp, id.vars=c("location_id","year_id","cause","age","grouping","draw","type"))
exp[,c("measure","risk"):=tstrsplit(variable,"_")]
exp[,variable:=NULL]

# reshape wide by draw
exp[,draw:=paste0("draw_",draw)]
exp <- dcast.data.table(exp, location_id + year_id + cause + age + grouping + type + measure + risk ~ draw, value.var="value")
  
setnames(exp,draw.cols,paf.cols)  
exp[, acause := cause]


# expand cvd_stroke to include relevant subcauses in order to prep for merge to YLDs, using your custom find/replace function
# first supply the values you want to find/replace as vectors
old.causes <- c('cvd_stroke')
replacement.causes <- c('cvd_stroke_isch',
                        "cvd_stroke_intracerebral", 
                        "cvd_stroke_subarachnoid")

# then pass to your custom function
exp <- findAndReplace(exp,
                      old.causes,
                      replacement.causes,
                      "acause",
                      "acause",
                      TRUE) #set this option to be true so that rows can be duplicated in the table join (expanding the rows)

# now replace each cause with cause ID
exp[, cause_id := acause] #create the variable
# first supply the values you want to find/replace as vectors
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

# then pass to your custom function
exp <- findAndReplace(exp,
                      cause.codes,
                      cause.ids,
                      "cause_id",
                      "cause_id")

exp <- exp[, c("risk",
               'measure',
               "type",
               "grouping",
               "location_id",
               "year_id",
               "cause",
               "acause",
               "cause_id",
               "age",
               paf.cols),
           with=F]

exp[risk=="ambient",risk:="air_pm"]
exp[risk=="hap",risk:="air_hap"]
exp[risk=="pm",risk:="air_pmhap"]

# RR save a file for each risk (air/air_pm/air_hap), cause 
saveRiskCause <- function(this.risk,this.cause, this.age,file){
  exp <- copy(file)
  
  exp <- exp[cause_id==this.cause 
             & risk==this.risk
             & measure=="rr"] #subset to the relevant rows
  exp[,measure:=NULL]
  
  write.csv(exp, paste0(home.dir,"/",this.risk,"FILEPATH",output.version,"/",
                        this.age,"_",
                        this.cause, "_", 
                        this.country, "_",
                        this.year, ".csv"),
            row.names=F)
}

save <- unique(exp[,.(risk, cause_id, age)])
save[,saveRiskCause(risk,cause_id,age,file=exp),by=1:nrow(save)]

# expand to the appropriate age groups using a custom function
exp <- expandAges(input.table=exp,cause.code=row$cause)

# create necessary variables
exp[type=="yld", measure_id := 3]
exp[type=="yll", measure_id := 4]

exp <- exp[, c("risk",
               'measure',
               "measure_id",
               "age_group_id",
               "sex_id",
               "location_id",
               "year_id",
               "acause",
               "cause_id",
               paf.cols),
           with=F]


saveSex <- function(this.sex, this.cause, this.measure, this.risk, this.age, file){
    
    exp <- copy(file)
    
    exp <- exp[sex_id==this.sex 
               & cause_id==this.cause 
               & measure_id==this.measure
               & risk==this.risk 
               & measure=="paf"
               & age_group_id==this.age] #subset to the relevant rows
    exp <- exp[,risk:=NULL]
    exp <- exp[,measure:=NULL]
    
    write.csv(exp, paste0(home.dir,"/",this.risk,"FILEPATH",output.version, "/", 
                          this.measure, "_", this.cause, "_", this.country, "_",
                          this.year, "_", this.sex,"_",this.age, ".csv"),
              row.names=F)
    
  }
  
  # PAF save save a file for each sex, cause, yll/yld, air/air_pm/air_hap, age
  save <- unique(exp[,.(sex_id,cause_id,measure_id,risk,age_group_id)])
  save[,saveSex(sex_id,cause_id,measure_id,risk,age_group_id,file=exp),by=1:nrow(save)]
  
  # write empty csv to show the job finished.
  write.csv(data.table(),paste0(status.dir,"/",this.country,"_",this.year,"_",i,".csv"),row.names=F)