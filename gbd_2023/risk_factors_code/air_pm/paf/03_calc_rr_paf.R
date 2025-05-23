
#-------------------Header------------------------------------------------
# Author: 
# Editor: 

# Purpose: Generate RR and PAF from RR curves and exposures

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


arg <- tail(commandArgs(),n=10)

print(paste0("Arguments: ",arg))

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
home.dir.2022 <- file.path("FILEPATH")
setwd(home.dir)

# set project values
# location_set_id <- 22 # SEEMS LIKE THIS WASN'T BEING USED, NOT SURE

# Set parameters from input args
this.country <- arg[1]
this.year <- arg[2]
exp.grid.version <- arg[3]
rr.version <- arg[4]
output.version <- arg[5]
draws.required <- as.numeric(arg[6])
run_id <- arg[7] 
cores <- arg[8]
hap.map.date <- arg[9]
i <- as.numeric(arg[10])


print(paste0("Country: ",this.country))
print(paste0("Year: ",this.year))
print(paste0("Exp Grid Version: ",exp.grid.version))
print(paste0("RR Version: ",rr.version))
print(paste0("Output Version: ",output.version))
print(paste0("Draws Required: ",draws.required))
print(paste0("Run ID: ",run_id))
print(paste0("Cores: ",cores))
print(paste0("HAP Map Date: ",hap.map.date))
print(paste0("i: ",i))



# Generate tmrel distribution
set.seed(143)
tmrel.draws <- runif(draws.required,2.4,5.9)

pred.cols <- paste0("pred_",1:draws.required)
draw.cols <- paste0("draw_", 1:draws.required)
silly.cols <- paste0("draw_", 0:(draws.required-1)) # comes in 0-999 format
hap.map.cols <- paste0("hap_map_", 1:draws.required)
hap.prop.cols <- paste0("hap_prop_", 1:draws.required)
hap.ratio.cols <- paste0("hap_ratio_",1:draws.required)

paf.cols <- paste0("draw_", 0:(draws.required-1)) # must be saved in 0-999 format
mr.brt.cols <- c("exposure",paste0("mrbrt_",1:draws.required))

exp_cats <- c("tmrel","ambient","hap_crop","hap_coal","hap_dung","hap_wood")


#Toggle for running on 3 sample ages
full_ages <- T


outcomes = fread("FILEPATH/air_pollution/air/paf/outcomes_no_ages.csv")


# Warnings for debugging---------------------------------------------------------------
warning(paste0("Location: ",this.country))
warning(paste0("Year: ",this.year))


# Directories & functions -------------------------------------------------------------

# air_pm functions
air.function.dir <- file.path(h_root, '/air_pollution/air/_lib')
# this pulls the miscellaneous helper functions for air pollution
file.path(air.function.dir, "misc.R") %>% source

# general functions
central.function.dir <- file.path(h_root, "/air_pollution/tools/")
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source


# BIG QUESTION IS WILL THE JOBMON SCRIPT STILL PULL FROM THE CORRECT PLACE
hap.prop.in <- file.path("FILEPATH")  

hap.fuel.in <- file.path(home.dir.2022,"air_hap/exp/d_stgpr/squeeze") 
hap.map.in <- file.path(home.dir.2022,"air_hap/map/results/draws",hap.map.date) 


ambient.grid.in <- file.path(paste0("/mnt/share/erf/GBD2022/air_pm/exp/collapse/", exp.grid.version))

pm.sum.out <- file.path(home.dir.2022,"air_pmhap/exp/summary",output.version)
rr.sum.out <- file.path(home.dir.2022,"air_pmhap/rr/summary",output.version)
paf.sum.out <- file.path(home.dir.2022,"air_pmhap/paf/summary",output.version)

status.dir <- file.path(home.dir.2022,"air_pmhap/paf/check_job_status/",output.version)
dir.create(status.dir, showWarnings = F)

dir.create(pm.sum.out, recursive = T, showWarnings = F)
dir.create(rr.sum.out, recursive = T, showWarnings = F)
dir.create(paf.sum.out, recursive = T, showWarnings = F)

reis <- c("air_pmhap","air_pm","air_hap")

for(rei in reis){
  
  dir.create(file.path(home.dir.2022,rei,"paf/draws",output.version), recursive = T, showWarnings = F)
  dir.create(file.path(home.dir.2022,rei,"rr/draws",output.version), recursive = T, showWarnings = F)
}


# Read in exposure data ----------------------------------------------------------------

# format HAP data (HAP disaggregation)
prep_fuel <- function(this.fuel,this.country){
  # load prop data
  hap.prop <- fread(paste0(hap.fuel.in,"/",this.fuel,"/",this.country,".csv"))[year_id==this.year]
  setnames(hap.prop,silly.cols,hap.prop.cols)
  hap.prop <- hap.prop[,c(hap.prop.cols),with=F]
  
  # load map data
  hap.map <- fread(paste0(hap.map.in,"/lm_pred_",this.fuel,"_",hap.map.date,".csv"))[location_id==this.country & year_id==this.year & grouping!="indoor"]
  setnames(hap.map,draw.cols,hap.map.cols)
  hap.map <- hap.map[,c("grouping",hap.map.cols),with=F]
  hap.map <- hap.map[,(hap.map.cols):=lapply(.SD,as.numeric), .SDcols=hap.map.cols]

  # make data long by draw for easier manipulation
  hap.prop <- melt(hap.prop,measure.vars=hap.prop.cols,value.name=paste0("hap_prop_",this.fuel),variable.name="draw",value.factor=T) %>% as.data.table
  hap.prop[,draw:=as.numeric(draw)]
  
  hap.map <- melt(hap.map,id.vars="grouping",measure.vars=hap.map.cols,value.name=paste0("hap_map_",this.fuel),variable.name="draw",value.factor=T) %>% as.data.table
  hap.map[,draw:=as.numeric(draw)]

  # merge hap datasets together
  exp <- merge(hap.prop,hap.map,by="draw")
  
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


# new ambient method (using collapsed gridcells by ug/m3)
ambient <- fread(paste0(ambient.grid.in,"/",this.country,"_",this.year,".csv"))

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

# Read in RR data ----------------------------------------------------------------

  row <- outcomes[i,]
  
  #subset to relevant groups (LRI is the only outcome evaluated in kids)
  if(row$group=="adults"){
    exp <- exp[grouping %in% c("female","male")]
  }else if(row$group=="all"){
    exp <- exp[grouping %in% c("female","male","child")]
  }
  
    mrbrt <- fread(row$filepath)
    exposure_vals <- mrbrt$risk
    mrbrt <- mrbrt[,1:(draws.required+1)]

    setnames(mrbrt,mr.brt.cols)
    mrbrt <- exp(mrbrt)
    mrbrt[,exposure:=NULL]
    mrbrt[,exposure_spline:=exposure_vals] # predicted out new exposures

  
  # make long by draw
  mrbrt <- melt.data.table(mrbrt,id.vars="exposure_spline", measure.vars=patterns("mrbrt_"), variable.name="draw", variable.factor=T, value.name="mrbrt")
  mrbrt[,draw:=as.numeric(draw)]
  
  mrbrt_exposures <- unique(mrbrt$exposure_spline)
  
  setkeyv(mrbrt,c("draw","exposure_spline"))
  
  # Calculate RR
  
  for(exp_cat in exp_cats){
    
    # merge on mrbrt predictions based on closest available exposure datapoint predicted for mr_brt & draw
    # make a join column because exp loses its exposure column (& we want to keep no2 and tmrel)
    mrbrt[,join_exp:=exposure_spline]
    exp[,join_exp:=get(exp_cat)]
    
    # set the key variables on which to join (the roll is performed on the last one)
    setkeyv(mrbrt,c("draw","join_exp"))
    setkeyv(exp,c("draw","join_exp"))
    
    # rolling join by nearest exposure
    exp <- mrbrt[exp, roll = "nearest"]
    
    # change name
    setnames(exp,"mrbrt",paste0("rr_",exp_cat))
    
    # remove the unnecessary columns
    exp[,exposure_spline:=NULL]
    exp[,join_exp:=NULL]
    
  }

  
  rm(mrbrt)
  
      # From GBD 2019 onward we assume that YLLs and YLD RRs are the same
      # In a future cycle maybe we can check this out again:)
      
      yll <- copy(exp)
      yll[,type:="yll"]
      yld <- copy(exp)
      yld[,type:="yld"]
      
      exp <- rbind(yll,yld)
    # }
    
  rm(yll,yld)
  

  # scale RRs based on predicted TMREL RR
  for(exp_cat in setdiff(exp_cats,"tmrel")){
    
    exp[,paste0("rr_",exp_cat):=ifelse(get(paste0(exp_cat))>tmrel,
                                (get(paste0("rr_",exp_cat))/rr_tmrel), 
                                1)]
    
  }
  
  
  # create overall HAP prop/exposure (solid) for proportional splitting
  exp[,"hap_prop":=hap_prop_crop+hap_prop_coal+hap_prop_dung+hap_prop_wood]
  
  # create an overall rr_hap from these four rr (weighted mean)
  exp[,"rr_hap":=((rr_hap_coal*hap_prop_coal)+
                    (rr_hap_crop*hap_prop_crop)+
                    (rr_hap_dung*hap_prop_dung)+
                    (rr_hap_wood*hap_prop_wood))/hap_prop]
  
  # calculate summary PM rr as weighted mean
  exp[,"rr_pm":=rr_hap*hap_prop + rr_ambient*(1-hap_prop)]
  
  # Use equation from Steve to recalculate hap RR. Excess risk for the combined exposure minus the excess risk due to ambient plus 1
  # This will be used in RR_max calculation for SEVs
  # NOTE: we only need this for the overall solid category
  exp[,"rr_hap":= (rr_hap-1) - (rr_ambient-1) + 1]
  
  # create an overall hap_excess from a weighted mean
  exp[,"hap_excess":=((hap_prop_crop*hap_excess_crop)+
                        (hap_prop_coal*hap_excess_coal)+
                        (hap_prop_dung*hap_excess_dung)+
                        (hap_prop_wood*hap_excess_wood))/hap_prop]
  
  # drop NA rows (small issue with 1990 and 1995 data, not sure what is causing these)
  exp <- na.omit(exp)
  
  # population weight shift, ambient exposure, RR
  # exp <- merge(exp[,lapply(.SD,weighted.mean,w=pop*weight),
  exp <- merge(exp[,lapply(.SD,weighted.mean,w=pop_weight), # with new ambient binning method, this is already calculated
  # exp <- merge(exp[,lapply(.SD,weighted.mean,w=pop), # with dimaq rescale, we are now just using gridded pop predictions
                   .SDcols=c("rr_hap_crop","rr_hap_coal","rr_hap_dung","rr_hap_wood","rr_hap","rr_ambient","rr_pm","ambient"),
                   by=c("grouping","draw","type")], # pop_weighted PM rr to calculate PM pafs, ambient and hap rrs for SEVs, exposure for proportional split
               exp[,c("hap_prop","hap_excess","hap_prop_crop","hap_prop_coal","hap_prop_dung","hap_prop_wood",
                      "hap_excess_crop","hap_excess_coal","hap_excess_dung","hap_excess_wood",
                      "grouping","draw"),with=F] %>% unique, #HAP doesn't differ by gridcell, only by group
               by=c("grouping","draw"))
  
  exp[,pop_average_pm:=ambient+(hap_prop*hap_excess)] # population average exposure (denominator of proportion for splitting pafs)
  exp[,hap_paf_ratio:=(hap_prop*hap_excess)/pop_average_pm] # proportion of paf attributable to hap
  exp[,ambient_paf_ratio:=ambient/pop_average_pm] # proportion of paf attributable to ambient
  
  exp[,paf_pm:=(rr_pm-1)/rr_pm] # calculate PM paf based on weighted PM RR
  exp[,paf_hap:=paf_pm*hap_paf_ratio] # proprotionally split PM paf to Hap and ambient
  exp[,paf_ambient:=paf_pm*ambient_paf_ratio]
  
  exp[,location_id:=this.country]
  exp[,year_id:=this.year]
  exp[,cause:=row$cause]
  exp[,age:=row$age]
    
    
  #summary file of exposures
  
  lower <- function(x){quantile(x,p=.025, na.rm = TRUE)} # na.rm added for 1990 and 1995 data issues
  upper <- function(x){quantile(x,p=.975, na.rm = TRUE)}
  
  summary <- melt(exp,id.vars=c("grouping","location_id","year_id","draw","type","cause","age")) %>% data.table
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
exp <- dcast.data.table(exp, location_id + year_id + cause + age + grouping + type + measure + risk ~ draw, value.var="value") %>% as.data.table

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
                      TRUE) # set this option to be true so that rows can be duplicated in the table join (expanding the rows)

# now replace each cause with cause ID
exp[, cause_id := acause] # create the variable
# first supply the values you want to find/replace as vectors
cause.codes <- c('cvd_ihd',
                 "cvd_stroke_isch",
                 "cvd_stroke_intracerebral",
                 "cvd_stroke_subarachnoid",
                 "lri",
                 'neo_lung',
                 'resp_copd',
                 'diabetes_typ2', # 't2_dm', # is this now diabetes_typ2?
                 'neuro_dementia')

cause.ids <- c(493,
               495,
               496,
               497,
               322,
               426,
               509,
               976,
               543)

# then pass to your custom function
exp <- findAndReplace(exp,
                      cause.codes,
                      cause.ids,
                      "cause_id",
                      "cause_id",
                      FALSE)

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

# if there are unexpected values less than 0 or greater than 1...
exp <- as.data.frame(exp)
exp <- exp %>%
  dplyr::mutate(across(starts_with("draw_"), ~pmax(pmin(.x, 1), 0)))
exp <- as.data.table(exp)

# RR save a file for each risk (air/air_pm/air_hap), cause 
saveRiskCause <- function(this.risk,this.cause, this.age,file){
  temp <- copy(file)
  
  temp <- temp[cause_id==this.cause 
             & risk==this.risk
             & measure=="rr"] #subset to the relevant rows
  temp[,measure:=NULL]
  
  write.csv(temp, paste0(home.dir.2022,"/",this.risk,"/rr/draws/",output.version,"/",
                        this.age,"_",
                        this.cause, "_", 
                        this.country, "_",
                        this.year, ".csv"),
            row.names=F)
}

save_dt <- unique(exp[,.(risk, cause_id, age)])
save_dt[,saveRiskCause(risk,cause_id,age,file=exp),by=1:nrow(save_dt)]

# expand to the appropriate age groups using a custom function
# exp <- expandAges(input.table=exp,cause.code=row$cause,gbd.version = "GBD2020")
exp <- expandAges(input.table=exp,cause.code=row$cause,gbd.version = "GBD2022")

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

    temp <- copy(file)

    temp <- temp[sex_id==this.sex
               & cause_id==this.cause
               & measure_id==this.measure
               & risk==this.risk
               & measure=="paf"
               & age_group_id==this.age] #subset to the relevant rows
    temp <- temp[,risk:=NULL]
    temp <- temp[,measure:=NULL]
    
    if(nrow(temp)==0){
      print("Attempted to save an empty file...")
      quit(status = 1)
    }
    
    # check

    write.csv(temp, paste0(home.dir.2022,"/",this.risk,"/paf/draws/",output.version, "/",
                          this.measure, "_", this.cause, "_", this.country, "_",
                          this.year, "_", this.sex,"_",this.age, ".csv"),
              row.names=F)

  }

  # PAF save save a file for each sex, cause, yll/yld, air/air_pm/air_hap, age
  save_dt <- unique(exp[,.(sex_id,cause_id,measure_id,risk,age_group_id)])
  save_dt[,saveSex(sex_id,cause_id,measure_id,risk,age_group_id,file=exp),by=1:nrow(save_dt)]
  
  # write empty csv to show the job finished.
  write.csv(data.table(),paste0(status.dir,"/",this.country,"_",this.year,"_",i,".csv"),row.names=F)

  

