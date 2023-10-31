#-------------------Header------------------------------------------------
# GBD 2020 - SECONHAND SMOKE - PREPARING FOR PAF CALCULATION
# Purpose: PAF STEP 1: Create contry-year specific rr values for PAf calculation
# Inputs:   1. Calculated exposure to PM2.5 from cigarettes smoked for each location year;
#           2. Outcome-specific risk curves from Mrbrt (continuous and dichotomous)
# Outputs:  1. Summary rr (mean, lower, upper) by location-year-cause
#           2. Location-year draws formatted for PAF calculation
# 
#------------------Set-up--------------------------------------------------
rm(list=ls())

#----CONFIG-------------------------------------------------------------------------------------------------------------
### qsub args
args <- commandArgs()[-(1:3)]
print(args)
hh <- args[1]
print(hh)
slots <- as.numeric(args[2])
print(slots)

### runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

### set scientific notation
options(scipen=15)

### load packages
pacman::p_load(dplyr, tidyr, plyr, magrittr, data.table)

### load functions
file.path("FILEPATH") %>% source
locations <- get_location_metadata(gbd_round_id=7, location_set_id=22)
file.path("FILEPATH") %>% source

### set directory
home <- file.path("FILEPATH")

#----------------------------------------------------------------------------------------------------------------------

# Set parameters from input args
this.location <- commandArgs(trailingOnly = T)[1]
print(this.location)
this.year <- commandArgs(trailingOnly = T)[2]
print(this.year)
#exp.version <- arg[3]
rr.version <- commandArgs(trailingOnly = T)[3]
print(rr.version)
otitis.version <- commandArgs(trailingOnly = T)[4]
print(otitis.version)
bcancer.version <- commandArgs(trailingOnly = T)[5]
print(bcancer.version)
output.version <- commandArgs(trailingOnly = T)[6]
print(output.version)
draws.required <- as.numeric(commandArgs(trailingOnly = T)[7])
print(draws.required)
cores <- commandArgs(trailingOnly = T)[8]
print(cores)

# Functions
paf.function.dir <- file.path(home.dir, "FILEPATH")
file.path(paf.function.dir, "_paf_functions.R") %>% source

# Directories

#Input
exposure.dir <- file.path(home.dir, "FILEPATH") #Continuous PM2.5 exposure
mrbrt.dir <- file.path(home.dir, "FILEPATH", paste0(rr.version), "FILEPATH") #Estimated RRs - continuous
cat.dir <- file.path(home.dir,"FILEPATH") #Estimated RRs - dichotomous folder
bcancer.draws <- paste0(cat.dir, "FILEPATH", bcancer.version, ".csv") #breast cancer RRs                   
otitis.draws <- paste0(cat.dir, "FILEPATH", otitis.version,".csv")   #otitis media RRs

#Output
rr.sum.out <- file.path(home.dir, "paf/data", paste0(output.version), "summary")
dir.create(rr.sum.out, recursive = T, showWarnings = F)
output.dir <- file.path(home.dir, "paf/data", paste0(output.version), "draws")
dir.create(output.dir, recursive = T, showWarnings = F)

#Draw columns 
silly.cols <- paste0("draw_", 0:(draws.required-1)) # Some inputs come in 0-999 format
draw.cols <- paste0("draw_", 1:draws.required)
mr.brt.cols <- paste0("mrbrt_",1:draws.required)

###################################################
################# READ-IN EXPOSURE ################
###################################################

## Format PM2.5 exposure data 
prep_exp <- function(this.location){
  # load prop data
  pm.exp <- fread(paste0(exposure.dir,"/",this.location,".csv"))[year_id==this.year]
  setnames(pm.exp,silly.cols,draw.cols) #From draw 0:999 to 1:1000
  pm.exp <- pm.exp[,c(draw.cols),with=F]

  pm.exp <- melt(pm.exp,measure.vars=draw.cols,variable.name="draw", value.name=paste0("pm_exposure"), value.factor=T)
  pm.exp[,draw:=as.numeric(draw)]
  
  return(pm.exp)
}

exp <- prep_exp(this.location)

###################################################
############### READ-IN MRBRT CURVES ##############
###################################################

## Create data set to store data
rr_output <- data.table()

## Read in curves for each of the causes and rbind  
some_causes <- c("cvd_ihd", "cvd_stroke", "neo_lung", "resp_copd", "lri", "t2_dm") #Only outcomes with continuous curves


for (cause in some_causes) {
  print(cause)
  if (cause %in% c("cvd_ihd", "cvd_stroke")){
    mrbrt_cvd <- fread(paste0(mrbrt.dir, "/", cause, "_ages_new.csv")) 
    cvd_ages <- unique(mrbrt_cvd$age_group_id)
    for (age in cvd_ages) {
      mrbrt <- mrbrt_cvd[age_group_id == age,]
      mrbrt$age_group_id <-NULL
      
      setnames(mrbrt,draw.cols,mr.brt.cols) 
      exposures <- mrbrt$exposure
      mrbrt$exposure <-NULL
      mrbrt <- exp(mrbrt) #Exponentiate rr draws 
      mrbrt <- cbind(exposures, mrbrt)
      
      ## Make draws long for easier merging
      mrbrt <- melt.data.table(mrbrt,id.vars="exposures", measure.vars=patterns("mrbrt_"), variable.name="draw", variable.factor=T, value.name="mrbrt")
      
      ## Keep unique exposures across all exposure draws 
      mrbrt_exposures <- unique(mrbrt$exposures)
      mrbrt$draw <- as.numeric(mrbrt$draw)
      setkeyv(mrbrt,c("draw","exposures"))
      
      ########################################################
      #################### CALC RRs CVDs #####################
      ########################################################
      
      ## Merge on mrbrt predictions based on closest available exposure datapoint predicted for mr_brt & draw
      cause_dt <- copy(exp)
      cause_dt[,paste0("round_pm_exposure"):=mrbrt_exposures[which.min(abs(mrbrt_exposures - get("pm_exposure")))],by=1:nrow(exp)]
      setkeyv(cause_dt,c("draw", "pm_exposure"))
        
      # Merge to get mrbrt rr estimate for this.locations & this.year & cause
      cause_dt <- merge(cause_dt,mrbrt,by.x=c("draw","round_pm_exposure"),by.y=c("draw","exposures"))
        
      # Change name
      setnames(cause_dt,"mrbrt",paste0("rr_pm_exposure"))
      
      rm(mrbrt)
      cause_dt[,location_id:=this.location]
      cause_dt[,year_id:=this.year]
      cause_dt[,cause:=paste0(cause)]
      cause_dt[,age:=age]
      cause_dt <- cause_dt[,.(location_id,year_id,cause, age, draw,rr_pm_exposure)]
      rr_output <- rbind(rr_output, cause_dt, fill = T)
    }
    rm(mrbrt_cvd)
  } else {
   
    mrbrt <- fread(paste0(mrbrt.dir, "/", cause, ".csv"))
    setnames(mrbrt,draw.cols,mr.brt.cols) #New format comes in 1-1000 already
    exposures <- mrbrt$exposure
    mrbrt$exposure <-NULL
    mrbrt <- exp(mrbrt) #Exponentiate rr draws 
    mrbrt <- cbind(exposures, mrbrt)
    
    ## Make draws long for easier merging
    mrbrt <- melt.data.table(mrbrt,id.vars="exposures", measure.vars=patterns("mrbrt_"), variable.name="draw", variable.factor=T, value.name="mrbrt")
    
    ## Keep unique exposures across all exposure draws 
    mrbrt_exposures <- unique(mrbrt$exposures)
    mrbrt$draw <- as.numeric(mrbrt$draw)
    setkeyv(mrbrt,c("draw","exposures"))
    
    ##########################################################
    #################### CALC RRs Others #####################
    ##########################################################
    
    ## Merge on mrbrt predictions based on closest available exposure datapoint predicted for mr_brt & draw
    exp_cats <- c("tmrel","pm_exposure")
    cause_dt <- copy(exp)
    cause_dt[,paste0("round_pm_exposure"):=mrbrt_exposures[which.min(abs(mrbrt_exposures - get("pm_exposure")))],by=1:nrow(exp)]
    setkeyv(cause_dt,c("draw", "pm_exposure"))
    
    #Merge to get mrbrt rr estimate for this.locations & this.year & cause
    cause_dt <- merge(cause_dt,mrbrt,by.x=c("draw","round_pm_exposure"),by.y=c("draw","exposures"))
    
    #Change name
    setnames(cause_dt,"mrbrt",paste0("rr_pm_exposure"))
    
    rm(mrbrt)
    cause_dt[,location_id:=this.location]
    cause_dt[,year_id:=this.year]
    cause_dt[,cause:=paste0(cause)]
    cause_dt[,age:=99]
    cause_dt <- cause_dt[,.(location_id,year_id,cause, age, draw,rr_pm_exposure)]
    rr_output <- rbind(rr_output, cause_dt, fill = T)
 }
}
    

## Now, read-in rr draws from dichotomous risk-outcomes pairs only: otitis & neo_breast 
# Otitis, format before rbinding
otitis <- fread(otitis.draws)
#otitis$V1  <- NULL
setnames(otitis,"draw", "rr_pm_exposure")
setnames(otitis,"V1", "draw")
otitis$rr_pm_exposure <- exp(otitis$rr_pm_exposure)
#setnames(otitis,draw.cols)
otitis$cause <- "otitis"

# Breast Cancer, format before rbinding
neo_breast <- fread(bcancer.draws)
#neo_breast$V1  <- NULL
setnames(neo_breast,"draw", "rr_pm_exposure")
setnames(neo_breast,"V1", "draw")
neo_breast$rr_pm_exposure <- exp(neo_breast$rr_pm_exposure)
#setnames(neo_breast,draw.cols)
neo_breast$cause <- "neo_breast"

# Rbibd
cat_causes <- rbind(otitis, neo_breast)
rm(otitis)
rm(neo_breast)

# Melt and format
#cat_causes <- melt(cat_causes,measure.vars=draw.cols,variable.name="draw", value.name=paste0("rr_pm_exposure"), value.factor=T)
cat_causes[,draw:=as.numeric(draw)]
cat_causes[,location_id:=this.location]
cat_causes[,year_id:=this.year]
cat_causes[,age:=99]


# Rbind with all other causes. One data frame with all causes  
cat_causes <- cat_causes[,.(location_id,year_id,cause, age, draw,rr_pm_exposure)]
cat_causes <- cat_causes[order(cause, draw),]
rr_output <- rbind(rr_output, cat_causes, fill = T)
rm(cat_causes)

#We assume that YLLs and YLD RRs are the same
yll <- copy(rr_output)
yll[,metric:="yll"]
yld <- copy(rr_output)
yld[,metric:="yld"]
rr_output <- rbind(yll,yld)
rm(yll,yld)

###################################################
############### SUMMARIZING RRs ###################
###################################################

## Summary rr
lower <- function(x){quantile(x,p=.025)}
upper <- function(x){quantile(x,p=.975)}

summary <- melt(rr_output,id.vars=c("location_id","year_id","draw","metric","cause", "age"))
summary <- summary[,.(mean=mean(value),lower=lower(value),upper=upper(value)),by=c("cause", "age","location_id","year_id","metric","variable")]

summary <- summary[variable %in% c("rr_pm_exposure")]
summary$variable <- "rr"

## Save summary rr. One file per location & year
write.csv(summary,
          paste0(rr.sum.out,"/rr_",this.location,"_",this.year,".csv"),row.names=F)


###################################################
################# SAVING DRAWS ####################
###################################################

## Preparig draws according to GBD round requirements 
rr_output <- rr_output[,.(location_id,year_id,cause,age,draw,metric,rr_pm_exposure)]

# Reshape wide by draw
rr_output[,draw:=paste0("draw_",draw)]
rr_output <- dcast.data.table(rr_output, location_id + year_id + cause + age + metric ~ draw, value.var="rr_pm_exposure")

# Replacing and expanding dt
# Expand cvd_stroke to include relevant subcauses in order to prep for merge to YLDs, using your custom find/replace function
rr_output[, acause := cause]

# First supply the values you want to find/replace as vectors
old.causes <- c('cvd_stroke')
replacement.causes <- c('cvd_stroke_isch',
                        "cvd_stroke_intracerebral", 
                        "cvd_stroke_subarachnoid")

# Then pass to your custom function
rr_output <- findAndReplace(rr_output,
                            old.causes,
                            replacement.causes,
                            "acause",
                            "acause",
                            TRUE) # set this option to be true so that rows can be duplicated in the table join (expanding the rows)


# Now replace each cause with cause ID
rr_output[, cause_id := acause] # create the variable
# first supply the values you want to find/replace as vectors
cause.codes <- c('cvd_ihd',
                 "cvd_stroke_isch",
                 "cvd_stroke_intracerebral",
                 "cvd_stroke_subarachnoid",
                 "lri",
                 'neo_lung',
                 'resp_copd',
                 't2_dm',
                 "otitis",
                 "neo_breast")


cause.ids <- c(493,
               495,
               496,
               497,
               322,
               426,
               509,
               976,
               329,
               429)

# Then pass to your custom function
rr_output <- findAndReplace(rr_output,
                            cause.codes,
                            cause.ids,
                            "cause_id",
                            "cause_id",
                            FALSE)


# Expand to the appropriate age groups using a custom function
rr_output <- lapply(c(some_causes, "otitis", "neo_breast"), expandAges, input.table = rr_output) %>% rbindlist(use.names=T) # Functon needs to change if age_group_ids change between GBD rounds

# Create parameters column for exposure (cat1) 
rr_output[, parameter := "cat1"]

# Finally, create a version for the tmrel (cat2 - highest category). For this risk all RR values for each risk will just be 1
rr_tmrel <- copy(rr_output)
rr_tmrel[, c(draw.cols) := 1]
rr_tmrel[, parameter := "cat2"]

# Rbind rr & tmrel dts
rr_output <- rbind(rr_output, rr_tmrel)

# Note the metric in the required manner
rr_output[, mortality := ifelse(metric=="yll", 1, 0)]
rr_output[, morbidity := ifelse(metric=="yld", 1, 0)]

setkeyv(rr_output, c('cause_id', "parameter"))
rr_output <- rr_output[, c("modelable_entity_id", #"risk",
                           #"age",
                           #"iso3",
                           "location_id",
                           "year_id",
                           "age_group_id",
                           #"acause",
                           "cause_id",
                           "mortality",
                           "morbidity",
                           "parameter",
                           "rei_id",
                           "metric_id",
                           c(draw.cols)), with=FALSE]

setnames(rr_output,draw.cols,silly.cols)
#save one file for each sex
for (sex.id in c(1,2)) {
  
  rr_output[, sex_id := sex.id]
  write.csv(rr_output, 
            paste0(output.dir, "/rr_", 
                   this.location, "_", this.year, "_", sex.id, ".csv"), row.names=FALSE)
  
}


