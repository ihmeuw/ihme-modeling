#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: RF: air_pm
# Purpose: Calculate PAFs from air PM for a given country year, also outputs location level exposure estimates
#           rewriting to calc at country level rather than grid cell level.
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())
user <- "NAME"

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- paste0("FILEPATH", user)
  
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  
  if (length(arg)==0) {
    #toggle for targeted run on cluster
    arg <- c("44783", #location
             1996, #year
             "33", #rr data version
             "power2_simsd_source_priors", #rr model version
             "power2", #rr functional form
             "33", #exposure grid version
             38, #output version
             1000, #draws required
             20) #number of cores to provide to parallel functions
  }
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  arg <- c("122", #location
           2005, #year
           "28", #rr data version
           "power2_simsd_source_phaseII", #rr model version
           "phaseII", #rr functional form
           "28", #exposure grid version
           30, #output version
           1000, #draws required
           20) #number of cores to provide to parallel functions
  
}



pacman::p_load(data.table, fst, magrittr, ggplot2, gdata, ini)


# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# set project values
location_set_id <- 22

# Set parameters from input args
this.country <- arg[1]
this.year <- arg[2]
rr.data.version <- arg[3]
rr.model.version <- arg[4]
rr.functional.form <- arg[5]
exp.grid.version <- arg[6]
output.version <- arg[7]
draws.required <- as.numeric(arg[8])
draw.cols <- paste0("draw_", 1:draws.required)
silly.cols <- paste0("draw_", 0:(draws.required-1)) 
hap.exp.cols <- paste0("hap_exp_", 1:draws.required)
paf.cols <- paste0("draw_", 0:(draws.required-1)) #must be saved in 0-999 format
cores.provided <- as.numeric(arg[9])

#***********************************************************************************************************************

#----IN/OUT-------------------------------------------------------------------------------------------------------------
##in##

exp.dir <-  file.path("FILEPATH", exp.grid.version)
hap.dir <- file.path(j_root, 'FILEPATH')
hap.exp.date <- "071018" 
hap.pm <- paste0(hap.dir, "/lm_pred_", hap.exp.date, '.csv') %>% fread
hap.ratio.date <- "071018" 
hap.ratio <- paste0(hap.dir, "/crosswalk_", hap.ratio.date, '.csv') %>% fread
rr.dir <- file.path(home.dir, 'FILEPATH', paste0(rr.data.version, rr.model.version))
tmrel.dir <- file.path(home.dir, 'FILEPATH')

##out##
out.paf.tmp <-  file.path("FILEPATH", output.version)
out.rr.tmp <- file.path("FILEPATH",output.version)
out.paf.dir <- file.path(home.dir, 'FILEPATH', output.version)
out.rr.dir <- file.path(home.dir,"FILEPATH",output.version)


#PAFs
dir.create(file.path(out.paf.dir, "summary"), recursive = T, showWarnings = F)
dir.create(file.path(out.paf.tmp, "draws", "air"), recursive = T, showWarnings = F)
dir.create(file.path(out.paf.tmp, "draws", "air_pm"), recursive = T, showWarnings = F)
dir.create(file.path(out.paf.tmp, "draws", "air_hap"), recursive = T, showWarnings = F)

#RRs
dir.create(file.path(out.rr.dir, "summary"), recursive = T, showWarnings = F)
dir.create(file.path(out.rr.tmp, "draws", "air"), recursive = T, showWarnings = F)
dir.create(file.path(out.rr.tmp, "draws", "air_pm"), recursive = T, showWarnings = F)
dir.create(file.path(out.rr.tmp, "draws", "air_hap"), recursive = T, showWarnings = F)
#***********************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#PAF functions#
paf.function.dir <- file.path(h_root, 'FILEPATH')
file.path(paf.function.dir, "paf_helpers.R") %>% source

#RR functions#
rr.function.dir <- file.path(h_root, 'FILEPATH')
file.path(rr.function.dir, "functional_forms.R") %>% source
fobject <- get(rr.functional.form)

#AiR PM functions#
air.function.dir <- file.path(h_root, 'FILEPATH')
# this pulls the miscellaneous helper functions for air pollution
file.path(air.function.dir, "misc.R") %>% source

#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source

#ubcov functions#
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH/db_tools.r") %>% source

#shared functions#
file.path(j_root, "FILEPATH/get_draws.R") %>% source
file.path(j_root,"FILEPATH/get_location_metadata.R") %>% source
#***********************************************************************************************************************

#----PREP DATA----------------------------------------------------------------------------------------------------------
# Make a list of all cause-age pairs that we have.
age.cause <- ageCauseLister(full.age.range, gbd.version="GBD2017", lri.version="single")

if(output.version>30 & output.version <36){
  if(rr.functional.form=="phaseII"){age.cause <- as.matrix(t(age.cause[34,]))}
  if(rr.functional.form=="power2"){age.cause <- age.cause[1:33,]}
}

#Get the list of most detailed GBD locations
locs <- get_location_metadata(location_set_id)
if (this.country != "EU" & this.country != "GLOBAL") {
  
  this.iso3 <- locs[location_id == this.country, ihme_loc_id]
  
} else {
  this.iso3 <- this.country
}

# Prep the RR curves into a single object, so that we can loop through different years without hitting the files extra times.
all.rr <- lapply(1:nrow(age.cause), prepRR, rr.dir=rr.dir)

#read in ambient exp data set for each year
if(this.year %in% c(1990,1995,2000,2005,2010:2017)){
  this.exp <- fread(paste0(exp.dir, "/final_draws/", this.country, "_", this.year, ".csv"))}else{
  this.exp <- fread(paste0(exp.dir,"/interpolated_draws/",this.country,".csv"))[year_id==this.year]
}

#rename columns
setnames(this.exp,silly.cols,draw.cols)

#drop unnecessary rows and columns
this.exp <- this.exp[1,c("location_id",draw.cols,"year_id"),with=F]


#also read in relevant estimates for HAP
hap.exp <- get_draws(gbd_id_type = "rei_id",
                     gbd_id=87,
                     source="exposure",
                     location_id=this.country,
                     year_id=this.year,
                     age_group_id=2,
                     sex_id=2)
hap.prop.cols <- paste0("hap_prop_", 1:draws.required)
setnames(hap.exp, silly.cols, hap.prop.cols)

#subset hap pm values to the country year in question
hap.pm <- hap.pm[location_id==this.country & year_id==this.year]
setnames(hap.pm, draw.cols, hap.exp.cols)

#combine the hap exposure with the m/f/c ratio
hap.pm[, merge:= 1]
hap.ratio[, merge:= 1]

hap.pm <- merge(hap.pm[, c(hap.exp.cols, 'merge'), with=F], 
                hap.ratio[, c(draw.cols, 'merge', 'grouping'), with=F],
                by='merge',
                allow.cartesian=TRUE)

#shift hap pm draws using ratio draws
hap.pm[, (hap.exp.cols) := lapply(1:draws.required, function(x) get(hap.exp.cols[x]) * get(draw.cols[x]))]

#now combine hap proportions to hap exposure in pm2.5
hap.exp[, merge:= 1]
hap.exp <- merge(hap.exp[parameter=="cat1", c(hap.prop.cols, 'merge'), with=F], #cat1 = exposed
                 hap.pm[, c(hap.exp.cols, 'merge', 'grouping'), with=F],
                 by='merge')
#***********************************************************************************************************************

#----CALC and SAVE------------------------------------------------------------------------------------------------------
message("calculating PAF and saving results for the year ", this.year)

# Calculate Mortality PAFS using custom function
out.paf.mort <- mclapply(1:nrow(age.cause),
                         FUN=calculatePAFs,
                         exposure.object = this.exp,
                         rr.curves = all.rr,
                         metric.type = "yll",
                         fx.cores = 1,
                         mc.cores = cores.provided) %>% rbindlist


# Calculate Morbidity PAFS using custom function
out.paf.morb <- mclapply(1:nrow(age.cause),
                         FUN=calculatePAFs,
                         exposure.object = this.exp,
                         rr.curves = all.rr,
                         metric.type = "yld",
                         fx.cores = 1,
                         draws.required,
                         mc.cores = cores.provided) %>% rbindlist


#combine the different pafs and then do prep/formatting for their dalynator run
results <- list(out.paf.mort, out.paf.morb) %>% rbindlist
results[, index := NULL] #no longer relevant

out.paf <- results[measure=="paf"]
out.rr <- results[measure=="rr"]

#Order columns to your liking
out.paf <- setcolorder(out.paf, c("risk",
                                  "cause",
                                  "measure",
                                  "age",
                                  "grouping",
                                  "type",
                                  "draw_lower",
                                  "draw_mean",
                                  "draw_upper",
                                  paf.cols))

# Save summary version of PAF and RR output for experts
summary <- out.paf[, c("risk",
                       "cause",
                       "measure",
                       "age",
                       "grouping",
                       "type",
                       "draw_lower",
                       "draw_mean",
                       "draw_upper"),
                   with=F]
summary[grouping=="male",sex_id:=1]
summary[grouping=="female",sex_id:=2]
summary[grouping=="child",sex_id:=3]
write.csv(summary, paste0(out.paf.dir, ".csv"))

#expand to the appropriate age groups using a custom function
out.paf <- lapply(unique(age.cause[,1]), expandAges, input.table = out.paf) %>% rbindlist(use.names=T)

#create necessary variables
out.paf[, location_id := this.country]
out.paf[, year_id := this.year]
out.paf[type=="yld", measure_id := 3]
out.paf[type=="yll", measure_id := 4]
out.paf[, acause := cause]

# expand cvd_stroke to include relevant subcauses in order to prep for merge to YLDs, using your custom find/replace function
# first supply the values you want to find/replace as vectors
old.causes <- c('cvd_stroke')
replacement.causes <- c('cvd_stroke_isch',
                        "cvd_stroke_intracerebral",
                        "cvd_stroke_subarachnoid")


# then pass to your custom function
out.paf <- findAndReplace(out.paf,
                          old.causes,
                          replacement.causes,
                          "acause",
                          "acause",
                          TRUE) #set this option to be true so that rows can be duplicated in the table join (expanding the rows)

# now replace each cause with cause ID
out.paf[, cause_id := acause] #create the variable
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
out.paf <- findAndReplace(out.paf,
                          cause.codes,
                          cause.ids,
                          "cause_id",
                          "cause_id")

out.paf <- out.paf[, c("risk",
                       "measure_id",
                       "age_group_id",
                       "sex_id",
                       "location_id",
                       "year_id",
                       "acause",
                       "cause_id",
                       "draw_mean",
                       paf.cols),
                   with=F]

saveSex <- function(this.sex, this.cause, this.measure, this.risk, file){
  
  out <- copy(file)
  
  out <- out[sex_id==this.sex 
             & cause_id==this.cause 
             & measure_id==this.measure
             & risk==this.risk ] #subset to the relevant rows
  out <- out[,risk:=NULL]
  
  write.csv(out, paste0(out.paf.tmp, "/draws/", this.risk, "/", 
                        this.measure, "_", this.cause, "_", this.country, "_",
                        this.year, "_", this.sex, ".csv"),
            row.names=F)
}

#PAF save save a file for each sex, cause, yll/yld, air/air_pm/air_hap
save <- unique(out.paf[,.(sex_id,cause_id,measure_id,risk)])
save[,saveSex(sex_id,cause_id,measure_id,risk,file=out.paf),by=1:nrow(save)]

#Format and save RR
out.rr[,acause:=cause]
#replaces stroke with 3 sub categories
out.rr <- findAndReplace(out.rr,
                         old.causes,
                         replacement.causes,
                         "acause",
                         "acause",
                         TRUE) #set this option to be true so that rows can be duplicated in the table join (expanding the rows)

# replace each cause with cause ID
out.rr[, cause_id := acause] #create the variable

#replaces cause names with ids
out.rr <- findAndReplace(out.rr,
                         cause.codes,
                         cause.ids,
                         "cause_id",
                         "cause_id")
out.rr[grouping=="child",age:="1"]
out.rr <- out.rr[, c("risk",
                     "grouping",
                     "cause_id",
                     "age",
                     "type",
                     "draw_mean",
                     paf.cols), 
                 with=F]

#RR save a file for each risk (air/air_pm/air_hap), cause 
saveRiskCause <- function(this.risk,this.cause,file){
  out <- copy(file)
  
  out <- out[cause_id==this.cause 
             & risk==this.risk ] #subset to the relevant rows
  
  write.csv(out, paste0(out.rr.tmp, "/draws/", this.risk, "/", 
                        this.cause, "_", this.country, "_",
                        this.year, ".csv"),
            row.names=F)
}

save <- unique(out.rr[,.(risk, cause_id)])
save[,saveRiskCause(risk,cause_id,file=out.rr),by=1:nrow(save)]

#*******************************************************************************************

