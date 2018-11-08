#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: RF: air_pm
# Purpose: Calculate PAFs from air PM for a given country year for cataracts
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())
user <- "name"

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- paste0("FILEPATH", user)
  
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  
  if (length(arg)==0) {
    #toggle for targeted run on cluster
    arg <- c(37, #output version
             1000, #draws required
             1990) #year
  }
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  arg <- c(35, #output version
           1000, #draws required
           1990) #year
}



pacman::p_load(data.table, fst, magrittr, ggplot2, gdata, ini)



# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# set project values
location_set_id <- 22

# Set parameters from input args
output.version <- arg[1]
draws.required <- as.numeric(arg[2])
year <- as.numeric(arg[3])
silly.cols <- paste0("draw_", 0:(draws.required-1))
paf.cols <- paste0("draw_", 0:(draws.required-1)) #must be saved in 0-999 format
rr.cols <- paste0("rr_", 0:(draws.required-1))

#create age sex groups
ages <- c(9:20,30:32,235) #20+
sexes <- 2    #females
cause <- 671  #cataract
cause_name <- "sense_cataract"
measure <- 3   #yld

#Cataracts RR comes from Smith et. al. 2014: 2.47 with 95% (1.61, 3.73).
rr <- 2.47
rr_lower <- 1.61
rr_upper <- 3.73


#***********************************************************************************************************************

#----IN/OUT-------------------------------------------------------------------------------------------------------------

##out##
out.paf.tmp <-  file.path("FILEPATH", output.version)
out.paf.dir <- file.path(home.dir, 'FILEPATH', output.version)

#PAFs
dir.create(file.path(out.paf.dir, "summary"), recursive = T, showWarnings = F)
dir.create(file.path(out.paf.tmp, "draws", "air"), recursive = T, showWarnings = F)
dir.create(file.path(out.paf.tmp, "draws", "air_pm"), recursive = T, showWarnings = F)
dir.create(file.path(out.paf.tmp, "draws", "air_hap"), recursive = T, showWarnings = F)

#***********************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#PAF functions#
paf.function.dir <- file.path(h_root, "FILEPATH")
file.path(paf.function.dir, "paf_helpers.R") %>% source

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
locs <- get_location_metadata(location_set_id)[most_detailed==1,location_id]

#read in hap exposure draws
hap.exp <- get_draws(gbd_id_type = "rei_id",
                     gbd_id=87,
                     source="exposure",
                     year_id=year,
                     location_id=locs,
                     age_group_id=2,
                     sex_id=2)
hap.prop.cols <- paste0("hap_prop_", 1:draws.required)
setnames(hap.exp, silly.cols, hap.prop.cols)

hap.exp <- hap.exp[parameter=="cat1",c("location_id","year_id",hap.prop.cols), with=F]


#generate draws
set.seed(5)
log_rr <- log(rr)
log_rr_sd <- (log(rr_upper)-log(rr_lower))/(2*1.96)
rr.draws <- exp(rnorm(draws.required,mean=log_rr,sd=log_rr_sd)) #assumes log_normal distribution

#save summary file of rr for RR table capstone
# just do this once!

if(year==1990){
  rr_summary <- data.table(upper=rr_upper,
                           lower=rr_lower,
                           location_id=1,
                           sex_id=3,
                           year_id=1990,
                           age_group_id=ages,
                           parameter="cat1",
                           morbidity=1,
                           mortality=0,
                           mean=rr)
  
  write.csv(rr_summary,file.path(j_root,"FILEPATH"),row.names=F)
}

                         
#generate paf predictions based on draws of proportion exposed and RR
hap.exp[, (paf.cols) := lapply(1:draws.required,
                         function(draw) {
                           ((get(hap.prop.cols[draw])*(rr.draws[draw]-1))/(get(hap.prop.cols[draw])*(rr.draws[draw]-1)+1))
                         })]

hap.exp[,(hap.prop.cols):=NULL]

hap.exp[, index := seq_len(.N)] # add index
hap.exp[, draw_lower := quantile(.SD, c(.025)), .SDcols=paf.cols, by="index"]
hap.exp[, draw_mean := rowMeans(.SD), .SDcols=paf.cols, by="index"]
hap.exp[, draw_upper := quantile(.SD, c(.975)), .SDcols=paf.cols, by="index"]


# Save summary version of PAF
summary <- hap.exp[, c("location_id",
                       "year_id",
                       "draw_lower",
                       "draw_mean",
                       "draw_upper"),
                   with=F]
write.csv(summary, paste0(out.paf.dir, "FILEPATH/cataract_paf.csv"), row.names=F)

hap.exp[,index:=NULL]
hap.exp[,merge:=1]

#set risks to be air_hap and air because air_hap does not get aggregated up. 
groups <- CJ(risk = c("air","air_hap"),
             measure_id = measure,
             age_group_id = ages,
             sex_id = sexes,
             acause = cause_name,
             cause_id=cause,
             merge=1)

out.paf <- merge(hap.exp,groups,by="merge",allow.cartesian=T)


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

saveSex <- function(this.sex, this.cause, this.measure, this.risk, this.location, this.year, file){
  
  out <- copy(file)
  
  out <- out[sex_id==this.sex 
             & cause_id==this.cause 
             & measure_id==this.measure
             & risk==this.risk 
             & location_id==this.location
             & year_id== this.year] #subset to the relevant rows
  out <- out[,risk:=NULL]
  
  write.csv(out, paste0(out.paf.tmp, "/draws/", this.risk, "/", 
                        this.measure, "_", this.cause, "_", this.location, "_",
                        this.year, "_", this.sex, ".csv"),
            row.names=F)
}

#PAF save save a file for each sex, cause, yll/yld, air/air_pm/air_hap, location, year
save <- unique(out.paf[,.(sex_id,cause_id,measure_id,risk,location_id,year_id)])
save[,saveSex(sex_id,cause_id,measure_id,risk,location_id,year_id,file=out.paf),by=1:nrow(save)]


#********************************************************************************************
