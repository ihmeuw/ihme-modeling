#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Create a clean environment to calc ozone PAFs, sample distributions to preserve covariance across parallel
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  arg <- c(1000) #toggle for targeted run
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- c(1000)
}

# Set parameters
draws.required <- arg[1]
years <- c(1990,1995,2000,2005,2010:2017)
output.version <- 15
out.rr.dir <- file.path("FILEPATH", output.version)

# load packages, install if missing
pacman::p_load(data.table, gdata, magrittr,ini,meta,grid)

"%ni%" <- Negate("%in%") # create a reverse %in% operator
#********************************************************************************************************************************
 
#----IN&OUT----------------------------------------------------------------------------------------------------------------------
###Input###
#N/A

###Output###
# clean environment with all necessary objects for the analysis
out.environment <- file.path("FILEPATH/clean.Rdata")
#objects kept:
#tmred - draws of the TMRED distribution
#rr.draws - draws of the RR distribution
#********************************************************************************************************************************
 
#----PREP------------------------------------------------------------------------------------------------------------------------
# generate draws of tmred

#Update for GBD 2017: 0 and 5% of (Aug-Sept) ozone from updated ACS II study
tmred <- data.frame(tmred=runif(draws.required, 29.1, 35.7))

# ozone meta analysis
      data <- fread("FILEPATH/ozone_metareg.csv")
      
      # convert all to ppb
      # using US EPA conversion factor of 0.51 * micrograms/m^3 = ppb
      
      data[unit=="microg/m^3",number:=number*.51]
      
      #converting every RR to per 10 ppb
      data[,rr_shift:=RR^(10/number)]
      data[,rr_lower_shift:=RRLower^(10/number)]
      data[,rr_upper_shift:=RRUpper^(10/number)]
      
      # if we have the upper and lower, we can just log those and convert to log SE, which avoids delta approximating
      data[, log_se := (log(rr_upper_shift)-log(rr_lower_shift))/(qnorm(0.975)*2)]
      data[, log_var := log_se^2]
      
      # then generate weights using inverse variance
      data[, weight := 1/(log_var)]
      
      #take the log of the risk
      data[,log_rr:=log(rr_shift)]
      data[,log_rr_lower:=log(rr_lower_shift)]
      data[,log_rr_upper:=log(rr_upper_shift)]
      
      #adds something to notes so it is not NA
      data[is.na(notes),notes:="none"]
      
      #save to pdf
      pdf("FILEPATH", width=11, height=8.5)
      
      data <- data[meta_analysis==1]
      
      title <- "Ozone and COPD meta-analysis"
      
      # perform meta analysis
      meta<-metagen(TE=data$log_rr,
                    seTE=data$log_se,
                    studlab=paste(Citation,Cohort),
                    data=data,
                    title=title,
                    sm="RR",
                    level=.95,
                    comb.random=T,
                    comb.fixed=F)
      
      
      forest<-forest(meta,col.inside="black")
      grid.text(title,.5,.775,gp=gpar(cex=2))
      
      dev.off()

#RR pulled from meta analysis
rr.mean <- exp(meta$TE.random)
rr.lower <- exp(meta$lower.random)
rr.upper <- exp(meta$upper.random)

log.rr.mean <- meta$TE.random
log.rr.se <- meta$seTE.random

rr.draws <- exp(rnorm(draws.required, log.rr.mean , log.rr.se))

#-----------------------------------SAVE RR draws for upload later--------------------------------------------
# Set up variables
out.rr <- data.table(cause_id = 509,
                     age_group_id = 99,
                     year_id = 1990,
                     location_id = 1,
                     sex_id =3,
                     parameter="per unit",
                     mortality=1,
                     morbidity=0)

#add draw columns
rr.draws.required <- 1000
rr.draw.colnames <- c(paste0("rr_", 0:(rr.draws.required-1)))
out.rr[,c(rr.draw.colnames) := as.list(rr.draws)]

# Convert from age 99 to the correct ages
for (cause.code in c(509)) {
  # Take out this cause
  temp.rr <- out.rr[out.rr$cause_id == cause.code, ]
  out.rr <- out.rr[!out.rr$cause_id == cause.code, ]
  
  # Add back in with proper ages (need to use age.id instead of age number)
  if (cause.code %in% c(509)) ages <-  c(10:20, 30:32, 235) # resp_copd are between 25 and 80
  
  for (age.code in ages) {
    temp.rr$age_group_id <- age.code
    out.rr <- rbind(out.rr, temp.rr)
  }
}

#duplicate sex
for (cause.code in c(509)) {
  # Take out this cause
  temp.rr <- out.rr[out.rr$cause_id == cause.code, ]
  out.rr <- out.rr[!out.rr$cause_id == cause.code, ]
  
  # Add back in with proper sexes
  if (cause.code %in% c(509)) sexes <- c(1,2)
  
  for (sex.code in sexes) {
    temp.rr$sex_id <- sex.code
    out.rr <- rbind(out.rr, temp.rr)
  }
}

#Add in years
for (cause.code in c(509)) {
  # Take out this cause
  temp.rr <- out.rr[out.rr$cause_id == cause.code, ]
  out.rr <- out.rr[!out.rr$cause_id == cause.code, ]
  
  # Add back in with proper sexes
  if (cause.code %in% c(509)) 
    
    for (year.code in years) {
      temp.rr$year_id <- year.code
      out.rr <- rbind(out.rr, temp.rr)
    }
}

write.csv(out.rr,file=file.path(out.rr.dir,"draws.csv"))
#********************************************************************************************************************************



#********************************************************************************************************************************
  
#----SAVE------------------------------------------------------------------------------------------------------------------------
# clean up environment (removing intermediate steps: keep only objects necessary to running 02_calc.R)
keep(tmred, #draws the of the TMRED
     rr.draws,
     out.environment,
     sure=T) #draws of the RR


# detach the gdata function, as it is pesky and masks other functions that i may want to use later. i only need it for the above keep() call
detach(package:gdata)

# output your clean, prepped environment for parallelized calculation files to run in
save(list=ls(), file=out.environment)
#********************************************************************************************************************************
