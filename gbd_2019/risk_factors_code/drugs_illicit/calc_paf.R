################################
# Compute IDU Hep PAF          #
################################

# source("/share/cc_resources/libraries/current/r/get_location_metadata.R")
# 
# locs<-get_location_metadata(location_set_id = 22, gbd_round_id = 6)
# loc_list<-unique(locs$location_id[locs$level>=3])
# 
# project<-"USERNAME"
# 
# 
# for(loc in  loc_list) {
#     command <- paste0("qsub -l m_mem_free=20G -l fthread=4 -q all.q -P ", project, " -e FILEPATH -N loc_", loc," FILEPATH FILEPATH " ,loc)
#     system(command)
# }

#############################################
# Source packages and initial user settings #
#############################################

library(plyr)
library(reshape2)
library(data.table)
source("FILEPATH")

#####################
# Read in arguments #
#####################

# Read the location as an argument
args <- commandArgs(trailingOnly = TRUE)
location <- ifelse(!is.na(args[1]),args[1], 7) # take location_id
ages      <- c(8:20, 30:32, 235)
sexes     <- c(1,2)
years     <- 1960:2019
gbd_years <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)

IDU_exposure     <- 16436
hepB_incidence <- c(1653, 1654, 2944)
hepC_incidence <- c(1657, 1658, 2945)

#Set up variable to easily access draws
draws <- c(paste0("draw_",seq(0,999)))
pafs  <- c(paste0("paf_", seq(0,999)))

#####################################################################
# Pull dismod models & format models to match requirements for PAF  #
#####################################################################

#Bring together the three hep models and sum across those categories
sum_hep <- function(models){
  hep <- data.frame()
  for (model in models){
    hep <- rbind(hep, read.csv(paste0("FILEPATH", location, "_", model,".csv")))
  }
  hep <- ddply(hep, .(year_id, sex_id, age_group_id, location_id), numcolwise(sum))
  hep <- hep[, c(draws, "year_id", "sex_id","age_group_id","location_id")]
  hep <- hep[(hep$age_group_id %in% ages),]
  return(hep)
}

#Load data, only hold onto ages we estimate
IDU_prevalence<-fread(paste0("FILEPATH", location, ".csv"))
IDU_prevalence<-IDU_prevalence[, c("metric_id", "measure_id", "model_version_id", "modelable_entity_id"):=NULL]
#IDU_prevalence <- read.csv(paste0(temp_folder, location, "_", IDU_exposure,".csv"))
IDU_prevalence <- IDU_prevalence[(IDU_prevalence$age_group_id %in% ages),]

hepB_incidence <- sum_hep(hepB_incidence)
hepC_incidence <- sum_hep(hepC_incidence)

#For cumulative exposure, we need estimates from 1960-2019. So we'll first make best guesses for prevalence based on the predicted
#beta for the year covariate in the dismod model for IDU prevalence (e.g. To get prevalence in 1985, multiply 1990 prevalence
#by inv_log(5*transformed year_hat)

#Make template for desired results
template <- expand.grid(
  "age_group_id" = unique(IDU_prevalence$age_group_id),
  "sex_id" = unique(IDU_prevalence$sex_id),
  "year_id"=years,
  "location_id" = unique(IDU_prevalence$location_id)
)

#Sample from beta obtained in dismod for year covariate
beta    <- 0.018 
beta_se <- (0.019-0.016)/(2*qnorm(.975))
#beta    <- 0.049
#beta_se <- (0.082-0.017)/(2*qnorm(.975))
beta    <- data.table(draw=draws, beta=rnorm(1000, mean=beta, sd=beta_se))

#Merge and reshape datasets to make it easier to use
ideal_datashape <- function(dataset){
  
  #When we reshape, name the new column we create the same thing as the dataset's name
  name_of_draws <- deparse(substitute(dataset))
  
  dataset <- join(x=template, y=dataset, by=c("year_id", "age_group_id", "sex_id"))
  dataset <- melt(dataset,
                  id.vars = c("year_id", "sex_id", "age_group_id", "location_id"),
                  measure.vars = draws,
                  value.name = name_of_draws,
                  variable.name = "draw")
  return(dataset)
}

#Join all of the datasets together to start interpolating and extrapolating.
final <- join(ideal_datashape(IDU_prevalence), beta, by="draw")
final <- join(final, ideal_datashape(hepB_incidence), by=c("year_id", "age_group_id", "sex_id", "location_id", "draw"))
final <- join(final, ideal_datashape(hepC_incidence), by=c("year_id", "age_group_id", "sex_id", "location_id", "draw"))

#Dataset is starting to get large so switch to data.table from here on out.
final <- data.table(final)

#Transform betas to backcast prevalence. Backcasted prevalence = (prevalence in 1990)*exp(-beta*time since 1990)
final[, beta := exp(-1*beta*(1990-year_id))]

prev_1990 <- final[(year_id==1990), IDU_prevalence, by=c("age_group_id", "sex_id", "draw")]
setnames(prev_1990, "IDU_prevalence", "prev_1990")

final <- join(final, prev_1990, by=c("age_group_id", "sex_id", "draw"))
final[year_id<=1989,IDU_prevalence:=prev_1990*beta]

#Interpolate for missing values in prevalence & incidence between 1990-2016 
inter_years <- c(1990:2019)

final[(year_id %in% inter_years), 
      `:=`(IDU_prevalence = approx(year_id, IDU_prevalence, xout = inter_years)$y, 
           hepB_incidence = approx(year_id, hepB_incidence, xout = inter_years)$y, 
           hepC_incidence = approx(year_id, hepC_incidence, xout = inter_years)$y), 
      by=c("age_group_id","sex_id","draw")]

#Backcast hepB and hepC incidence 
incidence_1990 <- final[(year_id==1990), .(hepB_incidence, hepC_incidence), by=c("age_group_id", "sex_id", "draw")]
setnames(incidence_1990, c("hepB_incidence", "hepC_incidence"), c("prev_hepB", "prev_hepC"))

final <- join(final, incidence_1990, by=c("age_group_id", "sex_id", "draw"))
final[(year_id<1990), `:=`(hepB_incidence = prev_hepB,
                           hepC_incidence = prev_hepC),
      by=c("age_group_id","sex_id","draw")]

#Clean dataframe a bit to help with memory usage and clarity
final[, c("beta", "prev_1990", "prev_hepB", "prev_hepC") := NULL]

#################
# Calculate PAF #
#################

#Get absolute risk and add onto final dataset
ar_hepB    <- .0466781
ar_hepB_sd <- .0128892
ar_hepB    <- data.table(draw=draws, ar_hepB=rnorm(1000, mean=ar_hepB, sd=ar_hepB_sd))
ar_hepB[ar_hepB<0, ar_hepB:=0]

ar_hepC    <- .143054
ar_hepC_sd <- .0175787
ar_hepC    <- data.table(draw=draws, ar_hepC=rnorm(1000, mean=ar_hepC, sd=ar_hepC_sd))
ar_hepC[ar_hepC<0, ar_hepC:=0]

#Merge absolute with cumulative incidence
final <- join(final, ar_hepB, by=c("draw"))
final <- join(final, ar_hepC, by=c("draw"))

#Calculate incidence amongst IDU users as prevalance of IDU * probability of contracting the disease
final[, c("IDU_hepB_incidence", "IDU_hepC_incidence") := list((IDU_prevalence*ar_hepB), (IDU_prevalence*ar_hepC))]

#Map age_groups to ages, then map to a template with 1-year age groups. Carry-forward observations within age (e.g. age 6,7 = age5)
age_map <- data.table(age_group_id = unique(final$age_group_id), age = seq(15,95,5))
final   <- join(final, age_map, by="age_group_id")

template <- data.table(expand.grid(
  "age" = c(15:99),
  "sex_id" = unique(final$sex_id),
  "year_id"= years,
  "location_id" = unique(final$location_id),
  "draw" = draws
))

setkeyv(final, c("year_id", "sex_id", "draw", "location_id", "age"))
setkeyv(template, c("year_id", "sex_id", "draw", "location_id", "age"))

final <- final[template, roll=TRUE]

#Create cohorts to subset data later. Order dataset correctly for cumulative product.
final[, cohort := year_id-age]
final <- final[order(draw, cohort, year_id)]

#Find cumulative product of 1 - P(Not exposed) i.e. probability a cohort was exposed that year
final[, `:=`(cumulative_IDU_hepB_incidence = 1-cumprod(1-IDU_hepB_incidence), 
             cumulative_IDU_hepC_incidence = 1-cumprod(1-IDU_hepC_incidence), 
             cumulative_hepB_incidence = 1-cumprod(1-hepB_incidence), 
             cumulative_hepC_incidence = 1-cumprod(1-hepC_incidence)),
      by=c("cohort", "sex_id", "draw")]

#Calculate PAF as cumulative incidence of drug users/ cumulative incidence in general population
final[, `:=`(paf_IDU_hepB = cumulative_IDU_hepB_incidence/cumulative_hepB_incidence,
             paf_IDU_hepC = cumulative_IDU_hepC_incidence/cumulative_hepC_incidence)]

#Cap since using absolute risk
final[paf_IDU_hepB >= 1, paf_IDU_hepB := 0.9]
final[paf_IDU_hepC >= 1, paf_IDU_hepC := 0.9]

#Reshape to match database needs, save in folder
final <- final[(year_id >=1990), .(year_id, age_group_id, sex_id, draw, paf_IDU_hepB, paf_IDU_hepC)]
final[, draw := gsub("draw", "paf", final$draw)]

hepB <- final[, .(year_id, age_group_id, sex_id, draw, paf_IDU_hepB)]
hepC <- final[, .(year_id, age_group_id, sex_id, draw, paf_IDU_hepC)]

hepB <- dcast(hepB, sex_id + year_id + age_group_id ~ draw, value.var="paf_IDU_hepB", fun = mean)
hepB <- as.data.table(hepB)
hepB <-  hepB[, cause_id := 402]

hepC <- dcast(hepC, sex_id + year_id + age_group_id ~ draw, value.var="paf_IDU_hepC", fun = mean)
hepC <-as.data.table(hepC)
hepC <- hepC[, cause_id := 403]

final <- rbind(hepB, hepC)

write.csv(final, paste0(temp_folder, "idu_temp_paf_", location, ".csv"), row.names = FALSE)

for (sex in sexes){
  for (year in gbd_years){
    
    temp <- final[year_id == year & sex_id == sex,]
    temp[, c("sex_id", "year_id") := NULL,]
    
    #For each sequela, duplicate format using hep B as base, and replace cause_id
    hepB_sequela <- c(418, 522)
    hepC_sequela <- c(419, 523)
    
    for (cause in hepB_sequela){
      tempy <- temp[cause_id==402,]
      tempy[, cause_id:=cause]
      temp <- data.table(rbind(tempy, temp))
    }
    
    for (cause in hepC_sequela){
      tempy <- temp[cause_id==403,]
      tempy[, cause_id:=cause]
      temp <- data.table(rbind(tempy, temp))
    }
    
    #For each disorder, duplicate format using hep B as base, and replace draws =1
    disorders <- c(562,563,564,565,566)
    
    for (cause in disorders){
      tempy <- temp[cause_id==402,]
      tempy[, cause_id:=cause]
      tempy[, c(pafs):=1]
      temp <- data.table(rbind(temp, tempy))
    }
    
    write.csv(temp, paste0("FILEPATH"), row.names = FALSE)
    write.csv(temp, paste0("FILEPATH"), row.names = FALSE)
  }
}