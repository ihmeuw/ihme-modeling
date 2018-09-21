################################################################################
## Description: Run the first stage of the prediction model for child mortality
##              and calculate inputs to GPR 
################################################################################

rm(list=ls())
library(foreign); library(zoo); library(nlme); library(plyr); library(data.table)


if (Sys.info()[1] == "Linux"){
 root <- "FILEPATH"
 rnum <- commandArgs()[3]
 hivsims <- as.integer(commandArgs()[4])
 user <- commandArgs()[5] 
 code_dir <- "FILEPATH"
 source("FILEPATH/shared/functions/get_locations.r")
 source("FILEPATH/shared/functions/get_spacetime_loc_hierarchy.R")
}else{
 root <- "FILEPATH"
 hivsims <- F
 user <- "USERNAME"
 code_dir <- "FILEPATH"
 source("FILEPATH/shared/functions/get_locations.r")
 source("FILEPATH/shared/functions/get_spacetime_loc_hierarchy.R")
}

gbd_year = 2016

setwd("FILEPATH")

source("FILEPATH/helper_functions/choose_reference_categories.r")
source("FILEPATH/space_time.r")
spacetime_parameters = read.csv("FILEPATH/selected_parameters.txt")

data <- read.csv("FILEPATH/prediction_input_data.txt", stringsAsFactors=F)

all_loc_len <- length(unique(data$ihme_loc_id))

## format data
data$method[grepl("indirect", data$type)] <- "SBH"
data$method[data$type == "direct"] <- "CBH"
data$method[grepl("VR|SRS|DSP", data$source)] <- "VR/SRS/DSP"
data$method[data$type == "hh" | (is.na(data$method) & grepl("census|survey", tolower(data$source)))] <- "HH"
data$method[data$source == "hsrc" & data$ihme_loc_id == "ZAF"] <- "CBH"
data$method[data$source == "icsi" & data$ihme_loc_id == "ZWE"] <- "SBH"
data$method[data$source == "indirect" & data$ihme_loc_id == "DZA"] <- "SBH"
data$method[is.na(data$method) & data$data == 1] <- "HH"

data$graphing.source[grepl("vr|vital registration", tolower(data$source))] <- "VR"
data$graphing.source[grepl("srs", tolower(data$source))] <- "SRS"
data$graphing.source[grepl("dsp", tolower(data$source))] <- "DSP"
data$graphing.source[grepl("census", tolower(data$source)) & !grepl("Intra-Census Survey",data$source)] <- "Census"
data$graphing.source[grepl("_IPUMS_", data$source) & !grepl("Survey",data$source)] <- "Census"
data$graphing.source[data$source == "DHS" | data$source == "dhs" | data$source == "DHS IN" | grepl("_DHS", data$source)] <- "Standard_DHS"
data$graphing.source[grepl("^DHS .*direct", data$source1)&!grepl("SP", data$source1)] <- "Standard_DHS"
data$graphing.source[tolower(data$source) %in% c("dhs itr", "dhs sp","dhs statcompiler") | grepl("DHS SP", data$source)] <- "Other_DHS"  #these will all go to other anyway
data$graphing.source[grepl("mics|multiple indicator cluster", tolower(data$source))] <- "MICS"
data$graphing.source[tolower(data$source) %in% c("cdc", "cdc-rhs", "cdc rhs", "rhs-cdc", "reproductive health survey") | grepl("CDC-RHS|CDC RHS", data$source)] <- "RHS"
data$graphing.source[grepl("world fertility survey|wfs|world fertitlity survey", tolower(data$source))] <- "WFS"
data$graphing.source[tolower(data$source) == "papfam" | grepl("PAPFAM", data$source)] <- "PAPFAM"
data$graphing.source[tolower(data$source) == "papchild" | grepl("PAPCHILD", data$source)] <- "PAPCHILD"
data$graphing.source[tolower(data$source) == "lsms" | grepl("LSMS", data$source)] <- "LSMS"
data$graphing.source[tolower(data$source) == "mis" | tolower(data$source) == "mis final report" | grepl("MIS", data$source)] <- "MIS"
data$graphing.source[tolower(data$source) == "ais" | grepl("AIS", data$source)] <- "AIS"
data$graphing.source[is.na(data$graphing.source) & data$data == 1] <- "Other"


#Combining certain types and graphing sources for survey fixed effects
data$graphing.source[data$graphing.source %in% c("Other_DHS","PAPCHILD","PAPFAM","LSMS","RHS")] <- "Other"
data$graphing.source[data$graphing.source == "Census" & data$type == "CBH"] <- "Other"
data$graphing.source[data$graphing.source %in% c("AIS","MIS")] <- "AIS_MIS"

#VR is split in 02 (data prep) into biased and unbiased. Use that here for the source and source.type
data$graphing.source[data$vr == 1 & data$data == 1] <- data$category[data$vr == 1 & data$data == 1]

#Get combo source/type
data$source.type <- paste(data$graphing.source, data$method, sep="_")
data$source.type[data$data == 0] <- NA

isos <- setDT(read.csv(paste0("FILEPATH", gbd_year,".csv"), stringsAsFactors = F))

isos <- isos[isos$level_all != 0,] # make sure we only include all locations that 5q0 uses
isos <- isos[,list(ihme_loc_id,local_id_2013)]
setnames(isos,c("ihme_loc_id","local_id_2013"),c("ihme_loc_id","iso3"))
data <- merge(data,isos,by="ihme_loc_id",all.x=T)

######################
#Choose reference categories
######################

data$reference <- 0
data = refcats.setRefs(input = data)

data$vr_no_overlap <- 0
data[data$category %in% c("vr_no_overlap"),]$vr_no_overlap <- 1

# set vr_no_overlap to vr_biased, but mark it so that it isn't run through the vr bias adjustment
data[data$category %in% c("vr_no_overlap"), ]$corr_code_bias <- T
data[data$category %in% c("vr_no_overlap"), ]$to_correct <- T
data[data$category %in% c("vr_no_overlap"), ]$source.type <- "vr_biased_VR/SRS/DSP"
data[data$category %in% c("vr_no_overlap"), ]$graphing.source <- "vr_biased"


data[data$category %in% c("vr_no_overlap"), ]$category <- "vr_biased"

#################################################################################
#################################################################################
#######################
# Fit first stage model
#######################

#solve for mx
data$mx <- log(1-data$mort)/-5

data$tLDI <- log(data$LDI)
data$ihme_loc_id <- as.factor(data$ihme_loc_id)

#grouped data object
data$dummy <- 1
data$source.type <- as.factor(data$source.type)

#sets dhs cbh as the first (and therefore reference) category for source.types
data$source.type <- relevel(data$source.type,"Standard_DHS_CBH")

## add check to see if all the locations are still here
stopifnot(length(unique(data$ihme_loc_id))==all_loc_len)

mod.data <- groupedData(mx~ 1 | ihme_loc_id/source1, data = data[!is.na(data$mort),])

#Model 2: fixed intercept, survey.type, random ihme_loc_id/survey
###########################################################
#have tested - model not sensitive to start values
fm1start <- c(rep(0, length(unique(data$source.type[data$data == 1]))+3))

#Model 2 formula: fixed effect on source.type
fm1form <- as.formula("mx ~ exp(beta1*tLDI + beta2*maternal_educ + beta5*dummy + beta4) + beta3*hiv")

#nlme with nested RE on ihme_loc_id/survey, FE on source.type
model <- nlme(fm1form, 
  data = mod.data, 
  fixed = list(beta1 + beta2 + beta3 ~1, beta5 ~ source.type),
  random = list(ihme_loc_id = beta1 + beta2 + beta4 ~ 1, source1 = beta4 ~ 1),
  start = fm1start,
  verbose = F)

# save first stage model
save(model, file="data/first_stage_regressions.rdata")
save(model, file=paste("data/archive/first_stage_regression_", Sys.Date() ,".rdata", sep=""))
  
##########################
#Merge residuals, fixed effects, random effects into data
#########################

###########################################################
##Merge residuals into data
data$resid1 <- rep("NA", length(data$data))
data$resid1[!is.na(data$mort)] <- model$residuals[,"source1"]
data$resid1 <- as.numeric(data$resid1)

##Merge ihme_loc_id:survey (nested) Random Effects into data
data$src.ihme_loc_id <- paste(data$ihme_loc_id, "/",data$source1, sep="")

src.re <- as.data.frame(model$coefficients$random$source1[,1])
colnames(src.re) <- "re2"
src.re$src.ihme_loc_id <- row.names(src.re)

data <- merge(data, src.re, by="src.ihme_loc_id", all.x = T)

##Merge ihme_loc_id random effects into data
data$b1.re <- data$b2.re <- data$ctr_re <- NA
for (ii in rownames(model$coefficients$random$ihme_loc_id)){
 data$b1.re[data$ihme_loc_id == ii] <- model$coefficients$random$ihme_loc_id[ii,1]
 data$b2.re[data$ihme_loc_id == ii] <- model$coefficients$random$ihme_loc_id[ii,2]
 data$ctr_re[data$ihme_loc_id == ii] <- model$coefficients$random$ihme_loc_id[ii,3]
}

##Merge source.type fixed effects into data
#Intercept/reference category is assigned to be Standard_DHS_CBH right now
st.fe <- fixef(model)[grep("(Intercept)",names(fixef(model))):length(fixef(model))] 

names(st.fe) <- levels(data$source.type)
st.fe[1] <- 0

st.fe <- as.data.frame(st.fe)
st.fe$source.type <- row.names(st.fe)
data <- merge(data, st.fe, by="source.type",all.x = T)

## add check to see if all the locations are still here
stopifnot(length(unique(data$ihme_loc_id))==all_loc_len)

#Get data back in order
data <- data[order(data$ihme_loc_id, data$year),]

########################
#Get reference value of FE+RE and adjust data
########################

dat3 <- ddply(data[!duplicated(data[,c("ihme_loc_id","source1")]),],
              .(ihme_loc_id),
              function(x){
                  data.frame(ihme_loc_id = x$ihme_loc_id[1],
                             mre2 = mean(x$re2[x$reference == 1]),
                             mfe = mean(x$st.fe[x$reference ==1]))
              })

dat3$summe <- dat3$mre2+dat3$mfe

#merge ref sum re/fe into data
data <- merge(data,dat3,all=T)

data <- data[,names(data) != "src.ihme_loc_id"]

#get adjusted re + fe into data
data$adjre_fe <- data$re2+data$st.fe-data$summe

#####################
#Get predictions 
####################

#predictions w/o any random or fixed effects
pred.mx <- exp(model$coefficients$fixed[1]*data$tLDI + model$coefficients$fixed[2]*data$maternal_educ + model$coefficients$fixed[4]) + model$coefficients$fixed[3]*data$hiv

data$pred.1b <- 1-exp(-5*pred.mx)

######################################
   
# manually set SAU to not be corrected 
data[grepl("SAU_", data$ihme_loc_id),]$corr_code_bias <- FALSE
data[grepl("SAU_", data$ihme_loc_id),]$to_correct <- FALSE

# set ZAF subnational VR to be treated like a survey 
ZAF_subnats <- grep("ZAF_", unique(data$ihme_loc_id), value=T)
data[(data$ihme_loc_id %in% ZAF_subnats) & data$vr==1 & !is.na(data$vr) ,]$to_correct <- FALSE
data[(data$ihme_loc_id %in% ZAF_subnats) & data$vr==1 & !is.na(data$vr),]$corr_code_bias <- FALSE
data[(data$ihme_loc_id %in% ZAF_subnats) & data$vr==1 & !is.na(data$vr),]$vr <- 0

#####################
#Get adjusted data points - only for non-incomplete-VR sources  
####################
#Calculate mort w/ survey random effects removed for residual calculation (2nd Stage Model) - not biased VR
#The nbs.ind determines which points should be bias (mixed-effects) adjusted
#to_correct means that a point will be corrected in the vr step 
  nbs.ind <- (data$data == 1 & !data$to_correct)
  data$mx2[nbs.ind] <- exp((model$coefficients$fixed[1]+data$b1.re[nbs.ind])*data$tLDI[nbs.ind] + (model$coefficients$fixed[2] + data$b2.re[nbs.ind])*data$maternal_educ[nbs.ind]  + model$coefficients$fixed[4] + data$ctr_re[nbs.ind] + data$summe[nbs.ind]) + model$coefficients$fixed[3]*data$hiv[nbs.ind] + data$resid1[nbs.ind]

  #don't let complete VR go down
  data$mx2[data$data==1 & data$category == "vr_unbiased" & data$mx2<data$mx] <- data$mx[data$data==1 & data$category == "vr_unbiased" & data$mx2<data$mx]
#AFG
  #find mean adjustment for category other sources in afghanistan, and use it to adjust the 'national demographic and family guidance survey' points
  afg.oth.m <- mean(data$adjre_fe[data$data == 1 & data$ihme_loc_id == "AFG" & grepl("other", data$source.type, ignore.case =T) & data$source != "national demographic and family guidance survey"])
  afg.ind <- (data$data == 1 & data$ihme_loc_id == "AFG" & data$source == "national demographic and family guidance survey")
  data$mx2[afg.ind] <-  exp((model$coefficients$fixed[1]+data$b1.re[afg.ind])*data$tLDI[afg.ind] + (model$coefficients$fixed[2] + data$b2.re[afg.ind])*data$maternal_educ[afg.ind]  + model$coefficients$fixed[4] + data$ctr_re[afg.ind] + data$re2[afg.ind] + data$st.fe[afg.ind] - afg.oth.m) + model$coefficients$fixed[3]*data$hiv[afg.ind] + data$resid1[afg.ind]

#RUS & POL - Soviet correction for VR
  #Both:
  #1) Find correction factor
  #2) Adjust VR1
  #Poland - assign this number to mx2
  #Russia - 1)assign this number to mx
  #       - 2)rename VR1 as VR so it all gets the same correction later

  #impute negative mx2's as 0.0001
  data$mx2[data$mx2 <= 0] <- 0.0001

#Transform back to qx space  
  data$mort2 <- 1-exp(-5*(data$mx2))
  data$log10_mort2 <- log(data$mort2, base=10)

## add check to see if all the locations are still here
stopifnot(length(unique(data$ihme_loc_id))==all_loc_len)


#######################
#Biased VR adjustment 
#######################

# for now, do not adjust SAU subnats
model_save <- model

# run a loess regression to determine the bias correction for biased countries
  data <- data[order(data$ihme_loc_id, data$year),]
  for(ihme_loc_id in unique(data$ihme_loc_id[data$corr_code_bias & (data$data == 1)])) {

        # loess non-vr data in these countries
        # changed span from 1.5 to 0.8 on 1/10/14 to try and make vr adjustment more responsive for HIV bumps
        #Warning: Don't use span < 0.9 or countries with < 7 points
        model <- loess(log(mort2,base=10) ~ year, span=.9, data=data[data$vr==0 & data$ihme_loc_id==ihme_loc_id,], control=loess.control(surface="direct"))
  
        # predict based on the loess
        preds <- predict(model, newdata=data.frame(year=data$year[data$ihme_loc_id==ihme_loc_id]))
  
        # add the predictions to the main dataset
        data$non.vr.loess[data$ihme_loc_id==ihme_loc_id] <- preds

        # find the min and max year where non-vr data is available at the country level
        data$min[data$ihme_loc_id==ihme_loc_id] <- min(data$year[data$vr==0 & data$ihme_loc_id==ihme_loc_id], na.rm=T)
        data$max[data$ihme_loc_id==ihme_loc_id] <- max(data$year[data$vr==0 & data$ihme_loc_id==ihme_loc_id], na.rm=T)
  
        # find the difference for each individual vr point in the dataset
        data$index <- 0
        data$index[data$corr_code_bias & data$year >= data$min & data$year <= data$max & data$ihme_loc_id==ihme_loc_id] <- 1   # vr where non-vr is available
        data$index[data$corr_code_bias & data$year < data$min & data$ihme_loc_id==ihme_loc_id] <- 2                            # vr where non-vr is not available (early)
        data$index[data$corr_code_bias & data$year > data$max & data$ihme_loc_id==ihme_loc_id] <- 3                            # vr where non-vr is not available (late)
  
        # find the difference between the loess of non-vr and the vr estimates (in the loess sample)
        data$diff[data$index==1] <- data$non.vr.loess[data$index==1] - log(data$mort[data$index==1],base=10)
        data$abs.diff[data$index==1] <- abs(data$diff[data$index==1])
  
        # convert this difference (in log10 space) to a bias correction factor (5q0 space)
        data$bias[data$index==1] <- 10^data$diff[data$index==1]
      
      #####  
      #Countries for which we want to estimate different VR bias for multiple different VR systems
          vr.systems <- unique(data$source[data$ihme_loc_id == ihme_loc_id & data$corr_code_bias])
          vr.systems <- vr.systems[!is.na(vr.systems)]
          for (sys in vr.systems){
              if(length(data$vr[data$ihme_loc_id==ihme_loc_id & data$index==1 & data$source == sys]) >= 5) {
                # find the 5 year rolling mean of the bias correction (for index 1 only)
                data$mean.bias[data$index==1 & data$source == sys] <- rollmean(data$bias[data$index==1 & data$source == sys],5,na.pad=T, align=c("center"))
                # find the 5 year rolling MAD of the bias correction (for index 1 only)
                data$mad[data$index==1 & data$source == sys] <- rollmedian(data$abs.diff[data$index==1 & data$source == sys],5,na.pad=T, align=c("center"))
        
                # fill in missing bias estimates on the two tails of our data series
                early <- data$bias[data$index==1 & data$source == sys][1:5]
                end <- length(data$bias[data$index==1 & data$source == sys])
                end.min5 <- end - 4
                late <- data$bias[data$index==1 & data$source == sys][end.min5:end]
                mean.early <- mean(early)
                mean.late <- mean(late)
                data$mean.bias[is.na(data$mean.bias) & data$index==2 & data$source == sys] <- mean.early
                data$mean.bias[is.na(data$mean.bias) & data$index==3 & data$source == sys] <- mean.late
        
                # still need to fill in the tails of the in-sample vr data
                max <- max(data$year[!is.na(data$mean.bias) & data$index==1 & data$source == sys])
                min <- min(data$year[!is.na(data$mean.bias) & data$index==1 & data$source == sys])
                data$mean.bias[is.na(data$mean.bias) & data$year<min & data$vr==1 & data$ihme_loc_id==ihme_loc_id & data$source == sys] <- mean.early
                data$mean.bias[is.na(data$mean.bias) & data$year>max & data$vr==1 & data$ihme_loc_id==ihme_loc_id & data$source == sys] <- mean.late
        
                # same as above for the mad estimator
                early <- data$abs.diff[data$index==1 & data$source == sys][1:5]
                end <- length(data$abs.diff[data$index==1 & data$source == sys])
                end.min5 <- end - 4
                late <- data$abs.diff[data$index==1 & data$source == sys][end.min5:end]
                mean.early <- mean(early)
                mean.late <- mean(late)
                data$mad[is.na(data$mad) & data$index == 2 & data$source == sys] <- mean.early
                data$mad[is.na(data$mad) & data$index == 3 & data$source == sys] <- mean.late
        
                # again, filling in missingingness in-sample, this time for the mad
                data$mad[is.na(data$mad) & data$year<min & data$vr==1 & data$ihme_loc_id==ihme_loc_id & data$source == sys] <- mean.early
                data$mad[is.na(data$mad) & data$year>max & data$vr==1 & data$ihme_loc_id==ihme_loc_id & data$source == sys] <- mean.late
            } else {
                # find the mean bias across all points
                mean.bias <- mean(data$bias[data$ihme_loc_id==ihme_loc_id & data$vr==1 & data$source == sys],na.rm=T)
                # find the mad across all points
                mad <- median(data$abs.diff[data$ihme_loc_id==ihme_loc_id & data$vr==1 & data$source == sys], na.rm=T)
        
                # fill in the mean bias and mad in the dataset
                data$mean.bias[data$ihme_loc_id==ihme_loc_id & data$vr==1 & data$source == sys] <- mean.bias
                data$mad[data$ihme_loc_id==ihme_loc_id & data$vr==1 & data$source == sys] <- mad

            }
          }
        
        # convert the MAD estimate to a data variance
        data$bias.var[data$ihme_loc_id==ihme_loc_id & data$vr==1 & !is.na(data$vr)] <- (1.4826*data$mad[data$ihme_loc_id==ihme_loc_id & data$vr==1 & !is.na(data$vr)])^2
  
        # do not want to adjust cy's where the vr is biased upwards (except IND, PAK, and BGD where we want to add data variance)
        data$bias.var[data$ihme_loc_id==ihme_loc_id & data$mean.bias <= 1 & data$to_correct] <- 0
        data$mean.bias[data$ihme_loc_id==ihme_loc_id & data$mean.bias <= 1] <- 1
  }


######
# also adjust VR-only countries with known biases using regional average bias

  data$mean.bias[data$vr_no_overlap==1] <- 1
  data$bias.var[data$vr_no_overlap==1] <- 0
  
## add check to see if all the locations are still here
stopifnot(length(unique(data$ihme_loc_id))==all_loc_len)

  # first fill in bias variables
  data$mean.bias[data$region_name=="CaribbeanI" & data$category=="vr_unbiased"] <- 1
  data$bias.var[data$region_name=="CaribbeanI" & data$category=="vr_unbiased"] <- 0

  #mark data from countries with surveys in addition to VR ["surveys" T/F]
  cardat <- data.table(data[data$region_name == "CaribbeanI",])
  cardat <- cardat[,surveys := (sum(data, na.rm = T) > sum(vr, na.rm = T)), by = ihme_loc_id]

  #find mean bias by year from countries with surveys in addtion to VR
  cardat <- cardat[,':='(rr.var = mean(bias.var[surveys == T], na.rm = T), rr.bias = mean(mean.bias[surveys == T], na.rm = T)), by = c("year")]

  # fill in years post 2008 with bias from 2008, as the more recent years appear too complete or don't have data
  cardat$rr.bias[cardat$year %in% c(2009.5,2010.5,2011.5,2012.5,2013.5,2014.5,2015.5,2016.5)] <- cardat$rr.bias[cardat$year==2008.5][1]
  cardat$rr.var[cardat$year %in% c(2009.5,2010.5,2011.5,2012.5,2013.5,2014.5,2015.5,2016.5)] <- cardat$rr.var[cardat$year==2008.5][1]

  #replace bias adjustment in vr-only CaribbeanI countries with the mean from every year for those CarI countries with surveys and vr
  cardat$mean.bias[cardat$surveys == F & cardat$data  == 1] <- cardat$rr.bias[cardat$surveys == F & cardat$data  == 1]
  cardat$bias.var[cardat$surveys == F & cardat$data  == 1] <- cardat$rr.var[cardat$surveys == F & cardat$data  == 1]

  #change status of no-survey CarI country points so they get corrected
  cardat$to_correct[cardat$surveys == F & cardat$data  == 1] <- T
  cardat$corr_code_bias[cardat$surveys == F & cardat$data  == 1] <- T

  #replace entries in 'data' with 'cardat' for CaribbeanI countries
  #first get rid of excess variables
  cardat$surveys <- cardat$rr.bias <- cardat$rr.var <- NULL

  data <- data[data$region_name != "CaribbeanI",]
  data <- as.data.frame(rbind(data,cardat))

# adjust all biased VR and VR in CaribbeanI
#####
# adjusting only vr data
  #first, get the indices of the data we actually want to correct, as there are some exceptions (see 02_adjust_biased_vr)
  vr.cor.inds <- data$to_correct & (data$data  == 1)
  data$mort2[vr.cor.inds] <- data$mort[vr.cor.inds]*data$mean.bias[vr.cor.inds]

#get mx for variance calculations later on
data$mx2[vr.cor.inds] <- log(1-data$mort2[vr.cor.inds])/-5


data <- data[order(data$ihme_loc_id, data$year),]


########################
# Fit second stage model - space time loess of residuals
########################  

data[data$reference==1,]$mort2 <- data[data$reference==1,]$mort

# calculate residuals from final first stage regression
data$resid <- logit(data$mort2) - logit(data$pred.1b)

# get new spacetime locations
st_locs <- get_spacetime_loc_hierarchy(prk_own_region=F)

# try just having one residual for each country-year with data for space-time, so as not to give years with more data more weight
stdata <- ddply(data, .(ihme_loc_id,year), 
  function(x){
      data.frame(region_name = x$region_name[1],
      ihme_loc_id = x$ihme_loc_id[1],
      year = x$year[1],
      vr = max(x$vr),    
      resid = mean(x$resid))
    })
st_data_copy <- copy(stdata)

stdata <- stdata[,!colnames(stdata) %in% "region_name"]

## merging on fake regions
stdata <- merge(stdata, st_locs, all.x=T, by="ihme_loc_id")

## fit spacetime
preds <- resid_space_time(data=stdata, param=spacetime_parameters)
preds <- preds[preds$keep==1,]
preds <- preds[!colnames(preds) %in% "keep"]

data <- merge(data, preds, by=c("ihme_loc_id", "year"))
data$pred.2.resid <- inv.logit(data$pred.2.resid)
  
data$pred.2.final <- inv.logit(logit(data$pred.2.resid) + logit(data$pred.1b))  

## add check to see if all the locations are still here
stopifnot(length(unique(data$ihme_loc_id))==all_loc_len)

######################
#causes mort to be adjusted, mort2 unadjusted, mort3 is intermediate step
  data$mort3 <- data$mort2 
  data$mort2 <- data$mort
  data$mort <- data$mort3

 
  # get subnationals - we only want to calculate mse from nationals
  
  subnats <- read.csv(paste0(root, "FILEPATH/locations.csv"), stringsAsFactors = F)
  subnats <- subnats[subnats$level > 3,]
  subnats <- unique(subnats$ihme_loc_id)
  
  se <- (logit(data[!data$ihme_loc_id %in% subnats,]$mort) - logit(data[!data$ihme_loc_id %in% subnats,]$pred.2.final))^2
  mse <- tapply(se, data[!data$ihme_loc_id %in% subnats,]$region_name, function(x) mean(x, na.rm=T)) 
  for (ii in names(mse)) data$mse[data$region_name == ii] <- mse[ii]

###########################
#Get estimate of variance to add on in 03 from taking
#standard deviation of RE for all surveys of a given source-type
#also, try doing this over region
###########################
data$source.type[data$data == 0] <- NA
data$adj.re2 <- data$re2-data$mre2
source.dat <- data[!duplicated(data[,c("ihme_loc_id","source1")]) & data$data == 1,]
sds <- tapply(source.dat$adj.re2, source.dat[,c("source.type")], function(x) sd(x))
sds[is.na(sds)] <- 0
var <- sds^2

#merge var into data
for (st in names(sds)){
    data$var.st[data$source.type == st] <- var[names(var) == st]
}


#delta method, log(mx) space to qx space
data$var.st.qx <- data$var.st * (exp(-5*data$mx2) *5* data$mx2)^2
  
# write results files
datas <- data[,c("super_region_name", "region_name", "ihme_loc_id", "year", "LDI_id", "maternal_educ", "hiv", 
                  "data", "category", "corr_code_bias","to_correct","vr", "mort", "mort2", "mse",
                  "pred.1b", "resid", "pred.2.resid", "pred.2.final","ptid","source1","re2", "adjre_fe",
                  "reference", "log10.sd.q5","bias.var", "var.st.qx",
                  "source.yr","source", "type","location_name","ctr_re")]

datas <- datas[order(datas$ihme_loc_id, datas$year, datas$data),]

## add check to see if all the locations are still here
stopifnot(length(unique(data$ihme_loc_id))==all_loc_len)
  
write.csv(data, "FILEPATH/prediction_model_results_all_stages.txt",row.names=F)
write.csv(data, paste("FILEPATH/prediction_model_results_all_stages_", Sys.Date(), ".txt", sep=""),row.names=F)