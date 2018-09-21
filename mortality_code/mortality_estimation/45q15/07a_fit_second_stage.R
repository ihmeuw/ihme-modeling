################################################################################
## Description: Run the second stage model 
################################################################################

## set up 

rm(list=ls())
library(foreign); library(zoo); library(nlme); library(data.table); library(plyr); library(reshape); library(ggplot2)

## Set local working directory (toggles by GIT user) 
if (Sys.info()[1] == "Linux") {
  root <- "filepath"
  user <- Sys.getenv("USER")
  code_dir <- paste0("filepath")
  sim <- as.numeric(commandArgs()[3])  ## this gets passed as 1 if not doing HIV sims
  hivsims <- as.logical(as.numeric(commandArgs()[4]))

} else if (Sys.getenv("USERNAME") != "msfraser") {
  root <- "filepath"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("filepath")
  sim <- 1 ## this gets passed as 1 if not doing HIV sims
  hivsims <- F
}  else {
  root <- "filepath"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("filepath")
  sim <- 1 ## this gets passed as 1 if not doing HIV sims
  hivsims <- F
}


if (hivsims) {
  setwd(paste0("filepath"))
  data <- read.csv(paste0("filepathv"),stringsAsFactors=F)
} else {
  setwd(paste("filepath", sep=""))  
  data <- read.csv("filepath", stringsAsFactors=F)
}

source(paste("filepath", sep = ""))
source(paste0( "filepath"))

transform="logit"
setwd(paste( "filepath", sep=""))
parameters <- read.csv("filepath")


## set countries that we have subnational estimates for
locations <- read.csv(paste0( "filepath"))
locs_gbd2013 <- unique(locations$ihme_loc_id[!is.na(locations$local_id_2013)])

#create fake regions so we can do spacetime on subnationals separately from their parents
st_locs <- get_spacetime_loc_hierarchy()


types <- ddply(.data=data, .variables=c("ihme_loc_id","sex"),.inform=T,
               .fun=function(x) {
                 cats <- unique(x$category)
                 cats <- cats[!is.na(cats)] # get rid of NA 
                 vr <- mean(x$vr, na.rm=T)
                 if(is.na(vr)) vr <- 0
                 vr.max <- ifelse(vr == 0, 0, max(x$year[x$vr==1 & !is.na(x$vr)]))
                 vr.num <- sum(x$vr==1, na.rm=T)
                 if (length(cats) == 1 & cats[1] == "complete" & vr == 1 & vr.max > 1980 & vr.num > 10){
                   type <- "complete VR only"
                 } else if (("ddm_adjust" %in% cats | "gb_adjust" %in% cats) & vr == 1 & vr.max > 1980 & vr.num > 10){
                   type <- "VR only"
                 } else if ((vr < 1 & vr > 0) | (vr == 1 & (vr.max <= 1980 | vr.num <= 10))){
                   type <- "VR plus"
                 } else if ("sibs" %in% cats & vr == 0){
                   type <- "sibs"
                 } else if (!("sibs" %in% cats) && length(cats) > 0 && vr == 0){
                   type <- "other"
                 } else{ 
                    type <- "no data"
                 }
                 return(data.frame(type2013=type, stringsAsFactors=F))
               })  

data <- merge(data, types, all.x=T, by=c("ihme_loc_id", "sex"))
# 
########################
# Fit second stage model
########################

# calculate residuals from final first stage regression
data$resid <- logit(data$mort) - logit(data$pred.1.noRE)

# merge on fake regions
data$ihme_loc_id <- as.character(data$ihme_loc_id)
data$region_name <- NULL
data$location_id <- NULL
data <- merge(data, st_locs, all.x=T, allow.cartesian = T, by="ihme_loc_id")
  
## do space time regression
preds <- resid_space_time(data, post_param_selection=T)

#remove fake regions
preds <- preds[preds$keep==1,]
preds <- preds[!colnames(preds) %in% "keep"]

## get results
data <- merge(data, preds, by=c("ihme_loc_id", "sex", "year"))
data <- data[data$keep == 1,]
data <- data[!colnames(data) %in% "keep"]

data$pred.2.resid <- inv.logit(data$pred.2.resid)
data$pred.2.final <- inv.logit(logit(data$pred.2.resid) + logit(data$pred.1.noRE))




################
# Calculate GPR inputs
################  

## Mean squared error


find_se <- function(transform, data){
  if (transform == "log10") se <- (log(data$mort, base=10) - log(data$pred.2.final, base=10))^2 ## if log base 10
  if (transform == "ln") se <- (log(data$mort) - log(data$pred.2.final))^2 ## if natural log
  if (transform == "logit") se <- (logit(data$mort) - logit(data$pred.2.final))^2 ## if logit
  if (transform == "logit10") se <- (logit10(data$mort) - logit10(data$pred.2.final))^2 ## if logit10
  return(se)
}

national_locs <- locations[locations$level == 3,]
nationals <- data[data$ihme_loc_id %in% national_locs$ihme_loc_id | data$ihme_loc_id == "CHN_44533",]
diff  <- logit(nationals$pred.2.final) - logit(nationals$pred.1.noRE)
mse <- tapply(diff[], nationals$ihme_loc_id, function(x) var(x, na.rm=T)) 
for (ii in names(mse)) data$mse[substr(data$ihme_loc_id, 1, 3) == ii] <- mse[ii]
data$mse[substr(data$ihme_loc_id, 1, 3) == "CHN"] <- mse["CHN_44533"]

mse_mid_east <- mean(mse[names(mse) %in% unique(data$ihme_loc_id[data$region_name == "North Africa and Middle East"])])
data$mse[data$ihme_loc_id == 'PSE'] <- mse_mid_east
data$mse[substr(data$ihme_loc_id, 1, 3) == "SAU"] <- mse_mid_east
## calculate data variance in normal space
## first, for all groups, calculate sampling variance for adjusted and unadjusted mortality. 
## We do this in Mx space, and then convert to qx and to transformed qx using the delta method
# adjusted
data$mx <- log(1-data$mort)/(-45)
data$varmx <- (data$mx*(1-data$mx))/data$exposure
data$varqx <- (45*exp(-45*data$mx))^2*data$varmx # delta transform to normal qx space
data$varlog10qx <- (1/(data$mort * log(10)))^2*data$varqx # delta transform to log10 qx space
if (transform == "log10") data$var <- data$varlog10qx # set variance for log10 qx space ## if log base 10
if (transform == "ln") data$var <- (1/(data$mort))^2*data$varqx # delta transform to natural log qx space ## if natural log
if (transform == "logit") data$var <-  (1/(data$mort*(1-data$mort)))^2*data$varqx ## if logit
if (transform == "logit10") data$var <-  (1/(data$mort*(1-data$mort)*log(10)))^2*data$varqx ## if logit10

# unadjusted
data$mx_unadjust <- log(1-data$obs45q15)/(-45)
data$varmx_unadjust <- (data$mx*(1-data$mx))/data$exposure
data$varqx_unadjust <- (45*exp(-45*data$mx))^2*data$varmx_unadjust # delta transform to normal qx space
data$varlog10qx_unadjust <- (1/(data$obs45q15 * log(10)))^2*data$varqx_unadjust # delta transform to log10 qx space for the addition of DDM variance
if (transform == "log10") data$var_unadjust <- data$varlog10qx_unadjust # set variance for log10 qx space ## if log base 10
if (transform == "ln") data$var_unadjust <- (1/(data$obs45q15))^2*data$varqx_unadjust # delta transform to natural log qx space ## if natural log
if (transform == "logit") data$var_unadjust <-  (1/(data$obs45q15*(1-data$obs45q15)))^2*data$varqx_unadjust ## if logit
if (transform == "logit10") data$var_unadjust <-  (1/(data$obs45q15*(1-data$obs45q15)*log(10)))^2*data$varqx_unadjust ## if logit10

## add in variance from DDM to VR complete and incomplete
cc <- (data$source_type %in% c("SRS", "VR-SSA", "VR", "DSP"))

if (transform == "log10") data$var[cc] <- data$var_unadjust[cc] + data$adjust.sd[cc]^2 ## if log base 10

if (transform == "ln") {  ## if natural log
  # transform from log base 10 space to normal space using delta method
  data$adjust.sd[cc] <- (10^(log(data$comp[cc],10))*log(10))^2*(data$adjust.sd[cc]^2) ## note that now, adjust.sd is actually the variance
  # transform from normal space to natural log space using delta method
  data$adjust.sd[cc] <- (1/(data$comp[cc]))^2*(data$adjust.sd[cc])
  # make it a standard deviation again
  data$adjust.sd[cc] <- sqrt(data$adjust.sd[cc])
  # add on variance
  data$var[cc] <- data$var[cc] + data$adjust.sd[cc]^2
}

if (transform == "logit") { ## if logit
  # combine variances in log10 space: 

  data$totvar <- 0 
  data$totvar[cc] <- data$varlog10qx_unadjust[cc] + data$adjust.sd[cc]^2
  # transform from log base 10 space to normal space using delta method
  data$totvar[cc] <- (10^(log(data$mort[cc],10))*log(10))^2*(data$totvar[cc])
  # transform from normal space to logit space using delta method
  data$var[cc] <- (1/(data$mort[cc]*(1-data$mort[cc])))^2*(data$totvar[cc])
}

if (transform == "logit10") { ## if logit10
  # combine variances in log10 space: 
  

  data$totvar <- 0 
  data$totvar[cc] <- data$varlog10qx_unadjust[cc] + data$adjust.sd[cc]^2    # transform from log base 10 space to normal space using delta method
  # transform from log base 10 space to normal space using delta method
  data$totvar[cc] <- (10^(log(data$mort[cc],10))*log(10))^2*(data$totvar[cc])
  # transform from normal space to logit10 space using delta method
  data$var[cc] <- (1/(data$mort[cc]*(1-data$mort[cc])*log(10)))^2*(data$totvar[cc])
}


diff <- logit(data$mort) - logit(data$pred.2.final)
## household
data$var_source[data$source_type %in%  c("MOH survey", "HOUSEHOLD", "HOUSEHOLD_DEATHS", "SSPC", "SURVEY", "DC") & data$data == 1] <- "household"
data$var_source[data$source_type %in%  c("CENSUS", "SUSENAS") & data$data == 1] <- "census"
data$var_source[data$source_type %in%  c("SIBLING_HISTORIES") & data$data == 1] <- "sibs"
cc <- (!is.na(data$var_source))
mad <- tapply(diff[cc], data$var_source[cc], function(x)median(abs(x - median(x)))) 
for (ii in names(mad)) data$x[data$var_source == ii] <- mad[ii] 
data$var[cc] <- (1.4826*data$x[cc])^2


## convert estimate to log space (and calculate std. errors)
if (transform == "log10") data$log_mort <- log(data$mort, base=10) ## if log base 10
if (transform == "ln") data$log_mort <- log(data$mort) ## if natural log
if (transform == "logit") data$log_mort <- logit(data$mort) ## if logit
if (transform == "logit10") data$log_mort <- logit10(data$mort) ## if logit10
data$log_stderr <- sqrt(data$var)
if (transform == "log10") data$stderr <- sqrt((10^(data$log_mort)*log(10))^2 * data$var) # delta transform back to normal qx space ## if log base 10
if (transform == "ln") data$stderr <- sqrt(exp(data$log_mort)^2 * data$var) # delta transform back to normal qx space ## if natural log
if (transform == "logit") data$stderr <- sqrt((exp(data$log_mort)/(1+exp(data$log_mort)))^4 * data$var) # delta transform back to normal qx space ## if logit
if (transform == "logit10") data$stderr <- sqrt(log(10)*(exp(data$log_mort)/(1+exp(data$log_mort)))^4 * data$var) # delta transform back to normal qx space ## if logit10

#
################
# Save output 
################    

data <- data[,c("location_id", "location_name", "super_region_name", "region_name", "ihme_loc_id", "sex", "year", "LDI_id", "mean_yrs_educ", "hiv",
                "data", "type", "category", "vr", "mort", "stderr", "log_mort", "log_stderr", 
                "pred.1.wRE", "pred.1.noRE", "resid", "pred.2.resid", "pred.2.final", "mse")]
data <- data[order(data$ihme_loc_id, data$sex, data$year, data$data),]

if (hivsims) {
  setwd(paste0("filepath"))
  write.csv(data, file=paste0("filepath"), row.names=F)
} else {
  write.csv(data, file="filepath", row.names=F)
  write.csv(data, file=paste("filepath", sep=""), row.names=F)
}



