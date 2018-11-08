################################################################################
## Description: Run the second stage model
################################################################################

rm(list=ls())
library(foreign); library(zoo); library(nlme); library(data.table); library(plyr); library(reshape); library(ggplot2); library(readr)
library(mortdb, lib = "FILEPATH")

data <- read.csv(input_file, stringsAsFactors=F)
parameter_data <- as.data.frame(fread("FILEPATH"))


transform="logit"


## set countries that we have subnational estimates for
locations <- read.csv("FILEPATH")
locs_gbd2013 <- unique(locations$ihme_loc_id[!is.na(locations$local_id_2013)])

#create fake regions so we can do spacetime on subnationals separately from their parents
st_locs <- get_spacetime_loc_hierarchy(prk_own_region=F, gbd_year = gbd_year)

## classify into 2013 categories so that we can use those for the mse
types <- ddply(.data=data, .variables=c("ihme_loc_id", "sex"), .inform=T,
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
preds <- resid_space_time(data, param_data = parameter_data, max_year = gbd_year)

#remove fake regions
preds <- preds[preds$keep==1, ]
preds <- preds[!colnames(preds) %in% "keep"]

## get results
data <- merge(data, preds, by=c("ihme_loc_id", "sex", "year"))

data <- data[data$keep == 1, ]
data <- data[!colnames(data) %in% "keep"]

data$pred.2.resid <- inv.logit(data$pred.2.resid)
data$pred.2.final <- inv.logit(logit(data$pred.2.resid) + logit(data$pred.1.noRE))


################
# Calculate GPR inputs
################
# Calculate mse as variance in difference between 1st and 2nd stage predictions
data <- merge(data, parameter_data[, c('ihme_loc_id', 'sex', 'data_density')],
              by=c('ihme_loc_id', 'sex'), all.x = T)
# Calculate the average amplitude value where data density >= 50 and the year is 1990 or later
high_data_density <- data[data$data_density >= 50 & data$year >= 1990,]
high_data_density$diff <- logit(high_data_density$pred.2.final) - logit(high_data_density$pred.1.noRE)
variance_diff <- tapply(high_data_density$diff, high_data_density$ihme_loc_id, function(x) var(x, na.rm=T))
amp_value <- mean(variance_diff)
data$mse <- amp_value


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
## change as of 4/3/17: we want to add variance for VR complete countries as well becuase of concerns of countries switching between complete/incomplete
cc <- (data$source_type %in% c("srs", "vr-ssa", "vr", "dsp") & data$category != 'complete')

## log10 is correct
if (transform == "log10") data$var[cc] <- data$var_unadjust[cc] + data$adjust.sd[cc]^2 ## if log base 10

## ln is correct yet
if (transform == "ln") {  ## if natural log
  # transform from log base 10 space to normal space using delta method
  data$adjust.sd[cc] <- (10^(log(data$comp[cc], 10))*log(10))^2*(data$adjust.sd[cc]^2) ## note that now, adjust.sd is actually the variance
  # transform from normal space to natural log space using delta method
  data$adjust.sd[cc] <- (1/(data$comp[cc]))^2*(data$adjust.sd[cc])
  # make it a standard deviation again
  data$adjust.sd[cc] <- sqrt(data$adjust.sd[cc])
  # add on variance
  data$var[cc] <- data$var[cc] + data$adjust.sd[cc]^2
}

## logit is currently correct
if (transform == "logit") { ## if logit
  # combine variances in log10 space:

  #############
  ## NOTE
  #############
  # We might want to calculate the combined variance in the following way, we should compare the two...
  # Let X be a random variable equal to the estimate of completeness unadjusted 45q15, and let Y be a random variable equal to the completeness estimate
  # Then the variable of interest is the completeness adjusted 45q15, which equals X/Y. So we want Var(X/Y).
  # Var(X/Y) ~ ((E(Y)^2)*Var(X) + (E(X)^2)*Var(Y))/E(Y)^4, this is derived in part using the delta method
  # Reference: http://stats.stackexchange.com/questions/32659/variance-of-x-y
  # The line of code would be:
  # data$totvar[cc] <- ((log(data$comp[cc], 10)^2)*data$varlog10qx[cc] + (log(data$obs45q15[cc], 10)^2*data$adjust.sd[cc]^2))/(log(data$obs45q15[cc], 10))^4
  # For now, we will add them, because we want the Var(log(X/Y)) = Var(log(X) + log(Y)) = Var(log(X)) + Var(log(Y))
  data$totvar <- 0
  data$totvar[cc] <- data$varlog10qx_unadjust[cc] + data$adjust.sd[cc]^2
  # transform from log base 10 space to normal space using delta method
  data$totvar[cc] <- (10^(log(data$mort[cc], 10))*log(10))^2*(data$totvar[cc])
  # transform from normal space to logit space using delta method
  data$var[cc] <- (1/(data$mort[cc]*(1-data$mort[cc])))^2*(data$totvar[cc])
}

## logit10 is currently correct
if (transform == "logit10") { ## if logit10
  # combine variances in log10 space:

  #############
  ## NOTE
  #############
  # We might want to calculate the combined variance in the following way, we should compare the two...
  # Let X be a random variable equal to the estimate of completeness unadjusted 45q15, and let Y be a random variable equal to the completeness estimate
  # Then the variable of interest is the completeness adjusted 45q15, which equals X/Y. So we want Var(X/Y).
  # Var(X/Y) ~ ((E(Y)^2)*Var(X) + (E(X)^2)*Var(Y))/E(Y)^4, this is derived in part using the delta method
  # Reference: http://stats.stackexchange.com/questions/32659/variance-of-x-y
  # The line of code would be:
  # data$totvar[cc] <- ((log(data$comp[cc], 10)^2)*data$varlog10qx[cc] + (log(data$obs45q15[cc], 10)^2*data$adjust.sd[cc]^2))/(log(data$obs45q15[cc], 10))^4
  # For now, we will add them, because we want the Var(log(X/Y)) = Var(log(X) + log(Y)) = Var(log(X)) + Var(log(Y))
  data$totvar <- 0
  data$totvar[cc] <- data$varlog10qx_unadjust[cc] + data$adjust.sd[cc]^2    # transform from log base 10 space to normal space using delta method
  # transform from log base 10 space to normal space using delta method
  data$totvar[cc] <- (10^(log(data$mort[cc], 10))*log(10))^2*(data$totvar[cc])
  # transform from normal space to logit10 space using delta method
  data$var[cc] <- (1/(data$mort[cc]*(1-data$mort[cc])*log(10)))^2*(data$totvar[cc])
}

## change as of 4/4/17 - use MAD estimator of census, household, and sibs data to add nonsampling variance
diff <- logit(data$mort) - logit(data$pred.2.final)
## household
data$var_source[data$source_type %in%  c("moh survey", "household", "household_deaths", "sspc", "survey", "dc") & data$data == 1] <- "household"
data$var_source[data$source_type %in%  c("census", "susenas") & data$data == 1] <- "census"
data$var_source[data$category %in%  c("sibs") & data$data == 1] <- "sibs"
cc <- (!is.na(data$var_source))
mad <- tapply(diff[cc], data$var_source[cc], function(x) median(abs(x - median(x))))
for (ii in names(mad)) data$x[data$var_source == ii] <- mad[ii]
data$var[cc] <- (1.4826*data$x[cc])^2


## Multiply DSS variance by 5 to increase variance
data$var[data$category %in% c("dss") & data$data == 1] <- data$var[data$category %in% c("dss") & data$data == 1] * 10

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


################
# Save output
################
data <- data[, c("location_id", "location_name", "super_region_name",
                "region_name", "ihme_loc_id", "sex", "year", "LDI_id",
                "mean_yrs_educ", "hiv", "data", "type", "category", "vr",
                "mort", "stderr", "log_mort", "log_stderr", "pred.1.wRE",
                "pred.1.noRE", "resid", "pred.2.resid", "pred.2.final", "mse",
                "data_density")]
data <- data[order(data$ihme_loc_id, data$sex, data$year, data$data), ]

filename <- "FILEPATH"
write.csv(data, file=filename, row.names=F)
