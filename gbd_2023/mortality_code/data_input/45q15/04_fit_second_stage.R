################################################################################
## Description: Run the second stage model (ST), prep for GPR
################################################################################

### SETUP ====================================================

rm(list=ls())

# Load packages and functions
library(pacman)
p_load(foreign, zoo, nlme, data.table, plyr, reshape, ggplot2, readr, argparse)
library(mortdb, lib.loc = "FILEPATH")

# Parse arguments
if(!interactive()) {
  parser <- ArgumentParser()
  parser$add_argument('--version_id', type="integer", required=TRUE,
                      help='The version_id for this run')
  parser$add_argument('--sim', type="integer", required=TRUE,
                      help='Sim for this run')
  parser$add_argument('--hivsims', type="integer", required=TRUE,
                      help='HIV toggle')
  parser$add_argument('--gbd_year', type="integer", required=TRUE,
                      help="GBD year")
  parser$add_argument('--end_year', type="integer", required=TRUE,
                      help="last year we produce estimates for")
  parser$add_argument('--code_dir', type="character", required=TRUE,
                      help="Directory where 45q16 code is cloned")

  args <- parser$parse_args()
  list2env(args, .GlobalEnv)

  hivsims <- as.logical(hivsims)

} else {

  version_id <-
  sim <-
  hivsims <-
  gbd_year <-
  end_year <-
  code_dir <- "FILEPATH"

}

transform <- "logit"

# load space-time function
source(paste("FILEPATH"))

# Create directories
output_dir <- paste0("FILEPATH")
dir.create(paste0(output_dir), showWarnings = FALSE)
dir.create(paste0("FILEPATH"), showWarnings = FALSE)

### LOAD INPUTS ====================================================

# Load first stage results
if (hivsims) {
  input_file <- paste0("FILEPATH")
} else {
  input_file <- paste0("FILEPATH")
}
data <- fread(input_file)
data <- as.data.frame(data)

# Load in data density
parameter_data <- as.data.frame(fread(paste0("FILEPATH")))

# Set countries that we have subnational estimates for
locations <- fread(paste0("FILEPATH"))
locs_gbd2013 <- unique(locations$ihme_loc_id[!is.na(locations$local_id_2013)])

st_locs <- get_spacetime_loc_hierarchy(prk_own_region=F, gbd_year = gbd_year)

types <- ddply(.data=data, .variables=c("ihme_loc_id", "sex"), .inform=T,
               .fun=function(x) {
                 cats <- unique(x$category)
                 cats <- cats[!is.na(cats)]
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

### FIT 2nd STAGE MODEL ====================================================

# Calculate residuals from final first stage regression
data$resid <- logit(data$mort) - logit(data$pred.1.noRE)

# Merge on fake regions
data$ihme_loc_id <- as.character(data$ihme_loc_id)
data$region_name <- NULL
data$location_id <- NULL
data <- merge(data, st_locs, all.x=T, allow.cartesian = T, by="ihme_loc_id")

# Do space time regression
preds <- resid_space_time(data, param_data = parameter_data, max_year = end_year)

# Remove fake regions
preds <- preds[preds$keep==1, ]
preds <- preds[!colnames(preds) %in% "keep"]

# Merge predictions onto data
data <- merge(data, preds, by=c("ihme_loc_id", "sex", "year"))
data <- data[data$keep == 1, ]
data <- data[!colnames(data) %in% "keep"]

# Inverse logit, add predicted smoothed residual to 1st stage predictions
data$pred.2.resid <- inv.logit(data$pred.2.resid)
data$pred.2.final <- inv.logit(logit(data$pred.2.resid) + logit(data$pred.1.noRE))

### GPR INPUTS ====================================================

# Merge on data density
data <- merge(data, parameter_data[, c('ihme_loc_id', 'sex', 'data_density')],
              by=c('ihme_loc_id', 'sex'), all.x = T)

# Subset to high data density (>= 50) and the year 1990 or later
high_data_density <- data[data$data_density >= 50 & data$year >= 1990,]
if (nrow(high_data_density) < 1) stop("There are no rows where data density is at least 50.")

# Amplitude
high_data_density$diff <- logit(high_data_density$pred.2.final) - logit(high_data_density$pred.1.noRE)
variance_diff <- tapply(high_data_density$diff, high_data_density$ihme_loc_id, function(x) var(x, na.rm=T))
amp_value <- mean(variance_diff)
data$mse <- amp_value

# Replace mse with location-specific mse for complete vr locations
data <- as.data.table(data)

# Only use high-income national locations
nat_locs <- get_locations(level='estimate', gbd_year = gbd_year)
nat_locs <- nat_locs[level==3| ihme_loc_id %in% c('CHN_44533', 'CHN_361', 'CHN_354',
                                                  'GBR_433', 'GBR_434', 'GBR_4636', 'GBR_4749')]
nat_locs <- nat_locs[super_region_name == "High-income"]
nat_locs <- nat_locs[, .(ihme_loc_id)]

# Count number of vr sources by location
comp_vr <- data[, vr_count := sum(vr, na.rm=T), by = .(ihme_loc_id)]

# Subset data to complete vr locations and use data from 1990+ vr sources in mse calculation
comp_vr <- comp_vr[vr_count >= 40 & year>1990 & grepl("vr", source_type)]
comp_vr <- comp_vr[, diff := logit(pred.2.final) - logit(mort)]
comp_vr[, mse_vr := var(diff, na.rm = T), by=.(ihme_loc_id)]
comp_vr <- comp_vr[, .(ihme_loc_id, mse_vr)]
comp_vr <- unique(comp_vr)
comp_vr <- merge(comp_vr, nat_locs, by = "ihme_loc_id")

# Replace mse value for vr locations
data <- merge(data, comp_vr, by = c("ihme_loc_id"), all.x=T)
data <- data[!is.na(mse_vr), mse := mse_vr]
data <- data[, mse_vr := NULL]
data <- data[, vr_count := NULL]

# Got back to data.frame syntax
data <- as.data.frame(data)

# adjusted
data$mx <- log(1-data$mort)/(-45)
data$varmx <- (data$mx*(1-data$mx))/data$exposure # binomial
data$varqx <- (45*exp(-45*data$mx))^2*data$varmx # delta transform to normal qx space
data$varlog10qx <- (1/(data$mort * log(10)))^2*data$varqx # delta transform to log10 qx space
if (transform == "log10") data$var <- data$varlog10qx # set variance for log10 qx space
if (transform == "ln") data$var <- (1/(data$mort))^2*data$varqx # delta transform to natural log qx space
if (transform == "logit") data$var <-  (1/(data$mort*(1-data$mort)))^2*data$varqx ## if logit
if (transform == "logit10") data$var <-  (1/(data$mort*(1-data$mort)*log(10)))^2*data$varqx ## if logit10

# unadjusted
data$mx_unadjust <- log(1-data$obs45q15)/(-45)
data$varmx_unadjust <- (data$mx*(1-data$mx))/data$exposure
data$varqx_unadjust <- (45*exp(-45*data$mx))^2*data$varmx_unadjust # delta transform to normal qx space
data$varlog10qx_unadjust <- (1/(data$obs45q15 * log(10)))^2*data$varqx_unadjust # delta transform to log10 qx space for the addition of DDM variance
if (transform == "log10") data$var_unadjust <- data$varlog10qx_unadjust # set variance for log10 qx space
if (transform == "ln") data$var_unadjust <- (1/(data$obs45q15))^2*data$varqx_unadjust # delta transform to natural log qx space
if (transform == "logit") data$var_unadjust <-  (1/(data$obs45q15*(1-data$obs45q15)))^2*data$varqx_unadjust ## if logit
if (transform == "logit10") data$var_unadjust <-  (1/(data$obs45q15*(1-data$obs45q15)*log(10)))^2*data$varqx_unadjust ## if logit10

# Add in variance
cc <- (data$source_type %in% c("srs", "vr-ssa", "vr", "dsp") & data$category != 'complete')

# log10
if (transform == "log10") data$var[cc] <- data$var_unadjust[cc] + data$adjust.sd[cc]^2

# ln
if (transform == "ln") {
  # transform from log base 10 space to normal space
  data$adjust.sd[cc] <- (10^(log(data$comp[cc], 10))*log(10))^2*(data$adjust.sd[cc]^2)
  # transform from normal space to natural log space
  data$adjust.sd[cc] <- (1/(data$comp[cc]))^2*(data$adjust.sd[cc])
  # standard deviation
  data$adjust.sd[cc] <- sqrt(data$adjust.sd[cc])
  # add on variance
  data$var[cc] <- data$var[cc] + data$adjust.sd[cc]^2
}

# logit
if (transform == "logit") {
  data$totvar <- 0
  data$totvar[cc] <- data$varlog10qx_unadjust[cc] + data$adjust.sd[cc]^2
  # transform from log base 10 space to normal space
  data$totvar[cc] <- (10^(log(data$mort[cc], 10))*log(10))^2*(data$totvar[cc])
  # transform from normal space to logit space
  data$var[cc] <- (1/(data$mort[cc]*(1-data$mort[cc])))^2*(data$totvar[cc])
}

# logit10
if (transform == "logit10") {
  data$totvar <- 0
  data$totvar[cc] <- data$varlog10qx_unadjust[cc] + data$adjust.sd[cc]^2
  # transform from log base 10 space to normal space
  data$totvar[cc] <- (10^(log(data$mort[cc], 10))*log(10))^2*(data$totvar[cc])
  # transform from normal space to logit10 space
  data$var[cc] <- (1/(data$mort[cc]*(1-data$mort[cc])*log(10)))^2*(data$totvar[cc])
}

# add nonsampling variance
diff <- logit(data$mort) - logit(data$pred.2.final)
# household
data$var_source[data$source_type %in%  c("moh survey", "household", "household_deaths", "sspc", "survey", "dc") & data$data == 1] <- "household"
data$var_source[data$source_type %in%  c("census", "susenas") & data$data == 1] <- "census"
data$var_source[data$category %in%  c("sibs") & data$data == 1] <- "sibs"
cc <- (!is.na(data$var_source))
mad <- tapply(diff[cc], data$var_source[cc], function(x) median(abs(x - median(x))))
for (ii in names(mad)) data$x[data$var_source == ii] <- mad[ii]
data$var[cc] <- (1.4826*data$x[cc])^2

# Multiply DSS variance by 5
data$var[data$category %in% c("dss") & data$data == 1] <- data$var[data$category %in% c("dss") & data$data == 1] * 10

# convert estimate to log space
if (transform == "log10") data$log_mort <- log(data$mort, base=10)
if (transform == "ln") data$log_mort <- log(data$mort)
if (transform == "logit") data$log_mort <- logit(data$mort)
if (transform == "logit10") data$log_mort <- logit10(data$mort)
data$log_stderr <- sqrt(data$var)
data$log_var <- (data$log_stderr)^2
if (transform == "log10") data$stderr <- sqrt((10^(data$log_mort)*log(10))^2 * data$var) # delta transform back to normal qx space
if (transform == "ln") data$stderr <- sqrt(exp(data$log_mort)^2 * data$var) # delta transform back to normal qx space
if (transform == "logit") data$stderr <- sqrt((exp(data$log_mort)/(1+exp(data$log_mort)))^4 * data$var) # delta transform back to normal qx space
if (transform == "logit10") data$stderr <- sqrt(log(10)*(exp(data$log_mort)/(1+exp(data$log_mort)))^4 * data$var) # delta transform back to normal qx space


### SAVE OUTPUT =====================================================================

data <- data[, c("location_id", "location_name", "super_region_name",
                "region_name", "ihme_loc_id", "sex", "year", "LDI_id",
                "mean_yrs_educ", "hiv", "data", "type", "category", "vr",
                "mort", "stderr", "log_mort", "log_stderr", "log_var", "pred.1.wRE",
                "pred.1.noRE", "resid", "pred.2.resid", "pred.2.final", "mse",
                "data_density")]
data <- data[order(data$ihme_loc_id, data$sex, data$year, data$data), ]

if (hivsims) {
  filename <- paste0("FILEPATH")

} else {
  filename <- paste0("FILEPATH")
}
readr::write_csv(data, filename)
