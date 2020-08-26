################################################################################
## Description: Run the first stage of the prediction model for adult mortality
##              and calculate inputs to GPR
################################################################################

rm(list=ls())

# Import installed libraries
library(foreign)
library(zoo)
library(nlme)
library(plyr)
library(data.table)
library(devtools)
library(methods)
library(argparse)
library(readr)


# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of 5q0')
parser$add_argument('--gbd_round_id', type="integer", required=TRUE,
                    help='The gbd_round_id for this run of 5q0')
parser$add_argument('--start_year', type="integer", required=TRUE,
                    help='The starting year for this run of 5q0')
parser$add_argument('--end_year', type="integer", required=TRUE,
                    help='The ending year for this run of 5q0')
parser$add_argument('--st_loess', type="integer", required=TRUE,
                    help='Whether to use the st_loess')
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id
gbd_round_id <- args$gbd_round_id
start_year <- args$start_year
end_year <- args$end_year
st_loess <- args$st_loess


# Set core directories
root <- "FILEPATH"
username <- Sys.getenv("USER")
code_dir <- "FILEPATH"
output_dir <- "FILEPATH"

# Load the GBD specific libraries
source(paste0("FILEPATH/data_container.R"))
source(paste0(code_dir, "/choose_reference_categories.r"))
source(paste0(code_dir, "/space_time.r"))
library(mortdb, lib = "FILEPATH")

# Get hyperparameters
spacetime_parameters_file <- paste0(output_dir, "/data/assigned_hyperparameters.csv")
spacetime_parameters <- fread(spacetime_parameters_file)


# Get data
location_data <- fread(paste0(output_dir, "/data/location.csv"))
data <- fread(paste0(output_dir, "/model/bias_adjusted.csv"))



########################
# Fit second stage model - space time loess of residuals
########################
data <- data[order(ihme_loc_id, year),]

# calculate residuals from final first stage regression
data$resid <- logit(data$mort2) - logit(data$pred.1b)

# Get spacetime locations (have fake regions to deal with running subnationals)
st_locs <- fread(paste0(output_dir, "/data/st_locs.csv"))

stdata <- ddply(data, .(ihme_loc_id,year),
                function(x){
                  data.frame(region_name = x$region_name[1],
                             ihme_loc_id = x$ihme_loc_id[1],
                             year = x$year[1],
                             vr = max(x$vr),
                             resid = mean(x$resid))
                })

# Merge on spacetime regions
stdata <- stdata[,!colnames(stdata) %in% "region_name"]
stdata <- merge(stdata, st_locs, all.x=T, by="ihme_loc_id")

fwrite(stdata, paste0(output_dir, "/model/spacetime_data.csv"), na = "")


# Fit spacetime model
preds <- resid_space_time(data=stdata, params=spacetime_parameters,
                          st_loess = st_loess, max_year = end_year)
preds <- preds[preds$keep==1,]
preds <- preds[!colnames(preds) %in% "keep"]

fwrite(preds, paste0(output_dir, "/model/spacetime_prediction.csv"), na= "")

data <- merge(data, preds, by=c("location_id", "year"),all.x=T)

data$pred.2.resid <- inv.logit(data$pred.2.resid)
data$pred.2.final <- inv.logit(logit(data$pred.2.resid) + logit(data$pred.1b))
if(nrow(data[is.na(pred.2.resid)&ihme_loc_id!="PRK"])>0) stop()
data[is.na(pred.2.resid),pred.2.final:=pred.1b]


######################
## CHANGE ME
## to run GPR using data w/out survey RE included, uncomment
#causes mort to be adjusted, mort2 unadjusted, mort3 is intermediate step
data$mort3 <- data$mort2
data$mort2 <- data$mort
data$mort <- data$mort3

spacetime_parameters <- fread(spacetime_parameters_file)
data <- merge(data,
              data.table(spacetime_parameters[, c('location_id', 'data_density', 'complete_vr_deaths')]),
              by=('location_id'), all.x = T)
# Calculate the average amplitude value where data density >= 50
high_data_density <- copy(data[complete_vr_deaths >= 30 & year >= 1980,])
high_data_density$diff <- logit(high_data_density$pred.2.final) - logit(high_data_density$mort)
variance_diff <- tapply(high_data_density$diff, high_data_density$ihme_loc_id, function(x) var(x, na.rm=T))
amp_value <- mean(variance_diff)
print(amp_value)
data[, mse := amp_value]

nat_locs <- data.table(get_locations(level='estimate', gbd_year = get_gbd_year(gbd_round_id)))
nat_locs <- nat_locs[level==3| ihme_loc_id %in% c('CHN_44533', 'CHN_361', 'CHN_354',
                                                  'GBR_433', 'GBR_434', 'GBR_4636', 'GBR_4749')]
nat_locs <- nat_locs[super_region_name == "High-income"]
nat_locs <- nat_locs[, .(ihme_loc_id)]

comp_vr <- data[, vr_count := sum(vr, na.rm=T), by = .(ihme_loc_id)]
comp_vr <- comp_vr[vr_count >= 40 & year>1990 & grepl("vr", source.type)]
comp_vr <- comp_vr[, diff := logit(pred.2.final) - logit(mort)]
comp_vr[, mse_vr := var(diff, na.rm = T), by=.(ihme_loc_id)]
comp_vr <- comp_vr[, .(ihme_loc_id, mse_vr)]
comp_vr <- unique(comp_vr)
comp_vr <- merge(comp_vr, nat_locs, by = "ihme_loc_id")

data <- merge(data, comp_vr, by = c("ihme_loc_id"), all.x=T)
data <- data[!is.na(mse_vr), mse := mse_vr]
data <- data[, mse_vr := NULL]
data <- data[, vr_count := NULL]

###########################
#Get estimate of variance to add on in 03 from taking
#standard deviation of RE for all surveys of a given source-type
#also, try doing this over region
###########################
data[data == 0, source.type := NA]
data$adj.re2 <- data$re2 - data$mre2

data <- as.data.frame(data)
source_data <- data[!duplicated(data[, c("ihme_loc_id","source1")]) & data$data == 1,]
sds <- tapply(source_data$adj.re2, source_data[,c("source.type")], function(x) sd(x))
sds[is.na(sds)] <- 0
#,"region_name"
var <- sds^2

#merge var into data
for (st in names(sds)) {
  data$var.st[data$source.type == st] <- var[names(var) == st]
}

#delta method, log(mx) space to qx space
data$var.st.qx <- data$var.st * (exp(-5*data$mx2) *5* data$mx2)^2

# Merge on super_region_name
data <- merge(data, location_data[, c('location_id', 'super_region_name')],
              by = 'location_id')

# Write results
keep_cols <- c("location_id", "ihme_loc_id", "year", "ldi", "maternal_edu",
               "hiv", "data", "category", "corr_code_bias", "to_correct", "vr",
               "mort", "mort2", "mse", "pred.1b", "resid", "pred.2.resid",
               "pred.2.final", "ptid", "source1", "re2", "adjre_fe",
               "reference", "log10.sd.q5", "bias.var", "var.st.qx",
               "source_year", "source", "type", "source.type", "graphingsource",
               "ctr_re", "super_region_name", "region_name", "data_density")
data <- data[order(data$ihme_loc_id, data$year, data$data), ]
fwrite(data, paste0(output_dir, "/model/spacetime_output.csv"), na = "")
