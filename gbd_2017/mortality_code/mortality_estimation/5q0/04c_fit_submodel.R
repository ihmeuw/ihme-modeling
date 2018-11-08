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


# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of 5q0')
parser$add_argument('--submodel_id', type="integer", required=TRUE,
                    help='The submodel_id for this submodel of 5q0')
parser$add_argument('--input_location_ids', type="integer", required=TRUE,
                    nargs='+', help='The input ids for this submodel of 5q0')
parser$add_argument('--output_location_ids', type="integer", required=TRUE,
                    nargs='+', help='The output ids for this submodel of 5q0')
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
submodel_id <- args$submodel_id
input_location_ids <- args$input_location_ids
output_location_ids <- args$output_location_ids
gbd_round_id <- args$gbd_round_id
start_year <- args$start_year
end_year <- args$end_year
st_loess <- args$st_loess


# Set core directories
code_dir <- "FILEPATH"
output_dir <- "FILEPATH"


# Get hyperparameters
spacetime_parameters_file <- "FILEPATH"
spacetime_parameters <- "FILEPATH"


# Get data
dc = DataContainer$new(gbd_round_id = gbd_round_id, start_year = start_year,
                       end_year = end_year, output_dir = output_dir)
location_data <- dc$get('location')
data <- dc$get_submodel(submodel_id, 'bias_adjusted')



########################
# Fit second stage model - space time loess of residuals
########################
data <- data[order(ihme_loc_id, year),]

# calculate residuals from final first stage regression
data$resid <- logit(data$mort2) - logit(data$pred.1b)

# Get spacetime locations (have fake regions to deal with running subnationals)
st_locs <- get_spacetime_loc_hierarchy(prk_own_region=F, gbd_round_id = 5)

# try just having one residual for each country-year with data for space-time, so as not to give years with more data more weight
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

dc$save_submodel(stdata, submodel_id, 'spacetime_data')


# Fit spacetime model
preds <- resid_space_time(data=stdata, params=spacetime_parameters,
                          st_loess = st_loess)
preds <- preds[preds$keep==1,]
preds <- preds[!colnames(preds) %in% "keep"]

dc$save_submodel(preds, submodel_id, 'spacetime_prediction')

data <- merge(data, preds, by=c("location_id", "year"))

data$pred.2.resid <- inv.logit(data$pred.2.resid)
data$pred.2.final <- inv.logit(logit(data$pred.2.resid) + logit(data$pred.1b))


######################
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
high_data_density <- copy(data[complete_vr_deaths >= 50,])
high_data_density$diff <- logit(high_data_density$pred.2.final) - logit(high_data_density$mort)
variance_diff <- tapply(high_data_density$diff, high_data_density$ihme_loc_id, function(x) var(x, na.rm=T))
print(variance_diff)
amp_value <- mean(variance_diff)
print(amp_value)
amp_value <- 0.01
data[, mse := amp_value]


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

#delta method, log(mx) space to qx space!!!!
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
#data <- data[, keep_cols]
data <- data[order(data$ihme_loc_id, data$year, data$data), ]
dc$save_submodel(data, submodel_id, 'spacetime_output')
