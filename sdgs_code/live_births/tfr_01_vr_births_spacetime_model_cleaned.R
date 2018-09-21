################################################################################
## Purpose: Applies spatiotemporal smoothing procedure to VR completeness model based on intercensal survival
################################################################################

################################################################################
### Setup
################################################################################

rm(list=ls())

# load packages 
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, magrittr, lme4, ggplot2) # load packages and install if not installed
 

username <- ifelse(Sys.info()[1]=="Windows","[username]",Sys.getenv("USER"))
j <- "FILEPATH"
h <- "FILEPATH"

################################################################################
### Arguments 
################################################################################
locsetid <- 21
  
################################################################################ 
### Paths for input and output 
################################################################################

censusdir <- "FILEPATH"
loessdir <- "FILEPATH"
tfrdir <- "FILEPATH"

output_path  <- "FILEPATH"

################################################################################
### Functions 
################################################################################
setwd("FILEPATH")
source("FILEPATH")

setwd("FILEPATH")
source("FILEPATH")


################################################################################
### Data 
################################################################################

setwd(censusdir)
census_ratios <- fread("FILEPATH")

setwd(loessdir) 
loess <- fread("FILEPATH")

setwd(tfrdir)
tfr <- fread("FILEPATH")

locs <- get_location_metadata(location_set_id = locsetid)

################################################################################
### Code 
################################################################################

#######################
## Step 1: Select Loess We Want -- Both Sex, National Level
#######################
loess <- loess[(iso3_sex_source %like% "both" & iso3_sex_source %like% "&&VR|VR_pre2011")] 

census_ratios <- merge(census_ratios[, .(location_id, year_id, log_implied_to_census = log(implied_to_census_ratio))], locs[, .(location_id, ihme_loc_id, region_id, super_region_id)], by = "location_id")

data <- merge(census_ratios, loess[, .(ihme_loc_id, year_id = year, u5_comp_pred)], by = c("ihme_loc_id", "year_id"))


########################
## Step 2: Fit model
########################
model_form <- as.formula("log_implied_to_census~ u5_comp_pred + (1|region_id) + (1|super_region_id)")

model <- lmer(model_form, data = data)

#########################
## Step 3: Predict using locations from which we have TFR VR data
#########################

tfr <- tfr[vital == T, .(location_id = unique(location_id))]

tfr <- locs[location_id %in% tfr$location_id & level == 3, .(ihme_loc_id, region_id, super_region_id)]

predict_locs <- merge(tfr, loess[, .(ihme_loc_id, year_id = year, u5_comp_pred)], by = "ihme_loc_id", all.x = T)

predict_locs[, log_implied_to_census_pred := predict(model, newdata = predict_locs, allow.new.levels = T)]

#########################
## Step 4: Convert back to normal space, merge on data
#########################

predict_locs[, implied_to_census_pred := exp(log_implied_to_census_pred)]
predict_locs <- merge(predict_locs, census_ratios[, .(ihme_loc_id, year_id, log_implied_to_census, implied_to_census = exp(log_implied_to_census))], by = c("ihme_loc_id", "year_id"), all.x = T)


#########################
## Step 5: Space-time, if necessary 
#########################

predict_locs[, resid := implied_to_census - implied_to_census_pred]
predict_locs[, iso3 := super_region_id]
predict_locs[, data_rich := F]
predict_locs[, vital := F]

resids_sr <- resid_space_time(data = predict_locs[, .(iso3, ihme_loc_id, year = year_id, resid, vital, data_rich)] , min_year= 1950, max_year= 2016, lambda= .9, zeta = .95, lambda_by_density = F, lambda_by_year = F, custom = T) %>% as.data.table 
resids_sr <- resids_sr[, .(ihme_loc_id, year_id = year, pred2resid_sr = pred.2.resid)]

predict_locs[, c("iso3", "data_rich", "vital") := NULL]

smoothed <- merge(predict_locs, resids_sr, by = c("ihme_loc_id", "year_id"), all.x = T)

smoothed[, log_implied_to_census_pred_smooth_sr := log_implied_to_census_pred + pred2resid_sr]
smoothed[, implied_to_census_pred_smooth_sr := exp(log_implied_to_census_pred_smooth_sr)]

#########################
## Step 5: Clean and Save
#########################
final <- copy(smoothed[implied_to_census_pred_smooth_sr < 1, .(ihme_loc_id, year_id, smoothed_vr_to_census = implied_to_census_pred_smooth_sr, vital = T)])
write.csv(final, "FILEPATH")


################################################################################ 
### End
################################################################################