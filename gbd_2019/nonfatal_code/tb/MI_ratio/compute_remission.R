## EMPTY THE ENVIRONMENT
rm(list = ls())

## SET UP FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
} else {
  ADDRESS <-"ADDRESS"
  ADDRESS <-paste0("ADDRESS/", Sys.info()[7], "/")
  ADDRESS <-"ADDRESS"
}

## LOAD FUNCTIONS AND PACKAGES
source(paste0(ADDRESS, "FILEPATH/upload_bundle_data.R"))
source(paste0(ADDRESS, "FILEPATH/get_covariate_estimates.R"))
source(paste0(ADDRESS, "FILEPATH/get_location_metadata.R"))
source(paste0(ADDRESS, "FILEPATH/get_demographics.R"))
library(writexl)
library(ggplot2)

#############################################################################################
###                               HELPER OBJECTS AND FUNCTIONS                            ###
#############################################################################################

## HELPER OBJECTS
date        <- "2019_11_18"
upload      <- F
decomp_step <- "step4"
out_dir     <- paste0(ADDRESS, "FILEPATH")

#############################################################################################
###                       INTERPOLATE DURATION AND COMPUTE REMISSION                      ###
#############################################################################################

## GET DEMOGRAICS TO ESTIMATE FOR
dems <- get_demographics(gbd_team = "epi")

## GET LOCATION DATA
locs <- get_location_metadata(location_set_id = 35)
locs <- locs[, .(location_id, ihme_loc_id, location_name)]

## GET NATIONAL LOCS
nats <- locs[ihme_loc_id %like% "_"]
nats[, parent := tstrsplit(ihme_loc_id, "_")[[1]]]
nats <- unique(nats$parent)

## FINAL PREDICTION LOCATIONS
locs <- locs[location_id %in% dems$location_id | ihme_loc_id %in% nats | ihme_loc_id %like% "GBR_" | ihme_loc_id == "IND_44538"]

## GET HAQ FOR ESTIMAION LOCATION-YEARS
haq <- get_covariate_estimates(covariate_id = 1099, 
                               location_id  = locs$location_id,
                               year_id      = min(dems$year_id):max(dems$year_id),
                               decomp_step  = decomp_step)

## CLEAN
haq <- haq[, .(location_id, location_name, year_id, mean_value, lower_value, upper_value)]
setnames(haq, old = c("mean_value", "lower_value", "upper_value"), new = c("haq", "low", "high"))

## GET REMISSION TABLE
remit <- data.table(haq = c(0, max(haq$high)), duration = c(3, 0.5))
haq   <- merge(haq, remit, all.x=T, all.y=T)

## GET INTERPOLATE VALUES
haq[, new_duration  := approx(x=haq, y=duration, xout=haq)$y]
haq[, duration_low  := approx(x=haq, y=duration, xout=high)$y]
haq[, duration_high := approx(x=haq, y=duration, xout=low)$y]

## CHECK FOR CORRECT INTERPOLATION
ggplot(data=haq, aes(x=haq, y=new_duration)) + 
  geom_point(size=3, alpha=0.6) + theme_bw() +
  labs(x="Healthcare access and quality index", y="Predicted duration") +
  theme(axis.text = element_text(size=22), axis.title = element_text(size=24))

## CLEAN
data <- haq[, .(location_id, year_id, new_duration, duration_low, duration_high)]
data <- data[order(location_id, year_id)]
data <- data[!is.na(location_id)]

## SAVE PREDICTED DURATION
write.csv(data, file = paste0(out_dir, "pred_duration_haq_", date, ".csv"), row.names = F)

## COMPUTE REMISSION
data[, `:=` (mean = (1/new_duration), lower = (1/duration_high), upper = (1/duration_low))]
data[, `:=` (new_duration = NULL, duration_low = NULL, duration_high = NULL)]
data[, standard_error := NA]

#############################################################################################
###                                      PREP FOR UPLOAD                                  ###
#############################################################################################

## GET LOCATION INFO
locs <- get_location_metadata(location_set_id = 35)
locs <- locs[, .(location_id, ihme_loc_id, location_name)]
data <- merge(locs, data, by="location_id")
data <- data[year_id %in% dems$year_id]

## GET AGE AND YEAR
data[, `:=` (year_start=year_id, year_end=year_id, age_start=0, age_end=100)]
data[, sex := "Male"]

## APPEND FEMALE COPY
female <- copy(data)
female[, sex := "Female"]
data   <- rbind(data, female)

## SAVE
writexl::write_xlsx(list(extraction = data), path = paste0(out_dir, "remission_haq_", date, ".xlsx"))

#############################################################################################
###                                         UPLOAD                                        ###
#############################################################################################

## UPLOAD TO BUNDLE
if (upload == T) {
  result <- upload_bundle_data(bundle_id   = 712, 
                               decomp_step = "iterative", 
                               filepath    = paste0(out_dir, "remission_haq_", date, ".xlsx"))
}

#############################################################################################
###                                          DONE                                         ###
#############################################################################################

