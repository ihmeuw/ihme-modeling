# params
lengthPCT <- 1 
minDuration <- 9 
maxDuration <- 11 
mda_start <- 1987
mda_data <- read.csv("FILEPATH",stringsAsFactors=FALSE) 
locations <- read.csv("FILEPATH")
nid <- 301934
bundle_id <- 1058
cause_name <- "oncho"
geographic_restrictions <- read.csv("FILEPATH") 
######################################

library(dplyr) # load package
library(stats) # load package
library(matrixStats) # load package
source("FILEPATH") # when_mda_draw function
source("FILEPATH") # load pre_mda_draw


subnational_to_exclude <- read.csv("FILEPATH")

#################
# PREP MDA DATA #
#################
loc_info <-   read.csv("FILEPATH", stringsAsFactors=FALSE) # load location_ids
loc_info <- loc_info[,-c(3:28)] # keep only columns we need
mda_data <- left_join(mda_data, loc_info, by = "location_name") # merge datasets

######################
# Remission with MDA #
######################
remission_estimates <- list() 

for(j in 1:length(mda_data$location_name)) {
  maxAge <- mda_data$age_end[j] 
  number_requiring <- mda_data$num_requiring[j]
  number_treated <- mda_data$num_treated[j]
  remission_estimates[[j]] <- when_mda_draw(number_treated, number_requiring, maxAge, lengthPCT, minDuration, maxDuration)
}

remission_estimates_v2 <- data.frame(matrix(unlist(remission_estimates), nrow=length(remission_estimates), byrow=T), 
                                     stringsAsFactors=FALSE) # unlist the list so you can append to mda_data
mda_data <- bind_cols(mda_data, as.data.frame(remission_estimates_v2)) # bind mda_data frame and estimates

# columns to be aggregated 
cols <- c(colnames(mda_data)[grep('X',colnames(mda_data))])
mda_data$mean_draws <- rowMeans(mda_data[,cols])   # use grep to get column names
mda_data <- mda_data %>% mutate(stand_error = rowSds(as.matrix(mda_data[,cols])/1000))

# format MDA data draws
format_mda_data <- data.frame(mean = mda_data$mean_draws,
                              standard_error = mda_data$stand_error,
                              location_id = mda_data$location_id,
                              age_start = mda_data$age_start,
                              age_end = mda_data$age_end,
                              year_start=mda_data$year,
                              year_end=mda_data$year)

################################################################################
# REMISSION countries post-mda within geographic restrictions but no-MDA data  #
################################################################################

vec_locations <- subset(locations, !location_id %in% format_mda_data$location_id)$location_id

remission_estimates <- list() # intialize a list
for(j in 1:length(unique(vec_locations))) {
  maxAge <- 99 
  remission_estimates[[j]] <- pre_mda_draw(maxAge, lengthPCT, minDuration, maxDuration)
}  

remission3 <- data.frame(matrix(unlist(remission_estimates), nrow=length(unique(vec_locations)), byrow=T), 
                         stringsAsFactors=FALSE) # unlist the list so you can append to mda_data
remission3$year_start <- rep(2005, length(unique(vec_locations))) 
remission3$year_end <- rep(2016, length(unique(vec_locations)))
remission3$age_start <- rep(0, length(unique(vec_locations)))
remission3$age_end <- rep(99, length(unique(vec_locations)))
remission3$location_id <- unique(vec_locations)

remission3 <- subset(remission3, !location_id %in% subnational_to_exclude$location_id)

###################
# Aggregate draws #
###################
remission3$mean <- rowMeans(remission3[,1:1000])
remission3 <- remission3 %>% mutate(standard_error = rowSds(as.matrix(remission3[1:1000]))/1000)

###########################
# Delete Individual Draws #
###########################
remission3 <- remission3[,-c(1:1000)]

############################
# PREPARE FOR EPI UPLOADER #
############################
dataframe1 <- format_mda_data
dataframe3 <- remission3

epi_uploader <- bind_rows(dataframe1,dataframe3)

epi_uploader$nid <- nid
epi_uploader$sex <- "Both"
epi_uploader$source_type <- "Unidentifiable"
epi_uploader$unit_type <- "Person"
epi_uploader$unit_value_as_published <- 1
epi_uploader$measure_adjustment <- 0
epi_uploader$urbanicity_type <- "Mixed/both" 
epi_uploader$representative_name <- "Nationally representative only"
epi_uploader$recall_type <- "Point"
epi_uploader$extractor <- "user"
epi_uploader$is_outlier <- 0
epi_uploader$smaller_site_unit <- 0
epi_uploader$sex_issue <- 0
epi_uploader$year_issue <- 0
epi_uploader$age_issue <- 0
epi_uploader$age_demographer <- 0
epi_uploader$measure <- "remission"
epi_uploader$measure_issue <- 0
epi_uploader$bundle_id <- bundle_id

write.csv(epi_uploader,"FILEPATH", row.names = FALSE)                          





