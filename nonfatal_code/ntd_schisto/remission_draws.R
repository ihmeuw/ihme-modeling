# params
lengthPCT <- 1 
minDuration <- 3 
maxDuration <- 5 
mda_start <- 2006
mda_data <- read.csv("FILEPATH",stringsAsFactors=FALSE) 
locations <- read.csv("FILEPATH")
nid <- 301927 
bundle_id <- 1046
cause_name <- "schisto"
geographic_restrictions <- read.csv("FILEPATH") 

library(dplyr) # load package
library(stats) # load package
library(matrixStats) # load package
source("FILEPATH") # load when_mda_draw function
source("FILEPATH") # load pre_mda_draw function

################
# LOAD IN DATA #
################
mda_data <- read.csv("FILEPATH",stringsAsFactors=FALSE) # read in MDA data
max_age <- read.csv("FILEPATH",stringsAsFactors=FALSE) # max age associated with age bins
subnational_to_exclude <- read.csv("FILEPATH")

#################
# PREP MDA DATA #
#################
loc_info <-   read.csv("FILEPATH", stringsAsFactors=FALSE) # load location_ids
loc_info <- loc_info[,-c(3:28)] # clean dataset by keeping only columns we need
mda_data$Age.group <- as.character(mda_data$Age.group)
mda_data <- left_join(mda_data, loc_info, by = "location_name") # merge datasets

######################
# Remission with MDA #
######################
remission_estimates <- list()
remission_non_treated_age <- list()
age_start <- c()
age_end <- c()
location_id <- c()
year_id <- c()
size_of_these_things <- 0

for(j in 1:length(mda_data$location_name)) {
  maxAge <- mda_data$age_end[j] 
  if (mda_data$split_SAC_Adult[j] == 0) {  ## if data was originally split
    number_requiring <- mda_data$Population.requiring.PC.for.SCH.annually[j]
    number_treated <- mda_data$Reported.number.of.people.treated[j]
    remission_estimates[[j]] <- when_mda_draw(number_treated, number_requiring, mda_data$age_end[j], lengthPCT, minDuration, maxDuration)
    if (mda_data$Age.group[j] == "SAC and Adults") {
      max <- 4
      age_start[j] <- 0 
      age_end[j] <- 4   
      remission_non_treated_age[[j]] <- pre_mda_draw(max, lengthPCT, minDuration, maxDuration)
    } else if (mda_data$Age.group[j] == "Adults") {
      max <- 14
      age_start[j] <- 0 
      age_end[j] <- 14   
      remission_non_treated_age[[j]] <- pre_mda_draw(max, lengthPCT, minDuration, maxDuration)
    } else  {
      max <- 4
      age_start[j] <- 0 
      age_end[j] <- 4   
      remission_non_treated_age[[j]] <- pre_mda_draw(max, lengthPCT, minDuration, maxDuration) 
   }
  } else { ## if when cleaning you have to split adults and children
    if (mda_data$Age.group[j] == "SAC") {
      number_requiring <- mda_data$SAC.population.requiring.PC.for.SCH.annually[j]
      number_treated <- mda_data$Reported.number.of.SAC.treated[j]
      max <- 4
      remission_non_treated_age[[j]] <- pre_mda_draw(max, lengthPCT, minDuration, maxDuration)
      age_start[j] <- 0
      age_end[j] <- 4
    } else {
      number_requiring <- mda_data$Population.requiring.PC.for.SCH.annually[j]-mda_data$SAC.population.requiring.PC.for.SCH.annually[j]
      number_treated <- mda_data$Reported.number.of.people.treated[j]-mda_data$Reported.number.of.SAC.treated[j]
      remission_non_treated_age[[j]] <- list(rep(0, 1000))
      age_start[[j]] <- 0 
      age_end[j] <- 0 
      }
    remission_estimates[[j]] <- when_mda_draw(number_treated, number_requiring, mda_data$age_end[j], lengthPCT, minDuration, maxDuration)
   
    }  
}

remission_estimates_v2 <- data.frame(matrix(unlist(remission_estimates), nrow=265, byrow=T), 
                                     stringsAsFactors=FALSE) # unlist the list so you can append to mda_data

mda_data <- bind_cols(mda_data, as.data.frame(remission_estimates_v2)) # bind mda_data frame and estimates


remission_estimates_v3 <- data.frame(matrix(unlist(remission_non_treated_age), nrow=length(remission_non_treated_age), byrow=T),
                                     stringsAsFactors=FALSE) # unlist the list so you can append to mda_data
remission_estimates_v4 <- as.data.frame(age_start)
remission_estimates_v5 <- as.data.frame(age_end)

nooo_mda_data <- bind_cols(as.data.frame(remission_estimates_v3), remission_estimates_v4, 
                           remission_estimates_v5)

# Aggregate draws
cols <- c(colnames(nooo_mda_data)[grep('X',colnames(nooo_mda_data))])
nooo_mda_data$mean_draws <- rowMeans(nooo_mda_data[,cols])   # use grep to get column names
nooo_mda_data <- nooo_mda_data %>% mutate(stand_error = rowSds(as.matrix(nooo_mda_data[,cols])/1000))

# format MDA data draws
format_no_mda_data <- data.frame(mean = nooo_mda_data$mean_draws,
                              standard_error = nooo_mda_data$stand_error,
                              location_id = mda_data$location_id,
                              age_start = nooo_mda_data$age_start,
                              age_end = nooo_mda_data$age_end,
                              year_start=mda_data$year,
                              year_end=mda_data$year)

# Aggregate draws 
cols <- c(colnames(mda_data)[grep('X',colnames(mda_data))])
mda_data$mean_draws <- rowMeans(mda_data[,cols])   ######### use grep to get column names
mda_data <- mda_data %>% mutate(stand_error = rowSds(as.matrix(mda_data[,cols])/1000))

# format MDA data draws
format_mda_data <- data.frame(mean = mda_data$mean_draws,
                              standard_error = mda_data$stand_error,
                              location_id = mda_data$location_id,
                              age_start = mda_data$age_start,
                              age_end = mda_data$age_end,
                              year_start=mda_data$year,
                              year_end=mda_data$year)

format_mda_data <- bind_rows(format_mda_data, format_no_mda_data)



#####################
# REMISSION PRE-MDA #
#####################
uniq_locs <- unique(locations$location_id)
uniq_locs <- append(uniq_locs, 152)
uniq_locs <- append(uniq_locs, 196)

unique_locations <- length(unique(locations$location_id))+2


remission_estimates <- list()
for(j in 1:unique_locations) {
  maxAge <- 99 
  remission_estimates[[j]] <- pre_mda_draw(maxAge, lengthPCT, minDuration, maxDuration)
}  

remission_estimates_pre_mda <- data.frame(matrix(unlist(remission_estimates), nrow=unique_locations,
                                                 byrow=T),stringsAsFactors=FALSE) # unlist the list so you can append to mda_data
remission_estimates_pre_mda$year_start <- rep(1990, unique_locations)
remission_estimates_pre_mda$year_end <- rep(mda_start-1, unique_locations)
remission_estimates_pre_mda$age_start <- rep(0, unique_locations)
remission_estimates_pre_mda$age_end <- rep(99, unique_locations)
remission_estimates_pre_mda$location_id <- uniq_locs

# Aggregate draws             
remission_estimates_pre_mda$mean <- rowMeans(remission_estimates_pre_mda[,1:1000])
remission_estimates_pre_mda <- remission_estimates_pre_mda %>% mutate(standard_error = rowSds(as.matrix(remission_estimates_pre_mda[1:1000]))/1000)


# Delete Individual Draws 
remission_estimates_pre_mda <- remission_estimates_pre_mda[,-c(1:1000)]

################################################################################
# REMISSION countries post-mda within geographic restrictions but no-MDA data  #
################################################################################

subnational_to_exclude <- read.csv("FILEPATH")

vec_locations <- subset(locations, !location_id %in% format_mda_data$location_id)$location_id
vec_locations <- append(vec_locations, 152) # add location
vec_locations <- append(vec_locations, 196) # add location


remission_estimates <- list() 
for(j in 1:length(unique(vec_locations))) {
  maxAge <- 99 
  remission_estimates[[j]] <- pre_mda_draw(maxAge, lengthPCT, minDuration, maxDuration)
}  

remission3 <- data.frame(matrix(unlist(remission_estimates), nrow=length(unique(vec_locations)), byrow=T), 
                         stringsAsFactors=FALSE) # unlist the list so you can append to mda_data
remission3$year_start <- rep(mda_start, length(unique(vec_locations))) 
remission3$year_end <- rep(2016, length(unique(vec_locations)))
remission3$age_start <- rep(0, length(unique(vec_locations)))
remission3$age_end <- rep(99, length(unique(vec_locations)))
remission3$location_id <- unique(vec_locations)

###################
# Aggregate draws #
###################
remission3$mean <- rowMeans(remission3[,1:1000])
remission3 <- remission3 %>% mutate(standard_error = rowSds(as.matrix(remission3[1:1000]))/1000)

###########################
# Delete Individual Draws #
###########################
remission3 <- remission3[,-c(1:1000)]

remission3 <- filter(remission3, !location_id %in% subnational_to_exclude$location_id)

#########################
# Fix MDA missing ages  #
#########################
extra <- list()
age_start <- c()
age_end <- c()
location_id <- c()
year_id <- c()

for(j in 1:length(mda_data$location_name)) {
  if (mda_data$split_SAC_Adult[j] == 0) {  ## if data was originally split
     if (mda_data$Age.group[j] == "SAC") {
      max <- 15
      age_start[j] <- 15 
      age_end[j] <- 99
      location_id[j] <- mda_data$location_id[j]
      year_id[j] <- mda_data$year[j]
      extra[[j]] <- pre_mda_draw(max, lengthPCT, minDuration, maxDuration)
     } else {
       age_start[j] <- 0
       age_end[j] <- 0 
       location_id[j] <- mda_data$location_id[j]
       year_id[j] <- mda_data$year[j]
       extra[[j]] <- list(rep(0, 1000))
    }
  } else {
    age_start[j] <- 0
    age_end[j] <- 0   
    location_id[j] <- mda_data$location_id[j]
    year_id[j] <- mda_data$year[j]
    extra[[j]] <- list(rep(0, 1000))
  }
}

remission_estimates_v3 <- data.frame(matrix(unlist(remission_non_treated_age), nrow=length(extra), byrow=T),
                                     stringsAsFactors=FALSE) # unlist the list so you can append to mda_data
remission_estimates_v4 <- as.data.frame(age_start)
remission_estimates_v5 <- as.data.frame(age_end)
remission_estimates_v6 <- as.data.frame(location_id)
remission_estimates_v7 <- as.data.frame(year_id)

nooo_mda_data <- bind_cols(as.data.frame(remission_estimates_v3), remission_estimates_v4, 
                           remission_estimates_v5, remission_estimates_v6, remission_estimates_v7)

# Aggregate draws
cols <- c(colnames(nooo_mda_data)[grep('X',colnames(nooo_mda_data))])
nooo_mda_data$mean_draws <- rowMeans(nooo_mda_data[,cols])   # use grep to get column names
nooo_mda_data <- nooo_mda_data %>% mutate(stand_error = rowSds(as.matrix(nooo_mda_data[,cols])/1000))

# format MDA data draws
format_no_mda_data <- data.frame(mean = nooo_mda_data$mean_draws,
                                 standard_error = nooo_mda_data$stand_error,
                                 location_id = nooo_mda_data$location_id,
                                 age_start = nooo_mda_data$age_start,
                                 age_end = nooo_mda_data$age_end,
                                 year_start=nooo_mda_data$year_id,
                                 year_end=nooo_mda_data$year_id)

format_no_mda_data <- filter(format_no_mda_data, age_start > 0)


############################
# PREPARE FOR EPI UPLOADER #
############################
dataframe1 <- format_mda_data
dataframe2 <- remission_estimates_pre_mda
dataframe3 <- remission3
dataframe4 <- format_no_mda_data

epi_uploader <- bind_rows(dataframe1,dataframe2,dataframe3, dataframe4)

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


epi_uploader <- filter(epi_uploader, standard_error != 0)
epi_uploader <- filter(epi_uploader, !location_id %in% subnational_to_exclude$location_id)
epi_uploader <- filter(epi_uploader, !location_id %in% geographic_restrictions$location_id)


write.csv(epi_uploader, "FILEPATH", row.names = FALSE)                          


