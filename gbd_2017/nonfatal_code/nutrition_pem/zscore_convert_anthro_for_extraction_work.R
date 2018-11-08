os <- .Platform$OS.type
if (os == "windows") {
  jpath <- FILEPATH
} else {
  jpath <- FILEPATH
}

library(data.table)
library(plyr)
library(parallel)
library(foreign)

## source functions
source("FILEPATH/get_location_metadata.R")

# set directories
data_dir <- FILEPATH
other_dir <- FILEPATH
other_dir2 <- FILEPATH
gbd_dir <- FILEPATH
save_dir <- FILEPATH
thesis_dir <- FILEPATH

# initiate timing
start_time <- Sys.time()

CHNS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
country_spec_mis_africa <- read.csv(FILEPATH, stringsAsFactors = FALSE)
country_spec_mis_africa <- subset(country_spec_mis_africa, country_spec_mis_africa$nid == "111815")
IDN <- read.csv(FILEPATH, stringsAsFactors = FALSE)
KIDS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
LSMS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
LSMS_to_keep <- c(1572, 7229, 7233, 7245, 7248, 9212, 9310, 9919, 10277, 45779, 45782, 45784, 45786, 45850)
LSMS <- LSMS[LSMS$nid %in% LSMS_to_keep,]
misc_DHS <- rread.csv(FILEPATH, stringsAsFactors = FALSE)
misc_DHS <- subset(misc_DHS, misc_DHS$nid == "21412")
MXFLS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
MXFLS <- subset(MXFLS, MXFLS$nid == "58419")
NHANES <- read.csv(FILEPATH, stringsAsFactors = FALSE)
NNMB <- read.csv(FILEPATH, stringsAsFactors = FALSE)
NNMB <- subset(NNMB, NNMB$nid == "129783" | NNMB$nid == "283811")
RLMS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
  
TTO <-  read.csv(FILEPATH, stringsAsFactors = FALSE)
WBPS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
WBPS <- subset(WBPS, WBPS$nid == "22930")
ZMBLCMS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
ZMBLCMS <- subset(ZMBLCMS, ZMBLCMS$nid == "13973")

surveys <- list(CHNS, country_spec_mis_africa, IDN, KIDS, LSMS, misc_DHS, MXFLS, NHANES, NNMB, RLMS, TTO, WBPS, ZMBLCMS)
all_data <- rbindlist(surveys, fill = TRUE)
rm(CHNS, country_spec_mis_africa, HSFE, IDN, KIDS, LSMS, misc_DHS, MXFLS, NHANES, NNMB, RLMS, SHS, TTO, WBPS, ZMBLCMS)

to_remove <- c(6614, 11160, 22286, 22306, 22317, 22329, 22341, 22463, 22930, 30368, 38496, 151802, 286801)
all_data <- all_data[!(all_data$nid %in% to_remove),]

all_data <- subset(all_data, all_data$age_yr < 6 | is.na(all_data$age_yr))
nids1 <- unique(all_data$nid)


all_data$interview_yr[all_data$nid == 7233] <- 1990 
all_data$interview_yr[all_data$nid == 10277] <- 1997
all_data$birth_yr[all_data$nid == 9919] <- paste0("19",all_data$birth_yr[all_data$nid == 9919])
all_data$birth_yr[all_data$nid == 10277] <- paste0("19",all_data$birth_yr[all_data$nid == 10277])


all_data$interview_date_cmc <- as.numeric(all_data$interview_date_cmc)
all_data$dob_cmc <- as.numeric(all_data$dob_cmc)
all_data$cmc_dff <- (all_data$interview_date_cmc - all_data$dob_cmc)*30.5
all_data$age_days[is.na(all_data$age_days)] <- all_data$cmc_dff[is.na(all_data$age_days)]

all_data$birth_day[is.na(all_data$birth_day)] <- 15
all_data$interview_day[is.na(all_data$interview_day)] <- 15

all_data$child_dob <- as.Date(with(all_data, paste(birth_yr, birth_mo, birth_day, sep="-")), "%Y-%m-%d")
all_data$interview_date <- as.Date(with(all_data, paste(interview_yr, interview_mo, interview_day, sep="-")), "%Y-%m-%d")
all_data$age_calc <- as.numeric(all_data$interview_date - all_data$child_dob)
all_data$age_days[is.na(all_data$age_days)] <- (all_data$age_calc[is.na(all_data$age_days)])


all_data$age_wks <- NA

all_data$age_wks[!is.na(all_data$age_days)] <- all_data$age_days[!is.na(all_data$age_days)]/7.0192

all_data$age_mo[!is.na(all_data$age_days)] <- all_data$age_days[!is.na(all_data$age_days)]/30.5

all_data$age_yr[!is.na(all_data$age_days)] <- all_data$age_days[!is.na(all_data$age_days)]/365

all_data$age_yr[is.na(all_data$age_days) & !is.na(all_data$age_mo)] <- all_data$age_mo[is.na(all_data$age_days) & !is.na(all_data$age_mo)]/12
all_data$age_wks[is.na(all_data$age_days) & !is.na(all_data$age_mo)] <- all_data$age_mo[is.na(all_data$age_days) & !is.na(all_data$age_mo)]*4.333
all_data$age_days[is.na(all_data$age_days) & !is.na(all_data$age_mo)] <- all_data$age_mo[is.na(all_data$age_days) & !is.na(all_data$age_mo)]*30.5

all_data <- subset(all_data, !is.na(all_data$age_days) & !is.na(all_data$age_wks) & !is.na(all_data$age_mo))

all_data$age_wks <- as.integer(round_any(all_data$age_wks, 1))
all_data$age_mo <- as.integer(round_any(all_data$age_mo, 1))


regions <- as.data.frame(get_location_metadata(location_set_id=35, gbd_round_id=5))
names(regions)[names(regions)=="super_region_name"] <- "region"
regions <- regions[c(3,17,20)]
regions <- subset(regions, !is.na(regions$ihme_loc_id))
all_data <- as.data.table(all_data)
regions <- as.data.table(regions)
all_data <- merge(all_data,regions, by="ihme_loc_id") 
all_data <- as.data.frame(all_data)


names(all_data)[names(all_data)=="start_year"] <- "year_start"
names(all_data)[names(all_data)=="end_year"] <- "year_end"
names(all_data)[names(all_data)=="survey_series"] <- "source"
names(all_data)[names(all_data)=="cluster_number"] <- "psu"
names(all_data)[names(all_data)=="sample_weight"] <- "pweight"
names(all_data)[names(all_data)=="male_id"] <- "sex"

all_data$age_cat_1 <- ifelse(all_data$age_wks <= 104, "0-2", "2-5")
all_data$age_cat_2 <- "0-5"

all_data <- subset(all_data, all_data$age_wks >= 0)
all_data <- subset(all_data, all_data$age_wks <= 260)
all_data$sex[all_data$sex == 2] <- 0
all_data$child_height <- as.numeric(all_data$child_height)  
all_data$child_weight <- as.numeric(all_data$child_weight)
all_data <- subset(all_data, !is.na(sex)) 

all_data$age_yr <- all_data$age_days/365
names(all_data)[names(all_data)=="age_yr"] <- "age_year"
names(all_data)[names(all_data)=="source"] <- "survey_name"

all_data$survey_module <- "none"
all_data$file_path <- "blank"


all_data$id <- 1:nrow(all_data)


vars_to_keep <- c("id", "nid", "psu", "ihme_loc_id", "location_id", "year_start", "year_end", "pweight", "strata", "sex", "age_wks", "age_days", "age_mo",
                  "birth_weight", "child_weight", "child_height", "age_cat_1", "age_cat_2", "age_year", "region", "survey_name", "survey_module", "file_path")

all_data <- all_data[vars_to_keep]  
nids3 <- unique(all_data$nid)


geospatial <- read.csv(paste0(other_dir2,"cgf_lbw_",date,".csv"), header = TRUE, sep = ",")

geospatial$year_start <- geospatial$start_year
geospatial$year_end <- geospatial$year_start
geospatial$survey_name <- geospatial$source
geospatial$survey_module <- "none"
geospatial$file_path <- "blank"
names(geospatial)[names(geospatial)=="sex"] <- "sex_id"

geospatial <- as.data.table(geospatial)
geospatial <- merge(geospatial,regions, by="ihme_loc_id")
geospatial <- as.data.frame(geospatial)

geospatial$id <- 1:nrow(geospatial)
geospatial$id <- geospatial$id + 100000

vars_to_keep <- c("id", "nid", "psu", "ihme_loc_id", "location_id", "year_start", "year_end", "pweight", "strata", "sex_id", "age_wks",
                  "birth_weight", "child_weight", "child_height", "HAZ", "WAZ", "WHZ", "WHZ_seas", "age_year", "region", "survey_name", "survey_module", "file_path")
geospatial <- geospatial[vars_to_keep]


HAZ_chart_months <- read.csv(FILEPATH, stringsAsFactors = FALSE)
WHZ_chart_months <- read.csv(FILEPATH, stringsAsFactors = FALSE)
WAZ_chart_months <- read.csv(FILEPATH, stringsAsFactors = FALSE)

HAZ_chart_weeks <- read.csv(FILEPATH, stringsAsFactors = FALSE)
WAZ_chart_weeks <- read.csv(FILEPATH, stringsAsFactors = FALSE)


######################################################################################################################
## HAZ 

all_data_HAZ <- all_data
print("HAZ")

to_remove <- c(22449, 22476, 95628, 95629, 95630, 95631, 95632, 161371, 162036)
all_data_HAZ <- all_data_HAZ[!(all_data_HAZ$nid %in% to_remove),]

all_data_HAZ <- subset(all_data_HAZ, !is.na(age_wks | age_mo | child_height))
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$child_height < 999 | all_data_HAZ$child_height != 888)

names(HAZ_chart_months)[names(HAZ_chart_months)=="l"] <- "HAZ_l"
names(HAZ_chart_months)[names(HAZ_chart_months)=="m"] <- "HAZ_m"
names(HAZ_chart_months)[names(HAZ_chart_months)=="s"] <- "HAZ_s"
names(HAZ_chart_months)[names(HAZ_chart_months)=="age_cat"] <- "age_cat_1"
names(HAZ_chart_months)[names(HAZ_chart_months)=="month"] <- "age_mo"

names(HAZ_chart_weeks)[names(HAZ_chart_weeks)=="l"] <- "HAZ_l"
names(HAZ_chart_weeks)[names(HAZ_chart_weeks)=="m"] <- "HAZ_m"
names(HAZ_chart_weeks)[names(HAZ_chart_weeks)=="s"] <- "HAZ_s"
names(HAZ_chart_weeks)[names(HAZ_chart_weeks)=="week"] <- "age_wks"


all_data_HAZ_wks <- subset(all_data_HAZ, all_data_HAZ$age_wks <= 13)
all_data_HAZ_wks <- merge(all_data_HAZ_wks, HAZ_chart_weeks, by=c("sex", "age_wks"), all.x = TRUE, allow.cartesian = TRUE)

all_data_HAZ_mo <- subset(all_data_HAZ, all_data_HAZ$age_wks > 13)
all_data_HAZ_mo <- merge(all_data_HAZ_mo, HAZ_chart_months, by=c("age_cat_1", "sex", "age_mo"), all.x = TRUE, allow.cartesian = TRUE) 

all_data_HAZ <- rbind(all_data_HAZ_wks, all_data_HAZ_mo)


all_data_HAZ$HAZ <- (((all_data_HAZ$child_height/all_data_HAZ$HAZ_m) ^ all_data_HAZ$HAZ_l)-1)/(all_data_HAZ$HAZ_s*all_data_HAZ$HAZ_l)

names(all_data_HAZ)[names(all_data_HAZ)=="sex"] <- "sex_id"

## add geospatial data
geospatial_HAZ <- geospatial
all_data_HAZ <- rbind.fill(all_data_HAZ, geospatial_HAZ)

all_data_HAZ$HAZ_b1 <- ifelse(all_data_HAZ$HAZ <= -1, 1, 0)
all_data_HAZ$HAZ_b2 <- ifelse(all_data_HAZ$HAZ <= -2, 1, 0)
all_data_HAZ$HAZ_b3 <- ifelse(all_data_HAZ$HAZ <= -3, 1, 0)


all_data_HAZ <- subset(all_data_HAZ, !is.na(HAZ))


all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$HAZ > -6)
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$HAZ < 6)

vars_to_keep <- c("id", "nid", "psu", "ihme_loc_id", "location_id", "year_start", "year_end", "pweight", "strata", "sex_id", "age_wks",
                  "birth_weight", "child_weight", "child_height", "HAZ", "age_year", "HAZ_b1", "HAZ_b2", "HAZ_b3", "region", "survey_name", "file_path", "survey_module")
all_data_HAZ <- all_data_HAZ[vars_to_keep]

all_data_HAZ$location_year <- paste0(all_data_HAZ$ihme_loc_id,"_",all_data_HAZ$year_end,"_",all_data_HAZ$nid)
survey_location_HAZ <- unique(all_data_HAZ$location_year)

rm(geospatial_HAZ, all_data_HAZ_mo, all_data_HAZ_wks)

write_location <- function(all_data_HAZ, location) {
  all_data_HAZ_1 <- all_data_HAZ

  all_data_HAZ_1 <- subset(all_data_HAZ_1, all_data_HAZ_1$location_year == location)

  write.csv(all_data_HAZ_1, file = FILEPATH, row.names = FALSE)
}

mclapply(unique(survey_location_HAZ), function(x) write_location(all_data_HAZ, x), mc.cores=4)


######################################################################################################################
## WAZ 

all_data_WAZ <- all_data
print("WAZ")

all_data_WAZ <- subset(all_data_WAZ, !is.na(age_wks | age_mo | child_weight))
all_data_WAZ <- subset(all_data_WAZ, all_data_WAZ$child_weight < 99)


names(WAZ_chart_months)[names(WAZ_chart_months)=="l"] <- "WAZ_l"
names(WAZ_chart_months)[names(WAZ_chart_months)=="m"] <- "WAZ_m"
names(WAZ_chart_months)[names(WAZ_chart_months)=="s"] <- "WAZ_s"
names(WAZ_chart_months)[names(WAZ_chart_months)=="age_cat"] <- "age_cat_2"
names(WAZ_chart_months)[names(WAZ_chart_months)=="month"] <- "age_mo"

names(WAZ_chart_weeks)[names(WAZ_chart_weeks)=="l"] <- "WAZ_l"
names(WAZ_chart_weeks)[names(WAZ_chart_weeks)=="m"] <- "WAZ_m"
names(WAZ_chart_weeks)[names(WAZ_chart_weeks)=="s"] <- "WAZ_s"
names(WAZ_chart_weeks)[names(WAZ_chart_weeks)=="week"] <- "age_wks"


all_data_WAZ_wks <- subset(all_data_WAZ, all_data_WAZ$age_wks <= 13)
all_data_WAZ_wks <- merge(all_data_WAZ_wks, WAZ_chart_weeks, by=c("sex", "age_wks"), all.x = TRUE, allow.cartesian = TRUE)

all_data_WAZ_mo <- subset(all_data_WAZ, all_data_WAZ$age_wks > 13)
all_data_WAZ_mo <- merge(all_data_WAZ_mo, WAZ_chart_months, by=c("age_cat_2", "sex", "age_mo"), all.x = TRUE, allow.cartesian = TRUE) 

all_data_WAZ <- rbind(all_data_WAZ_wks, all_data_WAZ_mo)


all_data_WAZ$WAZ <- (((all_data_WAZ$child_weight/all_data_WAZ$WAZ_m) ^ all_data_WAZ$WAZ_l)-1)/(all_data_WAZ$WAZ_s*all_data_WAZ$WAZ_l)

names(all_data_WAZ)[names(all_data_WAZ)=="sex"] <- "sex_id"


geospatial_WAZ <- geospatial
all_data_WAZ <- rbind.fill(all_data_WAZ, geospatial_WAZ)

all_data_WAZ$WAZ_b1 <- ifelse(all_data_WAZ$WAZ <= -1, 1, 0)
all_data_WAZ$WAZ_b2 <- ifelse(all_data_WAZ$WAZ <= -2, 1, 0)
all_data_WAZ$WAZ_b3 <- ifelse(all_data_WAZ$WAZ <= -3, 1, 0)

all_data_WAZ <- subset(all_data_WAZ, !is.na(WAZ))


all_data_WAZ <- subset(all_data_WAZ, all_data_WAZ$WAZ > -6)
all_data_WAZ <- subset(all_data_WAZ, all_data_WAZ$WAZ < 5)

vars_to_keep <- c("id", "nid", "psu", "ihme_loc_id", "location_id", "year_start", "year_end", "pweight", "strata", "sex_id", "age_wks",
                  "birth_weight", "child_weight", "child_height", "WAZ", "age_year", "WAZ_b1", "WAZ_b2", "WAZ_b3", "region", "survey_name", "survey_module", "file_path")
all_data_WAZ <- all_data_WAZ[vars_to_keep]

rm(geospatial_WAZ, all_data_WAZ_mo, all_data_WAZ_wks)


all_data_WAZ$location_year <- paste0(all_data_WAZ$ihme_loc_id,"_",all_data_WAZ$year_end,"_",all_data_WAZ$nid)
survey_location_WAZ <- unique(all_data_WAZ$location_year)


write_location <- function(all_data_WAZ, location) {
  all_data_WAZ_1 <- all_data_WAZ
  all_data_WAZ_1 <- subset(all_data_WAZ_1, all_data_WAZ_1$location_year == location)
  write.csv(all_data_WAZ_1, file = FILEPATH, row.names = FALSE)
}

mclapply(unique(survey_location_WAZ), function(x) write_location(all_data_WAZ, x), mc.cores=4)


######################################################################################################################
## WHZ 

all_data_WHZ <- all_data
print("WHZ")

to_remove <- c(22449, 22476, 95628, 95629, 95630, 95631, 95632, 161371, 162036)
all_data_WHZ <- all_data_WHZ[!(all_data_WHZ$nid %in% to_remove),]

all_data_WHZ <- subset(all_data_WHZ, !is.na(age_wks | child_weight | child_height))
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$child_weight < 99)
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$child_height < 999)


all_data_WHZ$child_height_round <- round_any(all_data_WHZ$child_height, .5)


all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$child_height >= 45)
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$child_height <= 120)


names(WHZ_chart_months)[names(WHZ_chart_months)=="l"] <- "WHZ_l"
names(WHZ_chart_months)[names(WHZ_chart_months)=="m"] <- "WHZ_m"
names(WHZ_chart_months)[names(WHZ_chart_months)=="s"] <- "WHZ_s"
names(WHZ_chart_months)[names(WHZ_chart_months)=="age_cat"] <- "age_cat_1"
names(WHZ_chart_months)[names(WHZ_chart_months)=="length"] <- "child_height_round"


all_data_WHZ <- merge(all_data_WHZ, WHZ_chart_months, by=c("age_cat_1", "sex", "child_height_round"), all.x = TRUE, allow.cartesian = TRUE) 

all_data_WHZ$WHZ <- (((all_data_WHZ$child_weight/all_data_WHZ$WHZ_m) ^ all_data_WHZ$WHZ_l)-1)/(all_data_WHZ$WHZ_s*all_data_WHZ$WHZ_l)

names(all_data_WHZ)[names(all_data_WHZ)=="sex"] <- "sex_id"

geospatial$WHZ <- NA
geospatial$WHZ <- geospatial$WHZ_seas
geospatial$WHZ_seas <- NULL
geospatial_WHZ <- geospatial
all_data_WHZ <- rbind.fill(all_data_WHZ, geospatial_WHZ)

all_data_WHZ$WHZ_b1 <- ifelse(all_data_WHZ$WHZ <= -1, 1, 0)
all_data_WHZ$WHZ_b2 <- ifelse(all_data_WHZ$WHZ <= -2, 1, 0)
all_data_WHZ$WHZ_b3 <- ifelse(all_data_WHZ$WHZ <= -3, 1, 0)

all_data_WHZ <- subset(all_data_WHZ, !is.na(WHZ))

 
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$WHZ > -5)
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$WHZ < 5)

vars_to_keep <- c("id", "nid", "psu", "ihme_loc_id", "location_id", "year_start", "year_end", "pweight", "strata", "sex_id", "age_wks",
                  "birth_weight", "child_weight", "child_height", "WHZ", "age_year", "WHZ_b1", "WHZ_b2", "WHZ_b3", "region", "survey_name", "survey_module", "file_path")
all_data_WHZ <- all_data_WHZ[vars_to_keep]

rm(geospatial_WHZ)
 
all_data_WHZ$location_year <- paste0(all_data_WHZ$ihme_loc_id,"_",all_data_WHZ$year_end,"_",all_data_WHZ$nid)
survey_location_WHZ <- unique(all_data_WHZ$location_year)


write_location <- function(all_data_WHZ, location) {
  all_data_WHZ_1 <- all_data_WHZ
 
  all_data_WHZ_1 <- subset(all_data_WHZ_1, all_data_WHZ_1$location_year == location)
  
  write.csv(all_data_WHZ_1, file = FILEPATH, row.names = FALSE)
}

mclapply(unique(survey_location_WHZ), function(x) write_location(all_data_WHZ, x), mc.cores=4)


########################################################################################################

write.csv(all_data_HAZ, file = paste0(save_dir, "HAZ_all_data.csv"), row.names = FALSE)
write.csv(all_data_WAZ, file = paste0(save_dir, "WAZ_all_data.csv"), row.names = FALSE)
write.csv(all_data_WHZ, file = paste0(save_dir, "WHZ_all_data.csv"), row.names = FALSE)

