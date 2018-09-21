
## Z score calculation and collapse code, to prep data for use in GBD models
## 1) calculate age in days, weeks, and months (days for gbd collapse, weeks/months for z score calc)
## 2) merge on growth charts depending on week/month
## 3) collapse for data upload 



os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "J:/"
} else {
  jpath <- "/home/j/"
}

library(data.table)
library(plyr)
library(parallel)
library(foreign)

# set directories
data_dir <- FILEPATH
other_dir <- FILEPATH
gbd_dir <- FILEPATH
save_dir <- FILEPATH
  
# initiate timing
start_time <- Sys.time()

AHS_CAB <- read.csv(FILEPATH, stringsAsFactors = FALSE)
CHNS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
CSES <- read.csv(FILEPATH, stringsAsFactors = FALSE)
names(CSES)[names(CSES)=="iso3"] <- "ihme_loc_id"
CVD_GEMS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
CVD_GEMS$child_weight[CVD_GEMS$nid == "224240"] <- CVD_GEMS$child_weight[CVD_GEMS$nid == "224240"]/10
CVD_GEMS$child_weight[CVD_GEMS$nid == "224854"] <- CVD_GEMS$child_weight[CVD_GEMS$nid == "224854"]/10
DLHS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
HHBUDGETYEM <- read.csv(FILEPATH, stringsAsFactors = FALSE) #some issues w/ weight distribution, too hard to figure out
names(HHBUDGETYEM)[names(HHBUDGETYEM)=="iso3"] <- "ihme_loc_id"

IFLS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
IFS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
LSMS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
MXFLS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
NHANES <- read.csv(FILEPATH, stringsAsFactors = FALSE)
NNMB <- read.csv(FILEPATH, stringsAsFactors = FALSE)
PCBS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
PNDS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
RLMS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
SHS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
WB_CWIQ <- read.csv(FILEPATH, stringsAsFactors = FALSE)
WB_CWIQ$ihme_loc_id <- NULL
names(WB_CWIQ)[names(WB_CWIQ)=="iso3"] <- "ihme_loc_id"
WBPS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
YNMS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
ZMBLCMS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
ZMBLCMS$child_weight[ZMBLCMS$nid == "14027"] <- ZMBLCMS$child_weight[ZMBLCMS$nid == "14027"]/10
ZMBLCMS$child_weight[ZMBLCMS$nid == "13973"] <- ZMBLCMS$child_weight[ZMBLCMS$nid == "13973"]/10
ZMBLCMS$child_weight[ZMBLCMS$nid == "14063"] <- ZMBLCMS$child_weight[ZMBLCMS$nid == "14063"]/10
HIS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
HNAS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
LCS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
ERHS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
NDNS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
GTM <- read.csv(FILEPATH, stringsAsFactors = FALSE)
IDN <- read.csv(FILEPATH, stringsAsFactors = FALSE)
IRQ <- read.csv(FILEPATH, stringsAsFactors = FALSE)
WMS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
KHM <- read.csv(FILEPATH, stringsAsFactors = FALSE)
SLIS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
MEX <- read.csv(FILEPATH, stringsAsFactors = FALSE)
MNG <- read.csv(FILEPATH, stringsAsFactors = FALSE)
PER <- read.csv(FILEPATH, stringsAsFactors = FALSE)
CSFSVAN <- read.csv(FILEPATH, stringsAsFactors = FALSE)
TTO <- read.csv(FILEPATH, stringsAsFactors = FALSE)
TZA <- read.csv(FILEPATH, stringsAsFactors = FALSE)
UGA <- read.csv(FILEPATH, stringsAsFactors = FALSE)
CFSS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
NSPMS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
KIDS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
NIDS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
country_spec_mis_africa <- read.csv(FILEPATH, stringsAsFactors = FALSE)
country_spec_mis_africa <- subset(country_spec_mis_africa, country_spec_mis_africa$nid != "30368")
SSNBHS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
NGA <- read.csv(FILEPATH, stringsAsFactors = FALSE) ## some height weight data needs to be transformed, but we drop those
DHS_Africa <- read.csv(FILEPATH, stringsAsFactors = FALSE)
names(DHS_Africa)[names(DHS_Africa)=="location_name_short_ihme_loc_id"] <- "ihme_loc_id"
DHS_MICS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
names(DHS_MICS)[names(DHS_MICS)=="loc_id"] <- "ihme_loc_id"
DHS_MICS$ihme_loc_id[DHS_MICS$ihme_loc_id == "KOSOVO"] <- "SRB"
DHS_MICS$child_height[DHS_MICS$nid == "12232"] <- DHS_MICS$child_height[DHS_MICS$nid == "12232"]/10
# remove duplicate survey
misc_DHS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
misc_DHS <- subset(misc_DHS, misc_DHS$nid != "21126")
misc_DHS$child_height[misc_DHS$nid == "157061"] <- misc_DHS$child_height[misc_DHS$nid == "157061"]/10
misc_DHS$child_weight[misc_DHS$nid == "157061"] <- misc_DHS$child_weight[misc_DHS$nid == "157061"]/10
# remove duplicate survey 
misc_MICS <- read.csv(FILEPATH, stringsAsFactors = FALSE)
misc_MICS <- subset(misc_MICS, misc_MICS$nid != "80731")


surveys <- list(AHS_CAB, CHNS, CSES, CVD_GEMS, DLHS, HHBUDGETYEM, IFLS, IFS, LSMS, MXFLS, NHANES, NNMB, 
                PCBS, PNDS, RLMS, SHS, WB_CWIQ, WBPS, YNMS, ZMBLCMS, HIS, HNAS, LCS, ERHS, NDNS, GTM, 
                IDN, IRQ, WMS, KHM, SLIS, MEX, MNG, PER, CSFSVAN, TTO, TZA, UGA, CFSS, NSPMS, KIDS, 
                NIDS, country_spec_mis_africa, SSNBHS, NGA, DHS_Africa, DHS_MICS, misc_DHS, misc_MICS)


all_data <- rbindlist(surveys, fill = TRUE)

all_data <- subset(all_data, all_data$age_yr < 6 | is.na(all_data$age_yr))
all_data <- subset(all_data, all_data$nid != "1970") ## per information in error_correcting file 
all_data <- subset(all_data, all_data$nid != "22463") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "112307") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "112308") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "112309") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "112310") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "200005") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "238481") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "22317") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "22286") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "22306") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "22329") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "22341") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "22930") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "38496") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "151802") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "30368") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "129523") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "21058") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "286801") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "31786") ## per information in error_correcting file , only have age in years
all_data <- subset(all_data, all_data$nid != "31797") ## per information in error_correcting file , only have age in years

all_data <- subset(all_data, all_data$nid != "951") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "1289") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "2053") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "7149") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "9522") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "11160") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "12595") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "12886") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "12950") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "13197") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "13436") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "13516") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "13719") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "13816") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "19035") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "19341") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "19431") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "19970") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "19979") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "19990") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "19999") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "20011") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "20021") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "20326") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "20595") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "20674") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "20683") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "20699") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "20780") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "20796") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "21024") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "21049") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "21421") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "23017") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "26444") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "27020") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "39999") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "76705") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "80733") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "104042") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "104043") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "125596") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "132739") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "141336") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "142943") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "152735") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "161587") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "200707") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "214734") ## per information in error_correcting file, doesn't have height and doesn't have weight
all_data <- subset(all_data, all_data$nid != "218566") ## per information in error_correcting file, doesn't have height and doesn't have weight

nids1 <- unique(all_data$nid)

## calculate age data
all_data$interview_yr[all_data$nid == 19546] <- 1995 #95
all_data$interview_yr[all_data$nid == 21151] <- 1999  #3899, assuming that it's 1999 since that's when the survey is from 
all_data$interview_yr[all_data$nid == 14341] <- 2004 #95
all_data$interview_yr[all_data$nid == 7233] <- 1990 #90
all_data$interview_yr[all_data$nid == 12101] <- 1993 #95
all_data$birth_yr[all_data$nid == 12101] <- paste0("19",all_data$birth_yr[all_data$nid == 12101])
all_data$interview_yr[all_data$nid == 10277] <- 1997 #95
all_data$interview_yr[all_data$nid == 14340] <- paste0("19",all_data$interview_yr[all_data$nid == 14340])
all_data$interview_yr[all_data$nid == 12672] <- 1993
all_data$interview_yr[all_data$nid == 20322] <- paste0("200",all_data$interview_yr[all_data$nid == 20322])
all_data$interview_yr[all_data$nid == 12732 & all_data$interview_yr == "1462"] <- 2005 ## coded as 1462, + 543 to do thai calendar offset
all_data$interview_yr[all_data$nid == 12732 & all_data$interview_yr == "1463"] <- 2006 ## coded as 1463, + 543 to do thai calendar offset
all_data$birth_yr[all_data$nid == 9919] <- paste0("19",all_data$birth_yr[all_data$nid == 9919])
all_data$birth_yr[all_data$nid == 14340] <- paste0("19",all_data$birth_yr[all_data$nid == 14340])
all_data$birth_yr[all_data$nid == 12672] <- paste0("19",all_data$birth_yr[all_data$nid == 12672])
all_data$birth_yr[all_data$nid == 10277] <- paste0("19",all_data$birth_yr[all_data$nid == 10277])




## 1: if age_days is blank, we'll try to fill it in by subtracting the number of 

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

## 2: use age_days to calculate weeks, months, and yrs IF age_days exists
# need to make age_wks column because it doesn't exist yet
all_data$age_wks <- NA
# then divide days by 7.0192 to get weeks (365/52=7.0192)
all_data$age_wks[!is.na(all_data$age_days)] <- all_data$age_days[!is.na(all_data$age_days)]/7.0192
# divide age_days by 30.5 to get months; overwrites data in age_mo column if it exists already
all_data$age_mo[!is.na(all_data$age_days)] <- all_data$age_days[!is.na(all_data$age_days)]/30.5
#divide age_days by 365 to get yrs; overwrites data in age_yr column if it exists already
all_data$age_yr[!is.na(all_data$age_days)] <- all_data$age_days[!is.na(all_data$age_days)]/365

## 3: use age_mo to calculate days, weeks, and yrs if age_days doesn't exist

all_data$age_yr[is.na(all_data$age_days) & !is.na(all_data$age_mo)] <- all_data$age_mo[is.na(all_data$age_days) & !is.na(all_data$age_mo)]/12
all_data$age_wks[is.na(all_data$age_days) & !is.na(all_data$age_mo)] <- all_data$age_mo[is.na(all_data$age_days) & !is.na(all_data$age_mo)]*4.333
all_data$age_days[is.na(all_data$age_days) & !is.na(all_data$age_mo)] <- all_data$age_mo[is.na(all_data$age_days) & !is.na(all_data$age_mo)]*30.5



## 4: drop if age_yr is the only thing available
## if they don't have day, wk, mo from above -- that's how we're determining age_yr is only thing available
all_data <- subset(all_data, !is.na(all_data$age_days) & !is.na(all_data$age_wks) & !is.na(all_data$age_mo))

all_data$age_wks <- as.integer(round_any(all_data$age_wks, 1))
all_data$age_mo <- as.integer(round_any(all_data$age_mo, 1))
all_data <- subset(all_data, !is.na(age_yr))

#add region data
regions <- read.csv(FILEPATH, stringsAsFactors = FALSE)
names(regions)[names(regions)=="super_region_name"] <- "region"
regions <- regions[c(17,20)]
regions <- subset(regions, !is.na(regions$ihme_loc_id))
all_data <- as.data.table(all_data)
regions <- as.data.table(regions)
all_data <- merge(all_data,regions, by="ihme_loc_id")
all_data <- as.data.frame(all_data)

# rename columns 
names(all_data)[names(all_data)=="start_year"] <- "year_start"
names(all_data)[names(all_data)=="end_year"] <- "year_end"
names(all_data)[names(all_data)=="survey_series"] <- "source"
names(all_data)[names(all_data)=="cluster_number"] <- "psu"
names(all_data)[names(all_data)=="sample_weight"] <- "pweight"
names(all_data)[names(all_data)=="male_id"] <- "sex"

# clean up, prepare data
all_data$age_cat_1 <- ifelse(all_data$age_wks <= 104, "0-2", "2-5")
all_data$age_cat_2 <- "0-5"

all_data <- subset(all_data, all_data$age_wks >= 0)
all_data <- subset(all_data, all_data$age_wks <= 260)
all_data$sex[all_data$sex == 2] <- 0
all_data$child_height <- as.numeric(all_data$child_height) #introduces about 1000 NAs by coercion
all_data$child_weight <- as.numeric(all_data$child_weight)
all_data <- subset(all_data, !is.na(sex)) #drops 4300

all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4841" & all_data$ihme_urban == 1] <- "IND_43872"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4842" & all_data$ihme_urban == 1] <- "IND_43873"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4843" & all_data$ihme_urban == 1] <- "IND_43874"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4844" & all_data$ihme_urban == 1] <- "IND_43875"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4846" & all_data$ihme_urban == 1] <- "IND_43877"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4849" & all_data$ihme_urban == 1] <- "IND_43880"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4850" & all_data$ihme_urban == 1] <- "IND_43881"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4851" & all_data$ihme_urban == 1] <- "IND_43882"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4852" & all_data$ihme_urban == 1] <- "IND_43883"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4853" & all_data$ihme_urban == 1] <- "IND_43884"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4854" & all_data$ihme_urban == 1] <- "IND_43885"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4855" & all_data$ihme_urban == 1] <- "IND_43886"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4856" & all_data$ihme_urban == 1] <- "IND_43887"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4857" & all_data$ihme_urban == 1] <- "IND_43888"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4859" & all_data$ihme_urban == 1] <- "IND_43890"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4860" & all_data$ihme_urban == 1] <- "IND_43891"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4861" & all_data$ihme_urban == 1] <- "IND_43892"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4862" & all_data$ihme_urban == 1] <- "IND_43893"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4863" & all_data$ihme_urban == 1] <- "IND_43894"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4864" & all_data$ihme_urban == 1] <- "IND_43895"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4865" & all_data$ihme_urban == 1] <- "IND_43896"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4867" & all_data$ihme_urban == 1] <- "IND_43898"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4868" & all_data$ihme_urban == 1] <- "IND_43899"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4869" & all_data$ihme_urban == 1] <- "IND_43900"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4870" & all_data$ihme_urban == 1] <- "IND_43901"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4871" & all_data$ihme_urban == 1] <- "IND_43902"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4872" & all_data$ihme_urban == 1] <- "IND_43903"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4873" & all_data$ihme_urban == 1] <- "IND_43904"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4874" & all_data$ihme_urban == 1] <- "IND_43905"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4875" & all_data$ihme_urban == 1] <- "IND_43906"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_44538" & all_data$ihme_urban == 1] <- "IND_44540"

all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4841" & all_data$ihme_urban == 0] <- "IND_43908"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4842" & all_data$ihme_urban == 0] <- "IND_43909"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4843" & all_data$ihme_urban == 0] <- "IND_43910"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4844" & all_data$ihme_urban == 0] <- "IND_43911"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4846" & all_data$ihme_urban == 0] <- "IND_43913"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4849" & all_data$ihme_urban == 0] <- "IND_43916"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4850" & all_data$ihme_urban == 0] <- "IND_43917"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4851" & all_data$ihme_urban == 0] <- "IND_43918"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4852" & all_data$ihme_urban == 0] <- "IND_43919"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4853" & all_data$ihme_urban == 0] <- "IND_43920"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4854" & all_data$ihme_urban == 0] <- "IND_43921"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4855" & all_data$ihme_urban == 0] <- "IND_43922"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4856" & all_data$ihme_urban == 0] <- "IND_43923"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4857" & all_data$ihme_urban == 0] <- "IND_43924"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4859" & all_data$ihme_urban == 0] <- "IND_43926"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4860" & all_data$ihme_urban == 0] <- "IND_43927"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4861" & all_data$ihme_urban == 0] <- "IND_43928"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4862" & all_data$ihme_urban == 0] <- "IND_43929"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4863" & all_data$ihme_urban == 0] <- "IND_43930"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4864" & all_data$ihme_urban == 0] <- "IND_43931"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4865" & all_data$ihme_urban == 0] <- "IND_43932"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4867" & all_data$ihme_urban == 0] <- "IND_43934"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4868" & all_data$ihme_urban == 0] <- "IND_43935"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4869" & all_data$ihme_urban == 0] <- "IND_43936"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4870" & all_data$ihme_urban == 0] <- "IND_43937"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4871" & all_data$ihme_urban == 0] <- "IND_43938"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4872" & all_data$ihme_urban == 0] <- "IND_43939"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4873" & all_data$ihme_urban == 0] <- "IND_43940"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4874" & all_data$ihme_urban == 0] <- "IND_43941"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_4875" & all_data$ihme_urban == 0] <- "IND_43942"
all_data$ihme_loc_id[all_data$ihme_loc_id == "IND_44538" & all_data$ihme_urban ==0] <- "IND_44539"

all_data$age_yr <- all_data$age_days/365
names(all_data)[names(all_data)=="age_yr"] <- "age_year"
names(all_data)[names(all_data)=="source"] <- "survey_name"

vars_to_keep <- c("nid", "psu", "survey_name", "ihme_loc_id", "year_start", "year_end", "pweight", "strata", "sex", "age_wks", "age_days", "age_mo",
                  "birth_weight", "child_weight", "child_height", "age_cat_1", "age_cat_2", "age_year", "region")

setDF(all_data)
all_data <- all_data[vars_to_keep]  
nids3 <- unique(all_data$nid)
      
setdiff(nids1,nids3)


## bring in growth charts 
HAZ_chart_months <- read.csv(FILEPATH, header = TRUE, sep =",")
WHZ_chart_months <- read.csv(FILEPATH, header = TRUE, sep =",")
WAZ_chart_months <- read.csv(FILEPATH, header = TRUE, sep =",")

HAZ_chart_weeks <- read.csv(FILEPATH, header = TRUE, sep =",")
WAZ_chart_weeks <- read.csv(FILEPATH, header = TRUE, sep =",")



######################################################################################################################
## HAZ 

all_data_HAZ <- all_data
print("HAZ")

all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "22449") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "22476") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "23219") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "95628") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "95629") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "95630") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "95631") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "95632") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "161371") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "162036") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "6614") ## per information in error_correcting file, doesn't have height 
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$nid != "246246") ## per information in error_correcting file, doesn't have height for the right ages

nids4 <- unique(all_data_HAZ$nid)

# prep all_data -- this needs to happen within each section, to maximize data use amidst missingness
all_data_HAZ <- subset(all_data_HAZ, !is.na(age_wks))
all_data_HAZ <- subset(all_data_HAZ, !is.na(age_mo))
all_data_HAZ <- subset(all_data_HAZ, !is.na(child_height))
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$child_height < 999)

# prep HAZ charts to be joined on
names(HAZ_chart_months)[names(HAZ_chart_months)=="l"] <- "HAZ_l"
names(HAZ_chart_months)[names(HAZ_chart_months)=="m"] <- "HAZ_m"
names(HAZ_chart_months)[names(HAZ_chart_months)=="s"] <- "HAZ_s"
names(HAZ_chart_months)[names(HAZ_chart_months)=="age_cat"] <- "age_cat_1"
names(HAZ_chart_months)[names(HAZ_chart_months)=="month"] <- "age_mo"

names(HAZ_chart_weeks)[names(HAZ_chart_weeks)=="l"] <- "HAZ_l"
names(HAZ_chart_weeks)[names(HAZ_chart_weeks)=="m"] <- "HAZ_m"
names(HAZ_chart_weeks)[names(HAZ_chart_weeks)=="s"] <- "HAZ_s"
names(HAZ_chart_weeks)[names(HAZ_chart_weeks)=="week"] <- "age_wks"

## subset data to get two datasets that will use different charts, then merge and rbind together
all_data_HAZ_wks <- subset(all_data_HAZ, all_data_HAZ$age_wks <= 13)
all_data_HAZ_wks <- merge(all_data_HAZ_wks, HAZ_chart_weeks, by=c("sex", "age_wks"), all.x = TRUE, allow.cartesian = TRUE)

all_data_HAZ_mo <- subset(all_data_HAZ, all_data_HAZ$age_wks > 13)
all_data_HAZ_mo <- merge(all_data_HAZ_mo, HAZ_chart_months, by=c("age_cat_1", "sex", "age_mo"), all.x = TRUE, allow.cartesian = TRUE) 

all_data_HAZ <- rbind(all_data_HAZ_wks, all_data_HAZ_mo)

# calculate HAZ score
all_data_HAZ$HAZ <- (((all_data_HAZ$child_height/all_data_HAZ$HAZ_m) ^ all_data_HAZ$HAZ_l)-1)/(all_data_HAZ$HAZ_s*all_data_HAZ$HAZ_l)

all_data_HAZ$HAZ_b1 <- ifelse(all_data_HAZ$HAZ <= -1, 1, 0)
all_data_HAZ$HAZ_b2 <- ifelse(all_data_HAZ$HAZ <= -2, 1, 0)
all_data_HAZ$HAZ_b3 <- ifelse(all_data_HAZ$HAZ <= -3, 1, 0)

# drop if HAZ is blank 
all_data_HAZ <- subset(all_data_HAZ, !is.na(HAZ))

all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$HAZ > -6)
all_data_HAZ <- subset(all_data_HAZ, all_data_HAZ$HAZ < 6)

nids5 <- unique(all_data_HAZ$nid)
print(setdiff(nids4,nids5))

setDF(all_data_HAZ)
vars_to_keep <- c("nid", "psu", "survey_name", "ihme_loc_id", "year_start", "year_end", "pweight", "strata", "sex", "age_wks",
                "birth_weight", "child_weight", "child_height",
                "HAZ", "age_year", "HAZ_b1", "HAZ_b2", "HAZ_b3", "region")
all_data_HAZ <- all_data_HAZ[vars_to_keep]

# identify locations for file saving 
all_data_HAZ$location_year <- paste(all_data_HAZ$ihme_loc_id, all_data_HAZ$year_end, all_data_HAZ$nid)
survey_location_HAZ <- unique(all_data_HAZ$location_year)


## For saving shiny input files 
write_location <- function(all_data_HAZ, location) {
all_data_HAZ_1 <- all_data_HAZ
all_data_HAZ_1 <- subset(all_data_HAZ_1, all_data_HAZ_1$location_year == location)
# for shiny

write.csv(all_data_HAZ_1, file = paste0(FILEPATH,"_HAZ.csv"), row.names = FALSE)
}

mclapply(unique(survey_location_HAZ), function(x) write_location(all_data_HAZ, x), mc.cores=4)

######################################################################################################################
## WAZ 

all_data_WAZ <- all_data
print("WAZ")

all_data_WAZ <- subset(all_data_WAZ, all_data_WAZ$nid != "6614") ## per information in error_correcting file, doesn't have  weight
all_data_WAZ <- subset(all_data_WAZ, all_data_WAZ$nid != "21409") ## per information in error_correcting file, doesn't have  weight
all_data_WAZ <- subset(all_data_WAZ, all_data_WAZ$nid != "246246") ##doesn't have weight info for the right ages

nids6 <- unique(all_data_WAZ$nid)

# prep all_data -- this needs to happen within each section, to maximize data use amidst missingness
all_data_WAZ <- subset(all_data_WAZ, !is.na(age_wks))
all_data_WAZ <- subset(all_data_WAZ, !is.na(age_mo))
all_data_WAZ <- subset(all_data_WAZ, !is.na(child_weight))
all_data_WAZ <- subset(all_data_WAZ, all_data_WAZ$child_weight < 99)

# prep WAZ chart to be joined on
names(WAZ_chart_months)[names(WAZ_chart_months)=="l"] <- "WAZ_l"
names(WAZ_chart_months)[names(WAZ_chart_months)=="m"] <- "WAZ_m"
names(WAZ_chart_months)[names(WAZ_chart_months)=="s"] <- "WAZ_s"
names(WAZ_chart_months)[names(WAZ_chart_months)=="age_cat"] <- "age_cat_2"
names(WAZ_chart_months)[names(WAZ_chart_months)=="month"] <- "age_mo"

names(WAZ_chart_weeks)[names(WAZ_chart_weeks)=="l"] <- "WAZ_l"
names(WAZ_chart_weeks)[names(WAZ_chart_weeks)=="m"] <- "WAZ_m"
names(WAZ_chart_weeks)[names(WAZ_chart_weeks)=="s"] <- "WAZ_s"
names(WAZ_chart_weeks)[names(WAZ_chart_weeks)=="week"] <- "age_wks"

## subset data to get two datasets that will use different charts, then merge and rbind together
all_data_WAZ_wks <- subset(all_data_WAZ, all_data_WAZ$age_wks <= 13)
all_data_WAZ_wks <- merge(all_data_WAZ_wks, WAZ_chart_weeks, by=c("sex", "age_wks"), all.x = TRUE, allow.cartesian = TRUE)

all_data_WAZ_mo <- subset(all_data_WAZ, all_data_WAZ$age_wks > 13)
all_data_WAZ_mo <- merge(all_data_WAZ_mo, WAZ_chart_months, by=c("age_cat_2", "sex", "age_mo"), all.x = TRUE, allow.cartesian = TRUE) 

all_data_WAZ <- rbind(all_data_WAZ_wks, all_data_WAZ_mo)

# calculate WAZ score
all_data_WAZ$WAZ <- (((all_data_WAZ$child_weight/all_data_WAZ$WAZ_m) ^ all_data_WAZ$WAZ_l)-1)/(all_data_WAZ$WAZ_s*all_data_WAZ$WAZ_l)


all_data_WAZ$WAZ_b1 <- ifelse(all_data_WAZ$WAZ <= -1, 1, 0)
all_data_WAZ$WAZ_b2 <- ifelse(all_data_WAZ$WAZ <= -2, 1, 0)
all_data_WAZ$WAZ_b3 <- ifelse(all_data_WAZ$WAZ <= -3, 1, 0)

# drop if WAZ is blank
all_data_WAZ <- subset(all_data_WAZ, !is.na(WAZ))

all_data_WAZ <- subset(all_data_WAZ, all_data_WAZ$WAZ > -6)
all_data_WAZ <- subset(all_data_WAZ, all_data_WAZ$WAZ < 5)

nids7 <- unique(all_data_WAZ$nid)
print(setdiff(nids6,nids7))

setDF(all_data_WAZ)
vars_to_keep <- c("nid", "psu", "survey_name", "ihme_loc_id", "year_start", "year_end", "pweight", "strata", "sex", "age_wks",
                "birth_weight", "child_weight", "child_height",
                "WAZ", "age_year", "WAZ_b1", "WAZ_b2", "WAZ_b3", "region")
all_data_WAZ <- all_data_WAZ[vars_to_keep]


# identify locations for file saving 
all_data_WAZ$location_year <- paste(all_data_WAZ$ihme_loc_id, all_data_WAZ$year_end, all_data_WAZ$nid)
survey_location_WAZ <- unique(all_data_WAZ$location_year)


## For saving shiny input files 
write_location <- function(all_data_WAZ, location) {
all_data_WAZ_1 <- all_data_WAZ

all_data_WAZ_1 <- subset(all_data_WAZ_1, all_data_WAZ_1$location_year == location)
# for shiny

write.csv(all_data_WAZ_1, file = paste0(FILEPATH,"_WAZ.csv"), row.names = FALSE)
}

mclapply(unique(survey_location_WAZ), function(x) write_location(all_data_WAZ, x), mc.cores=4)

######################################################################################################################
## WHZ 

all_data_WHZ <- all_data
print("WHZ")

all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "22449") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "22476") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "23219") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "95628") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "95629") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "95630") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "95631") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "95632") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "161371") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "162036") ## per information in error_correcting file, doesn't have height for under 1 year olds, which is the only age in mo

all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "6614") ## per information in error_correcting file, doesn't have  weight
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "21409") ## per information in error_correcting file, doesn't have  weight
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "6614") ## per information in error_correcting file, doesn't have height 
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$nid != "246246") ## per information in error_correcting file, doesn't have height for the right ages

nids8 <- unique(all_data_WHZ$nid)

# prep all_data -- this needs to happen within each section, to maximize data use amidst missingness
all_data_WHZ <- subset(all_data_WHZ, !is.na(age_wks))
all_data_WHZ <- subset(all_data_WHZ, !is.na(child_weight))
all_data_WHZ <- subset(all_data_WHZ, !is.na(child_height))
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$child_weight < 99)
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$child_height < 999)

# prep all_data_WHZ
all_data_WHZ$child_height <- round_any(all_data_WHZ$child_height, .5)

#remove children whose height is under 45 cm or over 120 cm; the growth charts don't have those values 
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$child_height >= 45)
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$child_height <= 120)

# prep WHZ chart to be joined on
names(WHZ_chart_months)[names(WHZ_chart_months)=="l"] <- "WHZ_l"
names(WHZ_chart_months)[names(WHZ_chart_months)=="m"] <- "WHZ_m"
names(WHZ_chart_months)[names(WHZ_chart_months)=="s"] <- "WHZ_s"
names(WHZ_chart_months)[names(WHZ_chart_months)=="age_cat"] <- "age_cat_1"
names(WHZ_chart_months)[names(WHZ_chart_months)=="length"] <- "child_height"

# merge on growth chart
all_data_WHZ <- merge(all_data_WHZ, WHZ_chart_months, by=c("age_cat_1", "sex", "child_height"), all.x = TRUE, allow.cartesian = TRUE) 

# calculate WHZ score
all_data_WHZ$WHZ <- (((all_data_WHZ$child_weight/all_data_WHZ$WHZ_m) ^ all_data_WHZ$WHZ_l)-1)/(all_data_WHZ$WHZ_s*all_data_WHZ$WHZ_l)

# create binary for wasting 
#all_data_WHZ$wasting <- ifelse(all_data_WHZ$WHZ <= -2, 1, 0)
#all_data_WHZ$N <- 1

all_data_WHZ$WHZ_b1 <- ifelse(all_data_WHZ$WHZ <= -1, 1, 0)
all_data_WHZ$WHZ_b2 <- ifelse(all_data_WHZ$WHZ <= -2, 1, 0)
all_data_WHZ$WHZ_b3 <- ifelse(all_data_WHZ$WHZ <= -3, 1, 0)

# drop if WHZ is blank
all_data_WHZ <- subset(all_data_WHZ, !is.na(WHZ))

all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$WHZ > -5)
all_data_WHZ <- subset(all_data_WHZ, all_data_WHZ$WHZ < 5)

nids9 <- unique(all_data_WHZ$nid)
print(setdiff(nids8,nids9))

setDF(all_data_WHZ)
vars_to_keep <- c("nid", "psu", "survey_name", "ihme_loc_id", "year_start", "year_end", "pweight", "strata", "sex", "age_wks",
                "birth_weight", "child_weight", "child_height",
                #"point", "latitude", "longitude", "uncertain_point", "buffer", "admin_1", "WHZ", "age_year", "survey_module", "WHZ_b1", "WHZ_b2", "WHZ_b3", "region")
                "WHZ", "age_year", "WHZ_b1", "WHZ_b2", "WHZ_b3", "region")
all_data_WHZ <- all_data_WHZ[vars_to_keep]


# identify locations for file saving 
all_data_WHZ$location_year <- paste(all_data_WHZ$ihme_loc_id, all_data_WHZ$year_end, all_data_WHZ$nid)
survey_location_WHZ <- unique(all_data_WHZ$location_year)


## For saving shiny input files 
write_location <- function(all_data_WHZ, location) {
all_data_WHZ_1 <- all_data_WHZ

all_data_WHZ_1 <- subset(all_data_WHZ_1, all_data_WHZ_1$location_year == location)
# for shiny

write.csv(all_data_WHZ_1, file = paste0(FILEPATH,"_WHZ.csv"), row.names = FALSE)
}

mclapply(unique(survey_location_WHZ), function(x) write_location(all_data_WHZ, x), mc.cores=4)



########################################################################################################


## save as csvs
write.csv(all_data_HAZ, file = paste0(save_dir, "/HAZ_all_data.csv"), row.names = FALSE)
write.csv(all_data_WAZ, file = paste0(save_dir, "/WAZ_all_data.csv"), row.names = FALSE)
write.csv(all_data_WHZ, file = paste0(save_dir, "/WHZ_all_data.csv"), row.names = FALSE)

subset_reg <- unique(all_data_WAZ$region)

for (reg in subset_reg){
  data1 <- subset(all_data_HAZ, all_data_HAZ$region == reg)
  write.csv(data1, file = paste0(FILEPATH,"_all_data_HAZ.csv"), row.names = FALSE)
}



for (reg in subset_reg){
  data1 <- subset(all_data_WAZ, all_data_WAZ$region == reg)
  write.csv(data1, file = paste0(FILEPATH,"_all_data_WAZ.csv"), row.names = FALSE)
}



for (reg in subset_reg){
  data1 <- subset(all_data_WHZ, all_data_WHZ$region == reg)
  write.csv(data1, file = paste0(FILEPATH,"_all_data_WHZ.csv"), row.names = FALSE)
}


## TIMING 
end_time <- Sys.time()

time_to_run <- end_time - start_time
print(time_to_run)


