################################################################################
## Description: Formats 45q15 data and covariates for the first and second
##              stage models
################################################################################

rm(list=ls())

library(foreign); library(plyr); library(argparse); library(reshape); library(data.table); library(haven); library(assertable); library(readr)
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

if (Sys.info()[1]=="Windows") {
  hivuncert <- F
  hivupdate <- F
}

# Parse arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run')
parser$add_argument('--data_45q15_version', type="character", required=TRUE,
                    help='Version id of 45q15 data')
parser$add_argument('--ddm_estimate_version', type="character", required=TRUE,
                    help='Version id of DDM estimate')
parser$add_argument('--m5q0_version_id', type="character", required=TRUE,
                    help='Version id of 5q0 estimate')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help="GBD year")

args <- parser$parse_args()
gbd_year <- args$gbd_year
version_id <- args$version_id
data_run_id <- args$data_45q15_version
ddm_version_id <- args$ddm_estimate_version
m5q0_version_id <- args$m5q0_version_id

output_dir <- "FILEPATH"

## set country list
codes <- fread(paste0(output_dir, "/data/locations.csv"))
iso3_map <- codes[, c("local_id_2013", "ihme_loc_id")]
codes <- codes[, c("location_id", "ihme_loc_id")]
years <- c(1950:gbd_year)

####################
## Load covariates
####################

## make a square dataset onto which we'll merge all covariates and data
data <- read_csv(paste0(output_dir, "/data/locations.csv"))
data <- data[, c("ihme_loc_id", "location_id", "location_name", "super_region_name", "region_name", "parent_id")]
data$region_name <- gsub(" ", "_", gsub(" / ", "_", gsub(", ", "_", gsub("-", "_", data$region_name))))
data$super_region_name <- gsub(" ", "_", gsub("/", "_", gsub(", ", "_", data$super_region_name)))
data <- merge(data, data.frame(year=years))
data <- data[data$location_id %in% unique(codes$location_id),]

## load pop data, only for weighting the mean of education over ages.
population <- fread(paste0(output_dir, "/data/population.csv"))
pop <- copy(population)
pop_ap <- pop[location_id %in% c("4841", "4871")]
pop_ap <- pop_ap[, list(population = sum(population), location_id = 44849), by = c("age_group_id", "year_id", "sex_id", "run_id") ]
pop <-rbind(pop, pop_ap, use.names = T)
setnames(pop, c("year_id", "population"), c("year", "pop"))
pop <- pop[sex_id == 1,  sex := "male"]
pop <- pop[sex_id == 2, sex := "female"]
pop <- pop[, list(location_id, year, sex, age_group_id, pop)]

## load LDI, edu, and HIV from covariates DB
LDI <- as.data.frame(fread(paste0(output_dir, "/data/ldi_pc.csv")))
LDI <- LDI[, c("location_id", "year_id", "mean_value")]
names(LDI)[names(LDI)=="year_id"] <- "year"
names(LDI)[names(LDI)=="mean_value"] <- "LDI_id"

# Creating old andhra pradesh
pop_ap <- population[location_id %in% c("4841", "4871")]
setnames(pop_ap, c("year_id"), c("year"))
pop_ap <- as.data.table(pop_ap[, list(pop = sum(population)), by = c("location_id", "year")])
LDI_ap <- as.data.table(LDI[LDI$location_id %in% c(4841, 4871), ])
LDI_ap <- merge(LDI_ap, pop_ap, by=c("location_id", "year"))
LDI_ap[, LDI_id := LDI_id * pop]
setkey(LDI_ap, year)
LDI_ap <- LDI_ap[, .(LDI_id = sum(LDI_id), pop = sum(pop)), by = key(LDI_ap)]
LDI_ap[, LDI_id := LDI_id/pop]
LDI_ap[, location_id := 44849]
LDI_ap[, pop:= NULL]
LDI <- rbind(LDI, LDI_ap)

LDI <- merge(LDI, codes, by = c("location_id"), all=FALSE) # Drops China and England

edu <- as.data.frame(fread(paste0(output_dir, "/data/educ_yrs_pc.csv")))
edu$year <- edu$year_id
edu <- edu[, c("location_id", "year", "sex_id", "age_group_id", "mean_value")]
names(edu)[names(edu) == "sex_id"] <- "sex"

edu <- edu[edu$year <= max(years) & edu$age_group_id > 7 & edu$age_group_id < 17, ]

# Creating old andhra pradesh
pop_ap <- population[location_id %in% c("4841", "4871")]
setnames(pop_ap, c("year_id", "sex_id", "population"), c("year", "sex", "pop"))
edu_ap <- as.data.table(edu[edu$location_id %in% c(4841, 4871), ])
edu_ap <- merge(edu_ap, pop_ap, by=c("location_id", "year", "sex", "age_group_id"))
edu_ap[, mean_value := mean_value * pop]
setkey(edu_ap, year, age_group_id, sex)
edu_ap <- edu_ap[, .(mean_value = sum(mean_value), pop = sum(pop)), by = key(edu_ap)]
edu_ap[, mean_value := mean_value/pop]
edu_ap[, location_id := 44849]
edu_ap[, pop := NULL]
edu <- rbind(edu, edu_ap)

edu <- merge(edu, codes, all = FALSE, by = "location_id")

hiv <- fread(paste0(output_dir, "/data/hiv.csv"))

hiv <- hiv[year_id >= 1970 & sex_id %in% c(1, 2), c("location_id", "sex_id", "year_id", "mean_value")]
setnames(hiv, c("mean_value", "year_id", "sex_id"), c("death_rt_1559_mean", "year", "sex"))

# creating old andhra pradesh
hiv_ap <- hiv[hiv$location_id %in% c("4841", "4871"), ]
pop_ap <- population[location_id %in% c("4841", "4871")]
pop_ap[, ihme_loc_id := paste0("IND_", location_id)]
pop_ap <- pop_ap[, list(pop = sum(population)), by = c("location_id", "year_id", "sex_id", "ihme_loc_id")]
setnames(pop_ap, c("year_id", "sex_id"), c("year", "sex"))

hiv_ap <- data.table(hiv_ap)
hiv_ap <- merge(hiv_ap, pop_ap, by=c("location_id", "year", "sex"))
hiv_ap[, hiv := death_rt_1559_mean*pop]
setkey(hiv_ap, year, sex)
hiv_ap <- hiv_ap[, .(hiv=sum(hiv), pop=sum(pop)), by=key(hiv_ap)]
hiv_ap[, death_rt_1559_mean := hiv/pop]
hiv_ap[, location_id := 44849]
hiv_ap[, pop:=NULL]
hiv_ap[, hiv:=NULL]
hiv <- rbind(hiv, hiv_ap, use.names=T)

# Add years before each country's min year, which had no HIV
hiv_rest <- copy(hiv)
setkey(hiv_rest, location_id, sex)
hiv_rest <- hiv_rest[, list(year=1950:(min(year)-1),
                            death_rt_1559_mean=0),
                     key(hiv_rest)]
hiv <- rbind(hiv, hiv_rest, use.names=T)

# Get 5q0 estimates
m5q0_estimates <- fread(paste0("FILEPATH/5q0/", m5q0_version_id, "/upload_estimates.csv"))
m5q0_estimates = m5q0_estimates[estimate_stage_id == 3]
m5q0_estimates[, year := year_id]
names(m5q0_estimates)[names(m5q0_estimates) == "mean"] <- "m5q0"
m5q0_estimates <- m5q0_estimates[, .SD, .SDcols = c('location_id', 'year', 'm5q0')]

# merge together
fwrite(edu, paste0(output_dir, "/data/edu_test.csv"), row.names = F)
fwrite(hiv, paste0(output_dir, "/data/hiv_test.csv"), row.names = F)
fwrite(LDI, paste0(output_dir, "/data/ldi_test.csv"), row.names = F)
fwrite(m5q0_estimates, paste0(output_dir, "/data/5q0_test.csv"), row.names = F)
codmod <- merge(edu, hiv, by=c("location_id", "year", "sex"), all.x = T)
codmod <- merge(codmod, LDI, by=c("location_id", "ihme_loc_id", "year"))
codmod <- merge(codmod, m5q0_estimates, by=c("location_id", "year"), all.x = T)
codmod$sex <- ifelse(codmod$sex==1, "male", "female")

# format
codmod <- codmod[!is.na(codmod$age_group_id) & codmod$year <= max(years), ]
codmod$age_group_id <- as.numeric(codmod$age_group_id)
codmod <- codmod[codmod$age_group_id > 7 & codmod$age_group_id < 17 & codmod$ihme_loc_id %in% unique(data$ihme_loc_id), ]
codmod <- merge(codmod, pop, by=c("location_id", "sex", "year", "age_group_id"), all.x=T)
codmod <- ddply(codmod, c("ihme_loc_id", "year", "sex"),
                function(x) {
                  if (sum(is.na(x$pop))==0) w <- x$pop else w <- rep(1, length(x$pop))
                  data.frame(LDI_id = x$LDI_id[1],
                             m5q0 = x$m5q0[1],
                             mean_yrs_educ = weighted.mean(x$mean_value, w),
                             hiv = ifelse(is.na(x$death_rt_1559_mean[1]), 0, x$death_rt_1559_mean[1])
                  )
                }
)
assert_values(data = codmod, colnames = colnames(codmod), test = "not_na")
assert_values(data = codmod, colnames = c("LDI_id", "hiv", "mean_yrs_educ", "m5q0"), test = "gte", test_val = 0)
id_vars <- list(year = years, sex = c("female", "male"), ihme_loc_id = unique(data$ihme_loc_id))
assert_ids(codmod, id_vars)
####################
## Prep 45q15 input data
####################

merge_completeness <- function(input, gbd_year, ddm_version_id) {
  locations <- get_locations(gbd_year = gbd_year)
  locations2017 <- get_locations(gbd_year = 2017)
  loc2019 <- setdiff(locations$location_id, locations2017$location_id)

  completeness <- get_mort_outputs(model_name = "ddm",
                                   model_type = "estimate",
                                   run_id = ddm_version_id,
                                   location_set_id = 82,
                                   estimate_stage_ids = 14)

  prev_completeness <- get_mort_outputs(model_name = "ddm",
                                        model_type = "estimate",
                                        run_id = 128,
                                        location_set_id = 82,
                                        estimate_stage_ids = 14)

  completeness <- completeness[location_id %in% loc2019 | year_id >= 2018]
  completeness <- rbind(prev_completeness, completeness, use.names = T)

  vr_complete_source_type_ids <- c(1, 2, 3, 20, 21, 22, 23, 18, 19, 24,
                                   25, 26, 27, 28, 29, 30, 31, 32, 33)
  completeness[mean == 1 & source_type_id %in% vr_complete_source_type_ids, vr_complete := T]
  completeness[is.na(vr_complete), vr_complete := F]

  completeness <- completeness[, c("location_id", "source_type_id", "vr_complete", "year_id")]

  output <- merge(input, completeness, by = c("location_id", "source_type_id", "year_id"), all.x = T)
  output[is.na(vr_complete), vr_complete := F]
  return(output)
}

## pull 45q15 data, format for modeling
raw <- get_mort_outputs(model_name = "45q15",
                        model_type = "data",
                        run_id = data_run_id,
                        outlier_run_id = 'active',
                        location_set_id = 82)

raw <- raw[outlier == 0,]

raw <- raw[!(grepl("CHN", ihme_loc_id) & !(ihme_loc_id %in% c("CHN_354", "CHN_361")) & (source_name == "VR") & (outlier != 1)), ]
raw <- raw[source!="irn_nocr_allcause_vr",]

raw[, upload_45q15_data_id := NULL]
unadjusted_raw <- raw[adjustment == 0]
unadjusted_raw[, adjustment := NULL]
raw <- raw[adjustment == 1]
raw[, adjustment := NULL]

# merge on obs45q15
setnames(unadjusted_raw, "mean", "obs45q15")
by_vars = names(raw)[names(raw) != "mean"]
raw <- merge(raw, unadjusted_raw, all = T, by = by_vars)

raw <- raw[!is.na(raw$ihme_loc_id),]

if(nrow(raw[year_id < 1950]) > 0) {
  warning(paste0("Found ", nrow(raw[year_id < 1950]), " rows with year_id less than 1950, dropping them."))
  raw = raw[year_id >= 1950,]
}

# Determine VR complete
vr_complete_source_type_ids <- c(1, 2, 3, 20, 21, 22, 23, 18, 19, 24,
                                 25, 26, 27, 28, 29, 30, 31, 32, 33)
raw[source_type_id %in% vr_complete_source_type_ids & method_id == 1, vr_complete := T]
raw[is.na(vr_complete), vr_complete := F]

raw <- raw[location_id %in% unique(codes$location_id)]

raw[sex_id == 2, sex := 'female']
raw[sex_id == 1, sex := 'male']
raw[sex_id == 3, sex := 'both']

raw[, source_name := tolower(source_name)]

# rename and remove variables unnecessary for modeling
setnames(raw, c("mean", "source_name"), c("adj45q15", "source_type"))

columns_to_remove <- c("run_id", "method_name", "viz_year", "sex_id",
                       "underlying_nid", "nid", "age_group_id", "outlier")
raw[, (columns_to_remove) := NULL]

#### drop shocks and exclusions ####

raw <- subset(raw, raw$adj45q15 < 1)
rows_greater_than_one <- nrow(subset(raw, raw$adj45q15 >= 1))
if (rows_greater_than_one > 0) {
  warning(paste0("Found ", rows_greater_than_one, " rows where adjusted 45q15 is greater than 1."))
}

raw$year <- floor(raw$year)

single <- table(raw$ihme_loc_id, raw$sex)
single <- melt(single)
single <- single[single$value == 1, ]
single$ihme_loc_id <- as.character(single$Var.1)
single$sex <- as.character(single$Var.2)
for (ii in 1:nrow(single)) {
  add <- subset(raw, raw$ihme_loc_id == single$ihme_loc_id[ii] & raw$sex == single$sex[ii])
  add$adj45q15 <- add$adj45q15*1.25
  raw <- rbind(raw, add)
  add$adj45q15 <- (add$adj45q15/1.25)*0.75
  raw <- rbind(raw, add)
}


raw <- raw[!is.na(raw$year), ]
n_raw <- nrow(raw) # Find the number of data rows before merging, to make sure none get dropped prior to regression

fwrite(data.table(raw[!(method_id %in% c(11, 5)), ]), paste0(output_dir, "/data/test_exposure.csv"), row.names = F)
assert_values(data = data.table(raw[!(method_id %in% c(11, 5)), ]), colnames = c("exposure"), test = "not_equal", test_val = 0)

## assign data categories
## category I: Complete
raw[vr_complete == T, category := "complete"]
## category II: DDM adjusted (include all subnational)
raw[method_id == 2 & vr_complete == F, category := "ddm_adjust"]
## category IV: Unadjusted
raw[method_id == 1 & vr_complete == F, category := "no_adjust"]
## category V: Sibs
raw[method_id == 5 & vr_complete == F, category := "sibs"]
## category VI: DSS
raw[method_id == 11, category := "dss"]

# adjust.sd can only be NA/missing for categories =="sibs" or for certain source_types
assert_values(data = raw, colnames = c("sd"), test = "gt", test_val = 0, na.rm = T)
assert_values(data = raw, colnames = c("sd"), test = "lt", test_val = 1, na.rm = T)
assert_values(data = raw[!(category %in% c("dss", "sibs")) & !source_type_id %in% c(5, 42, 36, 50, 40)], colnames = c("sd"), test = "not_na", warn_only = T)

raw[, c("method_id", "vr_complete", "source_type_id") := NULL]

raw[grepl("IND", ihme_loc_id) & (source_type == "srs"), category := "ddm_adjust"]
raw[grepl("BGD", ihme_loc_id) & (source_type == "srs"), category := "ddm_adjust"]
raw[grepl("CHN", ihme_loc_id) & (source_type == "dsp"), category := "ddm_adjust"]
raw[ihme_loc_id == "BWA" & (source_type == "vr"), category := "ddm_adjust"]
raw[ihme_loc_id == "HND" & (source_type == "vr"), category := "ddm_adjust"]
raw[ihme_loc_id == "SUR" & (source_type == "vr"), category := "ddm_adjust"]
raw[ihme_loc_id == "DZA" & (source_type == "vr"), category := "ddm_adjust"]
raw[ihme_loc_id == "PHL" & (source_type == "vr"), category := "ddm_adjust"]

raw[grepl("IRN_", ihme_loc_id) & (source_type == "vr"), category := "ddm_adjust"]

raw[ihme_loc_id == "JOR" & (source_type == "vr") & (year_id >= 2004), category := "ddm_adjust"]
raw[ihme_loc_id == "THA" & (source_type == "vr"), category := "ddm_adjust"]
raw[grepl("PAK", ihme_loc_id) & (source_type == "srs"), category := "ddm_adjust"]


## assign each country to a data group
raw$vr <- as.numeric(grepl("VR|SRS", raw$source_type, ignore.case = T))

types <- ddply(raw, c("location_id", "ihme_loc_id", "sex"),
               function(x) {
                 cats <- unique(x$category)
                 vr <- mean(x$vr)
                 vr.max <- ifelse(vr == 0, 0, max(x$year[x$vr==1]))
                 vr.num <- sum(x$vr==1)
                 if (length(cats) == 1 & cats[1] == "complete" & vr == 1 & vr.max > 1980 & vr.num > 10) type <- "complete VR only"
                 else if (("ddm_adjust" %in% cats | "gb_adjust" %in% cats) & vr == 1 & vr.max > 1980 & vr.num > 10) type <- "VR only"
                 else if ((vr < 1 & vr > 0) | (vr == 1 & (vr.max <= 1980 | vr.num <= 10))) type <- "VR plus"
                 else if ("sibs" %in% cats & vr == 0) type <- "sibs"
                 else if (!"sibs" %in% cats & vr == 0) type <- "other"
                 else type <- "none"
                 return(data.frame(type=type, stringsAsFactors=F))
               })

## drop excess variables
names(raw) <- gsub("adj45q15", "mort", names(raw))
names(raw) <- gsub("sd", "adjust.sd", names(raw))
raw <- raw[order(raw$ihme_loc_id, raw$year, raw$sex, raw$category), c("location_id", "ihme_loc_id", "year", "sex", "mort", "source_type", "category", "vr", "exposure", "adjust.sd", "obs45q15")]

####################
## Merge everything together
####################

## merge all covariates to build a square dataset
data <- merge(data, codmod, by = c("ihme_loc_id", "year"), all.x=T)

## merge in 45q15 data
data <- merge(data, raw, by=c("location_id", "ihme_loc_id", "sex", "year"), all.x=T)

data$data <- as.numeric(!is.na(data$mort))
data$year <- data$year + 0.5

## merge in country data classification
data <- merge(data, types, by=c("location_id", "ihme_loc_id", "sex"), all.x=T)
data$type <- as.character(data$type)
data$type[is.na(data$type)] <- "no data"

## Identify number of years covered by VR within each country after 1970
new_data <- data.table(unique(data[data$data==1 & !is.na(data$vr) & data$vr==1 & data$year>=1970, c("ihme_loc_id", "year")]))
setkey(new_data, ihme_loc_id)
data_length <- as.data.frame(new_data[, length(year), by=key(new_data)])
names(data_length) <- c("ihme_loc_id", "covered_years")

## Designate a cutoff point of covered years under which we will put into their own category
req_years <- 20
sibs_years <- 21
data <- merge(data, data_length, by="ihme_loc_id", all.x=T)
data[is.na(data$covered_years), ]$covered_years <- 0
data$ihme_loc_id <- as.character(data$ihme_loc_id)
data$type[data$type != "no data" & data$type != "sibs" & data$covered_years >= req_years] <- data$ihme_loc_id[data$type != "no data" & data$type != "sibs" & data$covered_years >= req_years]
data$type[data$type != "no data" & data$type != "sibs" & data$covered_years < req_years] <- paste0("sparse_data_", data$type[data$type != "no data" & data$type != "sibs" & data$covered_years < req_years])
data$type[data$type == "sibs" & data$covered_years >= sibs_years] <- "sibs_large"
data$type[data$type == "sibs" & data$covered_years < sibs_years] <- "sibs_small"

data$type[data$ihme_loc_id == "ALB"] <- "sparse_data_VR only"
data$type[data$ihme_loc_id == "IND_44849"] <- "sparse_data_VR only"
data$type[data$ihme_loc_id == "HND"] <- "sparse_data_VR plus"
data$type[data$ihme_loc_id == "SRB"] <- "sparse_data_VR plus"
data$type[data$ihme_loc_id %in% c("IND_43886", "IND_43890", "IND_43911", "IND_43916", "IND_43922", "IND_43926", "IND_43940", "IND_4844", "IND_4849", "IND_4855", "IND_4859", "IND_4873")] <- "sparse_data_VR only"
data$type[grepl("IRN_", data$ihme_loc_id)] <- "IRN"
data$type[grepl("NZL_", data$ihme_loc_id)] <- "NZL"
data$type[grepl("RUS_", data$ihme_loc_id)] <- "RUS"
data$type[grepl("NOR_", data$ihme_loc_id)] <- "NOR"

for(cc in unique(data$ihme_loc_id[substr(data$ihme_loc_id, 1, 3) == "SAU"])) {
  data$type[data$ihme_loc_id == cc] <- cc
}

n_formatted <- nrow(data[!is.na(data$mort), ])
if(n_raw != n_formatted) {
  stop(paste0("Number raw is ", n_raw, " and number formatted is ", n_formatted))
}

data <- data.table(data)

# data validation
if (nrow(data[mort == 0]) > 0) {
  warning(paste0("Found ", nrow(data[mort == 0]), " observations with mort of 0, setting to 0.0001. Locations: ", paste(unique(data[mort == 0, ihme_loc_id]))))
  data[mort == 0, mort := 0.0001]
}
if (nrow(data[obs45q15 == 0]) > 0) {
  warning(paste0("Found ", nrow(data[obs45q15 == 0]), " observations with obs45q15 of 0, setting to 0.0001. Locations: ", paste(unique(data[obs45q15 == 0, ihme_loc_id]))))
  data[obs45q15 == 0, obs45q15 := 0.0001]
}

## format and save
data <- data[order(ihme_loc_id, sex, year),
             list(location_id, location_name, super_region_name, region_name,
                  parent_id, ihme_loc_id, sex, year, LDI_id, mean_yrs_educ, m5q0,
                  adjust.sd, hiv, mort, exposure, obs45q15, source_type, type,
                  category, vr, data)]

assert_values(data = data, colnames = c("mort", "obs45q15", "LDI_id", "mean_yrs_educ", "hiv", "m5q0"), test = "not_inf")

fwrite(data, paste0(output_dir, "/data/input_data.csv"))

# Save hiv_covariates.csv for Empirical LT data prep
mean_adult_hiv = setDT(data)
setnames(mean_adult_hiv, "hiv", "adult_hiv_cdr")
mean_adult_hiv_deduplicated <- unique(mean_adult_hiv[, .(ihme_loc_id, year, sex, adult_hiv_cdr)])
fwrite(mean_adult_hiv_deduplicated, paste0(output_dir,"/data/hiv_covariate.csv"))

location_parameter_types <- unique(data[, list(location_id, ihme_loc_id, location_name, type, sex)])
fwrite(location_parameter_types, paste0(output_dir, "/data/location_parameter_types.csv"))
