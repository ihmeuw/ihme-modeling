
##
## Purpose: Data was sex-split, MarketScan & MarketScan 2000 prevalence data were crosswalked, 
##          hospital data from unrepresentative countries were outliered, data was age-split 
##          (prev/inc points going above 1 or cases <1 were outliered), and then uploaded after
##          MAD filtering
##

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2)
library(doBy, lib.loc = "FILEPATH")

###### Paths, args
#################################################################################

central <- "FILEPATH"

hf_bundle <- "VALUE"
bvid_2 <- "VALUE"
bvid_4 <- "VALUE"

hf_meid <- "VALUE"

## Re-extracted definitions
def_path <- "FILEPATH" 

for (func in paste0(central, list.files(central))) source(paste0(func))
locs <- get_location_metadata(9, gbd_round_id = 6)
outlier_choices <- c(518, # Tibet, all zeros
                     37, # Kyrgyzstan, all zeros
                     4742, # Papua, many zeros
                     4733, # North Sulawesi, all zeros 
                     4862, # Meghalaya, many zeros
                     16, # Philippines, too low
                     98, # Chile, too low
                     164, # Nepal, too low
                     142, # Iran, many zeros
                     59, # Latvia, too low
                     60, # Lithuania, too high, pulling up the region
                     102, # USA, too high
                     135, # Brazil
                     130, # Mexico
                     6, # China
                     11, # Indonesia
                     51, # Poland
                     193, # Botswana
                     122, # Ecuador
                     35, # Georgia
                     163, # India
                     144) # India
outlier_ids <- locs[location_id %in% outlier_choices | parent_id %in% outlier_choices, location_id]


###### Functions
#################################################################################

source("/FILEPATH/master_mrbrt_crosswalk_func.R")
source("/FILEPATH/age_split.R")
source("/FILEPATH/sex_split.R")
source("/FILEPATH/model_helper_functions.R")


###### Get data
#################################################################################

data <- get_bundle_version("VALUE", fetch = "all")


###### Crosswalk prevalence data
#################################################################################

## Don't use the outliers or group review data points.
data <- data[is_outlier==0 & input_type != "group_review",]

## Get rid of self-report data
defs <- fread(def_path)
self_report_nids <- defs[`Self-report` == 1, unique(nid)]
data <- data[!(nid %in% self_report_nids)]

## Assign clinical data types.
data[clinical_data_type == "inpatient", cv_inpatient := 1]
data[clinical_data_type == "claims" & grepl("Truven", field_citation_value), cv_claims := 1]
data[cv_claims == 1 & grepl("2000", field_citation_value), `:=` (cv_claims=0, cv_marketscan_2000=1)]
for (col in c("cv_claims", "cv_inpatient", "cv_marketscan_2000")) data[is.na(get(col)), paste0(col) := 0]

## Age restrictions for HF in clincial informatics data
data <- data[!(cv_inpatient == 1 & age_start < 40)]
data <- data[!(cv_claims == 1 & age_start < 40)]
data <- data[!(cv_marketscan_2000 == 1 & age_start < 40)]

## Aggregate Marketscan to national data points for matching
msn <- copy(data)[cv_claims==1,]
msn[, agg_cases := sum(cases), by=c("age_start", "age_end", "year_start", "year_end", "sex")]
msn[, agg_ss := sum(sample_size), by=c("age_start", "age_end", "year_start", "year_end", "sex")]
msn[, agg_mean := agg_cases/agg_ss]
z <- qnorm(0.975)
msn[measure == "prevalence", agg_se := sqrt(agg_mean*(1-agg_mean)/agg_ss + z^2/(4*agg_ss^2))]
msn[measure == "incidence" & cases < 5, agg_se := ((5-agg_mean*agg_ss)/agg_ss+agg_mean*agg_ss*sqrt(5/agg_ss^2))/5]
msn[measure == "incidence" & cases >= 5, agg_se := sqrt(agg_mean/agg_ss)]
msn[, agg_location_id := 102]
msn[, agg_location_name := "United States of America"]
msn[, c("location_name", "location_id", "mean", "upper", "lower", 
        "standard_error", "cases", "sample_size", "effective_sample_size", "seq", "origin_seq") := NA]
msn <- unique(msn)

msn[, `:=` (cases=agg_cases, standard_error=agg_se, sample_size=agg_ss, 
            mean=agg_mean, location_id=agg_location_id, location_name=agg_location_name)]
n <- names(msn)[grepl("agg", names(msn))]
msn[, c(n) := NULL]

msn[, note_modeler := paste0(note_modeler, " | DATA POINT IS AGGREGATED ONLY EXISTS FOR MATCHING - REMOVE BEFORE UPLOAD")]

## Pull aggregated Marketscan data back onto the dataset
data <- rbind(data, msn)

## Outlier CI data
data[clinical_data_type == "inpatient" & location_id %in% outlier_ids, is_outlier := 1]
data <- data[!(is_outlier == 1)]

## Update Canadian data
data[measure == "incidence" & location_name == "Canada", `:=` (mean = -log(1-mean)/3 , note_modeler = paste0(note_modeler, " | changed from 3-yr cumulative incidence to 1-year"))]
data[measure == "incidence" & location_name == "Canada", `:=` (upper = NA, lower = NA, standard_error = NA, cases = NA)]
data[measure == "incidence" & location_name == "Canada", cases := mean * sample_size]
data[measure == "incidence" & location_name == "Canada", standard_error := sqrt(mean/sample_size)]

data_all_types <- copy(data)

###### Crosswalk ########
data <- copy(data_all_types[measure == "prevalence",])
xw_measure <- "prevalence"

alternate_defs <- c("cv_claims", "cv_marketscan_2000")
folder_root <-  "FILEPATH"

crosswalk_holder <- master_mrbrt_crosswalk(
  
  model_abbr = "documentation_imp_hf",
  data = data,
  xw_measure = xw_measure,
  folder_root = folder_root,
  alternate_defs = alternate_defs,
  
  sex_split_only = F,
  subnat_to_nat = F,
  
  addl_x_covs = c("age_scaled", "male"),
  
  age_overlap = 5,
  year_overlap = 10,
  age_range = 15,
  year_range = 15,
  sex_age_overlap = 0,
  sex_year_overlap = 0,
  sex_age_range = 0,
  sex_year_range = 0,
  sex_fix_zeros = T,
  use_lasso = F,
  id_vars = "id_var",
  opt_method = "trim_maxL",
  trim_pct = 0.1,
  sex_remove_x_intercept = F,
  sex_covs = "age_scaled",
  
  gbd_round_id = 6,
  decomp_step = "step4",
  logit_transform = T,
  
  upload_crosswalk_version = F
  
)

print(crosswalk_holder$xw_covs)

prev <- crosswalk_holder$data
prev <- prev[!(note_modeler %like% "REMOVE BEFORE UPLOAD")]

prev <- dplyr::select(prev, names(data_all_types)) ## Get rid of extraneous columns


###### Sex-split the other measures - incidence, mtwith, cfr, mtstandard
#################################################################################

## Sex_split is a convenience function. It's a wraparound for master_mrbrt_crosswalk for only sex-splitting. 
## lapply is just calling sex_split for each of four measures
sex_split_data <- rbindlist(lapply(X = c("incidence", "mtwith", "cfr", "mtstandard"), 
                                   FUN = function(meas) {
                                           df <- sex_split(data = copy(data_all_types[measure == meas,]), xw_measure = meas)
                                           object <- df$obj
                                           data <- df$data
                                           save(object, file = paste0("FILEPATH", meas, ".Rdata"))
                                           data }))

## Pull all together
data <- rbind(sex_split_data, prev)


###### Turn cfr into mtwith
#################################################################################

data <- data[!(measure == "cfr" & mean == 1)] 

## Get sample size from SE 
data[measure == "cfr" & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]

## Transform
data[measure == "cfr", mean := -(log(1-mean))]

## Get SE back
z <- qnorm(0.975)
data[measure == "cfr", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
data[measure == "cfr", `:=` (upper = NA, lower = NA)]

## Update
data[measure == "cfr", `:=` (measure = "mtwith", note_modeler = paste0(note_modeler, " | transformed from cfr"))]
data[measure == "mtwith" & is.infinite(mean), is_outlier := 1]

data_sex_split <- copy(data)


###### Age-split
#################################################################################

data_sex_split[, crosswalk_parent_seq := seq]
data_sex_split[, seq := NA]

## age_split is a helper function using DisMod age patterns
age_split_inc <- age_split(df = copy(data_sex_split[measure == "incidence"]), 
                            model_id = hf_meid, 
                            decomp_step = "step2", 
                            measure = "incidence")

age_split_prev <- age_split(df = copy(data_sex_split[measure == "prevalence"]), 
                            model_id = hf_meid, 
                            decomp_step = "step2", 
                            measure = "prevalence")

mtwith_temp <- copy(data_sex_split[measure == "mtwith"])
mtwith_temp[, measure := "proportion"]
age_split_mtwith <- age_split(df = mtwith_temp, 
                            model_id = hf_meid, 
                            decomp_step = "iterative", 
                            measure = "proportion")
age_split_mtwith[, measure := "mtwith"]

age_split_data <- rbindlist(list(age_split_inc, age_split_prev, age_split_mtwith))
age_split_data <- dplyr::select(age_split_data, names(data_sex_split))

## I'm not age-splitting mtstandard
data <- rbind(age_split_data, data_sex_split[measure == "mtstandard"])


###### Final adjustments, upload
#################################################################################

## Check outliering decisions
data[clinical_data_type == "inpatient" & location_id %in% outlier_ids, is_outlier := 1]

## Get rid of age-split data points where cases < 1
data[note_modeler %like% "age-split" & cases < 1, is_outlier := 1]

## Uploader processing
data[standard_error > 1, standard_error := 1]
data[, c("upper", "lower", "uncertainty_type_value", "underlying_nid") := NA]
data[, unit_value_as_published := 1]
data[, c("cv_claims", "cv_marketscan_2000") := NULL]
data[location_id %in% c(4622,4624,95), `:=` (location_id=4749, location_name="England", ihme_loc_id="GBR_4749", 
          note_modeler=paste0(note_modeler,"UTLA data points reextracted as 'England' because of the location heirarchy."))]

keep_data <- copy(data)


####### Save a XW version with MAD filtering ALL types of data ####### 
data <- copy(keep_data)

mad_p <- summaryBy(data = data[measure == "prevalence"], mean ~ age_start + sex, FUN = mad.stats)
mad_p$mad.upper <- with(mad_p, ifelse(age_start<80, mean.median + 5*mean.mad, 
                                        ifelse(age_start>=80 & age_start<90, mean.median + 5*mean.mad, 
                                               ifelse(age_start>=90, mean.median + 4*mean.mad, NA))))
mad_p$mad.lower <- with(mad_p, ifelse(age_start<80, mean.median - 5*mean.mad, 
                                  ifelse(age_start>=80 & age_start<90, mean.median - 5*mean.mad, 
                                         ifelse(age_start>=90, mean.median - 4*mean.mad, NA))))
mad_p$measure <- "prevalence"
data <- merge(data, mad_p, by=c("age_start", "sex", "measure"), all.x = T)
data[measure == "prevalence" & (mean > mad.upper | mean < mad.lower), 
     `:=` (is_outlier = 1, note_modeler = paste0(note_modeler, " | MAD filtered"))]
data[, c("mad.upper", "mad.lower") := NULL]

mad_i <- summaryBy(data = data[measure == "incidence"], mean ~ age_start + sex, FUN = mad.stats)
mad_i$mad.upper <- with(mad_i, ifelse(age_start<80, mean.median + 5*mean.mad, 
                                      ifelse(age_start>=80 & age_start<90, mean.median + 5*mean.mad, 
                                             ifelse(age_start>=90, mean.median + 4*mean.mad, NA))))
mad_i$mad.lower <- with(mad_i, ifelse(age_start<80, mean.median - 5*mean.mad, 
                                      ifelse(age_start>=80 & age_start<90, mean.median - 5*mean.mad, 
                                             ifelse(age_start>=90, mean.median - 4*mean.mad, NA))))
mad_i$measure <- "incidence"
data <- merge(data, mad_i, by=c("age_start", "sex", "measure"), all.x = T)
data[measure == "incidence" & (mean > mad.upper | mean < mad.lower), 
     `:=` (is_outlier = 1, note_modeler = paste0(note_modeler, " | MAD filtered"))]
data[, c("mad.upper", "mad.lower") := NULL]

mad_m <- summaryBy(data = data[measure == "mtwith"], mean ~ age_start + sex, FUN = mad.stats)
mad_m$mad.upper <- with(mad_m, ifelse(age_start<80, mean.median + 5*mean.mad, 
                                      ifelse(age_start>=80 & age_start<90, mean.median + 5*mean.mad, 
                                             ifelse(age_start>=90, mean.median + 4*mean.mad, NA))))
mad_m$mad.lower <- with(mad_m, ifelse(age_start<80, mean.median - 5*mean.mad, 
                                      ifelse(age_start>=80 & age_start<90, mean.median - 5*mean.mad, 
                                             ifelse(age_start>=90, mean.median - 4*mean.mad, NA))))
mad_m$measure <- "mtwith"
data <- merge(data, mad_m, by=c("age_start", "sex", "measure"), all.x = T)
data[measure == "mtwith" & (mean > mad.upper | mean < mad.lower), 
     `:=` (is_outlier = 1, note_modeler = paste0(note_modeler, " | MAD filtered"))]
data[, c("mad.upper", "mad.lower") := NULL]

data[, names(data)[grepl("mean.", names(data))] := NULL]
data[cases < 0, `:=` (cases = NA, sample_size = NA)]

mad_filtering <- rbind(mad_p, mad_i, mad_m)
write.csv(mad_filtering, "FILEPATH")

write.xlsx(data, "FILEPATH", sheetName = "extraction")
save_crosswalk_version(bundle_version_id = 18722, 
                       data_filepath = "FILEPATH", 
                       description = "HF xw'd, age- and sex-split, mad filtering all data & types, outlier Georgia 1/16/20")

