###########################################################
### Author: 
### Adapted from Code written by 
### Date: 12/9/2016
### Project: GBD Nonfatal Estimation
### Purpose: EMR Regression for Parkinsons
###########################################################

#Setup
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j.root <- "/home/j/" 
  h.root <- "/homes/USERNAME/"
} else { 
  j.root <- "J:"
  h.root <- "H:"
}

set.seed(365687)

#load packages, install if missing
require(data.table)
require(RMySQL)
require(lme4)
require(stringr)

##set directories
central.function <- FILEPATH
output.dir <- FILEPATH

#get central functions
source(paste0(j.root, central.function, "get_draws.R"))
source(paste0(j.root, central.function, "get_covariate_estimates.R"))
source(paste0(j.root, central.function, "get_location_metadata.R"))
source(paste0(j.root, central.function, "get_ids.R"))

##pull locations
locations <- c(102, 79,75) ##getting info for US, Finland, and Austria

##define objects
ages <- c(9:20, 30:32, 235)
sexes <- c(1,2)
meid <- 11642
cid <- 544
dismod.model <- 152207
cod.model.m <- 347609
cod.model.f <- 347612
draws <- paste0("draw_", c(0:999))
prev <- paste0("prev_", c(0:999))
deaths <- paste0("deaths_", c(0:999))
csmr <- paste0("csmr_", c(0:999))
emr <- paste0("emr_", c(0:999))
log.emr <- paste0("log_emr_", c(0:999))
zero.vars <- c("sex_issue", "year_issue", "age_issue", "age_demographer", "measure_issue", "measure_adjustment", "is_outlier", "smaller_site_unit")
blank.vars <- c("standard_error", "effective_sample_size",
                "cases", "sample_size", "measure_issue", "recall_type_value", "sampling_type", "response_rate", "case_name", 
                "case_definition","case_diagnostics", "group", "specificity", "group_review", "seq", "seq_parent", "input_type",
                "underlying_nid", "underlying_field_citation_value", "field_citation_value", "page_num", "table_num", "ihme_loc_id",
                "site_memo", "design_effect", "uncertainty_type", "note_SR")
epi.order <- c(c("seq", "seq_parent", "input_type", "underlying_nid", "nid", "underlying_field_citation_value", "page_num", "table_num",
                 "source_type", "location_name", "location_id", "smaller_site_unit", "site_memo", "sex", "sex_issue", "year_start", 
                 "year_end", "year_issue", "age_start", "age_end", "age_issue", "age_demographer", "measure", "mean", "lower", "upper",
                 "standard_error", "effective_sample_size", "cases", "sample_size", "unit_type", "unit_value_as_published", "measure_issue",
                 "measure_adjustment", "uncertainty_type", "uncertainty_type_value", "design_effect","representative_name", "urbanicity_type", "recall_type",
                 "recall_type_value", "sampling_type", "response_rate", "case_name", "case_definition", "case_diagnostics", "group", 
                 "specificity", "group_review", "note_modeler", "note_SR", "extractor", "is_outlier"))

##get prevalence draws
prev.draws <- as.data.table(get_draws(gbd_id_field = "modelable_entity_id", gbd_id = meid, source = "epi", 
                                      model_version_id = dismod.model,  measure_ids = 5, sex_ids = sexes, age_group_ids = ages,
                                      location_ids = locations, year_ids = 2016))
prev.draws <- prev.draws[,c("location_id", "year_id", "sex_id", "age_group_id", draws), with=F] ##keep only needed columns
for (i in 0:999){
  setnames(prev.draws, paste0("draw_", i), paste0("prev_", i))
} ##rename draws

##get csmr draws
csmr.draws.m <- as.data.table(get_draws(gbd_id_field = "cause_id", source= "codem", gbd_id = cid, 
                                        model_version_id = cod.model.m, age_group_ids = ages, location_ids = locations,
                                        sex_ids = sexes, year_ids = 2016))
csmr.draws.f <- as.data.table(get_draws(gbd_id_field = "cause_id", source= "codem", gbd_id = cid, 
                                        model_version_id = cod.model.f, age_group_ids = ages, location_ids = locations,
                                        sex_ids = sexes, year_ids = 2016))
csmr.draws <- rbind(csmr.draws.m, csmr.draws.f)
for (i in 0:999){
  setnames(csmr.draws, paste0("draw_", i), paste0("deaths_", i))
} ##rename draws

##merge csmr, prev
all.draws <- merge(prev.draws, csmr.draws, by = c("location_id", "year_id", "age_group_id", "sex_id"))

##convert prevalence to cases so that can sum
mult.pop <- function(drawnum){
  all.draws[[paste0("prev_", drawnum)]]*all.draws[['pop']]
}
all.draws <- all.draws[, (prev) := lapply(0:999, mult.pop)]

##collapse age_group 9,10,11,12,13,14,15,16 (20-59) because not a lot of people have Parkinson's that young
all.draws <- all.draws[, new_age := age_group_id] ##create new column same as the old one
all.draws <- all.draws[age_group_id<=16, new_age := 1] ##new age group is one for 40-59

##collapse the bottom age_groups
sum.over <- c("location_id", "year_id", "new_age", "sex_id")
sum.value <- c("pop", deaths, prev)
all.draws <- all.draws[, lapply(.SD, sum), .SDcols= sum.value, by = sum.over]

##convert to csmr and prev
div.pop.prev <- function(drawnum){
  all.draws[[paste0("prev_", drawnum)]]/all.draws[['pop']]
}
div.pop.csmr <- function(drawnum){
  all.draws[[paste0("deaths_", drawnum)]]/all.draws[['pop']]
}
##functions that divide all draws by popoualation 
all.draws <- all.draws[, (prev) := lapply(0:999, div.pop.prev)] ##execute on draws for prev 
all.draws <- alloc.col(all.draws, 100000) ##allocate more columns to do this
all.draws <- all.draws[, (csmr) := lapply(0:999, div.pop.csmr)] ##same for csmr

##calculate emr and logemr
emr.fun <- function(drawnum){
  all.draws[[paste0("csmr_", drawnum)]]/all.draws[[paste0("prev_", drawnum)]]
} ##function to calculate draws for emr

all.draws <- alloc.col(all.draws, 10000) ##allocate more columns
all.draws <- all.draws[, (emr) := lapply(0:999, emr.fun)] ##execute emr on draws
all.draws <- alloc.col(all.draws, 10000) ##allocate even more columns
all.draws <- all.draws[, (log.emr) := lapply(.SD, log), .SDcols = emr]  ##execute log emr on draws
all.draws <- all.draws[,c("location_id", "sex_id", "new_age", emr, log.emr), with=F] ##get rid of unnecessary columns

##calculate mean and quantiles
all.draws <- all.draws[, mean_emr := rowMeans(.SD), .SDcols = emr] 
all.draws <- all.draws[, upper_emr := apply(.SD, 1, quantile, probs=0.975), .SDcols = emr]
all.draws <- all.draws[, lower_emr := apply(.SD, 1, quantile, probs= 0.025), .SDcols = emr]
all.draws <- all.draws[, mean_logemr := rowMeans(.SD), .SDcols = log.emr]
all.draws <- all.draws[,.(location_id, sex_id, new_age, mean_emr, upper_emr, lower_emr, mean_logemr)]

##save good emr data
emr.data <- all.draws[,.(location_id, sex_id, new_age, mean_emr, upper_emr, lower_emr)]

##dummy variables
age_group_ids <- paste0("age_group", unique(all.draws$new_age)) ##list of all unique age groups
all.draws <- all.draws[, (age_group_ids) := lapply(age_group_ids, function(x) new_age==as.integer(gsub("age_group","",x)))] ##identity funciton, will create logical indicator variables for each age group
all.draws <- all.draws[, (age_group_ids) := lapply(.SD, as.integer), .SDcols = age_group_ids] ##turn logical into integer (0,1)
sex_ids <- paste0("sex_id", unique(all.draws$sex_id))
all.draws <- all.draws[, (sex_ids) := lapply(sex_ids, function(x) sex_id==as.integer(gsub("sex_id","",x)))] ##same steps for age group
all.draws <- all.draws[, (sex_ids) := lapply(.SD, as.integer), .SDcols = sex_ids]


##run linear regression
model <- lm(mean_logemr ~ age_group1 + age_group17 + age_group18 + age_group19 + age_group20 + age_group30 +
              age_group31 + age_group32 + age_group235 + sex_id1 + sex_id2, data= all.draws)

##predict out- get mean and standard error
predict.frame <- all.draws[,c("sex_id", "new_age", age_group_ids, sex_ids), with=F] ##only necessary columns for prediction
predict.frame <- unique(predict.frame, by= c("sex_id", "new_age", age_group_ids, sex_ids)) ##get only unique (not one per location)
predictions <- predict(model, predict.frame, se.fit=T) ##generate predictions
predict.frame$log.prediction <- predictions$fit ##assign as new columns in frame
predict.frame$log.st.err <- predictions$se.fit

predict.frame <- predict.frame[,.(sex_id, new_age, log.prediction, log.st.err)] ##clean dataset

##generate draws and convert from logspace
predict.frame <- alloc.col(predict.frame, 2000) ##allocate more columns
predict.frame <- predict.frame[, (draws) := as.list(exp(rnorm(1000, log.prediction, log.st.err))), by= log.prediction] ##get wide draws
predict.frame <- predict.frame[, mean_emr := rowMeans(.SD), .SDcols = draws] 
predict.frame <- predict.frame[, upper_emr := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
predict.frame <- predict.frame[, lower_emr := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]

predict.frame <- predict.frame[,.(sex_id, new_age, mean_emr, upper_emr, lower_emr)] ##clean dataset
predict.frame <- predict.frame[, merge := 1] ##create variable to merge on 

##get locations for all epi databases
locations <- get_location_metadata(location_set_id = 9) ##get epi location set
locations <- locations[is_estimate==1 & level>=3,] ##keep if is estimate and if is national or subnational
locations <- locations[,.(location_id, location_name, level, parent_id)] ##clean dataset
locations <- locations[, merge := 1] ##create variable to merge on
us.sub <- as.list(locations[parent_id==102 & !location_id==385, .(location_id)]) ##list of us subnationals

##merge locations with predict.frame
all.emr <- merge(locations, predict.frame, by="merge", allow.cartesian=T) ##merge to create rows of emr by age and sex for all locations

##replace values with true values for countries used in the regression
all.emr <- all.emr[location_id %in% emr.data$location_id, mean_emr := emr.data$mean_emr]
all.emr <- all.emr[location_id %in% emr.data$location_id, upper_emr := emr.data$upper_emr]
all.emr <- all.emr[location_id %in% emr.data$location_id, lower_emr := emr.data$lower_emr]

##get us emr data
us.data <- as.vector(emr.data[location_id==102, .(mean_emr, upper_emr, lower_emr)])

##replace values with true values for US subnationals 
all.emr <- all.emr[location_id %in% us.sub$location_id, c("mean_emr", "upper_emr", "lower_emr") := us.data]

##get age start and age end
age.groups <- as.data.table(get_ids("age_group")) ##get age group id's 
age.groups <- age.groups <- age.groups[age_group_id %in% c(1, 17:20, 30:32, 235), ] ##only the id's we want 
age.groups <- age.groups[, age_start := str_sub(age_group_name, start=1, end=2)] ##get age start
age.groups <- age.groups[, age_end := str_sub(age_group_name, start=7, end=8)] ##get age end
age.groups <- age.groups[age_group_id==1, age_start := 20] ##set age group 1 manually
age.groups <- age.groups[age_group_id==1, age_end := 59]
age.groups <- age.groups[age_group_id==235, age_start := 95]
age.groups <- age.groups[age_group_id==235, age_end := 99]
setnames(age.groups, "age_group_id", "new_age") ##make the names match for the merge
age.groups <- age.groups[,.(new_age, age_start, age_end)] ##only the columns that we want

##create outputs for uploader
all.emr <- all.emr[, bundle_id := 146]
all.emr <- all.emr[, nid := 280480]
all.emr <- all.emr[, year_start :=  1990]
all.emr <- all.emr[, year_end := 2016]
all.emr <- merge(all.emr, age.groups, by="new_age")
all.emr <- all.emr[sex_id==1, sex := "Male"]
all.emr <- all.emr[sex_id==2, sex := "Female"]
all.emr <- all.emr[, source_type := "Mixed or estimation"]
all.emr <- all.emr[, measure := "mtexcess"]
all.emr <- all.emr[, unit_type := "Person*year"]
all.emr <- all.emr[, unit_value_as_published := 1]
all.emr <- all.emr[, (zero.vars) := 0]
all.emr <- all.emr[, uncertainty_type_value := 95]
all.emr <- all.emr[, representative_name := "Nationally and subnationally representative"]
all.emr <- all.emr[, urbanicity_type := "Unknown"]
all.emr <- all.emr[, recall_type := "Point"]
all.emr <- all.emr[, recall_type_value:= 1]
all.emr <- all.emr[, extractor := "USERNAME"]
all.emr <- all.emr[, note_modeler := "data estimated using linear regression on 4 top performing countries"]
setnames(all.emr, "mean_emr", "mean")
setnames(all.emr, "upper_emr", "upper")
setnames(all.emr, "lower_emr", "lower")
all.emr <- all.emr[, (blank.vars) := ""]
all.emr <- all.emr[,c(epi.order), with=F]
setcolorder(all.emr, epi.order)

##output in csv (need to change to excel mannually)
write.csv(all.emr, paste0(FILEAPTH, ".csv"), row.names = F)

