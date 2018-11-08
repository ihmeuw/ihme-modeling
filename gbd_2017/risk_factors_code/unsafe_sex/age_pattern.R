##################################################################################################
### Title: Age-splits for unsafe sex models
### Project: GBD Risk Factors, Unsafe Sex
#################################################################################################

#######################################################
## SETUP
#######################################################

rm(list=ls())

##Packages
library(data.table)
library(openxlsx)
library(tidyr)

##Objects to set for each age split
bundle_idu <- 385
bundle_sex <- 386
bundle_other <- 387
request_num_idu <- 6041
request_num_sex <- 73271
request_num_other <- 6039
acause <- "unsafe_sex"
study_cvs_idu <- c("cv_aids_transmission", "cv_conc_epidemic", "cv_cumulative", "cv_ecdc", "cv_mtct", "cv_unk_transmission")
study_cvs_sex <- c("cv_unk_transmission", "cv_aids_transmission", "cv_conc_epidemic", "cv_cumulative")
output_file <- "results_age_split"

##Other objects
blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "seq", "standard_error", "cases")
epi_order <- c("seq", "seq_parent", "input_type", "underlying_nid", "nid", "underlying_field_citation_value", "page_num", "table_num",
               "source_type", "location_name", "location_id", "smaller_site_unit", "site_memo", "sex", "sex_issue", "year_start", 
               "year_end", "year_issue", "age_start", "age_end", "age_issue", "age_demographer", "measure", "mean", "lower", "upper",
               "standard_error", "effective_sample_size", "cases", "sample_size", "unit_type", "unit_value_as_published", "measure_issue",
               "measure_adjustment", "uncertainty_type", "uncertainty_type_value", "design_effect","representative_name", "urbanicity_type", "recall_type",
               "recall_type_value", "sampling_type", "response_rate", "case_name", "case_definition", "case_diagnostics", "group", 
               "specificity", "group_review", "note_modeler", "note_SR", "extractor", "is_outlier", "field_citation_value")
epi_order_idu <- c(epi_order, study_cvs_idu)
epi_order_sex <- c(epi_order, study_cvs_sex)
draws <- paste0("draw_", 0:999)
age <- c(2:20, 30:32, 235) ##epi ages

##Set directories
central_function <- "FILEPATH"
uploads_idu <- "FILEPATH"
downloads_idu <- "FILEPATH"
uploads_sex <- "FILEPATH"
downloads_sex <- "FILEPATH"


##Get central functions
source("FILEPATH/get_draws.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_outputs.R")

## age table code
ages <- get_ids(table = "age_group")
dems <- get_demographics(gbd_team = "epi")$age_group_ids
ages <- ages[age_group_id %in% dems & !age_group_id %in% c(2, 3, 4), ]
neonates <- data.table(age_group_name = c("0 to 0.01", "0.01 to 0.1", "0.1 to 0.997"), age_group_id = c(2, 3, 4))
ages <- rbind(ages, neonates, use.names = TRUE)
ages <- separate(ages, age_group_name, into = c("age_start", "age_end"), sep = " to ")
ages[, age_start := gsub(" plus", "", age_start)]
invisible(ages[age_group_id == 235, age_end := 99])
ages[, age_start := as.numeric(age_start)]
ages[, age_end := as.numeric(age_end)]

########################################################
## (1) upload & prep data for age split
########################################################

##Load data 
all_age <- as.data.table(read.xlsx(paste0(downloads_idu, "request_", request_num_idu, ".xlsx"), sheet = 1)) ##get all age data
epi_order <- epi_order_idu

##make sure all necessary columns exist
vars <- names(all_age)
diff <- setdiff(epi_order, vars)
if (!length(diff) == 0) {
  all_age[, (diff) := ""]
}

##Get and format data points to split
all_age <- all_age[age_start>= 15] ##age pattern starts at 15 so have to start there
all_age <- all_age[!group_review==0 | is.na(group_review),] ##don't use group_review 0
all_age <- all_age[is_outlier==0,] ##don't age split outliered data
all_age <- all_age[(age_end-age_start)>20,] ##age split if the age range is >20
all_age <- all_age[!mean ==0, ] ##don't split points with zero prevalence
all_age[, sex_id := sex]
all_age[sex_id=="Both", sex_id :=3]
all_age[sex_id=="Female", sex_id := 2]
all_age[sex_id=="Male", sex_id :=1]
all_age[, sex_id := as.integer(sex_id)]
all_age[, measure_id := 18]

##get year_id rounded to nearest 5 years
all_age[year_start %in% c(1980:1992), year_id := 1990] ## so that can merge on year later
all_age[year_start %in% c(1993:1997), year_id := 1995]
all_age[year_start %in% c(1998:2002), year_id := 2000]
all_age[year_start %in% c(2003:2007), year_id := 2005]
all_age[year_start %in% c(2008:2012), year_id := 2010]
all_age[year_start %in% c(2013:2016), year_id := 2016]

##Calculate cases and sample size if missing
all_age[is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
all_age[is.na(cases), cases := sample_size * mean]
all_age <- all_age[!cases==0,] ##don't want to split points with zero cases

## Round age groups to the nearest 5-y boundary
all_age_round <- copy(all_age)
all_age_round[, age_start := age_start - age_start %%5]
all_age_round[, age_end := age_end - age_end %%5 + 4]
all_age_round <- all_age_round[age_end > 99, age_end := 99]

## Expand for age (and extend out to age 99 for all points)
all_age_round[, age_end_end := 99]
all_age_round[, n.age:=(age_end_end+1 - age_start)/5]
all_age_round[, age_start_floor:=age_start]
all_age_round[, drop := cases/n.age]
all_age_round <- all_age_round[!drop<0.5,] 
all_age_parents <- copy(all_age_round) ##keep copy of parents to attach on later
expanded <- rep(all_age_round$seq, all_age_round$n.age) %>% data.table("seq" = .)
split <- merge(expanded, all_age_round, by="seq", all=T)
split[,age.rep:= 1:.N - 1, by =.(seq)]
split[,age_start:= age_start+age.rep*5]
split[age_start >= age_end, expanded := 1] ##mark expanded data points
split[is.na(expanded), expanded := 0]
split[, age_end :=  age_start + 4 ]
split[, age_end_end := NULL]

##Get super region information and merge on 
super_region <- get_location_metadata(location_set_id = 22)
super_region <- super_region[, .(location_id, super_region_id)]
split <- merge(split, super_region, by = "location_id")
super_regions <- unique(split$super_region_id) ##get super regions for dismod results

## get age group ids
all_age_total <- merge(split, ages, by = c("age_start", "age_end"), all.x = T)

## create age_group_id == 1 for 0-4 age group
all_age_total[age_start == 0 & age_end == 4, age_group_id := 1]
all_age_total <- all_age_total[age_group_id %in% age | age_group_id ==1]

##Save locations and years
pop_locs_idu <- unique(all_age_total$location_id) ## save locations and years, used later
pop_years_idu <- unique(all_age_total$year_id)

########################################################
## (2) pull IDU-Hepatitis B PAF
########################################################

age_pattern <- as.data.table(get_draws(gbd_id_field = c("cause_id", "rei_id"), gbd_id = c(402, 103), 
                                       location_ids = super_regions, source = "burdenator", metric_ids = 2,
                                       status = "latest",  age_group_ids = age, measure_id = 3, 
                                       year_ids = 2016)) ##imposing age pattern (switch to status=best when there is one)

#########################################################
## (3) extract age-pattern
#########################################################

##need denominator for hepatitis PAF (chronic hep B infection)
us_population <- as.data.table(get_outputs(topic = "cause", cause_id = 402, location_id = super_regions, 
                                           year_id = 2016, sex_id = c(1, 2), age_group_id = age, compare_version_id = 172,
                                           measure_id = 5))
setnames(us_population, "val", "population")
us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]

age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
age_pattern[, (draws) := NULL]
age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]

## Create age group id 1 (collapse all age groups by summing population weighted rates)
age_1 <- copy(age_pattern)
age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
se <- copy(age_1)
se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)]
age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
age_1[, frac_pop := population / total_pop]
age_1[, weight_rate := rate_dis * frac_pop]
age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
age_1[, age_group_id := 1]
age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
age_pattern <- rbind(age_pattern, age_1)

## Get cases and sample size
age_pattern[, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
age_pattern[, cases_us := sample_size_us * rate_dis]
age_pattern[is.nan(sample_size_us), sample_size_us := 0]
age_pattern[is.nan(cases_us), cases_us := 0]

## Get sex_id 3
sex_3 <- copy(age_pattern)
sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, rate_dis := cases_us/sample_size_us]
sex_3[, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
sex_3[is.nan(rate_dis), rate_dis := 0] 
sex_3[is.nan(se_dismod), se_dismod := 0]
sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
sex_3[, sex_id := 3]
age_pattern <- rbind(age_pattern, sex_3)

age_pattern[, super_region_id := location_id]
age_pattern <- age_pattern[ ,.(age_group_id, sex_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]

write.csv(age_pattern, "FILEPATH/age_pattern_drugs.csv", row.names = F)

############################################################
## (4) apply age pattern to HIV due to IDU data
############################################################

## merge on age pattern info
all_age_total <- merge(all_age_total, age_pattern, by = c("sex_id", "age_group_id", "super_region_id"))

hiv <- get_draws(gbd_id_field = "modelable_entity_id", gbd_id = 9368, source = "epi", ages = age, 
                   status = "best", location_ids = pop_locs_idu, year_ids = pop_years_idu, measure_ids = 5) 
hiv[, mean := rowMeans(.SD), .SDcols = draws]
hiv[, (draws) := NULL]
populations <- as.data.table(get_population(location_id = pop_locs_idu, year_id = pop_years_idu,
                                            sex_id = c(1, 2, 3), age_group_id = age))
populations <- merge(hiv, populations, by = c("age_group_id", "location_id", "sex_id", "year_id"))
populations[, population := mean * population]
populations[, mean := NULL]

populations <- populations[, .(location_id, sex_id, year_id, age_group_id, population)]

##get sex_id 3
sex_3 <- copy(populations)
sex_3[, population := sum(population), by = c("age_group_id", "location_id", "year_id")]
sex_3 <- unique(sex_3, by = c("age_group_id", "location_id", "year_id"))
sex_3[, sex_id := 3]
populations <- rbind(populations, sex_3)

age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
age_1[, age_group_id := 1]
populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
populations <- rbind(populations, age_1)  ##add age group id 1 back on


all_age_total <- merge(all_age_total, populations, by = c("location_id", "sex_id", "year_id", "age_group_id"))


#####CALCULATE AGE SPLIT POINTS#######################################################################
##Create new split data points
all_age_total[expanded == 0, total_pop := sum(population), by = "seq"]
all_age_total[expanded == 0, sample_size := (population / total_pop) * sample_size]
all_age_total[expanded == 0, cases_dis := sample_size * rate_dis]
all_age_total[expanded == 0, total_cases_dis := sum(cases_dis), by = "seq"]
all_age_total[expanded == 0, total_sample_size := sum(sample_size), by = "seq"]
all_age_total[expanded == 0, all_age_rate := total_cases_dis/total_sample_size]
all_age_total[expanded == 0, ratio := mean / all_age_rate]

##Get ratios by sequence number
ratios <- copy(all_age_total)
ratios <- ratios[expanded == 0, .(seq, ratio)]
ratios <- unique(ratios)

##Merge ratios back on
all_age_total[, ratio := NULL]
all_age_total <- merge(ratios, all_age_total, by = "seq")

##Calculate means
all_age_total[, mean := ratio * rate_dis]

##Get rid of entire seq if one of means is over 1
over1 <- copy(all_age_total)
over1 <- over1[mean > 1]
over1_seqs <- unique(over1$seq)
all_age_total <- all_age_total[!seq %in% c(over1_seqs)]

##find covariate of variation for the oldest point that is not expanded
cv <- copy(all_age_total)
cv <- cv[expanded == 0]
cv[, max_agestart := max(age_start), by = "seq"]
cv <- cv[age_start == max_agestart]
cv[, se := sqrt(mean * (1-mean) / sample_size)]
cv[, cv := se / mean]
cv <- cv[, .(seq, cv)]
cv <- unique(cv)

##apply that covariate of variation to get sample sizes for expanded points
all_age_total <- merge(all_age_total, cv, by = "seq")
all_age_total[expanded == 1, standard_error := cv * mean]
all_age_total[expanded == 1, sample_size := mean * (1 - mean) / standard_error^2]
all_age_total[, cv := NULL]
######################################################################################################

##Epi uploader formatting
all_age_total <- all_age_total[, (blank_vars) := ""] ##these columns need to be blank
all_age_total[, group := 1]
all_age_total[, specificity := paste0(specificity, " | age-split child")]
all_age_total[, group_review := 1]
all_age_total <- all_age_total[,c(epi_order), with=F]
setcolorder(all_age_total, epi_order)

##Add to originals with group review 0 (exclude any seqs with split means over 1 which were excluded)
all_age_parents <- all_age_parents[!seq %in% c(over1_seqs)]
all_age_parents <- all_age_parents[,c(epi_order), with=F]
setcolorder(all_age_parents, epi_order)
invisible(all_age_parents[, group_review := 0])
invisible(all_age_parents[, group := 1])
invisible(all_age_parents[, specificity := paste0(specificity, " | age-split parent")])
total <- rbind(all_age_parents, all_age_total)
total[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern")]

## export .xlsx ready for upload for HIV-IDU with age-split points
write.xlsx(total, paste0(uploads_idu, output_file, ".xlsx"), sheetName = "extraction") 

###################################################################################################
## (5) Prep Data for Sexual Transmission
###################################################################################################

all_age <- as.data.table(read.xlsx(paste0(downloads_sex, "request_", request_num_sex, ".xlsx"), sheet = 1)) ##get all age data
epi_order <- epi_order_sex

##make sure all necessary columns exist
vars <- names(all_age)
diff <- setdiff(epi_order, vars)
if (!length(diff) == 0) {
  all_age[, (diff) := ""]
}

##Get and format data points to split
all_age <- all_age[age_start>= 15] ##age pattern starts at 15 so have to start there
all_age <- all_age[!group_review==0 | is.na(group_review),] ##don't use group_review 0
all_age <- all_age[is_outlier==0,] ##don't age split outliered data
all_age <- all_age[(age_end-age_start)>20,] ##age split if the age range is >20
all_age <- all_age[!mean ==0, ] ##don't split points with zero prevalence
all_age[, sex_id := sex]
all_age[sex_id=="Both", sex_id :=3]
all_age[sex_id=="Female", sex_id := 2]
all_age[sex_id=="Male", sex_id :=1]
all_age[, sex_id := as.integer(sex_id)]
all_age[, measure_id := 18]

##get year_id rounded to nearest 5 years
all_age[year_start %in% c(1980:1992), year_id := 1990] ## so that can merge on year later
all_age[year_start %in% c(1993:1997), year_id := 1995]
all_age[year_start %in% c(1998:2002), year_id := 2000]
all_age[year_start %in% c(2003:2007), year_id := 2005]
all_age[year_start %in% c(2008:2012), year_id := 2010]
all_age[year_start %in% c(2013:2016), year_id := 2016]

##Calculate cases and sample size if missing
all_age[is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
all_age[is.na(cases), cases := sample_size * mean]
all_age <- all_age[!cases==0,] ##don't want to split points with zero cases

## Round age groups to the nearest 5-y boundary
all_age_round <- copy(all_age)
all_age_round[, age_start := age_start - age_start %%5]
all_age_round[, age_end := age_end - age_end %%5 + 4]
all_age_round <- all_age_round[age_end > 99, age_end := 99]

## Expand for age (and extend out to age 99 for all points)
all_age_round[, age_end_end := 99]
all_age_round[, n.age:=(age_end_end+1 - age_start)/5]
all_age_round[, age_start_floor:=age_start]
all_age_round[, drop := cases/n.age] 
all_age_round <- all_age_round[!drop<0.5,] 
all_age_parents <- copy(all_age_round) ##keep copy of parents to attach on later
expanded <- rep(all_age_round$seq, all_age_round$n.age) %>% data.table("seq" = .)
split <- merge(expanded, all_age_round, by="seq", all=T)
split[,age.rep:= 1:.N - 1, by =.(seq)]
split[,age_start:= age_start+age.rep*5]
split[age_start >= age_end, expanded := 1] ##mark expanded data points
split[is.na(expanded), expanded := 0]
split[, age_end :=  age_start + 4 ]
split[, age_end_end := NULL]

##Get super region information and merge on 
super_region <- get_location_metadata(location_set_id = 22)
super_region <- super_region[, .(location_id, super_region_id)]
split <- merge(split, super_region, by = "location_id")
super_regions <- unique(split$super_region_id) ##get super regions for dismod results

## get age group ids
all_age_total <- merge(split, ages, by = c("age_start", "age_end"), all.x = T)

## create age_group_id == 1 for 0-4 age group
all_age_total[age_start == 0 & age_end == 4, age_group_id := 1]
all_age_total <- all_age_total[age_group_id %in% age | age_group_id ==1]

##Save locations and years
pop_locs_sex <- unique(all_age_total$location_id) ## save locations and years, used later
pop_years_sex <- unique(all_age_total$year_id)

###################################################################################################
## (6) Pull and calculate age-pattern for sexual transmission assuming no age-pattern in "other" category
###################################################################################################

age_pattern <- as.data.table(get_draws(gbd_id_field = c("cause_id", "rei_id"), gbd_id = c(402, 103), 
                                       location_ids = super_regions, source = "burdenator", metric_ids = 2,
                                       status = "best",  age_group_ids = age, measure_id = 3, 
                                       year_ids = 2016)) ##imposing age pattern (switch to status=best when there is one)

#########################################################
## (3) extract age-pattern
#########################################################

##need denominator for hepatitis PAF (chronic hep B infection)
us_population <- as.data.table(get_outputs(topic = "cause", cause_id = 402, location_id = super_regions, 
                                           year_id = 2016, sex_id = c(1, 2), age_group_id = age, compare_version_id = 172,
                                           measure_id = 5))
setnames(us_population, "val", "population")
us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]

age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
age_pattern[, (draws) := NULL]
age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]

##Reverse age pattern by inverting it 
age_pattern[, max_rate := max(rate_dis), by = c("sex_id", "location_id")]
age_pattern[, max_rate := max_rate + 0.01]
age_pattern[, rate_dis := max_rate - rate_dis]
age_pattern[, max_rate := NULL]

## Create age group id 1 (collapse all age groups by summing population weighted rates)
age_1 <- copy(age_pattern)
age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
se <- copy(age_1)
se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
age_1[, frac_pop := population / total_pop]
age_1[, weight_rate := rate_dis * frac_pop]
age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
age_1[, age_group_id := 1]
age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
age_pattern <- rbind(age_pattern, age_1)

## Get cases and sample size
age_pattern[, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
age_pattern[, cases_us := sample_size_us * rate_dis]
age_pattern[is.nan(sample_size_us), sample_size_us := 0] 
age_pattern[is.nan(cases_us), cases_us := 0]

## Get sex_id 3
sex_3 <- copy(age_pattern)
sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, rate_dis := cases_us/sample_size_us]
sex_3[, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
sex_3[is.nan(rate_dis), rate_dis := 0] 
sex_3[is.nan(se_dismod), se_dismod := 0]
sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
sex_3[, sex_id := 3]
age_pattern <- rbind(age_pattern, sex_3)

age_pattern[, super_region_id := location_id]
age_pattern <- age_pattern[ ,.(age_group_id, sex_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]

write.csv(age_pattern, "FILEPATH/age_pattern_sex.csv"), row.names = F)

#####################################################
## (7) apply age-pattern to HIV due to sex model
#####################################################

## merge on age pattern info
all_age_total <- merge(all_age_total, age_pattern, by = c("sex_id", "age_group_id", "super_region_id"))

hiv <- get_draws(gbd_id_field = "modelable_entity_id", gbd_id = 9368, source = "epi", ages = age, 
                 status = "best", location_ids = pop_locs_sex, year_ids = pop_years_sex, measure_ids = 5) 
hiv[, mean := rowMeans(.SD), .SDcols = draws]
hiv[, (draws) := NULL]
populations <- as.data.table(get_population(location_id = pop_locs_sex, year_id = pop_years_sex,
                                            sex_id = c(1, 2, 3), age_group_id = age))
populations <- merge(hiv, populations, by = c("age_group_id", "location_id", "sex_id", "year_id"))
populations[, population := mean * population]
populations[, mean := NULL]

populations <- populations[, .(location_id, sex_id, year_id, age_group_id, population)]

##get sex_id 3
sex_3 <- copy(populations)
sex_3[, population := sum(population), by = c("age_group_id", "location_id", "year_id")]
sex_3 <- unique(sex_3, by = c("age_group_id", "location_id", "year_id"))
sex_3[, sex_id := 3]
populations <- rbind(populations, sex_3)

age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
age_1[, age_group_id := 1]
populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
populations <- rbind(populations, age_1)  ##add age group id 1 back on


all_age_total <- merge(all_age_total, populations, by = c("location_id", "sex_id", "year_id", "age_group_id"))

#####CALCULATE AGE SPLIT POINTS#######################################################################
##Create new split data points
all_age_total[expanded == 0, total_pop := sum(population), by = "seq"]
all_age_total[expanded == 0, sample_size := (population / total_pop) * sample_size]
all_age_total[expanded == 0, cases_dis := sample_size * rate_dis]
all_age_total[expanded == 0, total_cases_dis := sum(cases_dis), by = "seq"]
all_age_total[expanded == 0, total_sample_size := sum(sample_size), by = "seq"]
all_age_total[expanded == 0, all_age_rate := total_cases_dis/total_sample_size]
all_age_total[expanded == 0, ratio := mean / all_age_rate]

##Get ratios by sequence number
ratios <- copy(all_age_total)
ratios <- ratios[expanded == 0, .(seq, ratio)]
ratios <- unique(ratios)

##Merge ratios back on
all_age_total[, ratio := NULL]
all_age_total <- merge(ratios, all_age_total, by = "seq")

##Calculate means
all_age_total[, mean := ratio * rate_dis]

##Get rid of entire seq if one of means is over 1
over1 <- copy(all_age_total)
over1 <- over1[mean > 1]
over1_seqs <- unique(over1$seq)
all_age_total <- all_age_total[!seq %in% c(over1_seqs)]

##find covariate of variation for the oldest point that is not expanded
cv <- copy(all_age_total)
cv <- cv[expanded == 0]
cv[, max_agestart := max(age_start), by = "seq"]
cv <- cv[age_start == max_agestart]
cv[, se := sqrt(mean * (1-mean) / sample_size)]
cv[, cv := se / mean]
cv <- cv[, .(seq, cv)]
cv <- unique(cv)

##apply that covariate of variation to get sample sizes for expanded points
all_age_total <- merge(all_age_total, cv, by = "seq")
all_age_total[expanded == 1, standard_error := cv * mean]
all_age_total[expanded == 1, sample_size := mean * (1 - mean) / standard_error^2]
all_age_total[, cv := NULL]
######################################################################################################

##Epi uploader formatting
all_age_total <- all_age_total[, (blank_vars) := ""] ##these columns need to be blank
all_age_total[, group := 1]
all_age_total[, specificity := paste0(specificity, " | age-split child")]
all_age_total[, group_review := 1]
all_age_total <- all_age_total[,c(epi_order), with=F]
setcolorder(all_age_total, epi_order)

##Add to originals with group review 0 (exclude any seqs with split means over 1 which were excluded)
all_age_parents <- all_age_parents[!seq %in% c(over1_seqs)]
all_age_parents <- all_age_parents[,c(epi_order), with=F]
setcolorder(all_age_parents, epi_order)
invisible(all_age_parents[, group_review := 0])
invisible(all_age_parents[, group := 1])
invisible(all_age_parents[, specificity := paste0(specificity, " | age-split parent")])
total <- rbind(all_age_parents, all_age_total)
total[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern")]

## export .xlsx ready for upload for HIV-IDU with age-split points
write.xlsx(total, paste0(uploads_sex, output_file, ".xlsx"), sheetName = "extraction")
