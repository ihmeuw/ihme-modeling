#######################################################################################
### Date:     21st June 2019
### Purpose:  Age-split estimates for GBD 2019
#######################################################################################

bundle_id <- "BUNDLE ID"
acause <- "ACAUSE"
bundle_version <- "BUNDLE VERSION"
crosswalk_version <- "CROSSWALK VERSION"
original_me <- "ORIGINAL ME"
age_pattern_me <- "AGE PATTERN ME"
d_step <- 'DECOMP STEP'
note_variable <- "note_modeler"
age <- c(2:20, 30:32, 235) ##epi ages

need_to_update_bundle <- F
need_to_update_crosswalk <- F

## Load libraries and functions ##

library(data.table)
library(openxlsx)
library(msm)

source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")

v_id <- "BUNDLE VERSION ID"

c_id <- "PRE-AGE SPLIT CROSSWALK VERSION ID"

#### Pull crosswalk version and age-split it ---------------------------

gbd_id <- age_pattern_me
output_file <- "results_age_split" ##name your output file

blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "seq", "standard_error", "cases")
draws <- paste0("draw_", 0:999)
age <- c(2:20, 30:32, 235) ##epi ages

##Load data
crosswalked_data <- get_crosswalk_version(crosswalk_version)
all_age <- copy(crosswalked_data)
epi_order <- names(all_age)

##Get and format data points to split
all_age <- all_age[measure %in% c("prevalence", "incidence"),]
all_age <- all_age[!group_review==0 | is.na(group_review),] ##don't use group_review 0
all_age <- all_age[is_outlier==0,] ##don't age split outliered data
all_age <- all_age[(age_end-age_start)>25,]
all_age <- all_age[!mean ==0, ] ##don't split points with zero prevalence
all_age[, sex_id := ifelse(sex == "Male", 1, ifelse(sex == "Female", 2, 3))]

all_age[measure == "prevalence", measure_id := 5]
all_age[measure == "incidence", measure_id := 6]
all_age[, year_id := year_start] ##so that can merge on year later

##Calculate cases and sample size if missing
all_age[is.na(sample_size) & is.na(effective_sample_size), imputed_sample_size := 1]

all_age[measure == "prevalence" & is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
all_age[measure == "prevalence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean*(1-mean)/standard_error^2]
all_age[measure == "incidence" & is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
all_age[measure == "incidence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean/standard_error^2]
all_age[is.na(cases), cases := sample_size * mean]
all_age <- all_age[!cases==0,] ##don't want to split points with zero cases

## Round age groups to the nearest 5-y boundary
all_age_round <- copy(all_age)
all_age_round <- all_age[, age_start := age_start - age_start %%5]
all_age_round[, age_end := age_end - age_end %%5 + 4]
all_age_round <- all_age_round[age_end > 99, age_end := 99]

## Expand for age
all_age_round[, n.age:=(age_end+1 - age_start)/5]
all_age_round[, age_start_floor:=age_start]
all_age_round[, drop := cases/n.age] ##drop the data points if cases/n.age is less than 1
all_age_round <- all_age_round[!drop<1,]
all_age_parents <- copy(all_age_round) ##keep copy of parents to attach on later
expanded <- rep(all_age_round$seq, all_age_round$n.age) %>% data.table("seq" = .)
split <- merge(expanded, all_age_round, by="seq", all=T)
split[,age.rep:= 1:.N - 1, by =.(seq)]
split[,age_start:= age_start+age.rep*5]
split[, age_end :=  age_start + 4 ]

##Get super region information and merge on
super_region <- get_location_metadata(location_set_id = 35)
super_region <- super_region[, .(location_id, super_region_id=region_id)]
split <- merge(split, super_region, by = "location_id")
super_regions <- unique(split$super_region_id) ##get super regions for dismod results

## get age group ids
all_age_total <- merge(split, ages, by = c("age_start", "age_end"), all.x = T)

## create age_group_id == 1 for 0-4 age group
all_age_total[age_start == 0 & age_end == 4, age_group_id := 1]
all_age_total <- all_age_total[age_group_id %in% age | age_group_id ==1]

##get locations and years for population info later
pop_locs <- unique(all_age_total$location_id)
pop_years <- unique(all_age_total$year_id)

######GET AND FORMAT AGE PATTERN DATA###############################################################
locations <- super_regions

age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = gbd_id,
                         measure_id = c(5), location_id = locations, source = "epi",
                         status = "best", sex_id = c(1,2), decomp_step = 'iterative',
                         age_group_id = age, year_id = 2019) ##imposing age pattern

population_data <- get_population(location_id = locations, year_id = 2019, sex_id = c(1, 2),
                                  age_group_id = age, decomp_step = d_step)
population_data <- population_data[, .(age_group_id, sex_id, population, location_id)]
age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
age_pattern[, (draws) := NULL]
age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]

##Create age group id 1 (collapse all age groups by summing population weighted rates)
age_1 <- copy(age_pattern)
age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
se <- copy(age_1)
se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] ##just use standard error from 1-4 age group
age_1 <- merge(age_1, population_data, by = c("age_group_id", "sex_id", "location_id"))
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

##Get cases and sample size
age_pattern[measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
age_pattern[, cases_us := sample_size_us * rate_dis]
age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
age_pattern[is.nan(cases_us), cases_us := 0]


##Get sex_id 3
sex_3 <- copy(age_pattern)
sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, rate_dis := cases_us/sample_size_us]
sex_3[measure_id == 5, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
sex_3[is.nan(se_dismod), se_dismod := 0]
sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
sex_3[, sex_id := 3]
age_pattern <- rbind(age_pattern, sex_3)

age_pattern[, super_region_id := location_id]
age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
######################################################################################################

##merge on age pattern info
age_pattern1 <- copy(age_pattern)
all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))

##get population info and merge on
populations <- get_population(location_id = pop_locs, year_id = pop_years, sex_id = c(1, 2, 3), age_group_id = age, decomp_step = d_step)
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
all_age_total[, total_pop := sum(population), by = "seq"]
all_age_total[, sample_size := (population / total_pop) * sample_size]
all_age_total[, cases_dis := sample_size * rate_dis]
all_age_total[, total_cases_dis := sum(cases_dis), by = "seq"]
all_age_total[, total_sample_size := sum(sample_size), by = "seq"]
all_age_total[, all_age_rate := total_cases_dis/total_sample_size]
all_age_total[, ratio := mean / all_age_rate]
all_age_total[, mean := ratio * rate_dis]
######################################################################################################

##Epi uploader formatting
all_age_total[, (blank_vars) := NA] ##these columns need to be blank
#all_age_total[, group := 1]
all_age_total[!is.na(specificity), specificity := paste0(specificity, ", age-split child")]
all_age_total[is.na(specificity), specificity := paste0(specificity, ",age-split child")]
all_age_total[, group_review := 1]
all_age_total <- all_age_total[,c(epi_order), with=F]
setcolorder(all_age_total, epi_order)
all_age_total[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq)]

##Add to originals with group review 0
all_age_parents <- all_age_parents[,c(epi_order), with=F]
setcolorder(all_age_parents, epi_order)
parent_seqs <- all_age_parents$seq

total <- copy(all_age_total)
rm(all_age_total)

setnames(total, note_variable, "note_modeler_info")
total[!is.na(note_modeler_info) & note_modeler_info != "", note_modeler_info := paste0(note_modeler_info, ". Age split using the region age pattern from me_id ", age_pattern_me, ".")]
total[is.na(note_modeler_info) | note_modeler_info == "", note_modeler_info := paste0(note_modeler_info, "Age split using the region age pattern from me_id ", age_pattern_me, ".")]
total[, specificity := gsub("NA", "", specificity)]
total[, note_modeler_info := gsub("NA", "", note_modeler_info)]
setnames(total, "note_modeler_info", note_variable)
total[, study_covariate := ifelse(study_covariate == "ref", "age", paste0(study_covariate, ", age"))]

total <- rbind(crosswalked_data[!(seq %in% parent_seqs),], total)
total[study_covariate!="ref", seq := NA]

## save file

crosswalk_save_folder <- paste0("FILEPATH", acause, "/", bundle_id, "FILEPATH")
crosswalk_save_file <- paste0(crosswalk_save_folder, "crosswalk_", Sys.Date(), "_agesplit_byregion.xlsx")
write.xlsx(total, crosswalk_save_file, sheetName = "extraction")

##### Upload crosswalked dataset to database -----------------------------------------------------------------------

save_crosswalk_version(bundle_version, crosswalk_save_file, description = paste0("CW version ", crosswalk_version, " age-split by regional pattern"))



