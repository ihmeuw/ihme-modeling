
#Setup
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "/homes/username/"
} else { 
  j_root <- "J:"
  h_root <- "H:"
}


##Packages
library(data.table)
library(openxlsx)

##Set objects for each age split
region_pattern <- T ##if true will use regional pattern for Dismod, if false uses US age pattern
location_pattern_id <- 102 ##location id of the country whose pattern you want to use if not using region pattern
bundle_id <- 157
request_num <- "60776"
acause <- "mental_eating_anorexia" 
gbd_id <- 1978 ##put me_id that you are age_splitting for
output_file <- "results_age_split_5thJune" ##name your output file
age_table_code <- paste0(j_root, "FILEPATH\age_table.R") ##wherever you are keeping the age_table code
note_variable <- "note_modeler"

##Other objects
blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "seq", "standard_error", "cases")
draws <- paste0("draw_", 0:999)
age <- c(2:20, 30:32, 235) ##epi ages

##Set directories
central_function <- paste0(j_root, "FILEPATH")
uploads <- paste0(j_root, "FILEPATH")
downloads <- paste0(j_root, "FILEPATH")

##Get central functions
source(paste0(central_function, "get_draws.R"))
source(paste0(central_function, "get_population.R"))
source(paste0(central_function, "get_location_metadata.R"))
source(age_table_code)

##Load data 
all_age <- as.data.table(read.xlsx(paste0(downloads, "request_", request_num, ".xlsx"), sheet = 1)) ##get all age data
epi_order <- names(all_age)

##make sure all necessary columns exist
vars <- names(all_age)
diff <- setdiff(epi_order, vars)
if (!length(diff) == 0) {
  all_age[, (diff) := ""]
}

##Get and format data points to split
all_age <- all_age[measure %in% c("prevalence", "incidence"),]
all_age <- all_age[!group_review==0 | is.na(group_review),] ##don't age split excluded data
all_age <- all_age[is_outlier==0,] ##don't age split outliered data
all_age <- all_age[(age_end-age_start)>20,]
all_age <- all_age[!mean ==0, ] ##don't split points with zero prevalence
all_age[, sex_id := sex]
all_age[sex_id=="Both", sex_id :=3]
all_age[sex_id=="Female", sex_id := 2]
all_age[sex_id=="Male", sex_id :=1]
all_age[, sex_id := as.integer(sex_id)]
all_age[measure == "prevalence", measure_id := 5]
all_age[measure == "incidence", measure_id := 6]
all_age[, year_id := year_start] 


##Calculate cases and sample size if missing
all_age[measure == "prevalence" & is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
all_age[measure == "prevalence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean(1-mean)/standard_error^2]
all_age[measure == "incidence" & is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
all_age[measure == "incidence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean/standard_error^2]
all_age[, cases := sample_size * mean] #Use "weighted cases"
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
regions <- get_location_metadata(location_set_id = 22)
regions <- regions[, .(location_id, region_id)]
split <- merge(split, regions, by = "location_id")
regions <- unique(split$region_id) ##get super regions for dismod results

## get age group ids
all_age_total <- merge(split, ages, by = c("age_start", "age_end"), all.x = T)

## create age_group_id == 1 for 0-4 age group
all_age_total[age_start == 0 & age_end == 4, age_group_id := 1]
all_age_total <- all_age_total[age_group_id %in% age | age_group_id ==1] ##don't keep where age group id isn't estimated for cause

##get locations and years for population info later
pop_locs <- unique(all_age_total$location_id)
pop_years <- unique(all_age_total$year_id)

######GET AND FORMAT AGE PATTERN DATA###############################################################
if (region_pattern == T) {
  locations <- regions
} else {
  locations <- location_pattern_id
}
age_pattern <- as.data.table(get_draws(gbd_id_field = "modelable_entity_id", gbd_id = gbd_id, 
                                       measure_ids = c(5, 6), location_ids = locations, source = "epi",
                                       status = "best", sex_ids = c(1,2),
                                       age_group_ids = age, year_ids = 2016)) ##imposing age pattern 
us_population <- as.data.table(get_population(location_id = locations, year_id = 2016, sex_id = c(1, 2), 
                                           age_group_id = age))
us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
age_pattern[, (draws) := NULL]
age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]

##Create age group id 1 (collapse all age groups by summing population weighted rates)
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

age_pattern[, region_id := location_id]
age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, region_id)]
######################################################################################################

##merge on age pattern info
if (region_pattern == T) {
  age_pattern1 <- copy(age_pattern)
  all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "region_id"))
} else {
  age_pattern1 <- copy(age_pattern)
  all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
}

##get population info and merge on
populations <- as.data.table(get_population(location_id = pop_locs, year_id = pop_years,
                                            sex_id = c(1, 2, 3), age_group_id = age))
populations[, process_version_map_id := NULL]
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
all_age_total <- all_age_total[, (blank_vars) := ""] ##these columns need to be blank
all_age_total[!is.na(specificity), specificity := paste0(specificity, ", age-split child")]
all_age_total[is.na(specificity), specificity := paste0(specificity, ",age-split child")]
all_age_total[, group_review := 1]
all_age_total <- all_age_total[,c(epi_order), with=F]
setcolorder(all_age_total, epi_order)

##Add to originals with group review 0
all_age_parents <- all_age_parents[,c(epi_order), with=F]
setcolorder(all_age_parents, epi_order)
invisible(all_age_parents[, group_review := 0])
invisible(all_age_parents[!is.na(specificity), specificity := paste0(specificity, ", age-split parent")])
invisible(all_age_parents[is.na(specificity), specificity := paste0(specificity, "age-split parent")])
total <- rbind(all_age_parents, all_age_total)
setnames(total, note_variable, "note_modeler_info")
if (region_pattern ==T) {
  total[group_review==1, note_modeler_info := paste0(note_modeler_info, "| Age split using the region age pattern.")]
  total[group_review==0, note_modeler_info := paste0(note_modeler_info, "| GR 0. Age split using the region age pattern in separate rows.")]
} else {
  total[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_pattern_id)]
}
total[, specificity := gsub("NA", "", specificity)]
total[, note_modeler_info := gsub("NA", "", note_modeler_info)]
setnames(total, "note_modeler_info", note_variable)
total[group_review==0, input_type := "group_review"]
total[is.na(group), group := nid]
total[, unit_value_as_published := 1]

write.xlsx(total, paste0(uploads, acause,"_", bundle_id, "_", output_file, ".xlsx"), sheetName = "extraction") 

