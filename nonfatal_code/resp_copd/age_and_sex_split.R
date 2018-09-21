#########################################################################################################
#########################################################################################################
## AGE/SEX SPLITTING -- FOR SOURCES THAT HAVE DATA SPLIT BY AGE AND BY SEX BUT NOT BOTH SIMULTANEOUSLY
#########################################################################################################
#########################################################################################################

###################
### Setting up ####
###################
rm(list=ls())

#load packages, install if missing
require(data.table)
require(xlsx)

#Load Data
df <- fread("FILEPATH.csv"), stringsAsFactors=FALSE)
df[, seq := as.character(seq)] #Allow missing seq for new obs
df[, note_modeler := as.character(note_modeler)]

df_split <- copy(df)  

#subset to data needing age-sex splitting
df_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to age sex split")]
df_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
df_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
df_split[is.na(cases), cases := sample_size * mean]
df_split <- df_split[!is.na(cases),]
df_split[, split := length(specificity[specificity == "age,sex"]), by = list(nid, group, measure, year_start, year_end)]
df_split <- df_split[specificity %in% c("age", "sex") & split == 0,]

#calculate proportion of cases male and female
df_split[, cases_total:= sum(cases), by = list(nid, group, specificity, measure)]
df_split[, prop_cases := round(cases / cases_total, digits = 3)]

#calculate proportion of sample male and female
df_split[, ss_total:= sum(sample_size), by = list(nid, group, specificity, measure)]
df_split[, prop_ss := round(sample_size / ss_total, digits = 3)]

#calculate standard error of % cases & sample_size M and F
df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]

#estimate ratio & standard error of ratio % cases : % sample
df_split[, ratio := round(prop_cases / prop_ss, digits = 3)]
df_split[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
#Save these for later
df_ratio <- df_split[specificity == "sex", list(nid, group, sex, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end)]

#Create age,sex observations 
age.sex <- copy(df_split[specificity == "age"])
age.sex[,specificity := "age,sex"]
age.sex[,seq := ""]
age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL]
male <- copy(age.sex[, sex := "Male"])
female <- copy(age.sex[, sex := "Female"])
age.sex <- rbind(male, female)   
#Merge sex ratios to age,sex observations
age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "sex", "measure", "year_start", "year_end"))

#calculate age-sex specific mean, standard_error, cases, sample_size
age.sex[, standard_error := round(sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2), digits = 4)]
age.sex[, mean := mean * ratio]
age.sex[, cases := round(cases * prop_cases, digits = 0)]
age.sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
age.sex[,note_modeler := paste(note_modeler, "| age,sex split using sex ratio", round(ratio, digits = 2))]
age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]

##create unique set of nid and group to pull from age.sex    
age.sex.m <- age.sex[,c("nid","group", "measure"), with=F]    
age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure"))

##merge to get the parent rows from age sex split
parent <- merge(age.sex.m, df, by= c("nid", "group", "measure"))
parent[specificity=="age" | specificity=="sex",group_review:=0]
parent[, note_modeler := paste0(note_modeler, "| parent data, has been age-sex split")]

##final dataset 
total <- rbind(parent, age.sex)

##Save data
write.xlsx("FILEPATH.xlsx"), row.names=F, showNA=F, sheetName="extraction")



##################################################################################################
##################################################################################################
## AGE SPLITTING -- APPLY AGE PATTERN FROM SUPERREGION TO WIDE AGE DATA POINTS
##################################################################################################
##################################################################################################


#Setup
rm(list=ls())

##Packages
library(data.table)
library(openxlsx)

##Objects to set for each age split
region_pattern <- F
location_pattern_id <- 102
study_covariates <- c("cv_marketscan_2000","cv_marketscan_2010","cv_marketscan_2012")
output_file <- "results_age_split_method2" 
age_table_code <- "FILEPATH"

##Other objects
blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "seq", "standard_error", "cases")
epi_order <- c("seq", "seq_parent", "input_type", "underlying_nid", "nid", "underlying_field_citation_value", "page_num", "table_num",
               "source_type", "location_name", "location_id", "smaller_site_unit", "site_memo", "sex", "sex_issue", "year_start", 
               "year_end", "year_issue", "age_start", "age_end", "age_issue", "age_demographer", "measure", "mean", "lower", "upper",
               "standard_error", "effective_sample_size", "cases", "sample_size", "unit_type", "unit_value_as_published", "measure_issue",
               "measure_adjustment", "uncertainty_type", "uncertainty_type_value", "design_effect","representative_name", "urbanicity_type", "recall_type",
               "recall_type_value", "sampling_type", "response_rate", "case_name", "case_definition", "case_diagnostics", "group", 
               "specificity", "group_review", "note_modeler", "note_SR", "extractor", "is_outlier", "field_citation_value")
epi_order <- c(epi_order, study_covariates)
draws <- paste0("draw_", 0:999)
age <- c(2:20, 30:32, 235) ##epi ages

##Set directories
central_function <- "FILEPATH"
uploads <- "FILEPATH"
downloads <- "FILEPATH"

##Get central functions
source(paste0(central_function, "get_draws.R"))
source(paste0(central_function, "get_population.R"))
source(paste0(central_function, "get_location_metadata.R"))
source(age_table_code)

##Load data 
all_age <- as.data.table(read.xlsx("FILEPATH.xlsx"), sheet = 1)) ##get all age data

##make sure all necessary columns exist
vars <- names(all_age)
diff <- setdiff(epi_order, vars)
if (!length(diff) == 0) {
  all_age[, (diff) := ""]
}

##Get and format data points to split
all_age <- all_age[measure %in% c("prevalence", "incidence"),]
all_age <- all_age[!group_review==0 | is.na(group_review),] ##don't use group_review 0
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
all_age[, year_id := year_start] ##so that can merge on year later


##Calculate cases and sample size if missing
all_age[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
all_age[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
all_age[is.na(cases), cases := sample_size * mean]
all_age <- all_age[!cases==0,] ##don't want to split points with zero cases

## Round age groups to the nearest 5-y boundary
all_age_round <- copy(all_age)
all_age_round[, age_start := age_start - age_start %%5]
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
super_region <- get_location_metadata(location_set_id = 22)
super_region <- super_region[, .(location_id, super_region_id)]
split <- merge(split, super_region, by = "location_id")
super_regions <- unique(split$super_region_id) ##get super regions for dismod results

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
  locations <- super_regions
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
se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] ##just use standard error from 1-4 age group
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
age_pattern[is.nan(sample_size_us), sample_size_us := 0] 
age_pattern[is.nan(cases_us), cases_us := 0]


##Get sex_id 3
sex_3 <- copy(age_pattern)
sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
sex_3[, rate_dis := cases_us/sample_size_us]
sex_3[measure_id == 5, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
sex_3[is.nan(rate_dis), rate_dis := 0] 
sex_3[is.nan(se_dismod), se_dismod := 0]
sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
sex_3[, sex_id := 3]
age_pattern <- rbind(age_pattern, sex_3)

age_pattern[, super_region_id := location_id]
age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
######################################################################################################

##merge on age pattern info
if (region_pattern == T) {
  age_pattern1 <- copy(age_pattern)
  all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
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
all_age_total[, group := 1]
all_age_total[, specificity := paste0(specificity, "age-split child")]
all_age_total[, group_review := 1]
all_age_total <- all_age_total[,c(epi_order), with=F]
setcolorder(all_age_total, epi_order)

##Add to originals with group review 0
all_age_parents <- all_age_parents[,c(epi_order), with=F]
setcolorder(all_age_parents, epi_order)
invisible(all_age_parents[, group_review := 0])
invisible(all_age_parents[, group := 1])
invisible(all_age_parents[, specificity := paste0(specificity, "age-split parent")])
total <- rbind(all_age_parents, all_age_total)
if (region_pattern ==T) {
  total[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern")]
} else {
  total[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_pattern_id)]
}

write.xlsx(total, paste0(uploads, output_file, ".xlsx"), sheetName = "extraction") 