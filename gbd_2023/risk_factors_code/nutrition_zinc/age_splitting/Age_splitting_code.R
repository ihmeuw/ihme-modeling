# age splitting code for micro zinc deficiencies
# Author: NAME: 
# Initial Date: 
# Last Updated: 
## CC libraries 


source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")
library(data.table)

## age splitting package

library(reticulate)
reticulate::use_python("FILEPATH/python")
splitter <- import("pydisagg.ihme.splitter")

## Load data 

inputdata = fread("FILEPATH")
inputdata1 = as.data.table(copy(inputdata))
ages = get_age_metadata(release_id = 16)
data_to_split = inputdata1[age_group_id ==22,]


## Check the age start is always smaller than age_end

data_to_split = data_to_split[original_age_start < original_age_end,]
age_start_equal_age_end = inputdata1[inputdata1$original_age_start == inputdata1$original_age_end, ]
data_to_split$sex_id[data_to_split$sex=="Male"] <- 1
data_to_split$sex_id[data_to_split$sex=="Female"] <- 2
data_to_split = data_to_split[standard_error > 0,]
data_to_split$year_id = as.integer(data_to_split$year_id)

## load the age pattern data 

age_pattern =  fread("FILEPATH")  

## merge the age pattern with age meta-data 

age_pattern = merge(age_pattern, ages , c("age_group_id"), all.x = TRUE)

age_pattern = age_pattern[, year_id := NULL]

## get population 

pop <- get_population(location_id = 'all', year_id= c(min(inputdata1$year_start): max(inputdata1$year_end)),
                      sex_id='all', age_group_id = 'all', with_ui=TRUE, release_id=16)

## config for age splitting 

data_config <- splitter$AgeDataConfig(
  index=c("underlying_nid","nid", "seq", "location_id", "year_id", "sex_id"),
  age_lwr="original_age_start",
  age_upr="original_age_end",
  val="mean",
  val_sd="standard_error"
)

# save the column names with "draw_" at the beginning

draw_cols <- grep("^draw_", names(age_pattern), value = TRUE)

 pattern_config <- splitter$AgePatternConfig(
  by=list("sex_id", "location_id"),
  age_key="age_group_id",
  age_lwr="age_group_years_start",
  age_upr="age_group_years_end",
  draws=draw_cols
 )


 pop_config <- splitter$AgePopulationConfig(
  index=c("age_group_id", "location_id", "year_id", "sex_id"),
  val="population"
)


##

age_splitter <- splitter$AgeSplitter(
  data=data_config, pattern=pattern_config, population=pop_config)

## split the data

result <- age_splitter$split(
  data=data_to_split,
  pattern=age_pattern,
  population=pop,
  model="rate",
  output_type="rate"
)

#`````````````````````````` Post_age splitting processing```````````````````````````````````````````````````````````````````#` 

#subset the columns needed for crosswalk upload

setDT(result)
result1 = result[, .(nid, underlying_nid, seq, location_id, year_id, sex_id, age_group_id,
                     original_age_start, original_age_end, mean, standard_error, age_split_result, age_split_result_se)]

## rename the columns

setnames(result1, c("mean", "standard_error", "age_split_result", "age_split_result_se"), c("original_mean", "original_standard_error","mean", "standard_error"))
result1$was_age_splitted = 1

### Append with data that did not need age splitting: 

age_specific_data = inputdata1[age_group_id !=22,]

age_specific_data = age_specific_data[,.(nid, underlying_nid, seq, location_id, year_id, sex_id, age_group_id,original_age_start, original_age_end, mean, 
                                        standard_error)]

age_specific_data$original_mean = age_specific_data$mean
age_specific_data$original_standard_error = age_specific_data$standard_error

age_specific_data$was_age_splitted = 0 


## age splitted data

age_splitted_data = rbind(result1, age_specific_data)   


## subset the columns needed for crosswalk upload 

inputdata1 = inputdata1[, c("location_id", "underlying_nid", "nid", "field_citation_value", "source_type", "location_name", "sex",
  "sex_issue", "year_start", "year_end", "year_issue", "sample_size", "representative_name" ,"urbanicity_type", "extractor",
  "bundle_id" , "seq", "is_outlier", "indicator", "survey_title", "representativeness", "population", "biomarker_sample.type","Method.of.analysis",
  "inidicator_comments", "Survey.Methodology", "Survey.type", "reference", "location_set_version_id", "original_year_start", "original_year_end",
  "original_age_end", "original_age_start","underlying_field_citation_value", "input_type_id", "sex_splitted", "year_id", "sex_id")]


inputdata1$year_id = as.integer(inputdata1$year_id)


## merge the age_splitted data with the original dataset to get the additional columns needed for the CW upload

nutrient_data_crosswalk = merge(age_splitted_data,inputdata1, by = c( "nid" ,"underlying_nid","seq","location_id","sex_id", "year_id",
                                                                    "original_age_start", "original_age_end"), all.x = TRUE) 


nutrient_data_crosswalk$measure_id = 18
nutrient_data_crosswalk$measure = "proportion"
setnames(nutrient_data_crosswalk, "mean", "val")

## calculate the variance from SE and sample size 

nutrient_data_crosswalk$variance = nutrient_data_crosswalk$standard_error^2

## assign sex_plitted 1 for mexico_1998 survey. It was confirmed this data source is sex specific

nutrient_data_crosswalk =nutrient_data_crosswalk[, sex_splitted := ifelse(is.na(sex_splitted), 1, sex_splitted)]

## For non age_splitted and sex splitted keep the seq values used in the bundle version and assign crosswalk_parent_seq as NULL
## for age_splitted and sex splitted set crosswalk_parent_seq and set seq as NULL

data_all =nutrient_data_crosswalk[, crosswalk_parent_seq := ifelse(sex_splitted == 0  &  was_age_splitted == 0, NA, seq)]
data_all =data_all[, seq := ifelse(sex_splitted == 0 & was_age_splitted == 0, seq, NA)]

data_all = data_all[!is.na(data_all$location_name),]
data_all = data_all[val > 1, val:=1]

# outlier datapoints which are confirmed error.

data_all = data_all[, is_outlier := ifelse(location_id %in% c(187,108), 1, 0)] 
data_all = data_all[, is_outlier := ifelse(location_id %in% c(214) & year_id ==2001, 1, is_outlier)] 


openxlsx::write.xlsx(data_all, "FILEPATH", sheetName = "extraction")

