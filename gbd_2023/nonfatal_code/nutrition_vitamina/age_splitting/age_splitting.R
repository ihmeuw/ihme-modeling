
## age splitting vitamin A deficiency prevalence


## sources 


source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")
library(data.table)

## age splitting package


library(reticulate)
reticulate::use_python("FILEPATH/python")
splitter <- import("pydisagg.ihme.splitter")


## Load sex splitted data 

inputdata = fread("FILEPATH/file.csv")
inputdata1 = as.data.table(copy(inputdata))

##
inputdata1$sex_id[inputdata1$sex =="Male"] <- 1
inputdata1$sex_id[inputdata1$sex =="Female"] <- 2

ages = get_age_metadata(release_id = 16)


data_to_split = inputdata1[age_group_id ==22,]

## assign the sub-national locations to admin0 because we do not have age pattern for subnationals

data_to_split$location_id[data_to_split$location_id == 4749] <- 95
data_to_split$location_id[data_to_split$location_id == 4649] <- 130

## Check the age start is always smaller than age_end

data_to_split = data_to_split[original_age_start < original_age_end,]
age_start_equal_age_end = inputdata1[inputdata1$original_age_start == inputdata1$original_age_end, ]
data_to_split$sex_id[data_to_split$sex=="Male"] <- 1
data_to_split$sex_id[data_to_split$sex=="Female"] <- 2
data_to_split = data_to_split[standard_error > 0,]
data_to_split$year_id = as.integer(data_to_split$year_id)

## load the age pattern data 

age_pattern =  fread("FILEPATH/agepattern.csv")  

## merge the age pattern with age meta-data 

age_pattern = merge(age_pattern, ages , c("age_group_id"), all.x = TRUE)

age_pattern = age_pattern[, year_id := NULL]

## get population 

pop <- get_population(location_id = 'all', year_id= c(min(inputdata1$original_year_start): max(inputdata1$original_year_end)),
                    sex_id='all', age_group_id = 'all', with_ui=TRUE, release_id=16)


## config 

data_config <- splitter$AgeDataConfig(
  index=c("underlying_nid","nid", "seq", "location_id", "year_id", "sex_id"),
  age_lwr="original_age_start",
  age_upr="original_age_end",
  val="mean",
  val_sd="standard_error"
)

draw_cols <- grep("^draw_", names(age_pattern), value = TRUE)

pattern_config <- splitter$AgePatternConfig(
  by=list("sex_id", "location_id"),
  age_key="age_group_id",
  age_lwr="age_group_years_start",
  age_upr="age_group_years_end",
  draws=draw_cols
  #val="mean_draw",
  #val_sd="var_draw"
)


pop_config <- splitter$AgePopulationConfig(
  index=c("age_group_id", "location_id", "year_id", "sex_id"),
  val="population"
)


##

age_splitter <- splitter$AgeSplitter(
  data=data_config, pattern=pattern_config, population=pop_config
)


##Model can be "rate" or "logodds"
#Output type should stay "rate" (for now)
result <- age_splitter$split(
  data=data_to_split,
  pattern=age_pattern,
  population=pop,
  model="rate",
  output_type="rate"
)

### Post_age splitting processing 

#subset the columns needed for crosswalk upload

setDT(result)
result1 = result[, .(nid, underlying_nid, seq, location_id, year_id, sex_id, age_group_id,
                     original_age_start, original_age_end, mean, standard_error, age_split_result, 
                     age_split_result_se)]

##

setnames(result1, c("mean", "standard_error", "age_split_result", "age_split_result_se"),
 c("original_mean", "original_standard_error","mean", "standard_error"))
result1$was_age_splitted = 1

### Append with data that did not need age splitting: Need some formatting to append the data  

age_specific_data = inputdata1[age_group_id !=22,]

age_specific_data = age_specific_data[,.(nid, underlying_nid, seq, location_id, year_id, sex_id,
                                     age_group_id,original_age_start, original_age_end, mean, standard_error)]

age_specific_data$original_mean = age_specific_data$mean
age_specific_data$original_standard_error = age_specific_data$standard_error

age_specific_data$was_age_splitted = 0 


## age_splitted_all

age_splitted_data = rbind(result1, age_specific_data)   


## subset columns needed for crosswalk upload

inputdata1$bundle_id = "bundle_id"
inputdata1 = inputdata1[, c("location_id", "underlying_nid", "nid", "field_citation_value", "source_type", "location_name", "sex",
  "sex_issue", "year_start", "year_end", "year_issue", "sample_size", "representative_name" ,"urbanicity_type", "extractor",
  "bundle_id" , "seq", "is_outlier", "original_year_start", "original_year_end",
  "original_age_end", "original_age_start","underlying_field_citation_value", "input_type_id", "sex_splitted", "year_id", "sex_id")]


inputdata1$year_id = as.integer(inputdata1$year_id)

## merge the age_splitted data with the original dataset to get the additional columns needed for upload

data_crosswalk = merge(age_splitted_data,inputdata1, by = c( "nid" ,"underlying_nid","seq","location_id","sex_id", "year_id",
                                                                    "original_age_start", "original_age_end"), all.x = TRUE) 


data_crosswalk$measure_id = 18
data_crosswalk$measure = "proportion"

setnames(data_crosswalk, "mean", "val")

## calculate the variance from SE and sample size 

data_crosswalk$variance = data_crosswalk$standard_error^2
data_all =data_crosswalk[, crosswalk_parent_seq := ifelse(sex_splitted == 0  &  was_age_splitted == 0, NA, seq)]
data_all =data_all[, seq := ifelse(sex_splitted == 0 & was_age_splitted == 0, seq, NA)]
data_all = data_all[val < 0.98, ]

## flag the outliers, based on the location and year: small scale studies or estimates from new borns, or unrealstic prevalence estimates

data_all =data_all[, is_outlier:= ifelse(location_id == 101, 1, is_outlier)]  
data_all =data_all[, is_outlier:= ifelse(location_id == 193 & year_id == 2003, 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 97 & year_id %in% c(1995,1998,1994) & nid %in% c(43713), 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 82 & year_id == 2005, 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 141 & year_id == 1997, 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 142 & year_id %in% c(2002,1989), 1, is_outlier)]  
data_all =data_all[, is_outlier:= ifelse(location_id == 144 & year_id == 1997, 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 155 & year_id == 1985, 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 163 & year_id == 1990, 1, is_outlier)]  
data_all =data_all[, is_outlier:= ifelse(location_id == 163 & year_id == 1995, 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 165 & year_id %in% c(1997, 1990), 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 18 & year_id == 2007, 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 18 & year_id %in% c(1985,1988), 1, is_outlier)]
data_all =data_all[, is_outlier:= ifelse(location_id == 20 & year_id %in% c(2002, 1994), 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 196 & year_id %in% c(1997,1998,1999,2002), 1, is_outlier)]  
data_all =data_all[, is_outlier:= ifelse(location_id == 214 & year_id %in% c(1992, 1997, 2004), 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 216 & year_id %in% c(2000, 1979,1980), 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 130 & year_id %in% c(1994, 1998), 1, is_outlier)]
data_all =data_all[, is_outlier:= ifelse(location_id == 133 & year_id %in% c(1995, 1996, 1998), 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 38 & year_id %in% c(2001), 1, is_outlier)] 
data_all =data_all[, is_outlier:= ifelse(location_id == 26 & year_id %in% c(1998,2000), 1, is_outlier)]

### do more formattting 

data_all[, year_start := ifelse(is.na(year_start), year_id, year_start)]
data_all[, year_end := ifelse(is.na(year_end), year_id, year_end)]
data_all[, source_type := ifelse(is.na(source_type), "Survey - other/unknown", source_type)]
data_all[, is_outlier := ifelse(is.na(is_outlier), 0, is_outlier)]

# Updating sex based on sex_id
data_all[, sex := ifelse(sex_id == 1, "Male", "Female")]

# Adding or correcting location names based on location_id
data_all[, location_name := ifelse(location_id == 95, "United Kingdom", location_name)]
data_all[, location_name := ifelse(location_id == 130, "Mexico", location_name)]



openxlsx::write.xlsx(data_all, "/FILEPATH/file.xlsx", sheetName = "extraction")


