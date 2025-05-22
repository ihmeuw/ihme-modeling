## Vitamin A Data Cleaning Documentation

# Overview:
# This script is designed for cleaning and preparing the VMNIS (Vitamin and Mineral Nutrition Information System) data,
# along with non-VMNIS data, for analyses related to vitamin A deficiency. The script performs data loading, cleaning,
# subsetting, and merging, to create a comprehensive dataset. The script also standardizes column names for consistency.


# Set environment
rm(list = ls())  # Clear the workspace

library(tidyverse)
library(data.table)
library(openxlsx)
library(tools)
library(data.table)

# Source required functions from the IHME resources
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")

# Set the directory where VMNIS data is stored
data_dir <- "FILEPATH"

# Load and prepare VMNIS data for vitamin A (retinol)
file_name1 <- "file1.xlsx"
retinol <- read.xlsx(paste0(data_dir, file_name1))[, 2:46]  # Read and subset columns
retinol <- data.table(retinol)
retinol <- retinol[to_be_included == 1,]  # Include only relevant studies
setnames(retinol, "Underlying.NID.2023", "underlying_nid")  # Rename column for clarity
retinol <- retinol[,"Underlying.NID.2021" := NULL]

# fixing issues on underlying_nid. It has some non-numeric values

retinol$underlying_nid <- as.numeric(gsub("[^0-9]", "", retinol$underlying_nid))

# Load and prepare VMNIS data for retinol-binding protein (RBP)

file_name2 <- "file2.xlsx"
rbp <- read.xlsx(paste0(data_dir, file_name2))[, 2:46]
rbp <- data.table(rbp)
rbp <- rbp[to_be_included == 1,]  # Include only relevant studies
setnames(rbp, "Underlying.NID.2021", "underlying_nid")  # Rename column for clarity
rbp = rbp[, Underlying.NID.2021.1 := NULL]
# Combine retinol and RBP data
vad <- rbind(retinol, rbp)

# Drop unnecessary columns from the combined dataset
drop_columns <- c("WHO.region", "ISOCODE", "Survey.(Id)", "Representativeness.(Id)", "Start.month", "End.month", "Severity.Alias", "General.data.comments", "Status")
vad <- vad[, (drop_columns) := NULL]

# Standardize column names for the combined dataset
names1 <- c("indicator", "to_be_included", "location_name", "underlying_nid", "survey_title",
           "representativeness", "urbanicity_type", "year_id", "year_start", "year_end",
           "population", "sex", "age_unit", "age_start", "age_end", "sample_size", "indicator_unit",
            "mean_biomarker", "geometric_mean_biomarker", "median_biomarker", "SD_biomarker", "SE_biomarker",
             "L95CI_biomarker", "U95CI_biomarker", "Value.type", "Cut.off.value", "mean", "Sample.collection.method", 
             "biomarker_sample.type", "Method.of.analysis", "data_adjusted", "inidicator_comments", "Survey.Methodology", 
             "Survey.type", "reference")
setnames(vad, old = names(vad), new = names1)

# Add additional columns to the combined dataset

vad[, c("nid", "lower", "upper", "URL") := .(409630, NA, NA, NA)]

# Load and prepare non-VMNIS data
non_vmnis_file <- "non_WHO_VMNIS_Vitamin_A.csv"
non_vmnis_vad <- fread(paste0(data_dir, non_vmnis_file))

# Standardize column names for the non-VMNIS data

names2 <- c("nid", "indicator", "sex", "location_name", "representativeness", "urbanicity_type", 
"year_start", "year_end", "Survey.Methodology", "population", "age_start", "age_end", "age_unit",
 "sample_size", "Cut.off.value", "mean", "lower", "upper", "underlying_nid", "mean_biomarker",
  "geometric_mean_biomarker", "SD_biomarker", "SE_biomarker", "median_biomarker", "L95CI_biomarker", 
  "U95CI_biomarker", "reference", "inidicator_comments", "URL")

setnames(non_vmnis_vad, old = names(non_vmnis_vad), new = names2)

# Add additional variables to non-VMNIS data for column match with VMNIS data
non_vmnis_vad[, c("survey_title", "indicator_unit", "Value.type", "Sample.collection.method", 
               "biomarker_sample.type", "Method.of.analysis", "data_adjusted", "Survey.type") := NA]
non_vmnis_vad[, to_be_included := 1]
non_vmnis_vad[, year_id := floor((year_start + year_end) / 2), ]

# Combine the WHO VMNIS data with the non-VMNIS data

input_data <- rbind(vad, non_vmnis_vad)

##: take  input data and prep for epi uploader
input_data$seq <- NA
input_data$input_type <- NA
input_data$page_num <- NA
input_data$table_num <- NA
input_data$source_type <- "Survey - other/unknown"	
input_data$note_SR <- NA
input_data$bundle_id <- "bundle_id"
input_data$seq <- NA


names(input_data)[names(input_data)=="measure"] <- "mean"  # Prevalence of vitamin A deficiency 
input_data$seq <- NA
input_data$measure <- "proportion"
input_data$mean <- as.numeric(input_data$mean)/100   
input_data <- input_data[!is.na(mean),]

# location info

location_data <- get_location_metadata(location_set_id = 35, release_id=16)

#  Check the location name differences between input_data and location_data 


print(setdiff(input_data$location_name, location_data$location_name))


# explicit recodes to get location data to merge on

input_data$location_name[input_data$location_name == "United Kingdom of Great Britain and Northern Ireland"] <- "United Kingdom"
input_data$location_name[input_data$location_name == "United Kingdom of Great Britain and Northern"] <- "United Kingdom"
input_data$location_name[input_data$location_name == "Turkey"] <- "Türkiye"
input_data$location_name[input_data$location_name == "The former Yugoslav Republic of Macedonia"] <- "North Macedonia"
input_data$location_name[input_data$location_name == "Cote d'Ivoire" ] <- "Côte d'Ivoire"
# "West Bank and Gaza Strip" is coded as "Palestine"
input_data$location_name[input_data$location_name == "West Bank and Gaza Strip" ] <- "Palestine"

# Merge the input data with the location_data

input_data = merge(input_data, location_data, by = "location_name")

### identify the countries with no vitamin A data: 
##  These countries should be the priorites for future data extraction on vitamin A deficiency 

no_countries_with_noVAdata = paste0(setdiff(location_data[location_type == "admin0"]$location_name, input_data$location_name), collapse = ",")


## add place holder noring subnationals

input_data$smaller_site_unit <- NA
input_data$site_memo <- NA
input_data$input_type_id <- NA

##`````````````` sex

input_data$sex[input_data$sex == "Females"] <- "Female"
input_data$sex[input_data$sex == "Males"] <- "Male"
input_data$sex[input_data$sex == "All"] <- "Both"
input_data$sex_issue <- 0

##```````````````````` year

input_data$year_issue  <- 0

# ``````````````````````age 
input_data$age_issue <- 0
input_data$age_issue[input_data$age_start == "" | input_data$age_end == ""] <- 1
input_data$age_issue[input_data$age_start == NA | input_data$age_end == NA ] <- 1

input_data$note_SR[input_data$age_start == "" | input_data$age_end == ""] <- "assumed age start or end"
input_data$note_SR[input_data$age_start == NA | input_data$age_end == NA] <- "assumed age start or end"

input_data$age_demographer <- 0

## Assign values for missed age_start and age_end

# age_start: assign 15 as age start for women in reproductive age 

input_data$age_start[input_data$population %in% c("Pregnant women", "Lactating women (LW)","Women of reproductive age ", "Non-pregnant women (NPW)") & is.na(input_data$age_start) & input_data$age_unit == "Year"] <- 15
input_data$age_start[input_data$population %in% c("Non-pregnant, non-lactating women (NPNLW)") & is.na(input_data$age_start) & input_data$age_unit == "Year"] <- 15

# age_end: assign 49 as age end for women in reproductive age 

input_data$age_end[input_data$population %in% c("Pregnant women", "Lactating women (LW)","Women of reproductive age ") & is.na(input_data$age_end) & input_data$age_unit == "Year"] <- 49
input_data$age_end[input_data$population %in% c("Adults ", "Men")  & is.na(input_data$age_end) & input_data$age_unit == "Year" & input_data$underlying_nid == 8618] <-  59
input_data$age_end[input_data$population %in% c("Elderly")  & is.na(input_data$age_end) & input_data$age_unit == "Year"] <- 99
input_data$age_end[input_data$population %in% c("All")  & is.na(input_data$age_end) & input_data$age_unit == "Year"] <- 99

input_data$age_end[input_data$population %in% c("Non-pregnant, non-lactating women (NPNLW)")  & is.na(input_data$age_end) & input_data$age_unit == "Year"] <- 49
input_data$age_end[input_data$population %in% c("Non-pregnant women (NPW)")  & is.na(input_data$age_end) & input_data$age_unit == "Year"] <- 49


## assign  age end with a value 0 to 99
input_data$age_end[input_data$age_end==0 & input_data$age_start == 65] <- 99

## fixing age issue for Guyana and Kenya 

## the age_start is 0 for infants in Guyana
input_data$age_start[is.na(input_data$age_start) & input_data$age_unit == "Month" & input_data$location_name == "Guyana"] <- 0

## assumed the age_start is 18 for adult males in Kenya

input_data$age_start[is.na(input_data$age_start) & input_data$age_unit == "Year" & input_data$location_name =="Kenya"] <- 18

## remove any blanks in the indicator of interest (retinol < 0.70)
input_data$measure <- "proportion"
input_data$standard_error <- NA
input_data$effective_sample_size <- NA
input_data$cases <- NA
input_data <- subset(input_data, input_data$sample_size != "") # remove any row with sample size to be null
input_data$unit_type <- "Person"
input_data$unit_value_as_published <- 1	
input_data$measure_issue <- 0
input_data$uncertainty_type <- NA
input_data$uncertainty_type_value <- NA

# geography data
input_data$representative_name <- "Unknown" # if it is not reported, we assume it is unknown
input_data$urbanicity_type <- NA
input_data$urbanicity_type[grepl("Rural", input_data$Administrative.Level, fixed = TRUE)] <- "Rural"
input_data$urbanicity_type[grepl("Urban", input_data$Administrative.Level, fixed = TRUE)] <- "Urban"
input_data$urbanicity_type[is.na(input_data$urbanicity_type)] <- "Unknown"

# recall
input_data$recall_type <- "Point"
input_data$sampling_type  <- NA
input_data$cases <- NA
input_data$case_definition  <- input_data$Cut.off.value
input_data$case_diagnostics  <- NA
input_data$note_modeler  <- NA
input_data$extractor  <- "nid 409630 copied from WHO VMNIS data and other nids are extracted"
input_data$is_outlier  <- 0
input_data$cv_subnational <- NA
input_data$recall_type_value <- NA
input_data$design_effect <- NA

#

input_data$field_citation_value <- ifelse(input_data$nid == 409630, "WHO Global Micronutrients Database", input_data$reference)
input_data$response_rate <- NA

# add the correct field_citation_value for non_vminis data 


 if(input_data$nid !=  409630) {
   input_data$field_citation_value =  input_data$survey_title
 }

#remove any w/ missing Ref.ID

input_data <- subset(input_data, !is.na(input_data$Ref.ID))
input_data$underlying_field_citation_value <- input_data$survey_title
input_data$response_rate <- NA

## Fixing age units 

for (x in 1:nrow(input_data)) {
  if (input_data$age_unit[x] %in% c("Month",  "months")) {
    if (input_data$age_start[x] > 12) {
      input_data$age_start[x] <- ceiling(input_data$age_start[x] / 12)
      input_data$age_end[x] <- ceiling(input_data$age_end[x] / 12)
      input_data$age_unit[x] <- "Years"
      
    } else {
      input_data$age_start[x] <- input_data$age_start[x] / 12
      input_data$age_end[x] <- input_data$age_end[x] / 12
      input_data$age_unit[x] <- "Years"
      
    }
  }
}

# keep preg women, preg/lactating, lactating women, mark as 1 cv_pregnant

input_data$cv_pregnant <- NA
input_data$cv_pregnant[input_data$population ==  "Pregnant women"   | input_data$population == "Lactating women (LW)"] <- 1
input_data$group_review <- NA
input_data$specificity <- NA
input_data$specificity[input_data$population == "Non-pregnant women (NPW)"  | input_data$population ==  "Non-pregnant, non-lactating women (NPNLW)"] <- 1
input_data$group <- NA
input_data$group[input_data$Population.Name == "Non-pregnant women (NPW)"  | input_data$Population.Name ==  "Non-pregnant, non-lactating women (NPNLW)"] <- 1
input_data$group_review[input_data$population == "Non-pregnant women (NPW)" | input_data$Population.Name == "Non-pregnant, non-lactating women (NPNLW)"] <- 1

input_data <- as.data.frame(input_data)

setDT(input_data)



## subset the data for vitamin A deficiency only 

table(input_data$Value.type)
input_data <- as.data.table(input_data)

setnames(input_data, "Value.type", "value_type")

input_data1 <- as.data.table(input_data)
input_data1$underlying_field_citation_value <- NA

vars_to_keep <- c("seq", "nid",	"underlying_nid",	"input_type",	"page_num",	"table_num",	"source_type",
                  "location_id",	"location_name",	"site_memo",	"sex",	"sex_issue",	"year_start", 	"year_end",	"year_issue",
                  "age_start",	"age_end", "age_issue",	"age_demographer",	"measure",	"mean",	"lower",	"upper",	"standard_error",
                  "effective_sample_size",	"cases",	"sample_size",	"unit_type",	"unit_value_as_published",	"measure_issue",
                  "uncertainty_type",	"uncertainty_type_value",	"representative_name",	"urbanicity_type",	"recall_type",	"sampling_type",	"case_definition",
                  "case_diagnostics",	"note_modeler",	"extractor",	"is_outlier",	"cv_subnational",	"recall_type_value",	"design_effect",	"field_citation_value",
                  "underlying_field_citation_value", "specificity", "group_review", "group", "smaller_site_unit", "cv_pregnant", "response_rate")


input_data1 <- input_data1 %>% select(all_of(vars_to_keep))


input_data1 = input_data1[, c("design_effect", "response_rate"):= NULL]
 

## calculate the variance using the mean 

input_data1 = input_data1[, variance := (sqrt(mean*(1-mean)/sample_size))^2 ]
input_data1 = input_data1[, standard_error := (sqrt(mean*(1-mean)/sample_size))]



input_data1 = input_data1[!is.na(sample_size),] # remove any row with sample size or variance to be null

## 

setnames(input_data1, "mean", "val") ## mean is not allowed in STGPR 

### Get the old bundle data(GBD 2021 and GBD 2019 data) and keep data that is relevant 

bundle_df = fread(paste0(data_dir, "GBD2021_bundle_vitamin_A_data.csv"))

## identify the countries that are not in the current vmnis data


gbd2021_data_to_add_gbd2023 = bundle_df[location_id %in% c(97, 181, 6,10, 11, 17,18,20, 25, 26,36,  38, 39,
                                                          53, 62, 71, 81, 82,85,86,92,98,101,102,121, 123, 125,
                                                           133,152,155,156,157,170,177,179,183,201,209,211,212,215,4649,4749,122,
                                                            123, 125,128, 139, 144,154,161,163, 164, 165, 179, 182,195,205,207,216),]

                    
## add location_year_id missed in new who VMNIS data. Whenever there is an overlap of data, we choose the new WHO VMNIS data 

data_toadd <- bundle_df %>%
  filter(
    (location_id == 114 & year_id == 1999) |
      (location_id == 16 & year_id %in% c(1973)) |
      (location_id == 123 & year_id %in% c(1987, 1992, 1996, 1998, 2005)) |
      (location_id == 125 & year_id %in% c(1978, 2005)) |
      (location_id == 130 & year_id %in% c(1998)) |
      (location_id == 130 & nid %in% c(129886)) | # only subset the national estimates 
      (location_id == 135 & year_id %in% c(1971, 1978, 1982, 1983, 1986, 1989, 1992, 1994, 1995, 1996, 1997, 1998, 2000, 2007)) |
      (location_id == 141 & year_id %in% c(1996, 1997, 1999)) |
      (location_id == 142 & year_id %in% c(1989, 2002)) |
      (location_id == 148 & year_id == 2004) |
      (location_id == 189 & year_id %in% c(1991, 1992, 1995, 2003)) |
      (location_id == 191 & year_id == 1994) |
      (location_id == 196 & year_id %in% c(1962, 1965, 1991, 1995, 1997, 1998, 1999, 2002)) |
      (location_id == 198 & year_id == 2001) |
      (location_id == 202 & year_id %in% c(1992, 1996, 2004)) |
      (location_id == 214 & year_id %in% c(1987, 1992, 1997, 2004)) |
      (location_id == 163 & year_id %in% c(2002) & nid %in% c(43713) & effective_sample_size %in% c(3934)) |  ## drop studies that small scale/ We only keep studies at least a represent state
      (location_id == 122 & year_id %in% c(1994) & sample_size ==1232) # select the national estimates for Ecuador. We excluded small scale studies
  )

## national data points to add for mexico

mexico <- bundle_df %>%
  
  filter(location_id == 130 & year_id %in% c(1998) & sample_size %in% c(1709, 855))


data_toadd = rbind(data_toadd, mexico)


total_data_fromgbd2021 =rbind(gbd2021_data_to_add_gbd2023, data_toadd) 

total_data_fromgbd2021 <- total_data_fromgbd2021[, c("input_type_id","year_id","age_group_id","orig_year_start", "orig_year_end", "bundle_325_seq") := NULL]

## merge the new vmnis data with the old bundle data

input_data2 = rbind(input_data1, total_data_fromgbd2021)

input_data2 = unique(input_data2)

setnames(input_data2, "val", "mean")

input_data3 = input_data2

## fix some issues on the underlying_id 

input_data3$underlying_nid <- ifelse(input_data3$underlying_nid == "need_nid", NA, input_data3$underlying_nid)

## create age_group_id 

input_data3[age_start >= 0 & age_end <= 0.01917808,age_group_id:=2]        
input_data3[age_start >= 0.01917808 & age_end <= 0.07671233, age_group_id:=3]
input_data3[age_start >= 5 & age_end <= 10, age_group_id:=6]                  
input_data3[age_start >= 10 & age_end <= 15, age_group_id:=7]
input_data3[age_start >= 15 & age_end <= 20, age_group_id:=8]                
input_data3[age_start >= 20 & age_end <= 25, age_group_id:=9]               
input_data3[age_start >= 25 & age_end <= 30, age_group_id:=10]               
input_data3[age_start >= 30 & age_end <= 35, age_group_id:=11]               
input_data3[age_start >= 35 & age_end <= 40, age_group_id:=12]                
input_data3[age_start >= 40 & age_end <= 45, age_group_id:=13]              
input_data3[age_start >= 45 & age_end <= 50, age_group_id:=14]                
input_data3[age_start >= 50 & age_end <= 55, age_group_id:=15]              
input_data3[age_start >= 55 & age_end <= 60, age_group_id:=16]               
input_data3[age_start >= 60 & age_end <= 65, age_group_id:=17]              
input_data3[age_start >= 65 & age_end <= 70, age_group_id:=18]               
input_data3[age_start >= 70 & age_end <= 75, age_group_id:=19]             
input_data3[age_start >= 75 & age_end <= 80, age_group_id:=20]              
input_data3[age_start >= 80 & age_end <= 85, age_group_id:=30]               
input_data3[age_start >= 85 & age_end <= 90, age_group_id:=31]               
input_data3[age_start >= 90 & age_end <= 95, age_group_id:=32]              
input_data3[age_start >= 2 & age_end <= 5, age_group_id:=34]                 
input_data3[age_start >= 95 & age_end <= 125, age_group_id:=235]            
input_data3[age_start >= 1 & age_end <= 2, age_group_id:=238]              
input_data3[age_start >= 0.07671233 & age_end <= 0.5, age_group_id:=388]     
input_data3[age_start >= 0.5 & age_end <= 1, age_group_id:=389]   

## #mark the ones that need splitting

input_data3[is.na(age_group_id), age_group_id:=22]
input_data3 = input_data3[, year_id := floor((year_start + year_end)/2)]

setnames(input_data3, "year_end", "original_year_end")
setnames(input_data3, "year_start", "original_year_start")

## keep the original age_start and age_end

input_data3$original_age_start = input_data3$age_start
input_data3$original_age_end = input_data3$age_end 

  
input_data3$seq = 1:nrow(input_data3)

## More formatting to pass validation 

input_data3$sex[input_data3$sex == "both"] <- "Both" 
input_data3$sex[input_data3$sex == "female"] <- "Female" 

## fixing issue on underlying_nid

input_data3$underlying_nid[input_data3$underlying_nid ==  409208] <- NA
input_data3$underlying_nid = as.integer(input_data3$underlying_nid)

## rename
setnames(input_data3, "mean", "val")

## Merge with ages meta-data 


ages = get_age_metadata(release_id = 16)

ages = ages[,.(age_group_years_start, age_group_years_end, age_group_id)]

inputdata4 = merge(input_data3, ages, by = "age_group_id", all.x = TRUE)

## match the year_start and year_end with the age_meta data.

inputdata4[!is.na(age_group_years_start), age_start:= age_group_years_start]
inputdata4[!is.na(age_group_years_end), age_end:= age_group_years_end]




write.xlsx(inputdata4, "FILEPATH/file.xlsx", sheetName = "extraction")



