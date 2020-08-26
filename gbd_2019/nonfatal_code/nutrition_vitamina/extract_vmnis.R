## AUTHOR
## GBD 2017
## Purpose: extract all VMNIS data from spreadsheets on disk, delete current bundle and reupload


# Set environment
rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "FILEPATH"
} else {
  jpath <- "FILEPATH"
}


# load libraries
library(openxlsx)
library(tools)


# set directories
data_dir <- paste0(jpath, "FILEPATH")
save_dir <- paste0(jpath, "FILEPATH")


## source functions
source(paste0(jpath, "FILEPATH/get_location_metadata.R"))
source(paste0(jpath, "FILEPATH/get_epi_data.R"))
source(paste0(jpath, "FILEPATH/upload_epi_data.R"))

############################################################################################
## pull current data, delete everything from the VMNIS


bundle <- get_epi_data(325, export = TRUE)
bundle <- subset(bundle, bundle$nid == 43713)
bundle <- bundle[,2]
write.xlsx(bundle, paste0(jpath, "FILEPATH/vitamin_a_delete.xlsx"), sheetName = "extraction")

# upload seq to delete 
upload_epi_data(325, paste0(jpath, "FILEPATH/vitamin_a_delete.xlsx"))


############################################################################################
## PREP DATA FOR UPLOAD
## step 1: read in xlsx, make one big file 
loc_files <- list.files(path = data_dir)
num <- length(loc_files)

input_data <- data.frame()

for (i in 1:num){
  # read in each file
  filename <- loc_files[i]
  print(filename)
  data <- read.xlsx(paste0(data_dir,filename), sheet = 2)
  input_data <- rbind(input_data, data)
}

## step 2: take combo input data and prep for epi uploader

input_data$seq <- NA
input_data$nid <- 43713 ## nid for the who vitamin and mineral nutrition information system 
input_data$underlying_nid	<- NA
input_data$input_type <- NA
input_data$page_num <- NA
input_data$table_num <- NA
input_data$source_type <- "Survey - other/unknown"	
input_data$note_SR <- NA
input_data$bundle_id <- NA
input_data$seq <- NA
names(input_data)[names(input_data)=="Prevalence.of.retinol.<0.70.µmol/L"] <- "mean"
input_data$mean <- as.numeric(input_data$mean)/100
input_data <- subset(input_data, input_data$mean != "")

# location info
location_data <- as.data.frame(get_location_metadata(location_set_id=9, gbd_round_id=5))
location_data <- location_data[,c(3,10)]
location_data <- as.data.table(location_data)

input_data <- as.data.table(input_data)
input_data$location_name <- toTitleCase(tolower(input_data$Country.Name))

#explicit recodes to get location data to merge on
input_data$location_name[input_data$location_name == "Bolivia (Plurinational States of)"] <- "Bolivia"
input_data$location_name[input_data$location_name == "Congo, the Democratic Republic of the"] <- "Democratic Republic of the Congo"
input_data$location_name[input_data$location_name == "Côte D'ivoire"] <- "Cote d'Ivoire"
input_data$location_name[input_data$location_name == "Gambia"] <- "The Gambia"
input_data$location_name[input_data$location_name == "Iran, Islamic Republic of"] <- "Iran"
input_data$location_name[input_data$location_name == "Korea, Republic of"] <- "South Korea"
input_data$location_name[input_data$location_name == "Lao People's Democratic Republic"] <- "Laos"
input_data$location_name[input_data$location_name == "Macedonia, the Former Yugoslav Republic of"] <- "Macedonia"
input_data$location_name[input_data$location_name == "Micronesia, Federated States of"] <- "Federated States of Micronesia"
input_data$location_name[input_data$location_name == "Serbia and Montenegro (Former)"] <- "Serbia"
input_data$location_name[input_data$location_name == "Tanzania, United Republic of"] <- "Tanzania"
input_data$location_name[input_data$location_name == "United Kingdom"] <- "England"
input_data$location_name[input_data$location_name == "Viet Nam"] <- "Vietnam"

## with all.x and without, the same number come out == not missing any data
input_data <- merge(input_data, location_data, by = "location_name")

input_data$smaller_site_unit <- NA
input_data$smaller_site_unit[grepl("Total", input_data$`Region/Sample`, fixed = TRUE)] <- 0
input_data$smaller_site_unit[grepl("region", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("area", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("zone", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("province", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("village", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("location", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("ethnic group", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("department", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("governorate", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("island", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("Island", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("district", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("site", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("state", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("hamlet", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("cluster", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("division", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("atoll", input_data$`Region/Sample`, fixed = TRUE)] <- 1
input_data$smaller_site_unit[grepl("city", input_data$`Region/Sample`, fixed = TRUE)] <- 1

input_data$smaller_site_unit[input_data$Administrative.Level != "National"] <- 1
# at this point, only national items haven't been assigned smaller site unit
input_data$smaller_site_unit[grepl("by age", input_data$`Region/Sample`, fixed = TRUE)] <- 0
# includes "by sex and age"
input_data$smaller_site_unit[grepl("by sex", input_data$`Region/Sample`, fixed = TRUE)] <- 0
## everything remaining is a national survey
input_data$smaller_site_unit[is.na(input_data$smaller_site_unit)] <- 0

input_data$site_memo <- NA


# sex
input_data$sex <- NA
input_data$sex[input_data$Sex == "Females"] <- "Female"
input_data$sex[input_data$Sex == "Males"] <- "Male"
input_data$sex[input_data$Sex == "Both Sexes"] <- "Both"
input_data$sex_issue <- 0

# year
names(input_data)[names(input_data)=="Start.Year"] <- "year_start"
names(input_data)[names(input_data)=="End.Year"] <- "year_end"
input_data$year_issue  <- 0

# age
names(input_data)[names(input_data)=="Start.Age"] <- "age_start"
names(input_data)[names(input_data)=="End.Age"] <- "age_end"
input_data$age_issue <- 0
input_data$age_issue[input_data$age_start == "" | input_data$age_end == ""] <- 1
input_data$note_SR[input_data$age_start == "" | input_data$age_end == ""] <- "assumed age start or end"
input_data$age_demographer <- 0
input_data <- subset(input_data, input_data$age_start != "")
input_data <- subset(input_data, input_data$age_end != "")

# remove any blanks in the indicator of interest (retinol < 0.70)
input_data$measure <- "prevalence"
input_data$lower <- NA
input_data$upper <- NA
input_data$standard_error <- NA
input_data$effective_sample_size <- NA
input_data$cases
names(input_data)[names(input_data)=="Sample.Size.(N)"] <- "sample_size"
input_data <- subset(input_data, input_data$sample_size != "")
input_data$unit_type <- "Person"
input_data$unit_value_as_published <- 1	
input_data$measure_issue <- 0
input_data$uncertainty_type <- NA
input_data$uncertainty_type_value <- NA

# geography data
input_data$representative_name <- "Unknown"
input_data$urbanicity_type <- NA
input_data$urbanicity_type[grepl("Rural", input_data$Administrative.Level, fixed = TRUE)] <- "Rural"
input_data$urbanicity_type[grepl("Urban", input_data$Administrative.Level, fixed = TRUE)] <- "Urban"
input_data$urbanicity_type[is.na(input_data$urbanicity_type)] <- "Unknown"

# recall
input_data$recall_type <- "Point"
input_data$sampling_type  <- NA
input_data$cases <- NA
input_data$case_definition  <- NA
input_data$case_diagnostics  <- NA
input_data$note_modeler  <- NA
input_data$extractor  <- "USERNAME"
input_data$is_outlier  <- 0
input_data$cv_subnational <- NA
input_data$recall_type_value <- NA
input_data$design_effect <- NA
input_data$field_citation_value <- NA
#remove any w/ missing Ref.ID
input_data <- subset(input_data, !is.na(input_data$Ref.ID))
input_data$underlying_field_citation_value <- input_data$Ref.ID	
input_data$response_rate <- NA

## group review agreement:
# keep preg women, preg/lactating, lactating women, mark as 1 cv_pregnant
# npw, npnlw group review out -- mark as 0, even if they're the only ones for the study 
input_data$cv_pregnant <- NA
input_data$cv_pregnant[input_data$Population.Name == "PW" | input_data$Population.Name == "LW" | input_data$Population.Name == "PW/LW"] <- 1
input_data$group_review <- NA
input_data$specificity <- NA
input_data$specificity[input_data$Population.Name == "NPW" | input_data$Population.Name == "NPNLW"] <- "non pregnant"
input_data$group <- NA
input_data$group[input_data$Population.Name == "NPW" | input_data$Population.Name == "NPNLW"] <- 1
input_data$group_review[input_data$Population.Name == "NPW" | input_data$Population.Name == "NPNLW"] <- 1

input_data <- as.data.frame(input_data)
vars_to_keep <- c("seq", "nid",	"underlying_nid",	"input_type",	"page_num",	"table_num",	"source_type",
                  "location_id",	"location_name",	"site_memo",	"sex",	"sex_issue",	"year_start", 	"year_end",	"year_issue",
                  "age_start",	"age_end", "age_issue",	"age_demographer",	"measure",	"mean",	"lower",	"upper",	"standard_error",
                  "effective_sample_size",	"cases",	"sample_size",	"unit_type",	"unit_value_as_published",	"measure_issue",	
                  "uncertainty_type",	"uncertainty_type_value",	"representative_name",	"urbanicity_type",	"recall_type",	"sampling_type",	"case_definition",
                  "case_diagnostics",	"note_modeler",	"extractor",	"is_outlier",	"cv_subnational",	"recall_type_value",	"design_effect",	"field_citation_value",	
                  "underlying_field_citation_value", "specificity", "group_review", "group", "smaller_site_unit", "cv_pregnant", "response_rate")
input_data <- input_data[vars_to_keep]

write.xlsx(input_data, paste0(save_dir, "vmnis_upload.xlsx"), sheetName = "extraction")

############################################################################################
## UPLOAD DATA
upload_epi_data(325, paste0(save_dir, "vmnis_upload.xlsx"))
