############################################################
### Author: USER
### Description: Mass outliering of GBS data which was causing issues with the model
### Date written: DATE

###########################################################


#2020 Iterative xwalk for step 2
rm(list=ls())
require(data.table)
require(openxlsx)
##working environment 
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  l_root <- "ADDRESS"
} else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  l_root <- "ADDRESS"
}

#set object

functions_dir <- "ADDRESS"
temp_dir <- paste0("ADDRESS")
date <- Sys.Date()
date <- gsub("-", "_", date)
decomp_step <- 'iterative'
bundle_id <- 278
xwalk_version_id_2020 <- 18629
gbd_round_id <- 7
path_to_data <- paste0("FILEPATH")
description <- paste('Crosswalk Version', xwalk_version_id_2020,  'with Outliers for Karnataka and HCUP')


#sourcing functions:
source(paste0(functions_dir, "get_crosswalk_version.R"))
source(paste0(functions_dir, "save_bulk_outlier.R"))

#Loading in Data from 2020 best xwalk

xwalk_2020 <- get_crosswalk_version(xwalk_version_id_2020)

#NIDs for areas we wist to outlier are:
#284421 and 284422 for the HCUP 2003-2007 and 2008-2009 
#337129 for the Nazareth Hospital, Shillong, JSS Hospital, Mysore, ... India Hospital Inpatient Data 2014-2017
#354896 for St. John's Medical College Hospital (India). India - Bangalore St. John's Medical College Hospital Inpatient Data 2017

to_be_outliered <- xwalk_2020[nid == 354896 | nid == 337129 | nid == 284422 | nid == 284421]
to_be_outliered <- to_be_outliered[,c("seq", "is_outlier")]
to_be_outliered[,is_outlier := 1]

write.xlsx(to_be_outliered, file = path_to_data, sheetName="extraction")

result <- save_bulk_outlier(crosswalk_version_id = xwalk_version_id_2020,
                            decomp_step = decomp_step,
                            filepath = pathtodata,
                            description = description,
                            gbd_round_id = gbd_round_id)

print(sprintf("Request ID: %s", result$request_id))
print(sprintf("New crosswalk Version ID with outliers: %s", result$crosswalk_version_id))