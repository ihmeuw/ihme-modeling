##################################################################################################
## Purpose: Guillian Barre Syndrome Norway subregions:
## Date: 04/09/2020
## Created by: USERNAME
##################################################################################################

require(data.table)
require(openxlsx)

rm(list=ls())
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}

source("FILEPATH/get_bundle_date.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_bundle_version.R")

date <- gsub("-", "_", Sys.Date())

#Getting the bundle data from 2019
bundle_id <- 278
decomp_step <- 'step4'
gbd_round_id <- 6
bundle_data_278 <- get_bundle_data(bundle_id, decomp_step, gbd_round_id=gbd_round_id, sync = "F")

###### SUBSET to NORWAY stuff/

#Check to see if the location IDs and the location names match-up
new_subnats_from_name <- bundle_data_278[  location_name == "Aust-Agder" | 
                                             location_name == "Vest-Agder" |
                                             location_name == "Hedmark" |
                                             location_name == "Oppland" |
                                             location_name == "Finnmark" |
                                             location_name == "Troms" |
                                             location_name == "Telemark" |
                                             location_name == "Vestfold" |
                                             location_name == "Hordaland" |
                                             location_name == "Sogn og Fjordane" |
                                             location_name == "Akershus" |
                                             location_name == "Buskerud" |
                                             location_name == "Ostfold"
]

new_subnats_from_id <- bundle_data_278[location_id == 4918 |
                                         location_id == 4919 |
                                         location_id == 4912 |
                                         location_id == 4913 |
                                         location_id == 4928 |
                                         location_id == 4927 |
                                         location_id == 4917 |
                                         location_id == 4916 |
                                         location_id == 4921 |
                                         location_id == 4922 |
                                         location_id == 4911 |
                                         location_id == 4915 |
                                         location_id == 4914
]

stopifnot((sum(new_subnats_from_name != new_subnats_from_id, na.rm = T) == 0)) #Checking that the location id's and the subregions names refer to the same data.

#The number of subnats from id does not match the number of subnats from name
table(new_subnats_from_id$location_name)
table(new_subnats_from_name$location_name)
sum(new_subnats_from_id$location_name == "Ostfold")
#Location name not picking up Ostfold. Doesnt really matter as we are working from the subnats selected from ID

new_subnats_from_id[, seq := as.character(seq)] #Assigning the seq column to be character so that we can remove the seq values for the new subnationals

sub_nat_change <- function(old_location_id, new_location_id, new_location_name, data_frame){
  data_frame[location_id == old_location_id, location_name := new_location_name]
  data_frame[location_id == old_location_id, seq := ""]
  data_frame[location_id == old_location_id, ihme_loc_id := ""]
  data_frame[location_id == old_location_id, location_id := new_location_id]
  data_frame
}

new_subnats_from_id <- sub_nat_change(4918, 60133, "Agder", new_subnats_from_id) #Aust-Agder
new_subnats_from_id <- sub_nat_change(4919, 60133, "Agder", new_subnats_from_id) #Vest-Agder

new_subnats_from_id <- sub_nat_change(4912, 60135, "Innlandet", new_subnats_from_id) #Hedmark
new_subnats_from_id <- sub_nat_change(4913, 60135, "Innlandet", new_subnats_from_id) #Oppland

new_subnats_from_id <- sub_nat_change(4927, 60137, "Troms og Finnmark", new_subnats_from_id) #Troms
new_subnats_from_id <- sub_nat_change(4928, 60137, "Troms og Finnmark", new_subnats_from_id) #Finnmark

new_subnats_from_id <- sub_nat_change(4916, 60134, "Vestfold og Telemark", new_subnats_from_id) #Vestfold
new_subnats_from_id <- sub_nat_change(4917, 60134, "Vestfold og Telemark", new_subnats_from_id) #Telemark

new_subnats_from_id <- sub_nat_change(4921, 60132, "Vestland", new_subnats_from_id) #Hordaland
new_subnats_from_id <- sub_nat_change(4922, 60132, "Vestland", new_subnats_from_id) #Sogn og Fjordane

new_subnats_from_id <- sub_nat_change(4911, 60136, "Viken", new_subnats_from_id) #Akershus
new_subnats_from_id <- sub_nat_change(4914, 60136, "Viken", new_subnats_from_id) #Buskerud
new_subnats_from_id <- sub_nat_change(4915, 60136, "Viken", new_subnats_from_id) #Ostfold

sum(new_subnats_from_id$seq == "") == nrow(new_subnats_from_id) #Checking that we have corrected the right number of rows.

#Make sure so SE's are above 1. If there is an SE above 1, If os is above 1 turn to NULL.
sum(new_subnats_from_id$standard_error > 1, na.rm = T) #Standard errors should be less than 1 to allow for re-upload, otherwise the code will fail.

#Note: sync by bundle and upload this data.
decomp_step <- "step2"
gbd_round_id <- 7
path_to_data <- paste0("FILEPATH")
write.xlsx(new_subnats_from_id, file = path_to_data, sheetName="extraction")
result <- get_bundle_data(bundle_id = bundle_id, decomp_step = decomp_step, gbd_round_id = gbd_round_id, sync = T, export = T) 

upload_bundle_data(bundle_id = bundle_id, 
                   decomp_step = decomp_step, 
                   filepath = path_to_data, 
                   gbd_round_id = gbd_round_id)

result <- get_bundle_data(bundle_id = bundle_id, 
                          decomp_step = decomp_step, 
                          gbd_round_id = gbd_round_id, 
                          sync = F, 
                          export = T) 

#Checking that the new subnational data have been successfully added.
nrow(result[location_id == 60133 | location_id == 60135 | location_id == 60137 | location_id == 60134 | location_id == 60132 | location_id == 60136]) == nrow(new_subnats_from_id)

#save bundle version as everything we want in our version, include clinical is whatever we need, the creates the step 2 bundle version.
save_bundle_version(bundle_id = bundle_id, 
                    decomp_step = decomp_step, 
                    gbd_round_id = gbd_round_id, 
                    include_clinical = c("claims", "inpatient"))