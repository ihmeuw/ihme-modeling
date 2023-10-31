##################################################################################################
## Purpose: Motor Neuron Disease Initisation
## Date: 2020/12/09
## Created by: USERNAME
##################################################################################################
require(data.table)
require(openxlsx)
require(DBI)

rm(list = ls())
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}


shared_fun_dir <- "FILEPATH"

functs <- c("get_bundle_data", 
            "upload_bundle_data",
            "get_bundle_version",
            "save_bundle_version",
            "get_crosswalk_version",
            "save_crosswalk_version")
for (funct in functs){
  source(paste0(shared_fun_dir, funct, ".R"))
}

date <- gsub("-", "_", Sys.Date())
bundle_id <- 449
decomp_step <- "iterative"
path_to_data_empty <- paste0("FILEPATH", date, "_", decomp_step, "_",bundle_id, "empty_bundle_data", ".xlsx")
path_to_data_2019_bun <- paste0("FILEPATH", date, "_", decomp_step, "_",bundle_id, "2019_best_version_2020_norway_bundle_data", ".xlsx")
path_to_data_2019_xwalk <- paste0("FILEPATH", date, "_", decomp_step, "_",bundle_id, "2019_best_xwalk_2020_norway_xwalk_data", ".xlsx")

#Step 1: 
# Getting the current bundle data and saving it as a bundle version
mnd_iter_2020 <- get_bundle_data(bundle_id = bundle_id,
                                 decomp_step = "iterative",
                                 gbd_round_id = 7)

result <- save_bundle_version(bundle_id = bundle_id,
                              decomp_step = "iterative",
                              gbd_round_id = 7,
                              include_clinical = NULL)

#Step 2:
#Deleting the old bundle data
empty_dt <- data.table("seq" = mnd_iter_2020[, seq])
write.xlsx(empty_dt, path_to_data_empty, sheetName = "extraction")
result2 <- upload_bundle_data(bundle_id, decomp_step = "iterative", path_to_data_empty, gbd_round_id = 7)

#Checking bundle data is empty
hopefully_empty <- get_bundle_data(bundle_id = bundle_id,
                                   decomp_step = "iterative",
                                   gbd_round_id = 7)
nrow(hopefully_empty)
#Step 3:
#Got bundle version from Epivis 2019
crosswalk_version_id <- 12422
odbc <- ini::read.ini('FILEPATH')
con_def <- 'clinical_dbview'
get_bun_vers_from_XW <- function(XW_version){
  myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
                              host = odbc[[con_def]]$SERVER,
                              username = odbc[[con_def]]$USER,
                              password = odbc[[con_def]]$PASSWORD)
  
  df <- dbGetQuery(myconn, sprintf(paste0("SELECT * from crosswalk_version.crosswalk_version cv where cv.crosswalk_version_id = ", XW_version)))
  bun_version <- df$bundle_version_id
  dbDisconnect(myconn)
  return(bun_version)
}
read <- get_bun_vers_from_XW(crosswalk_version_id)
mnd_2019_best_bun <- get_bundle_version(read, fetch = "all")
norway <- mnd_2019_best_bun[  location_name == "Aust-Agder" | 
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

sub_nat_change <- function(old_location_id, new_location_id, new_location_name, data_frame, note_modeler_addition){
  data_frame[location_id == old_location_id, location_name := new_location_name]
  data_frame[location_id == old_location_id, ihme_loc_id := ""]
  data_frame[location_id == old_location_id, note_modeler := paste(note_modeler, "|", date, note_modeler_addition, as.character(new_location_id))]
  data_frame[location_id == old_location_id, location_id := new_location_id]
  data_frame
}

mnd_2019_new_loc <- copy(mnd_2019_best_bun)

mnd_2019_new_loc <- sub_nat_change(4918, 60133, "Agder", mnd_2019_new_loc, "Location changed from Aust-Agder") #Aust-Agder
mnd_2019_new_loc <- sub_nat_change(4919, 60133, "Agder", mnd_2019_new_loc, "Location changed from Vest-Agder") #Vest-Agder

mnd_2019_new_loc <- sub_nat_change(4912, 60135, "Innlandet", mnd_2019_new_loc, "Location changed from Hedmark") #Hedmark
mnd_2019_new_loc <- sub_nat_change(4913, 60135, "Innlandet", mnd_2019_new_loc, "Location changed from Oppland") #Oppland

mnd_2019_new_loc <- sub_nat_change(4927, 60137, "Troms og Finnmark", mnd_2019_new_loc, "Location changed from Troms") #Troms
mnd_2019_new_loc <- sub_nat_change(4928, 60137, "Troms og Finnmark", mnd_2019_new_loc, "Location changed from Finnmark") #Finnmark

mnd_2019_new_loc <- sub_nat_change(4916, 60134, "Vestfold og Telemark", mnd_2019_new_loc, "Location changed from Vestfold") #Vestfold
mnd_2019_new_loc <- sub_nat_change(4917, 60134, "Vestfold og Telemark", mnd_2019_new_loc, "Location changed from Telemark") #Telemark

mnd_2019_new_loc <- sub_nat_change(4921, 60132, "Vestland", mnd_2019_new_loc, "Location changed from Hordaland") #Hordaland
mnd_2019_new_loc <- sub_nat_change(4922, 60132, "Vestland", mnd_2019_new_loc, "Location changed from Sogn og Fjordane") #Sogn og Fjordane

mnd_2019_new_loc <- sub_nat_change(4911, 60136, "Viken", mnd_2019_new_loc, "Location changed from Akershus") #Akershus
mnd_2019_new_loc <- sub_nat_change(4914, 60136, "Viken", mnd_2019_new_loc, "Location changed from Buskerud") #Buskerud
mnd_2019_new_loc <- sub_nat_change(4915, 60136, "Viken", mnd_2019_new_loc, "Location changed from Ostfold") #Ostfold

mnd_2019_dup <- within(mnd_2019_new_loc, x <- paste(nid, location_name, age_start, age_end, year_start, year_end, sex, is_outlier, urbanicity_type, group, group_review, specificity, measure, unit_type, uncertainty_type_value))
mnd_2019_no_nor <- copy(mnd_2019_dup[!location_id %in% 60132:60137, ])
mnd_2019_nor <- copy(mnd_2019_dup[location_id %in% 60132:60137, ])


mnd_2019_nor_no_dup <- mnd_2019_nor[duplicated(mnd_2019_nor[, x]), ]

mnd_2019_recom <- rbind(mnd_2019_no_nor, mnd_2019_nor_no_dup)

mnd_2019_recom[, x := NULL]

write.xlsx(mnd_2019_recom, path_to_data_2019_bun, sheetName = "extraction")
result3 <- upload_bundle_data(bundle_id = bundle_id,
                              decomp_step = decomp_step,
                              filepath = path_to_data_2019_bun, 
                              gbd_round_id = 7)

result4 <- save_bundle_version(bundle_id = bundle_id,
                               decomp_step = decomp_step,
                               gbd_round_id = 7,
                               include_clinical = NULL)

#Step 4:
mnd_2019_best_xwalk <- data.table(get_crosswalk_version(crosswalk_version_id = crosswalk_version_id))

mnd_2019_xw_new_loc <- copy(mnd_2019_best_xwalk)

mnd_2019_xw_new_loc <- sub_nat_change(4918, 60133, "Agder", mnd_2019_xw_new_loc, "Location changed from Aust-Agder") #Aust-Agder
mnd_2019_xw_new_loc <- sub_nat_change(4919, 60133, "Agder", mnd_2019_xw_new_loc, "Location changed from Vest-Agder") #Vest-Agder

mnd_2019_xw_new_loc <- sub_nat_change(4912, 60135, "Innlandet", mnd_2019_xw_new_loc, "Location changed from Hedmark") #Hedmark
mnd_2019_xw_new_loc <- sub_nat_change(4913, 60135, "Innlandet", mnd_2019_xw_new_loc, "Location changed from Oppland") #Oppland

mnd_2019_xw_new_loc <- sub_nat_change(4927, 60137, "Troms og Finnmark", mnd_2019_xw_new_loc, "Location changed from Troms") #Troms
mnd_2019_xw_new_loc <- sub_nat_change(4928, 60137, "Troms og Finnmark", mnd_2019_xw_new_loc, "Location changed from Finnmark") #Finnmark

mnd_2019_xw_new_loc <- sub_nat_change(4916, 60134, "Vestfold og Telemark", mnd_2019_xw_new_loc, "Location changed from Vestfold") #Vestfold
mnd_2019_xw_new_loc <- sub_nat_change(4917, 60134, "Vestfold og Telemark", mnd_2019_xw_new_loc, "Location changed from Telemark") #Telemark

mnd_2019_xw_new_loc <- sub_nat_change(4921, 60132, "Vestland", mnd_2019_xw_new_loc, "Location changed from Hordaland") #Hordaland
mnd_2019_xw_new_loc <- sub_nat_change(4922, 60132, "Vestland", mnd_2019_xw_new_loc, "Location changed from Sogn og Fjordane") #Sogn og Fjordane

mnd_2019_xw_new_loc <- sub_nat_change(4911, 60136, "Viken", mnd_2019_xw_new_loc, "Location changed from Akershus") #Akershus
mnd_2019_xw_new_loc <- sub_nat_change(4914, 60136, "Viken", mnd_2019_xw_new_loc, "Location changed from Buskerud") #Buskerud
mnd_2019_xw_new_loc <- sub_nat_change(4915, 60136, "Viken", mnd_2019_xw_new_loc, "Location changed from Ostfold") #Ostfold

mnd_2019_xw_dup <- within(mnd_2019_xw_new_loc, x <- paste(nid, location_name, age_start, age_end, year_start, year_end, sex, is_outlier, urbanicity_type, group, group_review, specificity, measure, unit_type, uncertainty_type_value))
mnd_2019_xw_no_nor <- copy(mnd_2019_xw_dup[!location_id %in% 60132:60137, ])
mnd_2019_xw_nor <- copy(mnd_2019_xw_dup[location_id %in% 60132:60137, ])

mnd_2019_xw_nor_no_dup <- mnd_2019_xw_nor[seq %in% mnd_2019_recom[, seq], ]

mnd_2019_xw_recom <- rbind(mnd_2019_xw_no_nor, mnd_2019_xw_nor_no_dup)

mnd_2019_xw_recom[, seq := as.character(seq)]
mnd_2019_xw_recom[, seq := ""]
mnd_2019_xw_recom[, crosswalk_parent_seq := origin_seq]



write.xlsx(mnd_2019_xw_recom, path_to_data_2019_xwalk, sheetName = "extraction")

result5 <- save_crosswalk_version(bundle_version_id = 36272,
                                  data_filepath = path_to_data_2019_xwalk,
                                  description = "2019 best xwalk, norway location change for 2020 but no norway locations")
