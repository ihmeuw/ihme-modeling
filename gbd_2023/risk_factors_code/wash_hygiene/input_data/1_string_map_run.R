
## Purpose: extract out toilet type and water source strings from new WaSH data sources
#######################################################################################

# source functions; includes the following:
  # str_map_check()
  # create_strings()
source("FILEPATH/string_map_functions.R")

## settings
limited_use <- T # T if sources are in limited use directory, F otherwise
gbd_round <- "2024" # current GBD year

## set directories
if (limited_use == T) {
  in_dir <- paste0("FILEPATH")
} else {
  in_dir <- paste0("FILEPATH")
}
out_dir <- paste0("FILEPATH")

## string mapping spreadsheets
w_strings <- fread("FILEPATH/w_source_defined_by_nid.csv") # water string mapping
w_strings[w_strings == ""] <- NA # replace empty strings with NA
setnames(w_strings, "iso3", "ihme_loc_id")

s_strings <- fread("FILEPATH/t_type_defined_by_nid.csv") # sanitation string mapping
s_strings[s_strings == ""] <- NA # replace empty strings with NA
setnames(s_strings, "iso3", "ihme_loc_id")

ec_strings <- fread("FILEPATH/t_emptied_contents_defined_by_nid.csv") # emptied_contents string mapping
ec_strings[ec_strings == ""] <- NA # replace empty strings with NA
setnames(ec_strings, "iso3", "ihme_loc_id")

ee_strings <- fread("FILEPATH/t_ever_emptied_defined_by_nid.csv") # ever_emptied string mapping
ee_strings[ee_strings == ""] <- NA # replace empty strings with NA
setnames(ee_strings, "iso3", "ihme_loc_id")

# every existing unique combination of NID-location-year in w_strings & s_strings
# this gets used in the str_map_check() function
w_check <- unique(w_strings[, paste(nid, ihme_loc_id, year_start, sep = "_")])
s_check<- unique((s_strings[, paste(nid, ihme_loc_id, year_start, sep = "_")]))
ec_check<- unique((ec_strings[, paste(nid, ihme_loc_id, year_start, sep = "_")]))
ee_check<- unique((ee_strings[, paste(nid, ihme_loc_id, year_start, sep = "_")]))

## create file of strings from new data sources that need to be added to w_strings and s_strings

types <- c("w_source_drink", "t_type", "t_ever_emptied","t_emptied_contents")
# first, list out all of the data sources that are new - specific to type
for (type in types) {
  to_string_map <- rbindlist(lapply(list.files(in_dir), str_map_check, dir = in_dir, type = type))
  
  if (nrow(to_string_map) > 0) {
    # Create and save files only if there are new strings to be mapped
    if (limited_use == T) {
      file_name <- paste0(type, "_lim_use_", format(Sys.Date(), "%m%d%y"), ".csv")
    } else if (limited_use == F) {
      file_name <- paste0(type, "_", format(Sys.Date(), "%m%d%y"), ".csv")
    }
    dt <- rbindlist(lapply(to_string_map$file, create_strings, type = type, dir = in_dir))
    write.csv(dt, file.path(out_dir, file_name), row.names = F)
  }
}