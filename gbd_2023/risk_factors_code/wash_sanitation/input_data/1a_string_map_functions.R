
## Purpose: functions for extracting out toilet type and water source strings from new WaSH data sources
## Notes: these functions are used in the string_map_run.R script
########################################################################################################

library(data.table)
library(magrittr)
"%ni%" <- Negate("%in%")

# check if data source is already in the string mapping spreadsheet; if not, need to add to the spreadsheet
  # file: filepath to data source
  # dir: directory that data source is in
str_map_check <- function(file, dir, type) {
  print(file)
  fname <- copy(file)

  ## read in extraction file
  dt <- fread(file.path(dir, file), encoding = "Latin-1")
  dt[dt == ""] <- NA # replace empty strings with NA

  # NID-location-year of current data source
  current_source <- dt[, unique(paste(nid, ihme_loc_id, year_start, sep = "_"))]

  # Check for each type of string
  types <- c("w_source_drink", "t_type", "t_ever_emptied", "t_emptied_contents")
  checks <- list(w_source_drink = w_check, t_type = s_check, t_ever_emptied = ee_check, t_emptied_contents = ec_check)
  
  if (current_source %ni% checks[[type]] & type %in% names(dt)) {
    needs_string_mapping <- data.table(file = fname)
  }

  if (exists("needs_string_mapping")) return(needs_string_mapping)
}

# create data.table with every t_type/w_source_drink string in the source
  # file: filepath to data source
  # type: either t_type or w_source_drink
  # dir: directory that data source is in
create_strings <- function(file, type, dir) {
  print(file)
  
  dt <- fread(file.path(dir, file), encoding = "Latin-1")
  dt[dt == ""] <- NA # replace empty strings with NA
  
  if (type == "t_type") {
    if ("t_type" %in% names(dt)) {
      # change to lowercase (s_strings is entirely lowercase)
      dt[, t_type := tolower(t_type)]
      
      # if source has both sewage and t_type, need to concatenate
      if ("sewage" %in% names(dt)) {
        setnames(dt, "t_type", "t_type_orig") # preserve a copy of the original t_type variable
        dt[, sewage := tolower(sewage)] # change to lowercase
        dt[!is.na(sewage) & !is.na(t_type_orig), t_type := paste(t_type_orig, sewage)] # concatenate
        dt[!is.na(sewage) & is.na(t_type_orig), t_type := sewage] # use sewage variable if t_type is missing
        dt[is.na(t_type), t_type := t_type_orig] # use original t_type if there's nothing to concatenate
      }
      
      dt <- dt[!is.na(t_type), .(nid, ihme_loc_id, year_start, t_type)] %>% unique
    }
  } else if (type == "w_source_drink") {
    if ("w_source_drink" %in% names(dt)) {
      # change to lowercase (w_strings is entirely lowercase)
      dt[, w_source_drink := tolower(w_source_drink)]
      
      dt <- dt[!is.na(w_source_drink), .(nid, ihme_loc_id, year_start, w_source_drink)] %>% unique
    }
  } else if (type == "t_ever_emptied") {
    if ("t_ever_emptied" %in% names(dt)) {
      # change to lowercase (ee_strings is entirely lowercase)
      dt[, t_ever_emptied := tolower(t_ever_emptied)]
      
      dt <- dt[!is.na(t_ever_emptied), .(nid, ihme_loc_id, year_start, t_ever_emptied)] %>% unique
    }
  } else if (type == "t_emptied_contents") {
    if ("t_emptied_contents" %in% names(dt)) {
      # change to lowercase (ec_strings is entirely lowercase)
      dt[, t_emptied_contents := tolower(t_emptied_contents)]
      
      dt <- dt[!is.na(t_emptied_contents), .(nid, ihme_loc_id, year_start, t_emptied_contents)] %>% unique
    }
  }
  return(dt)
}