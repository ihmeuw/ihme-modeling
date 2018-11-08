#######################################################################################
#' Author: 
#' 3/6/18
#' Purpose: Calculate means, upper and lower from draws for each location and save as one csv
#'
#######################################################################################

library("ihme", lib.loc = "FILEPATH")
ihme::setup()

library(dplyr)
library(magrittr)
library(data.table)
library(openxlsx)


# Read in country-specific data -------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
out_dir   <- args[1]
acause    <- args[2]
bundle_id <- args[3]

# Read in syphilis data ---------------------------------------------------

file_list <- list.files(paste0(out_dir, acause, "/"), pattern = paste0(acause, "\\.csv$"))

# trim off unnecessary extra columns
fread_plus <- function(filepath) {
  print(filepath)
  dt <- fread(filepath)
  dt <- dt[, c("location_id", "location_name", "year_id", "sex", "draw_name", "duration"), with = FALSE]
} 

# combine all location-specific csvs into one
data <- rbindlist(parallel::mclapply(paste0(out_dir, acause, "/", file_list), fread_plus))

data[, remission := 1 / duration]

# calculate draw means, and standard deviation
mean <- data[, lapply(.SD, mean), by = c("location_id", "location_name", "year_id", "sex"), .SDcols = "remission"]
sd   <- data[, lapply(.SD, sd), by = c("location_id", "location_name", "year_id", "sex"), .SDcols = "remission"]

setnames(mean, "remission", "mean")
setnames(sd, "remission", "sd")

# caclulate the upper and lower confidence interval bounds
capitalize <- function(x) {
  sapply(x, function(x) { paste(toupper(substring(x, 1,1)), substring(x, 2), sep = "", collapse = " ") })
}

mean_sd <- left_join(mean, sd, by = c("location_id", "location_name", "year_id", "sex")) %>% 
  mutate(lower = mean - 1.96 * sd,
         upper = mean + 1.96 * sd,
         year_start = year_id,
         year_end = year_id,
         sex = capitalize(sex)) %>% 
  select(-sd, -year_id)

# create necessary cols we need
col_order <- function(data.table){
  dt <- copy(data.table)
  epi_order <- fread(paste0("FILEPATH/upload_order.csv"), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
  names_dt <- tolower(names(dt))
  setnames(dt, names(dt), names_dt)
  for (name in epi_order){
    if (!name %in% names(dt)){
      dt[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dt), tolower(epi_order))
  new_epi_order <- c(epi_order, extra_cols)
  setcolorder(dt, new_epi_order)
  return(dt)
}


final <- col_order(as.data.table(mean_sd))

final <- final %>% 
  mutate(bundle_id  = bundle_id,
         nid        = 338694,
         sex_issue  = 0,
         year_issue = 0,
         age_start  = 0,
         age_end    = 100,
         age_issue  = 1,
         measure    = "remission",
         unit_type  = "Person*year",
         unit_value_as_published = 1,
         measure_issue           = 0,
         uncertainty_type_value  = 95,
         recall_type             = "Not Set",
         representative_name     = "Representative for subnational location only",
         source_type             = "Unidentifiable",
         urbanicity_type         = "Mixed/both",
         note_modeler = paste0("Remission rates for ", acause, " calculated ", Sys.Date(), " from WHO 2005 assumptions and interpolation with HAQ"),
         is_outlier   = 0,
         extractor    = "USERNAME")

# grab only years dismod models in otherwise slows down model with unnecessary data
final <- filter(final, year_start %in% c(1990, 1995, 2000, 2005, 2010, 2017))

real_acause <- if (acause == "syphilis") {
  "std_syphilis"
} else if (acause == "gonorrhea") {
  "std_gonnorhea"
} else if (acause == "chlamydia") {
  "std_chlamydia"
} else {
  "std_tricho"
}


write.xlsx(final, "FILEPATH", sheetName = "extraction")


source_functions(upload_epi_data = T, get_epi_data = T)

delete_remission <- function(bundle, acause) {
  cat(paste0("Deleting remission data for ", acause, "\n"))
  date <- gsub("-", "_", Sys.Date())
  
  dt <- get_epi_data(bundle)
  dt <- dt[measure == "remission"]
  dt <- dt[, .(seq)]
  
  output_file <- "FILEPATH"
  
  write.xlsx(dt, output_file, sheetName = "extraction")
  upload_epi_data(bundle_id = bundle, filepath = output_file)
}
  
delete_remission(bundle = bundle_id, acause = real_acause)
upload_epi_data(bundle_id, "FILEPATH")





