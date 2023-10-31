# update main extraction sheet with flunet data
pacman::p_load(data.table, openxlsx, lubridate, metafor, readxl, plyr, dplyr)
username <- Sys.info()[["user"]]

# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("/filepath", full.names = T), source))
source(paste0(h, "filepath/get_row_population.R"))
filepath <- "filepath"
message("Reading Data")
extraction_original <- read.xlsx(filepath)
extraction <- copy(as.data.table(extraction_original))
if(class(extraction$start_date) == "character"){
  extraction$start_date <- as.Date(extraction$start_date, tryFormats = c("%m/%d/%Y"))
  extraction$end_date <- as.Date(extraction$end_date, tryFormats = c("%m/%d/%Y"))
} else if(class(extraction$start_date) == "numeric"){
  extraction$start_date <- convertToDate(extraction$start_date)
  extraction$end_date <- convertToDate(extraction$end_date)
}

# read in all flunet data
all.files <- list.files(path = "/filepath/" ,pattern = ".xlsx", full.names = T)
temp <- lapply(all.files, read_excel, skip = 5)
flu_data <- as.data.table(rbindlist(temp))

hierarchy <- get_location_metadata(location_set_id = 35, gbd_round_id = 7, decomp_step = "iterative")
setnames(flu_data, c("Start date", "End date", "Received/ collected", "Year", "Total number of influenza positive viruses", "Country, area or territory"),
         c("start_date", "end_date", "received", "year_id", "cases", "location_name"))
# Merge on location, subset to nationals only to not deal with Georgia, Niger duplicates
flu_data <- merge(flu_data, hierarchy[nchar(ihme_loc_id) == 3,.(location_name, location_id, ihme_loc_id)], by = "location_name")
flu_data$start_date <- as.Date(flu_data$start_date)
flu_data$end_date <- as.Date(flu_data$end_date)

# Deal with messiness in the received/collected and the processed files.
# If Processed > received/collected, switch them
flu_data[Processed>received, `:=` (Processed = received, received = Processed)]
# Use received when Processed is missing or is fewer than cases
flu_data[(is.na(Processed) | Processed < cases), Processed := received]
# Leave these below rows in, even though they are implausible
# # Drop rows that have cases but are missing the processed
# flu_data <- flu_data[!is.na(Processed) | (is.na(Processed) & is.na(cases))]
# # Drop rows where cases > processed
# flu_data <- flu_data[Processed >= cases| (is.na(Processed) & is.na(cases))]

flu_data[, samples_tested := Processed]
flu_data[, year_start := year(start_date)]
flu_data[, year_end := year(end_date)]

flu_data <- flu_data[,intersect(names(extraction), names(flu_data)), with = F]
flu_data <- data.table(flu_data, extractor = "name",
                       nid = 475714,
                       surveillance_name = "WHO FluNet", source_type = "surveillance",
                       link = "https://apps.who.int/flumart/Default?ReportNo=12",
                       age_start = 0,
                       age_end = 99,
                       sex = "both",
                       sex_id = 3,
                       parent_cause = "lri",
                       cause_name = "flu",
                       measure_type = "incidence",
                       case_status = "confirmed",
                       is_outlier = 0,
                       age_demographer = 1,
                       notes = "flunet last updated 5/24/21")

# add sample size
message("Adding Population as Sample Size")
# Get population separately for all-age and age-specific rows, since get_row_population is slow
# For all age, just merge on all-age population
pop <- get_population(age_group_id = 22,
                      sex_id = c(1,2,3),
                      location_id = unique(flu_data$location_id),
                      year_id = unique(flu_data$year_id),
                      gbd_round_id = 7,
                      decomp_step = "iterative")
pop$run_id <- NULL
flu_data <- merge(flu_data, pop, by.x = c("location_id", "year_start", "sex_id"), by.y = c("location_id", "year_id", "sex_id"))
# Only actually fill this population field to sample_size for all-age, both-sex rows
flu_data[age_start == 0 & age_end >= 99, sample_size := population]
if(any(is.na(flu_data$sample_size))) message("need to add get_row_population function!")
flu_data$population <- NULL

extraction_updateflu <- rbind(extraction[surveillance_name != "WHO FluNet"], flu_data, fill = T)
write.xlsx(extraction_updateflu, "/filepath/2021_06_15_all_extractions.xlsx", sheetName = "extraction")
