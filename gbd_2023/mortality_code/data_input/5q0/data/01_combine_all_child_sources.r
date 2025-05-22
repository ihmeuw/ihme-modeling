###############################################################################################################
## Set up settings and import functions

library(argparse)
library(foreign)
library(grid)
library(lattice)
library(plyr)
library(haven)
library(magrittr)
library(tools)
library(data.table)
library(readstata13)
library(ini)
library(readr)
library(rjson)

if (Sys.info()[1] == 'Windows') {
  j <<- "FILEPATH"
  h <<- "FILEPATH"

  library(mortdb)
} else {
  j <<- "FILEPATH"
  h <<- Sys.getenv("HOME") #repo directory
  library(mortdb, lib.loc = "FILEPATH")
}

parser <- argparse::ArgumentParser()
parser$add_argument("--version_id", type = "integer", required = TRUE,
                    help = "5q0 data version id")
parser$add_argument("--ddm_version_id", type = "integer", required = TRUE,
                    help = "DDM version id")

args <- parser$parse_args()
list2env(args, .GlobalEnv)

new_run_id <- version_id
ddm_run_id <- ddm_version_id

master_folder <- paste0("FILEPATH")
input_folder <- paste0("FILEPATH")
output_folder <- paste0("FILEPATH")

source(paste0(h, "FILEPATH"))
source(paste0(get_path("FILEPATH"), "FILEPATH"))
source(paste0(get_path("FILEPATH"), "FILEPATH"))
wd <<- paste0("FILEPATH")
DIRECTORY.LIBRARIES <- paste0("FILEPATH")
## Demographic methods and graph code
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
SetGlobalDirectories()

locations <- fread(paste0("FILEPATH"))
births <- fread(paste0("FILEPATH"))

###############################################################################################################
## Load basic 5q0 foundation of data
LoadFoundation <- function() {
  ## Initialize data structure
  char_cols <- c("ihme.loc.id", "source", "compiling.entity", "data.age","filename", "source.type")
  numeric_cols <- c("t", "q5", "source.date",
                    "in.direct", "sd.q5",
                    "log10.sd.q5", "NID", "underlying_NID")

  data <- data.table(iso3 = character())
  data[, (char_cols) := character()]
  data[, (numeric_cols) := numeric()]
  return(data)
}

data <- LoadFoundation()
db.source_table[, added := 0]

# Remove duplicate sources from database
db.source_table <- db.source_table[append_order %in% c(482, 435, 894),
                                   keep_source := 0]

append_rows <- unique(db.source_table[keep_source == 1, append_order])
append_rows <- append_rows[append_rows != 1]

# List current files
all_files <- list()
for(folder in db.dataloc$lin_path){
  folder_list <- list.files(folder)
  all_files <- append(all_files, folder_list)
}

incrementer <- 0
for (i in append_rows){
  print(paste0("append dataset order #", i))
  if (incrementer %% 100 == 0) {
    cat(paste0("Adding datasets: ",incrementer, " of ", length(append_rows), ".\n"))
  }

  source.name <- db.source_table[append_order == i, filename]
  source.type <- db.source_table[append_order == i, source]

  # Skip if deleted/moved file
  if(source.name %in% all_files){

    add.data <- DbLoad(source.name, source.type, db.source_table, db.dataloc)

    # Perform any exceptions/special data treatment as loaded
    post_exclusion_data <- DoExclusions(data, add.data, i-1, births, ddm_run_id)
    if(nrow(post_exclusion_data$add.data) == 0) {
      print(paste("Source:", source.name))
      cat(paste("\nNote: all data were dropped from", source.name, "\n"))
    } else {
      post_exclusion_data[["add.data"]]$source.type <- source.type
      data <- AppendData(post_exclusion_data$data, post_exclusion_data$add.data)
      db.source_table[append_order == i, added := 1]
    }
  }

  incrementer <- incrementer + 1
}

# Remove overlapping SRS for IND locations
data <- data[!(filename == "EST_IND_SRS_1995_2013_v5Q0.dta" &
                 t >= 2008)]
# Rename updated SRS
data[grepl("IND", ihme.loc.id) & NID == 86967, source := "IND_SRS_1988_2018"]

diff <- subset(db.source_table[append_order != 1], keep_source != added & !(source %in% c("BIRTHS", "POP")))
if(nrow(diff) > 0){
  stop("Check your sources. For help, `View(diff)`")
}

if(nrow(data[is.na(source.type)]) > 0){
  stop("Check your source.type column. For help, `View(as.data.table(data)[is.na(source.type)])`")
}

# create a logical column to indicate whether a given source is microdata or report data
data$microdata <- mapvalues(data$source.type,
                            from=c("AGG", "CBH", "SBH", "CUSTOM_CHINA", "CUSTOM_NUARU", "VR"),
                            to=c(0, 1, 1, 0, 0, 1))
data[, source.type := NULL]

# Drop duplicates
deduplicated_data <- FinalDeduplicate(data, locations)

# NID 387640 is actually a birth registry, not VR
deduplicated_data <- deduplicated_data[!(deduplicated_data$NID == 387640),]

# Do scrubbing
scrubbed_data <- dirty_scrub(deduplicated_data)

# mark outliers
killed_data <- kill(scrubbed_data)

# add shocks
shocked_data <- shock(killed_data)

# More drops and location mapping
cleaned_data <- CleanData(shocked_data, locations)

# Additional IND de-dupping
cleaned_data <- as.data.table(cleaned_data)

srs <- cleaned_data[grepl("IND", ihme_loc_id) & grepl("SRS", source, ignore.case = TRUE)]
srs <- srs[!(ihme_loc_id == "IND" & year == 1996.5)]
srs[, round_year := floor(year)]
srs[, dup := .N, by = c("ihme_loc_id", "round_year")]
srs <- srs[!(dup == 2 & source.date != 2020)]
srs[, c("round_year", "dup") := NULL]

cleaned_data <- cleaned_data[!(grepl("IND", ihme_loc_id) & grepl("SRS", source, ignore.case = TRUE))]
cleaned_data <- rbind(cleaned_data, srs)

cleaned_data <- as.data.frame(cleaned_data)

final_data <- indiaFix(cleaned_data)

final_data$in.direct <- final_data[,ifelse(tolower(in.direct) == "na", NA, in.direct)]
final_data$ptid <- rep(0, dim(final_data)[1])
final_data$ptid[final_data$shock == 0 & final_data$outlier == 0] <- 1:length(final_data$ptid[final_data$shock == 0 & final_data$outlier == 0])

write_csv(final_data, paste0("FILEPATH"))
