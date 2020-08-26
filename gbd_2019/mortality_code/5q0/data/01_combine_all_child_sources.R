###############################################################################################################
## Set up settings and import functions

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
  h <<- Sys.getenv("HOME")
  library(mortdb, lib = "FILEPATH")
}

args <- commandArgs(trailingOnly = T)
new_run_id <- as.numeric(args[1])
ddm_run_id <- as.numeric(args[2])
master_folder <- "FILEPATH"
input_folder <- "FILEPATH"
output_folder <- "FILEPATH"

source(paste0(h, "FILEPATH/get_paths.R"))
source(paste0(get_path("child_utilities"), "/cac_utilities.r"))
source(paste0(get_path("combine_all_child"), "/do_exclusions.r"))
wd <<- paste0(j, "FILEPATH")
DIRECTORY.LIBRARIES <- paste0(j, "FILEPATH")
## Demographic methods and graph code
source(paste0(DIRECTORY.LIBRARIES,"gbd_envelopes_library.r"))
source(paste0(DIRECTORY.LIBRARIES, "Graph5q0_nosave.r"))
SetGlobalDirectories()

locations <- fread(paste0(input_folder, "/loc_map.csv"))
births <- fread(paste0(input_folder, "/births.csv"))

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


append_rows <- unique(db.source_table[keep_source == 1, append_order])
append_rows <- append_rows[append_rows != 1]

incrementer <- 0
for (i in append_rows){
  print(paste0("append dataset order #", i))
  if (incrementer %% 100 == 0) {
    cat(paste0("Adding datasets: ",incrementer, " of ", length(append_rows), ".\n"))
  }
  
  source.name <- db.source_table[append_order == i, filename]
  source.type <- db.source_table[append_order == i, source]
  
  add.data <- DbLoad(source.name, source.type, db.source_table, db.dataloc)
  
  post_exclusion_data <- DoExclusions(data, add.data, i-1, births, ddm_run_id)    # (i-1) because order starts at 0, not 1
  if(nrow(post_exclusion_data$add.data) == 0) {
    print(paste("Source:", source.name))
    cat(paste("\nNote: all data were dropped from", source.name, "\n"))
  } else {
    post_exclusion_data[["add.data"]]$source.type <- source.type
    data <- AppendData(post_exclusion_data$data, post_exclusion_data$add.data)
    db.source_table[append_order == i, added := 1]
  }
  
  incrementer <- incrementer + 1
}

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

# Drop duplicates more
deduplicated_data <- FinalDeduplicate(data, locations)

# Do scrubbing
scrubbed_data <- dirty_scrub(deduplicated_data)

# mark outliers
killed_data <- kill(scrubbed_data)

# add shocks
shocked_data <- shock(killed_data)

# More drops and location mapping
cleaned_data <- CleanData(shocked_data, locations)

final_data <- indiaFix(cleaned_data)

final_data$in.direct <- final_data[,ifelse(tolower(in.direct) == "na", NA, in.direct)]
final_data$ptid <- rep(0, dim(final_data)[1])
final_data$ptid[final_data$shock == 0 & final_data$outlier == 0] <- 1:length(final_data$ptid[final_data$shock == 0 & final_data$outlier == 0])

write_csv(final_data, paste0(output_folder, "/cac_output.csv"))