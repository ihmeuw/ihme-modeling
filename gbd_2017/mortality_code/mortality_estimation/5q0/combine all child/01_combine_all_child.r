#############################################
## Combine_all_child
## Description: This file combines all child mortality data into one dataset with consistent formatting.
############################################

library(foreign)
library(grid)
library(lattice)
library(plyr)
library(haven)
library(magrittr)
library(readr)
library(data.table)
library(rjson)
library(ini)
library(mortdb)

master_folder <- "FILEPATH"
input_folder <- paste0(master_folder, "/inputs")
output_folder <- paste0(master_folder, "/outputs")

source("FILEPATH/get_paths.R")
source("FILEPATH/cac_utilities.r")
source("FILEPATH/do_exclusions.r")
wd <<- "FILEPATH/01. Add child only data"
DIRECTORY.LIBRARIES <- "FILEPATH"
## Demographic methods and graph code
source(paste0(DIRECTORY.LIBRARIES,"gbd_envelopes_library.r"))
source(paste0(DIRECTORY.LIBRARIES, "Graph5q0_nosave.r"))
SetGlobalDirectories()

locations <- fread(paste0(input_folder, "/loc_map.csv"))
births <- read.csv(paste0(input_folder, "/births.csv")) ## Uses read.csv to avoid data.frame syntax issues later on in the code

###############################################################################################################
## Load basic 5q0 foundation of data
LoadFoundation <- function() {
  data<-DbLoad("FILEPATH", "AGG",
               db.source, db.dataloc)
  country.to.iso3 <<- read.csv(
    paste0(wd, "/03. aggregate estimates/tom country to iso3.csv"),
    stringsAsFactors = FALSE)   
  # Merge in iso3 codes
  data = merge(unique(country.to.iso3[,c("country", "iso3")]), data, all.y = TRUE)
  index = is.na(data$iso3)
  unique(data[index, "country"])
  
  temp = unique(data[,c("iso3", "country")])
  colnames(temp)[colnames(temp) == "country"] = "laxo.country"
  country.to.iso3 <<- merge(country.to.iso3, temp, all = TRUE)
  index = is.na(country.to.iso3$laxo.country)
  unique(country.to.iso3[index, "country"])
  
  data = data[,colnames(data) != "country"]
  data$source = tolower(data$source)
  
  # add in variables that will be populated in the CBH section
  data$sd.q5 <- NA
  data$log10.sd.q5 <- NA
  data$ihme.loc.id <- NA
  data$NID <- NA
  data$filename <- NA
  data$source.type <- NA
  data$underlying_NID <- NA
  
  data$filename <- "survey"
  data$source.type <- "Tom's"
  return(data)
}

data <- LoadFoundation()
setDF(data)
db.source[, added:=ifelse(filename == "survey", 1, 0)]

# Start at i=2 because 1st dataset loaded in LoadFoundation
for (i in 2:nrow(db.appendorder)){
  if (i%%100==0) {
    cat(paste0("Adding datasets: ",i, " of ", nrow(db.appendorder), ".\n"))
  }
  source.name <- db.appendorder[i, filename]
  source.type <- db.appendorder[i, source_type]
  used <- db.source[filename == source.name & source_type == source.type]$used
  add.data <- DbLoad(source.name, source.type, db.source, db.dataloc)
  # Normalize and underlying_NID NID column capitalization
  if ("nid" %in% tolower(names(add.data))){
    names(add.data)[tolower(names(add.data)) == "nid"] <- "NID"
  }
  if ("underlying_nid" %in% tolower(names(add.data))){
    names(add.data)[tolower(names(add.data)) == "underlying_nid"] <- "underlying_NID"
  }
  # Add source type
  if (source.type %in% c("AGG", "SSC")){
    add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  }
  # Perform any exceptions/special data treatment as loaded
  all.data <- DoExclusions(data, add.data, i-1, births, ddm_run_id)    # (i-1) because order starts at 0, not 1
  data <- all.data$data
  add.data <- all.data$add.data
  
  if(nrow(add.data) == 0 | used == 0) {
    print(paste("Source:", source.name))
    cat(paste("\nNote: all data were dropped from", source.name, "\n"))
  } else {
    add.data$source.type <- source.type
    data <- AppendData(data, add.data)
    db.source[, added := ifelse(filename == source.name & source_type == source.type, 1, added)]
    #print(paste("Source:", source.name))
  }
}

diff <- subset(db.source, used!=added & !(source_type %in% c("BIRTHS", "POP")))
if(nrow(diff) > 0){
  stop("Check your sources. For help, `View(diff)`")
}

if(nrow(as.data.table(data)[is.na(source.type)]) > 0){
  stop("Check your source.type column. For help, `View(as.data.table(data)[is.na(source.type)])`")
}

# create a logical column to indicate whether a given source is microdata or report data
data$microdata <- mapvalues(data$source.type,
                            from=c("AGG", "Tom's", "CBH", "SBH", "CUSTOM_CHINA", "CUSTOM_NUARU", "VR"),
                            to=c(0, 0, 1, 1, 0, 0, 1))
data <- data[, !(names(data) %in% c("source.type"))]

# Drop duplicates more
deduplicated_data <- FinalDeduplicate(data, add.data, locations)
# Do scrubbing
#test_scrubbed_data <- scrub(deduplicated_data)
scrubbed_data <- dirty_scrub(deduplicated_data)

# mark outliers
killed_data <- kill(scrubbed_data)

# add shocks
shocked_data <- shock(killed_data)

# More drops and location mapping
cleaned_data <- CleanData(shocked_data, locations)

# Do AP/Telangana fix
final_data <- indiaFix(cleaned_data)

final_data$in.direct <- final_data[,ifelse(tolower(in.direct) == "na", NA, in.direct)]
final_data$ptid <- rep(0, dim(final_data)[1])
final_data$ptid[final_data$shock == 0 & final_data$outlier == 0] <- 1:length(final_data$ptid[final_data$shock == 0 & final_data$outlier == 0])

write_csv(final_data, paste0(output_folder, "/cac_output.csv"))