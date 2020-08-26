# Reads in master me-bundle sheet
#   checks for differences, and pulls any newly added bundles


rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "/home/j/"
  h <- "/homes/chikeda/"
}

source(paste0(j, "temp/central_comp/libraries/current/r/get_bundle_data.R"))

map <- read.csv(paste0("/share/mnch/crosswalks/all_me_bundle.csv"))
bundle_ids <- unique(map$bundle_id)
bundle_ids <- as.character(bundle_ids)
# bundle_ids <- bundle_ids[bundle_ids != 612]


bundle_path <- paste0("/share/mnch/crosswalks/bundle_data/")
datanames <- dir(bundle_path, pattern = ".csv")
datanames <- gsub(".csv", "", datanames)

bundle_ids <- setdiff(bundle_ids, datanames)

for(bun in bundle_ids){
  print(bun)
  data <- get_bundle_data(bundle_id = bun, decomp_step = 'step1')
  write.csv(data, paste0("/share/mnch/crosswalks/bundle_data/", bun, ".csv"), row.names = FALSE)
  
}


