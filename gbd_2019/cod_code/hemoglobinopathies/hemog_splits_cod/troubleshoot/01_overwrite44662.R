###############
# Purpose: save over the existing 44662.csvs with a copy of 44661.csv
#   This means - i will have to create a new copy with the name 44662.csv
#   which will save over the incorrect old. 
###############
  
rm(list=ls())
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "J:/"
    h <- "H:/"
  } else {
    j <- "/snfs1/"
    h <- "/homes/chikeda/"
  }

  
filepath <- paste0(h, "repos/hemog/hemog_splits_cod/tmp/")
    
gender <- c(1,2)

goodutla <- 44661
badutla <- 44662

years <- c(2012:2017)
cause_list <- c(614, 615, 616, 618)
gender <- c(1,2)

for (g in gender){
  for(cause in cause_list){
    for (year in years){
    input <- paste0(filepath, cause, "/", g,  "/", goodutla, "_", year, "_", g, ".csv")
    output <- paste0(paste0(filepath, cause, "/", g,  "/", badutla, "_", year, "_", g, ".csv"))
    print(paste0("reading from ", input))
    data <- read.csv(paste0(input), header = TRUE, sep = ",")
    write.csv(data, paste0(output), row.names = FALSE)
    print(paste0("saved to ", output))
    
    }
  }
}
