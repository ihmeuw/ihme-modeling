

rm(list=ls())
library(foreign); library(haven)

if (Sys.info()[1]=="Windows"){
  root <- "FILEPATH"
  source("FILEPATH/get_locations.r")
} else {
  root <- "FILEPATH"
  div <- as.integer(commandArgs()[3]) 
  num <- as.integer(commandArgs()[4])  
  file <- commandArgs()[5]            
  outfile <- commandArgs()[6]         
  outtype <- commandArgs()[7]          
  source(paste0("FILEPATH/get_locations.r"))
}


if (grepl(".dta",file)) {
  data <- read_dta(file)
} else if (grepl(".csv",file)) {
  data <- read.csv(file,stringsAsFactors=F)
} else if (grepl(".rdata",file) | grepl(".Rdata",file)) {
  data <- load(data)
} else {
  stop("Not recognized file type for reading compiled location files")
}

## find locations we want to save in this parallel copy
locs <- unique(data$ihme_loc_id)
persave <- ceiling(length(locs)/div)

start <- (num-1)*persave + 1
end <- num*persave
loc_subset <- locs[start:end][!is.na(locs[start:end])]

data <- data[data$ihme_loc_id %in% loc_subset,]
stopifnot(dim(data)==dim(unique(data)))

## loop and save
for (loc in loc_subset) {
  if (grepl("dta",outtype)) {
    write.dta(data[data$ihme_loc_id == loc,],paste0(outfile,loc,".",outtype))
  } else if (grepl("csv",outtype)) {
    write.csv(data[data$ihme_loc_id == loc,],paste0(outfile,loc,".",outtype),row.names=F)
  } else if (grepl("rdata",outtype) | grepl("Rdata",outtype)) {
    data <- save(data[data$ihme_loc_id == loc,],file=paste0(outfile,loc,".",outtype))
  } else {
    stop("Not recognized file type for saving location files")
  }
}




