################################################################################
## Description: Compile GPR results
################################################################################

rm(list=ls())
library(foreign); library(data.table);

if (Sys.info()[1]=="Windows"){
    root <- "FILEPATH"
    save_prerake <- 1
    source("FILEPATH/shared/functions/get_locations.r")
} else {
    root <- "FILEPATH"
    save_prerake <- as.integer(commandArgs()[3])
    username <- commandArgs()[4]
    code_dir <- "FILEPATH"
    source(paste0("FILEPATH/shared/functions/get_locations.r"))
}

setwd("FILEPATH")  



setwd(paste("FILEPATH")  

codes <- read.csv("FILEPATH", stringsAsFactors = F)
codes <- codes[,c("ihme_loc_id","level","parent_id","location_id","level_1", "level_2", "level_3")]
locs <- unique(codes$ihme_loc_id)

file_fail <- c()
## compile all files
data <- NULL
for (cc in locs) { 
    file <- paste("FILEPATH/gpr_", cc, ".txt", sep="")
    if (file.exists(file)) data <- rbind(data, fread(file, stringsAsFactors = F))
    else {
        cat(paste("Does not exist:", file, "\n")); flush.console()
        file_fail <- c(file_fail, cc)
    }
} 

names(data) <- c("ihme_loc_id","year","med","lower","upper")
## we want to scale subnational estimates so they sum to the national, but at the sim level
  

if (is.null(file_fail)) {
    file_fail <- "NO MISSING FILES"
}

## save file that says which gpr files didn't exist
file_fail <- data.frame(ihme_loc_id = file_fail)
write.csv(file_fail,paste0(root, "FILEPATH/missing_gpr_files.csv"),row.names=F)
stopifnot(as.character(file_fail$ihme_loc_id[1]) == "NO MISSING FILES")  

na.obs <- unique(data$ihme_loc_id[is.na(data$med) | is.na(data$upper) | is.na(data$lower)])
write.csv(na.obs,paste0(root, "FILEPATH/NA_values_gpr.csv"),row.names=F)

## save final file 
data <- data[order(data$ihme_loc_id, data$year),c("ihme_loc_id","year","med","lower","upper"), with=F]

setwd(paste0(root, "FILEPATH"))

write.csv(data, file="FILEPATH", row.names=F)
write.csv(data, file=paste("FILEPATH/estimated_5q0_noshocks_", Sys.Date(), ".txt", sep=""), row.names=F)

