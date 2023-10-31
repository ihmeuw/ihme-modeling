rm(list=ls())

library(openxlsx)
library(lubridate)

# SETUP
extraction <- read.xlsx("/filepath/bolivia extraction.xlsx")
causes <- unique(extraction$parent_cause)

get_split <- function(cause) {
  for (i in 1:length(causes)) {
    data <- subset(extraction, parent_cause==cause)
    data_split <- split(data, data$start_date)
  }
  return(data_split)
}



# PROCESSING

diarrhea <- get_split("diarrhea")
lri <- get_split("LRI")
meningitis <- get_split("meningitis")
pertussis <- get_split("pertussis")

# diarrhea processing
n <- length(diarrhea)
keep_diarrhea <- Reduce(intersect, list(diarrhea[[1]]$department, diarrhea[[2]]$department))
diarrhea_2 <- lapply(1:n, function(x) { subset(diarrhea[[x]], diarrhea[[x]]$department %in% keep_diarrhea)})
diarrhea_2 <- rbindlist(diarrhea_2)

# LRI processing
n_lri <- length(lri)
keep_lri <- Reduce(intersect, list(lri[[1]]$department, lri[[2]]$department, lri[[3]]$department, lri[[4]]$department))
lri_2 <- lapply(1:n_lri, function(x) { subset(lri[[x]], lri[[x]]$department %in% keep_lri)})
lri_2 <- rbindlist(lri_2)

# meningitis processing
n_meningitis <- length(meningitis)
keep_meningitis <- Reduce(intersect, list(meningitis[[1]]$department, meningitis[[2]]$department, meningitis[[3]]$department, meningitis[[4]]$department))
meningitis_2 <- lapply(1:n_meningitis, function(x) { subset(meningitis[[x]], meningitis[[x]]$department %in% keep_meningitis)})
meningitis_2 <- rbindlist(meningitis_2)

# pertussis processing
n_pertussis <- length(pertussis)
keep_pertussis <- Reduce(intersect, list(pertussis[[1]]$department, pertussis[[2]]$department, pertussis[[3]]$department, pertussis[[4]]$department))
pertussis_2 <- lapply(1:n_pertussis, function(x) { subset(pertussis[[x]], pertussis[[x]]$department %in% keep_pertussis)})
pertussis_2 <- rbindlist(pertussis_2)

# bind then aggregate by department
combine <- rbindlist(list(diarrhea_2, lri_2, meningitis_2, pertussis_2))
cols <- colnames(combine)[-1]
cols <- cols[-grep("cases", cols)]
combine2 <- combine[,  lapply(.SD, sum) , by = cols, .SDcols = "cases"]

# convert dates
combine2 <- combine2[order(start_date)]
combine2$start_date <- convertToDate(combine2$start_date)
combine2$end_date <- convertToDate(combine2$end_date)
combine2$start_date <- format(combine2$start_date, "%m/%d/%Y")
combine2$end_date <- format(combine2$end_date, "%m/%d/%Y")

# export
write.xlsx(combine2, paste0(getwd(), "/bolivia_extraction.xlsx"))
