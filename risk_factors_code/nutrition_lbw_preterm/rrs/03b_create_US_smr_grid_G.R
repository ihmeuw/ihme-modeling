
rm(list=ls())

os <- .Platform$OS.type

if (os=="windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  my_libs <- NULL
  
  
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  my_libs <- "FILEPATH"
}


library(reshape2)
library(data.table)

## Load central functions

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

me_map <- fread("FILEPATH")
  

## Grab relative risks & aggregate both age groups

rr_1_3 <- fread("FILEPATH")
rr_1_3[, ager5 := 3]
rr_1_4 <- fread("FILEPATH")
rr_1_4[, ager5 := 4]
rr_2_3 <- fread("FILEPATH")
rr_2_3[, ager5 := 3]
rr_2_4 <- fread("FILEPATH")
rr_2_4[, ager5 := 4]

rrs <- rbindlist(list(rr_1_3, rr_1_4, rr_2_3, rr_2_4), use.names = T, fill = T)

rrs[, sex := as.integer(sex)]

rrs[,rr_exp := exposure * relative_risk]

rrs[, sum_rr_exp := lapply(.SD, sum, na.rm = T), by = list(sex, ager5), .SDcols = "rr_exp"]

rrs[, smr := rr_exp / sum_rr_exp]

write.csv(rrs, "FILEPATH", row.names=F, na="")

