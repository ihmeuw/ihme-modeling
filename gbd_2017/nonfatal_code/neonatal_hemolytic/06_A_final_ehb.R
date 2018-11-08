# Author: Helena Manguerra
# Date: 2018-04-16
# --------------

os <- .Platform$OS.type
if (os=="windows") {
  j <- FILEPATH
  h <- FILEPATH
  my_libs <- paste0(h, "/local_packages")
} else {
  j <- FILEPATH
  h <- FILEPATH
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(ggplot2)

source(FILEPATH)
source(FILEPATH)

demo <- get_demographics("epi")$location_id

demo_temp <- get_demographics_template("epi")

rh <- fread(FILEPATH)
rh[, type := "rh"]

g6pd <- fread(FILEPATH)
g6pd[, type := "g6pd"]
 
preterm <- fread(FILEPATH)
preterm[, type := "preterm"]
 

other <- fread(FILEPATH)
other[, type := "other"]
 
ehb <- rbindlist(list(rh, g6pd, preterm, other), use.names = T, fill = T)

ehb <- ehb[year %in% c(1990, 1995, 2000, 2005, 2010, 2017) & sex %in% c(1,2), c("location_id", "sex", "year", "type", paste0("draw_", 0:999))]

ehb <- melt(ehb, id.vars = c("location_id", "sex", "year", "type"), variable.name = "draw")

ehb <- ehb[location_id %in% demo, ]

ehb[, draw := gsub(draw, pattern = "draw_", replacement = "")]

ehb <- ehb[, sum(value), by = list(location_id, sex, year, draw)]

ehb <- dcast(ehb, location_id + sex + year ~ paste0("draw_", draw), value.var = "V1")

ehb_bp <- copy(ehb)
ehb_bp[,age_group_id := 164]

ehb_enn <- copy(ehb)
ehb_enn[,age_group_id := 2]

ehb_lnn <- copy(ehb)
ehb_lnn[,age_group_id := 3]

ehb_neonatal <- rbindlist(list(ehb_bp, ehb_enn, ehb_lnn), use.names = T, fill = T)

setnames(ehb_neonatal, c("sex", "year"), c("sex_id", "year_id"))

for(loc in demo){

  write.csv(ehb_neonatal[location_id == loc, ], FILEPATH, na = "", row.names = F)
  
}

## QSUB
job_name <- "save_ehb"
slot_number <- 25
next_script <- FILEPATH

## QSUB SHELL SCRIPT 