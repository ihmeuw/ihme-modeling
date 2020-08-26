# --------------
# Purpose:  Sum EHB prevalence across four etiologies of Rh disease, G6PD, preterm, and other.
# --------------

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH/j/"
  h <- paste0("FILEPATH", Sys.getenv("USER"),"/")
}

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/get_demographics.R"  )
source("FILEPATH/get_demographics_template.R"  )

demo <- get_demographics("epi")$location_id
demo_temp <- get_demographics_template("epi")

rh <- fread("FILEPATH/rh_disease_ehb_all_draws.csv")
rh[, type := "rh"]
# location_id, year, sex, ihme_loc_id

g6pd <- fread("FILEPATH/g6pd_ehb_all_draws.csv")
g6pd[, type := "g6pd"]
# location_id, sex, year, location_name

preterm <- fread("FILEPATH/preterm_ehb_all_draws.csv")
preterm[, type := "preterm"]
# location_id, sex, year

other <- fread("FILEPATH/other_ehb_all_draws.csv")
other[, type := "other"]
# location_id, sex, year, ihme_loc_id

ehb <- rbindlist(list(rh, g6pd, preterm, other), use.names = T, fill = T)

ehb <- ehb[year %in% c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019) & sex %in% c(1,2), c("location_id", "sex", "year", "type", paste0("draw_", 0:999))]

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

  write.csv(ehb_neonatal[location_id == loc, ], paste0("FILEPATH/", loc, ".csv"), na = "", row.names = F)
  
}


draw_cols <- paste0("draw_", 0:999)
ehb_neonatal$mean <- rowMeans(ehb_neonatal[,draw_cols,with=FALSE], na.rm = T)
ehb_neonatal$lower <- ehb_neonatal[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
ehb_neonatal$upper <- ehb_neonatal[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
ehb_neonatal <- ehb_neonatal[,-draw_cols,with=FALSE]
write.csv(ehb_neonatal,paste0("FILEPATH/all_locs_means.csv"))
