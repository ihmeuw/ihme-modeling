## Reaggregate into <2500g, <28, 28-32, 32-37, <37 week MEs


rm(list = ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  my_libs <- "FILEPATH"
  
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  my_libs <- paste0(j, "/temp/USER/cluster_packages")
}

library(data.table)
library(ggplot2)
library(parallel)

source("FILEPATH")
source("FILEPATH")

distn <- "ensemble"

args <- commandArgs(trailingOnly = TRUE)

loc <- args[1]

########################
## Load in exposures, me_map & save_template

  print(paste("Location", loc))
  
  birth_long <- data.table(readRDS("FILEPATH"))  
  birth_long[, draw := as.integer(draw)]
  
  
  correct_NA <- birth_long[grep(x = modelable_entity_name, pattern = "NA g"), c("location_id", "sex_id", "age_group_id", "year_id", "draw", "ME_prop")]
  setnames(correct_NA, "ME_prop", "add_ME_prop")
  
  birth_long <- merge(correct_NA, birth_long, by = c("location_id", "sex_id", "age_group_id", "year_id", "draw"), all = T)
  birth_long[is.na(add_ME_prop), add_ME_prop := 0]
  birth_long[, ME_prop := ME_prop + add_ME_prop]
  birth_long <- birth_long[!(grep(x = modelable_entity_name, pattern = "NA g")), ]
  birth_long <- birth_long[, -c("add_ME_prop")]
  
  ## Aggregate under 24
  
  birth_long[, ga_start := lapply(strsplit(x = modelable_entity_name, ","), '[[', 1)]
  birth_long[, ga_start := lapply(strsplit(x = unlist(ga_start), "\\["), '[[', 2)]
  
  birth_long[, bw_start := lapply(strsplit(x = modelable_entity_name, ","), '[[', 3)]
  birth_long[, bw_start := lapply(strsplit(x = unlist(bw_start), "\\["), '[[', 2)]
  
  birth_long[bw_start == "5000 ) g", bw_start := "5000"]
  birth_long[, bw_start := as.integer(bw_start)]
  birth_long[, ga_start := as.integer(ga_start)]
  
  birth_long[ga_start < 22, ga_start := 22]
  birth_long[, ME_prop := lapply(.SD, sum, na.rm = T), by = list(sex_id, year_id, age_group_id, draw, bw_start, ga_start), .SDcols = "ME_prop"]
  
  birth_long <- birth_long[!(grepl(x = modelable_entity_name, pattern = "\\[0, 20") | grepl(x = modelable_entity_name, pattern = "\\[20, 22)")), ]
  
  birth_long <- birth_long[, -c("ga_start", "bw_start")]
  birth_long <- unique(birth_long)
  
  birth_wide <- dcast(birth_long, location_id + sex_id + age_group_id + year_id + modelable_entity_name ~ paste0("draw_", draw), value.var = "ME_prop", variable.var = "draw_")
  

  ######
  
  bw_u_2500_row <- copy(birth_wide)
  
  
  bw_u_2500_row[, bw_start := lapply(strsplit(x = modelable_entity_name, ","), '[[', 3)]
  bw_u_2500_row[, bw_start := lapply(strsplit(x = unlist(bw_start), "\\["), '[[', 2)]
  bw_u_2500_row[bw_start == "5000 ) g", bw_start := "5000"]
  bw_u_2500_row[, bw_start := as.integer(bw_start)]
  
  bw_u_2500_row[bw_start <=2000, type := "bw_u_2500_cop"]
  bw_u_2500_row <- bw_u_2500_row[!(bw_start > 2000), ]
  
  
  cols <- paste0("draw_", 0:999)
  
  bw_u_2500_row <- bw_u_2500_row[, lapply(.SD, sum, na.rm=TRUE), by=list(location_id, sex_id, age_group_id, year_id), .SDcols = cols ]
  bw_u_2500_row[, age_group_id := 164]
  bw_u_2500_row[, measure_id := 5]


  ## Save location
  write.csv(bw_u_2500_row, "FILEPATH", row.names = F)
  
  
  ########
  
  birth_wide[, ga_start := lapply(strsplit(x = modelable_entity_name, ","), '[[', 1)]
  birth_wide[, ga_start := lapply(strsplit(x = unlist(ga_start), "\\["), '[[', 2)]
  
  birth_wide[ga_start <=26, type := "ga_u_28_cop"]
  birth_wide[ga_start >26 & ga_start <= 30, type := "ga_28_32_cop"]
  birth_wide[ga_start >30 & ga_start <= 36, type := "ga_32_37_cop"]
  

  birth_wide <- birth_wide[!(ga_start > 36), ]
  
  ga_u_28_cop <- birth_wide[type == "ga_u_28_cop", lapply(.SD, sum, na.rm=TRUE), by=list(location_id, sex_id, age_group_id, year_id), .SDcols = cols ]
  ga_u_28_cop[, age_group_id := 164]
  ga_u_28_cop[, measure_id := 5]
  
  write.csv(ga_u_28_cop, "FILEPATH", row.names = F)
  
  ##########################
  
  ga_28_32_cop <- birth_wide[type == "ga_28_32_cop", lapply(.SD, sum, na.rm=TRUE), by=list(location_id, sex_id, age_group_id, year_id), .SDcols = cols ]
  ga_28_32_cop[, age_group_id := 164]
  ga_28_32_cop[, measure_id := 5]
  
  write.csv(ga_28_32_cop, "FILEPATH", row.names = F)
  
  ga_32_37_cop <- birth_wide[type == "ga_32_37_cop", lapply(.SD, sum, na.rm=TRUE), by=list(location_id, sex_id, age_group_id, year_id), .SDcols = cols ]
  ga_32_37_cop[, age_group_id := 164]
  ga_32_37_cop[, measure_id := 5]
  
  write.csv(ga_32_37_cop, "FILEPATH", row.names = F)
  
  ga_u_37_cop <- birth_wide[, lapply(.SD, sum, na.rm=TRUE), by=list(location_id, sex_id, age_group_id, year_id), .SDcols = cols ]
  ga_u_37_cop[, age_group_id := 164]
  ga_u_37_cop[, measure_id := 5]
  
  write.csv(ga_u_37_cop, "FILEPATH", row.names = F)
  
  
