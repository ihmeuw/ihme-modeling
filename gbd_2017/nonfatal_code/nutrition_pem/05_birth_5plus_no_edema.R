
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- FILEPATH
  h <-FILEPATH
  my_libs <- paste0(h, "/local_packages")
} else {
  j<- FILEPATH
  h<-FILEPATH
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(ggplot2)

args <- commandArgs(trailingOnly = TRUE)
loc <- args[1]

source("FILEPATH/get_draws.R"  )
source("FILEPATH/find_non_draw_cols.R")


dt <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 16406, source = "epi", measure_id = list(5,6),
                location_id = loc, year_id = list(1990, 1995, 2000, 2005, 2010, 2017), sex_id = list(1,2),
                num_workers = 4)

dt <- melt(dt, find_non_draw_cols(dt), variable.name = "draw", value.name = "prev")
dt <- dt[!(age_group_id %in% c(22,27))]
dt <- dt[, -c("model_version_id")]

ratio <- get_draws(source = "epi", gbd_id_type = c("modelable_entity_id", "modelable_entity_id"), gbd_id = c(8945, 10558), measure_id = list(5),
                   location_id = loc, year_id = list(1990, 1995, 2000, 2005, 2010, 2017), sex_id = list(1,2), age_group_id = 5, num_workers = 4)

id_cols <- find_non_draw_cols(ratio)
ratio <- melt(ratio, id_cols, variable.name = "draw", value.name = "value")
ratio <- dcast(ratio, paste0(paste0(id_cols[!grepl(id_cols, pattern = "modelable_entity_id|model_version")], collapse = " + "), " + draw ~ modelable_entity_id"))
setnames(ratio, c("8945", "10558"), c("severe", "moderate"))
ratio <- ratio[, -c("age_group_id")]

combined <- merge(dt[measure_id == 5], ratio, all.x = T, by = intersect(names(dt), names(ratio)))
combined <- combined[!(age_group_id %in% c(22, 27))]

combined[, mod_wasting := prev * moderate / (moderate + severe)]
combined[, sev_wasting := prev * severe / (moderate + severe)]

combined[is.na(mod_wasting), mod_wasting := 0]
combined[is.na(sev_wasting), sev_wasting := 0]

ratio_prev_inc <- dcast(dt, age_group_id + location_id + sex_id + year_id + draw ~ paste0("measure_id_", measure_id), value.var = "prev")

combined <- merge(combined, ratio_prev_inc, by = intersect(names(combined), names(ratio_prev_inc)))

combined[, sev_wasting_inc := sev_wasting * (measure_id_6 / measure_id_5) ]
combined[, mod_wasting_inc := mod_wasting * (measure_id_6 / measure_id_5) ]

combined <- combined[!(age_group_id %in% c(2,3,4,5)), ]

severe <- combined[, list(location_id, sex_id, year_id, draw, age_group_id, sev_wasting, sev_wasting_inc)]
severe <- melt(severe, id_vars = c("location_id", "sex_id", "age_group_id", "year_id", "draw", "age_group_id"), measure.vars = c("sev_wasting", "sev_wasting_inc"))

setnames(severe, "variable", "measure_id")
severe[measure_id == "sev_wasting", measure_id := "5"]
severe[measure_id == "sev_wasting_inc", measure_id := "6"]

severe <- dcast(severe, location_id + sex_id + year_id + age_group_id + measure_id ~ draw)

moderate <- combined[, list(location_id, sex_id, year_id, draw, age_group_id, mod_wasting, mod_wasting_inc)]
moderate <- melt(moderate, id_vars = c("location_id", "sex_id", "age_group_id", "year_id", "draw", "age_group_id"), measure.vars = c("mod_wasting", "mod_wasting_inc"))

setnames(moderate, "variable", "measure_id")
moderate[measure_id == "mod_wasting", measure_id := "5"]
moderate[measure_id == "mod_wasting_inc", measure_id := "6"]
moderate <- dcast(moderate, location_id + sex_id + year_id + age_group_id + measure_id ~ draw)


save_files_by_age <- function(data, me){

  for(age in unique(data$age_group_id)){

    write.csv(data[age_group_id == age, ], paste0("FILEPATH/", me, "/FILEPATH/", loc, "_", age, ".csv"), row.names = F, na = "")

  }

}

reform_draw_files <- function(loc, me){

  if(me == 1607){ id = "whz3"  }
  if(me == 10981){ id = "whz2"  }

  prev_data <- fread(paste0("FILEPATH", id, "FILEPATH", loc, ".csv"))
  prev_data[, measure_id := 5][, location_id := loc]


  inc_data <- fread(paste0("FILEPATH/", id, "FILEPATH", loc, ".csv"))
  inc_data[, measure_id := 6][, location_id := loc]

  data <- rbindlist(list(prev_data, inc_data), use.names = T, fill = T)

  data <- data[, c("location_id", "sex_id", "year_id", "age_group_id", "measure_id", paste0("draw_", 0:999))]

  save_files_by_age(data = data, me = me)

}

save_files_by_age(data = severe, me = 1607)
save_files_by_age(data = moderate, me = 10981)

reform_draw_files(loc, me = 1607)
reform_draw_files(loc, me = 10981)

