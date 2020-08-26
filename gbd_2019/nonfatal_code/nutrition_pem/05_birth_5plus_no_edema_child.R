
# 1. Estimate >5 Moderate without Edema & Severe without Edema by using Protein-Energy Malnutrition as the Prevalence of Wasting over >5 as the parent, and splitting by ratio of moderate/severe wasting in 1-4
# 2. Read in <5 Moderate without Edema & Severe without Edema and resave in {location_id}_{age_group_id}.csv files
#
# --------------

rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  my_libs <- paste0(h, "/local_packages")
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(ggplot2)


## Move to parallel script
## Getting normal QSub arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- ifelse(is.na(args[1]), "FILEPATH", args[1])

## Retrieving array task_id
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
param_map <- fread(param_map_filepath)

loc <- param_map[task_id, loc]

source("FILEPATH"  )
source("FILEPATH")


# --------------------------
# Get PEM results, per loc-year-sex-measure

dt <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 16406, source = "epi", measure_id = list(5,6),
                location_id = loc, year_id = list(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019), sex_id = list(1,2),
                gbd_round_id = 6, decomp_step = "step4")

dt <- melt(dt, find_non_draw_cols(dt), variable.name = "draw", value.name = "prev")
dt <- dt[!(age_group_id %in% c(22,27))]
dt <- dt[, -c("model_version_id")]

print("received PEM")

# --------------------------
# Get ratio of moderate to severe in 1-4, per loc-year-sex-measure

ratio <- get_draws(source = "epi", gbd_id_type = c("modelable_entity_id", "modelable_entity_id"), gbd_id = c(23514, 23513),
                   location_id = loc, year_id = list(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019), sex_id = list(1,2), age_group_id = 5, num_workers = 4,
                   gbd_round_id = 6, decomp_step = "step4")

ratio[, measure_id := 5]


print("ratio")

id_cols <- find_non_draw_cols(ratio)
ratio <- melt(ratio, id_cols, variable.name = "draw", value.name = "value")
ratio <- dcast(ratio, paste0(paste0(id_cols[!grepl(id_cols, pattern = "modelable_entity_id|model_version")], collapse = " + "), " + draw ~ modelable_entity_id"))
setnames(ratio, c("23514", "23513"), c("severe", "moderate"))
ratio <- data.table(ratio)
ratio <- ratio[, list(location_id, measure_id, metric_id, sex_id, year_id, draw, severe, moderate) ]


# --------------------------
# Apply ratio to prevalence

combined <- merge(dt[measure_id == 5], ratio, all.x = T, by = intersect(names(dt), names(ratio)))
combined <- combined[!(age_group_id %in% c(22, 27))]

combined[, mod_wasting := prev * moderate / (moderate + severe)]
combined[, sev_wasting := prev * severe / (moderate + severe)]

combined[is.na(mod_wasting), mod_wasting := 0]
combined[is.na(sev_wasting), sev_wasting := 0]



# --------------------------
# Calculte incidence from prevalence -- the same way in prevalence to incidence script.
# Use the prevalence:incidence ratios in 16406


ratio_prev_inc <- dcast(dt, age_group_id + location_id + sex_id + year_id + draw ~ paste0("measure_id_", measure_id), value.var = "prev")

combined <- merge(combined, ratio_prev_inc, by = intersect(names(combined), names(ratio_prev_inc)))

combined[, sev_wasting_inc := sev_wasting * (measure_id_6 / measure_id_5) ]
combined[, mod_wasting_inc := mod_wasting * (measure_id_6 / measure_id_5) ]

# Do not save age groups 2,3,4,5
combined <- combined[!(age_group_id %in% c(2,3,4,5)), ]

# --------------------------
# Re-cast severe wide to {location_id}_{age_group_id}.csv

severe <- combined[, list(location_id, sex_id, year_id, draw, age_group_id, sev_wasting, sev_wasting_inc)]
severe <- melt(severe, id_vars = c("location_id", "sex_id", "age_group_id", "year_id", "draw", "age_group_id"), measure.vars = c("sev_wasting", "sev_wasting_inc"))

setnames(severe, "variable", "measure_id")
severe[measure_id == "sev_wasting", measure_id := "5"]
severe[measure_id == "sev_wasting_inc", measure_id := "6"]

severe <- dcast(severe, location_id + sex_id + year_id + age_group_id + measure_id ~ draw)


# --------------------------
# Re-cast moderate wide to {location_id}_{age_group_id}.csv

moderate <- combined[, list(location_id, sex_id, year_id, draw, age_group_id, mod_wasting, mod_wasting_inc)]
moderate <- melt(moderate, id_vars = c("location_id", "sex_id", "age_group_id", "year_id", "draw", "age_group_id"), measure.vars = c("mod_wasting", "mod_wasting_inc"))

setnames(moderate, "variable", "measure_id")
moderate[measure_id == "mod_wasting", measure_id := "5"]
moderate[measure_id == "mod_wasting_inc", measure_id := "6"]
moderate <- dcast(moderate, location_id + sex_id + year_id + age_group_id + measure_id ~ draw)



save_files_by_age <- function(data, me){
  
  if(me == 1607){ id = "whz3"  }
  if(me == 10981){ id = "whz2"  }
  
  for(age in unique(data$age_group_id)){
    
    write.csv(data[age_group_id == age, ], paste0("FILEPATH", id, "_noedema_all_ages/", loc, "_", age, ".csv"), row.names = F, na = "")
    
  }
  
}

save_old_draw_files_in_new_form <- function(loc, me){
  
  if(me == 1607){ id = "whz3"  }
  if(me == 10981){ id = "whz2"  }
  
  prev_data <- fread(paste0("FILEPATH", id, "_noedema/5_", loc, ".csv"))
  prev_data[, measure_id := 5][, location_id := loc]
  
  
  inc_data <- fread(paste0("FILEPATH", id, "_noedema/6_", loc, ".csv"))
  inc_data[, measure_id := 6][, location_id := loc]
  
  data <- rbindlist(list(prev_data, inc_data), use.names = T, fill = T)
  
  data <- data[, c("location_id", "sex_id", "year_id", "age_group_id", "measure_id", paste0("draw_", 0:999))]
  
 
  
  save_files_by_age(data = data, me = me)
  
}

save_files_by_age(data = severe, me = 1607)
save_files_by_age(data = moderate, me = 10981)

save_old_draw_files_in_new_form(loc, me = 1607)
save_old_draw_files_in_new_form(loc, me = 10981)
