#############################
## Scales ASFR by TFR draws for all locations we estimate
############################

### Settings ###

rm(list=ls())


if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, magrittr) 

mod_id <- commandArgs()[3]
loc_id <- commandArgs()[4]

root <- ifelse(Sys.info()[1]=="Windows","J:","/home/j")
dir <- "FILEPATH" 
setwd(dir)

tfr_draws_dir <- "FILEPATH"
asfr_draws_dir <- "FILEPATH"
if(!dir.exists("FILEPATH")) dir.create("FILEPATH")


source("FILEPATH") # load get_locations
codes <- get_locations(2016) %>% as.data.table
merge.codes <- codes[, .(ihme_loc_id, location_id)]

setwd("FILEPATH")
source("FILEPATH")
source("FILEPATH")



#########################
# Scale All to IHME TFR at the Same Time
#########################

combined <- fread("FILEPATH")

setwd(tfr_draws_dir)
tfr <- list.files(tfr_draws_dir, pattern = paste0("^",as.character(loc_id), ".csv$")) %>% lapply(., fread) %>% rbindlist

#saves draw variables to be used for column specific operations
vars <- grep("draw_", names(tfr), value = T)

tfr <- melt.data.table(tfr, measure.vars = vars, variable.name = "sim", value.name = "ihme_tfr")
tfr[, sim := as.numeric(gsub("draw_", "", sim))]

combined[, combined_tfr := 5*sum(asfr), by = .(location_id, year_id)]
combined <- merge(combined, tfr, by = c("location_id", "year_id"), allow.cartesian = T)
combined[, scaling_factor := ihme_tfr/combined_tfr]
combined[, asfr := asfr*scaling_factor]
combined[, scaling_factor := NULL]

combined[,scaled_tfr := 5*sum(asfr), by = .(location_id, year_id, sim)]
stopifnot(nrow(combined[abs(scaled_tfr - ihme_tfr > .0001)]) == 0)

#Save wide by draw
combined <- dcast.data.table(combined, location_id + year_id + age_group_id + level + parent_id ~ sim, value.var = "asfr")

##################
## Merge on populations
##################
pop <- get_population(location_id = loc_id, year_id = -1, sex_id = 2, age_group_id = c(8:14), location_set_id = 21)[, .(location_id, year_id, age_group_id, population)]
combined <- merge(combined, pop, by = c("location_id", "year_id", "age_group_id"))

write.csv(combined, "FILEPATH", row.names = F)



