#################################################################################################
#' Split the parent prevalence of Genital Herpes into its sequela
################################################################################################

#SETUP
source("FILEPATH")
library("openxlsx")
library("data.table")
pacman::p_load(data.table, openxlsx, ggplot2, dplyr)

#SOURCE FUNCTIONS----------------------------------------------------------------------------------------------------------------------------------------------------------------------
source_shared_functions(functions = c("get_draws"))

#ARGS & DIRS
draws <- paste0("draw_",0:999)
args <- commandArgs(trailingOnly = TRUE)
loc_id <- as.numeric(args[1])
out_dir <- as.character(args[2])

parent_meid <- as.numeric(args[3])
asymp_meid <- as.numeric(args[4])
recurr_meid <- as.numeric(args[5])
init_meid <- as.numeric(args[6])

#GET PARENT MODEL
cat("getting draws from source model")
prev_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = parent_meid, source = "epi",
                        measure_id = 5, location_id = loc_id, status = "best")
inc_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = parent_meid, source = "epi",
                       measure_id = 6, location_id = loc_id, status = "best")

prev_draws[, c("model_version_id", "modelable_entity_id") := NULL]
prev_draws <- prev_draws[!(age_group_id %in% c(164,27))]
inc_draws[, c("model_version_id", "modelable_entity_id") := NULL]
inc_draws <- inc_draws[!(age_group_id %in% c(164,27))]

#ASYMP INFECTION
asymp_prev <- copy(prev_draws)
asymp_prev[ ,(draws) := lapply(.SD, function(x) x*0.956), .SDcols = draws]

write.csv(x = asymp_prev[sex_id == 1], file = "FILEPATH", row.names = F)
write.csv(x = asymp_prev[sex_id == 2], file = "FILEPATH", row.names = F)

asymp_inc <- copy(inc_draws)
asymp_inc[ ,(draws) := lapply(.SD, function(x) x*(1-0.175)), .SDcols = draws]

write.csv(x = asymp_inc[sex_id == 1], file = "FILEPATH", row.names = F)
write.csv(x = asymp_inc[sex_id == 2], file = "FILEPATH", row.names = F)
  
#RECURRENT SYMP GH
recurr_prev <- copy(prev_draws)
recurr_prev[ ,(draws) := lapply(.SD, function(x) x*0.0436), .SDcols = draws]

write.csv(x = recurr_prev[sex_id == 1], file = "FILEPATH", row.names = F)
write.csv(x = recurr_prev[sex_id == 2], file = "FILEPATH", row.names = F)

#INITIAL SYMP GH - CONTINUE USING GBD SCALAR METHOD
prep props
prop_dt <- data.table(read.csv(paste0(out_dir, "FILEPATH")))
prop_dt$mvar <- NULL
prop_dt <- melt(data = prop_dt,variable.name = "draw", value.name = "prop_val")
prop_dt$draw <- gsub(pattern = "prop", replacement = "draw", x = prop_dt$draw)

inc to prev w. scalar
init_prev <- copy(inc_draws) 
init_prev[ ,measure_id := 5]
init_prev <- melt(data = init_prev, id.vars = c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"), variable.name = "draw", value.name = "draw_val")

init_prop <- merge(init_prev, prop_dt, by = c("draw"), all.x = T)
init_prop[ ,new_draw_val := draw_val * prop_val]
init_prop[ ,c("draw_val","prop_val") := NULL]
init_prop <- dcast(data = init_prop, formula = age_group_id+location_id+measure_id+sex_id+year_id+metric_id~draw, value.var = "new_draw_val")

write.csv(x = init_prop[sex_id == 1], file = "FILEPATH", row.names = F)
write.csv(x = init_prop[sex_id == 2], file = "FILEPATH", row.names = F)

init_inc <- copy(inc_draws)
init_inc[ ,(draws) := lapply(.SD, function(x) x*0.175), .SDcols = draws]

write.csv(x = init_inc[sex_id == 1], file = "FILEPATH", row.names = F)
write.csv(x = init_inc[sex_id == 2], file = "FILEPATH", row.names = F)

cat("finished!")


