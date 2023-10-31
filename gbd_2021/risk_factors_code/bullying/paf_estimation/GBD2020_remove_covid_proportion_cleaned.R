
# Remove COVID-19 attributable burden from bullying PAFs ------------------

source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_location_metadata.R")

paf_folder <- "/FILEPATH/pafs"

args<-commandArgs(trailingOnly = TRUE)
location <- args[1]

# Pull covid custom models ------------------------------------------------

baseline_mdd <- get_draws(year_id = 2020, gbd_id_type = "modelable_entity_id", gbd_id = 1981, gbd_round_id = 7, source = "epi", decomp_step = 'iterative', status = "best", measure_id = 5, location_id = location)
baseline_mdd <- melt.data.table(baseline_mdd, id.vars = names(baseline_mdd)[!(names(baseline_mdd) %like% "draw")], value.name="b_prev", variable.name="draw")

covid_mdd <- get_draws(year_id = 2020, gbd_id_type = "modelable_entity_id", gbd_id = 26756,  gbd_round_id = 7, source = "epi", decomp_step = 'iterative', status = "best", measure_id = 5, location_id = location)
covid_mdd <- melt.data.table(covid_mdd, id.vars = names(covid_mdd)[!(names(covid_mdd) %like% "draw")], value.name="c_prev", variable.name="draw")

mdd <- merge(baseline_mdd, covid_mdd, by = c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", "draw"))
mdd[, `:=` (cause_id = 568, model_version_id.x = NULL, modelable_entity_id.x = NULL, model_version_id.y = NULL, modelable_entity_id.y = NULL)]

rm(baseline_mdd, covid_mdd)

baseline_anx <- get_draws(year_id = 2020, gbd_id_type = "modelable_entity_id", gbd_id = 1989,  gbd_round_id = 7, source = "epi", decomp_step = 'iterative', status = "best", measure_id = 5, location_id = location)
baseline_anx <- melt.data.table(baseline_anx, id.vars = names(baseline_anx)[!(names(baseline_anx) %like% "draw")], value.name="b_prev", variable.name="draw")

covid_anx <- get_draws(year_id = 2020, gbd_id_type = "modelable_entity_id", gbd_id = 26759,  gbd_round_id = 7, source = "epi", decomp_step = 'iterative', status = "best", measure_id = 5, location_id = location)
covid_anx <- melt.data.table(covid_anx, id.vars = names(covid_anx)[!(names(covid_anx) %like% "draw")], value.name="c_prev", variable.name="draw")

anx <- merge(baseline_anx, covid_anx, by = c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", "draw"))
anx[, `:=` (cause_id = 571, model_version_id.x = NULL, modelable_entity_id.x = NULL, model_version_id.y = NULL, modelable_entity_id.y = NULL)]

rm(baseline_anx, covid_anx)

epi_results <- rbind(mdd, anx)
rm(mdd, anx)

epi_results[, `:=` (change = b_prev / c_prev)]
epi_results[b_prev == 0 & c_prev == 0, change := 1]
epi_results[, `:=` (b_prev = NULL, c_prev = NULL, paf = gsub("draw", "paf", draw))]

# Adjusted Pafs -----------------------------------------------------------

for(p in paste0(paf_folder, "/paf_yld_", location, "_2020_", c(1, 2), ".csv")){
  paf_estimates <- fread(p)
  paf_estimates <- melt.data.table(paf_estimates, id.vars = names(paf_estimates)[!(names(paf_estimates) %like% "paf")], value.name="val", variable.name="paf")
  paf_estimates[, location_id := as.numeric(location_id)]
  paf_estimates <- merge(paf_estimates, epi_results[,.(age_group_id, location_id = as.numeric(location_id), sex_id, year_id, cause_id, change, paf)], all.x = T, by = c("age_group_id", "location_id", "sex_id", "year_id", "cause_id", "paf"))
  paf_estimates[, val := val * change]
  paf_estimates[, `:=` (change = NULL)]
  paf_estimates <- dcast(paf_estimates, rei_id + age_group_id + location_id + sex_id + year_id + sex_id + cause_id + modelable_entity_id ~ paf, value.var="val")
  write.csv(paf_estimates, gsub(paf_folder, "/FILEPATH/pafs_2020_aftercovid", p), row.names = F, na = "")
}







