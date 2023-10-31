source("FILEPATH")
library(data.table)
library(readr)
library(assertable)
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

loc_map <- get_locations()

lt_version <-x
asfr_version <- x
srb_version <- x
lb_version <- x
pop_version <- x

output_dir <- "FILEPATH"

## Run NRR code first

nrr_dir <- paste0("FILEPATH")
nrr_files <- list.files(nrr_dir, pattern = "FILEPATH")
nrr <- assertable::import_files(nrr_files, nrr_dir, FUN=fread)
nrr_formatted <- copy(nrr)
nrr_formatted[,measure_id := 45]
nrr_formatted[,metric_id := 3]
nrr_formatted[,sex_id := 2]
nrr_formatted[,age_group_id := 22]
setnames(nrr_formatted, 'value', 'val')

write_csv(nrr_formatted, "FILEPATH")

asfr <- get_mort_outputs('asfr', 'estimate', run_id = asfr_version)
asfr_formatted <- asfr[,.(year_id, location_id, age_group_id, mean, lower, upper)]
asfr_formatted[,measure_id := 45]
asfr_formatted[,metric_id := 3]
asfr_formatted[,sex_id := 2]
setnames(asfr_formatted, 'mean', 'val')

write_csv(asfr_formatted, paste0("FILEPATH"))

tfr <- get_covariate_estimates(covariate_id = 149, model_version_id = 34914, 
                               decomp_step = 'iterative', gbd_round_id = 6)
tfr_formatted <- tfr[,.(year_id, location_id, age_group_id, sex_id, mean_value, lower_value, upper_value)]
tfr_formatted[,measure_id := 45]
tfr_formatted[,metric_id := 3]
setnames(tfr_formatted, c('mean_value', 'lower_value', 'upper_value'), c('val', 'lower', 'upper'))
write_csv(asfr_formatted, "FILEPATH")

asfr_dir <- paste0("FILEPATH")
asfr_draw_files <- list.files(asfr_dir, pattern = "FILEPATH", recursive = T)
asfr_draws <- import_files(asfr_draw_files, folder=asfr_dir, FUN=fread)

asfr <- data.table()
for(model_age in seq(10, 50, 5)){
  age_dir <- paste0("FILEPATH")
  asfr_draw_files <- list.files(age_dir, pattern = "FILEPATH")
  asfr_draws <- import_files(asfr_draw_files, folder=age_dir, FUN=fread)
  asfr_draws[, age := model_age]
  asfr_draws[, V1 := NULL]
  asfr <- rbind(asfr, asfr_draws)
}

tfu25 <- asfr[age < 25]
tfu25 <- tfu25[, .(tfu25 = 5*sum(val), age_group_id = 22), by = .(ihme_loc_id, year, sim)]
tfu25 <- tfu25[,.(mean = mean(tfu25), lower = quantile(tfu25, 0.025), upper = quantile(tfu25, 0.975)),
      by = c("ihme_loc_id", "year")]
tfu25_formatted <- copy(tfu25)
tfu25_formatted[,age_group_id := 159]
tfu25_formatted[,measure_id := 45]
tfu25_formatted[,metric_id := 3]
tfu25_formatted[,sex_id := 2]
tfu25_formatted <- merge(tfu25_formatted, loc_map[,.(ihme_loc_id, location_id)], by='ihme_loc_id')
tfu25_formatted[,ihme_loc_id := NULL]
tfu25_formatted[,year_id := floor(year)]
tfu25_formatted[,year := NULL]
setnames(tfu25_formatted, 'mean', 'val')
write_csv(tfu25_formatted, "FILEPATH")

lb <- get_mort_outputs('birth', 'estimate', run_id = lb_version)
lb_formatted <- lb[sex_id == 3,.(year_id, location_id, age_group_id, mean, lower, upper)]
lb_formatted[,measure_id := 45]
lb_formatted[,metric_id := 1]
lb_formatted[,sex_id := 3]
setnames(lb_formatted, 'mean', 'val')
write_csv(lb_formatted, "FILEPATH")

pop <- get_mort_outputs('population', 'estimate', run_id = pop_version)
setnames(pop, 'mean', 'pop')
cbr <- merge(lb_formatted[age_group_id == 169], pop[sex_id == 3 & age_group_id == 22,.(location_id, year_id, pop)], by=c('location_id', 'year_id'))
cbr_formatted <- cbr[,.(val = val/pop*1000, lower = lower/pop*1000, upper = upper/pop*1000), by=c('location_id', 'year_id', 'age_group_id', 'measure_id', 'metric_id', 'sex_id')]
write_csv(cbr_formatted, "FILEPATH")
