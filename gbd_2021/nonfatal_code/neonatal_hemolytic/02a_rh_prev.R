## ******************************************************************************
##
## Purpose: Save Rh negativity proportion model results to flat file
## Input: bundle ID
## Output:
## Last Update: 5/5/20
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "PATHNAME"
  h <- "PATHNAME"
} else {
  j <- "PATHNAME"
  h <- paste0("PATHNAME")
  my_libs <- "PATHNAME"
}

pacman::p_load(data.table, magrittr, ggplot2, plotly)

source("PATHNAME/get_location_metadata.R")
source("PATHNAME/get_draws.R")
source("PATHNAME/get_covariate_estimates.R")

output_dir <- 'PATHNAME'

locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
locs <- locs[location_id != 1]

#' A/B. Pull modeled prevalence of Rh negativity
results <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = 24625, measure_id = 5, status = 'best',
                     gbd_round_id = 7, decomp_step = 'iterative', location_id = locs$location_id, sex_id = 3, year_id = 2010, 
                     source = 'epi', metric_id = 3, age_group_id = 34)
results <- results[, -c('metric_id', 'age_group_id', 'measure_id', 'sex_id', 'model_version_id', 'modelable_entity_id')]

write.csv(results, file = paste0(output_dir, 'rh_neg_prev_all_draws.csv'),
          row.names = FALSE, na = '')


#' C. Find proportion Rh-incompatible pregnancies:
#' incompatible_prop = rh_negative_prop * (1-rh_negative_prop)
draw_cols <- paste0("draw_", 0:999)
results[, (draw_cols) := lapply(.SD, function(x) x * (1-x)), .SDcols = draw_cols]

#save draws
write.csv(results, file = paste0(output_dir, 'rh_incompatible_prev_all_draws.csv'),
          row.names = FALSE, na = '')


#' D. Multiply by births to get counts of Rh-incompatible pregnancies
#' incompatible_counts = incompatible_prop * births
live_births <- get_covariate_estimates(covariate_id = 60, sex_id = 3, year_id = c(1980:2022), gbd_round_id = 7,
                                       decomp_step = 'iterative')[,.(location_id, year_id, sex_id, mean_value)]
setnames(live_births, 'mean_value', 'births')
live_births[, births := births * 1000]

live_births_by_sex <- get_covariate_estimates(covariate_id = 1106, sex_id = c(1,2), year_id = c(1980:2022), gbd_round_id = 7,
                                       decomp_step = 'iterative')[,.(location_id, year_id, sex_id, mean_value)]
setnames(live_births_by_sex, 'mean_value', 'births')

live_births <- rbind(live_births, live_births_by_sex)

results <- results[, -c('year_id')]
count_results <- merge(live_births, results, by = c('location_id'), all.x = TRUE)
count_results[, (draw_cols) := lapply(.SD, function(x) x * births), .SDcols = draw_cols]
setnames(count_results, c('year_id', 'sex_id'), c('year','sex'))

#save draws
write.csv(count_results, file = paste0(output_dir,'rh_incompatible_count_all_draws.csv'),
          row.names = FALSE, na = '')


#quick diagnostic scatter
gbd20 <- fread(paste0(output_dir, 'rh_incompatible_count_all_draws.csv' ))
output_dir_old <- 'PATHNAME'
gbd19 <- fread(paste0(output_dir_old, 'rh_incompatible_count_all_draws.csv' ))

draw_cols <- paste0("draw_", 0:999)
gbd20$mean_val <- rowMeans(gbd20[,draw_cols,with=FALSE], na.rm = T)
gbd20 <- gbd20[,-draw_cols,with=FALSE]

gbd19$mean_val <- rowMeans(gbd19[,draw_cols,with=FALSE], na.rm = T)
gbd19 <- gbd19[,-draw_cols,with=FALSE]

gbd20[location_id == 60132, location_id := 4921]
gbd20[location_id == 60133, location_id := 4918]
gbd20[location_id == 60134, location_id := 4917]
gbd20[location_id == 60135, location_id := 4912]
gbd20[location_id == 60136, location_id := 4911]
gbd20[location_id == 60137, location_id := 4928]

dt <- merge(gbd20, gbd19, by = c('location_id', 'year', 'sex'),
            all.x = TRUE)
dt <- dt[year < 2020]

nrow(dt[is.nan(mean_val.y)])
dt <- dt[!is.nan(mean_val.y)]
dt <- dt[most_detailed == 1]

locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
dt <- merge(dt, locs, by = 'location_id', all.x = TRUE)

pdf(file = paste0(output_dir, 'diagnostic_scatter_gbd19_v_gbd20.pdf'), width = 15, height = 8)
gg1 <- ggplot(data = dt[year %in% c(2010) & sex == 3], aes(x = mean_val.y, y = mean_val.x, alpha = 0.3, color = region_name,
                                                                                   text = paste0(location_name, ' ', year, ' (',
                                                                                                 round(mean_val.y,4), ', ',round(mean_val.x,4), ')' ))) +
  geom_point() +
  facet_wrap(~ super_region_name, scales = 'free') +
  geom_abline(intercept = 0, slope = 1, alpha = 0.2) +
  labs(title = 'Rh Incompatible Birth Count Estimates (Linear Space)',
       x = 'GBD 2019',
       y = 'GBD 2020')

gg2 <- ggplot(data = dt[year %in% c(2010) & sex == 3], aes(x = log(mean_val.y), y = log(mean_val.x), alpha = 0.3, color = region_name,
                                                         text = paste0(location_name, ' ', year, ' (',
                                                                       round(mean_val.y,4), ', ',round(mean_val.x,4), ')' ))) +
  geom_point() +
  facet_wrap(~ super_region_name, scales = 'free') +
  geom_abline(intercept = 0, slope = 1, alpha = 0.2) +
  labs(title = 'Rh Incompatible Birth Count Estimates (Log Space)',
       x = 'GBD 2019',
       y = 'GBD 2020')

print(gg1)
print(gg2)

dev.off()
ggplotly(p = gg1, tooltip = 'text') 
