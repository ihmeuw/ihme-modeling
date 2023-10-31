## ******************************************************************************
##
## Purpose: Post-collapse processing
## Input:   Directory path containing collapsed survey data, directory path where
##          you want to save processed survey data
## Output:  Processed collapsed survey data
## Created: 2020-1-15
## Last updated: 2020-4-21
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "PATHNAME"
  h <-"PATHNAME"
  my_libs <- "PATHNAME"
} else {
  j<- "PATHNAME"
  h<-"PATHNAME"
  my_libs <- "PATHNAME"
}

pacman::p_load(data.table, readstata13, ggplot2, plotly, dplyr, gridExtra)

source("PATHNAME/get_location_metadata.R")
source("PATHNAME/get_covariate_estimates.R")

input_directory <- 'PATHNAME'

#thesis
input_directory <- 'PATHNAME'

dt <- as.data.table(read.csv(file = paste0(input_directory)))

#remove any duplicate rows
dt <- dt %>% distinct(nid, survey_name, ihme_loc_id, year_start, year_end, survey_module, cv_birth_year, mother_age_years, .keep_all = TRUE)

#outlier nids where maternal age column is clearly wrong
dt[nid %in% c(95474), is_outlier := 1]


dt[, is_outlier := 0]
#outlier Dominican Republic Experimental and Special DHS
dt[nid %in% c(19431, 165645, 21198), is_outlier := 1]

#outlier Togo 2000 UNICEF MICS and Cameroon UNICEF MICS with weirdly low values
dt[nid == 12886, is_outlier := 1]
dt[nid == 2063, is_outlier := 1]

#same with Yemen 2006 MICS3
dt[nid == 13816, is_outlier := 1]

#outlier UNICEF MICS 2006 Bosnia survey with implausible values
dt[nid == 1385, is_outlier := 1]

#outlier Tanzania AIDS survey
dt[nid == 77395, is_outlier := 1]

#outlier Nyanza survey that doesn't have any weights for further subnats
dt[nid == 135416, is_outlier := 1]

#drop national and province loc for this one Kenya survey that also has county level mean
dt <- dt[!(nid == 56420 & ihme_loc_id %in% c('KEN', 'KEN_44794')), ]

#duplicate the values for the Nigeria states with boundary changes
dt_dup <- dt[nid == 20552 & (ihme_loc_id == 'NGA_25329' | ihme_loc_id == 'NGA_25319')]
dt_dup[ihme_loc_id == 'NGA_25329', ihme_loc_id := 'NGA_25327']
dt_dup[ihme_loc_id == 'NGA_25319', ihme_loc_id := 'NGA_25352']

dt <- rbind(dt, dt_dup)

dt <- dt[!(nid == 20674 & ihme_loc_id == '')]

dt[sample_size < 25, is_outlier := 1]

#recode Sudan BR MICS survey to South Sudan
dt[nid == 32189, ihme_loc_id := 'SSD']

#save as final raw data
write.csv(dt, file = 'PATHNAME', row.names = FALSE)

#also plot this new data against GBD2019 nonfirstborn data
#merge on ihme_loc_id and cv_birth_year

gbd20_clean <- dt[, .(nid, ihme_loc_id, cv_birth_year, mean, standard_error, sample_size, is_outlier)]
locs <- get_location_metadata(location_set_id = 22, gbd_round_id = 7)[, .(location_id, ihme_loc_id, region_name)]
gbd20_clean <- merge(gbd20_clean, locs, by = 'ihme_loc_id', all.x = TRUE)
setnames(gbd20_clean, 'cv_birth_year', 'year_start')

gbd19 <- fread("PATHNAME")
gbd19_clean <- gbd19[, .(nid, location_id, year_start, mean, standard_error, sample_size, is_outlier)]

compare <- merge(gbd19_clean, gbd20_clean, by = c('location_id','year_start'), all.x = TRUE)

pdf(file = "PATHNAME"),
    width = 12, height = 8)
gg <- ggplot(data = compare, aes(x = mean.x, y = mean.y, color = region_name)) +
  geom_point(aes(text = paste0(ihme_loc_id, ' year_start: ', year_start, 
                               ' (', mean.x, ', ', mean.y, '), sample size new: ', sample_size.y))) +
  geom_abline(slope = 1) +
  labs(title = paste0('Comparison of Non-Firstborn Prevalence by GBD Version'),
       x = 'GBD 2019',
       y = 'GBD 2020 - Truncated Birth History (CH module)') +
  theme(legend.position = 'bottom')
print(gg)
dev.off()

ggplotly(p = gg, tooltip = 'text')



gbd20_stack <- copy(gbd20_clean)
gbd20_stack[, version := 'gbd2020']
gbd20_stack$ihme_loc_id <- NULL
gbd20_stack$region_name <- NULL

gbd19_stack <- copy(gbd19_clean)
gbd19_stack[, version := 'gbd2019']

stack <- rbind(gbd19_stack, gbd20_stack, fill = TRUE)
#loc set 35 is for modeling
locs <- get_location_metadata(location_set_id = 22, gbd_round_id = 7, decomp_step = 'step2')
stack <- merge(stack, locs, by = 'location_id', all.x = TRUE)


locs_sorted <- stack[, .(location_id, sort_order)]
locs_sorted <- locs_sorted[!duplicated(location_id)]
locs_sorted <- locs_sorted[order(sort_order)]

stack[, lower := mean - (1.96*standard_error)]
stack[, upper := mean + (1.96*standard_error)]
stack[lower < 0, lower := 0]

# stack[, small_sample := FALSE]
# stack[sample_size < 25, small_sample := TRUE]

#pull TFR to add to plots for vetting
tfr <- get_covariate_estimates(covariate_id = 149, gbd_round_id = 7, decomp_step = 'step2')
tfr <- tfr[, .(location_id, year_id, mean_value, lower_value, upper_value)]
setnames(tfr, c('mean_value', 'year_id'), c('tfr', 'year_start'))
stack <- merge(stack, tfr, by = c('location_id', 'year_start'), all.x = TRUE)
nrow(stack[is.na(tfr)])

stack <- stack[!is.na(ihme_loc_id)]

stack$is_outlier <- as.logical(stack$is_outlier) 

pdf(file = "PATHNAME", width = 12, height = 8)

for (loc_i in locs_sorted$location_id) {
  location_name_i <- unique(stack[location_id == loc_i, lancet_label])
  gg1 <- ggplot(data = stack[location_id == loc_i], aes(x = year_start, y = mean, color = version, alpha = is_outlier)) +
    geom_point() +
    geom_errorbar(aes(ymin=lower, ymax=upper)) +
    scale_color_manual(values=c("gbd2019" = "red", "gbd2020" = "blue")) +
    scale_alpha_manual(values=c("TRUE" = 0.2, "FALSE" = 0.9)) +
    xlim(1980,2019) +
    labs(title = paste0(location_name_i),
         x = 'Year',
         y = 'Proportion of Births that are not firstborn') +
    theme_bw()
  
  gg2 <- ggplot(data = tfr[location_id == loc_i & (year_start > 1979)], aes(x = year_start, y = tfr)) +
    geom_line(color = 'seagreen4') +
    geom_point(size = 1, color = 'seagreen4') +
    labs(x = 'Year',
         y = 'Total Fertility Rate') +
    xlim(1980,2024) +
    theme_bw()
  
  grid.arrange(gg1, gg2, ncol = 1, nrow = 2, heights = 2:1)

}

dev.off()

