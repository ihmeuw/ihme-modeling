# --------------
# Title:    08_final_kernicterus.R
# Date:     2018-04-16
# Purpose:  Sum EHB prevalence across four etiologies of Rh disease, G6PD, preterm, and other. Produce three identical
#           sets of estimates for the three age groups of at birth, early neonatal, and late neonatal.
# Requirements: Update the qsub so that it launches from the current directory of the repos.
# Last Update:  2020-07-30
# --------------

rm(list = ls())

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

library(data.table)
library(magrittr)
library(ggplot2)
library(plotly)

source("PATHNAME/get_demographics.R"  )
source("PATHNAME/get_demographics_template.R"  )
source("PATHNAME/get_location_metadata.R"  )
source("PATHNAME/get_covariate_estimates.R"  )


template <- get_demographics("epi")
template_locs <- template[[1]]
template_years <- template[[4]]

out_dir <- 'PATHNAME'

rh <- fread(paste0(out_dir,"PATHNAME/rh_disease_kernicterus_all_draws.csv"))
rh[, type := "rh"]
# location_id, year, sex, ihme_loc_id

g6pd <- fread(paste0(out_dir,"PATHNAME/g6pd_kernicterus_all_draws.csv"))
g6pd[, type := "g6pd"]
# location_id, sex, year, location_name
# g6pd_temp <- fread("PATHNAME/g6pd_model_205103_prev.csv")

preterm <- fread(paste0(out_dir,"PATHNAME/preterm_kernicterus_all_draws.csv"))
preterm[, type := "preterm"]
# location_id, sex, year

other <- fread(paste0(out_dir,"PATHNAME/other_kernicterus_all_draws.csv"))
other[, type := "other"]
# location_id, sex, year, ihme_loc_id


#test adding means instead of draws


kern <- rbindlist(list(rh, g6pd, other), use.names = T, fill = T)

kern <- kern[year %in% template_years & sex %in% c(1,2), c("location_id", "sex", "year", "type", paste0("draw_", 0:999))]

kern <- melt(kern, id.vars = c("location_id", "sex", "year", "type"), variable.name = "draw")

kern <- kern[location_id %in% template_locs, ]

kern[, draw := gsub(draw, pattern = "draw_", replacement = "")]

kern <- kern[, sum(value), by = list(location_id, sex, year, draw)]

kern <- dcast(kern, location_id + sex + year ~ paste0("draw_", draw), value.var = "V1")
kern <- as.data.table(kern)

kern_neonatal <- copy(kern)
kern_neonatal[,age_group_id := 2]

setnames(kern_neonatal, c("sex", "year"), c("sex_id", "year_id"))

draw_cols <- paste0("draw_", 0:999)
kern_neonatal$mean <- rowMeans(kern_neonatal[,draw_cols,with=FALSE], na.rm = T)
kern_neonatal$lower <- kern_neonatal[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
kern_neonatal$upper <- kern_neonatal[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
kern_neonatal <- kern_neonatal[,-draw_cols,with=FALSE]
kern_neonatal[lower < 0, lower := 0]
write.csv(kern_neonatal,paste0(out_dir, "PATHNAME/all_locs_means_nopreterm.csv"), row.names = FALSE)


#format for bundle upload
locs <- get_location_metadata(location_set_id = 22)[, .(location_id, location_name)]
locs <- locs[location_id %in% template_locs]
kern_neonatal <- merge(kern_neonatal, locs, by = 'location_id', all.y = TRUE)

kern_neonatal[, `:=` (modelable_entity_id = 3962,
                      age_start = 0,
                      age_end = 0.019)]

kern_neonatal[sex_id == 1, sex := 'Male']
kern_neonatal[sex_id == 2, sex := 'Female']

setnames(kern_neonatal, 'year_id', 'year_start')
kern_neonatal[, `:=` (year_end = year_start,
                      measure = "prevalence",
                      representative_name = "Nationally and subnationally representative",
                      year_issue = 0,
                      sex_issue = 0,
                      age_issue = 0,
                      age_demographer = 0,
                      unit_type = "Person",
                      unit_value_as_published = 1,
                      measure_issue = 0,
                      measure_adjustment = 0,
                      extractor = "steeple",
                      uncertainty_type = "Confidence interval",
                      uncertainty_type_value = 95,
                      urbanicity_type = "Unknown",
                      recall_type = "Not Set",
                      is_outlier = 0,
                      standard_error = '',
                      effective_sample_size = '' ,
                      cases = '' ,
                      sample_size = '' ,
                      nid = 143264,
                      source_type = "Surveillance - other/unknown",
                      row_num = '' ,
                      parent_id = '' ,
                      data_sheet_file_path = "",
                      input_type = "",
                      underlying_nid = '',
                      underlying_field_citation_value = "",
                      field_citation_value = "",
                      page_num = '',
                      table_num = '',
                      ihme_loc_id = "",
                      smaller_site_unit = 0,
                      site_memo = "",
                      design_effect = '',
                      recall_type_value = "",
                      sampling_type = "",
                      response_rate = '' ,
                      case_name = "",
                      case_definition = "",
                      case_diagnostics = "",
                      note_modeler = "",
                      note_SR = "",
                      specificity = '',
                      group = '',
                      group_review = '')]


write.xlsx(kern_neonatal, sheetName = 'extraction',
           file = "PATHNAME/3962_gbd2020_prevalence_preterm.xlsx")


# scatter kernicterus sum including and not including preterm
old_kern <- fread('PATHNAME/final_kernicterus_summary_stats.csv')
setnames(old_kern, 'year', 'year_id')
setnames(old_kern, 'sex', 'sex_id')
new_kern <- fread(paste0(out_dir, "PATHNAME/all_locs_means_nopreterm.csv"))
new_kern_preterm <- fread(paste0(out_dir, "PATHNAME/all_locs_means_preterm.csv"))

compare <- merge(old_kern, new_kern, by = c('location_id', 'year_id', 'sex_id'), all.x = TRUE)
setnames(compare, c('mean.x', 'mean.y'), c('previous', 'current'))
compare$version <- "GBD20"

compare2 <- merge(old_kern, new_kern_preterm, by = c('location_id', 'year_id', 'sex_id'), all.x = TRUE)
setnames(compare2, c('mean.x', 'mean.y'), c('previous', 'current'))
compare2$version <- "GBD20 + preterm"

compare2 <- rbind(compare, compare2, fill = TRUE)

compare <- merge(compare, new_kern_preterm, by = c('location_id', 'year_id', 'sex_id'), all.x = TRUE)
setnames(compare, c('mean'), c('current_preterm'))

locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7,
                              decomp_step = 'iterative')
compare <- merge(compare, locs, by = 'location_id', all.x = TRUE)
compare2 <- merge(compare2, locs, by = 'location_id', all.x = TRUE)

#stack
old_kern$version <- "GBD19"
new_kern$version <- "GBD20"
new_kern_preterm$version <- "GBD20 + preterm"
compare_long <- rbind(old_kern, new_kern, fill = TRUE)
compare_long <- rbind(compare_long, new_kern_preterm, fill = TRUE)
compare_long <- merge(compare_long, locs, by = 'location_id', all.x = TRUE)

#add haqi to g6pd prev
haqi <- get_covariate_estimates(1099, decomp_step='iterative')[,.(location_id,year_id,mean_value)]
setnames(haqi,c("mean_value"),c("haqi"))
compare_long <- merge(compare_long, haqi, by = c('location_id','year_id'))



pdf(file = paste0(out_dir, "PATHNAME"),
    width = 12, height = 8)
gg1 <- ggplot() +
  geom_point(data = compare2[year_id < 2020], aes(x = previous, y = current, color = version,
                                                 text = paste0(location_name_short, ' (', location_id,') ', year_id, ' ', sex_id, ': (',
                                                               round(previous,5),', ',round(current,5),')')),
             alpha = 0.5) +
  facet_wrap(~ year_id) +
  theme_bw() + 
  geom_abline(intercept = 0, slope = 1, alpha = 0.2) +
  labs(title = paste0('Prevalence of Total Kernicterus - Method Comparison'),
       x = 'GBD2019 Model',
       y = 'GBD2020 Model')
print(gg1)
dev.off()
