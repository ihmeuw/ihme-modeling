## ******************************************************************************
##
## Purpose: Apply severity proportions to prevalence at 28 days, then back-calculate
##          the severity proportions at birth, ENN and LNN based on that.
## Input:   28 day prevalence results, mild and modsev impairment proportions
## Output:  Two save results jobs for asymp and mild, and one file of data to upload
##          for modsev
## Last Update: 8/12/20
##
## ******************************************************************************

rm(list=ls())

print(paste("Working dir:", getwd()))
my_libs = "PATHNAME"

os <- .Platform$OS.type
if (os == "windows") {
  j <- "PATHNAME"
  h <- "PATHNAME"
} else {
  j <- "PATHNAME"
  h <- "PATHNAME"
  my_libs <- "PATHNAME"
}

library(data.table)
library(openxlsx, lib.loc = my_libs)

source("PATHNAME/get_demographics.R"  )
source("PATHNAME/get_demographics_template.R"  )
source("PATHNAME/validate_input_sheet.R"  )

out_dir <- "PATHNAME"

template <- get_demographics("epi")
template_locs <- template[[1]]
template_ages <- template[[3]]
template_yrs <- template[[4]]
template_dt <- get_demographics_template("epi")
template_dt_birth <- copy(template_dt[age_group_id == 2])
template_dt_birth[, age_group_id := 164]
template_dt <- rbind(template_dt_birth, template_dt)

mild_prop <- fread('PATHNAME')
mild_prop <- mild_prop[year %in% template_yrs]
mild_prop <- mild_prop[location_id %in% template_locs]

modsev_prop <- fread('PATHNAME')
modsev_prop <- modsev_prop[year %in% template_yrs]
modsev_prop <- modsev_prop[location_id %in% template_locs]

draw_cols <- paste0("draw_", 0:999)
mild_long <- as.data.table(melt.data.table(mild_prop, id.vars = names(mild_prop)[!grepl("draw", names(mild_prop))], 
                                         measure.vars = patterns("draw"),
                                         variable.name = 'draw.id'))
modsev_long <- as.data.table(melt.data.table(modsev_prop, id.vars = names(modsev_prop)[!grepl("draw", names(modsev_prop))], 
                                           measure.vars = patterns("draw"),
                                           variable.name = 'draw.id'))

both_long <- merge(mild_long, modsev_long, by = c('location_id', 'year', 'sex', 'draw.id'))
both_long[, prop_sum := value.x + value.y]
nrow(both_long[prop_sum > 0.9])

#great, they all sum to less than 0.9, so proceed to apply them to the prevalences
both_long[, value.asymp := 1 - prop_sum]
setnames(both_long, c('year', 'sex'), c('year_id', 'sex_id'))

#pull in the 28d prevalence, multiply by severity proportions
prev <- fread('PATHNAME')
prev <- prev[year_id %in% template_yrs & location_id %in% template_locs]
prev28 <- prev[age_group_id == 999]

prev28_long <- as.data.table(melt.data.table(prev28, id.vars = names(prev28)[!grepl("draw", names(prev28))], 
                                           measure.vars = patterns("draw"),
                                           variable.name = 'draw.id'))
prev28_long <- merge(prev28_long, both_long, by = c('location_id', 'year_id', 'sex_id', 'draw.id'))
prev28_long[, `:=` (prev.asymp = value * value.asymp, prev.mild = value * value.x, prev.modsev = value * value.y)]

#pull in LNN prevalence, keep asymp and mild the same, set remainder to mod/sev
prev3 <- prev[age_group_id == 3]
prev3_long <- as.data.table(melt.data.table(prev3, id.vars = names(prev3)[!grepl("draw", names(prev3))], 
                                             measure.vars = patterns("draw"),
                                             variable.name = 'draw.id'))
prev3_long$age_group_id <- NULL
setnames(prev3_long, 'value','prev3')
prev28_long <- merge(prev28_long, prev3_long, by = c('location_id', 'year_id', 'sex_id', 'draw.id'))
prev28_long[, `:=` (prev.modsev.3 = prev3 - prev.asymp - prev.mild)]

#pull in ENN prevalence, keep asymp and mild the same, set remainder to mod/sev
prev2 <- prev[age_group_id == 2]
prev2_long <- as.data.table(melt.data.table(prev2, id.vars = names(prev2)[!grepl("draw", names(prev2))], 
                                            measure.vars = patterns("draw"),
                                            variable.name = 'draw.id'))
prev2_long$age_group_id <- NULL
setnames(prev2_long, 'value','prev2')
prev28_long <- merge(prev28_long, prev2_long, by = c('location_id', 'year_id', 'sex_id', 'draw.id'))
prev28_long[, `:=` (prev.modsev.2 = prev2 - prev.asymp - prev.mild)]

#pull in birth prevalence, keep asymp and mild the same, set remainder to mod/sev
prev0 <- prev[age_group_id == 164]
prev0_long <- as.data.table(melt.data.table(prev0, id.vars = names(prev0)[!grepl("draw", names(prev0))], 
                                            measure.vars = patterns("draw"),
                                            variable.name = 'draw.id'))
prev0_long$age_group_id <- NULL
setnames(prev0_long, 'value','prev0')
prev28_long <- merge(prev28_long, prev0_long, by = c('location_id', 'year_id', 'sex_id', 'draw.id'))
prev28_long[, `:=` (prev.modsev.0 = prev0 - prev.asymp - prev.mild)]


#save a file of each set of results (asymp, mild, modsev) in draw space
mild_wide <- as.data.table(dcast(prev28_long, location_id + year_id + sex_id ~ draw.id,
                                 value.var = c('prev.mild')))
mild_wide <- merge(template_dt, mild_wide, by = c('location_id', 'year_id', 'sex_id'), all.x = TRUE)

#save a set of means for diagnostics
mild_plot <- copy(mild_wide)
mild_plot[, mean := apply(.SD, 1, mean), .SDcols = draw_cols]
mild_plot[, lower := apply(.SD, 1, quantile, c(0.025)), .SDcols = draw_cols]
mild_plot[, upper := apply(.SD, 1, quantile, c(0.975)), .SDcols = draw_cols]
mild_plot[, (draw_cols) := NULL]
write.csv(mild_plot, row.names = FALSE, file = paste0(out_dir, '/mild_prev_summary_means.csv'))

for (loc_i in template_locs) {
  for (year_i in template_yrs) {
    for (sex_i in c(1,2)) {
      write.csv(mild_wide[location_id == loc_i & year_id == year_i & sex_id == sex_i,], 
                file = paste0(out_dir, '/mild/5_',loc_i,'_',year_i,'_',sex_i,'.csv'),
                row.names = FALSE)
    }
  }
}

## QSUB
job_flag <- '-N save_mild'
project_flag <- paste0("-P proj_neonatal")
thread_flag <- "-l fthread=20"
mem_flag <- "-l m_mem_free=30G"
runtime_flag <- "-l h_rt=02:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q i.q"
next_script <- "PATHNAME"
errors_flag <- paste0("-e PATHNAME")
outputs_flag <- paste0("-o PATHNAME")
shell_script <- "-cwd PATHNAME/r_shell_ide.sh"
me_id <- 8652
filepath <- paste0(out_dir, '/mild/') 
desc <- 'mild_age_group_fix'

qsub <- paste("qsub ", job_flag, project_flag, thread_flag, mem_flag, runtime_flag, jdrive_flag, queue_flag,
              errors_flag, outputs_flag, shell_script, next_script, me_id, desc, filepath)
system(qsub)


asymp_wide <- as.data.table(dcast(prev28_long, location_id + year_id + sex_id ~ draw.id,
                                  value.var = c('prev.asymp')))
asymp_wide <- merge(template_dt, asymp_wide, by = c('location_id', 'year_id', 'sex_id'), all.x = TRUE)

#save a set of means for diagnostics
asymp_plot <- copy(asymp_wide)
asymp_plot[, mean := apply(.SD, 1, mean), .SDcols = draw_cols]
asymp_plot[, lower := apply(.SD, 1, quantile, c(0.025)), .SDcols = draw_cols]
asymp_plot[, upper := apply(.SD, 1, quantile, c(0.975)), .SDcols = draw_cols]
asymp_plot[, (draw_cols) := NULL]
write.csv(asymp_plot, row.names = FALSE, file = paste0(out_dir, '/asymp_prev_summary_means.csv'))

asymp_long <- as.data.table(melt.data.table(asymp_wide, id.vars = names(asymp_wide)[!grepl("draw", names(asymp_wide))], 
                                            measure.vars = patterns("draw"),
                                            variable.name = 'draw.id'))
asymp_long[(age_group_id %in% c(164,2,3,388,389)) == FALSE, value := 0]
asymp_wide <- as.data.table(dcast(asymp_long, location_id + year_id + sex_id + age_group_id ~ draw.id))

for (loc_i in template_locs) {
  for (year_i in template_yrs) {
    for (sex_i in c(1,2)) {
      write.csv(asymp_wide[location_id == loc_i & year_id == year_i & sex_id == sex_i,], 
                file = paste0(out_dir, '/asymp/5_',loc_i,'_',year_i,'_',sex_i,'.csv'),
                row.names = FALSE)
    }
  }
}

## QSUB
job_flag <- '-N save_asymp'
project_flag <- paste0("-P proj_neonatal")
thread_flag <- "-l fthread=20"
mem_flag <- "-l m_mem_free=30G"
runtime_flag <- "-l h_rt=02:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q i.q"
next_script <- "PATHNAME"
errors_flag <- paste0("-e PATHNAME")
outputs_flag <- paste0("-o PATHNAME")
shell_script <- "-cwd PATHNAME/r_shell_ide.sh"
me_id <- 9981
filepath <- paste0(out_dir, '/asymp/') 
desc <- 'asymp_age_group_fix'

qsub <- paste("qsub ", job_flag, project_flag, thread_flag, mem_flag, runtime_flag, jdrive_flag, queue_flag,
              errors_flag, outputs_flag, shell_script, next_script, me_id, desc, filepath)
system(qsub)


#for modsev, we want a row for birth, ENN, LNN, and 28 days. melt by age, then rbind
modsev0_wide <- as.data.table(dcast(prev28_long, location_id + year_id + sex_id ~ draw.id,
                                 value.var = c('prev.modsev.0')))
modsev0_wide[, `:=` (age_start = 0, age_end = 0)]
modsev2_wide <- as.data.table(dcast(prev28_long, location_id + year_id + sex_id ~ draw.id,
                                    value.var = c('prev.modsev.2')))
modsev2_wide[, `:=` (age_start = 0, age_end = 0.019)]
modsev3_wide <- as.data.table(dcast(prev28_long, location_id + year_id + sex_id ~ draw.id,
                                    value.var = c('prev.modsev.3')))
modsev3_wide[, `:=` (age_start = 0.02, age_end = 0.076)]
modsev28_wide <- as.data.table(dcast(prev28_long, location_id + year_id + sex_id ~ draw.id,
                                    value.var = c('prev.modsev')))
modsev28_wide[, `:=` (age_start = 28/365, age_end = 28/365)]

modsev_wide <- rbind(modsev0_wide, modsev2_wide, modsev3_wide, modsev28_wide)

#collapse to means and add epi upload columns
modsev_wide[, mean := apply(.SD, 1, mean), .SDcols = draw_cols]
modsev_wide[, lower := apply(.SD, 1, quantile, c(0.025)), .SDcols = draw_cols]
modsev_wide[, upper := apply(.SD, 1, quantile, c(0.975)), .SDcols = draw_cols]
modsev_wide[, (draw_cols) := NULL]

modsev_wide[sex_id == 1, sex := 'Male']
modsev_wide[sex_id == 2, sex := 'Female']

modsev_wide[, `:=` (year_start = year_id, year_end = year_id,
                    modelable_entity_id = 8653,
                    nid = 256562, underlying_nid = '',
                    bundle_id = 497,
                    measure = 'prevalence',
                    seq = '',
                    source_type = "Surveillance - other/unknown",
                    sampling_type = "",
                    representative_name = "Nationally and subnationally representative",
                    urbanicity_type = "Unknown",
                    recall_type = "Not Set", recall_type_value = "",
                    unit_type = "Person",
                    unit_value_as_published = 1,
                    uncertainty_type = "Confidence interval",
                    uncertainty_type_value = 95,
                    input_type = "",
                    standard_error = "",
                    effective_sample_size = "" ,
                    cases = "",
                    sample_size = "",
                    is_outlier = 0,
                    design_effect = "")]

write.xlsx(modsev_wide, sheetName = 'extraction',
           file = "PATHNAME")

validate_input_sheet(bundle_id = 497, filepath = "PATHNAME",
                     error_log_path = paste0("PATHNAME"))

