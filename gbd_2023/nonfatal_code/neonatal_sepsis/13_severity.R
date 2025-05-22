## ******************************************************************************
## Purpose: Apply severity proportions to prevalence at 28 days, then back-calculate
##          the severity proportions at ENN and LNN based on that.
## Input:   28 day prevalence results, mild and modsev impairment proportions
## Output:  Two save results jobs for asymp and mild, and one file of data to upload
##          for modsev
## ******************************************************************************

rm(list=ls())

print(paste("Working dir:", getwd()))

os <- .Platform$OS.type
if (os == "Windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH/"
  h <- paste0("FILEPATH/")
}

library(data.table); library(openxlsx)

source("FILEPATH/get_demographics.R"  )
source("FILEPATH/get_demographics_template.R"  )
source("FILEPATH/get_bundle_data.R"  )
source("FILEPATH/upload_bundle_data.R"  )
source("FILEPATH/get_draws.R"  )

out_dir <- "FILEPATH"

template <- get_demographics("epi")
template_locs <- template[[1]]
template_ages <- template[[3]]
template_yrs <- template[[4]]
template_dt <- get_demographics_template("epi")
template_dt_birth <- copy(template_dt[age_group_id == 2])
template_dt_birth[, age_group_id := 164]
template_dt <- rbind(template_dt_birth, template_dt)

mild_prop <- fread('FILEPATH/neonatal_sepsis_long_mild_draws.csv')
mild_prop <- mild_prop[year %in% template_yrs]
mild_prop <- mild_prop[location_id %in% template_locs]

modsev_prop <- fread('FILEPATH/neonatal_sepsis_long_modsev_draws.csv')
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

both_long[, value.asymp := 1 - prop_sum]
setnames(both_long, c('year', 'sex'), c('year_id', 'sex_id'))

#pull in the 28d prevalence, multiply by severity proportions
setwd("FILEPATH")
files <- list.files()
prev28 <- lapply(files, fread) %>% rbindlist()
prev28 <- prev28[year_id %in% template_yrs & location_id %in% template_locs]

prev28_long <- as.data.table(melt.data.table(prev28, id.vars = names(prev28)[!grepl("draw", names(prev28))], 
                                             measure.vars = patterns("draw"),
                                             variable.name = 'draw.id'))
prev28_long <- merge(prev28_long, both_long, by = c('location_id', 'year_id', 'sex_id', 'draw.id'))
prev28_long[, `:=` (prev.asymp = value * value.asymp, 
                    prev.mild = value * value.x, 
                    prev.modsev = value * value.y)]

#pull in LNN prevalence, keep asymp and mild the same, set remainder to mod/sev
setwd("FILEPATH")
files <- list.files()
prev3 <- lapply(files, fread) %>% rbindlist()
prev3_long <- as.data.table(melt.data.table(prev3, id.vars = names(prev3)[!grepl("draw", names(prev3))], 
                                            measure.vars = patterns("draw"),
                                            variable.name = 'draw.id'))
prev3_long$age_group_id <- NULL
setnames(prev3_long, 'value','prev3')
prev28_long <- merge(prev28_long, prev3_long, by = c('location_id', 'year_id', 'sex_id', 'draw.id'))
prev28_long[, `:=` (prev.modsev.3 = prev3 - prev.asymp - prev.mild)]
prev28_long[prev.modsev.3 < 0, prev.modsev.3 := 0] 

#pull in ENN prevalence, keep asymp and mild the same, set remainder to mod/sev
setwd("FILEPATH")
files <- list.files()
prev2 <- lapply(files, fread) %>% rbindlist()
prev2_long <- as.data.table(melt.data.table(prev2, id.vars = names(prev2)[!grepl("draw", names(prev2))], 
                                            measure.vars = patterns("draw"),
                                            variable.name = 'draw.id'))
prev2_long$age_group_id <- NULL
setnames(prev2_long, 'value','prev2')
prev28_long <- merge(prev28_long, prev2_long, by = c('location_id', 'year_id', 'sex_id', 'draw.id'))
prev28_long[, `:=` (prev.modsev.2 = prev2 - prev.asymp - prev.mild)]
prev28_long[prev.modsev.2 < 0, prev.modsev.2 := 0] 

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

# read in and append incidence before writing out
sepsis_inc <- get_draws("modelable_entity_id", gbd_id=25300, source="epi", location_id=template_locs,
                        gbd_round_id=7, decomp_step="iterative")[,-c("measure_id","metric_id","modelable_entity_id","model_version_id")]

# create rest of age groups with incidence=0
draw.id <- paste0("draw_",0:999)
temp_sepsis <- expand.grid(location_id=template_locs, age_group_id=template_ages[!(template_ages %in% c(2,3))], 
                           sex_id=c(1,2), year_id=template_yrs, draw.id=draw.id) %>% data.table
temp_sepsis[, value:=0]
temp_sepsis <- dcast(temp_sepsis, location_id + year_id + age_group_id + sex_id ~ draw.id, value.var="value")

sepsis_inc <- rbind(sepsis_inc, temp_sepsis)

for (loc_i in template_locs) {
  for (year_i in template_yrs) {
    for (sex_i in c(1,2)) {
      write.csv(sepsis_inc[location_id == loc_i & year_id == year_i & sex_id == sex_i,], 
                file = paste0(out_dir, '/asymp/6_',loc_i,'_',year_i,'_',sex_i,'.csv'),
                row.names = FALSE)
    }
  }
}


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

modsev_wide <- rbind(modsev2_wide, modsev3_wide, modsev28_wide)

#collapse to means and add epi upload columns
modsev_wide[, mean := apply(.SD, 1, mean), .SDcols = draw_cols]
modsev_wide[, lower := apply(.SD, 1, quantile, c(0.025)), .SDcols = draw_cols]
modsev_wide[, upper := apply(.SD, 1, quantile, c(0.975)), .SDcols = draw_cols]
modsev_wide[, (draw_cols) := NULL]

modsev_wide[sex_id == 1, sex := 'Male']
modsev_wide[sex_id == 2, sex := 'Female']

modsev_wide[, `:=` (year_start = year_id, year_end = year_id,
                    modelable_entity_id = 8674,
                    nid = 256561, underlying_nid = '',
                    bundle_id = 499,
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

# Save new data
write.xlsx(modsev_wide, sheetName = 'extraction',
           file = "FILEPATH/gbd20_iter_prevalence_sev_split.xlsx")

# Pull old bundle data to clear out
dt <- get_bundle_data(bundle_id = 499, decomp_step = 'iterative', gbd_round_id = 7)

#create a 1 column data table of the current prevalence seqs, which will be deleted
seqs_to_delete <- dt[measure == "prevalence", list(seq)]

#append the current seqs to the new data so that the current seq rows will get deleted during upload
file_for_upload <- rbindlist(list(seqs_to_delete, modsev_wide), use.names = T, fill = T)

upload_filepath <- paste0(j, "FILEPATH/gbd20_", bun_id, "_file_for_upload_iterative.xlsx")
write.xlsx(file_for_upload, file = upload_filepath, sheetName = "extraction", showNA = F, row.names=F )

upload_bundle_data(499, "iterative", upload_filepath, 7)

## END