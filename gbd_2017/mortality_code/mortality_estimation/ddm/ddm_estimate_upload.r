

rm(list=ls())
library(foreign); library(reshape); library(readstata13); library(data.table); library(readr); library(mortdb, lib = "FILEPATH"); library(haven); library(assertable);

if (Sys.info()[1] == "Linux") {
  root <- "/home/j/"
  version_id <- as.character(commandArgs(trailingOnly = T)[1])
  gbd_year <- as.numeric(commandArgs(trailingOnly = T)[2])
  mark_best <- as.logical(commandArgs(trailingOnly= T)[3])
} else {
  root <- "J:/"
}

# Set variables and filepaths
main_dir <- paste0("FILEPATH", version_id, "/")
d08_data_filepath <- paste0(main_dir, "FILEPATH")
d08_data_filepath_old <- paste0("FILEPATH")

d00_data_filepath <- paste0(main_dir, "FILEPATH")

# Get location metadata
ap_old <- data.table(get_locations(gbd_type = 'ap_old', level = 'estimate', gbd_year = gbd_year))
ap_old <- ap_old[location_name == "Old Andhra Pradesh"]

locations <- data.table(get_locations(gbd_year = gbd_year))
locations <- locations[!(grepl("KEN_", ihme_loc_id) & level == 4)]
locations <- rbind(locations, ap_old)
locations <- locations[, list(ihme_loc_id, location_id, location_name)]


####################
## DDM - Estimates
####################

est <- data.table(read_dta(d08_data_filepath))


split <- strsplit(est$iso3_sex_source, "&&")
iso3.sex.source <- do.call(rbind, split)         
est[, iso3 := iso3.sex.source[,1]]
est[, sex := iso3.sex.source[,2]]
est[, source := iso3.sex.source[,3]]  


est <- unique(est[, list(iso3 = ihme_loc_id, iso3_sex_source, source, year, sex, u5 = u5_comp_pred, first = pred1, second_med = pred, second_lower = lower, second_upper = upper, trunc_med = trunc_pred, trunc_lower, trunc_upper)])
est <- est[, lapply(.SD, mean, na.rm = T), .SDcols = c('u5', 'first', 'second_med', 'second_lower', 'second_upper', 'trunc_med', 'trunc_lower', 'trunc_upper'), by = c('iso3', 'iso3_sex_source', 'source', 'year', 'sex')]


est[, trunc.or.second := "second.med"]
est[grepl("DSP",source) | source == "SRS" | grepl("VR", source), trunc.or.second := "trunc.med"]


est[, final_est := 0]
est[trunc.or.second == "second.med", final_est := second_med]
est[trunc.or.second == "trunc.med", final_est := trunc_med]

est[, final_lower := 0]
est[trunc.or.second == "second.med", final_lower := second_lower]
est[trunc.or.second == "trunc.med", final_lower := trunc_lower]  

est[,final_upper := 0]   
est[trunc.or.second == "second.med", final_upper := second_upper]
est[trunc.or.second == "trunc.med", final_upper := trunc_upper]        


est[, trunc.or.second := NULL]
est[, iso3_sex_source := NULL]
est[, year := year + 0.5]  

setnames(est, 
         c('u5', 'first', 'second_med', 'second_lower', 'second_upper', 'trunc_med', 'trunc_lower', 'trunc_upper', 'final_est', 'final_lower', 'final_upper'),
         c('mean_u5', 'mean_first', 'mean_pred2', 'lower_pred2', 'upper_pred2', 'mean_trunc', 'lower_trunc', 'upper_trunc', 'mean_final', 'lower_final', 'upper_final'))
est <- unique(est)

# Reshape long
est <- melt(est, id.vars = c("iso3", "sex", "year", "source"), 
            variable.name = "method", 
            measure.vars = c(grep("mean", names(est), value=T), 
                             grep("lower", names(est), value=T),
                             grep("upper", names(est), value=T)))

est[, value_type :=  gsub("^(.*?)_.*", "\\1", method)]
est[, method := gsub("^.*\\_", "_", method)]
est <- dcast.data.table(est, iso3 + sex + year + source + method ~ value_type, value.var = 'value')


## Set estimate stage id
est[method == "_first", estimate_stage_id := 1]
est[method == "_pred2", estimate_stage_id := 2]
est[method == "_trunc", estimate_stage_id := 9]
est[method == "_final", estimate_stage_id := 10]
est[method == "_u5", estimate_stage_id := 11]

est[, method := NULL]

est[sex == "male", sex_id := 1]
est[sex == "female", sex_id := 2]
est[sex == "both", sex_id := 3]

setnames(est, "year", "viz_year")
est[, year_id := floor(viz_year)]
est[, age_group_id := 199]
est[estimate_stage_id == 11, age_group_id := 1]

## Generate type_id

est[source == "VR" | source == "VR-SSA", source_type_id := 1]
est[source == "SRS", source_type_id := 2]
est[grepl("DSP", source), source_type_id := 3]
est[source == "CENSUS", source_type_id := 5] 
est[source == "SURVEY", source_type_id := 16]

est[grepl("VR", source) & is.na(source_type_id), source_type_id := 1] 

est[source == "MOH survey", source_type_id := 34]


est[source == "SSPC-DC" , source_type_id := 50]

est[source == "FFPS", source_type_id := 38]
est[source == "SUPAS", source_type_id := 39]

est[source == "SUSENAS", source_type_id := 40]
est[source == "HOUSEHOLD", source_type_id := 42]
est[source == "HOUSEHOLD_HHC", source_type_id := 43]
est[source == "MCCD", source_type_id := 55]
est[source == "CR", source_type_id := 56]

assert_values(est, "source_type_id", "not_na")

setnames(est, "iso3", "ihme_loc_id")
est <- merge(est, locations, by = 'ihme_loc_id', all.x = T)

est <- est[, list(year_id, viz_year, location_id, sex_id, age_group_id, estimate_stage_id, source_type_id, mean, lower, upper, source)]
est <- est[order(year_id, location_id, sex_id, age_group_id, estimate_stage_id, source_type_id),]

# Bring in final comp estimates
final_comp <- data.table(read_dta(paste0(main_dir, "data/d08_final_comp.dta")))
final_comp <- final_comp[, list(ihme_loc_id, year, source, sex, mean = final_comp)]
setnames(final_comp, "year", "viz_year")
final_comp[, year_id := floor(viz_year)]

# Set sex_id
final_comp[sex == "male", sex_id := 1]
final_comp[sex == "female", sex_id := 2]
final_comp[sex == "both", sex_id := 3]
final_comp[, sex := NULL]

# Set age group id variable
final_comp[, age_group_id := 199]

# Set estimate stage id variable
final_comp[, estimate_stage_id := 14]

# Set source type id variable
final_comp[source == "VR" | source == "VR-SSA", source_type_id := 1]
final_comp[source == "SRS", source_type_id := 2]
final_comp[source == "DSP", source_type_id := 3]
final_comp[source == "CENSUS", source_type_id := 5] 
final_comp[source == "MOH survey", source_type_id := 34]
final_comp[source == "SSPC-DC" , source_type_id := 50]
final_comp[source == "DC", source_type_id := 37]
final_comp[source == "SSPC", source_type_id := 36]
final_comp[source == "FFPS", source_type_id := 38]
final_comp[source == "SUPAS", source_type_id := 39]
final_comp[source == "SURVEY", source_type_id := 16]
final_comp[source == "SUSENAS", source_type_id := 40]
final_comp[source == "HOUSEHOLD", source_type_id := 42]
final_comp[source == "HOUSEHOLD_HHC", source_type_id := 43]
final_comp[source == "MCCD", source_type_id := 55]
final_comp[source == "CR", source_type_id := 56]

assert_values(final_comp, "source_type_id", "not_na")

## Merge location metadata
final_comp <- merge(final_comp, locations, by = 'ihme_loc_id', all.x = T)
final_comp <- final_comp[, list(year_id, viz_year, location_id, sex_id, age_group_id, estimate_stage_id, source_type_id, mean, lower = NA, upper = NA, source)]

est <- rbind(est, final_comp)

assert_values(est[estimate_stage_id == 9], c("mean", "lower", "upper"), "not_na")
assert_values(est[estimate_stage_id == 10], c("mean", "lower", "upper"), "not_na")
assert_values(est[estimate_stage_id == 2], c("mean", "lower", "upper"), "not_na")
        
## Save in upload folder
write_csv(est, paste0(main_dir, "FILEPATH"), na = "")


# Upload ddm estimate file
upload_results(filepath = paste0(main_dir, "FILEPATH"),
               model_name = "ddm",
               model_type = "estimate",
               run_id = version_id, send_slack = T)

if (mark_best){
  update_status(model_name = "ddm",
                model_type = "estimate",
                run_id = version_id,
                new_status = "best",
                new_comment = "COMMENT", assert_parents=F, send_slack = F)
}
