
### Purpose: prep WaSH report extractions
#########################################

# libraries
library(data.table)
library(magrittr)
library(openxlsx)

source("FILEPATH/get_location_metadata.R")
locs <- get_location_metadata(location_set_id = 22, gbd_round_id = gbd_round_id)

# settings
gbd_cycle <- "GBD2020"
report.dir <- file.path("FILEPATH", gbd_cycle, "FILEAPATH")
report.file <- "wash_extraction_2020_08_13.xlsm" # update this as needed

# read in report extractions
wash_reports <- data.table(read.xlsx(file.path(report.dir, report.file)))[-1]
# clean up
wash_reports[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), nid = as.numeric(nid), cv_HH = as.numeric(cv_HH),
                     is_outlier = as.numeric(is_outlier), location_id = as.numeric(location_id))]
wash_reports <- wash_reports[!is.na(nid)] # this extraction sheet has a lot of extra rows with only NAs
wash_reports[, is_outlier := 0]
setnames(wash_reports, "mean", "val")
# add regions
wash_reports <- merge(locs[, .(ihme_loc_id, location_id, location_name, region_name)], wash_reports, by = c("ihme_loc_id","location_id","location_name"))
# calculate mean for sources that reported N and sample size
wash_reports[is.na(val), val := n/sample_size]
# calculate N for sources that reported mean and sample size
wash_reports[is.na(n), n := val*sample_size]

### generate the modeling indicators (e.g. wash_water_piped, wash_sanitation_imp_prop, etc.) for each data type (water, sanitation, & water treatment)
## water #####
w.piped <- c("piped")
w.improved <- c("bottled","imp","piped_imp","spring_imp","well_imp","improved")
w.cw <- c("well_cw","spring_cw","piped_cw") # ambiguous strings that need to be split
w.unimproved <- c("spring_unimp","well_unimp","surface","unimp")

water_reports <- wash_reports[risk == "w_source_drink", .(nid, underlying_nid, file_path, study, location_id, ihme_loc_id, location_name, year_start, year_end,
                                                          risk, measure, val, sample_size, is_outlier, cv_HH, underlying_field_citation_value)]
# reshape wide (so each row is a unique NID-location)
water_reports <- dcast.data.table(water_reports, ... ~ measure, value.var = "val")
# outlier NID-locations that have >15% missingness
water_reports[missing_w_source_mapped >= 0.15, `:=` (is_outlier = 1, outlier_reason = "missingness >= 15%")]
# redistribute missingness to all the different water types (assuming that missingness is evenly distributed, unless it results in proportion >1)
water.cols <- c("imp","unimp","surface","spring_imp","spring_unimp","spring_cw","well_imp",  # these are all the water types
                "well_unimp","well_cw","piped_imp","piped","piped_cw","bottled","not_piped")
water_reports[, (water.cols) := lapply(.SD, function(x) {min(x + missing_w_source_mapped*(x/(1-missing_w_source_mapped)), 1)}), by = 1:nrow(water_reports), .SDcols = water.cols]
# split ambiguous strings
# read in dataset
water.cw.orig <- rbindlist(lapply(c("FILEPATH/cw_water_2.csv", # LBD dataset (maintained by Mat Baumann)
                                    "FILEPATH/cw_water_new_iso3.csv"), # GBD dataset (countries not in LBD)
                                  fread), fill = T)
# for countries with <5 sources, we want to use region-level data
water.cw.reg <- copy(water.cw.orig) # make a copy where we will create region-level sums
water.cw.reg[reg == "oceania", reg := "se_asia"] # Oceania has only 2 countries with 1 source each, so i'm going to lump it in with SE Asia since they are both in the same GBD super region
water.cw.reg[, (water.cols) := lapply(.SD, sum), .SDcols = water.cols, by = "reg"] # sum
locs_under_5 <- water.cw.orig[sources < 5, iso3] # all of the countries with <5 sources
water.cw <- water.cw.orig[iso3 %ni% locs_under_5] # for countries with >=5 sources, keep original values
water.cw <- rbind(water.cw, water.cw.reg[iso3 %in% locs_under_5]) # add on all the countries with <5 sources, all of which now have region-level values
# now create the ratios of improved to total within each category          
water.cw[, well_imp_ratio := well_imp/(well_unimp + well_imp)] # proportion of total wells (improved + unimproved) that are improved
water.cw[, spring_imp_ratio := spring_imp/(spring_unimp + spring_imp)] # proportion of total springs (improved + unimproved) that are improved
water.cw[, piped_imp_ratio := piped_imp/(piped + piped_imp)] # proportion of total piped sources (improved + piped) that are improved
# merge with report data
water_reports[, iso3 := substr(ihme_loc_id,1,3)] # water.cw only has national-level data, so merging on national ihme_loc_id (i.e. subnats will have same values as their parent countries)
water_reports <- merge(water_reports, water.cw[, .(iso3, well_imp_ratio, spring_imp_ratio, piped_imp_ratio)], by = "iso3", all.x = TRUE)
# there are two countries, Saudi Arabia & Solomon Islands, that have *cw measures that need to be split but are not in the water.cw dataset
# so, manually inputting the values for their respective regions
water.cw.reg[, well_imp_ratio := well_imp/(well_unimp + well_imp)]
water.cw.reg[, spring_imp_ratio := spring_imp/(spring_unimp + spring_imp)]
water.cw.reg[, piped_imp_ratio := piped_imp/(piped + piped_imp)]
water_reports[iso3 == "SAU", `:=` (well_imp_ratio = water.cw.reg[reg == "name", unique(well_imp_ratio)], # Saudi Arabia's region is North Africa and Middle East
                                   spring_imp_ratio = water.cw.reg[reg == "name", unique(spring_imp_ratio)],
                                   piped_imp_ratio = water.cw.reg[reg == "name", unique(piped_imp_ratio)])]
water_reports[iso3 == "SLB", `:=` (well_imp_ratio = water.cw.reg[reg == "se_asia", unique(well_imp_ratio)], # Solomon Island's region is Oceania --> using SE Asia; see above
                                   spring_imp_ratio = water.cw.reg[reg == "se_asia", unique(spring_imp_ratio)],
                                   piped_imp_ratio = water.cw.reg[reg == "se_asia", unique(piped_imp_ratio)])]
water_reports[iso3 == "CPV", `:=` (well_imp_ratio = water.cw.reg[reg == "wssa", unique(well_imp_ratio)], # Cabo Verde's region is W. Sub-Saharan Africa
                                   spring_imp_ratio = water.cw.reg[reg == "wssa", unique(spring_imp_ratio)],
                                   piped_imp_ratio = water.cw.reg[reg == "wssa", unique(piped_imp_ratio)])]
# split ambiguous strings
water_reports[, `:=` (well_split_imp = well_cw*well_imp_ratio, # proportion of well_cw that is improved
                      spring_split_imp = spring_cw*spring_imp_ratio, # proportion of spring_cw that is improved
                      piped_split_imp = piped_cw*piped_imp_ratio, # proportion of piped_cw that is improved
                      piped_split_piped = piped_cw*(1-piped_imp_ratio))] # proportion of piped_cw that is piped
water_reports[is.na(well_split_imp), well_split_imp := 0]
water_reports[is.na(spring_split_imp), spring_split_imp := 0]
water_reports[is.na(piped_split_imp), piped_split_imp := 0]
water_reports[is.na(piped_split_piped), piped_split_piped := 0]
# now generate the modeling indicators
# wash_water_piped is simply proportion using piped water
water_reports[, wash_water_piped := piped+piped_split_piped]
# wash_water_imp_prop is proportion of people not using piped that use improved, so first need to get total proportion of improved
water_reports[, imp_total := bottled+imp+piped_imp+spring_imp+well_imp+well_split_imp+spring_split_imp+piped_split_imp]
# then divide by 1 minus proportion using piped
water_reports[, wash_water_imp_prop := imp_total/(1-wash_water_piped)]
# reshape long
water_reports <- melt(water_reports, id.vars = c("iso3","nid","underlying_nid","file_path","study","location_id","ihme_loc_id","location_name","year_start",
                                                 "year_end","risk","sample_size","is_outlier","outlier_reason","cv_HH","underlying_field_citation_value"))
water_reports <- water_reports[variable %in% c("wash_water_piped","wash_water_imp_prop")] # keep only the modeling indicators
setnames(water_reports, c("variable","value"), c("var","val"))
water_reports <- water_reports[order(location_id, year_start)] # sort by location & year
water_reports[, val := round(val,10)] # some values of 1 were 1.00000000000000004 or something. truncating them at 10 decimal points
# calculate SE (Wilson score interval)
water_reports[, standard_error := (1/(1+(1.96^2/sample_size)))*sqrt(((val*(1-val))/sample_size)+((1.96^2)/(4*sample_size^2)))]
# clean up
water_reports[, `:=` (iso3 = NULL, cv_microdata = 0, year_id = year_start, year_start_orig = year_start, year_end_orig = year_end, 
                      age_start = 0, age_end = 125, age_group_id = 22, sex = "Both", sex_id = 3)][, `:=` (year_start = year_id, year_end = year_id)]
# save
out.file <- paste0("water_reports_prepped_", format(Sys.time(), "%m%d%y"), ".csv")
write.csv(water_reports, file.path(report.dir, out.file), row.names = FALSE)



## sanitation #####
s.sewer <- c("septic","sewer","flush_imp_septic","flush_imp_sewer","flush_imp")
s.improved <- c("imp","latrine_imp")
s.cw <- c("flush_cw","latrine_cw")
s.unimproved <- c("flush_unimp","latrine_unimp","unimp","open")

sani_reports <- wash_reports[risk == "t_type", .(nid, underlying_nid, file_path, study, location_id, ihme_loc_id, location_name, year_start, year_end,
                                                 risk, measure, val, sample_size, is_outlier, cv_HH, underlying_field_citation_value)]
# looks like every NID has two entries for measure == latrine_unimp. need to fix
sani_reports <- unique(sani_reports)
to_fix <- c()
for (x in unique(sani_reports$nid)) {
  if (sani_reports[nid == x & measure == "latrine_unimp", length(unique(val))] == 2) {
    to_fix <- append(to_fix, x)
  }
}
sani_reports[nid %in% to_fix & measure == "latrine_unimp" & val == 0, keep := 0]
sani_reports[is.na(keep), keep := 1]
sani_reports <- sani_reports[keep == 1][, keep := NULL]
# reshape wide (so each row is a unique NID-location)
sani_reports <- dcast.data.table(sani_reports, ... ~ measure, value.var = "val")
# outlier NID-locations that have >15% missingness
sani_reports[missing_t_type_mapped >= 0.15, `:=` (is_outlier = 1, outlier_reason = "missingness >= 15%")]
# redistribute missingness to all the different toilet types (assuming that missingness is evenly distributed, unless it results in proportion >1)
sani.cols <- c("imp","unimp","open","latrine_imp","latrine_unimp","latrine_cw","flush_imp", # these are all the toilet types
               "flush_imp_septic","flush_imp_sewer","flush_cw","septic")
sani_reports[, (sani.cols) := lapply(.SD, function(x) {min(x + missing_t_type_mapped*(x/(1-missing_t_type_mapped)), 1)}), by = 1:nrow(sani_reports), .SDcols = sani.cols]
# split ambiguous strings
# read in dataset
sani.cw.orig <- rbindlist(lapply(c("FILEPATH/cw_sani_2.csv", # LBD dataset (maintained by Mat Baumann)
                                   "FILEPATH/cw_sani_new_iso3.csv"), # GBD dataset (countries not in LBD)
                            fread), fill = T)
sani.cw.cols <- c("imp","unimp","od","latrine_imp","latrine_unimp","latrine_cw","flush_imp", # slightly different from sani.cols
                  "flush_unimp","flush_cw","septic","sewer")
# for countries with <5 sources, we want to use region-level data
sani.cw.reg <- copy(sani.cw.orig) # make a copy where we will create region-level sums
sani.cw.reg[reg == "oceania", reg := "se_asia"] # Oceania has only 3 countries with 1 source each, so i'm going to lump it in with SE Asia since they are both in the same GBD super region
sani.cw.reg[, (sani.cw.cols) := lapply(.SD, sum), .SDcols = sani.cw.cols, by = "reg"] # sum
locs_under_5 <- sani.cw.orig[sources < 5, iso3] # all of the countries with <5 sources
sani.cw <- sani.cw.orig[iso3 %ni% locs_under_5] # for countries with >=5 sources, keep original values
sani.cw <- rbind(sani.cw, sani.cw.reg[iso3 %in% locs_under_5]) # add on all the countries with <5 sources, all of which now have region-level values
# now create the ratios of improved to total within each category   
sani.cw[, latrine_imp_ratio := latrine_imp/(latrine_unimp + latrine_imp)] # proportion of total latrines (improved + unimproved) in this location that are improved
sani.cw[, flush_imp_ratio := flush_imp/(flush_unimp + flush_imp)] # proportion of total flush toilets that are flush_imp (which we put in the same category as sewer/septic)
# merge with report data
sani_reports[, iso3 := substr(ihme_loc_id,1,3)] # water.cw only has national-level data, so merging on national ihme_loc_id (i.e. subnats will have same values as their parent countries)
sani_reports <- merge(sani_reports, sani.cw[, .(iso3, latrine_imp_ratio, flush_imp_ratio)], by = "iso3", all.x = TRUE)
# [8/19/20] there are four countries, Antigua and Barbuda, Grenada, N Korea, and Solomon Islands, that have *cw measures that need to be split but are not in the sani.cw dataset
# so, manually inputting the values for their respective regions
sani.cw.reg[, latrine_imp_ratio := latrine_imp/(latrine_unimp + latrine_imp)]
sani.cw.reg[, flush_imp_ratio := flush_imp/(flush_unimp + flush_imp)]
sani_reports[iso3 == "ATG", `:=` (latrine_imp_ratio = sani.cw.reg[reg == "mcacaf", unique(latrine_imp_ratio)], # Antigua's region is Caribbean
                                  flush_imp_ratio = sani.cw.reg[reg == "mcacaf", unique(flush_imp_ratio)])]
sani_reports[iso3 == "GRD", `:=` (latrine_imp_ratio = sani.cw.reg[reg == "mcacaf", unique(latrine_imp_ratio)], # Grenada's region is Caribbean
                                  flush_imp_ratio = sani.cw.reg[reg == "mcacaf", unique(flush_imp_ratio)])]
sani_reports[iso3 == "PRK", `:=` (latrine_imp_ratio = sani.cw.reg[reg == "se_asia", unique(latrine_imp_ratio)], # N Korea's region is East Asia, but we don't have that, so using SE Asia instead since it's in that super region
                                  flush_imp_ratio = sani.cw.reg[reg == "se_asia", unique(flush_imp_ratio)])]
sani_reports[iso3 == "SLB", `:=` (latrine_imp_ratio = sani.cw.reg[reg == "se_asia", unique(latrine_imp_ratio)], # Solomon Island's region is Oceania --> using SE Asia; see above
                                  flush_imp_ratio = sani.cw.reg[reg == "se_asia", unique(flush_imp_ratio)])]
# split ambiguous strings
sani_reports[, `:=` (latrine_split_imp = latrine_cw*latrine_imp_ratio, # proportion of latrine_cw that is improved
                     flush_split_imp = flush_cw*flush_imp_ratio)] # proportion of flush_cw that is flush_imp
sani_reports[is.na(latrine_split_imp), latrine_split_imp := 0]
sani_reports[is.na(flush_split_imp), flush_split_imp := 0]
# now generate the modeling indicators
# wash_sanitation_piped is simply proportion using sewer/septic
sani_reports[, wash_sanitation_piped := septic+flush_imp_septic+flush_imp_sewer+flush_imp+flush_split_imp]
# wash_sanitation_imp_prop is proportion of people not using sewer/septic that use improved, so first need to get total proportion of improved
sani_reports[, imp_total := imp+latrine_imp+latrine_split_imp]
# then divide by 1 minus proportion using sewer/septic
sani_reports[, wash_sanitation_imp_prop := imp_total/(1-wash_sanitation_piped)]
# reshape long
sani_reports <- melt(sani_reports, id.vars = c("iso3","nid","underlying_nid","file_path","study","location_id","ihme_loc_id","location_name","year_start",
                                                "year_end","risk","sample_size","is_outlier","outlier_reason","cv_HH","underlying_field_citation_value"))
sani_reports <- sani_reports[variable %in% c("wash_sanitation_piped","wash_sanitation_imp_prop")] # keep only the modeling indicators
setnames(sani_reports, c("variable","value"), c("var","val"))
sani_reports <- sani_reports[order(location_id, year_start)] # sort by location & year
sani_reports[, val := round(val,10)] # in case some values are 1.000000000000000004. truncating them at 10 decimal points
# calculate SE (Wilson score interval)
sani_reports[, standard_error := (1/(1+(1.96^2/sample_size)))*sqrt(((val*(1-val))/sample_size)+((1.96^2)/(4*sample_size^2)))]
# clean up
sani_reports[, `:=` (iso3 = NULL, cv_microdata = 0, year_id = year_start, year_start_orig = year_start, year_end_orig = year_end, 
                      age_start = 0, age_end = 125, age_group_id = 22, sex = "Both", sex_id = 3)][, `:=` (year_start = year_id, year_end = year_id)]
# save
out.file <- paste0("sani_reports_prepped_", format(Sys.time(), "%m%d%y"), ".csv")
write.csv(sani_reports, file.path(report.dir, out.file), row.names = FALSE)

## water treatment #####
treat_reports <- wash_reports[risk == "w_treat", .(nid, underlying_nid, file_path, study, location_id, ihme_loc_id, location_name, year_start, year_end,
                                                   risk, measure, val, sample_size, is_outlier, cv_HH, underlying_field_citation_value)]
# reshape wide (so each row is a unique NID-location)
treat_reports <- dcast.data.table(treat_reports, ... ~ measure, value.var = "val")
# calculate proportion who do not use any of these treatment types
treat_reports[, wash_no_treat := 1-(w_bleach+w_boil+w_filter+w_solar)]
# calculate proportion of those who DO use treatments that use boil or filter
treat_reports[, wash_filter_treat_prop := (w_boil+w_filter)/(w_bleach+w_boil+w_filter+w_solar)]
# reshape long
treat_reports <- melt(treat_reports, id.vars = c("nid","underlying_nid","file_path","study","location_id","ihme_loc_id","location_name","year_start",
                                                 "year_end","risk","sample_size","is_outlier","cv_HH","underlying_field_citation_value"))
treat_reports <- treat_reports[variable %in% c("wash_no_treat","wash_filter_treat_prop")] # keep only the modeling indicators
setnames(treat_reports, c("variable","value"), c("var","val"))
treat_reports <- treat_reports[order(location_id, year_start)] # sort by location & year
# calculate SE (Wilson score interval)
treat_reports[, standard_error := (1/(1+(1.96^2/sample_size)))*sqrt(((val*(1-val))/sample_size)+((1.96^2)/(4*sample_size^2)))]
# clean up
treat_reports[, `:=` (cv_microdata = 0, year_id = year_start, year_start_orig = year_start, year_end_orig = year_end, 
                      age_start = 0, age_end = 125, age_group_id = 22, sex = "Both", sex_id = 3)][, `:=` (year_start = year_id, year_end = year_id)]
# save
out.file <- paste0("w_treat_reports_prepped_", format(Sys.time(), "%m%d%y"), ".csv")
write.csv(treat_reports, file.path(report.dir, out.file), row.names = FALSE)
