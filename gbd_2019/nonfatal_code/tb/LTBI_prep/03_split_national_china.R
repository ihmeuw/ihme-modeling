## EMPTY THE ENVIRONMENT
rm(list = ls())

## SET UP FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  j <-"J:/"
  h <-"H:/"
  k <-"K:/"
} else {
  j <-"/home/j/"
  h <-paste0("homes/", Sys.info()[7], "/")
  k <-"/ihme/cc_resources/"
}

## LOAD FUNCTIONS
source(paste0(k, "libraries/current/r/get_outputs.R"))
source(paste0(k, "libraries/current/r/get_population.R"))
source(paste0(k, "libraries/current/r/save_crosswalk_version.R"))
source(paste0(k, "libraries/current/r/get_location_metadata.R"))

library(ggplot2)
library(readxl)
library(writexl, lib.loc = paste0(j, "temp/jledes2/libraries"))
library(data.table)

#############################################################################################
###                                    GATHER DATA                                        ###
#############################################################################################

## ESTABLISH DIRECTORIES
date        <- "2020_07_27"
decomp_step <- "iterative"
input_dir   <- paste0(j, "WORK/04_epi/01_database/02_data/tb/1175/GBD_2020/crosswalk/ltbi/inputs/", date, "/")
dir_gbd19   <- paste0(j, "WORK/12_bundle/tb/711/01_input_data/")

## GET DATA
national <- as.data.table(read_excel(paste0(dir_gbd19, "04_crosswalk/", date, "/", decomp_step, "_bcg_adj_data_logit_rm_dups.xlsx")))
national <- national[nid == 121868]
national[, sample_size := NULL]

## STUDY DATA
study_data <- as.data.table(read_excel(paste0(dir_gbd19, "02_raw_data/china_subnat_redist/china_national_pattern.xlsx")))
study_data <- unique(study_data[, .(nid, location_id, sex, age_start, age_end, sample_size)])
national   <- merge(national, study_data)

## GET LOCATION METADATA
locs <- get_location_metadata(location_set_id = 35)
locs <- locs[, .(location_id, ihme_loc_id)]

## GET SUBNATIONAL PREVALENCE COUNTS
subnat <- get_outputs(topic = "cause", 
                      cause_id     = 954,
                      location_id  = locs[ihme_loc_id %like% "CHN_", location_id],
                      age_group_id = c(1, 6:21), 
                      year_id      = 2000, 
                      sex_id       = 1:2, 
                      measure_id   = 5,
                      gbd_round_id = 5)

## CLEAN
subnat <- subnat[, .(location_id, sex, age_group_name, age_group_id, val)]
subnat <- subnat[order(location_id, sex, age_group_id)]
subnat <- merge(subnat, locs)
setnames(subnat, old = "val", new = "cases")

## FIX AGE GROUPS
subnat[, c("age_start", "age_end") := tstrsplit(age_group_name, " to ")]
subnat[age_group_id == 1, `:=`  (age_start = "0",  age_end = "4")]
subnat[age_group_id == 21, `:=` (age_start = "80", age_end = "99")]
subnat[, `:=` (age_group_name = NULL, year_start = 2000, year_end = 2000)]
subnat[, `:=` (age_start = as.integer(age_start), age_end = as.integer(age_end))]

#############################################################################################
###                                   SPLIT CASES                                         ###
#############################################################################################

## AGGREGATE COUNTS BY AGE-SEX COMBINATION AND CREATE PROPORTIONS
agg_data <- subnat[, .(agg_cases = sum(cases)), by = c("year_start", "year_end", "sex", "age_start", "age_end")]
subnat   <- merge(subnat, agg_data, by = c("year_start", "year_end", "sex", "age_start", "age_end"))
subnat[, prop := cases / agg_cases]

## CREATE NATIONAL CASES
national[, cases := mean*sample_size]

## PREP TO MERGE NATIONAL CASES 
national_sub <- national[, .(sex, age_start, age_end, cases)]
setnames(national_sub, old = "cases", new = "national_cases")

## SPLIT CASES
subnat <- merge(subnat, national_sub, by = c("sex", "age_start", "age_end"))
subnat[, new_cases := national_cases*prop]

## CLEAN
subnat <- subnat[, .(location_id, ihme_loc_id, year_start, year_end, sex, age_group_id, age_start, age_end, new_cases)]
setnames(subnat, old = "new_cases", new = "cases")

#############################################################################################
###                                 SPLIT SAMPLE SIZE                                     ###
#############################################################################################

## GET POPULATION
pops <- get_population(age_group_id = unique(subnat$age_group_id), 
                       location_id  = unique(subnat$location_id), 
                       year_id      = 2000, 
                       sex_id       = 1:2, 
                       gbd_round_id = 5)

## PREP FOR MERGE
pops[sex_id == 1, sex := "Male"][sex_id == 2, sex := "Female"]
pops[, `:=` (sex_id = NULL, run_id = NULL, year_id = NULL)]

## MERGE POPULATIONS
subnat <- merge(subnat, pops, by = c("location_id", "sex", "age_group_id"))

## AGGREGATE POPULATIONS BY AGE-SEX COMBINATION AND CREATE PROPORTIONS
agg_data <- subnat[, .(agg_pop = sum(population)), by = c("year_start", "year_end", "sex", "age_start", "age_end")]
subnat   <- merge(subnat, agg_data, by = c("year_start", "year_end", "sex", "age_start", "age_end"))
subnat[, prop := population / agg_pop]

## PREP TO MERGE NATIONAL SAMPLE SIZE TO SPLIT
national_sub <- national[, .(sex, age_start, age_end, sample_size)]
setnames(national_sub, old = "sample_size", new = "national_sample_size")

## SPLIT SAMPLE SIZE
subnat <- merge(subnat, national_sub, by = c("sex", "age_start", "age_end"))
subnat[, sample_size := national_sample_size*prop]

## CLEAN
subnat <- subnat[, .(location_id, ihme_loc_id, year_start, year_end, sex, age_start, age_end, cases, sample_size)]
subnat <- subnat[order(ihme_loc_id, sex, age_start)]
subnat[, mean := cases/sample_size]

#############################################################################################
###                                 PLOT THE SPLIT                                        ###
#############################################################################################

## GET LOCATION METADATA
locs   <- get_location_metadata(location_set_id = 35)
locs   <- locs[, .(location_id, location_name)]
subnat <- merge(subnat, locs)

## CREATE PLOTTING TABLE
plot_data <- copy(subnat)
plot_data <- plot_data[sex == "Male"]

## CREATE UPPER AND LOWER
plot_data[, se := sqrt(mean*(1-mean)/sample_size)]
plot_data[, lower := mean - 1.96*se][, upper := mean + 1.96*se]
plot_data[lower < 0, lower := 0][upper > 1, upper := 1]

## PLOT
ggplot(data = plot_data, aes(x = (age_start+age_end)/2, y = mean)) +
  geom_point(size = 1, alpha = 0.1) + geom_errorbarh(aes(xmin = age_start, xmax = age_end)) +
  geom_errorbar(aes(ymin = lower, ymax = upper), size = 0.5, alpha = 0.25, width = 0.1) +
  facet_wrap(~location_name) + labs(y = "Prevalence", x = "Age") + ylim(0, 0.80)+
  theme_bw()

#############################################################################################
###                               UPLOAD CROSSWALK VERSION                                ###
#############################################################################################

## TEMPLATE
template <- copy(national)
template[, `:=` (cases = NULL, sample_size = NULL, mean = NULL, location_id = NULL, ihme_loc_id = NULL)]
template[, `:=` (location_name = NULL, lower = NULL, upper = NULL, standard_error = NULL)]

## MERGE TEMPLATE
subnat <- merge(subnat, template, by = c("year_start", "year_end", "sex", "age_start", "age_end"))
subnat[, specificity := paste0(specificity, "; Split to subnational level using GBD2017 subnat rates and population")]

## COMPUTE UNCERTAINTY
subnat[, standard_error := sqrt((mean*(1-mean))/sample_size)]
subnat[, lower := mean - 1.96*standard_error]
subnat[, upper := mean + 1.96*standard_error]
subnat[lower < 0, lower := 0][upper > 1, upper := 1]

## APPEND TO DATA
all <- as.data.table(read_excel(paste0(dir_gbd19, "04_crosswalk/", date, "/", decomp_step, "_bcg_adj_data_logit_rm_dups.xlsx")))
all <- all[nid != 121868]
all <- rbind(all, subnat)
all <- all[order(origin_seq)]

## SAVE
#all <- all[nid != 279246]
all[, step2_location_year := as.character(step2_location_year)]
all[nid == 121868, step2_location_year := "split national China data point to subnational level using GBD2017 rates and population"]

## COPY 2000 CHN TO 2010 TO INCREASE TIME WINDOW
add <- all[nid == 121868]
add[, `:=` (year_start=2010, year_end=2010)]
all <- rbind(all, add)

writexl::write_xlsx(list(extraction = all), path = paste0(dir_gbd19, "04_crosswalk/", date, "/", decomp_step, "_bcg_adj_data_logit_rm_dups_china_split.xlsx"))

## SAVE CROSSWALK VERSION
bundle_metadata        <- fread(paste0(input_dir, decomp_step, "_bundle_metadata.csv"))
cross_walk_description <-  paste0("Updated risk curve; removed albania data; split national china 2000; dropped rural china/duplicates; ", 
                                 "1 TU adjustment at induration level (1 mm diff), removed 10 tu; sex split (age dummies), ",
                                 "bcg (network analysis) in logit space")
save_xwalk_result      <- save_crosswalk_version(bundle_version_id = bundle_metadata$bundle_version_id,
                                                 data_filepath     = paste0(dir_gbd19, "04_crosswalk/", date, "/", decomp_step, "_bcg_adj_data_logit_rm_dups_china_split.xlsx"),
                                                 description       = cross_walk_description)

## SAVE CROSSWALK RESULT METADATA
save_xwalk_result[, filepath := paste0(dir_gbd19, "04_crosswalk/", date, "/", decomp_step, "_bcg_adj_data_logit_rm_dups_china_split.xlsx")]
fwrite(save_xwalk_result, file = paste0(input_dir, decomp_step, "xwalk_metadata.csv"), row.names = F)

#############################################################################################
###                                      OUTLIER UPLOAD                                   ###
#############################################################################################

## OUTLIER
add <- copy(all)
add <- add[nid == 121868]
add[, `:=` (year_start=2010, year_end=2010)]

## APPEND
all <- rbind(all, add)

## OTHER OUTLIERS
all <- all[nid != 281347]
all <- all[!((nid == 279265) & (mean > 0.5))]
all <- all[!((nid == 279348) & (age_start >15))]
all <- all[!((nid %in% c(110300, 52110)) & (age_start > 75))]
all <- all[nid != 279240]
all <- all[!((nid %in% c(110300, 52110)) & (age_start > 65))]

## OUTPUT XLSX
writexl::write_xlsx(list(extraction = all), path = paste0(dir_gbd19, "04_crosswalk/", date, "/", decomp_step, "_bcg_adj_data_china_split_outliers.xlsx"))

## UPLOAD
cross_walk_description <-  paste0("new BCG xwalk; with outliers from GBD2019")
save_xwalk_result      <- save_crosswalk_version(bundle_version_id = bundle_metadata$bundle_version_id,
                                                 data_filepath     = paste0(dir_gbd19, "04_crosswalk/", date, "/", decomp_step, "_bcg_adj_data_china_split_outliers.xlsx"),
                                                 description       = cross_walk_description)

## SAVE CROSSWALK RESULT METADATA
save_xwalk_result[, filepath := paste0(dir_gbd19, "04_crosswalk/", date, "/", decomp_step, "_bcg_adj_data_china_split_outliers.xlsx")]
fwrite(save_xwalk_result, file = paste0(input_dir, decomp_step, "xwalk_metadata_v2.csv"), row.names = F)



# 
# 
# 
# test <- as.data.table(read_excel("/home/j/WORK/12_bundle/tb/711/01_input_data/04_crosswalk/2019_09_23/step3_bcg_adj_data_logit_rm_dups_china_split_update.xlsx"))
# add  <- test[nid==121868]
# add[, `:=` (year_start=2010, year_end=2010)]
# 
# test <- rbind(test, add)
# test[seq == "TRUE", seq2 := origin_seq]
# test[,seq := NULL]
# setnames(test, "seq2", "seq")
# 
# write.csv(test, file = "/home/j/WORK/12_bundle/tb/711/01_input_data/04_crosswalk/2019_09_23/step3_bcg_adj_data_logit_rm_dups_china_split_year_shift.csv", row.names = F, na= "")
# writexl::write_xlsx(list(extraction = test), path = "/home/j/WORK/12_bundle/tb/711/01_input_data/04_crosswalk/2019_09_23/step3_bcg_adj_data_logit_rm_dups_china_split_year_shift.xlsx")
# 
# test <- fread("/snfs1/WORK/12_bundle/tb/711/01_input_data/04_crosswalk/2019_09_23/step3_bcg_adj_data_logit_rm_dups_china_split_year_shift.csv")
# test <- test[!((nid == 279265) & (mean > 0.5))]
# test <- test[!((nid == 279348) & (age_start >15))]
# test <- test[!((nid %in% c(110300, 52110)) & (age_start > 75))]
# write.csv(test, "/snfs1/WORK/12_bundle/tb/711/01_input_data/04_crosswalk/2019_10_21/step3_china_split_year_shift_rm_PHL_US_IND.csv", row.names = F, na="")
# 
# test <- fread("/snfs1/WORK/12_bundle/tb/711/01_input_data/04_crosswalk/2019_10_21/step3_china_split_year_shift_rm_PHL_US_IND.csv")
# test <- test[nid != 279240]
# test <- test[!((nid %in% c(110300, 52110)) & (age_start > 65))]
# write.csv(test, "/snfs1/WORK/12_bundle/tb/711/01_input_data/04_crosswalk/2019_10_21/step3_china_split_year_shift_rm_PHL_US_IND_ITA.csv", row.names = F, na="")
# 
# save_crosswalk_version(bundle_version_id = bundle_metadata$bundle_version_id,
#                        data_filepath     = "/snfs1/WORK/12_bundle/tb/711/01_input_data/04_crosswalk/2019_10_21/step3_china_split_year_shift_rm_PHL_US_IND_ITA.xlsx",
#                        description       = cross_walk_description)
