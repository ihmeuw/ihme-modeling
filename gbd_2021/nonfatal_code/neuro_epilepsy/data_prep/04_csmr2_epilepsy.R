rm(list= ls())

source("FILEPATH/get_bundle_version.R")
pacman::p_load(data.table, ggplot2, dplyr, plyr, stringr, openxlsx, gtools)

functions_dir <- "FILEPATH"
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_bundle_data", "get_bundle_version", "get_ids", "get_crosswalk_version", "save_crosswalk_version")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

date <- "2020_09_23"
flat_file_dir <- paste0("FILEPATH", date)
dt_w_csmr_data <- read.xlsx(paste0(flat_file_dir, "adjusted_data_global_age_sex_split_logit_xwalks_mkt_scn_outlier", date,".xlsx"))

dt_w_csmr <- copy(data.table(dt_w_csmr_data))

unique(dt_w_csmr$measure)

dt_red <- dt_w_csmr[measure != "mtspecific",]

table(dt_red[, sex])

sex_both <- dt_red[sex == "Both",]
sex_other <- dt_red[sex != "Both",]
sex_male <- copy(sex_both)
sex_female <- copy(sex_both)

sex_male[, sex := "Male"]
sex_male[, specificity := "age,sex"]
sex_female[, sex := "Female"]
sex_female[, specificity := "age,sex"]

#Binding the obervations back together
rebound <- do.call("rbind", list(sex_male, sex_female, sex_other, dt_w_csmr[measure == "mtspecific"]))

rebound[, note_sr := paste(note_SR, "|", note_sr)]
rebound[, note_SR := NULL]

write.xlsx(rebound, paste0("FILEPATH", date,"_iterative_277_mkt_scn_outlier_both_sex_fix.xlsx"), sheetName= 'extraction')
aa <- read.xlsx(paste0("FILEPATH", date,"_iterative_277_mkt_scn_outlier_both_sex_fix.xlsx"))
save_crosswalk_version(33689, paste0("FILEPATH", date,"_iterative_277_mkt_scn_outlier_both_sex_fix.xlsx"),
                       description = "mktscn 2000 outlier, crosswalked, global age, sex split data, 2020_09_25 logit xwalks, un-age-split parent fix, manual sex split of remission, mtexcess, and mtstandard obs from within bundle")

rebound <- data.table(read.xlsx(paste0("FILEPATH", date,"_iterative_277_age_split_parent_fix.xlsx")))
#Partially reduced CSMR data
years <- c(1990, 2000, 2010, 2020, 2021, 2022)

dt_w_csmr[, age_start := as.numeric(age_start)]
dt_w_csmr[, age_end := as.numeric(age_end)]
dt_w_csmr[, location_id := as.numeric(location_id)]

csmr_red <- dt_w_csmr[nid == 416752 & year_start %in% years & year_end %in% years, ]

not_mali_f <- dt_w_csmr[nid == 416752 & location_id != 211 & sex != "Female",]
mali_f <- dt_w_csmr[nid == 416752 & location_id == 211 & sex == "Female",]

ages <- sort(unique(not_mali_f[, age_start]))

ages_kept <- ages[c(1:5, seq(6, 24, by = 2))] 
ages_mali_f <- sort(unique(mali_f[, age_start]))

colnames(csmr_red)[!colnames(csmr_red) %in% colnames(rebound)]
colnames(rebound)[!colnames(rebound) %in% colnames(csmr_red)]

csmr_red <- csmr_red[age_start %in% c(ages_kept, ages_mali_f),]
csmr_red[, note_SR := NULL]
csmr_red[, X.2 := NULL]

rebound[group_review == "" & specificity == "age,sex" & group == "",  `:=` (group_review = 1, group = 1)] 

dt_full <- do.call(rbind, list(csmr_red, rebound))

dt_full[,c("case_name", "case_diagnostics", "case_definition") := NULL]
dt_full[,case_name := ""]
dt_full[,case_definition := ""]
dt_full[, case_diagnostics := ""]

date <- "2020_09_01"
write.xlsx(dt_full, paste0("FILEPATH", date, "_iterative_277_CSMR_reduced_num.xlsx"), sheetName= 'extraction')
save_crosswalk_version(33689, paste0("FILEPATH", date, "_iterative_277_CSMR_reduced_num.xlsx") ,
                       description = "crosswalked, global age, sex split data, 2020_09_01 logit xwalks, un-age-split parent fix, reduced CSMR (Every second age group post age 5) all step 3 Mali female CSMR, manual sex split of remission, mtexcess, and mtstandard obs from within bundle")

csmr_full <- copy(dt_w_csmr[nid == 416752,])
csmr_full[,note_SR := NULL]

colnames(csmr_full)[!colnames(csmr_full) %in% colnames(rebound)]
colnames(rebound)[!colnames(rebound) %in% colnames(csmr_full)]

dt_all <- rbind(csmr_full, rebound)

dt_all[,c("case_name", "case_diagnostics", "case_definition") := NULL]
dt_all[,case_name := ""]
dt_all[,case_definition := ""]
dt_all[, case_diagnostics := ""]

write.xlsx(dt_all, paste0("FILEPATH", date, "_iterative_277_CSMR_all.xlsx"), sheetName= 'extraction')
save_crosswalk_version(33689, paste0("FILEPATH", date, "_iterative_277_CSMR_all.xlsx") ,
                       description = "crosswalked, global age, sex split data, 2020_09_01 logit xwalks, un-age-split parent fix, all CSMR, manual sex split of remission, mtexcess, and mtstandard obs from within bundle")

