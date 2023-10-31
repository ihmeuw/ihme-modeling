# Purpose: Identify studies in which subjects were likely vaccinated

rm(list = ls())

library(openxlsx, plyr)

source(FILEPATH)

# Get the bundle version associated with the correct step 
b_id <- OBJECT
gbd_round_id <- 7
decomp_step <- "iterative"

orig_dt1 <- get_bundle_data(bundle_id = b_id, decomp_step = "iterative", gbd_round_id = 7)


# Read in the vaccine year of introduction data 
vac_intro <- readr::read_rds(FILEPATH)
vac_intro <- vac_intro[me_name == "vacc_hepb3", ]
vac_intro <- unique(vac_intro[, .(ihme_loc_id, location_id, cv_intro)])


# Merge the data sets together 
dt_all <- merge(dt_prev, vac_intro, by = "location_id")

# Create year of birth cols based on different permutations with year_start, year_end, age_start, and age_end 
dt_all[, `:=` (yob_ys_as = round(year_start - age_start, 0), 
               yob_ys_ae = round(year_start - age_end, 0), 
               yob_ye_as = round(year_end - age_start, 0), 
               yob_ye_ae = round(year_end - age_end, 0))]
head(dt_all)

dt_all[, `:=` (keep_ys_as = ifelse(yob_ys_as < cv_intro, 1, 0), 
               keep_ys_ae = ifelse(yob_ys_ae < cv_intro, 1, 0), 
               keep_ye_as = ifelse(yob_ye_as < cv_intro, 1, 0), 
               keep_ye_ae = ifelse(yob_ye_ae < cv_intro ,1, 0))]


keep_cols <- names(dt_all)[grepl("keep", names(dt_all))]
keep_cols
dt_all[, keep_total := rowSums(.SD), .SDcols = keep_cols]
head(dt_all)


dt_processing <- copy(dt_all)
dt_def_drop <- dt_processing[keep_total == 0, ]
nrow(dt_def_drop)
dt_processing <- dt_processing[keep_total == 1 | keep_total == 2 | keep_total == 3, ]
dt_processing[, `:=` (mean = (yob_ys_ae + yob_ye_as) / 2, sd = (yob_ye_as - yob_ys_ae) / 6 )]
dt_processing[, coverage := pnorm(cv_intro, mean = mean, sd = sd)]
dt_processing[, drop := ifelse(coverage <= 0.5, 1, 0 )]
nrow(dt_processing[drop == 1,]) # 41
dt_drop <- copy(dt_processing[drop == 1, ])
dt_drop_both <- rbind(dt_def_drop, dt_drop, fill = TRUE)
nrow(dt_drop_both)

outlier_seqs <- dt_drop_both[, unique(seq)]
dt_prev[seq %in% outlier_seqs, is_outlier := 1]

nrow(dt_prev[is_outlier == 1, ])
nrow(test[is_outlier == 1, ])
dt_prev[is_outlier == 1, length(unique(nid))]
test[is_outlier == 1, length(unique(nid))]


data_filepath <-  FILEPATH
write.xlsx(dt_prev, data_filepath, sheetName = "extraction")


description <- paste("outliered data for definitely drop cases and less than 50% (pnorm) study population not vaccinated")
decomp_step <- "iterative"

result <- upload_bundle_data(bundle_id = b_id, decomp_step = decomp_step, filepath = data_filepath, gbd_round_id = gbd_round_id)

bundle_version <- save_bundle_version(bundle_id = b_id, decomp_step = decomp_step, gbd_round_id = gbd_round_id)


# Stats

nrow(dt_all[keep_total == 4, ])
# There are 2453 rows of 2690 to keep

nrow(dt_all[keep_total == 0, ])
#There are 126 rows of data to definitely drop 


dt_rule <- dt_all[keep_total == 1 | keep_total == 2 | keep_total == 3]
# There are 111 rows where we need to apply some decision rule 


dt_rule[, id := .GRP, by = c("keep_ys_as", "keep_ys_ae", "keep_ye_as", "keep_ye_ae")]

dt_rule2 <- copy(dt_rule[, c("id", "keep_ys_as", "keep_ys_ae", "keep_ye_as", "keep_ye_ae")])
dt_rule2[, count := .N, by = "id"]


dt_all[is_outlier == 1, ]