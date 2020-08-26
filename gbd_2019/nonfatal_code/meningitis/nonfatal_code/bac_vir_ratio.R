#####################################################################################################################################################################################
#' @Title: 00_master - Master file for meningitis: see specific step files for descriptions.
#' @Author: 
#' @Description: Calculate bacterial vs viral meningitis ratio - needs to be updated per hospital data set
#####################################################################################################################################################################################
rm(list=ls())

pacman::p_load(stats, openxlsx, data.table)

# SOURCE FUNCTIONS --------------------------------------------------------
source(paste0(k, "current/r/get_bundle_version.R"))
source(paste0(k, "current/r/get_age_metadata.R"))

# USER SPECIFIED OPTIONS --------------------------------------------------
date <- format(Sys.Date(), "%Y_%m_%d")
gbd_round_year <- 2019
ds <- 'step4'

viral_bundle_id <- 833
# viral_bundle_version <- 9005
bacterial_bundle_id <- 28
bacterial_bundle_version <- 18884

out_dir <- # filepath
dir.create(out_dir, showWarnings = F)

# get age metadata
age.metadata <- get_age_metadata(age_group_set = 12, gbd_round_id = 6)
# combine <1 age groups
age.metadata <- age.metadata[age_group_id > 4]
under1 <- data.table(28, "under 1", 0, 2, NA, NA)
age.metadata <- rbind(age.metadata, under1, use.names = F)
age.metadata <- age.metadata[, c("age_group_years_start", "age_group_years_end", "age_group_id"), with = FALSE]
age.metadata <- data.table(age.metadata)
age.metadata <- age.metadata[,age_group_years_end:= (age_group_years_end - 1)]

# get viral clinical data CF2
dt <- read.csv("filepath")
dt <- data.table(dt)
dt <- merge(dt, age.metadata, by = "age_group_id")
setnames(dt, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
hospital_dt <- dt[estimate_id == 3] #inpatient
hospital_dt <- hospital_dt[, .(inc_viral = mean(mean)), 
                           by = c("sex_id", "age_start", "age_end")]
claims_dt <- dt[estimate_id == 17] #claims
claims_dt <- claims_dt[, .(cases = sum(cases), sample_size = sum(sample_size)), 
                       by = c("sex_id", "age_start", "age_end")]
claims_dt[, inc_viral := cases / sample_size]
viral_dt <- rbind(hospital_dt, claims_dt, fill = T)
viral_dt <- viral_dt[, .(inc_viral = mean(inc_viral)),
                     by = c("sex_id", "age_start", "age_end")]
viral_dt$sex <- NA_character_
viral_dt <- viral_dt[sex_id == 1, sex:='Male']
viral_dt <- viral_dt[sex_id == 2, sex:='Female']

# get bacterial meningitis data
dt <- get_bundle_version(bacterial_bundle_version, fetch = 'all', transform = T)
hospital_dt <- dt[clinical_data_type == 'inpatient']
hospital_dt <- hospital_dt[, .(inc_bacterial = mean(mean)), 
                           by = c("sex", "age_start", "age_end")]
claims_dt <- dt[clinical_data_type == 'claims']
claims_dt <- claims_dt[, .(cases = sum(cases), sample_size = sum(sample_size)), 
                       by = c("sex", "age_start", "age_end")]
claims_dt[, inc_bacterial := cases / sample_size]
bacterial_dt <- rbind(hospital_dt, claims_dt, fill = T)
bacterial_dt <- bacterial_dt[, .(inc_bacterial = mean(inc_bacterial)),
                             by = c("sex", "age_start", "age_end")]

cols <- names(bacterial_dt)[2:3]
bacterial_dt[,(cols) := round(.SD,1), .SDcols=cols]


# merge viral and bactieral
bac_vir_ratio_dt <- merge(viral_dt, bacterial_dt, 
                          by = c("sex", "age_start", "age_end"))
bac_vir_ratio_dt[, ratio := inc_viral/ inc_bacterial]
# duplicate age 0-1 to get neonatal groups
under1_dt <- copy(bac_vir_ratio_dt[age_start == 0])
late_neonatal <- copy(under1_dt[, `:=` (age_start = .01, age_end = .1)])
post_neonatal <- copy(under1_dt[, `:=` (age_start = .1, age_end = 0.999)])
bac_vir_ratio_dt[age_start == 0, age_end := 0.01]
bac_vir_ratio_dt <- rbindlist(list(late_neonatal, post_neonatal, bac_vir_ratio_dt))
# run LOWESS regression of ratio on age start
male_lowess <- lowess(x = bac_vir_ratio_dt[sex == "Male", age_start], 
                      y = bac_vir_ratio_dt[sex == "Male", ratio],
                      delta = 0, iter = 0, f = 0.8)
male_lowess_dt <- data.table(sex = "Male", 
                             age_start = male_lowess$x,
                             lowess_ratio = male_lowess$y)
female_lowess <- lowess(x = bac_vir_ratio_dt[sex == "Female", age_start], 
                        y = bac_vir_ratio_dt[sex == "Female", ratio],
                        delta = 0, iter = 0, f = 0.8)
female_lowess_dt <- data.table(sex = "Female", 
                               age_start = female_lowess$x,
                               lowess_ratio = female_lowess$y)
lowess_dt <- rbind(male_lowess_dt, female_lowess_dt)
bac_vir_ratio_dt <- merge(bac_vir_ratio_dt, lowess_dt, by = c("sex", "age_start"))

bac_vir_ratio_dt[, sex_id := ifelse(sex == "Male", 1, 2)]
age_map <- data.table(age_start = sort(bac_vir_ratio_dt[, unique(age_start)]),
                      age_group_id = c(2:20, 30:32, 235))
bac_vir_ratio_dt <- merge(bac_vir_ratio_dt, age_map, by = "age_start")
cols.remove <- c("age_start", "age_end", "sex", "inc_viral", "inc_bacterial", "lowess_ratio")
bac_vir_ratio_dt[, (cols.remove) := NULL]
setkeyv(bac_vir_ratio_dt, c("sex_id", "age_group_id"))
# write to input directory
write.csv(bac_vir_ratio_dt, paste0(out_dir, "bac_vir_ratio_", gbd_round_year, "_", ds, ".csv"))
