#################################################################################################
#' Purpose: Estimate Sequela for Congenital Syphilis Latest
#weight cases from parent model by proportion symptomatic, age group, duration,
#& proportion of each sequela among symptomatic.the remainder are the asymptomatic cases
#################################################################################################
#SETUP
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
user <- Sys.info()["user"]
source("FILEPATH")
library("openxlsx")
library("data.table")
pacman::p_load(data.table, openxlsx, ggplot2, dplyr)

#SOURCE FUNCTIONS-----------------------------------------------------------------------------------------
source(paste0(h_root,"FILEPATH"))
source_shared_functions(functions = c("get_draws"))

#ARGS & DIRS
cat("prepping arguments")
args <- commandArgs(trailingOnly = TRUE)
loc_id <- as.numeric(args[1])

#MEIDS
asymp_meid <- as.numeric(args[2])

modinf_early_meid <- as.numeric(args[3])
sdis_early_meid <- as.numeric(args[4])

sdis_late_meid <- as.numeric(args[5])
neuro_late_meid <- as.numeric(args[6])
unihl_late_meid <- as.numeric(args[7])
interk_late_meid <- as.numeric(args[8])

out_dir <- as.character(args[9])
draws <- paste0("draw_", 0:999)

#EARLY SEQUELA PROPORTIONS & DURATION
early_symp_prop <- 0.52

early_modinf_prop <- 0.35
early_sdis_prop <- 0.25
early_seq_sum <- 0.35+0.25

early_duration <- 4/52

#LATE SEQUELA PROPORTIONS & DURATION
late_motorcog_prop <- 0.20
late_sdis_prop <- 0.31
late_unihl_prop <- 0.06
late_vision_prop <- 0.39

late_duration <-  1

#TREATMENT DATASETS
presumpt_gradient <- data.table(read.csv(file = paste0(out_dir, "FILEPATH")))
early_gradient <- data.table(read.csv(file = paste0(out_dir, "FILEPATH")))

pre_treatment <- unique(presumpt_gradient[location_id == loc_id ,c("pct_treated","pct_untreated" ,"year_id")])
early_treatment <- unique(early_gradient[location_id == loc_id, c("pct_treated","pct_untreated" ,"year_id")])

#GET & FORMAT DRAWS OF PARENT MODEL---------------------------------------------------------------------------------------------
cat("getting draws")

#get draws for a given location (and all of its ages, years, and both sexes)
#10 est yrs * 2 sex * 28 age groups = 560 rows
draws_dt <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 10498, source = "epi", measure_id = 5,
                      location_id = loc_id, status = "best", gbd_round_id = 7, decomp_step = "iterative")

#format draws
draws_dt <- draws_dt[!(age_group_id %in% c(27))]
draws_dt[ ,c("model_version_id", "modelable_entity_id") := NULL]
id_col_names <- c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id")

#specify age "groups"
birth_week1 <- c(164, 2)
early_ages <- c(3, 388:389, 238)
late_ages <- c(6:20, 30:34, 235)

#separate draws into dt's by age "groups"
birth_week1_dt <- draws_dt[age_group_id %in% birth_week1]
birth_week1_cols <- birth_week1_dt[ ,..id_col_names]

early_dt <- draws_dt[age_group_id %in% early_ages]
late_dt <- draws_dt[age_group_id %in% late_ages]

#estimate EARLY draws by treatment status
untreated_early_dt <- merge(early_dt, pre_treatment,  by = "year_id")
untreated_early_dt[ ,(draws) := lapply(.SD, function(x) x * untreated_early_dt$pct_untreated), .SDcols = draws]

#estimate LATE draws by treatment status
final_untreated <- merge(pre_treatment, early_treatment, by = "year_id")
final_untreated[ ,pct_untreated_final := pct_untreated.x * pct_untreated.y]
final_untreated <- final_untreated[ ,c("year_id", "pct_untreated_final")]

untreated_late_dt <- merge(late_dt, final_untreated, by = "year_id")
untreated_late_dt[ ,(draws) := lapply(.SD, function(x) x * untreated_late_dt$pct_untreated_final), .SDcols = draws]

#estimate 3rd treatment gradient for interstitial keratitis
ik_final_untreated <- merge(pre_treatment, early_treatment, by = "year_id")
ik_final_untreated[ ,pct_untreated_final := pct_untreated.x * pct_untreated.y * pct_untreated.y]
ik_final_untreated <- ik_final_untreated[ ,c("year_id", "pct_untreated_final")]

untreated_ik_dt <- merge(late_dt, ik_final_untreated, by = "year_id")
untreated_ik_dt[ ,(draws) := lapply(.SD, function(x) x * untreated_ik_dt$pct_untreated_final), .SDcols = draws]

#CALCULATE BIRTH & EARLY NEONATAL--------------------------------------------------------------------------------------
cat("estimating birth and early neonatal")

#100% of infants are asymptomatic at birth and in early neonatal stage, so zeros for all sequela
asymp_birth_week1 <- copy(birth_week1_dt)
zero_birth_week1 <- cbind(birth_week1_cols, birth_week1_dt[ ,lapply(.SD, function(x) x*0), .SDcols = draws])

###################################################
##CALCULATE EARLY SYNDROME OF CONGENITAL SYPHILIS##
###################################################
cat("estimating early syndrome")

#ESTIMATE LATE NEONATAL SYNDROME FOR ALL INDIVIDUALS
#late neonatal stage - no difference with treatment
#late neonatal stage only has 3 weeks but duration is expected to last for 4 weeks
#allocating 3 weeks duration to late neo and one additional week duration to 1-5 months
cat("estimating late neonatal stage")
wght_lateneo <- 3/103
ln_idcols <- early_dt[age_group_id == 3, ..id_col_names]
dt_lateneo <- early_dt[age_group_id == 3]

modinf_lateneo <- cbind(ln_idcols, dt_lateneo[ ,lapply(.SD, function(x) x * early_symp_prop * wght_lateneo * (3/52) * (early_modinf_prop/early_seq_sum)), .SDcols = draws])
sdis_lateneo <- cbind(ln_idcols, dt_lateneo[ ,lapply(.SD, function(x) x * early_symp_prop * wght_lateneo * (3/52) * (early_sdis_prop/early_seq_sum)), .SDcols = draws])
asymp_lateneo <- cbind(ln_idcols, dt_lateneo[ ,lapply(.SD, function(x) (x - (x * early_symp_prop * wght_lateneo * (3/52))) ), .SDcols = draws])
zero_lateneo <- cbind(ln_idcols, dt_lateneo[ ,lapply(.SD, function(x) x * 0 ), .SDcols = draws])

#ESTIMATE 1-5 MONTHS
#estimate 1-5 months untreated
#no values are needed for 1-5 months treated
#asymptomatic is original draws - untreated draws
cat("estimating 1-5 months")

wght_1_5mnths <- 22/103
idcols_1_5 <- untreated_early_dt[age_group_id == 388, ..id_col_names]
dt_1_5mnths <- untreated_early_dt[age_group_id == 388,]

modinf_1_5mnths <- cbind(idcols_1_5, dt_1_5mnths[ ,lapply(.SD, function(x) x * early_symp_prop * wght_1_5mnths * (5/52) * (early_modinf_prop/early_seq_sum)), .SDcols = draws])
sdis_1_5mnths <- cbind(idcols_1_5, dt_1_5mnths[ ,lapply(.SD, function(x) x * early_symp_prop * wght_1_5mnths * (5/52) * (early_sdis_prop/early_seq_sum)), .SDcols = draws])

#estimate asymptomatic
seq_1_5mnths <- cbind(idcols_1_5, dt_1_5mnths[ ,lapply(.SD, function(x) x * early_symp_prop * wght_1_5mnths * (5/52)), .SDcols = draws])
asymp_1_5mnths <- copy(early_dt[age_group_id == 388,])
asymp_1_5mnths <- cbind(idcols_1_5, asymp_1_5mnths[ ,..draws] - seq_1_5mnths[,..draws])

#estimate zero draws
zero_1_5months <- cbind(idcols_1_5, dt_1_5mnths[ ,lapply(.SD, function(x) x * 0 ), .SDcols = draws])

#ESTIMATE 6-11 MONTHS
#estimate 6-11 months untreated
#no values are needed for 6-11 months treated
#asymptomatic is original draws - untreated draws
cat("estimating 6-11 months")

wght_6_11mnths <- 26/103
idcols_6_11 <- untreated_early_dt[age_group_id == 389, ..id_col_names]
dt_6_11mnths <- untreated_early_dt[age_group_id == 389,]

modinf_6_11mnths <- cbind(idcols_6_11, dt_6_11mnths[ ,lapply(.SD, function(x) x * early_symp_prop * wght_6_11mnths * early_duration * (early_modinf_prop/early_seq_sum)), .SDcols = draws])
sdis_6_11mnths <- cbind(idcols_6_11, dt_6_11mnths[ ,lapply(.SD, function(x) x * early_symp_prop * wght_6_11mnths * early_duration * (early_sdis_prop/early_seq_sum)), .SDcols = draws])

#estimate asymptomatic
seq_6_11mnths <- cbind(idcols_6_11, dt_6_11mnths[ ,lapply(.SD, function(x) x * early_symp_prop * wght_6_11mnths * early_duration), .SDcols = draws])
asymp_6_11mnths <- copy(early_dt[age_group_id == 389,])
asymp_6_11mnths <- cbind(idcols_6_11, asymp_6_11mnths[ ,..draws] - seq_6_11mnths[,..draws])

#estimate zero draws
zero_6_11mnths <- cbind(idcols_6_11, dt_6_11mnths[ ,lapply(.SD, function(x) x * 0 ), .SDcols = draws])

#ESTIMATE 12-23 MONTHS
#estimate 12-23 months untreated
#no values are needed for 12-23 months treated
#asymptomatic is original draws - untreated draws
cat("estimating 12-23 months")

wght_12_23mnths <- 52/103
idcols_12_23 <- untreated_early_dt[age_group_id == 238, ..id_col_names]
dt_12_23mnths <- untreated_early_dt[age_group_id == 238, ]

modinf_12_23mnths <- cbind(idcols_12_23, dt_12_23mnths[ ,lapply(.SD, function(x) x * early_symp_prop * wght_12_23mnths * early_duration * (early_modinf_prop/early_seq_sum)), .SDcols = draws])
sdis_12_23mnths <- cbind(idcols_12_23, dt_12_23mnths[ ,lapply(.SD, function(x) x * early_symp_prop * wght_12_23mnths * early_duration * (early_sdis_prop/early_seq_sum)), .SDcols = draws])

#estimate asymptomatic
seq_12_23mnths <- cbind(idcols_12_23, dt_12_23mnths[ ,lapply(.SD, function(x) x * early_symp_prop * wght_12_23mnths * early_duration), .SDcols = draws])
asymp_12_23mnths <- copy(early_dt[age_group_id == 238,])
asymp_12_23mnths <- cbind(idcols_12_23, asymp_12_23mnths[ ,..draws] - seq_12_23mnths[,..draws])

#estimate zero draws
zero_12_23mnths <- cbind(idcols_12_23, dt_12_23mnths[ ,lapply(.SD, function(x) x * 0 ), .SDcols = draws])

#BIND THE EARLY DTS TOGETHER AND WRITE TO FILE
cat("creating zeros for early dts, age2+")
zero_2_older <- copy(draws_dt[age_group_id %in% late_ages,])
zero_2_older[ ,c(draws) := 0]

cat('binding early dts and writing to file')
early_modinf_dt <- rbind(zero_birth_week1, modinf_lateneo, modinf_1_5mnths, modinf_6_11mnths, modinf_12_23mnths, zero_2_older, fill =TRUE)
early_sdis_dt <- rbind(zero_birth_week1, sdis_lateneo, sdis_1_5mnths, sdis_6_11mnths, sdis_12_23mnths, zero_2_older, fill =TRUE)

write.csv(early_modinf_dt, "FILEPATH", row.names = FALSE)
write.csv(early_sdis_dt, "FILEPATH", row.names = FALSE)

###################################################
##CALCULATE LATE SYNDROME OF CONGENITAL SYPHILIS###
###################################################
cat("estimating late syndrome")

#YRS PER AGE GROUP
yrs_2_20 <- 18
yrs_5_20 <- 15

#ESTIMATE AGE 2-4 YEARS
#estimate 2-4 yrs untreated
#no values for treated individuals
#asymptomatic is original draws - untreated draws
cat("estimating 2-4 years")

sequela_2_4 = late_motorcog_prop
idcols_2_4 <- untreated_late_dt[age_group_id == 34, ..id_col_names]
dt_2_4 <- untreated_late_dt[age_group_id == 34, ]

motorcog_2_4 <- cbind(idcols_2_4, dt_2_4[,lapply(.SD, function(x) x * early_symp_prop * 3/yrs_2_20 * (late_motorcog_prop/sequela_2_4) * late_duration), .SDcols = draws])

#estimate asymptomatic
seq_2_4 <- copy(motorcog_2_4)
asymp_2_4 <- copy(late_dt[age_group_id == 34,])
asymp_2_4 <- cbind(idcols_2_4, asymp_2_4[ ,..draws] - seq_2_4[,..draws])

zero_2_4 <- cbind(idcols_2_4, dt_2_4[,lapply(.SD, function(x) x * 0), .SDcols = draws])

#ESTIMATE AGE 5-9 YEARS
#estimate 5-9 yrs untreated
#no values for treated individuals
#asymptomatic is original draws - treated draws
cat("estimating 5-9 years")

sequela_5_9 = late_motorcog_prop + late_sdis_prop
idcols_5_9 <- untreated_late_dt[age_group_id == 6, ..id_col_names]
dt_5_9 <- untreated_late_dt[age_group_id == 6,]

motorcog_5_9 <- cbind(idcols_5_9, dt_5_9[ ,lapply(.SD, function(x) x * early_symp_prop * 8/yrs_2_20 * (late_motorcog_prop/sequela_5_9) * late_duration), .SDcols = draws])
sdis_5_9 <- cbind(idcols_5_9, dt_5_9[,lapply(.SD, function(x) x * early_symp_prop * 5/yrs_5_20 * (late_sdis_prop/sequela_5_9) * late_duration), .SDcols = draws])

#estimate asymptomatic
seq_5_9 <- motorcog_5_9[,..draws] + sdis_5_9[,..draws]
asymp_5_9 <- copy(late_dt[age_group_id == 6,])
asymp_5_9 <- cbind(idcols_5_9, asymp_5_9[ ,..draws] - seq_5_9[,..draws])

zero_5_9 <- cbind(idcols_5_9, dt_5_9[,lapply(.SD, function(x) x * 0), .SDcols = draws])

#ESTIMATE AGE 10-EOL YEARS
#estimate 10-eol yrs untreated for non-hearing sequela, no values for treated individuals for these sequela
#estimate 10-eol yrs for HEARING regardless of treatment
#asymptomatic...is original draws - symptomatic draws
cat("estimating 10 through EOL for all but IK")

ages_10_eol <- late_ages[!(late_ages %in% c(34,6))]
sequela_10_eol = late_motorcog_prop + late_sdis_prop + late_unihl_prop + late_vision_prop
idcols_10_eol <- untreated_late_dt[age_group_id %in% ages_10_eol, ..id_col_names]
dt_10_eol <- untreated_late_dt[age_group_id %in% ages_10_eol, ]

motorcog_10_eol <- cbind(idcols_10_eol, dt_10_eol[ ,lapply(.SD, function(x) x * early_symp_prop * (late_motorcog_prop/sequela_10_eol) * late_duration), .SDcols = draws])
sdis_10_eol <- cbind(idcols_10_eol, dt_10_eol[ ,lapply(.SD, function(x) x * early_symp_prop * (late_sdis_prop/sequela_10_eol) * late_duration), .SDcols = draws])

interk_10_14 <- copy(dt_10_eol[age_group_id == 7])
interk_10_14 <- cbind(interk_10_14[, ..id_col_names], interk_10_14[ ,lapply(.SD, function(x) x * early_symp_prop * (late_vision_prop/sequela_10_eol) * late_duration), .SDcols = draws])

unihl_10_eol <- copy(late_dt[age_group_id %in% ages_10_eol, ])
unihl_10_eol <- cbind(idcols_10_eol, unihl_10_eol[ ,lapply(.SD, function(x) x * early_symp_prop * (late_unihl_prop/sequela_10_eol) * late_duration), .SDcols = draws])

#ESTIMATE AGE 15-EOL FOR INTERK ONLY
ages_15_eol <- late_ages[!(late_ages %in% c(34,6,7))]
sequela_15_eol = copy(sequela_10_eol)
idcols_15_eol <- untreated_late_dt[age_group_id %in% ages_15_eol, ..id_col_names]
dt_15_eol <- untreated_ik_dt[age_group_id %in% ages_15_eol, ]

interk_15_eol <- cbind(idcols_15_eol, dt_15_eol[ ,lapply(.SD, function(x) x * early_symp_prop * (late_vision_prop/sequela_15_eol) * late_duration), .SDcols = draws])

interk_10_eol <- rbind(interk_10_14, interk_15_eol)
#estimate asymptomatic
#drop cases after age 20 to zero
seq_10_eol <- motorcog_10_eol[ ,..draws] + sdis_10_eol[ ,..draws] + interk_10_eol[ ,..draws] + unihl_10_eol[ ,..draws]
asymp_10_eol <- copy(late_dt[age_group_id %in% ages_10_eol, ])
asymp_10_eol <- cbind(idcols_10_eol, asymp_10_eol[ ,..draws] - seq_10_eol[ ,..draws])
asymp_10_eol[!(age_group_id %in% c(7,8)), c(draws) := 0]

#BIND THE DTS AND WRITE THEM TO FILE
cat("creating zeros for late dts, ages 0-2")
zero_2_younger <- copy(draws_dt[age_group_id %in% c(2, early_ages)])
zero_2_younger[ ,c(draws) := 0]

cat("binding late dts and writing them to file")
late_motorcog_dt <- rbind(zero_2_younger, motorcog_2_4, motorcog_5_9, motorcog_10_eol, fill =TRUE)
late_sdis_dt <- rbind(zero_2_younger, zero_2_4, sdis_5_9, sdis_10_eol, fill =TRUE )
late_unihl_dt <- rbind(zero_2_younger, zero_2_4, zero_5_9, unihl_10_eol, fill =TRUE)
late_interk_dt <- rbind(zero_2_younger, zero_2_4, zero_5_9, interk_10_14, interk_15_eol, fill =TRUE)

cat("binding asymptomatic dt and writing to file")
full_asymp_dt <- rbind(asymp_birth_week1, asymp_lateneo, asymp_1_5mnths, asymp_6_11mnths, asymp_12_23mnths, asymp_2_4, asymp_5_9, asymp_10_eol)

write.csv(late_motorcog_dt, "FILEPATH", row.names = F)
write.csv(late_sdis_dt, "FILEPATH", row.names = F)
write.csv(late_unihl_dt, "FILEPATH", row.names = F)
write.csv(late_interk_dt, "FILEPATH", row.names = F)
write.csv(full_asymp_dt, "FILEPATH", row.names = F)

