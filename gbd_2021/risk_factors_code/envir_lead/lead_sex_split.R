# 7/22/20
# sex split for lead

rm(list=ls())

# load packages
library(data.table)
library(tidyr)
library(dplyr)
library(reshape2)
library(msm)
library(openxlsx)
library(binom)
library(mrbrt001, lib.loc = "FILEPATH")

# set custom functions
"%unlike%" <- Negate("%like%")
"%ni%" <- Negate("%in%")

# load shared functions
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")

# set bundle, bundle version, and decomp step
bundle_id <- 4739
b_version <- 29981
decomp_step <- "iterative"

# read in bundle data
data <- get_bundle_version(bundle_version_id = b_version, fetch = "all")
setnames(data, "val", "mean")
data[, standard_error := standard_deviation/(sqrt(sample_size))]

if ("note_modeler" %ni% names(data)) {
  data[, note_modeler := as.character()]
}

### SEX SPLIT ------------------------------------------------------------------------------------------

# format the data for sex ratio model
sex_specific <- data[sex != "Both"]
sex_specific[, index := .GRP, by = .(nid, location_id, year_start, year_end, age_start, age_end)]

link <- unique(sex_specific[, .(nid, index)])

ss_long <- sex_specific[, .(index, sex, mean, standard_error)]
ss <- dcast(melt(ss_long, id.vars = c("index", "sex")), index~variable+sex, fun.aggregate = sum)
ss <- merge(ss, link, by = "index", all.x = T)
setDT(ss)

ss <- ss[mean_Female > 0 & mean_Male > 0] # will be running model in log space so can't have observations of 0

# calculate the sex ratios in the sex specific data sources
calc_sex_ratios <- function(dt){
  ratio_dt <- copy(dt)
  ratio_dt[, `:=` (ratio = mean_Female/mean_Male,
                   ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
  ratio_dt[, log_ratio := log(ratio)]
  ratio_dt$log_se <- sapply(1:nrow(ratio_dt), function(i) {
    mean_i <- ratio_dt[i, "ratio"]
    se_i <- ratio_dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  return(ratio_dt)
}

mrbrt_sex_dt <- calc_sex_ratios(ss)

# run MRBRT model
ss_data <- MRData()
ss_data$load_df(
  data = mrbrt_sex_dt, col_obs = "log_ratio", col_obs_se = "log_se",
  col_covs = list(), col_study_id = "nid"
)

ss_model <- MRBRT(
  data = ss_data,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE)
  )
)

ss_model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

# predict & get draws
ss_pred_df <- data.frame(intercept = 1)

ss_pred_data <- MRData()
ss_pred_data$load_df(
  data = ss_pred_df,
  col_covs = list("intercept")
)

ss_pred <- ss_model$predict(data = ss_pred_data)

ss_samples <- ss_model$sample_soln(sample_size = 1000L)
ss_draws <- ss_model$create_draws(
  data = ss_pred_data,
  beta_samples = ss_samples[[1]],
  gamma_samples = ss_samples[[2]],
  random_study = TRUE
)
ss_draws <- data.table(ss_draws)
setnames(ss_draws, paste0("V",1:1000), paste0("draw_",0:999)) # rename columns

# define sex split functions
get_row <- function(n, dt, pop_dt){
  row_dt <- copy(dt)
  row <- row_dt[n]
  row[age_start>=.999, age_end := ceiling(age_end)]
  pops_sub <- pop_dt[location_id == row[, location_id] & as.integer(year_id) == row[, as.integer(midyear)] &
                       age_group_years_start >= row[, age_start]][age_group_years_end <= row[, age_end] | age_group_years_end==min(age_group_years_end), ]
  agg <- pops_sub[, .(pop_sum = sum(population, na.rm = T)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}

split_data <- function(input_dt, input_draws) {
  
  print("initializing...")
  dt <- copy(input_dt)
  draws <- copy(input_draws)
  
  nosplit_dt <- dt[sex %in% c("Male", "Female")]
  tosplit_dt <- dt[sex == "Both"]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  tosplit_dt[is.na(sample_size), sample_size := (mean*(1-mean))/(standard_error^2)]

  draw.cols <- paste0("draw_", 0:999)
  draws[, (draw.cols) := lapply(.SD, exp), .SDcols = draw.cols]
  ratio_mean <- round(draws[, rowMeans(.SD), .SDcols = draw.cols], 2)
  ratio_se <- round(draws[, apply(.SD, 1, sd), .SDcols = draw.cols], 2)
  
  print("getting pops...")
  pops <- get_population(location_id = tosplit_dt[, unique(location_id)],  year_id = tosplit_dt[, unique(midyear)],  sex_id = 1:3,
                         gbd_round_id = 7, decomp_step = "iterative", age_group_id = 50:147, single_year_age = T)
  pops <- rbind(pops, get_population(location_id = tosplit_dt[, unique(location_id)],  year_id = tosplit_dt[, unique(midyear)], sex_id = 1:3,
                                     gbd_round_id = 7, decomp_step = "iterative", age_group_id = c(2,3,388,389,238)))
  
  dMeta <- merge(data.frame(age_group_id = c(2,3,388,389,238,50:147),
                            age_group_years_start = c(0, 0.01917808, 0.07671233, 0.5, 1:99),
                            age_group_years_end = c(0.01917808, 0.07671233, 0.5, 1:100)),
                 data.frame(sex_id = 1:3, sex = c("Male", "Female", "Both"), stringsAsFactors = F),
                 all = T)
  
  pops <- merge(pops, dMeta, by = c("age_group_id", "sex_id"), all.x=T)
  setDT(pops)
  
  print("splitting...")
  tosplit_dt <- rbindlist(lapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pop_dt = pops)))
  tosplit_dt[, merge := 1]
  draws[, merge := 1]
  split_dt <- merge(tosplit_dt, draws, by = "merge", allow.cartesian = T)
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draw.cols, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  
  z <- qnorm(0.975)
  
  print("cleaning up male_dt...")
  male_dt <- copy(split_dt)
  male_dt[mean == 0, sample_size := sample_size * male_N/both_N]
  male_dt[mean == 0 & measure == "prevalence", male_standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  male_dt[mean == 0 & measure == "incidence", male_standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  male_dt[is.na(male_standard_error), male_standard_error := sqrt((standard_error^2)*both_N/male_N)]
  
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA,
                  sample_size = NA, uncertainty_type_value = NA, sex = "Male", crosswalk_parent_seq = seq,
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", ratio_se, ")"))]
  male_dt[, variance := standard_error^2]

  print("cleaning up female_dt...")
  female_dt <- copy(split_dt)
  female_dt[mean == 0, sample_size := sample_size * female_N/both_N]
  female_dt[mean == 0 & measure == "prevalence", female_standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  female_dt[mean == 0 & measure == "incidence", female_standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  female_dt[is.na(female_standard_error), female_standard_error := sqrt((standard_error^2)*both_N/female_N)]
  
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA,
                    sample_size = NA, uncertainty_type_value = NA, sex = "Female", crosswalk_parent_seq = seq,
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", ratio_se, ")"))]
  female_dt[, variance := standard_error^2]

  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt), fill = T)

  print("done")
  return(total_dt)
}

# run the sex split
predict_sex <- split_data(data, ss_draws)
# clean up
predict_sex[, `:=` (age_range = age_end - age_start, year_mid = (year_start + year_end)/2)]
predict_sex[, gbd_year := round(year_mid/5, digits = 0) * 5][year_mid>=2016.5 & year_mid<2018, gbd_year := 2017][year_mid>=2018, gbd_year := 2019]
predict_sex[sex == "Male", sex_id := 1][sex == "Female", sex_id := 2]
# save
write.csv(predict_sex, "FILEPATH/lead_sex_split.csv", row.names = FALSE)
