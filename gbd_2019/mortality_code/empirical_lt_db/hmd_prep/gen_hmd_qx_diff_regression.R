## Regress at 75-79, 80-84, 85-89
## Restrict to national only for stability
## Generate relationship and MAD estimator
## Plot points and MAD band

## Try on HMD lifetables first
## 75-79: Soft relationship, lots more in location specific that pop up.
## 80-84: probably biggest relationship and movement
## 85-89: Biggest chance for cutting on both ends.
library(ltcore, lib = "/FILEPATH/r-pkg") # For logit function

## Generate HMD regression coefficients
source("FILEPATH/empirical-lt/functions/import_hmd_lts.R")
hmd_data <- import_hmd_lts()

setorder(hmd_data, iso3, year, sex, age)
hmd_data[, qx_diff := (qx - shift(qx, type = "lag")) / shift(qx, type = "lag"), by = c("iso3", "year", "sex")]

run_hmd_regression <- function(hmd_data, sel_age, sel_sex) {
  regress_data <- hmd_data[sex == sel_sex & age == sel_age]
  reg_results <- lm(qx_diff ~ qx, data = regress_data)
  reg_results <- data.table(sex = sel_sex,
                            age = sel_age,
                            intercept = reg_results$coefficients["(Intercept)"],
                            qx_coef = reg_results$coefficients["qx"])
  return(reg_results)
}

compiled_regressions <- list()
iter <- 1
for(age in seq(75, 90, 5)) {
  for(sex in c("male", "female")) {
    compiled_regressions[[iter]] <- run_hmd_regression(hmd_data, age, sex)
    iter <- iter + 1
  }
}

compiled_regressions <- rbindlist(compiled_regressions)
write_csv(compiled_regressions, "FILEPATH/empirical_lt/inputs/hmd_qx_diff_regression.csv")


## Generate regressions based on existing data -- log 45q15 and qx
used_data <- get_mort_outputs("life table empirical", "data", life_table_parameter_id = 3)
age_map <- setDT(get_age_map(type = "all"))
age_map <- age_map[, .(age_group_id, age = age_group_years_start)]
used_data <- merge(used_data, age_map, by = "age_group_id")
used_data[sex_id == 1, sex := "male"]
used_data[sex_id == 2, sex := "female"]
setnames(used_data, "mean", "qx")

# used_data <- merge(used_data, compiled_regressions, by = c("sex", "age"), all.x = T)
used_45q15 <- used_data[age_group_id == 199]
setnames(used_45q15, "qx", "mean_45q15")
used_data <- used_data[!age_group_id %in% c(1, 199)]
used_ids <- c("location_id", "ihme_loc_id", "year_id", "life_table_category_id", "sex_id", "nid", "underlying_nid", "source_type_id")
used_data <- merge(used_data, used_45q15[, .SD, .SDcols = c(used_ids, "mean_45q15")], by = used_ids)
setorderv(used_data, c(used_ids, "age"))
used_data[, qx_diff := (qx - shift(qx, type = "lag")) / shift(qx, type = "lag"), by = used_ids]
used_data[, log_qx_diff := log(qx_diff)]

run_log_45q15_regression <- function(used_data, sel_age, sel_sex) {
  regress_data <- used_data[sex == sel_sex & age == sel_age]
  reg_results <- lm(log_qx_diff ~ mean_45q15, data = regress_data)
  reg_results <- data.table(sex = sel_sex,
                            age = sel_age,
                            intercept = reg_results$coefficients["(Intercept)"],
                            qx_coef = reg_results$coefficients["mean_45q15"])
  return(reg_results)
}

used_regressions <- list()
iter <- 1
for(age in seq(75, 90, 5)) {
  for(sex in c("male", "female")) {
    used_regressions[[iter]] <- run_log_45q15_regression(used_data, age, sex)
    iter <- iter + 1
  }
}

used_regressions <- rbindlist(used_regressions)
write_csv(used_regressions, "FILEPATH/empirical_lt/inputs/log_diff_45q15_regression.csv")

plot_hmd_regression_data <- function(sel_age, sel_sex) {
  plot <- ggplot() +
            geom_point(data = used_data[age == sel_age & sex == sel_sex],
                       aes(x =  qx, y = qx_diff), alpha = .2, size = .6) +
            geom_abline(intercept = compiled_regressions[age == sel_age & sex == sel_sex, intercept],
                        slope = compiled_regressions[age == sel_age & sex == sel_sex, qx_coef]) +
            theme_minimal() +
            ggtitle(paste0("HMD qx regression: ", age, " ", sex))
  print(plot)
}

pdf("FILEPATH/temp/hmd_reg_plot.pdf", width = 10, height = 5.6)
for(age in seq(75, 90, 5)) {
  for(sex in c("male", "female")) {
    plot_hmd_regression_data(age, sex)
  }
}
dev.off()

plot_log_diff_45q15_regression_data <- function(sel_age, sel_sex) {
  plot_data <- used_data[age == sel_age & sex == sel_sex]
  plot_range <- range(plot_data$mean_45q15)
  reg_inter <- used_regressions[age == sel_age & sex == sel_sex, intercept]
  reg_coef <- used_regressions[age == sel_age & sex == sel_sex, qx_coef]
  pred_line <- data.table(mean_45q15 = seq(plot_range[1], plot_range[2], .01))
  pred_line[, pred_log_diff := reg_inter + (reg_coef * mean_45q15)]
  pred_line[, pred_diff := exp(pred_log_diff)]

  plot <- ggplot() +
            geom_point(data = plot_data,
                       aes(x =  mean_45q15, y = qx_diff), alpha = .2, size = .6) +
            geom_line(data = pred_line, aes(x = mean_45q15, y = pred_diff)) +
            theme_minimal() +
            ggtitle(paste0("Log qx diff vs. 45q15 normalized results: ", age, " ", sex))
  print(plot)
}

pdf("FILEPATH/temp/log_diff_45q15_plot_normalized.pdf", width = 10, height = 5.6)
for(age in seq(75, 90, 5)) {
  for(sex in c("male", "female")) {
    plot_log_diff_45q15_regression_data(age, sex)
  }
}
dev.off()



