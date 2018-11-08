library(readr)
## Generate HMD regression coefficients
source("empir_gen_funcs/R/import_hmd_lts.R")
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
write_csv(compiled_regressions, "hmd_qx_diff_regression.csv")

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

pdf("hmd_reg_plot.pdf", width = 10, height = 5.6)
for(age in seq(75, 90, 5)) {
  for(sex in c("male", "female")) {
    plot_hmd_regression_data(age, sex)
  }
}
dev.off()


