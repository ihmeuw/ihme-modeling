####################################################################################################
## Description: Prep estimates of survival proportion by location. Use the lifetables from HMD.
##              Draws and original_e0 are just copies from HMD
##
## Outputs:     survival_draws
##                '[temp_dir]/input_survival_draws.rdata'
##              survival_data, survival_matrix, survival_sigma2_matrix
##                '[temp_dir]/input_survival_data.rdata'
####################################################################################################

rm(list=ls())
root <- ifelse(Sys.info()[1]=="Windows","FILEPATH","FILEPATH")

library(DescTools, lib = paste0(root, "FILEPATH/r-pkg"))
library(data.table)
library(slackr, lib = paste0(root, "FILEPATH/r-pkg"))
library(ltcore, lib = paste0(root, "FILEPATH/r-pkg"))
library(mortcore, lib = paste0(root, "FILEPATH/r-pkg"))
library(readr)
library(boot)
library(ggplot2)

# Read in most recent download of HMD lifetables --------------------------

shared_functions_dir <- "FILEPATH"
hmd_lifetable_dir <- "FILEPATH"
output_dir <- paste0(root, "FILEPATH")
lt_keys <- c("location", "year_id", "sex_id")


lt_full <- lapply(1L:2L, function(s) {
  sex_var <- ifelse(s == 1, "male", "female")
  sex_abr <- ifelse(s == 1, "m", "f")

  lt_path <- paste0(hmd_lifetable_dir, "lt_", sex_var, "/", sex_abr, "ltper_1x1/")
  files <- grep("1x1", list.files(lt_path), value = T)

  # get all 1X1 lifetables for specified location
  files <- data.table(fpath = files)
  files[, location := gsub(paste0(".", sex_abr, "ltper_1x1.txt"), "", fpath)]

  # read in lifetable
  lt_data <- lapply(1:nrow(files), function(i) {
    fpath <- files[i, fpath]
    location <- files[i, location]
    fpath <- paste0(hmd_lifetable_dir, "lt_", sex_var, "/", sex_abr, "ltper_1x1/", fpath)
    lt_data <- read_fwf(file = fpath,
                        fwf_cols(year_id = c(3, 6), age = c(16, 18),
                                 mx = c(24, 30), qx = c(33, 39), ax = c(42, 45)),
                        skip = 4)
    lt_data <- data.table(lt_data)
    lt_data[, c("sex_id", "location") := list(s, location)]
    return(lt_data)
  })
  lt_data <- rbindlist(lt_data)

})
lt_full <- rbindlist(lt_full)

# drop years where data are indicated by HMD as problematic
lt_full <- lt_full[!(location == "TWN" & year_id < 1980)]
lt_full <- lt_full[!(location == "UKR" & year_id < 1970)]
lt_full <- lt_full[!(location == "BLR" & year_id < 1970)]
lt_full <- lt_full[!(location == "EST" & year_id > 2000)]
lt_full <- lt_full[!(location == "IRL" & between(year_id, 1950, 1985))]
lt_full <- lt_full[!(location == "ITA" & between(year_id, 1872, 1905))]
lt_full <- lt_full[!(location == "LVA" & between(year_id, 1959, 1969))]
lt_full <- lt_full[!(location == "LTV" & between(year_id, 1959, 1969))]
lt_full <- lt_full[!(location == "PRT" & between(year_id, 1940, 1970))]
lt_full <- lt_full[!(location == "RUS" & between(year_id, 1959, 1969))]
lt_full <- lt_full[!(location == "SVK" & between(year_id, 1950, 1961))]
lt_full <- lt_full[!(location == "ESP" & between(year_id, 1908, 1960))]
lt_full <- lt_full[!(location == "FIN" & year_id == 1918)] # civil war
lt_full <- lt_full[!(location == "FIN" & between(year_id, 1939, 1949))] # war with Soviet Union
lt_full <- lt_full[!(between(year_id, 1914, 1920))] # Spanish Flu
lt_full <- lt_full[!(between(year_id, 1940, 1945))] # World War 2
lt_full <- lt_full[!(year_id < 1900)]

lt_full[, c("mx", "ax") := NULL]

# determine abridged lt age groups
abridged_ages <- c(0, 1, seq(5, 110, 5))
lt_full[, abridged_age := cut(age, breaks = c(abridged_ages, Inf), labels = abridged_ages, right = F, include.lowest = T)]
lt_full[, abridged_age := as.integer(as.character(abridged_age))]

setcolorder(lt_full, c("location", "year_id", "sex_id", "age", "abridged_age", "qx"))
setkeyv(lt_full, c("location", "year_id", "sex_id", "age", "abridged_age"))


# aggregate via px
lt_full[, px := 1 - qx]
lt_full[, px_abridged := prod(px), by = c(lt_keys, "abridged_age")]
lt_full[, qx_abridged := 1 - px_abridged]

lt_full <- lt_full[age > 0 & age < 110]

lt_full[, n := 5]
lt_full[abridged_age == 1, n := 4]

lt_full[, log_qx := log(qx)]
lt_full[, log_qx_abridged := log(qx_abridged)]
lt_full <- lt_full[!is.infinite(log_qx) & !is.infinite(log_qx_abridged)]


# Fit all the linear models -----------------------------------------------

log_fits <- lapply(unique(lt_full$age), function(a) {
  fits <- lapply(1:2, function(s) {
    fit_data <- lt_full[age == a & sex_id == s]

    fit <- lm(formula = log_qx ~ log_qx_abridged, data = fit_data)

    coef <- summary(fit)$coefficients[, 1]
    p_values <- round(summary(fit)$coefficients[, 4], 5)

    fit <- data.table(sex_id = s, age = a, intercept = coef[1], slope = coef[2],
                      intercept_p_value = p_values[1], slope_p_value = p_values[2])
    return(fit)
  })
  fits <- rbindlist(fits)
  return(fits)
})
log_fits <- rbindlist(log_fits)
write_csv(log_fits, path = paste0(output_dir, "log_fit_coefficients.csv"))

# Plot scatters ------------------------------------------------------------

pdf(paste0(output_dir, "log_scatters.pdf"), width=14, height=8)
for(s in 1:2) {
  for(ages in list(c(1, 19), c(20, 39), c(40, 59), c(60, 79), c(80, 99), c(100, 109))) {

    print(s)
    print(ages)
    sex <- ifelse(s == 1, "Males", "Females")

    title <- paste0(sex, ". Scatter of 5qx and 1qx. Ages ", ages[1], "-", ages[2], ".")

    plot_data <- lt_full[between(abridged_age, ages[1], ages[2]) & sex_id == 1]
    p <- ggplot(plot_data, aes(x = log_qx_abridged, y = log_qx)) + geom_point(alpha = 0.2) +
      geom_abline(data = log_fits[sex_id == s & between(age, ages[1], ages[2])], aes(intercept = intercept, slope = slope), colour = "red") +
      facet_wrap(~ age, scales = "free") + labs(x = "log(5qx)", y = "log(1qx)", title = title) + theme_bw()
    print(p)
  }
}
dev.off()


# Predict 1qx for HMD lifetables ------------------------------------------

fit_data_log <- lapply(unique(lt_full$age), function(a) {
  fit_data <- lapply(1:2, function(s) {

    intercept <- log_fits[sex_id == s & age == a, intercept]
    slope <- log_fits[sex_id == s & age == a, slope]

    fit_data <- lt_full[age == a & sex_id == s]
    fit_data[, pred_log_qx := ((log_qx_abridged * slope) + intercept)]
    return(fit_data)
  })
  fit_data <- rbindlist(fit_data)
  return(fit_data)
})
fit_data_log <- rbindlist(fit_data_log)

calc_other_columns <- function(data) {
  data[, pred_px := 1 - pred_qx]
  data[, pred_px_abridged := prod(pred_px), by = c(lt_keys, "abridged_age")]
  data[, pred_qx_abridged := 1 - pred_px_abridged]
  data[, adjustment_factor := (px_abridged / pred_px_abridged)]
  data[, adjustment_factor := (adjustment_factor) ^ (1/n)]
  data[, pred_px_adjusted := pred_px * adjustment_factor]
  data[, pred_qx_adjusted := 1 - pred_px_adjusted]
  data[, pred_px_abridged_adjusted := prod(pred_px_adjusted), by = c(lt_keys, "abridged_age")]
  return(data)
}

fit_data_log[, pred_qx := exp(pred_log_qx) * qx_abridged]
fit_data_log <- calc_other_columns(fit_data_log)

fit_data_log <- fit_data_log[, list(location, year_id, sex_id, age,
                                        abridged_age, method = "qx_ratio",
                                        qx, pred_qx = pred_qx_adjusted)]

fit_data <- rbind(fit_data_log)
fit_data[, CCC := CCC(qx, pred_qx)$rho.c["est"][1, 1], by = c("sex_id", "age", "method")]
fit_data[, lab := paste0("CCC: ", round(CCC, 4))]
fit_data[, sex_id := factor(sex_id, labels = c("Males", "Females"))]

pdf(paste0(output_dir, "results_scatter.pdf"), width=14, height=8)
for(a in unique(fit_data$age)) {
  print(a)

  title <- paste0("Age ", a, ".")

  plot_data <- fit_data[age == a]
  ccc_data <- unique(fit_data[age == a, list(sex_id, method, CCC, lab)])

  p <- ggplot(plot_data, aes(x = qx, y = pred_qx, label = lab)) +
    geom_point(alpha = 0.2) +
    geom_text(data = ccc_data, mapping = aes(x = Inf, y = -Inf, label = lab), hjust = 1, vjust = 0) +
    facet_grid(method ~ sex_id) +
    geom_abline() +
    labs(x = "qx", y = "pred_qx", title = title) +
    coord_equal() +
    theme_bw()
  print(p)
}

dev.off()

ccc_data <- unique(fit_data[, list(sex_id, age, method, CCC)])
setkeyv(ccc_data, c("sex_id", "age", "method"))
write_csv(ccc_data, path = paste0(output_dir, "concordance_correlation_coefficients.csv"))
