
rm(list=ls())

library(DescTools)
library(data.table)
library(slackr)
library(ltcore)
library(mortcore)
library(readr)
library(boot)
library(ggplot2)

# Read in most recent download of HMD lifetables --------------------------

hmd_lifetable_dir <- FILEPATH
output_dir <- FILEPATH
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
