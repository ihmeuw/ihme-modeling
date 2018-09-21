## Model retinopathy scalars
##
#####################################################

rm(list=ls())

library(data.table)
library(ggplot2)
library(foreign)
library(lme4)

## Set roots

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  jpath <- "FILEPATH"
  hpath <- "FILEPATH"
} else {
  lib_path <- "FILEPATH"
  jpath <- "FILEPATH"
  hpath <- "FILEPATH"
}

## Set directories for data input and regression output

data_dir <- paste0(hpath, "FILEPATH")
out_dir <- paste0(hpath, "FILEPATH")

## Set functions (get_location_metadata, get_covariates_estimates)

source("FILEPATH")
source("FILEPATH")

## Load input data

data <- fread(paste0(data_dir, "FILEPATH"))

## Load covariates 

nmr_explore <- data.table(read.dta(paste0(jpath, "FILEPATH")))

nmr_explore[, nmr :=q_nn_med * 1000]

nmr <- data.table(read.dta(paste0(jpath, "FILEPATH")))
setnames(nmr, c("year", "sex"), c("year_id", "sex_id"))
nmr <- nmr[sex_id == "both", c("ihme_loc_id", "sex_id", "year_id", "q_nn_med")]
nmr[, ln_nmr := log(q_nn_med * 1000)]
nmr[, sex_id := "3"]
nmr[, sex_id := as.numeric(sex_id)]
nmr[, year_id := floor(year_id)]

haqi <- get_covariate_estimates(covariate_name_short = "haqi")
haqi <- data.table(haqi[, c("mean_value", "location_id", "sex_id", "year_id")])
setnames(haqi, "mean_value", "haqi_mean")

sdi <- get_covariate_estimates(covariate_name_short = "sdi")
sdi <- data.table(sdi[, c("mean_value", "location_id", "sex_id", "year_id")])
setnames(sdi, "mean_value", "sdi_mean")

ldi <- get_covariate_estimates(covariate_name_short = "LDI_pc")
ldi <- data.table(ldi[, c("mean_value", "location_id", "sex_id", "year_id")])
ldi[, ln_ldi := log(mean_value)]

## Prep data

data[, proportion := cases / sample_size]

data[ga == 1, ga_u_28 := 1]
data[ga == 2, ga_u_28 := 0]

data[ga == 1, ga_28_32 := 0]
data[ga == 2, ga_28_32 := 1]

data <- merge(data, get_location_metadata(location_set_id = 35))


## Merge on potential covariates (_recode)

data <- merge(data, haqi, by = c("location_id", "year_id")) ## drops data points before 1980 
data <- merge(data, nmr, by = c("ihme_loc_id", "year_id", "sex_id")) ## drops data points before 1980 
data <- merge(data, sdi, by = c("location_id", "year_id", "sex_id"))
data <- merge(data, ldi, by = c("location_id", "year_id", "sex_id"))

## Recode of covariates, evenly spaced intervals based on full range of haqi (_recode_1)

setorder(haqi, haqi_mean)
row <- floor(nrow(haqi) / 3)
cut_1 <- haqi[row, haqi_mean][[1]]
cut_2 <- haqi[row*2, haqi_mean][[1]]
cutpts <- c(min(haqi[["haqi_mean"]]), cut_1, cut_2, max(haqi[["haqi_mean"]]))
data[, haqi_mean_recode_1 := cut(haqi_mean, breaks = cutpts)]

setorder(nmr, ln_nmr)
row <- floor(nrow(nmr) / 3)
cut_1 <- nmr[row, ln_nmr][[1]]
cut_2 <- nmr[row*2, ln_nmr][[1]]
cutpts <- c(min(nmr[["ln_nmr"]]), cut_1, cut_2, max(nmr[["ln_nmr"]]))
data[, ln_nmr_recode_1 := cut(ln_nmr, breaks = cutpts)]

setorder(sdi, sdi_mean)
row <- floor(nrow(sdi) / 3)
cut_1 <- sdi[row, sdi_mean][[1]]
cut_2 <- sdi[row*2, sdi_mean][[1]]
cutpts <- c(min(sdi[["sdi_mean"]]), cut_1, cut_2, max(sdi[["sdi_mean"]]))
data[, sdi_mean_recode_1 := cut(sdi_mean, breaks = cutpts)]

setorder(ldi, ln_ldi)
row <- floor(nrow(ldi) / 3)
cut_1 <- ldi[row, ln_ldi][[1]]
cut_2 <- ldi[row*2, ln_ldi][[1]]
cutpts <- c(min(ldi[["ln_ldi"]]), cut_1, cut_2, max(ldi[["ln_ldi"]]))
data[, ln_ldi_recode_1 := cut(ln_ldi, breaks = cutpts)]



## Recode of covariates, even bins of data (_recode_2)

setorder(data, haqi_mean, ga_28_32)
data[, haqi_mean_recode_2 := cut(as.numeric(rownames(data)), 3)]

setorder(data, ln_nmr, ga_28_32)
data[, ln_nmr_recode_2 := cut(as.numeric(rownames(data)), 3)]

setorder(data, sdi_mean, ga_28_32)
data[, sdi_mean_recode_2 := cut(as.numeric(rownames(data)), 3)]

setorder(data, ln_ldi, ga_28_32)
data[, ln_ldi_recode_2 := cut(as.numeric(rownames(data)), 3)]


## Recode of covariates, evenly spaced intervals based on full range of haqi (_recode_1)

setorder(data, haqi_mean)
row <- floor(nrow(data) / 3)
cut_1 <- data[row, haqi_mean][[1]]
cut_2 <- data[row*2, haqi_mean][[1]]
cutpts <- c(min(haqi[["haqi_mean"]]), cut_1, cut_2, max(haqi[["haqi_mean"]]))
data[, haqi_mean_recode_2 := cut(haqi_mean, breaks = cutpts)]

setorder(data, ln_nmr)
row <- floor(nrow(data) / 3)
cut_1 <- data[row, ln_nmr][[1]]
cut_2 <- data[row*2, ln_nmr][[1]]
cutpts <- c(min(nmr[["ln_nmr"]]), cut_1, cut_2, max(nmr[["ln_nmr"]]))
data[, ln_nmr_recode_2 := cut(ln_nmr, breaks = cutpts)]

setorder(data, sdi_mean)
row <- floor(nrow(data) / 3)
cut_1 <- data[row, sdi_mean][[1]]
cut_2 <- data[row*2, sdi_mean][[1]]
cutpts <- c(min(sdi[["sdi_mean"]]), cut_1, cut_2, max(sdi[["sdi_mean"]]))
data[, sdi_mean_recode_2 := cut(sdi_mean, breaks = cutpts)]

setorder(data, ln_ldi)
row <- floor(nrow(data) / 3)
cut_1 <- data[row, ln_ldi][[1]]
cut_2 <- data[row*2, ln_ldi][[1]]
cutpts <- c(min(ldi[["ln_ldi"]]), cut_1, cut_2, max(ldi[["ln_ldi"]]))
data[, ln_ldi_recode_2 := cut(ln_ldi, breaks = cutpts)]


setorder(data, ln_nmr)
cutpts <- c(min(nmr[["ln_nmr"]]), log(5), log(15), max(nmr[["ln_nmr"]]))
data[, ln_nmr_recode_3 := cut(ln_nmr, breaks = cutpts, labels = c("low", "medium", "high"))]

model.ln_nmr <- lm(proportion ~ ga_28_32 + factor(ln_nmr_recode_3), data = data)
summary(model.ln_nmr)

amodel.ln_nmr <- lm(proportion ~ ga_28_32 * factor(ln_nmr_recode_3), data = data)
summary(amodel.ln_nmr)


intercept <- coef(amodel.ln_nmr)[[1]]

beta_ga_28_32 <- coef(amodel.ln_nmr)[[2]]

beta_nmr_medium <- coef(amodel.ln_nmr)[[3]]

beta_nmr_high <- coef(amodel.ln_nmr)[[4]]

beta_32_medium <- coef(amodel.ln_nmr)[[5]]

beta_32_high <- coef(amodel.ln_nmr)[[6]]


## Calculate mean & variance for:

## ga_u_28, low
mean_28_low <- intercept

## ga_u_28, medium
mean_28_medium <- intercept + beta_nmr_medium

## ga_u_28, high
mean_28_high <- intercept + beta_nmr_high

## ga_28_32, low
mean_32_low <- intercept + beta_ga_28_32 

## ga_28_32, medium
mean_32_medium <- intercept + beta_ga_28_32 + beta_nmr_medium + beta_32_medium

## ga_28_32, high
mean_32_high <- intercept + beta_ga_28_32 + beta_nmr_high + beta_32_high


estBetaParams <- function(mu, var) {
  alpha <- ((1 - mu) / var - 1 / mu) * mu ^ 2
  beta <- alpha * (1 / mu - 1)
  return(params = list(alpha = alpha, beta = beta))
}


betas <- data.table(ga = c(1, 1, 1, 2, 2, 2),
                    nmr_level = c("low", "medium", "high", "low", "medium", "high"),
                    mean = c(mean_28_low, mean_28_medium, mean_28_high, mean_32_low, mean_32_medium, mean_32_high))


collapse <- data[, c("ga", "ln_nmr_recode_3", "cases", "sample_size")]


collapse[, sample_size_grouped := sum(sample_size), by = c("ln_nmr_recode_3", "ga")][, cases_grouped := sum(cases), by = c("ln_nmr_recode_3", "ga")]

collapse <- unique(collapse[, c("ga", "ln_nmr_recode_3", "sample_size_grouped", "cases_grouped")])

setnames(collapse, "ln_nmr_recode_3", "nmr_level")

betas <- merge(betas, collapse, by = c("ga", "nmr_level"))

betas[, se := sqrt(mean * (1 - mean) / sample_size_grouped + 1.96^2 / (4 * sample_size_grouped^2)) ]

## Comparison to previous estimates
old <- data.table(ga = c(3, 3, 3), nmr_level = c("low", "medium", "high"), mean = c(0.018, 0.018, 0.018), se = c(0.004592, 0.004592, 0.004592), sample_size_grouped = c(838.3225, 838.3225, 838.3225))

betas <- rbindlist(list(betas, old), fill = T)

cols <- c("alpha", "beta")

betas[, (cols) := as.list(estBetaParams(mean, se^2))]

## Clean to match retinopathy code

betas[, nmr_level := as.character(as.numeric(nmr_level))]

betas[nmr_level == "1", nmr_level := "low"][nmr_level == "2", nmr_level := "mid"][nmr_level == "3", nmr_level := "high"]

betas[ ga == 1, ga := 1557][ga == 2, ga := 1558][ga == 3, ga := 1559]

setnames(betas, "ga", "me_id")

write.csv(betas, "FILEPATH", row.names = F)
