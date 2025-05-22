###########################################################
### Project: GBD Nonfatal Estimation
### Purpose: Meta-Analysis Proportion MOH 
###########################################################

rm(list=ls())

set.seed(98736)

pacman::p_load(data.table, boot)
library(meta, lib.loc = "FILEPATH")
library(openxlsx, lib.loc = "FILEPATH")

## SET OBJECTS
bundle <- 1
draws <- paste0("draw_", 0:999)
date <- gsub("-", "_", Sys.Date())

## SOURCE FUNCTIONS
source(paste0("FILEPATH", "get_epi_data.R"))

## PULL IN RESULTS FROM FREQ/DUR
files <- list.files("FILEPATH")
files <- files[grep("results2", files)]
dates <- substr(files, 8, 17)
dates <- gsub("_", "-", dates)
last_date <- dates[which.max(as.POSIXct(dates))]
freq_results <- as.data.table(fread("FILEPATH"))
moh_results <- freq_results[type == "moh", .(time_ictal, time_ictal_se)]
moh_results[, time_asy := 1 - time_ictal]

## GET DATA
dt <- get_epi_data(bundle_id = bundle)
dt[, n := round(n, 0)]
dt[, num_migraine := round(num_migraine, 0)]

## META-ANALYSIS
meta <- metaprop(data = dt, event = num_migraine, n = n,
                   studylab = name, comb.random = T, level = .95)
sum <- summary(meta)
random <- sum$random
results <- data.table(mean = random$TE, se = random$seTE)
draw_results <- rnorm(n = 1000, mean = random$TE, sd = random$seTE)
results[, (draws) := lapply(1:1000, function(x) draw_results[x])]
results[, (draws) := lapply(0:999, function(x) inv.logit(get(paste0("draw_", x))))]
results[, mean_draws := apply(.SD, 1, mean), .SDcols = draws]
results[, se_draws := apply(.SD, 1, sd), .SDcols = draws]
results <- data.table(name = c("prop_migraine", "prop_tth"), 
                      mean = c(results$mean_draws, 1-results$mean_draws), 
                      se = rep(results$se_draws, 2),
                      prop_ictal = rep(moh_results$time_ictal, 2),
                      prop_asym = rep(moh_results$time_asy, 2),
                      se_time = rep(moh_results$time_ictal_se, 2))

## CALCULATE PROPORTIONS
results[, asym := mean * prop_asym]
results[, asym_se := sqrt(se_time^2*se^2+se^2*prop_asym^2+se_time^2*mean)]
results[, sym := mean * prop_ictal]
results[, sym_se := sqrt(se_time^2*se^2+se^2*prop_ictal^2+se_time^2*mean)]
final <- results[, .(name, asym, asym_se, sym, sym_se)]
final <- melt(final, measure.vars = c("asym", "sym"))
final[variable == "asym", `:=` (lower = value - 1.96*asym_se, upper = value + 1.96*asym_se)]
final[variable == "sym", `:=` (lower = value - 1.96*sym_se, upper = value + 1.96*sym_se)]
final[, c("asym_se", "sym_se") := NULL]

write.xlsx(final, "FILEPATH")

##create plots
pdf("FILEPATH", height = 9, width = 10)
forest(meta, studlab = dt$study, weight.study = "random", comb.fixed = F)
dev.off()

