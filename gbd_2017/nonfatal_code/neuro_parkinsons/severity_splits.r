###########################################################
### Author: 
### Date: 3/21/2016
### Updated: 12/25/2017
### Project: GBD Nonfatal Estimation
### Purpose: Parkinson's Severity Splits
###########################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "/homes/USERNAME/"
} else { 
  j_root <- "J:/"
  h_root <- "H:/"
}
set.seed(98736)

pacman::p_load(data.table, boot)
library(meta, lib.loc = paste0(j_root, FILEPATH))
library(openxlsx, lib.loc = paste0(j_root, FILEPATH))

## SET OBJECTS
functions_dir <- paste0(j_root, FILEPATH)
results_dir <- paste0(j_root, FILEPATH)
bundle <- 1358
date <- gsub("-", "_", Sys.Date())

## SOURCE FUNCTIONS
source(paste0(functions_dir, "get_epi_data.R"))

## USER FUNCTIONS
scale <- function(x) {
  x[, total := sum(mean)]
  x[, mean := mean/total]
  x[, lower := lower/total]
  x[, upper := upper/total]
  x[, total := NULL]
  return(x)
}

##Get data
dt <- get_epi_data(bundle_id = bundle)
dt[, n := round(n, 0)]
dt[, mild := round(mild, 0)]
dt[, moderate := round(moderate, 0)]
dt[, severe := round(severe, 0)]

##Meta-Analysis
results <- rbindlist(lapply(c("mild", "moderate", "severe"), function(x) {
  meta <- metaprop(data = dt, event = get(x), n = n,
                     studylab = name, comb.random = T, level = .95)
  sum <- summary(meta)
  random <- sum$random
  new_row <- c(random$TE, random$lower, random$upper)
  new_row <- as.list(inv.logit(new_row))
  c(x, new_row)
}))
setnames(results, paste0("V", 1:4), c("severity", "mean", "lower", "upper"))
scale(results)


write.xlsx(results, paste0(results_dir, "metaresults_", date, ".xlsx"))

##create plots
pdf(paste0(results_dir, "forest_plots", date, ".pdf"), height = 9, width = 10)
lapply(c("mild", "moderate", "severe"), function(x) {
  meta <- metaprop(data = dt, event = get(x), n = n,
                   studylab = name, comb.random = T, level = .95)
  forest(meta, studlab = dt$name, weight.study = "random", comb.fixed = F)
})
dev.off()

