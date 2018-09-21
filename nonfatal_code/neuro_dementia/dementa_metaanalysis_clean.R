###########################################################
### Author: 
### Date: 3/21/2016
### Project: GBD Nonfatal Estimation
### Purpose: Dementia Severity Splits
###########################################################

##Setup
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "/homes/USERNAME/"
} else { 
  j_root <- "J:/"
  h_root <- "H:/"
}
set.seed(98736)

if (!require(pacman)) install.packages("pacman")
pacman::p_load(data.table, meta, boot, openxlsx)

##Set directories
dir <- FILEPATH
doc <- FILEPATH

##function to scale
scale <- function(x) {
  x[, total := sum(mean), by = "age_start"]
  x[, mean := mean/total]
  x[, lower := lower/total]
  x[, upper := upper/total]
  x[, total := NULL]
  return(x)
}

##get and format data
data <- as.data.table(read.xlsx(paste0(FILEPATH, ".xlsx")))
data[, N := round(N, 0)]
data[, asymptomatic := round(asymptomatic, 0)]
data[, mild := round(mild, 0)]
data[, moderate := round(moderate, 0)]
data[, severe := round(severe, 0)]

##combine asymptomatic and mild into mild 
data[!is.na(asymptomatic), mild := asymptomatic + mild]
data[, asymptomatic := NULL]
##fix rounding error
data[nid == 149989 & age_start == 0, N := 2]

##Meta-analysis with DSM-III severities
results_all <- rbindlist(lapply(c("mild", "moderate", "severe"), function(x) {
  rbindlist(lapply(c(0, 70, 80), function(y) {
    meta <- metaprop(data = data[age_start == y], event = get(x), n = N,
                     studylab = study_name, comb.random = T, level = .95)
    sum <- summary(meta)
    random <- sum$random
    new_row <- c(random$TE, random$lower, random$upper)
    new_row <- as.list(inv.logit(new_row))
    c(x, y, new_row)
  }))
}))
setnames(results_all, c("V1", "V2", "V3", "V4", "V5"), c("severity", "age_start", "mean", "lower", "upper"))
results_all[, dsm := 1]
scale(results_all)

##Meta-analysis without DSM-III severities 
results_small <- rbindlist(lapply(c("mild", "moderate", "severe"), function(x) {
  rbindlist(lapply(c(0, 70, 80), function(y) {
    meta <- metaprop(data = data[age_start == y & DSM == 0], event = get(x), n = N,
                     studylab = study_name, comb.random = T, level = .95)
    sum <- summary(meta)
    random <- sum$random
    new_row <- c(random$TE, random$lower, random$upper)
    new_row <- as.list(inv.logit(new_row))
    c(x, y, new_row)
  }))
}))
setnames(results_small, c("V1", "V2", "V3", "V4", "V5"), c("severity", "age_start", "mean", "lower", "upper"))
results_small[, dsm := 0]
scale(results_small)

##Write csv
total <- rbind(results_all, results_small)
write.csv(total, paste0(FILEPATH, ".csv"), row.names = F)

##create pdfs
pdf(paste0(FILEPATH, ".pdf"), height = 9, width = 10)
lapply(c("mild", "moderate", "severe"), function(x) {
  lapply(c(0, 70, 80), function(y) {
    meta <- metaprop(data = data[age_start == y & DSM == 0], event = get(x), n = N,
                     studylab = study, comb.random = T, level = .95)
    used <- data[age_start == y & DSM == 0]
    forest(meta, studlab = used$study, weight.study = "random", comb.fixed = F)
  })
})
dev.off()
