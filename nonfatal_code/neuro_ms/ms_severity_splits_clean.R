###########################################################
### Author: 
### Date: 3/21/2016
### Project: GBD Nonfatal Estimation
### Purpose: MS Severity Splits
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

##Load data and create two datasets
data <- as.data.table(read.xlsx(paste0(FILEPATH, ".xlsx")))
data[, N := round(N, 0)]
data[, asymptomatic := round(asymptomatic, 0)]
data[, mild := round(mild, 0)]
data[, moderate := round(moderate, 0)]
data[, severe := round(severe, 0)]

with_asy <- copy(data)
with_asy <- with_asy[all_cat == 1]

no_asy <- copy(data)
no_asy[!is.na(asymptomatic), mild := asymptomatic + mild]
no_asy[, asymptomatic := NULL]

##Meta-Analysis of Asymptomatic and Mild- Data with asymptomatic
results_all <- data.table(name = character(0), mean = numeric(0), lower = numeric(0), upper = numeric(0))
categories <- c("asymptomatic", "mild")
for (x in categories) {
  meta <- metaprop(data = with_asy, event = get(x), n = N,
                   studylab = study_name, comb.random = T, level = .95)
  sum <- summary(meta)
  random <- sum$random
  new_row <- c(random$TE, random$lower, random$upper)
  new_row <- as.list(inv.logit(new_row))
  new_row <- c(x, new_row)
  results_all <- rbind(results_all, new_row)
}

##pdfs
pdf(paste0(FILEPATH, ".pdf"), height = 9, width = 10)
lapply(c("asymptomatic", "mild"), function(x) {
  meta <- metaprop(data = with_asy, event = get(x), n = N,
                   studylab = study_name, comb.random = T, level = .95)
  forest(meta, studlab = with_asy$study_name, weight.study = "random", comb.fixed = F)
})
dev.off()

##Create proportion of asymptomatic and mild out of total asymptomatic + mild
results_all[, mean_total := sum(mean)]
results_all[, prop := mean/mean_total]

prop_asym <- as.numeric(results_all[name == "asymptomatic", .(prop)])
prop_mild <- as.numeric(results_all[name == "mild", .(prop)])

##Meta-Analysis of all Data
results <- data.table(name = character(0), mean = numeric(0), lower = numeric(0), upper = numeric(0))
categories <- c("mild", "moderate", "severe")
for (x in categories) {
  meta <- metaprop(data = no_asy, event = get(x), n = N,
                   studylab = study_name, comb.random = T, level = .95)
  sum <- summary(meta)
  random <- sum$random
  new_row <- c(random$TE, random$lower, random$upper)
  new_row <- as.list(inv.logit(new_row))
  new_row <- c(x, new_row)
  results <- rbind(results, new_row)
}

##pdfs
pdf(paste0(FILEPATH, ".pdf"), height = 9, width = 10)
lapply(c("mild", "moderate", "severe"), function(x) {
  meta <- metaprop(data = no_asy, event = get(x), n = N,
                   studylab = study_name, comb.random = T, level = .95)
  forest(meta, studlab = no_asy$study_name, weight.study = "random", comb.fixed = F)
})
dev.off()

##Scale results to make sure they add to 1
results[, total := sum(mean)]
results[, mean := mean/total]
results[, lower := lower/total]
results[, upper := upper/total]
results[, total := NULL]

##Rescale asymptomatic and mild
mild <- copy(results)
mild <- mild[name == "mild"]
asymp <- copy(mild)
asymp[, name := "asymptomatic"]
mild <- rbind(asymp, mild)
cols <- c("mean", "upper", "lower")
mild[name == "asymptomatic", (cols) := lapply(.SD, function(x) x*prop_asym), .SDcols = cols]
mild[name == "mild", (cols) := lapply(.SD, function(x) x*prop_mild), .SDcols = cols]
setnames(mild, "upper", "up") ## switch upper and lower because they flip when multiply by proportions
setnames(mild, "lower", "upper")
setnames(mild, "up", "lower")

results <- results[!name == "mild"]
results <- rbind(mild, results)

write.csv(results, paste0(FILEPATH, ".csv"), row.names = F)
