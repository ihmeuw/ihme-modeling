###########################################################
### Author: 
### Date: 3/21/2016
### Project: GBD Nonfatal Estimation
### Purpose: GBS Splits
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

##Set directories and objects
custom_input <- FILEPATH
gbs <- FILEPATH

##Get packages
library(data.table)
library(meta)
library(arm)
library(boot)

##Get and format data
data <- fread(paste0(FILEPATH, ".csv"))
setnames(data, "cases", "sample_size")
data[, all_specified := round(sample_size * value_prop_all_specified)]
data[, influenza := round(sample_size * value_prop_influenza)]
data[, URI := round(sample_size * value_prop_URI)]
data[, GI := round(sample_size * value_prop_GI)]
data[, other_infectious := round(sample_size * value_prop_other_infectious)]
all_specified <- data[!is.na(all_specified)]
all_specified[, cases := all_specified]
influenza <- data[!is.na(influenza)]
influenza[, cases := influenza]
URI <- data[!is.na(URI)]
URI[, cases := URI]
GI <- data[!is.na(GI)]
GI[, cases := GI]
other_infectious <- data[!is.na(other_infectious)]
other_infectious[, cases := other_infectious]

##Frame for Results
results <- data.table(name = character(0), mean = numeric(0), lower = numeric(0), upper = numeric(0))

categories <- c("all_specified", "influenza", "URI", "GI", "other_infectious")
for (x in categories) {
  meta <- metaprop(data = get(x), event = cases, n = sample_size,
                   studylab = citation, comb.random = T, level = .95)
  sum <- summary(meta)
  random <- sum$random
  new_row <- c(random$TE, random$lower, random$upper)
  new_row <- as.list(inv.logit(new_row))
  new_row <- c(x, new_row)
  results <- rbind(results, new_row)
}

##squeeze to all_specified
squeezed <- copy(results)
all_specified <- as.numeric(squeezed[name == "all_specified", .(mean)])
squeeze1 <- cbind(squeezed, all_specified)
squeezed[!name == "all_specified", total := sum(mean)]
squeezed[!name == "all_specified", mean := mean * all_specified / total]
squeezed[!name == "all_specified", lower := lower * all_specified / total]
squeezed[!name == "all_specified", upper := upper * all_specified / total]

##Other neurological disorders is the difference between 100% and all_specified
squeezed[name == "all_specified", name := "other_neurological"]
squeezed[name == "other_neurological", mean := 1- mean]
squeezed[name == "other_neurological", lower := 1- upper]
squeezed[name == "other_neurological", upper := 1 - lower]

squeezed <- squeezed[, .(name, mean, lower, upper)]
write.csv(squeezed, paste0(FILEPATH, ".csv"), row.names = F)
