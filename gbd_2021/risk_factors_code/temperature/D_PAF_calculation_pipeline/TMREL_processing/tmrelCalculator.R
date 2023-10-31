# Run TMREL calculations. Launched by tmrelCalculator_launch.R.

## SYSTEM SETUP ----------------------------------------------------------
# Clear memory
rm(list=ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]


# Drives 
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"


# Base filepaths
work_dir <- paste0(j, '/FILEPATH/')
share_dir <- '/FILEPATH/' # only accessible from the cluster


## LOAD DEPENDENCIES -----------------------------------------------------
library(dplyr)
library(ggplot2)
library("data.table")
library("matrixStats")
source("/FILEPATH/r/get_outputs.R")
source("/FILEPATH/r/get_draws.R")
source("/FILEPATH/r/get_location_metadata.R")
source("/FILEPATH/r/get_cause_metadata.R")
source("../../../FILEPATH/csv2objects.R")
library(argparse)


## DEFINE ARGS -----------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--loc_id", help = "location to evaluate for",
                    default = 1, type = "integer")
parser$add_argument("--cause_list", help = "list of cause IDs to evaluate for, comma-separated with no spaces",
                    default = "322,493,494,498,499,509,587,589,689,695,698,704,709,729,716,718,724", type = "character")
parser$add_argument("--acause_list", help = "cause name abbreviations associated with causes in cause_list, comma-separated without spaces",
                    default = "lri,cvd_ihd,cvd_stroke,cvd_htn,cvd_cmp,resp_copd,diabetes,ckd,inj_trans_road,inj_trans_other,inj_drowning,inj_mech,inj_animal,inj_disaster,inj_othunintent,inj_suicide,inj_homicide", type = "character")
parser$add_argument("--outdir", help = "output path",
                    default = '/FILEPATH/config20220728_20220801', type = "character")
parser$add_argument("--indir", help = "input directory",
                    default = "/FILEPATH/updated_with_gamma_0.05-0.95/", type = "character")
parser$add_argument("--tmrel_min", help = "lower bound of TMREL",
                    default = 6.6, type = "double")
parser$add_argument("--tmrel_max", help = "upper bound of TMREL",
                    default = 34.6, type = "double")
parser$add_argument("--year_list", help = "comma-separated string of years to evaluate for",
                    default = "1990", type = "character")
parser$add_argument("--config_file", help = "path to config file",
                    default = "/FILEPATH/config20220728.csv", type = "character")
parser$add_argument("--job_name", help = "name of job",
                    default = "tmrel_1_1990", type = "character")
parser$add_argument("--saveGlobalToCC", help = "whether to save global to CC",
                    default = FALSE, type = "logical")
args <- parser$parse_args()
list2env(args, environment()); rm(args)



cause_list <- as.integer(strsplit(cause_list, split = ",")[[1]])
acause_list <- strsplit(acause_list, split = ",")[[1]]
cause_link <- data.table(cause_id = cause_list, acause = acause_list)
year_list <- as.integer(strsplit(year_list, split = ",")[[1]])

csv2objects(config_file, exclude = ls())

save.image(file = paste0(outdir, "/startingImage_tmrel_", loc_id, "_", year_list, ".RData"))


## IMPORT COD ESTIMATES FOR SELECTED CAUSES ------------------------------

if (cod_source==("get_draws")) {
  cod <- get_draws("cause_id", cause_list, location_id = loc_id, metric_id = 1, measure_id = 1, age_group_id = 22, sex_id = 3, year_id = year_list,
                    source="codcorrect", version = codcorrect_version, gbd_round_id = gbd_round_id, decomp_step = codcorrect_step)

  cod <- cod[, c("age_group_id", "sex_id", "measure_id", "metric_id") := NULL]
  warning("CoD data loaded")

} else {
  cod <- fread(cod_source)[location_id==loc_id & year_id %in% year_list & acause %in% acause_list, ]
  cod <- merge(cod, cause_link, by = "acause", all.x = T)

  warning("CoD data loaded")
}

maxDraw <- length(grep("draw_", names(cod)))-1


# Convert deaths to weights
cod[, paste0("weight_", 0:maxDraw) := lapply(.SD, function(x) {x/sum(x, na.rm = T)}), by = .(location_id, year_id), .SDcols = paste0("draw_", 0:maxDraw)]
cod[, paste0("draw_", 0:maxDraw) := NULL]


warning("CoD data converted to weights")


rr <- do.call(rbind, lapply(acause_list, function(acause) {
  cbind(fread(paste0(indir, "/", acause, "/", acause, "_curve_samples.csv")), acause)}))

setnames(rr, "annual_temperature", "meanTempCat")
setnames(rr, "daily_temperature", "dailyTempCat")
rr <- rr[, .SD, .SDcols = c("meanTempCat", "dailyTempCat", "acause", paste0("draw_", 0:maxDraw))]

warning("RR data loaded")

startSize <- nrow(rr)
rr <- rr[dailyTempCat>=tmrel_min & dailyTempCat<=tmrel_max & meanTempCat>=6, ]
endSize <- nrow(rr)

warning(paste0("RR data trimmed to temp limits, from ", startSize, " to ", endSize))


rr[, paste0("rr_", 0:maxDraw) := lapply(.SD, exp), .SDcols = c(paste0("draw_", 0:maxDraw))]
rr[, "rrMean" := apply(.SD, 1, mean), .SDcols=paste0("draw_", 0:maxDraw)]


## clean up (drop unnecessary variables and rename others)
rr[, c(paste0("draw_", 0:maxDraw)) := NULL]
rr[, meanTempCat := as.integer(meanTempCat)]
rr[, acause := as.character(acause)]
rr[, dailyTempCat := as.integer(round(dailyTempCat*10))]

# merge in cause ids
rr <- merge(rr, cause_link, by = "acause", all.x = T)

warning("RRs merged to cause ids")

## MERGE COD & RR DATA ---------------------------------------------------
master <- merge(cod, rr, by = "cause_id", all = TRUE, allow.cartesian = TRUE)
warning("RRs merged to CoD Data")




## CALCULATE DEATH_WEIGHTED MEAN RRs -------------------------------------
master[, id := .GRP, by = .(location_id, year_id, dailyTempCat, meanTempCat)]
setkey(master, id)

ids <- unique(master[, .(id, location_id, year_id, dailyTempCat, meanTempCat)])

rrWt <- master[, lapply(0:maxDraw, function(x) {sum(get(paste0("rr_", x)) * get(paste0("weight_", x)))}), by = .(id)]
setnames(rrWt, paste0("V", (0:maxDraw)+1), paste0("rr_", 0:maxDraw))


rrWt <- merge(ids, rrWt, by = "id", all = TRUE)
setkey(rrWt, id)

rrWt[, dailyTempCat := as.numeric(dailyTempCat/10)]

warning("Death weighted RRs calculated")


tmrel <- rrWt[, lapply(.SD, function(x) {sum(dailyTempCat*(x==min(x)))}), by = c("location_id", "year_id", "meanTempCat"), .SDcols = paste0("rr_", 0:maxDraw)]
names(tmrel) <- sub("rr_", "tmrel_", names(tmrel))
tmrel <- tmrel[is.na(location_id)==F]

warning("TMRELs calculated")


write.csv(tmrel, file = paste0(outdir, "/tmrel_", loc_id, "_", year_list, ".csv"), row.names = F)

if (saveGlobalToCC==TRUE & loc_id==1) {
  write.csv(tmrel, file = paste0("/FILEPATH/tmrel_", loc_id, "_", year_list, ".csv"), row.names = F)
}

warning("Draw file saved")


tmrel[, "tmrelMean" := apply(.SD, 1, mean), .SDcols=paste0("tmrel_", 0:maxDraw)]
tmrel[, "tmrelLower" := apply(.SD, 1, quantile, c(.025)), .SDcols=paste0("tmrel_", 0:maxDraw)]
tmrel[, "tmrelUpper" := apply(.SD, 1, quantile, c(.975)), .SDcols=paste0("tmrel_", 0:maxDraw)]
tmrel[, paste0("tmrel_", 0:maxDraw) := NULL]

write.csv(tmrel, file = paste0(outdir, "/tmrel_", loc_id, "_", year_list, "_summaries.csv"), row.names = F)
warning("Summary file saved")


write("Complete", paste0(outdir, "/tmrel_", loc_id, "_", year_list, "_complete.txt"))


rrWt[, "rrWtMean" := apply(.SD, 1, mean), .SDcols=paste0("rr_", 0:maxDraw)]
test <- merge(tmrel, rrWt, by = c("location_id", "year_id", "meanTempCat"), all = T)
test[, paste0("rr_", 0:maxDraw) := NULL]
test <- merge(test, rr, by = c("meanTempCat", "dailyTempCat"), all = T)
test[, paste0("rr_", 0:maxDraw) := NULL]


