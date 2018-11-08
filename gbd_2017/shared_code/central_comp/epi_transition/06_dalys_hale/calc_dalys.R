##########################################################
# Description: Calculates Expected DALYs
##########################################################
## DRIVE MACROS
rm(list = ls())
if (Sys.info()[1] == "Linux") {
    j <- "FILEPATH"
    h <- "FILEPATH"
    c <- "FILEPATH"
} else if (Sys.info()[1] == "Darwin") {
    j <- "FILEPATH"
    h <- "FILEPATH"
    c <- "FILEPATH"
}

##########################################################
## LOAD DEPENDENCIES
source(paste0(c, "helpers/primer.R"))

##########################################################
##  Define arguments (defaults are set for manual run)
parser <- ArgumentParser()
parser$add_argument("--data_dir",
                    help = "Site where data will be stored",
                    default = "FILEPATH", type = "character"
)
parser$add_argument("--etmvid",
                    help = "DALYs/HALE Version ID",
                    default = 3, type = "integer"
)
parser$add_argument("--etmvid_yll",
                    help = "Expected YLL version",
                    default = 23, type = "integer"
)
parser$add_argument("--etmvid_yld",
                    help = "Expected YLD version",
                    default = 12, type = "integer"
)
parser$add_argument("--agid",
                    help = "Age Group ID",
                    default = 32, type = "integer"
)
parser$add_argument("--sid",
                    help = "Sex ID",
                    default = 1, type = "integer"
)
parser$add_argument("--gbdid",
                    help = "Cause ID",
                    default = 399, type = "integer"
)
args <- parser$parse_args()

list2env(args, environment())
rm(args)

##########################################################

##  Read in DALY inputs and combine
ylldf <- NULL
ylddf <- NULL
if (file.exists(paste0(data_dir, "/t4/v", etmvid_yll, "/YLLs/agid_", agid, "_sid_", sid, "_gbdid_", gbdid, ".RDs"))) ylldf <- readRDS(paste0(data_dir, "/t5/v", etmvid_yll, "/YLLs/agid_", agid, "_sid_", sid, "_gbdid_", gbdid, ".RDs"))
if (file.exists(paste0(data_dir, "/t5/v", etmvid_yld, "/raked/agid_", agid, "_sid_", sid, "_gbdid_", gbdid, ".RDs"))) ylddf <- readRDS(paste0(data_dir, "/t6/v", etmvid_yld, "/raked/agid_", agid, "_sid_", sid, "_gbdid_", gbdid, ".RDs"))
if (gbdid == 294) ylldf <- ylldf[, .(pred = mean(pred)), by = eval(names(ylldf)[!names(ylldf) %in% c("pred")])]
if (gbdid == 294) ylldf[, cause_id := gbdid]
if (gbdid == 399 & agid == 32) ylddf[is.nan(pred), pred := 0]

dalydf <- rbindlist(list(ylldf, ylddf), use.names = T, fill = T)
dalydf[is.na(pred), pred := 0]

## Calculate DALYs and save
dalydf <- dalydf[, list(pred = sum(pred)), by = eval(names(dalydf)[!names(dalydf) %in% c("measure_id", "etmtid", "etmvid", "pred")])][, measure_id := 2]
dalydf[, ":="(etmvid_yll = etmvid_yll, etmvid_yld = etmvid_yld)]
saveRDS(dalydf,
        file = paste0(data_dir, "/t6/v", etmvid, "/raked/MEAN_agid_", agid, "_sid_", sid, "_gbdid_", gbdid, ".RDs")
)
