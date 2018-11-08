##########################################################
# Description: Calculates Expected HALE with Epi Transition Outputs
##########################################################
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

## LOAD DEPENDENCIES
source(paste0(c, "/helpers/primer.R"))

##########################################################
##  Define arguments
parser <- ArgumentParser()
parser$add_argument("--data_dir",
                    help = "Site where data will be stored",
                    default = "FILEPATH", type = "character"
)
parser$add_argument("--etmtid",
                    help = "Epi Trans Model Type",
                    default = 5, type = "integer"
)
parser$add_argument("--etmvid",
                    help = "Model Version ID",
                    default = 3, type = "integer"
)
parser$add_argument("--etmvid_lt",
                    help = "Expected Lifetable version",
                    default = 9, type = "integer"
)
args <- parser$parse_args()

list2env(args, environment())
rm(args)
agids <- c(28, 5:20, 30:32, 235)
sids <- 1:3


ylddf <- et.getProduct(
    etmtid = etmtid, etmvid = etmvid, data_dir = data_dir,
    agids = agids, sids = sids, gbdids = 294, mean = T, scale = "normal",
    process_dir = "raked"
)
setnames(ylddf, "pred", "pred_yld")

ltdf <- fread(paste0(data_dir, "/t1/v", etmvid_lt, "/summaries/summary_lt.csv"))
setnames(ltdf, "pred", "pred_ex")

agesdf <- get_age_map(type = "all") %>% as.data.table()

haledf <- merge(ylddf[, .(sdi = round_any(sdi, .005), age_group_id, sex_id, pred_yld)],
                ltdf[, .(sdi = round_any(sdi, .005), age_group_id, sex_id, nLx, pred_lx)],
                by = c("sdi", "age_group_id", "sex_id")
)
haledf <- merge(haledf, agesdf[, c("age_group_id", "age_group_years_start")], by = "age_group_id")

## Calculate HALE and save
calcHALE <- function(agys, haledf) {
    agehaledf <- haledf[age_group_years_start >= agys, pred := sum(nLx * (1 - pred_yld)) / pred_lx, by = c("sdi", "sex_id")]
    agehaledf <- agehaledf[age_group_years_start == agys, c("sdi", "age_group_id", "sex_id", "pred"), with = FALSE]
    agehaledf[, measure_id := 28][, metric_id := 5]
    assert_values(agehaledf, colnames = "pred", "not_na")
    return(agehaledf)
}

haledf <- rbindlist(lapply(sort(unique(haledf$age_group_years_start)), calcHALE, haledf = haledf))
haledf[, c("etmtid", "etmvid") := .(etmtid, etmvid)]
setcolorder(haledf, c("etmtid", "etmvid", "sdi", "age_group_id", "sex_id", "measure_id", "metric_id", "pred"))
write.csv(haledf,
          file = paste0(data_dir, "/t", etmtid, "/v", etmvid, "/hale.csv"),
          row.names = FALSE
)
