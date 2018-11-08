##########################################################
## Calculate YLLs
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
}

##########################################################
## Load Dependencies
source(paste0(c, "/helpers/primer.R"))

# Parse arguments
parser <- ArgumentParser()
parser$add_argument("--data_dir",
                    help = "Site where data will be stored",
                    default = "FILEPATH", type = "character"
)
parser$add_argument("--etmtid",
                    help = "Model type ID",
                    default = 4, type = "integer"
)
parser$add_argument("--etmvid",
                    help = "Model version ID",
                    default = 15, type = "integer"
)
parser$add_argument("--etmvid_lt",
                    help = "Life table model version ID",
                    default = 14, type = "integer"
)
args <- parser$parse_args()
list2env(args, environment())
rm(args)

taskid <- ifelse(interactive(), loadTask(data_dir, etmtid, etmvid), Sys.getenv("SGE_TASK_ID"))
resub <- F
whichmap <- ifelse(resub, "resubmitmap", "submitmap")
gbdid <- readRDS(sprintf("%s/t%d/v%d/%s.RDS", data_dir, etmtid, etmvid, whichmap))[task_id == taskid, unique(gbd_id)]
pdir <- "raked"

# Pull in theoretical minimum risk life table
tmrltdf <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/tmrltdf.RDS", data_dir, etmtid, etmvid))

if (gbdid == 294) {
    etmtid <- 1
    etmvid <- etmvid_lt
    pdir <- "fits"
}

mxdf <- et.getProduct(
    data_dir = data_dir, etmtid = etmtid, etmvids = etmvid,
    agids = c(2:20, 30:32, 235), sids = 1:2, gbdids = gbdid,
    mean = T, process_dirs = pdir, scale = "normal"
)

axdf <- et.getProduct(
    data_dir = data_dir, etmtid = 1, etmvids = etmvid_lt,
    agids = c(28, 5:20, 30:32, 235), sids = 1:2, gbdids = 294,
    mean = T, process_dirs = "life_table", scale = "normal"
)[, .(
    etmtid = 1, etmvid = etmvid_lt,
    age_group_id, sex_id, metric_id = 5, process, sdi, ax
)]
axdf <- rbind(axdf[age_group_id != 28],
              axdf[age_group_id == 28, .(age_group_id = 2:4), by = .(etmtid, etmvid, sex_id, metric_id, process, sdi, ax)],
              use.names = T, fill = T
)
axdf[age_group_id == 2, ax := ax * (6 / 365)][age_group_id == 3, ax := ax * (21 / 365)][age_group_id == 4, ax := ax * (337 / 365)]

axdf <- merge(axdf, data.table(get_age_map(type = "all"))[, .(age_group_id, age_group_years_start)], by = "age_group_id")
axdf[, age := round(age_group_years_start + ax, 2)]

deathsdf <- merge(mxdf, axdf[, .(age_group_id, sex_id, sdi = round_any(sdi, .005), age)], by = c("age_group_id", "sex_id", "sdi"), all.x = T)
deathsdf <- merge(deathsdf, tmrltdf[, .(age, mr_ex)], by = "age", all.x = T)[order(age_group_id, sex_id, sdi)]
deathsdf[, pred := pred * mr_ex][, measure_id := 4]
deathsdf[, c("age", "process", "scale", "mr_ex") := NULL]
setcolorder(deathsdf, c("etmtid", "etmvid", "age_group_id", "sex_id", "measure_id", "metric_id", "gbd_id", "sdi", "pred"))

# Assert everything we're expecting is present
assert_ids(deathsdf, id_vars = list(
    gbd_id = gbdid, age_group_id = c(2:20, 30:32, 235), sex_id = 1:2,
    measure_id = 4, metric_id = 3,
    sdi = seq(0, 1, .005)
))

# Save
mclapply(split(deathsdf, by = c("age_group_id", "sex_id")), function(df, ...) {
    saveRDS(df, sprintf(
        "%s/t%d/v%d/YLLs/MEAN_agid_%d_sid_%d_gbdid_%d.RDs",
        data_dir, etmtid, etmvid, unique(df$age_group_id), unique(df$sex_id), gbdid
    ))
}, data_dir = data_dir, etmtid = etmtid, etmvid = etmvid, gbdid = gbdid, mc.cores = 10)
