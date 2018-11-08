##########################################################
## Epi transition raking job
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
## LOAD DEPENDENCIES

source(paste0(c, "/helpers/primer.R"))
parser <- ArgumentParser()
parser$add_argument("--data_dir",
                    help = "Site where data will be stored",
                    default = "FILEPATH", type = "character"
)
parser$add_argument("--etmtid",
                    help = "Model type ID",
                    default = 101, type = "integer"
)
parser$add_argument("--etmvid",
                    help = "Model version ID",
                    default = 1, type = "integer"
)
parser$add_argument("--agid",
                    help = "Age group ID",
                    default = 12, type = "integer"
)
parser$add_argument("--sid",
                    help = "Sex ID",
                    default = 2, type = "integer"
)
args <- parser$parse_args()
print(args)
list2env(args, environment())
rm(args)

medf <- readRDS(paste0(data_dir, "/t", etmtid, "/v", etmvid, "/dem_inputs/medf.RDS"))
submitmap <- readRDS(sprintf("%s/t%d/v%d/submitmap.RDS", data_dir, etmtid, etmvid))
model_floor <- unique(submitmap$model_floor)

rakedf <- {}
sfdf <- {}

for (par_lvl in 0:3) {

    message(paste0("RAKING FROM LEVEL ", par_lvl))
    pids <- medf[level == par_lvl, gbd_id]

    if (par_lvl == 0) {
        pmids <- if (etmtid == 4) c(1, 14) else c(etmtid, etmvid)
        pdf <- et.getProduct(
            etmtid = pmids[1], etmvids = pmids[2], data_dir = data_dir, agids = agid, sids = sid, gbdids = 294, mean = T,
            process_dirs = "fits", scale = "normal"
        )
        acdf <- copy(pdf)[, .(sdi, gbd_id, etmtid, etmvid, age_group_id, sex_id, measure_id, metric_id, pred, level = par_lvl)]
        pdf <- pdf[, .(sdi, par_pred = pred, parent_id = gbd_id)]
    } else {
        pdf <- rakedf[gbd_id %in% pids, .(sdi, par_pred = pred, parent_id = gbd_id)]
    }

    cids <- medf[level == par_lvl + 1, gbd_id]
    cdf <- et.getProduct(
        etmtid = etmtid, etmvids = etmvid, data_dir = data_dir, agids = agid, sids = sid, gbdids = cids, mean = T,
        process_dirs = "fits", scale = "normal"
    )[, .(etmtid, etmvid, age_group_id, sex_id, measure_id, metric_id, gbd_id, sdi, pred)]
    cdf <- merge(cdf, medf[, .(gbd_id, parent_id, level)], by = "gbd_id")

    assert_values(pdf, colnames = "pred", test = "not_na")
    assert_values(cdf, colnames = "pred", test = "not_na")

    rdf <- merge(pdf, cdf, by = c("parent_id", "sdi"), all.y = T)
    rdf[, scale_factor := par_pred / sum(pred), by = .(sdi, parent_id)][, pred := pred * scale_factor]
    sfdf <- rbind(sfdf, unique(rdf[, .(parent_id, sdi, scale_factor, level = par_lvl)]), use.names = T, fill = T)
    rakedf <- rbind(rakedf, rdf[, !c("par_pred", "parent_id", "scale_factor")], use.names = T, fill = T)
}

sfdf <- merge(sfdf, medf[, .(parent_id = gbd_id, parent_name = gbd_name_short)], by = "parent_id", all.x = T)
if (etmtid != 4) rakedf <- rbind(acdf, rakedf, use.names = T, fill = T)
rakedf[, level := NULL]

setcolorder(rakedf, c("etmtid", "etmvid", "age_group_id", "sex_id", "measure_id", "metric_id", "gbd_id", "sdi", "pred"))
assert_ids(rakedf, id_vars = list(
    gbd_id = medf$gbd_id, age_group_id = agid, sex_id = sid,
    measure_id = unique(rakedf$measure_id), metric_id = unique(rakedf$metric_id),
    sdi = seq(0, 1, .005)
))
mclapply(split(rakedf, by = "gbd_id"), function(df, ...) {
    saveRDS(df, sprintf(
        "%s/t%d/v%d/raked/MEAN_agid_%d_sid_%d_gbdid_%d.RDs",
        data_dir, etmtid, etmvid, agid, sid, unique(df$gbd_id)
    ))
}, data_dir = data_dir, etmtid = etmtid, etmvid = etmvid, agid = agid, sid = sid, mc.cores = 10)
