##########################################################
## Queries input data for all
##########################################################

rm(list = ls())
if (Sys.info()[1] == "Linux") {
    j <- "FILEPATH"
    h <- "FILEPATH"
    c <- "FILEPATH"
} else if (Sys.info()[1] == "Darwin") {
    j <- "FILEPATH"
    h <- "FILEPATH"
}

## LOAD DEPENDENCIES
source(paste0(c, "/helpers/primer.R"))
library(gam)


## ############################################
## PARSE ARGUMENTS ############################
## ############################################

parser <- ArgumentParser()
parser$add_argument("--data_dir",
                    help = "Site where data will be stored",
                    default = "FILEPATH", type = "character"
)
parser$add_argument("--etmtid",
                    help = "Model type ID",
                    default = 1, type = "integer"
)
parser$add_argument("--etmvid",
                    help = "Model version ID",
                    default = 1, type = "integer"
)
parser$add_argument("--mvid",
                    help = "Model version ID (of input)",
                    default = 333, type = "integer"
)
args <- parser$parse_args()

print(args)
list2env(args, environment())
rm(args)

taskid <- ifelse(interactive(), 3, as.numeric(Sys.getenv("SGE_TASK_ID")))

## Map age and sex from task
agid <- rep(c(2:20, 30:32, 235), 2)[taskid]
sid <- ifelse(taskid < 24, 1, 2)
## Load gbd ids to be queried
medf <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/medf.RDS", data_dir, etmtid, etmvid))
gbdids <- medf$gbd_id
ntds <- medf[parent_id %in% c(344, 347, 360), gbd_id]
hiv <- medf[parent_id == 298 | gbd_id == 298, gbd_id]
restrictions <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/restrictionsdf.RDS", data_dir, etmtid, etmvid))[age_group_id == agid & sex_id == sid]

## Query results
trans <- "none"
message(paste0("LOADING DATA WITH ", trans, " TRANSFORM"))
df <- et.getOutputs(etmtid, etmvid, mvid,
                    agids = agid, sids = sid,
                    gbdids = gbdids, transform = trans
)

# Calculate floors for use in log-normal models (CoD and Non-fatal)
floor_df <- df[val > 1e-9]
floor_df[, sdi := round_any(sdi, .005)]
floor_df[, val := log(val)]
floor_df_sdi <- floor_df[, .(med = median(val), mad = mad(val), n = .N), by = .(age_group_id, sex_id, gbd_id, sdi)]
floor_df_sdi[, log_floor := med - 2 * mad]
floor_df_all <- floor_df[, .(med = median(val), mad = mad(val), n = .N), by = .(age_group_id, sex_id, gbd_id)]
floor_df_all[, log_floor := med - 2 * mad]

if (nrow(restrictions) == 0) restrictions <- data.table(expand.grid(gbd_id = gbdids, restr = F))

# Run regressions on log - floor by cause, age, and sex: method adapted from cod data prep team
pred_template <- data.table(expand.grid(etmtid = etmtid, mvid = mvid, sdi = seq(0, 1, .005), age_group_id = agid, sex_id = sid, gbd_id = gbdids))
pred_floors <- mclapply(gbdids, function(g, floor_df, floor_df_all) {
    print(g)
    pred_df <- copy(pred_template)[gbd_id == g]
    if (restrictions[gbd_id == g, restr]) return(pred_df[, pred_floor := 1e-9][, floor_type := "restricted"])
    design_df <- floor_df_sdi[gbd_id == g & n >= 5]
    design_df[, bin := round_any(sdi, .1, floor)]
    data_density <- design_df[sdi %between% c(.2, .895), .N, by = bin]
    if (length(data_density$bin) == 7 & all(data_density$N > 5) & !g %in% hiv) {
        mod <- gam(formula = log_floor ~ lo(sdi), data = design_df)
        pred_df[, pred_floor := exp(predict(mod, newdata = pred_df))][, floor_type := "dense"]
    } else {
        pred_df[, pred_floor := floor_df_all[gbd_id == g, exp(log_floor)]][, floor_type := "sparse"]
    }
}, floor_df, floor_df_all, mc.cores = 10) %>% rbindlist()

dfs <- split(df, by = "gbd_id")
lapply(dfs, function(d) {
    saveRDS(d, sprintf("%s/t%d/gbd_inputs/%d/obs_agid_%d_sid_%d_gbdid_%d.RDS", data_dir, etmtid, mvid, agid, sid, unique(d$gbd_id)))
})
saveRDS(pred_floors, sprintf("%s/t%d/gbd_inputs/%d/floors_agid_%d_sid_%d.RDS", data_dir, etmtid, mvid, agid, sid))
message(sprintf("Inputs sucessfully saved for agid (%d) sid (%d)", agid, sid))
