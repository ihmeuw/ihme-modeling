##########################################################
## Aggregate ages/sexes
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
## DEFINE FUNCTIONS
source(paste0(c, "/helpers/primer.R"))

agggregateAges <- function(aggid, startid, endid, preddf, agepropdf, sexpropdf, gbdrid, agids) {
    # Get scaled age weights for group
    if (aggid != 27) {
        agepropdf <- agepropdf[age_group_id >= startid & age_group_id <= endid][, ageprop := ageprop / sum(ageprop), by = c("sdi", "sex_id")]
        agedf <- merge(preddf,
                       agepropdf,
                       by = c("sdi", "age_group_id", "sex_id")
        )
        agesexpropdf <- merge(agepropdf,
                              sexpropdf,
                              by = c("sdi", "age_group_id", "sex_id")
        )
    } else {
        agepropdf <- et.getAgeWeights(gbdrid, agids)
        agepropdf <- rename(agepropdf, c("age_group_weight_value" = "ageprop"))
        agedf <- merge(preddf,
                       agepropdf,
                       by = "age_group_id"
        )
        agesexpropdf <- merge(agepropdf,
                              sexpropdf,
                              by = "age_group_id"
        )
    }
    # Combine aggregate
    agedf <- agedf[, age_group_id := aggid][, .(pred = weighted.mean(pred, w = ageprop)), by = eval(names(agedf)[!names(agedf) %in% c("pred", "ageprop")])]

    # Get age-sex weight
    agesexpropdf <- agesexpropdf[, agesexprop := (ageprop * sexprop) / sum(ageprop * sexprop), by = c("sdi")][, c("ageprop", "sexprop") := NULL]
    agesexdf <- merge(preddf,
                      agesexpropdf,
                      by = c("sdi", "age_group_id", "sex_id")
    )
    agesexdf <- agesexdf[, c("age_group_id", "sex_id") := .(aggid, 3)][, .(pred = weighted.mean(pred, w = agesexprop)), by = eval(names(agesexdf)[!names(agesexdf) %in% c("pred", "agesexprop")])]
    # Combine and return
    df <- rbind(agedf, agesexdf)
    return(df[, names(preddf), with = FALSE])
}

# Parse arguments
parser <- ArgumentParser()
parser$add_argument("--data_dir",
                    help = "Site where data will be stored",
                    default = "FILEPATH", type = "character"
)
parser$add_argument("--agg_step",
                    help = "Which step should be read in to aggregate",
                    default = "YLLs", type = "character"
)
parser$add_argument("--etmtid",
                    help = "Model type ID",
                    default = 4, type = "integer"
)
parser$add_argument("--etmvid",
                    help = "Model version ID",
                    default = 15, type = "integer"
)
parser$add_argument("--etmvid_age",
                    help = "Model version ID for population (age)",
                    default = 5, type = "integer"
)
parser$add_argument("--etmvid_sex",
                    help = "Model version ID for population (sex)",
                    default = 5, type = "integer"
)
args <- parser$parse_args()
list2env(args, environment())
rm(args)
print("Made it")

taskid <- ifelse(interactive(), loadTask(data_dir, etmtid, etmvid), Sys.getenv("SGE_TASK_ID"))
print(taskid)
gbdid <- readRDS(sprintf("%s/t%d/v%d/submitmap.RDS", data_dir, etmtid, etmvid))[task_id == taskid, unique(gbd_id)]
agids <- c(2:20, 30:32, 235)
sids <- 1:2
gbdrid <- 5

# Load each data frame (rescale age and sex proportions)
preddf <- et.getProduct(etmtid, etmvid,
                        data_dir,
                        agids = agids, sids = sids, gbdids = gbdid, mean = T,
                        process_dir = agg_step, scale = "normal"
)
agepropdf <- et.getProduct(
    etmtid = 2, etmvid = etmvid_age,
    data_dir,
    agids = agids, sids = sids, gbdids = 0, mean = T,
    process_dir = "fits", scale = "normal"
)
agepropdf <- agepropdf[, ageprop := pred / sum(pred), by = .(sdi, sex_id)][, c("sdi", "age_group_id", "sex_id", "ageprop"), with = FALSE]
sexpropdf <- et.getProduct(
    etmtid = 3, etmvid = etmvid_sex,
    data_dir,
    agids = agids, sids = sids, gbdids = 0, mean = T,
    process_dir = "fits", scale = "normal"
)
sexpropdf <- sexpropdf[, sexprop := pred / sum(pred), by = .(sdi, age_group_id)][, c("sdi", "age_group_id", "sex_id", "sexprop"), with = FALSE]

# Aggregate sexes by age
bsdf <- merge(preddf,
              sexpropdf,
              by = c("sdi", "age_group_id", "sex_id"), all.x = T
)
bsdf <- bsdf[, sex_id := 3][, .(pred = weighted.mean(pred, w = sexprop)), by = eval(names(bsdf)[!names(bsdf) %in% c("pred", "sexprop")])]

# Aggregate ages (and age-sex multivariate aggregate)
aggmapdf <- fread(paste0(c, "/metadata/age_aggregates_gbdrid_", gbdrid, ".csv"))
aggagedf <- rbindlist(mapply(
    aggid = aggmapdf$agg_age_group_id,
    startid = aggmapdf$age_group_id_start,
    endid = aggmapdf$age_group_id_end,
    agggregateAges,
    MoreArgs = list(
        preddf,
        agepropdf,
        sexpropdf,
        gbdrid,
        agids
    ),
    SIMPLIFY = FALSE
))

# Store each age/sex combination that has been created
df <- rbind(
    bsdf,
    aggagedf
)
df <- df[, .(etmtid, etmvid, age_group_id, sex_id, measure_id, metric_id, gbd_id = as.numeric(as.character(gbd_id)), sdi, pred)]
assert_ids(df[!age_group_id %in% agids], id_vars = list(
    etmtid = etmtid, etmvid = etmvid,
    age_group_id = unique(aggagedf$age_group_id), sex_id = 1:3, gbd_id = gbdid, sdi = seq(0, 1, .005)
))
assert_ids(df[sex_id == 3], id_vars = list(
    etmtid = etmtid, etmvid = etmvid,
    age_group_id = c(agids, aggagedf$age_group_id), sex_id = 3, gbd_id = gbdid, sdi = seq(0, 1, .005)
))
dfs <- split(df, by = c("age_group_id", "sex_id"))
mclapply(dfs,
         function(df, data_dir, etmtid, etmvid, agg_step, gbdid) {
             saveRDS(df,
                     file = paste0(data_dir, "/t", etmtid, "/v", etmvid, "/", agg_step, "/MEAN_agid_", unique(df$age_group_id), "_sid_", unique(df$sex_id), "_gbdid_", gbdid, ".RDs")
             )
         },
         data_dir, etmtid, etmvid, agg_step, gbdid,
         mc.cores = et.coreNum()
)
