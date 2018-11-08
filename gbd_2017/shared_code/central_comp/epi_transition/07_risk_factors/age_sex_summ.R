rm(list=ls())
library(RMySQL)
library(DBI)
library(argparse)
library(data.table)
library(magrittr)
library(boot)
library(plyr)
library(assertable)
library(parallel)

agggregateAges <- function(aggid, startid, endid, preddf, agepropdf, sexpropdf, gbdrid, agids) {
    # Get scaled age weights for group
    if (aggid != 27) {
        agepropdf <- agepropdf[age_group_id >= startid & age_group_id <= endid][, ageprop := ageprop / sum(ageprop),
                                                                                by = c("sdi", "sex_id")]
        agedf <- merge(preddf, agepropdf, by = c("sdi", "age_group_id", "sex_id"))
        agesexpropdf <- merge(agepropdf, sexpropdf, by = c("sdi", "age_group_id", "sex_id"))
    } else {
        agepropdf <- et.getAgeWeights(gbdrid, agids)
        agepropdf <- rename(agepropdf, c("age_group_weight_value" = "ageprop"))
        agedf <- merge(preddf, agepropdf, by = "age_group_id")
        agesexpropdf <- merge(agepropdf, sexpropdf, by = "age_group_id")
    }

    # Combine aggregate
    agedf <- agedf[, age_group_id := aggid][, .(pred = weighted.mean(pred, w = ageprop)),
                                            by = eval(names(agedf)[!names(agedf) %in% c("pred", "ageprop")])]

    # Get age-sex weight
    agesexpropdf <- agesexpropdf[, agesexprop := (ageprop * sexprop) / sum(ageprop * sexprop), by = c("sdi")][, c("ageprop", "sexprop") := NULL]
    agesexdf <- merge(preddf, agesexpropdf, by = c("sdi", "age_group_id", "sex_id"))
    agesexdf <- agesexdf[, c("age_group_id", "sex_id") := .(aggid, 3)][, .(pred = weighted.mean(pred, w = agesexprop)),
                                                                       by = eval(names(agesexdf)[!names(agesexdf) %in% c("pred", "agesexprop")])]

    # Combine and return
    df <- rbind(agedf, agesexdf)
    return(df[, names(preddf), with = FALSE])
}

# GET ARGS ---------------------------------------------------------------------

parser <- ArgumentParser()
parser$add_argument("--out_dir", help = "where are we saving things", type = "character")
parser$add_argument("--code_dir", help = "where is the code", type = "character")
parser$add_argument("--t7", help = "version of expected sevs", type = "integer")
parser$add_argument("--t2", help = "version of expected age prop", type = "integer")
parser$add_argument("--t3", help = "version of expected sex prop", type = "integer")
parser$add_argument("--gbdrid", help = "gbd round id", type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

source("FILEPATH/get_demographics.R")
source(paste0(code_dir, "/helpers/data_grabber.R"))
source(paste0(code_dir, "/helpers/db_fetcher.R"))

taskid <- Sys.getenv("SGE_TASK_ID")
gbdid <- readRDS(sprintf('%s/t7/v%d/PAFs/submitmap.RDS', out_dir, t7))[task_id == taskid, unique(rei_id)]
demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbdrid)

# Load each data frame (rescale age and sex proportions)
preddf <- rbindlist(lapply(list.files(paste0(out_dir, "/t7/v", t7,"/PAFs/fits"), pattern = paste0("_gbdid_",gbdid, ".RDs$"), full.names=T),
                           readRDS), use.names = T) %>% data.table
preddf <- preddf[age_group_id %in% demo$age_group_id & sex_id %in% demo$sex_id, ]
agepropdf <- et.getProduct(etmtid = 2, etmvid = t2,
                           out_dir,
                           agids = demo$age_group_id, sids = demo$sex_id, gbdids = 0, mean = T,
                           process_dir = "fits", scale = 'normal')
agepropdf <- agepropdf[, ageprop := pred / sum(pred), by = .(sdi, sex_id)][, c("sdi", "age_group_id", "sex_id", "ageprop"), with = FALSE]
sexpropdf <- et.getProduct(etmtid = 3, etmvid = t3,
                           out_dir,
                           agids = demo$age_group_id, sids = demo$sex_id, gbdids = 0, mean = T,
                           process_dir = "fits", scale = 'normal')
sexpropdf <- sexpropdf[, sexprop := pred / sum(pred), by = .(sdi, age_group_id)][, c("sdi", "age_group_id", "sex_id", "sexprop"), with = FALSE]

# Aggregate sexes by age
bsdf <- merge(preddf,
              sexpropdf,
              by = c("sdi", "age_group_id", "sex_id"), all.x = T)
bsdf <- bsdf[, sex_id := 3][, .(pred = weighted.mean(pred, w = sexprop)), by = eval(names(bsdf)[!names(bsdf) %in% c("pred", "sexprop")])]

# Aggregate ages (and age-sex multivariate aggregate)
aggmapdf <- fread(paste0(code_dir, "/metadata/age_aggregates_gbdrid_", gbdrid, ".csv"))
aggagedf <- rbindlist(mapply(aggid = aggmapdf$agg_age_group_id,
                             startid = aggmapdf$age_group_id_start,
                             endid = aggmapdf$age_group_id_end,
                             agggregateAges,
                             MoreArgs = list(preddf,
                                             agepropdf,
                                             sexpropdf,
                                             gbdrid,
                                             demo$age_group_id),
                             SIMPLIFY = FALSE))

# Store each age/sex combination that has been created
df <- rbind(bsdf, aggagedf)
df <- df[, .(etmtid, etmvid, sdi, rei_id, cause_id, sex_id, age_group_id, measure_id, metric_id, pred)]
dfs <- split(df, by = c('age_group_id', 'sex_id'))
mclapply(dfs,
         function(df, out_dir, t7, gbdid) {
             saveRDS(df,
                     file = paste0(out_dir, "/t7/v", t7, "/PAFs/fits/MEAN_agid_", unique(df$age_group_id), "_sid_", unique(df$sex_id), "_gbdid_", gbdid, ".RDs"))
         },
         out_dir, t7, gbdid, mc.cores = 5)

# then write out summaries
df <- rbindlist(lapply(list.files(paste0(out_dir, "/t7/v", t7,"/PAFs/fits"), pattern = paste0("_gbdid_",gbdid, ".RDs$"), full.names=T),
                       readRDS), use.names = T) %>% data.table
setnames(df, "pred", "mean")
df[, c("lower", "upper") := mean]
df <- df[, .(etmtid, etmvid, rei_id, cause_id, age_group_id, sex_id, measure_id, metric_id, sdi, mean, lower, upper)][order(rei_id, cause_id, age_group_id, sex_id, measure_id, metric_id, sdi)]
dir.create(paste0(out_dir, "/t7/v", t7,"/PAFs/summaries"), showWarnings = F, recursive = T)
write.csv(df,
          file = paste0(out_dir, "/t7/v", t7, "/PAFs/summaries/gbdid_", gbdid, ".csv"),
          row.names = FALSE)

