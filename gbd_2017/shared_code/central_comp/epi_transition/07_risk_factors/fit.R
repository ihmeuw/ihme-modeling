rm(list=ls())
library(data.table)
library(magrittr)
library(argparse)
library(parallel)
library(boot)
library(gam)
library(assertable)

# GET ARGS ---------------------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--out_dir", help = "where are we saving things", type = "character")
parser$add_argument("--code_dir", help = "where is the code", type = "character")
parser$add_argument("--t7", help = "version of expected sevs", type = "integer")
parser$add_argument("--gbdrid", help = "gbd round id", type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

taskid <- as.numeric(Sys.getenv("SGE_TASK_ID"))
parammap <- readRDS(sprintf('%s/t7/v%d/PAFs/submitmap.RDS', out_dir, t7))[task_id == taskid]
setnames(parammap, c('age_group_id', 'sex_id', 'measure_id'), c('agid', 'sid', 'measid'))
for(col in names(parammap)){
    if(!is.character(parammap[[col]])) assign(col, eval(parse(text = as.character(parammap[[col]]))), environment()) else assign(col, as.character(parse(text = as.character(parammap[[col]]))), environment())
}
source("FILEPATH/get_demographics.R")
demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbdrid)

# PULL OBSERVED PAFS -----------------------------------------------------------

parammap <- fread(sprintf("%s/t7/v%d/model_param.csv", out_dir, t7))
model_transform <- unique(parammap$model_transform)
model_floor <- unique(parammap$model_floor)
model_family <- unique(parammap$model_family)
model_type <- unique(parammap$model_type)
use_weights <- unique(parammap$use_weights)
smoothing_param <- .7
df <- readRDS(paste0(out_dir, "/t7/v", t7, "/PAFs/gbd_inputs/obs_agid_", agid, "_sid_", sid, "_measid_", measid, ".RDS")) %>% data.table
df <- df[location_id %in% demo$location_id, ]
df[val < 1e-12, val := 1e-12][val > (1 - 1e-12), val := 1 - 1e-12]
df[, val := logit(val)]
df[, scale := model_transform]
assert_values(df, colnames = 'val', test = 'not_na')
assert_values(df, colnames = 'val', test = 'not_inf')

# MAKE FITS ----------------------------------------------------------------------

fitPredMod <- function(datadf) {

    message(paste0("Fitting ", paste(model_type, model_family, smoothing_param, use_weights, sep = ";")))
    print(datadf)
    if(!use_weights) datadf[, w := 1]

    form <- 'val ~ lo(sdi, span = smoothing_param)'
    mod <- gam(formula = as.formula(form), data = datadf, family = as.character(model_family))

    preddf <- data.table(expand.grid(etmtid = 7,
                                     etmvid = t7,
                                     age_group_id = agid,
                                     sex_id = sid,
                                     measure_id = unique(datadf$measure_id),
                                     metric_id = unique(datadf$metric_id),
                                     rei_id = unique(datadf$rei_id),
                                     cause_id = unique(datadf$cause_id),
                                     sdi = seq(0, 1, .005)))
    preddf[, pred := predict(mod, newdata = preddf, type = 'response')]
    rm(datadf)
    return(preddf)

}

df_list <- split(df, by=c("rei_id","cause_id"))
preddf <- mclapply(df_list, fitPredMod, mc.cores = 6) %>% rbindlist(., use.names = T, fill = T)

# SAVE IN NORMAL SPACE -----------------------------------------------------------------------------

preddf[, pred := inv.logit(pred)]
assert_values(preddf, colnames = 'pred', test = 'not_na')
assert_values(preddf, colnames = 'pred', test = 'gte', test_val = 0)

setnames(preddf, "pred", "paf")
preddf <- preddf[, .(etmtid, etmvid, sdi, rei_id, cause_id, sex_id, age_group_id, measure_id, metric_id, paf)]
dir.create(paste0(out_dir, "/t7/v", t7,"/PAFs/gbd_fits"), showWarnings = F)
saveRDS(preddf, sprintf('%s/t7/v%d/PAFs/gbd_fits/MEAN_agid_%d_sid_%d_measid_%d.RDs', out_dir, t7, agid, sid, measid))
