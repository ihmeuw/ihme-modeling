rm(list=ls())
library(data.table)
library(magrittr)
library(boot)
library(argparse)

# GET ARGS ---------------------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--out_dir", help = "where are we saving things", type = "character")
parser$add_argument("--code_dir", help = "where is the code", type = "character")
parser$add_argument("--t7", help = "version of expected sevs", type = "integer")
parser$add_argument("--t4", help = "version of expected mortality", type = "integer")
parser$add_argument("--t5", help = "version of expected morbidity", type = "integer")
parser$add_argument("--gbdrid", help = "gbd round id", type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

taskid <- as.numeric(Sys.getenv("SGE_TASK_ID"))
parammap <- readRDS(sprintf('%s/t7/v%d/PAFs/submitmap.RDS', out_dir, t7))[task_id == taskid]
setnames(parammap, c('age_group_id', 'sex_id', 'measure_id'), c('agid', 'sid', 'measid'))
for(col in names(parammap)){
    if(!is.character(parammap[[col]])) assign(col, eval(parse(text = as.character(parammap[[col]]))), environment()) else assign(col, as.character(parse(text = as.character(parammap[[col]]))), environment())
}

# PULL EXPECTED YLLS AND YLDS -------------------------------------------

# risk attributable ylls and ylds
erisk <- rbindlist(lapply(paste0(out_dir, "/t7/v", t7,"/PAFs/fits/MEAN_agid_", agid, "_sid_", sid, "_measid_", c(3,4), ".RDs"),
                          readRDS), use.names = T) %>% data.table

# cause specific YLLs and YLDs
eyll <- rbindlist(lapply(list.files(paste0(out_dir, "/t4/v", t4,"/YLLs"),
                                    pattern = paste0("^MEAN_agid_", agid, "_sid_", sid, "_gbdid_"), full.names=T),
                         readRDS), use.names = T, fill = T) %>% data.table
setnames(eyll, c("gbd_id", "pred"), c("cause_id", "yll_rate"))
eyll[, c("etmtid", "etmvid", "metric_id") := NULL]
eyld <- rbindlist(lapply(list.files(paste0(out_dir, "/t5/v", t5,"/raked"),
                                    pattern = paste0("^MEAN_agid_", agid, "_sid_", sid, "_gbdid_"), full.names=T),
                         readRDS), use.names = T, fill = T) %>% data.table
setnames(eyld, c("gbd_id", "pred"), c("cause_id", "yld_rate"))
eyld[, c("etmtid", "etmvid", "metric_id") := NULL]

# CALC DALYS AND BACK CALCULATE THE PAF ----------------------------------------

# risk dalys
erisk <- erisk[measure_id %in% c(3,4) & metric_id == 3, ]
erisk <- dcast(erisk, etmtid + etmvid + sdi + rei_id + cause_id + sex_id + age_group_id + metric_id ~ measure_id, value.var = "pred")
setnames(erisk, c("3", "4"), c("yld_rate", "yll_rate"))
erisk[is.na(yll_rate), yll_rate := 0][is.na(yld_rate), yld_rate := 0]
erisk[, daly_risk := yll_rate + yld_rate][, measure_id := measid]
erisk[, c("yll_rate", "yld_rate") := NULL]
# cause dalys
edaly <- merge(eyll, eyld, by = c("age_group_id", "sex_id", "cause_id", "sdi"), all = T)
edaly[is.na(yll_rate), yll_rate := 0][is.na(yld_rate), yld_rate := 0]
edaly[, daly_cause := yll_rate + yld_rate][, measure_id := measid]
edaly[, c("yll_rate", "yld_rate", "measure_id.x", "measure_id.y") := NULL]

# back calculate paf
erisk <- merge(erisk, edaly, by = c("sdi", "cause_id","sex_id","age_group_id","measure_id"))
erisk[, daly_paf := daly_risk / daly_cause][, daly_cause := NULL]
setnames(erisk, c("daly_risk","daly_paf"),c("metric_3","metric_2"))
erisk <- melt(erisk, id.vars = c("etmtid", "etmvid", "sdi", "rei_id", "cause_id", "sex_id", "age_group_id", "measure_id"),
           measure.vars = c("metric_2","metric_3"), value.name = "pred", variable.name = "metric_id")
erisk[, metric_id := as.numeric(gsub("metric_","",metric_id))]

#  SAVE -------------------------------------------------------------

saveRDS(erisk, paste0(out_dir, "/t7/v", t7,"/PAFs/fits/MEAN_agid_", agid, "_sid_", sid, "_measid_", measid, ".RDs"))
