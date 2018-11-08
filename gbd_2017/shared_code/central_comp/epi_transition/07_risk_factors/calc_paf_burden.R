rm(list=ls())
library(data.table)
library(magrittr)
library(boot)
library(argparse)
library(assertable)

# GET ARGS ---------------------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--out_dir", help = "where are we saving things", type = "character")
parser$add_argument("--code_dir", help = "where is the code", type = "character")
parser$add_argument("--t7", help = "version of expected sevs", type = "integer")
parser$add_argument("--t4", help = "version of expected mortality", type = "integer")
parser$add_argument("--t5", help = "version of expected morbidity", type = "integer")
parser$add_argument("--compare_version", help = "compare version of sevs", type = "integer")
parser$add_argument("--rrmax", help = "version of rrmax that was used to calculate the sevs used in producing expected sevs", type = "integer")
parser$add_argument("--gbdrid", help = "gbd round id", type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

taskid <- as.numeric(Sys.getenv("SGE_TASK_ID"))
parammap <- readRDS(sprintf('%s/t7/v%d/PAFs/submitmap.RDS', out_dir, t7))[task_id == taskid]
setnames(parammap, c('age_group_id', 'sex_id', 'measure_id'), c('agid', 'sid', 'measid'))
for(col in names(parammap)){
    if(!is.character(parammap[[col]])) assign(col, eval(parse(text = as.character(parammap[[col]]))), environment()) else assign(col, as.character(parse(text = as.character(parammap[[col]]))), environment())
}

source("FILEPATH/get_outputs.R")
source("FILEPATH/get_rei_metadata.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/get_restrictions.R")
source(paste0(code_dir, "/t7/agg_risk_cause.R"))

# PULL SEV + RRMAX AND BACK CALC PAF -------------------------------------------

rei_meta <- get_rei_metadata(rei_set_id = 1, gbd_round_id = gbdrid)
rei_ids <- rei_meta[most_detailed == 1 & !(rei_id %in% c(170,131)), ]$rei_id %>% unique
if (measid == 3) rei_ids <- rei_ids[!rei_ids %in% c(88,334,335)]
if (measid == 4) rei_ids <- rei_ids[!rei_ids %in% c(130,132,363)]

# read expected SEVs for a given risk
esev <- rbindlist(lapply(paste0(out_dir, "/t7/v", t7, "/fits/MEAN_agid_", agid, "_sid_", sid, "_gbdid_", rei_ids, ".RDs"),
                         readRDS), use.names = T) %>% data.table
setnames(esev, "gbd_id", "rei_id")
esev[, etmvid := t7]

# pull RRmax values used to calculate SEV
rrmax <- rbindlist(lapply(paste0("FILEPATH/", rrmax, "/rrmax/", rei_ids, ".csv"),
                          fread), use.names = T)
rrmax[, rr := rowMeans(.SD), .SDcols = paste0("draw_",0:999)]
rrmax <- rrmax[, .(rei_id, cause_id, age_group_id, sex_id, rr)]
rrmax <- rrmax[age_group_id == agid & sex_id == sid, ]

# PAF = 1-(1/(SEV(RRmax-1)+1))
dt <- merge(esev, rrmax, by = c("age_group_id", "sex_id", "rei_id"), allow.cartesian = T)
dt[, paf := 1-(1/(pred*(rr-1)+1))]
rm(esev)

#  CORRECT PAFS ----------------------------------------------------------------

# calculate and apply a correction factor to correct the distribution across causes
gbd_paf <- get_outputs(topic = "rei", rei_id = rei_ids, cause_id = "most_detailed",
                       measure_id = measid, metric_id = 2, sex_id = sid, location_id = 1,
                       age_group_id = agid, compare_version_id = compare_version,
                       gbd_round_id = gbdrid)
setnames(gbd_paf,"val","paf")
gbd_sev <- get_outputs(topic = "rei", rei_id = rei_ids, measure_id = 29,
                       metric_id = 3, sex_id = sid, location_id = 1,
                       age_group_id = agid, compare_version_id = compare_version,
                       gbd_round_id = gbdrid)
setnames(gbd_sev,"val","sev")
gbd <- merge(gbd_paf[, .(rei_id, cause_id, age_group_id, sex_id, paf)],
             gbd_sev[, .(rei_id, age_group_id, sex_id, sev)], by = c("age_group_id", "sex_id", "rei_id"))
gbd <- gbd[!is.na(paf) & !is.na(sev), ]
gbd <- merge(gbd, rrmax, by = c("age_group_id", "sex_id", "rei_id", "cause_id"))
gbd[, back_paf := 1-(1/(sev*(rr-1)+1))]

gbd[, correct := logit(paf)-logit(back_paf)]
gbd[paf <=  0, correct := paf-back_paf]
dt <- merge(dt, gbd[, .(rei_id, cause_id, age_group_id, sex_id, correct)],
            by = c("rei_id", "cause_id", "age_group_id", "sex_id"))
rm(gbd_paf);rm(gbd_sev);rm(gbd);rm(rrmax)
dt[paf > 0, paf := inv.logit(logit(paf) + correct)]
dt[paf < 0, paf := paf + correct]
dt[paf == 0, paf := 0]

dt[, metric_id := 2][, measure_id := measid]
dt <- dt[, .(etmtid, etmvid, sdi, rei_id, cause_id, sex_id, age_group_id, measure_id, metric_id, paf)]

#  ADD ON PAF FITS FOR RO PAIRS W/O SEVS ---------------------------------------

dt <- rbind(dt, readRDS(sprintf('%s/t7/v%d/PAFs/gbd_fits/MEAN_agid_%d_sid_%d_measid_%d.RDs', out_dir, t7, agid, sid, measid)))

#  AGG RISKS ADD PAFS OF 1 -----------------------------------------------------

dt <- agg_risk(dt, id_vars = c("etmtid", "etmvid", "sdi", "rei_id", "cause_id",
                               "sex_id", "age_group_id", "measure_id", "metric_id"),
               gbd_round_id = gbdrid, med = T)

paf_one <- fread(paste0(code_dir, "/t7/pafs_of_one.csv"))[, etmtid := 7]
paf_one <- merge(paf_one, data.table(expand.grid(etmtid = 7, etmvid = t7,
                                                 sdi = seq(0,1,0.005),
                                                 sex_id = sid, age_group_id = agid,
                                                 measure_id = measid, metric_id = 2)),
                 by = "etmtid", allow.cartesian = T)
if (agid < 7) paf_one <- paf_one[cause_id != 394, ]
dt <- merge(dt, unique(paf_one[,.(rei_id,cause_id)])[, paf_one :=1], by = c("rei_id", "cause_id"), all.x = T)
dt <- dt[is.na(paf_one), ]
dt <- rbind(dt[, paf_one := NULL], paf_one)
dt <- dt[, .(etmtid, etmvid, sdi, rei_id, cause_id, sex_id, age_group_id, measure_id, metric_id, paf)]
rm(paf_one)

# apply cause restrictions
get_rstr <- function(type, dt) {
    rstr <- get_restrictions(restriction_type = type, age_group_id = agid,
                             cause_id = unique(dt$cause_id), sex_id = sid,
                             measure_id = measid, gbd_round_id = gbdrid)
    if (nrow(rstr) == 0) return(dt)
    if (type == "age") {
        rstr <- rstr[is_applicable == 1]
        setnames(rstr, "is_applicable", "drop")
        dt <- merge(dt, rstr, by = c("age_group_id", "measure_id", "cause_id"), all.x = T)
    } else {
        rstr[, drop := 1]
        dt <- merge(dt, rstr, by = c(paste0(type, "_id"), "cause_id"), all.x = T)
    }
    dt <- dt[is.na(drop), ]
    dt[, drop := NULL]
    return(dt)
}
for(type in c("age","measure","sex")) dt <- get_rstr(type, dt)
dt[, measure_id := NULL]

#  BURDENATE -------------------------------------------------------------------

# pull all expected cause specific burden
if (measid == 4) {
    # pull deaths and ylls (all mortality)
    ecause <- rbindlist(lapply(c(list.files(paste0(out_dir, "/t4/v", t4,"/YLLs"),
                                            pattern = paste0("^MEAN_agid_", agid, "_sid_", sid, "_gbdid_"), full.names=T),
                                 list.files(paste0(out_dir, "/t4/v", t4,"/raked"),
                                            pattern = paste0("^MEAN_agid_", agid, "_sid_", sid, "_gbdid_"), full.names=T)),
                               readRDS), use.names = T, fill = T) %>% data.table
    setnames(ecause, c("gbd_id", "pred"), c("cause_id", "cause_rate"))
    ecause[, c("etmtid", "etmvid", "metric_id") := NULL]
    allcause <- ecause[cause_id %in% c(295,409,687) & measure_id == 1] %>% .[, .(cause_id = 294, cause_rate = sum(cause_rate)),
                                                           by = c("age_group_id", "sex_id", "measure_id", "sdi")]
    ecause <- rbind(ecause, allcause)
    rm(allcause)
} else {
    # ylds
    ecause <- rbindlist(lapply(list.files(paste0(out_dir, "/t5/v", t5,"/raked"),
                                            pattern = paste0("^MEAN_agid_", agid, "_sid_", sid, "_gbdid_"), full.names=T),
                               readRDS), use.names = T, fill = T) %>% data.table
    setnames(ecause, c("gbd_id", "pred"), c("cause_id", "cause_rate"))
    ecause[, c("etmtid", "etmvid", "metric_id") := NULL]
}

# find risk attrib rate and agg up cause hierarchy
rate_dt <- merge(dt, ecause, by = c("age_group_id", "sex_id", "cause_id", "sdi"), all.x = T, allow.cartesian = T)
rate_dt[, risk_rate := paf * cause_rate][, c("paf", "cause_rate", "metric_id") := NULL]
rate_dt <- agg_cause(rate_dt, gbd_round_id = gbdrid)

# back calculate paf
dt <- merge(dt, rate_dt, by = c("sdi", "rei_id","cause_id","sex_id","age_group_id","etmtid","etmvid"),all=T, allow.cartesian = T)
dt <- merge(dt, ecause, by = c("sdi", "cause_id","sex_id","age_group_id","measure_id"))
dt[is.na(paf), paf := risk_rate / cause_rate][, c("cause_rate","metric_id") := NULL]
setnames(dt, c("paf","risk_rate"),c("metric_2","metric_3"))
dt <- melt(dt, id.vars = c("etmtid", "etmvid", "sdi", "rei_id", "cause_id", "sex_id", "age_group_id", "measure_id"),
           measure.vars = c("metric_2","metric_3"), value.name = "pred", variable.name = "metric_id")
dt[, metric_id := as.numeric(gsub("metric_","",metric_id))]
rm(ecause);rm(rate_dt)
assert_values(dt, colnames = 'pred', test = 'not_na')
assert_values(dt, colnames = 'pred', test = 'not_inf')

#  SAVE -------------------------------------------------------------

dir.create(paste0(out_dir, "/t7/v", t7,"/PAFs/fits"), showWarnings = F)
for(m in unique(dt$measure_id)) {
    saveRDS(dt[measure_id == m, ], paste0(out_dir, "/t7/v", t7,"/PAFs/fits/MEAN_agid_", agid, "_sid_", sid, "_measid_", m, ".RDs"))
}
