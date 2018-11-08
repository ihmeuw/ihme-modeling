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
parser$add_argument("--compare_version", help = "compare version of sevs", type = "integer")
parser$add_argument("--gbdrid", help = "gbd round id", type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

source("FILEPATH/db.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/get_outputs.R")

taskid <- as.numeric(Sys.getenv("SGE_TASK_ID"))
parammap <- readRDS(sprintf('%s/t7/v%d/PAFs/submitmap.RDS', out_dir, t7))[task_id == taskid]
setnames(parammap, c('age_group_id', 'sex_id', 'measure_id'), c('agid', 'sid', 'measid'))
for(col in names(parammap)){
    if(!is.character(parammap[[col]])) assign(col, eval(parse(text = as.character(parammap[[col]]))), environment()) else assign(col, as.character(parse(text = as.character(parammap[[col]]))), environment())
}
cause_dt <- get_cause_metadata(3, gbd_round_id = gbdrid)

# GET PAFS ---------------------------------------------------------------------

# query results
rc <- fread(paste0(out_dir, "/t7/v", t7,"/PAFs/gbd_inputs/risk_cause.csv"))
mvids <- rbindlist(lapply(unique(rc$rei_id), function(x)
    get_rei_mes(x, gbd_round_id=gbdrid)[draw_type=="paf"][, .(rei_id, modelable_entity_id, model_version_id)]), use.names=T)
df <- rbindlist(lapply(paste0("FILEPATH/summaries.csv"),
                       fread), use.names=T)
df <- df[age_group_id == agid & sex_id == sid & measure_id == measid, ]
setnames(df, "mean", "val")
df[, metric_id := 2]
df <- merge(df, mvids, by="modelable_entity_id")[, .(rei_id, cause_id, location_id, year_id, age_group_id, sex_id, measure_id, metric_id, val, upper, lower)]
df <- rbind(df[cause_id %in% c(878, 923, 2141, 2145)],
            merge(df, rc[, .(rei_id, cause_id)], by=c("rei_id", "cause_id")))

# PAF AGG ----------------------------------------------------------------

# cap at 0
df[val < 0, val := 0][upper < 0, upper := 0][lower < 0, lower := 0]
df <- df[!(rei_id == 201 & age_group_id < 8), ]
df <- df[!(rei_id == 102 & cause_id == 704), ]
df <- df[!cause_id %in% c(954,840,841,933), ]

drug_use_hiv <- function() {
    exp <- rbindlist(lapply(get_rei_mes(170, gbd_round_id=gbdrid)[draw_type=="exposure"]$modelable_entity_id, function(x)
        get_model_results(gbd_team="epi",gbd_id=x,age_group_id = agid,sex_id = sid)[, modelable_entity_id := x]), use.names=T)
    exp[, total := sum(mean), by = c("location_id", "year_id", "age_group_id", "sex_id")]
    exp[, mean := mean/total][, total := NULL]
    exp[, total := sum(lower), by = c("location_id", "year_id", "age_group_id", "sex_id")]
    exp[, lower := lower/total][, total := NULL]
    exp[, total := sum(upper), by = c("location_id", "year_id", "age_group_id", "sex_id")]
    exp[, upper := upper/total][, total := NULL]
    cause_ids <- unique(cause_dt[acause %like% "hiv_" & level == 4, ]$cause_id)
    paf <- exp[modelable_entity_id == 20953, ]
    paf <- paf[, .(cause_id = cause_ids), by=c("age_group_id", "sex_id", "location_id",
                                               "year_id", "mean", "lower", "upper")]
    paf[, rei_id := 138][, measure_id := measid][, metric_id := 2]
    setnames(paf, "mean", "val")
    paf <- paf[, .(rei_id, cause_id, location_id, year_id, age_group_id, sex_id, measure_id, metric_id, val, upper, lower)]
    return(paf)
}
if(138 %in% unique(rc$rei_id)) df <- rbind(df, drug_use_hiv())

smoking_inj <- function() {
    inj_paf <- df[cause_id %in% c(878, 923) & rei_id == 99, ]
    if(nrow(inj_paf) == 0) return(data.table())
    inj_paf[, fracture := ifelse(cause_id == 878, "hip", "non-hip")][, cause_id := NULL]
    ages <- unique(inj_paf$age_group_id)
    acauses <- c("inj_trans_road_pedest", "inj_trans_road_pedal", "inj_trans_road_2wheel",
                 "inj_trans_road_4wheel", "inj_trans_road_other", "inj_trans_other",
                 "inj_falls", "inj_mech_other", "inj_animal_nonven", "inj_homicide_other")
    cause_tmp <- cause_dt[acause %in% acauses, .(cause_id, acause)]
    cause_ids <- unique(cause_tmp$cause_id)
    hosp_deaths <- fread("FILEPATH/gbd2017_proportions_of_hospital_deaths.csv")
    setnames(hosp_deaths, c("acause", "inj"), c("fracture", "acause"))
    hosp_deaths <- hosp_deaths[acause %in% acauses, .(sex_id, age_group_id, fracture,
                                                      acause, fraction)]
    hosp_deaths <- merge(hosp_deaths[, c("sex_id", "age_group_id", "acause", "fracture",
                                         "fraction"), with=F], cause_tmp, by = "acause")
    inj_yll <- get_outputs(topic="cause", cause_id = cause_ids, location_id = "most_detailed",
                           age_group_id = agid, year_id = 1990:2017, measure_id = 1, sex_id = sid,
                           metric_id = 1, gbd_round_id = gbdrid, compare_version_id = compare_version)
    setnames(inj_yll, c("val","lower","upper"),c("yv","yl","yu"))
    inj_paf <- merge(inj_paf, hosp_deaths,
                     by = c("fracture", "age_group_id", "sex_id"), allow.cartesian = T)
    inj_paf <- merge(inj_paf, inj_yll, by = c("cause_id", "location_id", "year_id",
                                              "age_group_id", "sex_id"))
    inj_paf[, val := val * yv * fraction][, lower := lower * yl * fraction][, upper := upper * yu * fraction]
    inj_paf <- inj_paf[, .(val = sum(val), lower = sum(lower), upper = sum(upper)), by = c("cause_id", "location_id", "year_id",
                                                   "age_group_id", "sex_id")]
    inj_paf <- merge(inj_paf, inj_yll, by = c("cause_id", "location_id", "year_id",
                                              "age_group_id", "sex_id"))
    inj_paf[, val := val/yv][yv == 0, val := 0][, yv := NULL]
    inj_paf[, lower := lower/yl][yl == 0, lower := 0][, yl := NULL]
    inj_paf[, upper := upper/yu][yu == 0, upper := 0][, yu := NULL]
    inj_paf <- inj_paf[age_group_id %in% ages]
    inj_paf[, rei_id := 99][, measure_id := measid][, metric_id := 2]
    inj_paf <- inj_paf[, .(rei_id, cause_id, location_id, year_id, age_group_id, sex_id, measure_id, metric_id, val, upper, lower)]
    return(inj_paf)

}
if (99 %in% unique(rc$rei_id)) df <- rbind(df[!(cause_id %in% c(878, 923)), ], smoking_inj())

convert_osteo <- function() {
    osteo_paf <- df[cause_id %in% c(2141, 2145) & measure_id == 3 & rei_id == 370, ]
    if(nrow(osteo_paf) == 0) return(data.table())
    setnames(osteo_paf, "cause_id", "parent_id")
    ages <- unique(osteo_paf$age_group_id)
    cause_id <- 628
    osteo_map <- query(sprintf(
        "SELECT parent_id, sequela_name, sequela_id, cause_id
        FROM epi.sequela_hierarchy_history
        JOIN (SELECT sequela_set_version_id
        FROM epi.sequela_set_version_active
        WHERE gbd_round_id = %s
        AND sequela_set_id = 2) ssvd USING(sequela_set_version_id)
        WHERE cause_id = %s", gbdrid, cause_id),
        "epi"
    )
    osteo_map[sequela_name %like% "osteoarthritis of the hip", parent_id := 2141]
    osteo_map[sequela_name %like% "osteoarthritis of the knee", parent_id := 2145]
    osteo_map <- osteo_map[, .(sequela_id, cause_id, parent_id)]
    sequela_ids <- unique(osteo_map$sequela_id)
    seq_yld <- get_outputs(topic="sequela", sequela_id = sequela_ids, location_id = "most_detailed",
                           age_group_id = agid, year_id = c(1990,2007,2017), measure_id = 3, sex_id = sid,
                           metric_id = 1, gbd_round_id = gbdrid, compare_version_id = compare_version)
    seq_yld <- merge(seq_yld, osteo_map, by="sequela_id")
    setnames(seq_yld, c("val","lower","upper"),c("sv","sl","su"))
    seq_yld[, sv_total := sum(sv), by = c("cause_id", "location_id", "year_id",
                                            "age_group_id", "sex_id")]
    seq_yld[, sl_total := sum(sl), by = c("cause_id", "location_id", "year_id",
                                          "age_group_id", "sex_id")]
    seq_yld[, su_total := sum(su), by = c("cause_id", "location_id", "year_id",
                                          "age_group_id", "sex_id")]
    cause_yld <- get_outputs(topic="cause", cause_id = cause_id, location_id = "most_detailed",
                             age_group_id = agid, year_id = c(1990,2007,2017), measure_id = 3, sex_id = sid,
                             metric_id = 1, gbd_round_id = gbdrid, compare_version_id = compare_version)
    setnames(cause_yld, c("val","lower","upper"),c("cv","cl","cu"))
    yld_dt <- merge(osteo_paf, seq_yld, by = c("parent_id", "location_id", "year_id",
                                               "age_group_id", "sex_id"))
    yld_dt <- merge(yld_dt, cause_yld, by = c("cause_id", "location_id", "year_id",
                                              "age_group_id", "sex_id"))
    yld_dt[, sv := cv * sv/sv_total][sv_total == 0, sv := 0]
    yld_dt[, val := val * sv]
    yld_dt[, sl := cl * sl/sl_total][sl_total == 0, sl := 0]
    yld_dt[, lower := lower * sl]
    yld_dt[, su := cu * su/su_total][su_total == 0, su := 0]
    yld_dt[, upper := upper * su]
    yld_dt <- yld_dt[, .(val = sum(val), lower = sum(lower), upper = sum(upper)), by = c("cause_id", "location_id", "year_id",
                                                 "age_group_id", "sex_id")]
    yld_dt <- merge(yld_dt, cause_yld, by = c("cause_id", "location_id", "year_id",
                                              "age_group_id", "sex_id"))
    yld_dt[, val := val/cv][cv == 0, val := 0][, cv := NULL]
    yld_dt[, lower := lower/cl][cl == 0, lower := 0][, cl := NULL]
    yld_dt[, upper := upper/cu][cu == 0, upper := 0][, cu := NULL]
    yld_dt <- yld_dt[age_group_id %in% ages]
    yld_dt[, rei_id := 370][, measure_id := measid][, metric_id := 2]
    yld_dt <- yld_dt[, .(rei_id, cause_id, location_id, year_id, age_group_id, sex_id, measure_id, metric_id, val, upper, lower)]
    return(yld_dt)
}
if (370 %in% unique(rc$rei_id)) df <- rbind(df[!(cause_id %in% c(2141, 2145)), ], convert_osteo())
df <- merge(df, rc[, .(rei_id, cause_id)], by=c("rei_id","cause_id"))

# VARIANCE AND SDI AND SAVE ----------------------------------------------------

# compute variance from upper and lower
df[, var := (((upper - lower) / 2) / 1.96)^2]
df[abs(var - 0) < 1e-16, var := 1e-16]

# compute weights of inverse variance
df[, w := var][w > 1, w := 1 - (0.00000001)][, w := log(1 / w)]
df[, w := w / mean(w), by = .(age_group_id, sex_id, rei_id, cause_id)]

if (all(is.nan(df$w))) df[, w := 1]
if (all(is.na(df$var))) df[, c("var", "w") := 1]
df[, scale := "none"]
sdidf <- readRDS(sprintf("%s/t7/v%d/dem_inputs/sdidf_full.RDS", out_dir, t7))
df <- merge(df, sdidf[, !"sdi_vers_id"], by = c("location_id", "year_id"))
df <- df[order(location_id, year_id, age_group_id, sex_id),
         c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "metric_id", "rei_id", "cause_id", "scale", "sdi", "val", "var", "w")]
assert_values(df, colnames = c('val',"upper","lower"), test = 'not_na')
assert_values(df, colnames = c('val',"upper","lower"), test = 'not_inf')
saveRDS(df, sprintf("%s/t7/v%d/PAFs/gbd_inputs/obs_agid_%d_sid_%d_measid_%d.RDS", out_dir, t7, agid, sid, measid))
