# load libraries and functions
library(data.table)
library(magrittr)
library(ini)
library(RMySQL)
library(parallel)
library(readr)

source("./utils/data.R")
source("./utils/db.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_rei_metadata.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_restrictions.R")
source("FILEPATH/interpolate.R")

set.seed(124535)

#-- SET UP ARGS ----------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)
task_id <- Sys.getenv("SGE_TASK_ID") %>% as.numeric
params <- fread(args[1])[task_id, ]

location_id <- unique(params$location_id)
year_ids <- eval(parse(text = args[2]))
n_draws <- as.numeric(args[3])
gbd_round_id <- as.numeric(args[4])
out_dir <- args[5]

# MAPPING FILES ----------------------------------------------------------------------------------

best_pafs <- query(paste0(
    "SELECT model_version_id, modelable_entity_id, rei_id, rei
    FROM epi.model_version
    JOIN epi.modelable_entity USING (modelable_entity_id)
    JOIN epi.modelable_entity_rei USING (modelable_entity_id)
    JOIN shared.rei USING (rei_id)
    JOIN (SELECT modelable_entity_id FROM epi.modelable_entity_metadata
          WHERE modelable_entity_metadata_type_id = 26
          and modelable_entity_metadata_value = 1
          and last_updated_action != 'DELETE') gbd using(modelable_entity_id)
    JOIN (SELECT modelable_entity_id FROM epi.modelable_entity_metadata
          WHERE modelable_entity_metadata_type_id = 17
          and modelable_entity_metadata_value = 'paf'
          and last_updated_action != 'DELETE') paf using(modelable_entity_id)
    WHERE model_version_status_id = 1 and gbd_round_id = ", gbd_round_id), "epi")
rei_ids <- best_pafs$rei_id %>% unique
demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
draw_cols <- paste0("draw_",0:(n_draws-1))

cause_dt <- get_cause_metadata(cause_set_id=3, gbd_round_id=gbd_round_id)

# READ/APPEND/RENAME/CAP ----------------------------------------------------------------------------------

pull_paf <- function(rid, year_ids){
    paf_dt <- fread(paste0("FILEPATH/", rid, "/", location_id, ".csv"))
    paf_dt <- paf_dt[year_id %in% year_ids, ]
    return(paf_dt)
}
dt <- rbindlist(mclapply(rei_ids, pull_paf, year_ids = year_ids, mc.cores=10), use.names=T, fill=T)
rows <- nrow(dt)
dt <- merge(dt, best_pafs, by = c("modelable_entity_id", "rei_id"))
missing <- setdiff(unique(best_pafs$modelable_entity_id), unique(dt$modelable_entity_id))
if (length(missing) != 0) stop("PAFs for the following risks are missing or were dropped: ", paste(missing, collapse = ", "))

# DROP THINGS ---------------------------------------------------------------

# if PAF < 0 (bc the RR < 1), replace with 0 unless a risk where we allow protective effects
for (col in draw_cols) dt[!((rei_id == 99 & cause_id == 544) | # smoking parkinson
                                (rei_id == 370 & cause_id == 429 & age_group_id < 15) | # bmi/breast cancer in pre-menopausal women
                                (rei_id == 102 & cause_id %in% c(954,934,946,947,322,423,493,495,
                                                                 496,497,498,535,976,725,726,941,727)) | # alcohol and certain outcomes
                                rei %like% "eti_") & # etiologies
                              get(col) < 0, (col) := 0]
# BMI for breast cancer in pre-menopausal women should only be protective
for (col in draw_cols) dt[(rei_id ==370 & cause_id == 429 & age_group_id < 15) & get(col) > 0, (col) := 0]
# custom PAF for HIV IPV is only for ages 15+
dt <- dt[!(rei_id == 201 & age_group_id < 8), ]

# keep only needed columns
dt <- dt[, c("rei_id", "cause_id", "measure_id", "location_id", "year_id", "sex_id", "age_group_id", draw_cols), with = F]

# DRUG USE/HIV PAFS-------------------------------------------------------------

drug_use_hiv <- function() {

    # pull unsafe sex exposure (all "Proportion HIV..." MEs), scale to 1
    exp <- get_draws(gbd_id_type = "rei_id", gbd_id = 170,
                     location_id = location_id, year_id = year_ids,
                     sex_id = demo$sex_id, age_group_id = demo$age_group_id,
                     gbd_round_id = gbd_round_id, source = "exposure")
    exp <- melt(exp, id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                                 "modelable_entity_id"),
                measure.vars = draw_cols,
                variable.name = "draw", value.name = "exp_mean")
    exp[, draw := as.numeric(gsub("draw_", "", draw))]
    exp[, exp_total := sum(exp_mean), by = c("location_id", "year_id", "age_group_id",
                                             "sex_id", "draw")]
    exp[, exp_mean := exp_mean/exp_total][, exp_total := NULL]
    exp <- dcast(exp, modelable_entity_id + location_id + year_id + sex_id + age_group_id ~ draw,
                 value.var = "exp_mean")
    setnames(exp, paste0(0:(n_draws - 1)), draw_cols)

    # find most detailed hiv outcomes
    cause_ids <- unique(cause_dt[acause %like% "hiv_" & level == 4, ]$cause_id)

    # keep the proportion HIV due to intravenous drug use and assign outcomes
    paf <- exp[modelable_entity_id == 20953, ]
    paf <- paf[, .(cause_id = cause_ids), by=c("age_group_id", "sex_id", "location_id",
                                               "year_id", draw_cols)]
    # duplicate for morbidity and mortality
    paf[, rei_id := 138]
    paf <- paf[, .(measure_id = c(3,4)), by = c("rei_id", "cause_id", "location_id", "year_id", "sex_id", "age_group_id", draw_cols)]

    return(paf)
}
if(138 %in% rei_ids) dt <- rbind(dt, drug_use_hiv())

# ADD SMOKING INJURIES ---------------------------------------------------------

smoking_inj <- function() {

    # subest paf dt to only where we have hip/non-hip fractures to turn to injuries
    inj_paf <- dt[cause_id %in% c(878, 923) & rei_id == 99, ]
    if(nrow(inj_paf) == 0) return(data.table())
    inj_paf <- melt(inj_paf, id.vars = c("rei_id", "cause_id", "measure_id", "location_id", "year_id", "sex_id", "age_group_id"),
                    measure.vars = draw_cols, variable.name = "draw", value.name = "paf")
    inj_paf[, draw := as.numeric(gsub("draw_", "", draw))]
    inj_paf[, fracture := ifelse(cause_id == 878, "hip", "non-hip")][, cause_id := NULL]
    ages <- unique(inj_paf$age_group_id)

    #---- SET CAUSE MAPS -------------------------------------------------------

    # acause and cause_ids for injury outcomes we use
    acauses <- c("inj_trans_road_pedest", "inj_trans_road_pedal", "inj_trans_road_2wheel",
                 "inj_trans_road_4wheel", "inj_trans_road_other", "inj_trans_other",
                 "inj_falls", "inj_mech_other", "inj_animal_nonven", "inj_homicide_other")
    cause_tmp <- cause_dt[acause %in% acauses, .(cause_id, acause)]
    cause_ids <- unique(cause_tmp$cause_id)

    #-- CONVERT ----------------------------------------------------------------

    # pull proportion hospital deaths
    hosp_deaths <- fread("FILEPATH/gbd2017_proportions_of_hospital_deaths.csv")
    setnames(hosp_deaths, c("acause", "inj"), c("fracture", "acause"))
    hosp_deaths <- hosp_deaths[acause %in% acauses, .(sex_id, age_group_id, fracture,
                                                      acause, fraction)]
    hosp_deaths <- merge(hosp_deaths[, c("sex_id", "age_group_id", "acause", "fracture",
                                         "fraction"), with=F], cause_tmp, by = "acause")

    # pull ylLs for all the injuries causes
    inj_yll <- get_draws(gbd_id_type = rep("cause_id", length(cause_ids)),
                         gbd_id = cause_ids, location_id = location_id, age_group_id = demo$age_group_id,
                         year_id = year_ids, measure_id = 1, sex_id = demo$sex_id,
                         gbd_round_id = gbd_round_id, source = "codcorrect", version_id=89)
    inj_yll <- melt(inj_yll, id.vars = c("cause_id", "location_id", "year_id",
                                         "age_group_id", "sex_id"),
                    measure.vars = paste0("draw_", 0:(n_draws - 1)),
                    variable.name = "draw", value.name = "yll")
    inj_yll[, draw := as.numeric(gsub("draw_", "", draw))]

    # compbine YLLs and hosp deaths and PAFs
    inj_paf <- merge(inj_paf[measure_id == 4, ], hosp_deaths,
                     by = c("fracture", "age_group_id", "sex_id"), allow.cartesian = T)
    inj_paf <- merge(inj_paf, inj_yll, by = c("cause_id", "location_id", "year_id",
                                              "age_group_id", "sex_id", "draw"))
    inj_paf[, paf := paf * yll * fraction]
    # collapse over hip non hip
    inj_paf <- inj_paf[, .(paf = sum(paf)), by = c("cause_id", "location_id", "year_id",
                                                   "age_group_id", "sex_id", "draw")]
    # divide by original # ylls by cause
    inj_paf <- merge(inj_paf, inj_yll, by = c("cause_id", "location_id", "year_id",
                                              "age_group_id", "sex_id", "draw"))
    inj_paf[, paf := paf/yll][yll == 0, paf := 0][, yll := NULL]
    inj_paf <- inj_paf[age_group_id %in% ages]

    #-- RETURN ALL PAFS --------------------------------------------------------
    inj_paf <- inj_paf[, .(measure_id = c(3,4)), by = c("cause_id", "location_id", "year_id",
                                                        "age_group_id", "sex_id", "draw", "paf")]
    inj_paf[, rei_id := 99]
    inj_paf <- dcast(inj_paf, rei_id + cause_id + location_id + year_id + sex_id + age_group_id + measure_id ~ draw,
                     value.var = "paf")
    setnames(inj_paf, paste0(0:(n_draws - 1)), draw_cols)
    return(inj_paf)

}
if (99 %in% rei_ids) dt <- rbind(dt[!(cause_id %in% c(878, 923)), ], smoking_inj())

# PREP FOR PAFS OF 1 -----------------------------------------------------------

# Drop PAFs of 1 if they came through before joint calculation
paf_one <- fread("pafs_of_one.csv")[, .(rei_id, cause_id)]
paf_one[, paf_one := 1]
dt <- merge(dt, paf_one, by = c("rei_id", "cause_id"), all.x = T)
dt <- dt[is.na(paf_one), ]
dt[, paf_one := NULL]

# check for missings, or numbers >= 1 or < -1
for (col in draw_cols) dt[is.na(get(col)), (col) := 0]
for (col in draw_cols) dt[get(col) >= 1, (col) := .999999]
for (col in draw_cols) dt[get(col) <= -1, (col) := -.999999]

# JOINT PAF CALC ---------------------------------------------------------------

agg_results <- function(data, id_vars, value_vars, gbd_round_id, child_check = T, med = T) {

    # make rei map with parent and children risks
    reis <- get_rei_metadata(rei_set_id = 2, gbd_round_id = gbd_round_id)
    # for lbw/sg and air we estimate the joint directly
    reis <- reis[!rei_id %in% c(334, 335, 86, 87), ]
    reis[rei_id %in% c(339, 380), most_detailed := 1]
    parent_cols <- paste0("parent_",min(reis$level):max(reis$level))
    reis <- reis[, .(rei_id, path_to_top_parent, most_detailed)]
    reis[, (parent_cols) := tstrsplit(path_to_top_parent, ",")][, path_to_top_parent := NULL]
    reis <- melt(reis, id.vars = c("rei_id", "most_detailed"), value.vars = parent_cols,
                 value.name = "parent_id", variable.name = "level")
    reis[, parent_id := as.integer(parent_id)]
    reis <- reis[!is.na(parent_id) & (most_detailed == 1 | rei_id == parent_id), ]
    reis[, level := NULL]
    # add on sdg amenable PAF
    sdg <- get_rei_metadata(rei_set_id = 8, gbd_round_id = gbd_round_id)
    parent_cols <- paste0("parent_",min(sdg$level):max(sdg$level))
    sdg <- sdg[, .(rei_id, path_to_top_parent, most_detailed)]
    sdg[, (parent_cols) := tstrsplit(path_to_top_parent, ",")][, path_to_top_parent := NULL]
    sdg <- melt(sdg, id.vars = c("rei_id", "most_detailed"), value.vars = parent_cols,
                value.name = "parent_id", variable.name = "level")
    sdg[, parent_id := as.integer(parent_id)]
    sdg <- sdg[!is.na(parent_id) & (most_detailed == 1 | rei_id == parent_id) & parent_id == 378, ]
    sdg[, level := NULL]
    reis <- rbind(sdg, reis)
    # add overlap PAFs
    donut <- reis[parent_id %in% c(202,203,104) & most_detailed == 1]
    donut[parent_id %in% c(104,202), "246" := 1][parent_id %in% c(202,203), "247" := 1][parent_id %in% c(104,203), "248" := 1]
    donut <- melt(donut, id.vars = c("rei_id","most_detailed","parent_id"),na.rm = T)
    donut[, parent_id := variable][, c("variable", "value") := NULL]
    donut <- rbind(donut, data.table(rei_id = c(246, 247, 248),
                                     most_detailed = 0,
                                     parent_id = c(246, 247, 248)))
    reis <- rbind(donut, reis)

    if (med) {
        mediation <- fread("FILEPATH/mediation_matrix_draw_gbd_2017.csv")
        mediation <- mediation[, c("rei_id", "cause_id", "med_id", draw_cols), with = F]
        med_cols <- paste0("med_", 0:(n_draws-1))
        setnames(mediation, draw_cols, med_cols)
    }

    rei_collapse_vars <- c(id_vars[!id_vars %in% "rei_id"], "parent_id")
    # make sure parent doesn't already exist in the datset
    rei_list <- reis[!(rei_id %in% unique(data$rei_id)), ]$rei_id %>% unique
    # check that all the children are present
    child_list <- reis[parent_id %in% rei_list & most_detailed == 1, ]$rei_id %>% unique
    child_exist <- data[rei_id %in% child_list, ]$rei_id %>% unique
    missing_list <- setdiff(child_list, child_exist)
    if(child_check & length(missing_list) != 0) {
        stop("The child reis [", paste(missing_list, collapse = ", "), "] are missing")
    }
    agg <- merge(data, reis[rei_id %in% child_list & parent_id %in% rei_list, ], by="rei_id", allow.cartesian=T)
    setkeyv(agg, rei_collapse_vars)

    # mediation
    if (med) {
        med_tmp <- mediation[rei_id %in% child_list & med_id %in% child_list, ]
        med_tmp <- merge(med_tmp, reis, by="rei_id", all.x=T, allow.cartesian = T)
        setnames(med_tmp, c("most_detailed","parent_id"),c("r_md","r_parent"))
        med_tmp <- merge(med_tmp, reis, by.x="med_id", by.y="rei_id", all.x=T, allow.cartesian = T)
        setnames(med_tmp, c("most_detailed","parent_id"),c("m_md","m_parent"))
        med_tmp <- med_tmp[m_parent == r_parent]
        setnames(med_tmp,"r_parent","parent_id")
        med_tmp <- med_tmp[, lapply(.SD, function(x) prod(1-x)), .SDcols = med_cols, by = c("rei_id", "cause_id", "parent_id")]
        agg <- merge(agg, med_tmp, by = c("cause_id", "rei_id","parent_id"), all.x = T)
        for (col in med_cols) agg[is.na(get(col)), (col) := 1]
        agg[, (draw_cols) := lapply(1:n_draws, function(draw)
            (draw_cols[draw] %>% get * med_cols[draw] %>% get))]
    }

    # aggregation
    agg <- agg[, lapply(.SD, function(x) 1-prod(1-x)), .SDcols = value_vars, by = rei_collapse_vars]
    setnames(agg, "parent_id", "rei_id")
    data <- rbindlist(list(data, agg), use.names = T, fill = T)
    data[, rei_id := as.integer(as.character(rei_id))]
    return(data)
}
dt <- agg_results(dt, id_vars = c("rei_id", "cause_id", "measure_id", "location_id",
                                  "year_id", "sex_id", "age_group_id"),
                  value_vars = draw_cols, gbd_round_id = gbd_round_id,
                  med = T, child_check = T)

#  ADD PAFS OF 1 ---------------------------------------------------------------

paf_one <- agg_results(paf_one, id_vars = c("rei_id", "cause_id"),
                       value_vars = "paf_one", gbd_round_id = gbd_round_id,
                       child_check = F, med = F)
paf_one[, location_id := location_id][, paf_one := NULL]
paf_one <- merge(paf_one, data.table(expand.grid(location_id = location_id,
                                                 year_id = year_ids,
                                                 age_group_id = demo$age_group_id,
                                                 sex_id = demo$sex_id,
                                                 measure_id = c(3,4))),
                 by = "location_id", allow.cartesian = T)
paf_one[, (draw_cols) := 1]

# unsafe sex should start at age 10, regardless of if it is 100% attributable
paf_one <- paf_one[!(cause_id == 394 & age_group_id < 7), ]

dt <- merge(dt, unique(paf_one[,.(rei_id,cause_id)])[, paf_one :=1], by = c("rei_id", "cause_id"), all.x = T)
dt <- dt[is.na(paf_one), ]
dt[, paf_one := NULL]
rei_ids <- dt$rei_id %>% unique
dt <- rbind(dt, paf_one[rei_id %in% rei_ids])
dt <- dt[, c("rei_id","cause_id", "location_id", "year_id", "age_group_id", "sex_id", "measure_id", draw_cols), with = F]

# APPLY CAUSE RESTRICTIONS -----------------------------------------------------

get_rstr <- function(type, dt) {
    rstr <- get_restrictions(restriction_type = type, age_group_id = demo$age_group_id,
                             cause_id = unique(dt$cause_id), sex_id = demo$sex_id,
                             measure_id = c(3,4), gbd_round_id = gbd_round_id)
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
for(type in c("age","measure","sex")) {
    dt <- get_rstr(type, dt)
}

#  SAVE ------------------------------------------------------------------------

write_paf <- function(dt, y) {
    write_csv(dt[year_id == y, ], path=gzfile(paste0(out_dir, "/", location_id, "_", y, ".csv.gz")))
}
for (y in year_ids) write_paf(dt, y)

# write out file for the burdenator to determine what risk/cause/measure/sex combos are present
if(location_id==101) write_csv(unique(dt[, .(rei_id, cause_id, measure_id, sex_id)]),
                               path=gzfile(paste0(out_dir, "/existing_reis.csv.gz")))
