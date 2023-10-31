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
source("FILEPATH/get_age_metadata.R")
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
decomp_step <- args[5]
out_dir <- args[6]
codcorrect_version <- as.numeric(args[7])
do_aggregation <- args[8]
add_uhc_reis <- args[9]

# MAPPING FILES ----------------------------------------------------------------
decomp_step_id <- get_decomp_step_id(decomp_step, gbd_round_id)
best_pafs <- fread(list.files(out_dir, pattern="^PAF_inputs_v", full.names = TRUE))
best_pafs[, `:=` (year_id=NULL, n_draws=NULL)]
add_meningitis <- ifelse(nrow(best_pafs[modelable_entity_name %like% "meningitis"]) > 0, TRUE, FALSE)
best_pafs <- best_pafs[!modelable_entity_name %like% "meningitis"]
rei_ids <- best_pafs$rei_id %>% unique
mvids <- best_pafs$model_version_id %>% unique
demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
draw_cols <- paste0("draw_",0:(n_draws-1))

cause_dt <- get_cause_metadata(cause_set_id=2, gbd_round_id=gbd_round_id,
                               decomp_step=decomp_step)

# READ/APPEND/RENAME/CAP -------------------------------------------------------
pull_paf <- function(mvid, year_ids) {
    message("Reading PAF model version ID ", mvid)
    paf_dt <- get_draws(gbd_id_type="rei_id", gbd_id=best_pafs[model_version_id == mvid,]$rei_id,
                        location_id=location_id, sex_id=demo$sex_id,
                        age_group_id=demo$age_group_id, source="paf",
                        version_id=mvid, n_draws=n_draws, year_id=year_ids,
                        downsample=TRUE, gbd_round_id=gbd_round_id,
                        decomp_step=decomp_step)
    return(paf_dt)
}
dt_list <- lapply(mvids, pull_paf, year_ids = year_ids)
dt <- rbindlist(dt_list, use.names=TRUE, fill=TRUE)
rows <- nrow(dt)
dt <- merge(dt, best_pafs, by = c("modelable_entity_id", "rei_id"))
missing <- setdiff(unique(best_pafs$modelable_entity_id), unique(dt$modelable_entity_id))
if (length(missing) != 0) stop("PAFs for the following risks are missing or were dropped: ", paste(missing, collapse = ", "))

# DROP THINGS ---------------------------------------------------------------
# HIV IPV is only for ages 15+
under_fifteen <- get_age_metadata(gbd_round_id=gbd_round_id)[age_group_years_start < 15]
dt <- dt[!(rei_id == 201 & age_group_id %in% under_fifteen$age_group_id), ]

# keep only needed columns
dt <- dt[, c("rei_id", "cause_id", "measure_id", "location_id", "year_id", "sex_id", "age_group_id", draw_cols), with = F]
dt <- unique(dt)

# DRUG USE/HIV PAFS-------------------------------------------------------------
drug_use_hiv <- function() {
    message("Adding drug use HIV PAFs")
    # pull unsafe sex exposure (all "Proportion HIV..." MEs), scale to 1
    exp <- get_draws(gbd_id_type = "rei_id", gbd_id = 170,
                     location_id = location_id, year_id = year_ids,
                     sex_id = demo$sex_id, age_group_id = demo$age_group_id,
                     gbd_round_id = gbd_round_id, source = "exposure",
                     decomp_step = decomp_step, n_draws = n_draws, downsample = TRUE)
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
    message("Adding smoking injury PAFs")
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
    hosp_deaths <- hosp_deaths[acause %in% acauses, .(sex_id, age_group_id, fracture,
                                                      acause, fraction)]
    hosp_deaths <- merge(hosp_deaths[, c("sex_id", "age_group_id", "acause", "fracture",
                                         "fraction"), with=F], cause_tmp, by = "acause")

    # pull ylls for all the injuries causes
    gbd_2020r1_codcorrect_step <- "step3"
    inj_yll <- get_draws(gbd_id_type = rep("cause_id", length(cause_ids)),
                         gbd_id = cause_ids, location_id = location_id, age_group_id = demo$age_group_id,
                         year_id = year_ids, measure_id = 1, sex_id = demo$sex_id,
                         gbd_round_id = gbd_round_id, source = "codcorrect",
                         decomp_step = gbd_2020r1_codcorrect_step, n_draws = n_draws, downsample = TRUE,
                         version_id = codcorrect_version)
    inj_yll <- melt(inj_yll, id.vars = c("cause_id", "location_id", "year_id",
                                         "age_group_id", "sex_id"),
                    measure.vars = paste0("draw_", 0:(n_draws - 1)),
                    variable.name = "draw", value.name = "yll")
    inj_yll[, draw := as.numeric(gsub("draw_", "", draw))]

    # compbine YLLs and hosp deaths and PAFs
    inj_paf <- merge(inj_paf[measure_id == 4, ], hosp_deaths,
                     by = c("fracture", "age_group_id", "sex_id"), allow.cartesian = TRUE)
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

# MENINGITIS PAFS --------------------------------------------------------------
meningitis_eti <- function() {
    message("Adding meningitis PAFs")
    paf <- get_draws(gbd_id_type = "modelable_entity_id",
                     gbd_id = c(10494, 10495, 10496, 24739, 24741, 24740),
                     location_id = location_id, year_id = year_ids,
                     sex_id = demo$sex_id, age_group_id = demo$age_group_id,
                     gbd_round_id = gbd_round_id, source = "epi",
                     decomp_step = decomp_step, n_draws = n_draws, downsample = TRUE)
    paf[, cause_id := 332]
    paf[, rei_id := ifelse(modelable_entity_id %in% c(10494, 24739), 188,
                           ifelse(modelable_entity_id %in% c(10495, 24741), 386, 189))]
    paf[, measure_id := ifelse(modelable_entity_id %in% c(10494, 10495, 10496), 4, 3)]
    paf <- paf[, c("rei_id", "cause_id", "measure_id", "location_id", "year_id", "sex_id", "age_group_id", draw_cols), with = F]
    return(paf)
}
if (add_meningitis) dt <- rbind(dt, meningitis_eti())

# PREP FOR PAFS OF 1 -----------------------------------------------------------
# Drop PAFs of 1 if they came through before joint calculation
paf_one <- fread("pafs_of_one.csv")[, .(rei_id, cause_id)]
paf_one[, paf_one := 1]
dt <- merge(dt, paf_one, by = c("rei_id", "cause_id"), all.x = TRUE)
dt <- dt[is.na(paf_one), ]
dt[, paf_one := NULL]

# drop any causes that aren't actual causes or aren't most detailed but flag them in the logs first!
invalid_causes <- setdiff(unique(dt$cause_id), cause_dt[most_detailed == 1, ]$cause_id)
if (length(invalid_causes) > 0) {
    warning("Dropping invalid causes found in PAFs for the following risk-cause pairs: ",
            paste(unique(dt[cause_id %in% invalid_causes, .(rei_id, cause_id)]), collapse = "-"))
    dt <- dt[!cause_id %in% invalid_causes, ]
}

# check again for missings, or numbers > 1
for (col in draw_cols) dt[is.na(get(col)), (col) := 0]
for (col in draw_cols) dt[get(col) > 1, (col) := .999999]

# APPLY CAUSE RESTRICTIONS -----------------------------------------------------
get_rstr <- function(type, dt) {
    rstr <- get_restrictions(restriction_type = type, age_group_id = demo$age_group_id,
                             cause_id = unique(dt$cause_id), sex_id = demo$sex_id,
                             measure_id = c(3,4), gbd_round_id = gbd_round_id,
                             decomp_step = decomp_step, cause_set_id = 2)
    if (type == "age") {
        rstr <- rstr[is_applicable == 1]
        setnames(rstr, "is_applicable", "drop")
        if(nrow(rstr) == 0) return(dt)
        dt <- merge(dt, rstr, by = c("age_group_id", "measure_id", "cause_id"), all.x = TRUE)
    } else {
        rstr[, drop := 1]
        if(nrow(rstr) == 0) return(dt)
        dt <- merge(dt, rstr, by = c(paste0(type, "_id"), "cause_id"), all.x = TRUE)
    }
    dt <- dt[is.na(drop), ]
    dt[, drop := NULL]
    return(dt)
}
message("Applying cause restrictions")
for(type in c("age","measure","sex")) {
    dt <- get_rstr(type, dt)
}

# JOINT PAF CALC ---------------------------------------------------------------
agg_results <- function(data, id_vars, value_vars, gbd_round_id, child_check = TRUE,
                        med = TRUE, mediation = NULL, add_uhc_reis = FALSE) {

    # rei hierarchy for aggregation is round-dependent
    rei_set_id <- ifelse(gbd_round_id < 7, 2, 13)

    # make rei map with parent and children risks
    rei_meta <- get_rei_metadata(rei_set_id = rei_set_id, gbd_round_id = gbd_round_id,
                             decomp_step = decomp_step)
    # for lbwsga and air we estimate the joint directly
    rei_meta <- rei_meta[!rei_id %in% c(334, 335, 86, 87), ]
    rei_meta[rei_id %in% c(339, 380), most_detailed := 1]

    # collapse hierarchical tree to two level tree, parent to all most-detailed children
    reis <- rei_meta[, .(parent_id=as.numeric(tstrsplit(path_to_top_parent, ","))),
                     by=c("rei_id", "most_detailed")]
    reis <- reis[most_detailed == 1 | rei_id == parent_id, ]

    # add on uhc risk aggregates for UHC
    uhc_rei_meta <- list(
        # UHC - all risk factors minus fpg/ldl/sbp
        "378" = rei_meta[(most_detailed == 1) & !(rei_id %in% c(105, 367, 107)), ]$rei_id,
        # UHC - all risk factors minus fpg/ldl/sbp and air pollution
        "403" = rei_meta[(most_detailed == 1) & !(rei_id %in% c(105, 367, 107)) &
                         !(path_to_top_parent %like% ",85,"), ]$rei_id
    )
    uhc_reis <- lapply(names(uhc_rei_meta), function(x) {
        data.table(
            rei_id = c(as.integer(x), uhc_rei_meta[[x]]),
            most_detailed = c(0, rep(1, times = length(uhc_rei_meta[[x]]))),
            parent_id = as.integer(x)
        )
    }) %>% rbindlist(., use.names = TRUE)
    if (add_uhc_reis) reis <- rbind(reis, uhc_reis)

    # make sure parent doesn't already exist in the dataset
    rei_list <- reis[!(rei_id %in% unique(data$rei_id)), ]$rei_id %>% unique
    # check that all the children are present
    child_list <- reis[parent_id %in% rei_list & most_detailed == 1, ]$rei_id %>% unique
    child_exist <- data[rei_id %in% child_list, ]$rei_id %>% unique
    missing_list <- setdiff(child_list, child_exist)
    if(child_check & length(missing_list) != 0) {
        stop("The child reis [", paste(missing_list, collapse = ", "), "] are missing")
    }

    # loop
    calc_parent <- function(parent) {
        message("... aggregating rei ID ", parent)
        child_list <- reis[parent_id == parent & most_detailed == 1, ]$rei_id %>% unique
        agg <- merge(data, reis[rei_id %in% child_list & parent_id == parent, ],
                     by="rei_id", allow.cartesian=TRUE)
        # mediation
        if (med) {
            med_tmp <- mediation[rei_id %in% child_list & med_id %in% child_list, ]
            med_tmp <- merge(med_tmp, unique(agg[,.(rei_id,cause_id,measure_id,sex_id,age_group_id,most_detailed,parent_id)]),
                             by=c("rei_id","cause_id"), all.x=TRUE, allow.cartesian = TRUE)
            setnames(med_tmp, c("most_detailed","parent_id"),c("r_md","r_parent"))
            med_tmp <- merge(med_tmp, unique(agg[,.(rei_id,cause_id,measure_id,sex_id,age_group_id,most_detailed,parent_id)]),
                             by.x=c("med_id","cause_id","measure_id","sex_id","age_group_id"),
                             by.y=c("rei_id","cause_id","measure_id","sex_id","age_group_id"),
                             all.x=TRUE, allow.cartesian = TRUE)
            setnames(med_tmp, c("most_detailed","parent_id"),c("m_md","m_parent"))
            med_tmp <- med_tmp[m_parent == r_parent]
            setnames(med_tmp,"r_parent","parent_id")
            med_tmp <- med_tmp[, list(mean_mediation=1 - prod(1-mean_mediation)),
                               by = c("rei_id", "cause_id", "parent_id", "measure_id", "sex_id", "age_group_id")]
            agg <- merge(agg, med_tmp, by = c("rei_id", "cause_id", "parent_id", "measure_id", "sex_id", "age_group_id"), all.x = TRUE)
            # if 100% mediated, PAF -> 0
            agg[mean_mediation == 1, (draw_cols) := 0]
            # if otherwise mediated, use unmediated PAF
            agg[!is.na(mean_mediation) & mean_mediation != 1,
                (draw_cols) := lapply(1:n_draws, function(draw) (med_cols[draw] %>% get))]
        }
        # aggregation
        if (any(med_cols %in% names(agg))) agg[, (med_cols) := NULL]
        rei_collapse_vars <- c(id_vars[!id_vars %in% "rei_id"], "parent_id")
        agg <- agg[, lapply(.SD, function(x) 1-prod(1-x)), .SDcols = value_vars, by = rei_collapse_vars]
        setnames(agg, "parent_id", "rei_id")
        return(agg)
    }
    agg_full <- rbindlist(lapply(unique(reis[most_detailed == 0, ]$parent_id), calc_parent), use.names = TRUE)
    data <- rbindlist(list(data, agg_full), use.names = TRUE, fill = TRUE)
    data[, rei_id := as.integer(as.character(rei_id))]
    return(data)
}

if (do_aggregation) {
    message("Calculating risk aggregate PAFs")
    mediation <- fread("FILEPATH/mediation_matrix_draw_gbd_2020.csv")
    mediation <- mediation[, c("rei_id", "cause_id", "med_id", "mean_mediation"), with = F]
    risks_with_unmed_pafs <- mediation[
        , .(mean_mediation = 1 - prod(1 - mean_mediation)),
        by=c("rei_id", "cause_id")][mean_mediation != 1, ]$rei_id %>% unique %>% as.list
    med_cols <- paste0("med_", 0:(n_draws-1))
    med_dt <- rbindlist(lapply(risks_with_unmed_pafs, function(x) {
        get_draws("rei_id", x, source="paf_unmediated", location_id=location_id,
                  year_id=year_ids, gbd_round_id=gbd_round_id, decomp_step=decomp_step,
                  n_draws=n_draws, downsample=TRUE)
    }), use.names = TRUE)
    med_dt <- med_dt[, c("rei_id", "cause_id", "measure_id", "location_id", "year_id",
                         "sex_id", "age_group_id", draw_cols), with = F]
    setnames(med_dt, draw_cols, med_cols)
    dt <- merge(dt, med_dt, by= c("rei_id", "cause_id", "measure_id", "location_id",
                                  "year_id", "sex_id", "age_group_id"), all.x=TRUE)
    dt <- agg_results(dt, id_vars = c("rei_id", "cause_id", "measure_id", "location_id",
                                      "year_id", "sex_id", "age_group_id"),
                      value_vars = draw_cols, gbd_round_id = gbd_round_id,
                      child_check = TRUE, mediation = mediation, med = TRUE,
                      add_uhc_reis = add_uhc_reis)
}
dt <- dt[, c("rei_id","cause_id", "location_id", "year_id", "age_group_id", "sex_id", "measure_id", draw_cols), with = F]

#  ADD PAFS OF 1 ---------------------------------------------------------------
message("Adding PAFs of 100%")
if (do_aggregation) {
    paf_one <- agg_results(paf_one, id_vars = c("rei_id", "cause_id"),
                           value_vars = "paf_one", gbd_round_id = gbd_round_id,
                           child_check = FALSE, med = FALSE, add_uhc_reis = add_uhc_reis)
}
paf_one[, location_id := location_id][, paf_one := NULL]
paf_one <- merge(paf_one, data.table(expand.grid(location_id = location_id,
                                                 year_id = year_ids,
                                                 age_group_id = demo$age_group_id,
                                                 sex_id = demo$sex_id,
                                                 measure_id = c(3,4))),
                 by = "location_id", allow.cartesian = TRUE)
paf_one[, (draw_cols) := 1]
# unsafe sex should start at age 10, but syphilis has burden for younger ages, drop those
under_ten <- get_age_metadata(gbd_round_id=gbd_round_id)[age_group_years_start < 10]
paf_one <- paf_one[!(cause_id == 394 & age_group_id %in% under_ten$age_group_id), ]
# Drop joint pafs of 1 if they slip through
dt <- merge(dt, unique(paf_one[,.(rei_id,cause_id)])[, paf_one :=1], by = c("rei_id", "cause_id"), all.x = TRUE)
dt <- dt[is.na(paf_one), ]
dt[, paf_one := NULL]
rei_ids <- dt$rei_id %>% unique
dt <- rbind(dt, paf_one[rei_id %in% rei_ids])

dt <- dt[, c("rei_id","cause_id", "location_id", "year_id", "age_group_id", "sex_id", "measure_id", draw_cols), with = F]

# APPLY CAUSE RESTRICTIONS -----------------------------------------------------
for(type in c("age","measure","sex")) {
    dt <- get_rstr(type, dt)
}

# SAVE -------------------------------------------------------------------------
message("Saving output files")
# check again for missings, or numbers > 1
for (col in draw_cols) dt[is.na(get(col)), (col) := 0]
for (col in draw_cols) dt[get(col) > 1, (col) := .999999]
write_paf <- function(dt, y) {
    message("Saving year ID ", y)
    write_csv(dt[year_id == y, ], file=gzfile(paste0(out_dir, "/", location_id, "_", y, ".csv.gz")))
}
for (year_id in year_ids) write_paf(dt=dt, y=year_id)

if (location_id==101) {
    write_csv(unique(dt[, .(rei_id, cause_id, measure_id, sex_id)]),
              file=gzfile(paste0(out_dir, "/existing_reis.csv.gz")))
    write_csv(data.table(paf_version=as.integer(tail(strsplit(out_dir, "/")[[1]], n = 1)),
                         decomp_step_id=decomp_step_id,
                         gbd_round_id=gbd_round_id),
              file=paste0(out_dir, "/version.csv"))
}
