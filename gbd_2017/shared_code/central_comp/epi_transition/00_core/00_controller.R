##########################################################
## Epi transition controller
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
source(paste0(c, "helpers/primer.R"))

# Parse arguments
parser <- ArgumentParser()
parser$add_argument("--data_dir",
                    help = "Site where data will be stored",
                    default = "FILEPATH", type = "character"
)
parser$add_argument("--etmtid",
                    help = "Model type ID",
                    default = 7, type = "integer"
)
parser$add_argument("--etmvid",
                    help = "Model version ID",
                    default = 9, type = "integer"
)
parser$add_argument("--steps",
                    help = "Steps of epi transition to be launched", nargs = '+',
                    default = c(2, 4), type = "integer"
)
parser$add_argument("--note",
                    help = "Note to save for plot labels",
                    default = "GAM+LO(.7, 1ex, m95%, YLL SEV)", type = "character"
)
args <- parser$parse_args()
list2env(args, environment())
rm(args)

if (nchar(note) > 30) stop("Please shorten note to less than 30 characters before proceeding.")

m <- 10
sl <- 5
proj <- 'proj_epitrans'
gbdrid <- 5
lsid <- 35
csid <- 3
rsid <- 1
agids <- c(2:20, 30:32, 235)
sids <- 1:2
update_dems <- any(!file.exists(sprintf("%s/t%d/v%d/dem_inputs/%s.RDS", data_dir, etmtid, etmvid, c("locsdf", "popsdf", "sdidf", "medf", "restrictionsdf", "versiondf"))))
plot_fits <- T
if (etmtid == 101){
    agids <- 7:15
    sids <- 2
}
if (etmtid %in% c(1:3)) yids <- 1950:2017
if (etmtid %in% c(4)) yids <- 1980:2017
if (etmtid %in% c(5, 6, 7, 101)) yids <- c(1990:2017)
base_processes <- c("dem_inputs", "fits", "summaries", "diagnostics")
if (etmtid == 1) spec_processes <- "life_table"
if (etmtid %in% c(2, 3, 7, 101)) spec_processes <- c()
if (etmtid == 4) spec_processes <- c("raked", "YLLs")
if (etmtid == 5) spec_processes <- c("raked", "HALE")
if (etmtid == 6) spec_processes <- c("raked")
if (etmtid == 7) spec_processes <- c("PAFs")
et_best <- fread("FILEPATH")

################
## 1: Setup ####
################

if (1 %in% steps) {

    for (process_dir in c(base_processes, spec_processes)) {
        recurse <- !dir.exists(sprintf("%s/t%d/v%d/", data_dir, etmtid, etmvid))
        if (!dir.exists(sprintf("%s/t%d/v%d/%s", data_dir, etmtid, etmvid, process_dir))) dir.create(sprintf("%s/t%d/v%d/%s", data_dir, etmtid, etmvid, process_dir), recursive = recurse)
    }
    if (!file.exists(sprintf("%s/t%d/v%d/model_param.csv", data_dir, etmtid, etmvid))){

        message("Copy model parameters from a previous version")
        system(sprintf("cp %s/t%d/v%d/model_param.csv %s/t%d/v%d/model_param.csv", data_dir, etmtid, as.numeric(readline(prompt = "Template ETMVID: ")),
                       data_dir, etmtid, etmvid))
        model_param <- fread(sprintf('%s/t%d/v%d/model_param.csv', data_dir, etmtid, etmvid))
        stop("Update model_param file from previous version")
        saveParams(model_param, data_dir, etmtid, etmvid)
    }

    if (update_dems) {

        # Get best model versions for appropriate input data
        if (etmtid == 1) mvid <- get_best_versions("with shock death number estimate", gbd_year = max(yids)) %>% unlist()
        if (etmtid %in% 2:3) mvid <- get_best_versions("population estimate", gbd_year = max(yids)) %>% unlist()
        cvid <- 353
        if (etmtid %in% 4:6) mvid <- if (!is.null(cvid)) cvid else as.numeric(et.dbQuery("gbd", "SELECT MAX(compare_version_id) FROM gbd.compare_version WHERE compare_version_status_id = 2"))
        if (etmtid == 7) mvid <- cvid
        if (etmtid %in% c(91, 92, 93, 94)) mvid <- 0

        # Load and save data
        locsdf <- get_location_metadata(location_set_id = lsid, gbd_round_id = gbdrid)
        locsdf <- locsdf[level == 3]
        sdidf <- get_covariate_estimates(covariate_id = 881, gbd_round_id = gbdrid, location_id = locsdf$location_id, year_id = yids) %>%
            .[, .(sdi_vers_id = model_version_id, location_id, year_id, sdi = mean_value)]
        popsdf <- get_mort_outputs(
            model_name = "population", model_type = "estimate", gbd_year = 2012 + gbdrid,
            location_ids = locsdf$location_id,
            age_group_ids = agids,
            sex_ids = sids,
            year_ids = yids
        ) %>% as.data.table() %>% .[, .(pop_vers_id = run_id, location_id, age_group_id, sex_id, year_id, pop = mean)]

        if (etmtid %in% c(1, 4:6, 101)) {
            me_type <- "cause"
            medf <- get_cause_metadata(cause_set_id = csid, gbd_round_id = gbdrid)
            medf <- merge(medf, et.dbQuery("gbd", "SELECT cause_id, cause_medium FROM shared.cause") %>% setnames(., 2, "cause_name_short"), by = "cause_id")
            if (etmtid == 1) medf <- medf[cause_id == 294] else if (etmtid == 4) medf <- medf[!cause_id %in% c(294) & is.na(yld_only)] else if (etmtid == 5) medf <- medf[is.na(yll_only), ] else if (etmtid == 101) medf <- medf[level >= 3 & acause %like% 'maternal']
        } else if (etmtid %in% 2:3) {
            mt_name <- ifelse(etmtid == 2, "Population Age", "Population Sex")
            me_type <- "population"
            medf <- data.table(
                gbd_set_id = NA_integer_, gbd_set_name = NA_character_,
                gbd_name_short = mt_name, gbd_id = 0, parent_id = 0, level = 0, most_detailed = 1,
                sort_order = 1, gbd_type = me_type
            )
        } else if (etmtid %in% 7) {
            me_type <- "rei|risk"
            medf <- get_rei_metadata(rei_set_id = rsid, gbd_round_id = 5)
        }
        setnames(medf, names(medf), gsub(me_type, "gbd", names(medf)))
        medf <- medf[, .(gbd_set_id, gbd_set_name, gbd_name_short, gbd_id, parent_id, level, most_detailed, sort_order, gbd_type = me_type)]

        restrictionsdf <- if (etmtid %in% 4:7) et.getRestrictions(etmtid, medf, mvid) else data.table(age_group_id = integer(), sex_id = integer(), gbd_id = integer(), restr = logical())

        versiondf <- data.table(
            etmtid = etmtid, etmvid = etmvid, sdi_vers_id = unique(sdidf$sdi_vers_id),
            pop_vers_id = unique(popsdf$pop_vers_id),
            input_version_id = mvid, note = note,
            rundate = gsub("-", "", Sys.Date())
        )

        saveRDS(medf,
                file = sprintf("%s/t%d/v%d/dem_inputs/medf.RDS", data_dir, etmtid, etmvid)
        )
        saveRDS(restrictionsdf,
                file = sprintf("%s/t%d/v%d/dem_inputs/restrictionsdf.RDS", data_dir, etmtid, etmvid)
        )
        saveRDS(locsdf,
                file = sprintf("%s/t%d/v%d/dem_inputs/locsdf.RDS", data_dir, etmtid, etmvid)
        )
        saveRDS(sdidf,
                file = sprintf("%s/t%d/v%d/dem_inputs/sdidf.RDS", data_dir, etmtid, etmvid)
        )
        saveRDS(popsdf,
                file = sprintf("%s/t%d/v%d/dem_inputs/popsdf.RDS", data_dir, etmtid, etmvid)
        )
        saveRDS(versiondf,
                file = sprintf("%s/t%d/v%d/dem_inputs/versiondf.RDS", data_dir, etmtid, etmvid)
        )


        ## Query data
        query_args <- list(
            data_dir = data_dir,
            etmtid = etmtid,
            etmvid = etmvid,
            mvid = mvid
        )
        queried_inputs <- sprintf(
            "obs_agid_%d_sid_%d_gbdid_%d.RDS",
            rep(agids, length(sids) * length(medf$gbd_id)),
            rep(sids, length(agids) * length(medf$gbd_id)),
            rep(medf$gbd_id, length(agids) * length(sids))
        )
        if (!dir.exists(sprintf("%s/t%d/gbd_inputs/%d", data_dir, etmtid, mvid))) dir.create(sprintf("%s/t%d/gbd_inputs/%d", data_dir, etmtid, mvid))
        query_sub <- length(check_files(queried_inputs, sprintf("%s/t%d/gbd_inputs/%d", data_dir, etmtid, mvid), warn_only = T)) > 0
        array_qsub(
            jobname = sprintf("epi_trans_query_inputs_etmtid_%d_etmvid_%d", etmtid, etmvid),
            shell = sprintf("%s/shells/r_shell.sh", c),
            code = sprintf("%s/00_core/01_query_inputs.R", c),
            pass_argparse = query_args,
            slots = 10,
            mem = 20,
            proj = proj,
            num_tasks = length(agids) * length(sids),
            submit = query_sub
        )
        if (query_sub) check_files(queried_inputs, sprintf("%s/t%d/gbd_inputs/%d", data_dir, etmtid, mvid), sleep_time = 60, sleep_end = 30, continual = T)
    }
}


locsdf <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/locsdf.RDS", data_dir, etmtid, etmvid))
sdidf <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/sdidf.RDS", data_dir, etmtid, etmvid))
popsdf <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/popsdf.RDS", data_dir, etmtid, etmvid))
medf <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/medf.RDS", data_dir, etmtid, etmvid))
restrictionsdf <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/restrictionsdf.RDS", data_dir, etmtid, etmvid))
versiondf <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/versiondf.RDS", data_dir, etmtid, etmvid))
gbdids <- if (etmtid %in% c(2, 3)) 0 else medf$gbd_id
mvid <- versiondf$input_version_id


submitmap <- data.table(expand.grid(etmtid = etmtid, etmvid = etmvid, age_group_id = agids, sex_id = sids, gbd_id = unique(medf$gbd_id)))
submitmap <- merge(submitmap, restrictionsdf, by = c("age_group_id", "sex_id", "gbd_id"), all.x = T)
parammap <- fread(sprintf("%s/t%d/v%d/model_param.csv", data_dir, etmtid, etmvid))
submitmap <- merge(submitmap, parammap, by = c("etmtid", "age_group_id", "sex_id", "gbd_id"), all.x = T)[order(gbd_id, age_group_id, sex_id)]
submitmap[, done_fit := file.exists(sprintf(
    "%s/t%d/v%d/fits/MEAN_agid_%d_sid_%d_gbdid_%d.RDs",
    data_dir, etmtid, etmvid, age_group_id, sex_id, gbd_id
))]
resub <- sum(submitmap$done_fit) / length(submitmap$done_fit) > .90 & any(submitmap$done_fit == F)
submitmap[, task_id := seq(.N), by = .(age_group_id, sex_id)]
submitmap[done_fit == F, resub_task_id := seq(.N), by = .(age_group_id, sex_id)][, done_fit := NULL]
saveRDS(submitmap,
        file = sprintf("%s/t%d/v%d/submitmap.RDS", data_dir, etmtid, etmvid)
)


#####################
## 2: FIT MODELS ####
#####################

if (2 %in% steps) {

    if (etmtid %in% c(1, 2, 3, 4, 5, 7, 101)) {
        whichtask <- ifelse(resub, "resub_task_id", "task_id")

        for (agid in agids) {
            for (sid in sids) {
                ntasks <- max(submitmap[age_group_id == agid & sex_id == sid, get(whichtask)], na.rm = T)

                message(sprintf("Submitting %d tasks for age group id (%d) and sex id (%d).", ntasks, agid, sid))

                fit_args <- list(
                    data_dir = data_dir,
                    etmtid = etmtid,
                    etmvid = etmvid,
                    agid = agid,
                    sid = sid,
                    resub = as.integer(resub)
                )
                array_qsub(
                    jobname = sprintf("epi_trans_fit_etmtid_%d_etmvid_%d_agid_%d_sid_%d", etmtid, etmvid, agid, sid),
                    shell = sprintf("%s/shells/r_shell.sh", c),
                    code = sprintf("%s/00_core/02_fit.R", c),
                    pass_argparse = fit_args,
                    slots = sl,
                    mem = m,
                    proj = proj,
                    num_tasks = ntasks,
                    submit = any(!is.na(submitmap[age_group_id == agid & sex_id == sid]$resub_task_id))
                )

                ## Rake if CoD or Non-fatal
                if (etmtid %in% c(4, 5)) {
                    rake_args <- list(
                        data_dir = data_dir,
                        etmtid = etmtid,
                        etmvid = etmvid,
                        agid = agid,
                        sid = sid
                    )
                    qsub(
                        jobname = sprintf("epi_trans_rake_etmtid_%d_etmvid_%d_agid_%d_sid_%d", etmtid, etmvid, agid, sid),
                        shell = sprintf("%s/shells/r_shell.sh", c),
                        code = sprintf("%s/00_core/03_rake.R", c),
                        pass_argparse = rake_args,
                        slots = sl,
                        mem = m,
                        proj = proj,
                        hold = sprintf("epi_trans_fit_etmtid_%d_etmvid_%d_agid_%d_sid_%d", etmtid, etmvid, agid, sid),
                        submit = T
                    )
                }
            }
        }
    }

    ## CHECK THAT FITS AND RAKING ARE COMPLETE
    check_processes <- if (etmtid %in% c(1:3, 7, 101)) "fits" else c("fits", "raked")
    fitfiles <- data.table(expand.grid(process_dir = check_processes, age_group_id = agids, sex_id = sids, gbd_id = medf$gbd_id))
    fitfiles[, file := sprintf("%s/MEAN_agid_%d_sid_%d_gbdid_%d.RDs", process_dir, age_group_id, sex_id, gbd_id)]
    check_files(
        filenames = fitfiles$file, folder = sprintf("%s/t%d/v%d/", data_dir, etmtid, etmvid),
        continual = T, sleep_time = 60, sleep_end = 60
    )

}


#################################
## 3: CALC PRE AGG MEASURES #####
#################################

if (3 %in% steps) {

    # Calc YLL if type 4
    if (etmtid == 4) {
        tmrltdf <- get_mort_outputs(
            model_name = "theoretical minimum risk life table",
            model_type = "estimate",
            estimate_stage_ids = 6
        )[, .(tmrlt_vers_id = run_id, age = precise_age, mr_ex = mean)]
        versiondf[, tmrlt_vers_id := unique(tmrltdf$tmrlt_vers_id)]
        saveRDS(tmrltdf,
                file = sprintf("%s/t%d/v%d/dem_inputs/tmrltdf.RDS", data_dir, etmtid, etmvid)
        )
        saveRDS(versiondf,
                file = sprintf("%s/t%d/v%d/dem_inputs/versiondf.RDS", data_dir, etmtid, etmvid)
        )
        array_qsub(
            jobname = sprintf("epi_trans_calc_yll_etmtid_%d_etmvid_%d", etmtid, etmvid),
            shell = sprintf("%s/shells/r_shell.sh", c),
            code = sprintf("%s/04_causes_of_death/calc_ylls.R", c),
            pass_argparse = list(
                data_dir = data_dir,
                etmtid = etmtid,
                etmvid = etmvid,
                etmvid_lt = et_best[etmtid == 1, best_mvid]
            ),
            slots = sl,
            mem = m,
            proj = proj,
            num_tasks = length(gbdids),
            submit = T
        )
        yllfiles <- data.table(expand.grid(process_dir = "YLLs", age_group_id = agids, sex_id = sids, gbd_id = medf$gbd_id))
        yllfiles[, file := sprintf("%s/MEAN_agid_%d_sid_%d_gbdid_%d.RDs", process_dir, age_group_id, sex_id, gbd_id)]
        check_files(
            filenames = yllfiles$file, folder = sprintf("%s/t%d/v%d/", data_dir, etmtid, etmvid),
            continual = T, sleep_time = 60, sleep_end = 60
        )
    }
}

##################################
## 4: MAKE AGE-SEX AGGREGATES ####
##################################

if (4 %in% steps) {
    if (etmtid == 7) spec_processes <- c()
    for (process_dir in c("fits", spec_processes)) {
        array_qsub(
            jobname = sprintf("epi_trans_agg_age_sex_%s_etmtid_%d_etmvid_%d", process_dir, etmtid, etmvid),
            shell = sprintf("%s/shells/r_shell.sh", c),
            code = sprintf("%s/00_core/04_aggregate_age_sex.R", c),
            pass_argparse = list(
                data_dir = data_dir,
                agg_step = process_dir,
                etmtid = etmtid,
                etmvid = etmvid,
                etmvid_age = et_best[etmtid == 2, best_mvid],
                etmvid_sex = et_best[etmtid == 3, best_mvid]
            ),
            slots = sl,
            mem = m,
            proj = proj,
            num_tasks = length(medf$gbd_id),
            submit = process_dir %in% c("fits", "raked", "YLLs")
        )
    }
    aggfiles <- data.table(expand.grid(process_dir = c("fits", spec_processes)[c("fits", spec_processes) %in% c("fits", "raked", "YLLs")], age_group_id = c(agids, 1, 21:28), sex_id = c(sids, 3), gbd_id = medf$gbd_id))
    aggfiles[, file := sprintf("%s/MEAN_agid_%d_sid_%d_gbdid_%d.RDs", process_dir, age_group_id, sex_id, gbd_id)]
    check_files(
        filenames = aggfiles$file, folder = sprintf("%s/t%d/v%d/", data_dir, etmtid, etmvid),
        continual = T, sleep_time = 60, sleep_end = 60
    )
}

#################################
## 5: CALC POST AGG MEASURES ####
#################################

if (5 %in% steps) {

    # CALC ex if type 1
    if (etmtid == 1) {
        qsub(
            jobname = sprintf("epi_trans_life_table_etmtid_%d_etmvid_%d", etmtid, etmvid),
            shell = sprintf("%s/shells/r_shell.sh", c),
            code = sprintf("%s/mortality/calc_life_table.R", c),
            pass_argparse = list(
                data_dir = data_dir,
                etmvid = etmvid
            ),
            slots = sl,
            mem = m,
            proj = proj,
            submit = T
        )
        ltfiles <- data.table(expand.grid(process_dir = c("life_table"), age_group_id = c(28, 5:20, 30:32, 235), sex_id = 1:3, gbd_id = medf$gbd_id))
        ltfiles[, file := sprintf("%s/MEAN_agid_%d_sid_%d_gbdid_%d.RDs", process_dir, age_group_id, sex_id, gbd_id)]
        check_files(
            filenames = ltfiles$file, folder = sprintf("%s/t%d/v%d/", data_dir, etmtid, etmvid),
            continual = T, sleep_time = 60, sleep_end = 60
        )
    }

    # CALC HALE if type 6
    if (etmtid == 6) {
        qsub(
            jobname = sprintf("epi_trans_hale_etmtid_%d_etmvid_%d", etmtid, etmvid),
            shell = sprintf("%s/shells/r_shell.sh", c),
            code = sprintf("%s/06_dalys_hale/calc_hale.R", c),
            pass_argparse = list(
                data_dir = data_dir,
                etmvid = etmvid
            ),
            slots = sl,
            mem = m,
            proj = proj,
            submit = T
        )
    }

    # Calc DALYs if type 6
    if (etmtid == 6) {
        qsub(
            jobname = sprintf("epi_trans_daly_etmtid_%d_etmvid_%d", etmtid, etmvid),
            shell = sprintf("%s/shells/r_shell.sh", c),
            code = sprintf("%s/06_dalys_hale/calc_daly.R", c),
            pass_argparse = list(
                data_dir = data_dir,
                etmvid = etmvid
            ),
            slots = sl,
            mem = m,
            proj = proj,
            submit = T
        )
    }

}

#########################
## 6: MAKE SUMMARIES ####
#########################

if (6 %in% steps) {
    summ_steps <- if (etmtid %in% c(1:3, 7)) "fits" else spec_processes
    array_qsub(
        jobname = sprintf("epi_trans_summarize_etmtid_%d_etmvid_%d", etmtid, etmvid),
        shell = sprintf("%s/shells/r_shell.sh", c),
        code = sprintf("%s/00_core/05_summarize.R", c),
        pass_argparse = list(
            data_dir = data_dir,
            etmtid = etmtid,
            etmvid = etmvid,
            summ_steps = summ_steps
        ),
        hold_jid_ad = sprintf("epi_trans_agg_age_sex_etmtid_%d_etmvid_%d", etmtid, etmvid),
        num_tasks = length(medf$gbd_id),
        slots = sl,
        mem = m,
        proj = proj,
        submit = T
    )
    summfiles <- data.table(expand.grid(process_dir = "summaries", gbd_id = medf$gbd_id))
    summfiles[, file := sprintf("%s/gbdid_%d.csv", process_dir, gbd_id)]
    check_files(
        filenames = summfiles$file, folder = sprintf("%s/t%d/v%d/", data_dir, etmtid, etmvid),
        continual = T, sleep_time = 60, sleep_end = 60
    )
}
