# Check that we have calculated a full set of PAFs. There have been rare instances of
# PAF output missing all or part of a year-cause, possibly due to reading partial RRs.
validate_paf_complete <- function(dt, year_ids) {
    all_years_present <- all(year_ids %in% dt$year_id)
    all_years_same_nrows <- length(unique(table(dt$year_id))) == 1
    if (!all_years_present | !all_years_same_nrows)
        stop("Incomplete PAFs generated for this location-sex. This error may be transient ",
             "and job can be resumed to try again.")
}

save_paf <- function(dt, rid, rei, n_draws, out_dir) {
    # Forcing garbage collection to prevent oom kills during save
    gc()

    # subset columns
    dt[, rei_id := rid]
    dt <- dt[, c("rei_id", "location_id", "year_id", "age_group_id", "sex_id",
                 "cause_id", "mortality", "morbidity", "draw", "paf"),
             with = F] %>% setkey %>% unique
    dt[, paf := as.numeric(paf)]

    # expand mortality and morbidity
    dt <- rbind(dt[!(mortality == 1 & morbidity == 1), ],
                dt[mortality == 1 & morbidity == 1,
                   .(mortality = c(0, 1), morbidity = c(1, 0)),
                   by = c("rei_id", "location_id", "year_id", "age_group_id", "sex_id",
                          "cause_id", "draw", "paf")])
    # switch to measure_id
    dt[, measure_id := ifelse(mortality == 1, 4, 3)]

    # if PAF >= 1, this is an error.
    pafs_above_1 <- nrow(dt[paf > 1])
    if (pafs_above_1 != 0) {
        warning("Some PAF draws (", pafs_above_1, ") in data ",
                "are greater than 1.")
        dt[paf > 1, paf := NA]
    }

    # make sure there aren't any infinities
    infinite_pafs <- nrow(dt[is.infinite(paf)])
    if (infinite_pafs != 0) {
        warning("Some PAF draws (", infinite_pafs, ") in data ",
                "are not finite.")
        dt[is.infinite(paf), paf := NA]
    }

    # if PAF is missing, sometimes the fitdistrplus doesn't compile correctly
    # and returns NAs. as long as not all draws are NA (massive failure due to
    # something else), fill missings with mean of other draws.
    dt[, mean_paf := mean(paf, na.rm=T), by=c("rei_id", "location_id", "year_id",
                                              "age_group_id", "sex_id", "cause_id",
                                              "measure_id")]
    if (any(is.nan(unique(dt$mean_paf))) | any(is.na(unique(dt$mean_paf))))
        stop("All PAF draws in data are NA. Stopping job.")
    dt[is.na(paf), paf := mean_paf][, mean_paf := NULL]

    # cast wide, rename draw cols, order/sort cols
    dt <- dcast(dt, rei_id + location_id + year_id + age_group_id + sex_id + cause_id + measure_id ~ draw,
                value.var = "paf")
    setnames(dt, paste0(0:(n_draws - 1)), paste0("draw_", 0:(n_draws - 1)))
    setorder(dt, rei_id, location_id, year_id, age_group_id, sex_id, cause_id, measure_id)

    # save for save results
    file <- paste0("/", location_id, "_", sex_id, ".csv")
    write.csv(dt, paste0(out_dir, file), row.names = F)

}

# save exp sd that's calculated on the fly for bmi and fpg so we can run SR later.
# these csvs are split by location_sex whereas those from exp_sd.R are by location.
save_exp_sd <- function(dt, rid, rei, n_draws, out_dir) {
    dt <- dt[, c("location_id", "year_id", "age_group_id", "sex_id",
                 "draw", "exp_sd"),
             with = F] %>% setkey %>% unique
    dt <- dcast(dt, location_id + year_id + age_group_id + sex_id ~ draw,
                value.var = "exp_sd")
    setnames(dt, paste0(0:(n_draws - 1)), paste0("draw_", 0:(n_draws - 1)))
    setorder(dt, location_id, year_id, age_group_id, sex_id)
    dir.create(paste0(out_dir, "/exposure_sd"), showWarnings = FALSE)
    file <- paste0(location_id, "_", sex_id, ".csv")
    write.csv(dt, paste0(out_dir, "/exposure_sd/", file), row.names = F)
}

save_results_epi <- function(cluster_proj, log_dir, rei, me_id, year_id, measure_id,
                             description, input_dir, bundle_id, crosswalk_version_id,
                             gbd_round_id, decomp_step, n_draws, input_file_pattern) {
    # mem <- length(year_id)  * 2
    mem <- 100 # ifelse(mem < 10, 10, mem)
    system(paste0("qsub -P ", cluster_proj, " -l m_mem_free=", mem, "G,fthread=10 -q all.q",
                  " -e ", log_dir, "/errors -o ", log_dir, "/output",
                  " -cwd -N save_results_epi_", rei,
                  " utils/python.sh utils/epi_save_results.py",
                  " --modelable_entity_id ", me_id,
                  " --year_id \"", paste(year_id, collapse=" "),
                  "\" --measure_id ", measure_id,
                  " --description \"", description,
                  "\" --input_dir ", input_dir,
                  " --input_file_pattern ", input_file_pattern,
                  " --bundle_id ", bundle_id,
                  " --crosswalk_version_id ", crosswalk_version_id,
                  " --gbd_round_id ", gbd_round_id,
                  " --decomp_step ", decomp_step,
                  " --n_draws ", n_draws))
}
save_results_risk <- function(cluster_proj, log_dir, rei, me_id, year_id, measure_id,
                              sex_ids, risk_type, description, input_dir, input_file_pattern,
                              gbd_round_id, decomp_step, n_draws) {
    # try to guess mem needed based on number of years
    # 5G per year is based on SHS which has the most outcomes, so this should
    # be an upper limit
    # mem <- length(year_id)  * 5
    mem <- 100 # ifelse(mem < 40, 40, mem)
    system(paste0("qsub -P ", cluster_proj, " -l m_mem_free=", mem, "G,fthread=24 -q all.q -l h_rt=72:00:00",
                  " -e ", log_dir, " -o ", log_dir,
                  " -cwd -N save_results_risk_", rei,
                  " utils/python.sh utils/risk_save_results.py",
                  " --modelable_entity_id ", me_id,
                  " --year_id \"", paste(year_id, collapse=" "),
                  "\" --measure_id \"", paste(measure_id, collapse=" "),
                  "\" --sex_id \"", paste(sex_ids, collapse = " "),
                  "\" --risk_type \"", risk_type,
                  "\" --description \"", description,
                  "\" --input_dir ", input_dir,
                  " --input_file_pattern ", input_file_pattern,
                  " --gbd_round_id ", gbd_round_id,
                  " --decomp_step ", decomp_step,
                  " --n_draws ", n_draws))
}

# save pafs at rr level for sev calc
save_for_sev <- function(dt, rid, rei, n_draws, out_dir) {
    if (rid == 109) dt <- dt[mortality == 1, ]
    if (rid == 130) dt <- dt[morbidity == 1, ]
    dt <- dt[, c("location_id", "year_id", "age_group_id", "sex_id",
                 "cause_id", "draw", "paf"),
             with = F] %>% setkey %>% unique
    pafs_above_1 <- nrow(dt[paf >= 1])
    if (pafs_above_1 != 0) dt[paf >= 1, paf := NA]
    if (!(rid ==370)) dt[cause_id != 419 & paf < 0, paf := 0]
    dt[, mean_paf := mean(paf, na.rm=T), by=c("location_id", "year_id", "age_group_id",
                                              "sex_id", "cause_id")]
    if (any(is.nan(unique(dt$mean_paf))) | any(is.na(unique(dt$mean_paf))))
        stop("All PAF draws in data are NA. Stopping job.")
    dt[is.na(paf), paf := mean_paf][, mean_paf := NULL]

    out_dir <- paste0("FILEPATH", rei, "/sev")
    dir.create(out_dir, showWarnings = FALSE)
    file <- paste0("/", location_id, "_", sex_id, ".csv")
    write.csv(dt, paste0(out_dir, file), row.names = F)
}
