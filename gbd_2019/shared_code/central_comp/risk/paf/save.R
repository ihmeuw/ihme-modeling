save_paf <- function(dt, rid, rei, n_draws, out_dir) {

    # subset columns
    dt[, rei_id := rid]
    dt <- dt[, c("rei_id", "location_id", "year_id", "age_group_id", "sex_id",
                 "cause_id", "mortality", "morbidity", "draw", "paf"),
             with = F] %>% setkey %>% unique

    # expand mortality and morbidity
    dt <- rbind(dt[!(mortality == 1 & morbidity == 1), ],
                dt[mortality == 1 & morbidity == 1,
                   .(mortality = c(0, 1), morbidity = c(1, 0)),
                   by = c("rei_id", "location_id", "year_id", "age_group_id", "sex_id",
                          "cause_id", "draw", "paf")])
    # switch to measure_id
    dt[, measure_id := ifelse(mortality == 1, 4, 3)]

    pafs_above_1 <- nrow(dt[paf >= 1])
    if (pafs_above_1 != 0) {
        warning("Some PAF draws (", pafs_above_1, ") in data ",
                "are greater than 1.")
        dt[paf >= 1, paf := NA]
    }

    # make sure there aren't any infinities
    infinite_pafs <- nrow(dt[is.infinite(paf)])
    if (infinite_pafs != 0) {
        warning("Some PAF draws (", infinite_pafs, ") in data ",
                "are not finite.")
        dt[is.infinite(paf), paf := NA]
    }

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

# save exp sd that's calculated on the fly for bmi and fpg so we can run SR later
save_exp_sd <- function(dt, rid, rei, n_draws, out_dir) {
    dt <- dt[, c("location_id", "year_id", "age_group_id", "sex_id",
                 "draw", "exp_sd"),
             with = F] %>% setkey %>% unique
    dt <- dcast(dt, location_id + year_id + age_group_id + sex_id ~ draw,
                value.var = "exp_sd")
    setnames(dt, paste0(0:(n_draws - 1)), paste0("draw_", 0:(n_draws - 1)))
    setorder(dt, location_id, year_id, age_group_id, sex_id)
    out_dir <- paste0("FILEPATH", rei, "/exp_sd/")
    dir.create(out_dir, showWarnings = FALSE)
    file <- paste0("/", location_id, "_", sex_id, ".csv")
    write.csv(dt, paste0(out_dir, file), row.names = F)
}

save_results_epi <- function(cluster_proj, log_dir, rei, me_id, year_id, measure_id,
                             description, input_dir, gbd_round_id, decomp_step, n_draws) {

    mem <- 100
    system(paste0("qsub -P ", cluster_proj, " -l m_mem_free=", mem, "G,fthread=10 -q all.q",
                  " -e ", log_dir, "/errors -o ", log_dir, "/output",
                  " -cwd -N save_results_epi_", rei,
                  " utils/python.sh utils/epi_save_results.py",
                  " --me_id ", me_id,
                  " --year_id \"", paste(year_id, collapse=" "),
                  "\" --measure_id ", measure_id,
                  " --description \"", description,
                  "\" --input_dir ", input_dir,
                  " --gbd_round_id ", gbd_round_id,
                  " --decomp_step ", decomp_step,
                  " --n_draws ", n_draws))
}
save_results_risk <- function(cluster_proj, log_dir, rei, me_id, year_id, measure_id,
                              sex_ids, risk_type, description, input_dir, input_file_pattern,
                              gbd_round_id, decomp_step, n_draws) {
    mem <- 100
    system(paste0("qsub -P ", cluster_proj, " -l m_mem_free=", mem, "G,fthread=24 -q all.q -l h_rt=72:00:00",
                  " -e ", log_dir, " -o ", log_dir,
                  " -cwd -N save_results_risk_", rei,
                  " utils/python.sh utils/risk_save_results.py",
                  " --me_id ", me_id,
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

    out_dir <- paste0("FILEPATH", rei, "/sev/")
    dir.create(out_dir, showWarnings = FALSE)
    file <- paste0("/", location_id, "_", sex_id, ".csv")
    write.csv(dt, paste0(out_dir, file), row.names = F)
}
