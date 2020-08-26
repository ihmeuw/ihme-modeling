shift_rr <- function(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws) {

      # pull sbp exp and exp SD
      sbp <- get_exp(107, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
      sbp_exp_sd <- get_exp_sd(107, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
      sbp <- merge(sbp, sbp_exp_sd, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))

      # calc prevalence of hypertension
      source("./ensemble/edensity.R")
      sbp_weights <- fread(paste0("FILEPATH/metab_sbp.csv"))
      calc_prev <- function(i) {
          weights <- sbp_weights[age_group_id == sbp[i, ]$age_group_id & sex_id == sbp[i, ]$sex_id,
                           ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
          fx <- get_edensity(weights, sbp[i, ]$exp_mean, sbp[i, ]$exp_sd)
          dens <- approxfun(fx$x, fx$fx, yleft=0, yright=0)
          total_integ <- integrate(dens, fx$XMIN, fx$XMAX, rel.tol=.1, abs.tol=.1)$value
          prev <- integrate(dens, 140, fx$XMAX, rel.tol=.1, abs.tol=.1)$value/total_integ
          return(prev)
      }
      calc_prevc <- compiler::cmpfun(calc_prev)
      prevs <- mclapply(1:nrow(sbp), calc_prevc, mc.cores = 6)
      sbp <- cbind(sbp, hyper_prev = unlist(prevs))

      # read flat file shifts
      locs <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id)
      loc_id <- location_id; sup_id <- locs[location_id == loc_id, ]$super_region_id %>% unique
      sbp_shift <- fread("FILEPATH/diet_salt_rr_shift.csv")[super_region_id == sup_id, ]
      sbp_shift <- melt(sbp_shift, id.vars = c("age_group_id", "sbp_shift"),
                        measure.vars = paste0("draw_", 0:(n_draws - 1)),
                        variable.name = "draw", value.name = "shift")
      sbp_shift[, draw := as.numeric(gsub("draw_", "", draw))]
      sbp_shift <- dcast(sbp_shift, age_group_id + draw ~ sbp_shift,
                         value.var = "shift")

      # shift = prev * >140 + (1 - prev) * <140
      sbp <- merge(sbp, sbp_shift, by = c("age_group_id", "draw"))
      sbp[, shift := hyper_prev * more140 + (1 - hyper_prev) * less140]
      # pull sbp RRs, drop stomach cancer (414), RR = RR^(shift/10/2.299)
      # 100mmol or 2.299 g per SBP, SBP RRs are in 10 mmHg dose response unit-space
      rr <- get_rr(107, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
      rr <- rr[cause_id != 414, ]
      rr <- merge(rr, sbp[, .(location_id, year_id, sex_id, age_group_id, draw, shift)],
                  by = c("location_id", "year_id", "sex_id", "age_group_id", "draw"))
      rr[, rr := rr^(shift/10/2.299)][, shift := NULL]

      # save file to disk
      write.csv(rr, paste0("FILEPATH", location_id, "_", sex_id, ".csv"),
                row.names=F)
      return(rr)

}

