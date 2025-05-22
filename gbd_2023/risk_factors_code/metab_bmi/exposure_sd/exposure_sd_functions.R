#' Functions for code creating and saving exposure SD draws.
#'
#' @param exp_dt
#' @param location_id
#' @param year_id
#' @param sex_id
#' @param gbd_round_id
#' @param decomp_step
#' @param n_draws
#'
#' @return
calc_metab_bmi_adult_exp_sd <- function(exp_dt, location_id, year_id, sex_id, age_group_id=NULL,
                                                 gbd_round_id=NULL, decomp_step, n_draws, release_id=NULL,
                                                 weight_dir, global=T) {
  # convert to release_id if necessary
  if (is.null(release_id)) {
    if (is.null(gbd_round_id) | is.null(decomp_step))
      stop("Both gbd_round_id and decomp_step must be provided if not providing release_id.")
    release_id <- get_release_id(get_decomp_step_id(decomp_step, gbd_round_id))
  } else {
    if (!is.null(gbd_round_id))
      stop("Only one of gbd_round_id or release_id must be provided.")
  }
  
  #--PULL OBESITY/OVERWEIGHT PREV--------------------------------------------
  ov_ob_mes <- c("ID", "ID")
  ov_ob <- get_draws(gbd_id_type = c("modelable_entity_id", "modelable_entity_id"), gbd_id = ov_ob_mes,
                     measure_id = 18, location_id = location_id, year_id = year_id,
                     sex_id = sex_id, release_id = release_id, source = "epi", n_draws = n_draws,
                     downsample = TRUE)
  if (!all(year_id %in% unique(ov_ob$year_id))) {
    stop("Not all provided year_id(s) are present in obesity/overweight prevalence models ",
         "(modelable entity IDs ", paste(ov_ob_mes, collapse=", "), ")")
  }
  ov_ob <- melt(ov_ob, id.vars = c("modelable_entity_id", "age_group_id", "sex_id", "location_id", "year_id"),
                measure.vars = paste0("draw_", 0:(n_draws - 1)),
                variable.name = "draw", value.name = "prev")
  ov_ob[, draw := as.numeric(gsub("draw_", "", draw))]
  ov_ob <- dcast(ov_ob, age_group_id + sex_id + location_id + year_id + draw ~ modelable_entity_id, value.var = "prev")
  setnames(ov_ob, c("ID", "ID"), c("over_prev", "obes_prev"))
  dt <- merge(exp_dt, ov_ob, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
  rm(ov_ob)
  
  #--CALC EXPOSURE SD----------------------------------------------------------
  setwd("FILEPATH")
  source("./ensemble/edensity.R")
  Rcpp::sourceCpp("./custom/integ_bmi.cpp")
  
  wlist <- read_ensemble_weights(weights_dir=weights_dir, global=global)
  fit_bmi_ensemble <- function(b, over, obese, weights, mean) {
    tryCatch({
      fx <- NULL
      fx <- get_edensity(weights, mean, Vectorize(b), 10, 50)
      out <- NULL
      out <- integ_bmi(fx$x, fx$fx)
      ((out$over-over)^2 + (out$obese-obese)^2)
    }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
  }
  fit_bmi_ensemble <- compiler::cmpfun(fit_bmi_ensemble) # use cmpfun function from compiler package
  calc_exp_sd <- function(i){
    weights <- wlist[age_group_id == dt[i, ]$age_group_id & sex_id == dt[i, ]$sex_id,
    ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
    optPARAMS=list()
    optVALS=list()
    for(p in seq(1,10,by=2)) {
      SOPT <- nlminb(start=dt[i, ]$exp_mean/p,objective = fit_bmi_ensemble,
                     over=dt[i, ]$over_prev, obese=dt[i, ]$obes_prev, weights=weights, mean=dt[i, ]$exp_mean,
                     lower=dt[i, ]$exp_mean*.01, upper=dt[i, ]$exp_mean*1.5,
                     control=list(iter.max=3, eval.max=3))
      optPARAMS = rbind(optPARAMS, SOPT$par)
      optVALS = rbind(optVALS, SOPT$objective)
    }
    optPARAMS[which.min(optVALS),][[1]]
  }
  calc_exp_sd <- compiler::cmpfun(calc_exp_sd)
  exp_sds <- mclapply(1:nrow(dt), calc_exp_sd, mc.cores = 6)
  
  # if the exp_sd is greater than 15, re-sample the values from < 15 values
  sd_vec <- unlist(exp_sds)
  rep_sd <- sample(sd_vec[sd_vec < 15],length(sd_vec[sd_vec > 15]), replace = T)
  # update the exp_sd values
  sd_vec[sd_vec > 15] <- rep_sd
  
  dt <- cbind(dt, exp_sd = sd_vec)

  dt[, c("over_prev", "obes_prev") := NULL]
  rm(exp_sds)
  return(dt)
}

# Read the updated ensemble weights for adult BMI
# use either the global weights or
# weights vary by super region, sex and by larger age groups (4 categories)
read_ensemble_weights <- function(location_id=NA, sex_id=NA, age_group_id=NA, weights_dir, global=T) {
  lid <- location_id
  sid <- sex_id
  aid <- age_group_id
  if(global){
    weights <- fread(weights_dir)
  } else {
    weights <- fread(weights_dir)[location_id == lid & sex_id == sid & age_group_id == aid,]
  }
  return(weights)
}

# save exp sd
save_exp_sd <- function(dt, location_id=NA, sex_id=NA, n_draws, out_dir) {
  dt <- dt[, c("location_id", "year_id", "age_group_id", "sex_id",
               "draw", "exp_sd"),
           with = F] %>% setkey %>% unique
  dt <- dcast(dt, location_id + year_id + age_group_id + sex_id ~ draw,
              value.var = "exp_sd")
  setnames(dt, paste0(0:(n_draws - 1)), paste0("draw_", 0:(n_draws - 1)))
  setorder(dt, location_id, year_id, age_group_id, sex_id)
  
  # add necessary columns for upload
  dt[,measure_id := 19]; dt[, metric_id := 3]; dt[,rei_id:=370]; dt[,modelable_entity_id:= 18706]
  
  dir.create(paste0(out_dir, "/exposure_sd"), showWarnings = FALSE)
  file <- paste0(location_id, "_", sex_id, ".csv")
  fwrite(dt, paste0(out_dir, "/exposure_sd/", file), row.names = F)
}