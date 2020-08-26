# load libraries and functions
rm(list=ls())
library(data.table)
library(magrittr)
library(gtools)
library(ini)
library(RMySQL)
library(fitdistrplus)
library(actuar)
library(Rcpp)
library(compiler)
library(parallel)
source("./utils/data.R")
source("./utils/db.R")
source("./custom/shift_rr.R")
source("math.R")
source("save.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_location_metadata.R")

set.seed(124535)

#-- SET UP ARGS ----------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
task_id <- Sys.getenv("SGE_TASK_ID") %>% as.numeric
params <- fread(args[1])[task_id, ]

location_id <- unique(params$location_id)
sex_id <- unique(params$sex_id)
rei_id <- as.numeric(args[2])
year_id <- eval(parse(text = args[3]))
n_draws <- as.numeric(args[4])
gbd_round_id <- as.numeric(args[5])
decomp_step <- args[6]
out_dir <- args[7]
mediate <- as.logical(args[8])

# get risk info
rei_meta <- get_rei_meta(rei_id)
rei <- rei_meta$rei
inv_exp <- rei_meta$inv_exp
tmrel <- fread("FILEPATH/2019_tmrels.csv")[rei == rei_meta$rei, ]
tmrel_lower <- tmrel$tmrel_lower
tmrel_upper <- tmrel$tmrel_upper


#--PULL EXPOSURE AND SD --------------------------------------------------------
exp <- get_exp(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
exp_sd <- get_exp_sd(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
exp <- merge(exp, exp_sd, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))

#--ADD TMREL -------------------------------------------------------------------
ages <- unique(exp$age_group_id)
tmrel <- get_tmrel(rei_id, location_id, year_id, sex_id, gbd_round_id,
                   decomp_step, n_draws, tmrel_lower, tmrel_upper, ages)
dt <- merge(exp, tmrel, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))

if (rei == "diet_salt") {
  # rrs are mediated sbp RRs
  sbp_rr <- shift_rr(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
  med_sbp <- merge(dt, sbp_rr, by = c("location_id", "year_id", "age_group_id", "sex_id",
                                      "parameter", "draw"))
}

# convert exposures and tmrel to be in the same unit space as RR
# for calcium, g to mg and for pufa and transfat proportion to percent
if (rei == "diet_calcium_low") {
  dt[, `:=` (exp_mean=exp_mean * 1000, exp_sd=exp_sd * 1000, tmrel=tmrel * 1000)]
}
if (rei %in% c("diet_pufa", "diet_transfat")) {
  dt[, `:=` (exp_mean=exp_mean * 100, exp_sd=exp_sd * 100, tmrel=tmrel * 100)]
}

#--MAKE RR FUNCTIONS ---------------------------------------------------------------------
rr <- fread(paste0(out_dir, "/mrbrt/rr.csv"))
for(c in unique(rr$cause_id)) {
  for(d in 0:(n_draws-1)) {
    assign(paste0("rr_", c, "_", d),
           approxfun(rr[cause_id==c, ]$exposure, rr[cause_id==c, ][[paste0("draw_", d)]], rule = 2, ties = "ordered"))
  }
}

age_trends <- fread("FILEPATH/2019_age_trends.csv")
age_trends[, cause_id := ifelse(outcome == "ihd", 493, ifelse(outcome == "hemstroke", 496, 495))]
age_trends <- rbind(age_trends, age_trends[cause_id == 496][, cause_id := 497])
age_trends <- age_trends[, .(cause_id, age_group_id, percentage_change)]

if (mediate) {
  mediation <- fread("FILEPATH/mediation_matrix_draw_gbd_2019.csv")
  mediation <- melt(mediation, id.vars = c("rei_id", "med_id", "cause_id"),
                    measure.vars = paste0("draw_", 0:(n_draws - 1)),
                    variable.name = "draw", value.name = "mediation")
  mediation[, draw := as.numeric(gsub("draw_", "", draw))]
  mediation <- mediation[mediation != 1 & rei_id == rei_meta$rei_id, ]
  mediation <- mediation[, list(mediation=1-prod(1-mediation)), by=c("cause_id", "draw")]
  rr <- rr[cause_id %in% unique(mediation$cause_id), ]
  if (nrow(rr) == 0) stop("No mediation to apply")
}

apply_age_trend <- function(rr, age, cause, d) {
  # We shouldn't ever have an RR that is below 1 (protective, negative PAF)
  rr[rr < 1] <- 1
  pct_chg <- age_trends[age_group_id == age & cause_id == cause, ]
  if (nrow(pct_chg) == 1) {
    rr <- exp(log(rr) * pct_chg$percentage_change)
  }
  if (mediate) {
    rr <- (rr-1)*(1-mediation[cause_id == cause & draw == d, ]$mediation) + 1
  }
  return(rr)
}

# expand dt by cause
dt <- dt[, list(cause_id=unique(rr$cause_id)), by=names(dt)]

#--CALC PAF ---------------------------------------------------------------------
wlist <- fread(paste0("FILEPATH/", rei, ".csv"))
source("./ensemble/edensity.R")
calc_paf <- function(i) {
  exp_pdf <- approxfun(dens[[demo_dt[i, ]$draw[1] + 1]]$x,
                       dens[[demo_dt[i, ]$draw[1] + 1]]$fx,
                       yleft=0, yright=0)
  rr_pdf <- get(paste0("rr_", demo_dt[i, ]$cause_id, "_", demo_dt[i, ]$draw[1]))
  denom <- tryCatch({
    integrate(function(x)
      exp_pdf(x) * apply_age_trend(rr_pdf(x)/rr_pdf(demo_dt[i, ]$tmrel), demo_dt[i, ]$age_group_id, demo_dt[i, ]$cause_id, demo_dt[i, ]$draw[1]),
      lower=dens[[demo_dt[i, ]$draw[1] + 1]]$XMIN,
      upper=dens[[demo_dt[i, ]$draw[1] + 1]]$XMAX, stop.on.error = FALSE)$value
  }, error = function(cond) {
    as.numeric(NA)
  })
  return((denom-1)/denom)
}
calc_paf <- compiler::cmpfun(calc_paf)
dt[, demo_id := .GRP, by=c("location_id", "year_id", "age_group_id", "sex_id")]
demo_ids <- unique(dt$demo_id)
out <- vector("list", length(demo_ids))
for(demo in demo_ids) {
  demo_dt <- dt[demo_id == demo, ]
  weights <- wlist[age_group_id == demo_dt$age_group_id[1] & sex_id == demo_dt$sex_id[1],
                   ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
  dens <- mclapply(0:(n_draws-1), function(d) get_edensity(weights, demo_dt[draw == d, ]$exp_mean[1],
                                                           demo_dt[draw == d, ]$exp_sd[1]), mc.cores = 2)
  cap <- mclapply(1:n_draws, function(d)
    if (inv_exp) dens[[d]]$XMIN else dens[[d]]$XMAX, mc.cores = 2)
  pafs <- lapply(1:nrow(demo_dt), calc_paf)
  demo_dt <- cbind(demo_dt, paf = unlist(pafs))
  out[[demo]] <- demo_dt
  rm(demo_dt);rm(weights);rm(dens);rm(cap);rm(pafs)
}
dt <- rbindlist(out, fill=T, use.names=T)

if (rei == "diet_salt") {
  med_sbp <- cont_paf_ensemble(med_sbp, wlist, rei_meta$rr_scalar, inv_exp, n_draws, use_cpp = FALSE)
  dt <- rbind(dt, med_sbp, fill=TRUE)
}
dt[exp_mean == 0 & exp_sd == 0, paf := 0]

#--SAVE + VALIDATE -------------------------------------------------------------
dt[, `:=` (mortality=1, morbidity=1)]
dt[paf < 0, paf := NA]
save_paf(dt, rei_id, rei, n_draws, out_dir)
