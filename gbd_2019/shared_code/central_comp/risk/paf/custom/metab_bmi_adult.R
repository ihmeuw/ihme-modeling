# load libraries and functions
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
library(mvtnorm)

source("./utils/data.R")
source("./utils/db.R")
source("mediate_rr.R")
source("save.R")
source("FILEPATH/get_draws.R")

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
exp_dist <- rei_meta$exp_dist
inv_exp <- rei_meta$inv_exp
rr_scalar <- rei_meta$rr_scalar
tmrel_dist <- rei_meta$tmrel_dist
tmrel_lower <- rei_meta$tmrel_lower
tmrel_upper <- rei_meta$tmrel_upper

#--PULL EXPOSURE AND OBESITY/OVERWEIGHT PREV--------------------------------------------
exp <- get_exp(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
ov_ob <- get_draws(gbd_id_type = c("modelable_entity_id", "modelable_entity_id"), gbd_id = c(24742, 24743),
                   measure_id = 18, location_id = location_id, year_id = year_id,
                   sex_id = sex_id, gbd_round_id = gbd_round_id,
                   decomp_step = decomp_step, source = "epi", n_draws = n_draws,
                   downsample = TRUE)
ov_ob <- melt(ov_ob, id.vars = c("modelable_entity_id", "age_group_id", "sex_id", "location_id", "year_id"),
              measure.vars = paste0("draw_", 0:(n_draws - 1)),
              variable.name = "draw", value.name = "prev")
ov_ob[, draw := as.numeric(gsub("draw_", "", draw))]
ov_ob <- dcast(ov_ob, age_group_id + sex_id + location_id + year_id + draw ~ modelable_entity_id, value.var = "prev")
setnames(ov_ob, c("24742", "24743"), c("over_prev", "obes_prev"))
dt <- merge(exp, ov_ob, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
rm(exp);rm(ov_ob)

#--CALC EXPOSURE SD----------------------------------------------------------
source("./ensemble/edensity.R")
Rcpp::sourceCpp("./custom/integ_bmi.cpp")
wlist <- fread(paste0("FILEPATH/", rei, ".csv"))

fit_bmi_ensemble <- function(b, over, obese, weights, mean) {
    tryCatch({
        fx <- NULL
        fx <- get_edensity(weights, mean, Vectorize(b), 10, 50)
        out <- NULL
        out <- integ_bmi(fx$x, fx$fx)
        ((out$over-over)^2 + (out$obese-obese)^2)
    }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
}
fit_bmi_ensemble <- cmpfun(fit_bmi_ensemble)
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
dt <- cbind(dt, exp_sd = unlist(exp_sds))
dt[, c("over_prev", "obes_prev") := NULL]
if (mediate == FALSE) save_exp_sd(dt, rei_id, rei, n_draws, out_dir)
rm(exp_sds)

#--PULL RR AND TMREL ----------------------------------------------------------
rr <- get_rr(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
dt <- merge(dt, rr, by = c("location_id", "year_id", "age_group_id", "sex_id",
                           "parameter", "draw"))
rm(rr)
if (mediate) dt <- mediate_rr(rei_id, dt)

ages <- unique(dt$age_group_id)
tmrel <- get_tmrel(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step,
                   n_draws, tmrel_lower, tmrel_upper, ages)
dt <- merge(dt, tmrel, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
rm(tmrel)

#--CALC PAF ---------------------------------------------------------------------
Rcpp::sourceCpp("./ensemble/ensemble.cpp")
calc_paf <- function(i) {
    tryCatch({
        ensemble(dens[[demo_dt[i, ]$draw[1] + 1]]$x, dens[[demo_dt[i, ]$draw[1] + 1]]$fx, demo_dt[i, ]$tmrel,
                 demo_dt[i, ]$rr, rr_scalar, inv_exp, cap[[demo_dt[i, ]$draw[1] + 1]])$paf
    }, error = function(cond) {
        as.numeric(NA)
    })
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
                                                             demo_dt[draw == d, ]$exp_sd[1], .min=10, .max=50), mc.cores = 6)
    cap <- mclapply(1:n_draws, function(d)
        if (inv_exp) dens[[d]]$XMIN else dens[[d]]$XMAX, mc.cores = 6)
    pafs <- mclapply(1:nrow(demo_dt), calc_paf, mc.cores = 6)
    demo_dt <- cbind(demo_dt, paf = unlist(pafs))
    out[[demo]] <- demo_dt
    rm(demo_dt);rm(weights);rm(dens);rm(cap);rm(pafs)
}
dt <- rbindlist(out, fill=T, use.names=T)

#--SAVE + VALIDATE -------------------------------------------------------------
save_paf(dt, rei_id, rei, n_draws, out_dir)
