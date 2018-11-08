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
library(sfsmisc)

source("./utils/data.R")
source("./utils/db.R")
source("math.R")
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
out_dir <- args[6]

# get risk info
rei_meta <- get_rei_meta(rei_id)
rei <- rei_meta$rei
exp_dist <- rei_meta$exp_dist
inv_exp <- rei_meta$inv_exp
rr_scalar <- rei_meta$rr_scalar
tmrel_dist <- rei_meta$tmrel_dist
tmrel_lower <- rei_meta$tmrel_lower
tmrel_upper <- rei_meta$tmrel_upper

#--PULL EXPOSURE AND SD-----------------------------------------------

exp <- get_exp(rei_id, location_id, year_id, sex_id, gbd_round_id, n_draws)
exp_sd <- get_exp_sd(rei_id, location_id, year_id, sex_id, gbd_round_id, n_draws)
exp <- merge(exp, exp_sd, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))

#--PULL RR AND MERGE------------------------------------------------------------

draw_cols <- paste0("draw_",0:(n_draws-1))
rr_dt <- fread("FILEPATH/activity_rr_by_exposure.csv")
if(sex_id == 1) rr_dt <- rr_dt[cause_id != 429,]
for (col in draw_cols) rr_dt[get(col) > 1, (col) := 1]
cause_ids <- rr_dt$cause_id %>% unique
## define splines for each RR - extrapolate
for(cid in cause_ids) {
    for(i in 0:(n_draws-1)) {
        rr <- rr_dt[cause_id == cid, get(paste0('draw_',i))]
        EXP = log(rr_dt[cause_id == cid]$exposure)
        EXP[1] <- 0
        ## flatten
        rr_f = vector(mode="numeric", length=length(rr))
        rr_f[1] <- 1
        for(j in 2:length(rr_f)) {
            rb = rr[[j-1]]
            rp = rr[[j]]
            rp = ifelse(rp<min(rr[2:(j-1)]),rp,min(rr[2:(j-1)]))
            rr_f[[j]] <- rp
        }
        ## flatten rr
        rr_flat <- approxfun(EXP, rr_f, method="linear", rule=2)
        assign(paste0("rr_fun_", cid, "_", i), rr_flat)
    }
}

#--ADD TMREL -------------------------------------------------------------------

ages <- unique(exp$age_group_id)
tmrel <- get_tmrel(rei_id, location_id, year_id, sex_id, gbd_round_id,
                   n_draws, tmrel_lower, tmrel_upper, ages)
tmrel[, tmrel := log(tmrel)]
dt <- merge(exp, tmrel, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
rm(exp);rm(tmrel);rm(exp_sd);rm(rr_dt)

#--CALC PAF ---------------------------------------------------------------------

wlist <- fread(paste0("FILEPATH/", rei, ".csv"))
# load ensemble PAF function from C++
Rcpp::sourceCpp("./ensemble/ensemble.cpp")
source("./ensemble/edensity_activity.R")

calc_paf <- function(i) {
    pafs <- list()
    for(cid in cause_ids) {
        paf <- tryCatch({
            rr_fun <- get(paste0("rr_fun_", cid, "_", demo_dt[i, ]$draw))
            denom <- integrate(function(x) density[[demo_dt[i, ]$draw[1] + 1]](x) * (1/rr_fun(x))^((demo_dt[i, ]$tmrel-x + abs(demo_dt[i, ]$tmrel-x))/2), log(20), log(5000), stop.on.error=FALSE)$value
            integrate(function(x) density[[demo_dt[i, ]$draw[1] + 1]](x) * (1/rr_fun(x)^((demo_dt[i, ]$tmrel-x + abs(demo_dt[i, ]$tmrel-x))/2) - 1)/denom, log(20), log(5000), stop.on.error=FALSE)$value
        }, error = function(cond) {
            as.numeric(NA)
        })
        pafs <- c(pafs, paf)
    }
    return(pafs)
}
calc_paf <- compiler::cmpfun(calc_paf)

dt[, demo_id := .GRP, by=c("location_id", "year_id", "age_group_id", "sex_id")]
demo_ids <- unique(dt$demo_id)
out <- vector("list", length(demo_ids))
for(demo in demo_ids) {
    demo_dt <- dt[demo_id == demo, ]
    # Build density given w=data.frame of weights, M = Mean, S=SD
    weights <- wlist[age_group_id == demo_dt$age_group_id[1] & sex_id == demo_dt$sex_id[1],
                     ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
    ## build density for that exposure
    den <- mclapply(0:(n_draws-1),
                    function(d) get_edensity_activity(weights,log(demo_dt[draw == d, ]$exp_mean),
                                                      log(demo_dt[draw == d, ]$exp_sd)), mc.cores = 6)
    density <- mclapply(1:n_draws,
                        function(d) approxfun(x=log(seq(20, 50000, length=1000)), y=den[[d]]$fx, yleft=0, yright=0), mc.cores = 6)
    pafs <- mclapply(1:nrow(demo_dt), calc_paf, mc.cores = 12)
    demo_dt <- demo_dt[, .(cause_id = cause_ids), by = names(demo_dt)]
    demo_dt <- cbind(demo_dt, paf = unlist(pafs))
    out[[demo]] <- demo_dt
    rm(demo_dt);rm(weights);rm(den);rm(density);rm(pafs)
}
dt <- rbindlist(out, fill=T, use.names=T)

#--SAVE + VALIDATE -------------------------------------------------------------

dt[, mortality := 1]
dt[, morbidity := 1]
save_paf(dt, rei_id, rei, n_draws, out_dir)
