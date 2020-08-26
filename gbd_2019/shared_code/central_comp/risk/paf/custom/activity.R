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
decomp_step <- args[6]
out_dir <- args[7]
mediate <- as.logical(args[8])

# get risk info
rei_meta <- get_rei_meta(rei_id)
rei <- rei_meta$rei
tmrel_lower <- rei_meta$tmrel_lower
tmrel_upper <- rei_meta$tmrel_upper

#--PULL EXPOSURE AND SD-----------------------------------------------
exp <- get_exp(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
exp_sd <- get_exp_sd(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
exp <- merge(exp, exp_sd, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
exp[, exp_sd := exp_sd * .5]

#--ADD TMREL -------------------------------------------------------------------
ages <- unique(exp$age_group_id)
tmrel <- get_tmrel(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step,
                   n_draws, tmrel_lower, tmrel_upper, ages)
tmrel[, tmrel := tmrel]
dt <- merge(exp, tmrel, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
rm(exp);rm(tmrel);rm(exp_sd)

#--PULL RR AND MERGE------------------------------------------------------------
# Read in relative risk points and create function of RR in log space to match distribution
# draws with imposed correlation
draw_cols <- paste0("draw_", 0:(n_draws-1))
rr_dt <- fread("FILEPATH/activity_rr_by_exposure.csv")
# drop male breast cancer
if(sex_id == 1) rr_dt <- rr_dt[cause_id != 429,]
for (col in draw_cols) rr_dt[get(col) > 1, (col) := 1]
cause_ids <- rr_dt$cause_id %>% unique
## define splines for each RR - extrapolate
for(cid in cause_ids) {
    for(i in 0:(n_draws-1)) {
        assign(paste0("rr_fun_", cid, "_", i),
               approxfun(rr_dt[cause_id == cid]$exposure,
                         rr_dt[cause_id == cid, get(paste0('draw_',i))],
                         rule = 2))
    }
}

age_trends <- fread("FILEPATH/2019_LPA_age_trend.csv")
age_trends[, cause_id := ifelse(outcome == "ihd", 493, ifelse(outcome == "hemstroke", 496, ifelse(outcome=="diabetes", 976, 495)))]
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
    cause_ids <- intersect(cause_ids, unique(mediation$cause_id))
    if (length(cause_ids) == 0) stop("No mediation to apply")
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

#--CALC PAF ---------------------------------------------------------------------

wlist <- fread(paste0("FILEPATH/", rei, ".csv"))
source("./ensemble/edensity.R")

calc_paf <- function(i) {
    exp_pdf <- approxfun(dens[[demo_dt[i, ]$draw[1] + 1]]$x,
                         dens[[demo_dt[i, ]$draw[1] + 1]]$fx,
                         yleft=0, yright=0)
    pafs <- list()
    for(cid in cause_ids) {
        paf <- tryCatch({
            rr_fun <- get(paste0("rr_fun_", cid, "_", demo_dt[i, ]$draw))
            denom <- integrate(function(x)
                exp_pdf(x) * apply_age_trend(rr_fun(x)/rr_fun(demo_dt[i, ]$tmrel), demo_dt[i, ]$age_group_id, cid, demo_dt[i, ]$draw[1]),
                lower=dens[[demo_dt[i, ]$draw[1] + 1]]$XMIN,
                upper=dens[[demo_dt[i, ]$draw[1] + 1]]$XMAX, stop.on.error = FALSE)$value
            (denom-1)/denom
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
    dens <- mclapply(0:(n_draws-1), function(d) get_edensity(weights, demo_dt[draw == d, ]$exp_mean[1],
                                                             demo_dt[draw == d, ]$exp_sd[1]), mc.cores = 2)
    cap <- mclapply(1:n_draws, function(d)
        if (inv_exp) dens[[d]]$XMIN else dens[[d]]$XMAX, mc.cores = 2)
    pafs <- mclapply(1:nrow(demo_dt), calc_paf, mc.cores = 6)
    demo_dt <- demo_dt[, .(cause_id = cause_ids), by = names(demo_dt)]
    demo_dt <- cbind(demo_dt, paf = unlist(pafs))
    out[[demo]] <- demo_dt
    rm(demo_dt);rm(weights);rm(den);rm(density);rm(pafs)
}
dt <- rbindlist(out, fill=T, use.names=T)

#--SAVE + VALIDATE -------------------------------------------------------------
dt[, `:=` (mortality=1, morbidity=1)]
dt[paf < 0, paf := NA]
save_paf(dt, rei_id, rei, n_draws, out_dir)
