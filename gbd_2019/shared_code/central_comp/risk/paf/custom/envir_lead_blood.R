# load libraries and functions
library(data.table)
library(magrittr)
library(gtools)
library(ini)
library(RMySQL)

source("./utils/data.R")
source("./utils/db.R")
source("mediate_rr.R")
source("save.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/interpolate.R")

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

#--PULL LEAD (BLOOD) AND INTELLECTUAL DISABILITY EXPOSURE ----------------------

# iq_shifts that modeler uploads as exposure
iq_shifts <- get_exp(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
setnames(iq_shifts, "exp_mean", "iq_shift")

# pull idiopathic developmental intellectual disability severity sequela YLDs
sequela_ids <- c(487, 488, 489, 490, 491)
int_dis <- rbindlist(lapply(sequela_ids, function(x)
  get_draws(gbd_id_type = "sequela_id", gbd_id = x, location_id = location_id,
            year_id = year_id, sex_id = sex_id, measure_id = 3,
            gbd_round_id = gbd_round_id, decomp_step = decomp_step,
            source = "como", n_draws = n_draws, downsample = TRUE)
  ), use.names = T)
int_dis <- melt(int_dis,
                id.vars = c("sequela_id", "location_id", "year_id", "age_group_id", "sex_id"),
                measure.vars = paste0("draw_", 0:(n_draws - 1)),
                variable.name = "draw", value.name = "yld")
int_dis[, draw := as.numeric(gsub("draw_", "", draw))]

# the ids are in order from borderline -> profound. so sort by seqeula_id
# and then take cumulative sum to find cumulative severities
int_dis <- int_dis[order(-sequela_id)]
int_dis[, yld_cum := cumsum(yld), by=c("location_id", "year_id", "age_group_id",
                                       "sex_id", "draw")]
int_dis[, yld_total := sum(yld), by=c("location_id", "year_id", "age_group_id",
                                      "sex_id", "draw")]
# set IQ cutoff for each severity level
int_dis[, iq_cutoff := ifelse(sequela_id==487, 85,
                       ifelse(sequela_id==488, 70,
                       ifelse(sequela_id==489, 50,
                       ifelse(sequela_id==490, 35, 20))))]
# assume mean of human intelligence as measured by IQ is 100, as indicated by
# the expert group in Comparative Qualification of Health Risks paper
int_dis[, mean_iq := 100]

dt <- merge(iq_shifts, int_dis,
            by=c("location_id", "year_id", "age_group_id", "sex_id", "draw"))

#--CALC PAF --------------------------------------------------------------------

# use the mean SD observed as a conservative approach to our assumed IQ distribution
dt[, sd := (iq_cutoff - mean_iq)/qnorm(yld_cum)][yld_cum==0, sd := iq_cutoff]
dt[, sd_max := mean(sd), by=c("location_id", "year_id", "age_group_id", "sex_id",
                              "draw")]

# use this to rescale prevalences to our assumed normal distribution
dt[, shifted := pnorm((iq_cutoff-(mean_iq+iq_shift))/sd_max)]
dt[, not_shifted := pnorm((iq_cutoff-mean_iq)/sd_max)]

# calculate the paf, weight by ylds, and sum across severity
dt[, paf := (not_shifted-shifted)/not_shifted]
dt[, paf := (paf*yld)/yld_total][yld_total==0, paf := 0]
dt <- dt[, .(paf = sum(paf)), by=c("location_id", "year_id", "age_group_id",
                                   "sex_id", "draw")]

# apply to intellectual disability, YLD only
dt[, cause_id := 582]
dt[, mortality := 0][, morbidity := 1]

#--SAVE + VALIDATE -------------------------------------------------------------

save_paf(dt, rei_id, rei, n_draws, out_dir)
