# load libraries and functions
library(data.table)
library(magrittr)
library(gtools)
library(ini)
library(RMySQL)
library(zoo)

source("./utils/data.R")
source("./utils/db.R")
source("math.R")
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

#--PULL EXPOSURE ---------------------------------------------------------------
# pull BW/GA exposures
lbwsga <- get_exp(rei_id=339, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
setnames(lbwsga, "exp_mean", "lbwsga")
# pull shifted BW/GA exposures
lbwsga_shift <- get_exp(rei_id=rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
setnames(lbwsga_shift, "exp_mean", "lbwsga_shift")
# merge together and drop resid category get draws adds as it's modeled directly
lbwsga <- merge(lbwsga, lbwsga_shift, by = c("location_id", "year_id", "age_group_id", "sex_id", "parameter", "draw"))
lbwsga <- lbwsga[parameter != "cat125"]
rm(lbwsga_shift)

#--PULL BW/GA RR AND MERGE------------------------------------------------------
rr <- get_rr(rei_id=339, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
dt <- merge(lbwsga, rr, by = c("location_id", "year_id", "age_group_id", "sex_id", "parameter", "draw"))

#--PULL BW/GA TMREL AND MERGE---------------------------------------------------
tmrel <- fread("FILEPATH/nutrition_lbw_preterm_tmrel.csv")
full_term <- tmrel[preterm %in% c(37, 38, 40)]$parameter %>% unique
tmrel <- tmrel[, .(age_group_id, sex_id, parameter, preterm, lbw)]
dt <- merge(dt, tmrel, by = c("age_group_id", "sex_id", "parameter"))

# rescale both sets of BW/GA exposures to sum to <= 1 and add residual category
residual_categ <- unique(dt[preterm == 40 & lbw == 4000]$parameter)
dt <- dt[parameter != residual_categ]
dt[, `:=` (total_lbwsga=sum(lbwsga), total_lbwsga_shift=sum(lbwsga_shift)),
   by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id",
          "mortality", "morbidity", "draw")]
dt[total_lbwsga_shift > 1, lbwsga_shift := lbwsga_shift/total_lbwsga_shift]
dt[total_lbwsga > 1, lbwsga := lbwsga/total_lbwsga][, total_lbwsga := NULL]
dt[total_lbwsga_shift > 1, lbwsga_shift := lbwsga_shift/total_lbwsga_shift][, total_lbwsga_shift := NULL]
dt <- rbind(dt[, .(location_id, year_id, age_group_id, sex_id, cause_id, mortality,
                   morbidity, parameter, draw, rr, lbwsga, lbwsga_shift)],
            dt[, .(rr = 1,
                   lbwsga = 1 - sum(lbwsga),
                   lbwsga_shift = 1 - sum(lbwsga_shift),
                   parameter = residual_categ),
               by = c("location_id", "year_id", "age_group_id", "sex_id",
                      "cause_id", "mortality", "morbidity", "draw")])

# add tmrel
dt[, tmrel := ifelse(parameter == residual_categ, 1, 0)]

#--CALC PAF --------------------------------------------------------------
# isolate to ptb incidence
ptb <- dt[cause_id==381, ]
ptb[, `:=` (mortality=0, morbidity=1)]
# define which babies are full/preterm
ptb[, ptb_cat := ifelse(parameter %in% full_term, FALSE, TRUE)]
# sum proportions over categories to get the total % of babies who were pre-term
ptb <- ptb[, lapply(.SD,sum),
           by=setdiff(names(ptb), c("parameter","rr","lbwsga","lbwsga_shift","tmrel")),
           .SDcols=c("lbwsga","lbwsga_shift")]
# calculate the PAF by taking 1 - the CF proportion of ptb/true proportion of ptb
# rational here is that we want to estimate what proportion of ptb is attributable to air pollution
# if there had been no air pollution we would have seen the CF proportion of ptb. By taking 1 - the
# quotient, we get the fraction of LBW babies that could have been avoided.
ptb <- ptb[ptb_cat == TRUE, ]
ptb[ , paf := 1-lbwsga_shift/lbwsga]

# mult exp * RR across BW/GA categories for normal and shifted for all other causes
dt <- dt[, .(lbwsga=sum(lbwsga * rr), lbwsga_shift=sum(lbwsga_shift * rr)),
         by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id",
                "mortality", "morbidity", "draw")]
dt[, paf := (lbwsga - lbwsga_shift)/lbwsga]

#--FORMAT AND SAVE -------------------------------------------------------------
dt <- rbind(dt, ptb, fill=TRUE)
# clean columns
dt$rei_id <- rei_id
# switch to measure_id
dt[, measure_id := ifelse(mortality == 1, 4, 3)]
dt <- dt[, c("rei_id", "location_id", "year_id", "age_group_id", "sex_id",
             "cause_id", "measure_id", "draw", "paf"),
         with = F] %>% setkey %>% unique
setorder(dt, rei_id, location_id, year_id, age_group_id, sex_id, cause_id, measure_id)

# save
file <- paste0("/", location_id, "_", sex_id, ".csv")
write.csv(dt, paste0(out_dir, file), row.names = F)
