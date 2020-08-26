# load libraries and functions
rm(list=ls())
library(data.table)
library(magrittr)
library(gtools)
library(ini)
library(RMySQL)
library(reticulate)
source("./utils/db.R")

set.seed(124535)

#-- SET UP ARGS ----------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
rei_id <- as.numeric(args[1])
out_dir <- args[2]

# get risk info
rei_meta <- get_rei_meta(rei_id)
rei <- rei_meta$rei
mes <- fread(paste0(out_dir, "/mes.csv"))

draw_cols <- paste0("draw_", 0:999)

#--PULL EXPOSURE --------------------------------------------------------
dt <- fread(paste0("FILEPATH/model_estimate_final.csv"))
all_exp <- c(dt$upper, dt$lower, dt$mean)
dt <- data.table(exposure=c(min(all_exp), sample(all_exp, 1000-2), max(all_exp)))
dt <- dt[order(exposure)]

# convert exposures to be in the same unit space as RR
# for calcium, g to mg and for pufa and transfat proportion to percent
if (rei == "diet_calcium_low") {
  dt[, exposure := exposure * 1000]
}
if (rei %in% c("diet_pufa", "diet_transfat")) {
  dt[, exposure := exposure * 100]
}

#--PULL RR AND MERGE------------------------------------------------------------
base_rr_dir <- "FILEPATH"

use_condaenv(condaenv="mr_brt_refactor_env",
             conda="FILEPATH")
source_python("FILEPATH/risk_curve.py")

dir.create(paste0(out_dir, "/mrbrt"), showWarnings = F)
exp_file <- paste0(out_dir, "/mrbrt/exposure.csv")
write.csv(dt, exp_file, row.names = F, na="")

pull_rrs <- function(rc) {
  rr_file <- paste0(out_dir, "/mrbrt/", rc, ".csv")
  risk_curve(exp_path=exp_file,
             mod_path=paste0(base_rr_dir, "/", rc, "/ratio_mod_mono.pkl"),
             write_path=rr_file)
  rr <- fread(rr_file)
  rr[, rr := apply(.SD, 1, mean), .SDcols=draw_cols]
  rr[, risk_cause := rc]
  rr <- rr[, c("exposure", "risk_cause", "rr", draw_cols), with=F]
  return(rr)
}
diet_rr <- fread(paste0(base_rr_dir, "/diet_rr.csv")) %>%
  setnames(., "rei_id", "rid") %>%
  .[rid == rei_id, ] %>%
  setnames(., "rid", "rei_id")
all_rrs <- lapply(unique(diet_rr$risk_cause), pull_rrs) %>% rbindlist(., use.names = T)

# add column for cause id
all_rrs <- merge(all_rrs, diet_rr[, .(risk_cause, cause_id)], by="risk_cause", all.x=TRUE, allow.cartesian = TRUE)
write.csv(all_rrs[, c("cause_id", "exposure", "rr", draw_cols), with=F],
          paste0(out_dir, "/mrbrt/rr.csv"), row.names = FALSE)

# old clean up for now
unlink(paste0(out_dir, "/mrbrt/rr"), recursive = T)
unlink(paste0(out_dir, "/mrbrt/exp"), recursive = T)
