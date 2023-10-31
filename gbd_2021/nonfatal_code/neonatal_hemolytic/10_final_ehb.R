# --------------
# Title:    10_A_final_ehb.R
# Date:     2018-04-16
# Purpose:  Sum EHB prevalence across four etiologies of Rh disease, G6PD, preterm, and other. Produce three identical
#           sets of estimates for the three age groups of at birth, early neonatal, and late neonatal.
# Requirements: Update the qsub so that it launches from the current directory of the repos.
# Last Update:  2020-07-30
# --------------

rm(list = ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "PATHNAME"
  h <-"PATHNAME"
  my_libs <- "PATHNAME"
} else {
  j<- "PATHNAME"
  h<-"PATHNAME"
  my_libs <- "PATHNAME"
}

library(data.table)
library(magrittr)
library(ggplot2)

source("PATHNAME/get_demographics.R"  )
source("PATHNAME/get_demographics_template.R"  )

template <- get_demographics("epi")
template_locs <- template[[1]]
template_yrs <- template[[4]]

out_dir <- 'PATHNAME'

rh <- fread("PATHNAME")
rh[, type := "rh"]
# location_id, year, sex, ihme_loc_id

g6pd <- fread(paste0(out_dir,"PATHNAME/g6pd_ehb_all_draws.csv"))
g6pd[, type := "g6pd"]
# location_id, sex, year, location_name

preterm <- fread(paste0(out_dir,"PATHNAME/preterm_ehb_all_draws.csv"))
preterm[, type := "preterm"]
# location_id, sex, year

other <- fread(paste0(out_dir,"PATHNAME/other_ehb_all_draws.csv"))
other[, type := "other"]
# location_id, sex, year, ihme_loc_id

ehb <- rbindlist(list(rh, g6pd, preterm, other), use.names = T, fill = T)

ehb <- ehb[year %in% template_yrs & sex %in% c(1,2), c("location_id", "sex", "year", "type", paste0("draw_", 0:999))]

ehb <- melt(ehb, id.vars = c("location_id", "sex", "year", "type"), variable.name = "draw")

ehb <- ehb[location_id %in% template_locs, ]

ehb[, draw := gsub(draw, pattern = "draw_", replacement = "")]

ehb <- ehb[, sum(value), by = list(location_id, sex, year, draw)]

ehb <- as.data.table(dcast(ehb, location_id + sex + year ~ paste0("draw_", draw), value.var = "V1"))

ehb_bp <- copy(ehb)
ehb_bp[,age_group_id := 164]

ehb_enn <- copy(ehb)
ehb_enn[,age_group_id := 2]

ehb_lnn <- copy(ehb)
ehb_lnn[,age_group_id := 3]

ehb_neonatal <- rbindlist(list(ehb_bp, ehb_enn, ehb_lnn), use.names = T, fill = T)

setnames(ehb_neonatal, c("sex", "year"), c("sex_id", "year_id"))

for(loc in template_locs){

  write.csv(ehb_neonatal[location_id == loc, ], paste0("PATHNAME", loc, ".csv"), na = "", row.names = F)
  
}

# commenting out until we're ready to actually save results - KML
## QSUB
job_flag <- '-N save_ehb'
project_flag <- paste0("-P PATHNAME")
thread_flag <- "-l fthread=20"
mem_flag <- "-l m_mem_free=10G"
runtime_flag <- "-l h_rt=12:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q i.q"
next_script <- "PATHNAME/06_B_save_ehb_results.R"
errors_flag <- paste0("-e PATHNAME")
outputs_flag <- paste0("-o PATHNAME")
shell_script <- "-cwd PATHNAME/r_shell_ide.sh"

qsub <- paste("qsub ", job_flag, project_flag, thread_flag, mem_flag, runtime_flag, jdrive_flag, queue_flag,
              errors_flag, outputs_flag, shell_script, next_script)

system(qsub)


draw_cols <- paste0("draw_", 0:999)
ehb_neonatal$mean <- rowMeans(ehb_neonatal[,draw_cols,with=FALSE], na.rm = T)
ehb_neonatal$lower <- ehb_neonatal[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
ehb_neonatal$upper <- ehb_neonatal[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
ehb_neonatal <- ehb_neonatal[,-draw_cols,with=FALSE]
write.csv(ehb_neonatal,paste0("PATHNAME/06_final_ehb_all_locs_means.csv"))
