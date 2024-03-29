# Project: RF: Lead Exposure
# Purpose: Calculate IQ shifts from blood lead (calc script)

rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {

  jpath <- "FILEPATH"
  hpath <- "FILEPATH"

  args <- commandArgs(trailingOnly = T)

} else {

  jpath <- "FILEPATH"
  hpath <- "FILEPATH"

}

library(ggplot2)
library(data.table)
library(compiler)
library(parallel)
library(fitdistrplus)
library(magrittr)
library(Rcpp)

# set args from array job
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
parameters <- fread("FILEPATH/edensity_parameters.csv")
year <- parameters[task_id, year_id]
file <- parameters[task_id, file]

input_version <- as.numeric(args[1])
output_version <- as.numeric(args[2])
run_id <- as.numeric(args[3])
threads <- as.numeric(args[4])
draws_required <- as.numeric(args[5])

print(c(year, file, args))

## directories
input_dir <- file.path("FILEPATH", input_version, run_id, "output_by_loc")
output_dir <- file.path("FILEPATH", output_version, run_id, "loc_year")
if(!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

code.dir <- "FILEPATH"
source(file.path(code.dir,"get_edensity.R"))

#################### Running Regression to predict SD ##################################

source("FILEPATH/get_location_metadata.R")
locations <- get_location_metadata(gbd_round_id = 7, location_set_id = 22) ######### CHECK GBD ROUND ID ###########
locs <- copy(locations[, c("location_id","ihme_loc_id","super_region_id"), with=F])

df <- fread(file.path(jpath,"FILEPATH/compiled_lead_exp.csv"))
df <- df[,list(ihme_loc_id,year_start,year_end,age_start,age_end,sex_id,mean,standard_deviation,cv_mean_type)]
df <- merge(df,locs,by="ihme_loc_id",all.x=T)
df[,year_id := floor((year_start + year_end)/2)]
df <- df[!is.na(standard_deviation)]
presplit <- df[age_start >= 1 & age_end < 5]

locs <- locs[,list(location_id,super_region_id)]

mod <- lm(log(standard_deviation) ~ mean + as.factor(super_region_id),
            data = presplit)
coefficients <- as.data.table(coef(summary(mod)), keep.rownames = T)

################################# Melting Data table ##########################################################

weights <- fread(file.path(jpath,"FILEPATH/envir_lead_blood.csv"))
weights <- weights[1,-c("location_id","year_id","sex_id","age_group_id"),with=F]
weights <- data.frame(weights)

dt <- fread(file.path(input_dir,file))
dt <- merge(dt,locs,by="location_id")
dt[,super_region_id := as.factor(super_region_id)]
dt[,measure_id := NULL]
draws <- paste0("draw_",seq(0,draws_required-1,1))

dt <- melt(dt,id.vars = c("location_id","super_region_id","year_id","age_group_id","sex_id"))

######################################################

dt <- dt[year_id == year]

total_groups <- nrow(dt[variable == "draw_0"])

# get every draw of mean exposure and expected sd from that
setnames(dt,"value","mean")
dt[,exp_sd := exp(predict(mod,dt))]

# fit the ensemble distribution for each mean-sd combo (edensity only runs on one set at a time)
run_mcmapply <- function(num) {
  start <- (num*total_groups)+1
  end <- (num+1)*total_groups
  return(mcmapply(get_edensity, mean = dt[start:end,mean], sd = dt[start:end,exp_sd], MoreArgs = list(weights = weights), mc.cores = threads))
}

dens <- lapply(0:(draws_required-1),run_mcmapply)

# generate indices for different outputs of dens
xes <- seq(2,length(dens),4)
fxes <- seq(1,length(dens),4)
xmins <- seq(3,length(dens),4)
xmaxes <- seq(4,length(dens),4)

# pull in coefficients for IQ shift relative risks
rr <- fread(file.path(jpath,"FILEPATH/blood_lead_coefs.csv"))
rr <- as.numeric(as.vector(unlist(rr[2])))
rr <- rr[-1]

# translate ensemble distribution x values into their corresponding shift in IQ (rr*log(exp+1)), setting shift to 0
# at exposure of TMREL (currently 0.016). Also need to keep relative risk coefficient consistent within each draw
iq_shift <- function(draw_num,group_num) {
  shift <- data.table(shift = log(as.vector(unlist(dens[[draw_num]][,group_num][2])) + 1))
  shift[shift <= log(0.016+1), shift := 0]
  return(shift$shift * rr[draw_num])
}

run_iq_mclapply <- function(draw_num) {
  # browser()
  return(mclapply(seq(1,total_groups),iq_shift,draw_num = draw_num,mc.cores = threads))
}

shift <- lapply(1:draws_required, run_iq_mclapply)

# using frequencies at different exposure values, determine average IQ shift for each distribution by summing
# IQ shift by the corresponding frequency. But have to rescale it because the frequencies do not add to 1
# (instead the actual function it represents integrates to 1, but the spacing between X values is much smaller
# than 1, resulting in sum of point-frequencies much larger than 1 to compensate)

calc_avg_shift <- function(draw_num,group_num) {
  avg_shift <- sum(as.numeric(unlist(shift[[draw_num]][group_num]))*as.numeric(as.vector(unlist(dens[[draw_num]][,group_num][1]))))/sum(as.numeric(as.vector(unlist(dens[[draw_num]][,group_num][1]))))
  return(avg_shift)
}

run_avg_mclapply <- function(draw_num,threads) {
  return(mclapply(seq(1,total_groups),calc_avg_shift,draw_num = draw_num,mc.cores = threads))
}

avg_shift_long <- lapply(1:draws_required, run_avg_mclapply,threads = threads)

# Hard to tell, but this seems to be point in code where mclapply may be occasionally messing up, so introduced an error check
# here to rerun with fewer cores to try to fix it
errors <- lapply(1:draws_required, function(x) any(is.na(avg_shift_long[[x]])))
new.cores <- threads
while (any(unlist(errors))) {
  new.cores <- round(new.cores / 2, digits=0)

  message('multicore error...trying again with ', new.cores, ' cores!')
  avg_shift_long <- lapply(1:draws_required, run_avg_mclapply,threads = new.cores)

  errors <- lapply(1:draws_required, function(x) any(is.na(avg_shift_long[[x]])))
}

# compile average IQ shifts from the 1000 draws that all correspond to the same year-age-sex group
avg_shift <- list()
for (draw_num in seq(1,draws_required)){
  for (group_num in seq(1,total_groups)) {
    avg_shift[group_num] <- list(append(unlist(avg_shift[group_num]),unlist(avg_shift_long[[draw_num]][group_num])))
  }
}

# Make one datatable of the 1000 draws of average IQ shift for each year-age-sex group
reshape_draws <- function(group_num){
  return(data.table(t(unlist(avg_shift[group_num]))))
}
all_draws <- rbindlist(mclapply(seq(1,total_groups),reshape_draws,mc.cores = threads))

df <- cbind(dt[variable == "draw_0"],all_draws)
oldnames <- paste0("V",seq(1,draws_required))
setnames(df,oldnames,draws)
df <- df[,-c("variable","mean","exp_sd","super_region_id"),with=F]
df[,measure_id := 19]

df <- df[,lapply(.SD, as.numeric), .SDcols = names(df)]

check <- function(col) {
  if (any(is.na(col))) return("NAs found!!!")
}

output_file <- paste0(gsub(".csv", "_", file), year, ".csv")

if (nrow(df[, lapply(.SD, check), .SDcols = names(df)]) == 0) write.csv(df, file.path(output_dir, output_file), row.names=F)

# confirm that the file has been successfully saved
if (output_file %in% list.files(output_dir)) {
  print("JOB FINISHED")
} else {
  print("FILE MISSING...")
}

## END
