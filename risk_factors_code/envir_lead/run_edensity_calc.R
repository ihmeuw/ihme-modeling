
## Generate blood lead distribution from backcasted mean exposures

rm(list=ls())

# ptm <- proc.time()

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  jpath <- "FILEPATH" 
  hpath <- "FILEPATH"
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  
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

file <- arg[1]
model.version <- arg[2]
model.id <- arg[3]
cores.provided <- as.numeric(arg[4])
draws.required <- 1000

input_dir <- file.path(jpath,"FILEPATH")
output_dir <- file.path(jpath,"FILEPATH",model.id)
code.dir <- file.path(jpath,"FILEPATH")

source(paste0(code_dir,"FILEPATH"))

#################### Running Regression to predict SD of distribution when missing ##################################

source(paste0(jpath,"FILEPATH"))
locations <- get_location_metadata(gbd_round_id = 4, location_set_id = 22) #REMEMBER TO CHANGE THE GBD_ROUND_ID
locs <- copy(locations[, c("location_id","ihme_loc_id","super_region_id"), with=F])

df <- fread(file.path(jpath,"FILEPATH"))
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

# read in ensemble weights
weights <- fread(file.path(jpath,"FILEPATH"))
weights <- weights[1,-c("location_id","year_id","sex_id","age_group_id"),with=F]
weights <- data.frame(weights)

dt <- fread(file.path(input_dir,file))
dt <- merge(dt,locs,by="location_id")
dt[,super_region_id := as.factor(super_region_id)]
dt[,measure_id := NULL]
draws <- paste0("draw_",seq(0,draws.required-1,1))

dt <- melt(dt,id.vars = c("location_id","super_region_id","year_id","age_group_id","sex_id"))

#################### ##################################

total_groups <- nrow(dt[variable == "draw_0"])

# get every draw of mean exposure and expected sd from that
setnames(dt,"value","mean")
dt[,exp_sd := exp(predict(mod,dt))]

# fit the ensemble distribution for each mean-sd combo (edensity only runs on one set at a time)
run_mcmapply <- function(num) {
  start <- (num*total_groups)+1
  end <- (num+1)*total_groups
  return(mcmapply(get_edensity,mean = dt[start:end,mean],sd = dt[start:end,exp_sd],MoreArgs = list(weights = weights),mc.cores = cores.provided))
}

# ptm <- proc.time()
dens <- lapply(0:(draws.required-1),run_mcmapply)

# generate indices for different outputs of dens
xes <- seq(2,length(dens),4)
fxes <- seq(1,length(dens),4)
xmins <- seq(3,length(dens),4)
xmaxes <- seq(4,length(dens),4)

# pull in coefficients for IQ shift relative risks
rr <- fread(file.path(jpath,"FILEPATH"))
rr <- rr$x

# translate ensemble distribution x values into their corresponding shift in IQ (rr*log(exp+1)), setting shift to 0 
# at exposure of TMREL (currently 2). Also need to keep relative risk coefficient consistent within each draw
iq_shift <- function(draw_num,group_num) {
  shift <- data.table(shift = log(as.vector(unlist(dens[[draw_num]][,group_num][2])) + 1))
  shift[shift <= log(3), shift := 0]
  return(shift$shift * rr[draw_num])
}

run_iq_mclapply <- function(draw_num) {
  return(mclapply(seq(1,total_groups),iq_shift,draw_num = draw_num,mc.cores = cores.provided))
}

shift <- lapply(1:draws.required, run_iq_mclapply)

# mclapply(seq(1,length(dens[xes])),iq_shift,mc.cores = cores.provided,mc.preschedule = F)

# using frequencies at different exposure values, determine average IQ shift for each distribution by summing
# IQ shift by the corresponding frequency. But have to rescale it because the frequencies do not add to 1
# (instead the actual function it represents integrates to 1, but the spacing between X values is much smaller
# than 1, resulting in sum of point-frequencies much larger than 1 to compensate)

calc_avg_shift <- function(draw_num,group_num) {
  avg_shift <- sum(as.numeric(unlist(shift[[draw_num]][group_num]))*as.numeric(as.vector(unlist(dens[[draw_num]][,group_num][1]))))/sum(as.numeric(as.vector(unlist(dens[[draw_num]][,group_num][1]))))
  return(avg_shift)
}

run_avg_mclapply <- function(draw_num,cores.provided) {
  return(mclapply(seq(1,total_groups),calc_avg_shift,draw_num = draw_num,mc.cores = cores.provided))
}

avg_shift_long <- lapply(1:draws.required, run_avg_mclapply,cores.provided = cores.provided)
  

# error checking for mclapply
errors <- lapply(1:draws.required, function(x) any(is.na(avg_shift_long[[x]])))
new.cores <- cores.provided
while (any(unlist(errors))) {
  new.cores <- round(new.cores / 2, digits=0)
  
  message('multicore error...trying again with ', new.cores, ' cores!')
  avg_shift_long <- lapply(1:draws.required, run_avg_mclapply,cores.provided = new.cores)
  
  errors <- lapply(1:draws.required, function(x) any(is.na(avg_shift_long[[x]])))
}

# compile average IQ shifts from the 1000 draws that all correspond to the same year-age-sex group
avg_shift <- list()
for (draw_num in seq(1,draws.required)){
  for (group_num in seq(1,total_groups)) {
    avg_shift[group_num] <- list(append(unlist(avg_shift[group_num]),unlist(avg_shift_long[[draw_num]][group_num])))
  }
}

# Make one datatable of the 10000 draws of average IQ shift for each year-age-sex group
reshape_draws <- function(group_num){
  return(data.table(t(unlist(avg_shift[group_num]))))
}
all_draws <- rbindlist(mclapply(seq(1,total_groups),reshape_draws,mc.cores = cores.provided))

df <- cbind(dt[variable == "draw_0"],all_draws)
oldnames <- paste0("V",seq(1,draws.required))
setnames(df,oldnames,draws)
df <- df[,-c("variable","mean","exp_sd","super_region_id"),with=F]
df[,measure_id := 19]

df <- df[,lapply(.SD, as.numeric), .SDcols = names(df)]


check <- function(col) {
  if (any(is.na(col))) return("NAs found!!!")
}

if (nrow(df[,lapply(.SD, check), .SDcols = names(df)]) == 0) write.csv(df,file.path(output_dir,file),row.names=F)
