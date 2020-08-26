#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Calculate PAFs for occ injuries
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# load packages, install if missing
pacman::p_load(data.table, ggplot2, magrittr, openxlsx)
library(fst, lib.loc = "FILEPATH")

# set working directories
home.dir <- "FILEPATH"
setwd(home.dir)

# set project values
location_set_version_id <- 443

# Set parameters from input args
arg <- commandArgs(trailingOnly = T)
country <- arg[1]
squeeze.version <- arg[2]
output.version <- arg[3]
draws.required <- as.numeric(arg[4])
decomp <- arg[5]
cores.provided <- as.numeric(arg[6])

#settings
year_start <- 1990
year_end <- 2019
years <- 1990:2019
relevant.ages <- c(8:20, 30) #only ages 15-80 (85+ = 0 participation)

#colnames
draw.cols <- paste0('draw_', 0:(draws.required-1))
death.cols <- paste0('cod_', 0:(draws.required-1))
paf.cols <- paste0('paf_', 0:(draws.required-1))

##in##
doc.dir <- file.path(home.dir, "FILEPATH")
injury.causes <- file.path(doc.dir, "FILEPATH") %>% fread
  injury.causes <- injury.causes[keep==1]
  injury.causes[, isic_code := major]
isic.map <- file.path(doc.dir, "FILEPATH")
isic.3.map <- read.xlsx(isic.map, sheet = "ISIC_REV_3_1") %>% as.data.table
exp.dir <- file.path("FILEPATH", squeeze.version)

##out##
summary.dir <- file.path("FILEPATH", output.version)
paf.dir <- file.path("FILEPATH", output.version)
if(!dir.exists(summary.dir)) dir.create(summary.dir, recursive = TRUE)
if(!dir.exists(paf.dir)) dir.create(paf.dir, recursive = TRUE)

#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "FUNCTION") %>% source
# central functions
source("FUNCTION")
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#create a function and use it to follow just N draw(s) through the whole process
drawTracker <- function(varlist,
                        random.draws) {

  varlist[-random.draws] %>% return

}

subtract <- sapply(list(draw.cols, death.cols, paf.cols),
                   drawTracker,
                   random.draws=sample(draws.required, 3))
#***********************************************************************************************************************

#----CALC PAF-----------------------------------------------------------------------------------------------------------
#read in the squeezed injuries
all <- file.path(exp.dir, paste0(country, '_inj.fst')) %>% read.fst(as.data.table=T)
setkeyv(all, c('location_id', 'year_id', 'age_group_id', 'sex_id', 'isic_code'))
all <- unique(all, by=key(all))

#now sum the total squeezed injuries by id variable in order to calculate the PAF
#PAF = total occ injuries / total injuries

#first get the relevant injuries
#read in this year file
relevant.causes <- unique(injury.causes$cause_id)
deaths <- get_draws(gbd_id_type=rep("cause_id", relevant.causes %>% length),
                    gbd_id=relevant.causes, source="codcorrect", location_id=country,
                    year_id=c(year_start:year_end), version=101, age_group_id=relevant.ages,
                    measure_id=1, decomp_step=decomp, num_workers=(cores.provided-1), gbd_round_id = 6)

setnames(deaths, draw.cols, death.cols)

#combine with the selected cause pairs
deaths <- merge(deaths, injury.causes[, list(cause_id, cause_name, isic_code)], by='cause_id', allow.cartesian=TRUE)
pafs <- merge(deaths,  all[, c(key(all), draw.cols), with=F], by=key(all))

#sum over causes to get the total relevant injuries for an industry type (some have different denominator)
pafs[, (death.cols) := lapply(.SD, sum), .SDcols=death.cols, by=key(pafs)] #calculate totals

#calculate PAFs as total occ injuries / total injuries
pafs[, (paf.cols) := lapply(1:draws.required, function(draw) get(draw.cols[draw]) / get(death.cols[draw]))]

#now sum across the industries to get the cause specific PAF
setkeyv(pafs, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'cause_id'))
pafs[, (paf.cols) := lapply(.SD, sum), .SDcols=paf.cols, by=key(pafs)] #calculate totals
pafs <- unique(pafs, by=key(pafs)) #no longer need industry specific values
  pafs[, isic_code := NULL]

#now, cap the PAF to 85% using the average value seen in highest observed country with insurance reported data
#this is guatemala, so used the mean value for males 15-49 in 1990 as the maximum feasible PAF
#this is a fix for possible overestimation (paf>1) where the numerator (total injuries) is undercounted
#or where the denomator (total injuries based on the rates/industry composition) is being extrapolated too high
paf.cap <- .85
pafs[, paf_mean := rowMeans(.SD), .SDcols=paf.cols]
pafs[, residual := paf_mean - paf.cap]
pafs[residual > 0, shift := residual]
pafs[residual <=0, shift := 0]
pafs[, (paf.cols) := lapply(.SD, function(x) {x-shift}), .SDcols=paf.cols]

#calculate mean/CI for PAF
pafs[, paf_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=paf.cols]
pafs[, paf_mean := rowMeans(.SD), .SDcols=paf.cols]
pafs[, paf_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=paf.cols]

#now output values
for (year in years) {

  for (sex in c(1,2)) {

    out <- pafs[sex_id == sex & year_id == year, c(key(pafs), paf.cols), with=F]

    out[, risk := "occ_inj"]

    write.csv(out, file= paste0(paf.dir, "/paf_yll_", country, "_", year, "_", sex, ".csv"))
    write.csv(out, file= paste0(paf.dir, "/paf_yld_", country, "_", year, "_", sex, ".csv"))

  }

}

message("DONE! draws saved to ", paf.dir)

summary <- pafs[, c(key(pafs),
                    names(pafs)[grep("lower|mean|upper", names(pafs))]), with=F]

write.csv(summary,
          file.path(summary.dir, paste0(country, '_paf.csv')))

message("DONE! summary saved to ", summary.dir)

#***********************************************************************************************************************
