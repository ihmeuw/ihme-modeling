rm(list=ls())
start_time <- Sys.time()

library(Rcpp)
library(data.table)
library(dplyr)
library(lme4)
library(mvtnorm)
library(actuar)
library(zipfR)
library(magrittr)
library(parallel)



os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "FILEPATH"
} else {
  jpath <- "FILEPATH"
}

# source the central functions
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_draws.R")


## ensemble functions
source("FILEPATH/eKS_parallel.R")
source("FILEPATH/pdf_families.R")
source("FILEPATH/edensity.R")
sourceCpp("FILEPATH/scale_density_simpson.cpp")

# read in arguments from qsub
print(paste('THE ARGS', as.character(commandArgs())))
var <- as.character(commandArgs()[3])
loc <- as.numeric(commandArgs()[4])
cores <- as.integer(commandArgs()[5])*.5
cores <- as.integer(cores)
gbd_round_id <- as.integer(commandArgs()[6])
thresh_save_dir <- as.character(commandArgs()[7])
weight_version <- as.numeric(commandArgs()[8])
paf_save_dir <- as.character(commandArgs()[9])

is.integer(cores)
is.integer(gbd_round_id)
is.integer(weight_version)

# set other args
demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
sex <- demo$sex_id
year <- demo$year_id
age <- c(2:5)
dlist <- c(classA, classB, classM)

is.integer(sex)
is.integer(year)

cgf_ids <- read.csv(FILEPATH, stringsAsFactors = FALSE)

if (loc == 133 & var == "WHZ"){

  calc_optim_sd <- function(b,mean,prev_sev,prev_mod,weight_list) {

    Edensity <- get_edensity(weight_list, mean, b)

    base <- seq(Edensity$XMIN, Edensity$XMAX, length.out = length(Edensity$fx))
    dOUT <- bind_cols(as.data.table(x = base), as.data.table(Edensity["fx"]))
    SUMt <- sum(dOUT$fx)

    e_prev_mod <- dOUT %>% dplyr::filter(base<8) %>%
      dplyr::summarise(sSUM = sum(fx)) %>%
      dplyr::mutate(preterm=sSUM/SUMt) %>% dplyr::select(preterm)

    return(((e_prev_mod-prev_mod)^2) %>% as.numeric)
  }
} else {

  calc_optim_sd <- function(b,mean,prev_sev,prev_mod,weight_list) {

    Edensity <- get_edensity(weight_list, mean, b)

    base <- seq(Edensity$XMIN, Edensity$XMAX, length.out = length(Edensity$fx))
    dOUT <- bind_cols(as.data.table(x = base), as.data.table(Edensity["fx"]))
    SUMt <- sum(dOUT$fx)


    e_prev_sev <- dOUT %>% dplyr::filter(base<7) %>%
      dplyr::summarise(sSUM = sum(fx)) %>%
      dplyr::mutate(ext_preterm=sSUM/SUMt) %>% dplyr::select(ext_preterm)
    e_prev_mod <- dOUT %>% dplyr::filter(base<8) %>%
      dplyr::summarise(sSUM = sum(fx)) %>%
      dplyr::mutate(preterm=sSUM/SUMt) %>% dplyr::select(preterm)

    return(((e_prev_sev-prev_sev)^2 + (e_prev_mod-prev_mod)^2) %>% as.numeric)

  }

}

integrate_draws <- function(loc, sex, age, year, draw) {

  M <- mean_val[year_id == year & sex_id == sex & age_group_id == age,
                paste0("draw_", draw), with=F] %>% as.numeric
  M <- (M * 10)  
  sev <- sev_val[year_id == year & sex_id == sex & age_group_id == age,
                 paste0("draw_", draw), with=F] %>% as.numeric
  mod <- mod_val[year_id == year & sex_id == sex & age_group_id == age,
                 paste0("draw_", draw), with=F] %>% as.numeric

  SD <-optim((2),
             fn = calc_optim_sd,
             mean = M,
             prev_sev = sev,
             prev_mod = mod,
             weight_list=weights,
             method="Brent",
             lower=0,
             upper=5)
  SD <- SD$par
 
  Edensity <- get_edensity(weights, mean=M, sd=SD)
  x <- seq(Edensity$XMIN, Edensity$XMAX, length=length(Edensity$fx))
  fx <- Edensity$fx
  den_fun <- approxfun(x, fx, yleft=0, yright=0)
  prev_mild <- integrate(den_fun, Edensity$XMIN, 9)$value
  prev_mod <- integrate(den_fun, Edensity$XMIN, 8)$value
  prev_sev <- integrate(den_fun, Edensity$XMIN, 7)$value
  compiled <- data.table(location_id=loc,year_id=year,age_group_id=age,sex_id=sex,
                         prev_mild=prev_mild,prev_mod=prev_mod,prev_sev=prev_sev,
                         draw=paste0("draw_",draw))
  return(compiled)
}
################################################################################

if (var == "HAZ") {
  mean_id <- 10512
  sd_id <- 10513
  sev_id <- 8949
  mod_id <- 10556
  name <- "stunting"
} else if (var == "WAZ") {
  mean_id <- 10514
  sd_id <- 10515
  sev_id <- 2540
  mod_id <- 10560
  name <- "underweight"
} else {
  # var is WHZ
  mean_id <- 10516
  sd_id <- 10517
  sev_id <- 8945
  mod_id <- 10558
  name <- "wasting"
}


print(mean_id)
typeof(mean_id)
print(loc)
typeof(loc)
print(year)
typeof(year)
print(age)
typeof(age)
print(gbd_round_id)
typeof(gbd_round_id)
print(cores)
typeof(cores)

mean_val <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = mean_id,
                      location_id = loc, year_id = year, sex_id = sex, age_group_id = age,
                      measure_id = 19, gbd_round_id = gbd_round_id, source = "epi",
                      num_workers = cores)
sev_val <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = sev_id,
                     location_id = loc, year_id = year, sex_id = sex, age_group_id = age,
                     measure_id = 5, gbd_round_id = gbd_round_id, source = "epi",
                     num_workers = cores)
mod_val <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = mod_id,
                     location_id = loc, year_id = year, sex_id = sex, age_group_id = age,
                     measure_id = 5, gbd_round_id = gbd_round_id, source = "epi",
                     num_workers = cores)

print("got draws")

## use edensity function to get distribution of mean scores
weights <- fread(FILEPATH,
                 stringsAsFactors = FALSE)
weights <- weights[1, !grepl("_id",names(weights)), with=F]

#############################################################################
 
grid <- expand.grid(loc, sex, age, year, 0:999) %>% data.table
setnames(grid, c("loc", "sex", "age", "year", "draw"))
x <- mclapply(1:nrow(grid),
              function(x) {
                integrate_draws(grid[x,]$loc, grid[x,]$sex, grid[x,]$age, grid[x,]$year, grid[x,]$draw)
 
                }, mc.cores = cores)
output <- rbindlist(x)

# recode negatives to zero; xmin is greater than the threshold
output[prev_mild < 0, prev_mild := 0]
output[prev_mod < 0, prev_mod := 0]
output[prev_sev < 0, prev_sev := 0]
# recode greater than 1 to 1; xmax less than threshold
output[prev_mild > 1, prev_mild := 1]
output[prev_mod > 1, prev_mod := 1]
output[prev_sev > 1, prev_sev := 1]

## recode nulls to 0
output[is.na(prev_mild), prev_mild := 0]
output[is.na(prev_mod), prev_mod := 0]
output[is.na(prev_sev), prev_sev := 0]

## make the mutually exclusive categories for paf inputs
output[, prev1_2 := prev_mild - prev_mod]
output[, prev2_3 := prev_mod - prev_sev]

## reshape data frame to be wide and save
data_mild <- dcast(output, location_id + year_id + sex_id + age_group_id ~ draw,
                   value.var="prev_mild")
data_mod <- dcast(output, location_id + year_id + sex_id + age_group_id ~ draw,
                  value.var="prev_mod")
data_sev <- dcast(output, location_id + year_id + sex_id + age_group_id ~ draw,
                  value.var="prev_sev")
data_1_2 <- dcast(output, location_id + year_id + sex_id + age_group_id ~ draw,
                  value.var = "prev1_2")
data_2_3 <- dcast(output, location_id + year_id + sex_id + age_group_id ~ draw,
                  value.var = "prev2_3")
write.csv(data_mild, paste0(thresh_save_dir,var,"/mild/5_",loc,".csv"), row.names = FALSE)
write.csv(data_mod, paste0(thresh_save_dir,var,"/mod/5_",loc,".csv"), row.names = FALSE)
write.csv(data_sev, paste0(thresh_save_dir,var,"/sev/5_",loc,".csv"), row.names = FALSE)
write.csv(data_1_2, paste0(paf_save_dir,var,"/1to2/5_",loc,".csv"), row.names = FALSE)
write.csv(data_2_3, paste0(paf_save_dir,var,"/2to3/5_",loc,".csv"), row.names = FALSE)

message("Start time:", start_time)
message("End time:", Sys.time())
