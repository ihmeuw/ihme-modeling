# take outputs of weighting function to make a density function for each geog/age/year/sex
# (cont) and integrate to get the area under the curve for prevalence of each var

library(data.table)
library(dplyr)
library(rio)
library(lme4)
library(mvtnorm)
library(actuar)
library(zipfR)


os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "J:/"
} else {
  jpath <- "/home/j/"
}

# source the central functions
source(paste0(jpath,"FILEPATH/get_location_metadata.R"))
source(paste0(jpath,"FILEPATH/get_demographics.R"))
source(paste0(jpath,"FILEPATH/get_ids.R"))
source(paste0(jpath,"FILEPATH/get_draws.R"))

## functions
source(paste0(jpath,"FILEPATH/eKS_parallel.R"))
source(paste0(jpath,"FILEPATH/ihmeDistList.R"))
source("FILEPATH/get_edensity.R")

sex <- c(1:2)
age <- c(2:5)
year <- c(1990, 1995, 2000, 2005, 2010, 2016)
draws <- c(0:999)

# read in arguments 
print(paste('THE ARGS', as.character(commandArgs())))
loc <- as.numeric(commandArgs()[3])
var <- as.character(commandArgs()[4])
region <- as.character(commandArgs()[5])
loc_name <- as.character(commandArgs()[7])
cores <- as.numeric(commandArgs()[6])*.86

# Establish directories
weights_dir <- paste0(jpath, "FILEPATH/")
save_dir <- paste0("FILEPATH")

dlist <- c(classA, classB, classM)


#########################################################################################################################
if (loc == 133 & var == "WHZ"){
  
  calc_optim_sd <- function(b,mean,prev_sev,prev_mod,weight_list) {
    
    Edensity <- get_edensity(weight_list, mean, Vectorize(b)) 
    
    base = seq(Edensity$XMIN,Edensity$XMAX,length.out=length(Edensity$fx))
    dOUT <- bind_cols(as.data.table(x = base),as.data.table(Edensity["fx"]))
    SUMt <- (sum(dOUT$fx))
    
    e_prev_sev = dOUT %>% dplyr::filter(base<7) %>% dplyr::summarise(sSUM = sum(fx)) %>% dplyr::mutate(ext_preterm=sSUM/SUMt) %>% dplyr::select(ext_preterm)
    e_prev_mod = dOUT %>% dplyr::filter(base<8) %>% dplyr::summarise(sSUM = sum(fx)) %>% dplyr::mutate(preterm=sSUM/SUMt) %>% dplyr::select(preterm)
    e_pred = data.table(prev_mod)
    
    obs = data.table(prev_mod=e_prev_mod)
    
    sum(((obs-e_pred)^2))
    
  }
} else {
  calc_optim_sd <- function(b,mean,prev_sev,prev_mod,weight_list) {
    
    Edensity <- get_edensity(weight_list, mean, Vectorize(b)) 
    
    base = seq(Edensity$XMIN,Edensity$XMAX,length.out=length(Edensity$fx))
    dOUT <- bind_cols(as.data.table(x = base),as.data.table(Edensity["fx"]))
    SUMt <- (sum(dOUT$fx))
    
    e_prev_sev = dOUT %>% dplyr::filter(base<7) %>% dplyr::summarise(sSUM = sum(fx)) %>% dplyr::mutate(ext_preterm=sSUM/SUMt) %>% dplyr::select(ext_preterm)
    e_prev_mod = dOUT %>% dplyr::filter(base<8) %>% dplyr::summarise(sSUM = sum(fx)) %>% dplyr::mutate(preterm=sSUM/SUMt) %>% dplyr::select(preterm)
    e_pred = data.table(prev_sev,prev_mod)
    
    
    obs = data.table(prev_sev=e_prev_sev, prev_mod=e_prev_mod)
    
    sum(((obs-e_pred)^2))
    
  }
  
}

integrate_draws <- function(loc, sex, age, year, draw) {
  Means <- setDT(mean_val)[year_id == year & sex_id == sex & age_group_id == age]
  sev_vals <- setDT(sev_val)[year_id == year & sex_id == sex & age_group_id == age]
  mod_vals <- setDT(mod_val)[year_id == year & sex_id == sex & age_group_id == age]
  
  # back transform from st gpr input/output
  M <- Means[1, .SD, .SDcols=paste0("draw_",draw)]
  M <- as.numeric(M)
  M <- (M * 10) 
  
  sev <- sev_vals[1, .SD, .SDcols=paste0("draw_",draw)]
  sev <- as.numeric(sev)
  mod <- mod_vals[1, .SD, .SDcols=paste0("draw_",draw)]
  mod <- as.numeric(mod)
  
  
  SD <-optim((2), 
             fn = calc_optim_sd,
             mean = M,
             prev_sev = sev,
             prev_mod = mod,
             weight_list=weights,
             method="METHODNAME",
             lower=0,
             upper=5)
  
  SD <- SD$par
  
  Edensity <- get_edensity(weights,mean=M,sd=SD)
  
  x <- seq(XMIN,XMAX,length=length(Edensity$fx)) 
  fx <- Edensity$fx
  den_fun <- approxfun(x,fx,yleft=0,yright=0)
  
  prev_mild <- integrate(den_fun,XMIN,9)$value
  prev_mod <- integrate(den_fun,XMIN,8)$value
  prev_sev <- integrate(den_fun,XMIN,7)$value
  
  compiled <- data.frame(location_id=loc,year_id=year,age_group_id=age,sex_id=sex,prev_mild=prev_mild,prev_mod=prev_mod,prev_sev=prev_sev,draw=paste0("draw_",draw))
  
  return(compiled)
  
}
#########################################################################################################################
print(weights_dir)
print(var)
print(loc)

# get arguments for edensity function
if (var == "HAZ"){
  mean_id <- 10512
  sd_id <- 10513
  sev_id <- 8949
  mod_id <- 10556
  name <- "stunting"
} else if (var == "WAZ"){
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

start_time <- Sys.time()

mean_val <- get_draws(gbd_id_field = "modelable_entity_id", gbd_id = mean_id, location_ids = loc, measure_ids = 19, status = "best", source = "epi")
sev_val <- get_draws(gbd_id_field = 'modelable_entity_id', gbd_id = sev_id, location_ids = loc, measure_ids = 5, status = "best", source = "epi")
mod_val <- get_draws(gbd_id_field = 'modelable_entity_id', gbd_id = mod_id, location_ids = loc, measure_ids = 5, status = "best", source = "epi")

print(paste0("Cores = ",cores))

## use edensity function to get distribution of mean scores 
weights <- read.csv(paste0(jpath,"FILEPATH/",name,"_global_weights.csv"), stringsAsFactors = FALSE)
weights <- weights[1,2:14]

#############################################################################
# create data frame to mclapply over
grid <- expand.grid(loc, sex, age, year, draws)
setnames(grid, c("loc", "sex", "age", "year", "draw"))

x <- mclapply(1:nrow(grid), function(x) integrate_draws(grid[x,"loc"], grid[x,"sex"], grid[x,"age"], grid[x,"year"], grid[x,"draw"]), mc.cores = cores)
output <- rbindlist(x)
end_time <- Sys.time()

#############################################################################
end_time <- Sys.time()

# recode negatives to zero; xmin is greater than the threshold

output$prev_mild[output$prev_mild < 0] <- 0
output$prev_mod[output$prev_mod < 0] <- 0
output$prev_sev[output$prev_sev < 0] <- 0

# recode greater than 1 to 1; xmax less than threshold
output$prev_mild[output$prev_mild > 1] <- 1
output$prev_mod[output$prev_mod > 1] <- 1
output$prev_sev[output$prev_sev > 1] <- 1

## reshape data frame to be wide 
data_mild <- output[,c(1,2,3,4,5,8)]
data_mild <- dcast(output, location_id + year_id + sex_id + age_group_id ~ draw, value.var="prev_mild")

data_mod <- output[,c(1,2,3,4,6,8)]
data_mod <- dcast(output, location_id + year_id + sex_id + age_group_id ~ draw, value.var="prev_mod")

data_sev <- output[,c(1,2,3,4,7,8)]
data_sev <- dcast(output, location_id + year_id + sex_id + age_group_id ~ draw, value.var="prev_sev")


write.csv(data_mild,paste0(save_dir,"FILEPATH/5_",loc,".csv"),row.names=FALSE)
write.csv(data_mod,paste0(save_dir,"FILEPATH/5_",loc,".csv"),row.names=FALSE)
write.csv(data_sev,paste0(save_dir,"FILEPATH/5_",loc,".csv"),row.names=FALSE)


