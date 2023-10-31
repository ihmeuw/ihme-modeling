# take outputs of weighting function to make a density function for each geog/age/year/sex
# (cont) and integrate to get the area under the curve for prevalence of each var

#rm(list=ls())
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
library(ggplot2)



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
source("FILEPATH/pdf_families.R")


## Move to parallel script
## Getting normal QSub arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- ifelse(is.na(args[1]), "FILEPATH", args[1])

## Retrieving array task_id
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
param_map <- fread(param_map_filepath)

var <- param_map[task_id, var]
loc <- param_map[task_id, location_id]
age <- param_map[task_id, age_group_id]
sex <- param_map[task_id, sex_id]
year <- param_map[task_id, year_id]
gbd_round_id <- param_map[task_id, gbd_round_id]
thresh_save_dir <- param_map[task_id, thresh_save_dir]
weight_version <- param_map[task_id, weight_version]
paf_save_dir <- param_map[task_id, paf_save_dir]
launch.date <- param_map[task_id, launch.date]







dlist <- c(classA, classM)


cgf_ids <- fread("FILEPATH")
loc_metadata <- get_location_metadata(location_set_id = 9)

################################################################################
## load function for sd optim, from Helena M (hpm7@uw.edu)


calc_optim_sd <- function(b,mean,prev_sev,prev_mod,weight_list) {
  
  variance <- b^2
  
  mu <- log(mean/sqrt(1 + (b^2/(mean^2))))
  sdlog <- sqrt(log(1 + (b^2/mean^2)))
  
  XMIN <- qlnorm(0.001, mu, sdlog)
  XMAX <- qlnorm(0.999, mu, sdlog)
  
  
  xx = seq(XMIN, XMAX, length = 1000)
  fx <- xx*0
  
  CDF_Data_Params <- list()
  CDF_Data_Params$mn <- mean
  CDF_Data_Params$variance <- variance
  CDF_Data_Params$XMIN <- XMIN
  CDF_Data_Params$XMAX <- XMAX
  CDF_Data_Params$x <- xx
  CDF_Data_Params$fx <- fx
  offset = 10
  
  
  create_pdf <- function(distlist, weights, CDF_Data_Params) {
    for(z in 1:length(weights)){
      LENGTH <- length(formals(unlist(distlist[[z]]$mv2par)))
      if (LENGTH==4) {
        est <- try((unlist(distlist[[z]]$mv2par(CDF_Data_Params$mn,CDF_Data_Params$variance,XMIN=CDF_Data_Params$XMIN,XMAX=CDF_Data_Params$XMAX))),silent=T)
      } else {
        est <- try((unlist(distlist[[z]]$mv2par(CDF_Data_Params$mn,CDF_Data_Params$variance))),silent=T)
      }
      fxj <- try(distlist[[z]]$dF(CDF_Data_Params$x,est),silent=T)
      if(class(est)=="try-error") {
        fxj <- rep(0,length(CDF_Data_Params$fx))
      }
      fxj[!is.finite(fxj)] <- 0
      CDF_Data_Params$fx <- (CDF_Data_Params$fx + fxj*weights[[z]])
    }
    CDF_Data_Params$fx[!is.finite(CDF_Data_Params$fx)] <- 0
    CDF_Data_Params$fx[length(CDF_Data_Params$fx)] <- 0
    CDF_Data_Params$fx[1] <- 0
    den <- approxfun(CDF_Data_Params$x, CDF_Data_Params$fx, yleft=0, yright=0) #density function
    
    return(den)
  }
  
  den <- create_pdf(distlist = dlist, weights = weights, CDF_Data_Params = CDF_Data_Params)
  
  
  integrate_pdf <- function(den, CDF_Data_Params) {
    CDF_Data_Params$cdfFITYout <- c()
    for(val in CDF_Data_Params$x) {
      v<-try(integrate(den, min(CDF_Data_Params$x), val)$value)
      if (class(v)=="try-error") {
        v <- NA
      }
      CDF_Data_Params$cdfFITYout <- append(CDF_Data_Params$cdfFITYout,v)
    }
    
    
    return(CDF_Data_Params)
  } 
  
  
  CDF_Data_Params <- integrate_pdf(den, CDF_Data_Params)
  
  e_prev_sev_index <- which.min(abs(CDF_Data_Params$x - (offset-3)))
  e_prev_sev <- CDF_Data_Params$cdfFITYout[e_prev_sev_index]
  
  e_prev_mod_index <- which.min(abs(CDF_Data_Params$x - (offset-2)))
  e_prev_mod <- CDF_Data_Params$cdfFITYout[e_prev_mod_index]
  
  
  return(((e_prev_sev-prev_sev)^2 + (e_prev_mod-prev_mod)^2) %>% as.numeric)
  
}


integrate_draws <- function(loc, sex, age, year, draw, weight_version, var) {
  
  
  M <- mean_val[year_id == year & sex_id == sex & age_group_id == age,
                paste0("draw_", draw), with=F] %>% as.numeric
  M <- (M * 10)# should be (m*10)-10, but have to add 10 to get no negative numbers
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
  variance <- SD^2
  
  mu <- log(M/sqrt(1 + (SD^2/(M^2))))
  sdlog <- sqrt(log(1 + (SD^2/M^2)))
  
  XMIN <- qlnorm(0.001, mu, sdlog)
  XMAX <- qlnorm(0.999, mu, sdlog)
  
  
  xx = seq(XMIN, XMAX, length = 1000)
  fx <- xx*0
  
  CDF_Data_Params <- list()
  CDF_Data_Params$mn <- M
  CDF_Data_Params$variance <- variance
  CDF_Data_Params$XMIN <- XMIN
  CDF_Data_Params$XMAX <- XMAX
  CDF_Data_Params$x <- xx
  CDF_Data_Params$fx <- fx
  
  
  create_pdf <- function(distlist, weights, CDF_Data_Params) {
    for(z in 1:length(weights)){
      LENGTH <- length(formals(unlist(distlist[[z]]$mv2par)))
      if (LENGTH==4) {
        est <- try((unlist(distlist[[z]]$mv2par(CDF_Data_Params$mn,CDF_Data_Params$variance,XMIN=CDF_Data_Params$XMIN,XMAX=CDF_Data_Params$XMAX))),silent=T)
      } else {
        est <- try((unlist(distlist[[z]]$mv2par(CDF_Data_Params$mn,CDF_Data_Params$variance))),silent=T)
      }
      fxj <- try(distlist[[z]]$dF(CDF_Data_Params$x,est),silent=T)
      if(class(est)=="try-error") {
        fxj <- rep(0,length(CDF_Data_Params$fx))
      }
      fxj[!is.finite(fxj)] <- 0
      CDF_Data_Params$fx <- (CDF_Data_Params$fx + fxj*weights[[z]])
    }
    CDF_Data_Params$fx[!is.finite(CDF_Data_Params$fx)] <- 0
    CDF_Data_Params$fx[length(CDF_Data_Params$fx)] <- 0
    CDF_Data_Params$fx[1] <- 0
    den <- approxfun(CDF_Data_Params$x, CDF_Data_Params$fx, yleft=0, yright=0) #density function
    
    return(den)
  }
  
  den <- create_pdf(distlist = dlist, weights = weights, CDF_Data_Params = CDF_Data_Params)
  
  
  integrate_pdf <- function(den, CDF_Data_Params) {
    CDF_Data_Params$cdfFITYout <- c()
    for(val in CDF_Data_Params$x) {
      v<-try(integrate(den, min(CDF_Data_Params$x), val)$value)
      if (class(v)=="try-error") {
        v <- NA
      }
      CDF_Data_Params$cdfFITYout <- append(CDF_Data_Params$cdfFITYout,v)
    }
    
    
    return(CDF_Data_Params)
  } 
  
  
  CDF_Data_Params <- integrate_pdf(den, CDF_Data_Params)
  
  offset = 10
  
  prev.mild.index <- which.min(abs(CDF_Data_Params$x - (offset-1)))
  prev.mild.final <- CDF_Data_Params$cdfFITYout[prev.mild.index]
  prev.mod.index <- which.min(abs(CDF_Data_Params$x - (offset -2)))
  prev.mod.final <- CDF_Data_Params$cdfFITYout[prev.mod.index]
  prev.sev.index <- which.min(abs(CDF_Data_Params$x - (offset-3)))
  prev.sev.final <- CDF_Data_Params$cdfFITYout[prev.sev.index]
  prev.extrem.sev.index <- which.min(abs(CDF_Data_Params$x - (offset-4)))
  prev.extrem.sev.final <- CDF_Data_Params$cdfFITYout[prev.extrem.sev.index]
  
  compiled <- data.table(location_id = loc, year_id = year, age_group_id = age, sex_id = sex,
                         prev_mild = prev.mild.final, prev_mod = prev.mod.final, prev_sev = prev.sev.final, prev_e_sev = prev.extrem.sev.final, option = weight_version, meanval = M, sdev = SD,
                         draw = paste0("draw_", draw))

  
  
  return(compiled)
}




################################################################################

# get arguments for edensity function
if (var == "HAZ") {
  mean_id <- cgf_ids[ensemble_me_id == 10512, stgpr_me_id]
  sev_id <- cgf_ids[ensemble_me_id == 8949, stgpr_me_id]
  mod_id <- cgf_ids[ensemble_me_id == 10556, stgpr_me_id]
  name <- "stunting"
} else if (var == "WAZ") {
  mean_id <- cgf_ids[ensemble_me_id == 10514, stgpr_me_id]
  sev_id <- cgf_ids[ensemble_me_id == 2540, stgpr_me_id]
  mod_id <- cgf_ids[ensemble_me_id == 10560, stgpr_me_id]
  name <- "underweight"
} else {
  # var is WHZ
  mean_id <- cgf_ids[ensemble_me_id == 10516, stgpr_me_id]
  sd_id <- cgf_ids[ensemble_me_id == 10517, stgpr_me_id]
  sev_id <- cgf_ids[ensemble_me_id == 8945, stgpr_me_id]
  mod_id <- cgf_ids[ensemble_me_id == 10558, stgpr_me_id]
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


mean_val <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = mean_id,
                      location_id = loc, year_id = year, sex_id = sex, age_group_id = age,
                      measure_id = 19, gbd_round_id = gbd_round_id, source = "epi", decomp_step = "iterative")
sev_val <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = sev_id,
                     location_id = loc, year_id = year, sex_id = sex, age_group_id = age,
                     measure_id = 18, gbd_round_id = gbd_round_id, source = "epi", decomp_step = "iterative")
mod_val <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = mod_id,
                     location_id = loc, year_id = year, sex_id = sex, age_group_id = age,
                     measure_id = 18, gbd_round_id = gbd_round_id, source = "epi", decomp_step = "iterative")

print("got draws")

## use edensity function to get distribution of mean scores
weights <- fread(paste0("FILEPATH"), stringsAsFactors = F)


#############################################################################
# create data frame to lapply over
grid <- expand.grid(loc, sex, age, year, 0:999) %>% data.table
setnames(grid, c("loc", "sex", "age", "year", "draw"))

x <- lapply(1:nrow(grid),
            function(x) {
              
              if(x %% 20 == 0){
                print(Sys.time())
                print(x)
              }
              return(integrate_draws(grid[x,]$loc, grid[x,]$sex, grid[x,]$age, grid[x,]$year, grid[x,]$draw, weight_version, var))
              
            })

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

final.mean <- mean(output$meanval)
final.sd <- mean(output$sdev)

output$meanval <- NULL
output$sdev <- NULL

## reshape data frame to be wide and save
data_mild <- dcast(output, location_id + year_id + sex_id + age_group_id + option ~ draw,
                   value.var="prev_mild")
data_mod <- dcast(output, location_id + year_id + sex_id + age_group_id + option ~ draw,
                  value.var="prev_mod")
data_sev <- dcast(output, location_id + year_id + sex_id + age_group_id + option ~ draw,
                  value.var="prev_sev")
data_e_sev <- dcast(output, location_id + year_id + sex_id + age_group_id + option ~ draw,
                    value.var = "prev_e_sev")
data_1_2 <- dcast(output, location_id + year_id + sex_id + age_group_id + option ~ draw,
                  value.var = "prev1_2")
data_2_3 <- dcast(output, location_id + year_id + sex_id + age_group_id + option ~ draw,
                  value.var = "prev2_3")





# add in the portion about saving the meanSD

mod.ens <- rowMeans(data_mod[,6:1005])
sev.ens <- rowMeans(data_sev[,6:1005])
e.sev.ens <- rowMeans(data_e_sev[, 6:1005])

mean.stgpr <- rowMeans(mean_val[, 2:1001])
mod.stgpr <- rowMeans(mod_val[, 2:1001])
sev.stgpr <- rowMeans(sev_val[, 2:1001])

mean.stgpr.name <- paste0("meid_", mean_id)
mod.stgpr.name <- paste0("meid_", mod_id)
sev.stgpr.name <-  paste0("meid_", sev_id)
mod.ens.name <- paste0("est_meid_", mod_id)
sev.ens.name <- paste0("est_meid_", sev_id)

meansdobj <- data.table(location_id =  loc,
                        sex_id = sex,
                        year_id = year,
                        age_group_id = age,
                        draw = "draw_0",
                        mean.stgpr.name = mean.stgpr,
                        mod.stgpr.name = mod.stgpr,
                        sev.stgpr.name = sev.stgpr,
                        sd = final.sd,
                        mod.ens.name = mod.ens,
                        sev.ens.name = sev.ens,
                        extrem.sev.ens = e.sev.ens,
                        var = var,
                        mean = mean.stgpr)


label = "meanSD"

if(length(loc)==1){label <- paste0(label, "_", loc)}
if(length(age)==1){label <- paste0(label, "_", age)}
if(length(sex)==1){label <- paste0(label, "_", sex)}
if(length(year)==1){label <- paste0(label, "_", year)}



dir.create(paste0("FILEPATH"), recursive = T)
saveRDS(meansdobj, paste0("FILEPATH"))





write.csv(data_mild, paste0("FILEPATH"), row.names = FALSE)
write.csv(data_mod, paste0("FILEPATH"), row.names = FALSE)
write.csv(data_sev, paste0("FILEPATH"), row.names = FALSE)
write.csv(data_e_sev, paste0("FILEPATH"), row.names = FALSE)
write.csv(data_1_2, paste0("FILEPATH"), row.names = FALSE)
write.csv(data_2_3, paste0("FILEPATH"), row.names = FALSE)

message("Start time:", start_time)
message("End time:", Sys.time())





















