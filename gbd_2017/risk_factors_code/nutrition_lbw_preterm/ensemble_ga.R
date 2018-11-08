
rm(list=ls())

print(Sys.time())

start.time <- proc.time()

os <- .Platform$OS.type
if (os=="windows") {
  j <- FILEPATH
  h <- FILEPATH
} else {
  j <- FILEPATH
  h <- FILEPATH
}


args <- commandArgs(trailingOnly = TRUE)

loc <- args[1]
sss <- args[2]
 

print(paste("GA Job:", loc, sss))

.libPaths(FILEPATH) 

library(splitstackshape);library(dplyr);library(actuar);library(data.table);library(foreach)
library(iterators);library(doParallel);library(rio);library(dfoptim);library(fitdistrplus)
library(RColorBrewer);library(ggplot2);library(grid);library(lme4);library(mvtnorm)
library(zipfR);library(magrittr);library(parallel)

source("FILEPATH/get_draws.R")
source("FILEPATH/get_edensity.R")

microdata <- fread(FILEPATH)

Corner_text <- function(text, location="topright"){
  legend(location,legend=text, bty ="n", pch=NA) 
}

clean_dt <- function(dt_name){
  dt <- get(dt_name)
  dt <- dt[, -c("measure_id", "modelable_entity_id", "model_version_id")]
  setnames(dt, names(dt), gsub(pattern = "draw", x = names(dt), replacement = paste0(dt_name))
}


ga_start_optim <- function(location_id, sex_id, age_group_id, year_id, draw, 
                           ga_mean_, ga_u_37_){
  
   
  ga_row <- data.table(location_id = location_id, sex_id = sex_id, age_group_id = age_group_id, year_id = year_id, draw = draw,
                       ga_mean_ = ga_mean_, ga_u_37_ = ga_u_37_)
  
  
  
  ga_row[,ga_sd_optim:= {
    if (inherits(try(ans<-optim((ga_mean_/10),
                                fn = ga_calc_optim_sd,
                                mean =ga_mean_,
                                ga_u_37 = ga_u_37_,
                                weight_list=ga_wlist,
                                method="Brent",
                                lower= 0.01,
                                upper= 5)$par,silent=TRUE),"try-error")) 
      as.numeric(NA)
    else
      ans
  }]
  
  
  
  Edensity <- get_edensity(ga_wlist, mean = ga_mean_, sd = ga_row[[1, "ga_sd_optim"]])
  
  for(i in 1:length(Edensity$weights)){
    
    if(Edensity$weights[[i]] == 0){
      
      Edensity$weights[[i]] <- NULL
      Edensity$pars[[i]] <- NULL
      
    }
  }
  
  pars <- data.table(data.frame(Edensity$pars))
  weights <- data.table(data.frame(Edensity$weights))
  pars_weights <- cbind(pars, weights)
  setnames(pars_weights, names(pars_weights), paste0("ga_", names(pars_weights)))
  
  ga_row <- cbind(ga_row, pars_weights)
  
  ga_row[, ga_XMIN := Edensity$XMIN][, ga_XMAX := Edensity$XMAX]
  
  return(ga_row)
  
}
 
ga_calc_optim_sd <- function(b,mean,ga_u_37,weight_list) {
  
  
  Edensity <- get_edensity(weight_list, mean, Vectorize(b))  
  
  base = seq(Edensity$XMIN,Edensity$XMAX,length.out=length(Edensity$fx))
  dOUT <- bind_cols(as.data.table(x = base),as.data.table(Edensity["fx"]))
  
  SUMt <- (sum(dOUT$fx))
    
  e_ga_u_37 = dOUT %>% dplyr::filter(base<37) %>% dplyr::summarise(sSUM = sum(fx)) %>% dplyr::mutate(preterm=sSUM/SUMt) %>% dplyr::select(preterm)
  
  e_pred = data.table(e_ga_u_37)
  
  obs = data.table(preterm=ga_u_37)
  
  sum((obs-e_pred)^2)
 
}


#------------------------------------------------

ga_u_37 <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = 15801, source = "epi", measure_id = 5, location_id = loc, age_group_id = 2)
ga_mean <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = 15802, source = "epi", measure_id = 5, location_id = loc, age_group_id = 2)
ga_mean[, measure_id := 19 ]
 
dt_list <- c("ga_u_37", "ga_mean")
 
ga_wlist <- fread(FILEPATH)
ga_wlist <- ga_wlist[1, list(exp, gamma, invgamma, weibull, lnorm, norm)]
ga_wlist <- data.frame(ga_wlist)

clean_list <- lapply(dt_list, clean_dt)

file <- Reduce(function(x, y) merge(x, y, all=TRUE), clean_list)

file <- merged.stack(file,var.stubs=c("ga_u_37_","ga_mean_"),sep="var.stubs",keep.all=T)

setnames(file, ".time_1", "draw")
 
file <- file[draw < 200, ]

ga_file <- file[, c("location_id", "sex_id", "age_group_id", "year_id", "draw", "ga_u_37_", "ga_mean_")]

source(FILEPATH)

dlist <- c(classA,classB,classM)
 
slots <- 4
if (os=="windows") { cores_to_use = 1  } else {    cores_to_use = ifelse(grepl('Intel', system("cat /proc/cpuinfo | grep \'name\'| uniq", inter = T)), floor(slots * .86), floor(slots*.64)) }

row_list <- split(ga_file, 1:nrow(ga_file))

ga_file_post <- rbindlist(mclapply( row_list, function(x) ga_start_optim(location_id = x$location_id,
                                                                         sex_id = x$sex_id, age_group_id = x$age_group_id,
                                                                         year_id = x$year_id, draw = x$draw, 
                                                                         ga_mean_ = x$ga_mean_, ga_u_37_ = x$ga_u_37_),
                                    mc.cores = cores_to_use ) , use.names = T, fill = T)


print(paste("Rows", nrow(ga_file_post)))

saveRDS(ga_file_post, file = FILEPATH)

print(Sys.time())

job.runtime <- proc.time() - start.time
job.runtime <- job.runtime[3]

print(paste0("Time elapsed:", job.runtime))