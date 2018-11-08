
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

print(paste("BW Job:", loc, sss))

.libPaths(new = FILEPATH) 

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


bw_start_optim <- function(location_id, sex_id, age_group_id, year_id, draw, 
                           bw_mean_, bw_u_2500_){
  
  bw_row <- data.table(location_id = location_id, sex_id = sex_id, age_group_id = age_group_id, year_id = year_id, draw = draw,
                       bw_mean_ = bw_mean_, bw_u_2500_ = bw_u_2500_, removal_flag = 0)

  bw_row[,bw_sd_optim:= {
    if (inherits(try(ans<-optim((bw_mean_/5),
                                fn = bw_calc_optim_sd,
                                mean =bw_mean_,
                                bw_u_2500 = bw_u_2500_,
                                weight_list=bw_wlist,
                                method="Brent",
                                lower=200,
                                upper=900)$par,silent=TRUE),"try-error")) 
      as.numeric(NA)
    else
      ans
  }]
  
  
  Edensity <- get_edensity(bw_wlist, mean = bw_mean_, sd = bw_row[[1, "bw_sd_optim"]])
 
  x <- Edensity$x
  fx <- Edensity$fx
  
 
  for(i in 1:length(Edensity$weights)){
    
    if(Edensity$weights[[i]] == 0){
      
      Edensity$weights[[i]] <- NULL
      Edensity$pars[[i]] <- NULL
      
    }
  }
  
  pars <- data.table(data.frame(Edensity$pars))
  weights <- data.table(data.frame(Edensity$weights))
  pars_weights <- cbind(pars, weights)
  setnames(pars_weights, names(pars_weights), paste0("bw_", names(pars_weights)))
  
  bw_row <- cbind(bw_row, pars_weights)
  
  bw_row[, bw_XMIN := Edensity$XMIN][, bw_XMAX := Edensity$XMAX] 
  
  return(bw_row)
  
}


clean_dt <- function(dt_name){
  
  dt <- get(dt_name)
  dt <- dt[, -c("measure_id", "modelable_entity_id", "model_version_id")]
  setnames(dt, names(dt), gsub(pattern = "draw", x = names(dt), replacement = paste0(dt_name)))
  
}

bw_calc_optim_sd <- function(b, mean, bw_u_2500, weight_list) {
  
  
  Edensity <- get_edensity(weight_list, mean, Vectorize(b)) 
  
  base = seq(Edensity$XMIN,Edensity$XMAX,length.out=length(Edensity$fx))
  dOUT <- bind_cols(as.data.table(x = base),as.data.table(Edensity["fx"]))
  
  SUMt <- (sum(dOUT$fx))
  
  e_bw_u_2500 = dOUT %>% dplyr::filter(base<=2500) %>% dplyr::summarise(sSUM = sum(fx)) %>% dplyr::mutate(lbw=sSUM/SUMt) %>% dplyr::select(lbw)
  e_pred = data.table(lbw = e_bw_u_2500)
  
  obs = data.table(lbw=bw_u_2500)
  sum(((obs-e_pred)^2))
  
}
 
source("FILEPATH/get_draws.R"  )

bw_u_2500 <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = 16282, source = "epi", measure_id = 5, location_id = loc, age_group_id = 2)
bw_mean <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = 15803, source = "epi", measure_id = 5, location_id = loc, age_group_id = 2)
bw_mean[, measure_id := 19 ]
 
dt_list <- c("bw_u_2500", "bw_mean")
 
bw_wlist <- fread(FILEPATH)  
bw_wlist <- bw_wlist[1, list(exp, gamma, invgamma, weibull, lnorm, norm)]
 
bw_wlist <- data.frame(bw_wlist)

clean_list <- lapply(dt_list, clean_dt)

file <- Reduce(function(x, y) merge(x, y, all=TRUE), clean_list)

file <- merged.stack(file,var.stubs=c("bw_u_2500_","bw_mean_"),sep="var.stubs",keep.all=T)

setnames(file, ".time_1", "draw")
 
file <- file[draw < 200, ]

bw_file <- file[, c("location_id", "sex_id", "age_group_id", "year_id", "draw", "bw_u_2500_", "bw_mean_")]
 
source(FILEPATH)

dlist <- c(classA,classB,classM)
 
slots <- 4
if (os=="windows") { cores_to_use = 1  } else {    cores_to_use = ifelse(grepl('Intel', system("cat /proc/cpuinfo | grep \'name\'| uniq", inter = T)), floor(slots * .86), floor(slots*.64)) }

row_list <- split(bw_file, 1:nrow(bw_file))

bw_file_post <- rbindlist(mclapply( row_list, function(x) bw_start_optim(location_id = x$location_id,
                                                                         sex_id = x$sex_id, age_group_id = x$age_group_id,
                                                                         year_id = x$year_id, draw = x$draw, 
                                                                         bw_mean_ = x$bw_mean_, bw_u_2500_ = x$bw_u_2500_),
                                    mc.cores = cores_to_use ), use.names = T, fill = T ) %>% try

 
if(nrow(bw_file_post[removal_flag == 1, ]) != 0){
  
  to_replace <- copy(bw_file_post[removal_flag == 1, ])
  to_replace_list <- split(to_replace, 1:nrow(to_replace))
  
  bw_file_post <- bw_file_post[removal_flag == 0, ]
  
  replace_rows <- function(prev_sex_id, prev_year_id, prev_draw){
     
    replace_pool <- copy(bw_file_post[sex_id == prev_sex_id & year_id == prev_year_id, ])
    
    if(nrow(replace_pool) == 0){
      
      replace_pool <- copy(bw_file_post)
      replace_pool[, sex_id := prev_sex_id]
      replace_pool[, year_id := prev_year_id]
      
    }
    
    new_draw <- replace_pool[draw == sample(replace_pool$draw, 1), ]
    new_draw[, draw := prev_draw]
    
    return(new_draw)
    
  }
  
  replaced_list <- rbindlist(mclapply( to_replace_list, function(x) replace_rows(prev_sex_id = x$sex_id, prev_year_id = x$year_id, prev_draw = x$draw),
                                       mc.cores = cores_to_use ), use.names = T, fill = T )
  
  
  bw_file_post <- rbindlist(list(replaced_list, bw_file_post), use.names = T, fill = T)
  
  
}

bw_file_post <- bw_file_post[, -c("removal_flag")]

saveRDS(bw_file_post, file = FILEPATH)

print(Sys.time())

job.runtime <- proc.time() - start.time
job.runtime <- job.runtime[3]

print(paste0("Time elapsed:", job.runtime))

