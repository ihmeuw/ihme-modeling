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
source("FILEPATH"  )
source("FILEPATH"  )
source("FILEPATH"  )
source("FILEPATH"  )

## ensemble functions
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
sourceCpp("FILEPATH")

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


dlist <- c(classA, classB, classM)


cgf_ids <- fread("FILEPATH")
loc_metadata <- get_location_metadata(location_set_id = 9)

################################################################################
## load function for sd optim, from user


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


integrate_draws <- function(loc, sex, age, year, draw) {
  
  
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
  
  if(draw == 0 & sex == 1 & year == 2017){
    saveRDS(data.table(location_id = loc, age_group_id = age, sex_id = sex, year_id = year, x = Edensity$x - 10, y = Edensity$fx, 
                       mild_prev = round(integrate(den_fun, Edensity$XMIN, 9)$value, 2),
                       mod_prev = round(integrate(den_fun, Edensity$XMIN, 8)$value, 2),
                       sev_prev = round(integrate(den_fun, Edensity$XMIN, 7)$value, 2)), paste0("FILEPATH", var, "_", loc_metadata[location_id == loc, ihme_loc_id], "_", age, ".rds") )
    
    gg <- ggplot(data.table(x = Edensity$x - 10, y = Edensity$fx)) + 
      geom_point(aes(x,y)) +
      geom_vline(xintercept = -1, color = "pink") + 
      geom_vline(xintercept = -2, color = "red") + 
      geom_vline(xintercept = -3, color = "darkred") +
      ggtitle(label = paste0(loc_metadata[location_id == loc, ihme_loc_id], " ", age, " ", sex, " ", year), 
              subtitle = c(paste0(
                "Prev of mild " , var , ": ", round(integrate(den_fun, Edensity$XMIN, 9)$value, 2),
                "\nPrev of mod " , var , ": ", round(integrate(den_fun, Edensity$XMIN, 8)$value, 2),
                "\nPrev of sev " , var , ": ", round(integrate(den_fun, Edensity$XMIN, 7)$value,2)
              ))) +
      xlab(paste0(var, " distribution "))
    
    pdf(paste0("FILEPATH", var, "/", var, "_", loc_metadata[location_id == loc, ihme_loc_id], "_", age, ".pdf" ))
    print(gg)
    dev.off()
    
  }
  
  
  return(compiled)
}
################################################################################

# get arguments for edensity function
if (var == "HAZ") {
  mean_id <- cgf_ids[ensemble_meid == 10512, stgpr_me]
  sev_id <- cgf_ids[ensemble_meid == 8949, stgpr_me]
  mod_id <- cgf_ids[ensemble_meid == 10556, stgpr_me]
  name <- "stunting"
} else if (var == "WAZ") {
  mean_id <- cgf_ids[ensemble_meid == 10514, stgpr_me]
  sev_id <- cgf_ids[ensemble_meid == 2540, stgpr_me]
  mod_id <- cgf_ids[ensemble_meid == 10560, stgpr_me]
  name <- "underweight"
} else {
  # var is WHZ
  mean_id <- cgf_ids[ensemble_meid == 10516, stgpr_me]
  sd_id <- cgf_ids[ensemble_meid == 10517, stgpr_me]
  sev_id <- cgf_ids[ensemble_meid == 8945, stgpr_me]
  mod_id <- cgf_ids[ensemble_meid == 10558, stgpr_me]
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
                      measure_id = 19, gbd_round_id = gbd_round_id, source = "epi", decomp_step = "step4")
sev_val <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = sev_id,
                     location_id = loc, year_id = year, sex_id = sex, age_group_id = age,
                     measure_id = 18, gbd_round_id = gbd_round_id, source = "epi", decomp_step = "step4")
mod_val <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = mod_id,
                     location_id = loc, year_id = year, sex_id = sex, age_group_id = age,
                     measure_id = 18, gbd_round_id = gbd_round_id, source = "epi", decomp_step = "step4")

print("got draws")

## use edensity function to get distribution of mean scores
weights <- fread(paste0("FILEPATH", name, "/", weight_version, "/weights.csv"),
                 stringsAsFactors = FALSE)
weights <- weights[1, !grepl("_id",names(weights)), with=F]

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
              return(integrate_draws(grid[x,]$loc, grid[x,]$sex, grid[x,]$age, grid[x,]$year, grid[x,]$draw))
              
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

write.csv(data_mild, paste0(thresh_save_dir,var,"/mild/5_",loc, "_", age, "_", sex, "_", year, ".csv"), row.names = FALSE)
write.csv(data_mod, paste0(thresh_save_dir,var,"/mod/5_",loc, "_", age, "_", sex, "_", year, ".csv"), row.names = FALSE)
write.csv(data_sev, paste0(thresh_save_dir,var,"/sev/5_",loc, "_", age, "_", sex, "_", year, ".csv"), row.names = FALSE)
write.csv(data_1_2, paste0(paf_save_dir,var,"/1to2/5_",loc, "_", age, "_", sex, "_", year, ".csv"), row.names = FALSE)
write.csv(data_2_3, paste0(paf_save_dir,var,"/2to3/5_",loc, "_", age, "_", sex, "_", year, ".csv"), row.names = FALSE)

message("Start time:", start_time)
message("End time:", Sys.time())