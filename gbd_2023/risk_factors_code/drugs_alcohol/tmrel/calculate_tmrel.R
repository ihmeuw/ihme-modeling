################
# 4.3 Model TMREL
################

##### Load packages & functions ---------------------------------------------------------------------------------------
library(tidyverse)
library(data.table)
library(plyr)
library(dplyr)
library(parallel)


arguments <- commandArgs(trailingOnly = T)
folder <- arguments[1]
run_all <- arguments[2]
message(folder)
message(paste0("run all? ", run_all))

share_dir <- 'FILEPATH'

code_directory <- 'FILEPATH'
source(paste0('FILEPATH'))
source(paste0('FILEPATH'))
save_dir <- 'FILEPATH'


daly_path <- paste0('FILEPATH')
rr_version <- "v4"
rr_path <- 'FILEPATH'

invisible(sapply(list.files('FILEPATH', full.names = T), source)) 


##### Clean merged RR & DALY file ---------------------------------------------------------------------------------------

direct <- F


if (direct == T){
  l <- 159
  a <- 8
  s <- 1
  aud <- FALSE
  cap <- 3
  
} else{
  param_map <- fread(paste0('FILEPATH'))
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  
  #d <- param_map[task_id, draw]
  s <- param_map[task_id, sex]
  l <- param_map[task_id, location]
  a <- param_map[task_id, age]
  aud <- as.logical(arguments[3])
  cap <- as.numeric(arguments[4])
}

print(paste0("Location: ", l))
print(paste0("Sex: ", s))
print(paste0("Age: ", a))


draw <- 0:999

if (aud){
  input_path <- paste0('FILEPATH')
  path       <- paste0('FILEPATH')
  input_path <- paste0('FILEPATH')
  path       <- paste0('FILEPATH')
  
}
dir.create(path)


message("creating weighted rr curve")
print(paste(l, a, s))
    
rr <- fread(paste0(input_path, l,"_",s,"_",a,".csv"))
if("decade" %in% specific){
  rr[year_id %in% c(1990:1999), decade := "1990"]
  rr[year_id %in% c(2000:2009), decade := "2000"]
  rr[year_id %in% c(2010:2019), decade := "2010"]
  rr[year_id %in% c(2020:2029), decade := "2020"]
} else{
  rr[, decade := year_id]
}

rr[, min_all_cause_rr := min(.SD$all_cause_rr), by=c("draw", specific)]

cols <- c("draw", specific)
test <- copy(rr[,c("exposure", "draw", specific, "all_cause_rr", "min_all_cause_rr"), with = F]) %>%
      filter(all_cause_rr == min_all_cause_rr) %>%
      group_by(dplyr::across({{ cols }}))%>%
      mutate(tmrel = exposure) %>%
      as.data.frame() %>%
      unique




rr <- merge(rr, test[,c("draw","tmrel", specific)], by = c("draw", specific), all.x = T, allow.cartesian = TRUE)

rr <- rr %>%
  
  .[, derivat := (abs(all_cause_rr))] %>%
  .[exposure == 0 & min_all_cause_rr != all_cause_rr, derivat := 999] %>%
  .[, min_derivat := min(derivat), by = c(specific, "draw")] %>%
  .[min_derivat == derivat, crossover_point := exposure] %>%
  .[, crossover_point := min(crossover_point, na.rm = T), by = c(specific, "draw")] 
  
  
rr <- rr %>% filter(draw %in% c(0:100))  

fwrite(rr, paste0('FILEPATH'), row.names = F)


message("rr written successfully!")


all_cause_rr <- rr[, c(specific, "exposure", "draw", "all_cause_rr", "crossover_point"), with = F] %>% 
  unique() %>%
  # all cause rr
  .[, `:=`(mean_all_cause_rr = mean(.SD$all_cause_rr),
           lower_all_cause_rr = quantile(.SD$all_cause_rr, 0.025),
           upper_all_cause_rr = quantile(.SD$all_cause_rr, 0.975)),
    by = c("exposure", specific)] %>%
  
  # crossover/ abstinence equivalency point
  .[, `:=`(mean_crossover = mean(.SD$crossover_point),
           lower_crossover = quantile(.SD$crossover_point, 0.025),
           upper_crossover = quantile(.SD$crossover_point, 0.975)),
    by = c(specific)] %>%

  .[, c(specific, "exposure", 
        "mean_all_cause_rr", "lower_all_cause_rr", "upper_all_cause_rr", 
        "mean_crossover", "lower_crossover", "upper_crossover"), with = F] %>%
  unique()


tmrel_summary <- copy(rr[, c(specific, "draw", "tmrel", "min_all_cause_rr"), with = F]) %>%
  unique %>%
  .[, `:=`(mean_tmrel = mean(.SD$tmrel),
           lower_tmrel = quantile(.SD$tmrel, 0.025),
           upper_tmrel = quantile(.SD$tmrel, 0.975)),
    by = c(specific)] %>%
  .[, c(specific, "mean_tmrel", "lower_tmrel", "upper_tmrel"), with = F] %>%
  unique 

