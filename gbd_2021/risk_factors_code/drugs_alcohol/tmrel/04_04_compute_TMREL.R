################
# 4.3 Model TMREL
################

##### Load packages & functions ---------------------------------------------------------------------------------------
library(dplyr)
library(tidyverse)
library(data.table)
library(plyr)
library(dplyr)
library(parallel)
library(ggplot2)

source('FILEPATH')
source('FILEPATH')
source('FILEPATH') # 04_00_tmrel_config.R
source('FILEPATH') # 04_00_tmrel_functions.R

##### Clean merged RR & DALY file ---------------------------------------------------------------------------------------
daly_vers <- "oldagecollapse"

param_map <- fread('FILEPATH')
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))

draw <- 0:999

l <- param_map[task_id, location]
a <- param_map[task_id, age]
s <- param_map[task_id, sex]

input_path <- 'FILEPATH'
path       <- 'FILEPATH'
dir.create(path)

message("creating weighted rr curve")

rr <- rbindlist(mclapply(l, weighted_rr, sex = s, age = a, mc.cores = 5))
fwrite(rr, 'FILEPATH', row.names = F)

rr[, min_all_cause_rr := min(.SD$all_cause_rr), by=c("draw", specific)]

cols <- c("draw", specific)
test <- copy(rr[,c("exposure", "draw", specific, "all_cause_rr", "min_all_cause_rr"), with = F]) %>% 
  filter(all_cause_rr == min_all_cause_rr) %>%
  group_by(dplyr::across({{ cols }}))%>% 
  mutate(tmrel = exposure) %>% 
  as.data.frame() %>% 
  unique

rr <- merge(rr, test[,c("draw","tmrel", specific)], by = c("draw", specific), all.x = T)

fwrite(rr, 'FILEPATH', row.names = F)
message("rr written successfully!")

##### Create sub-files ---------------------------------------------------------------------------------------

rr <- rr %>%
  .[, derivat := (abs(log(all_cause_rr)))] %>%
  .[exposure == 0 & min_all_cause_rr == all_cause_rr, derivat := (abs(log(all_cause_rr)))] %>%
  .[exposure == 0 & min_all_cause_rr != all_cause_rr, derivat := 999] %>%
  .[, min_derivat := min(derivat), by = c(specific, "draw")] %>%
  .[min_derivat == derivat, crossover_point := exposure] %>%
  .[, crossover_point_min := min(crossover_point, na.rm = T), by = c(specific, "draw")] %>%
  .[, crossover_point_max := max(crossover_point, na.rm = T), by = c(specific, "draw")] %>%
  .[, test_col := crossover_point_min == crossover_point_max] 

all_cause_rr <- rr[, c(specific, "exposure", "draw", "all_cause_rr", "crossover_point_min"), with = F] %>% 
  unique %>%
  # all cause rr
  .[, `:=`(mean_all_cause_rr = mean(.SD$all_cause_rr),
           lower_all_cause_rr = quantile(.SD$all_cause_rr, 0.025),
           upper_all_cause_rr = quantile(.SD$all_cause_rr, 0.975)),
    by = c("exposure", specific)] %>%
  
  # crossover/ abstinence equivalency point
  .[, `:=`(mean_crossover = mean(.SD$crossover_point_min),
           lower_crossover = quantile(.SD$crossover_point_min, 0.025),
           upper_crossover = quantile(.SD$crossover_point_min, 0.975)),
    by = c(specific)] %>%
  
  .[, c(specific, "exposure", 
        "mean_all_cause_rr", "lower_all_cause_rr", "upper_all_cause_rr", 
        "mean_crossover", "lower_crossover", "upper_crossover"), with = F] %>%
  unique


tmrel_summary <- copy(rr[, c(specific, "draw", "tmrel", "min_all_cause_rr"), with = F]) %>%
  unique %>%
  .[, `:=`(mean_tmrel = mean(.SD$tmrel),
           lower_tmrel = quantile(.SD$tmrel, 0.025),
           upper_tmrel = quantile(.SD$tmrel, 0.975)),
    by = c(specific)] %>%
  .[, c(specific, "mean_tmrel", "lower_tmrel", "upper_tmrel"), with = F] %>%
  unique 

presentation_df <- merge(all_cause_rr, tmrel_summary, by = specific, all = T)

### Clean summary data table -------------------------------------------------------------------------

if (! "age_group_id" %in% specific){
  presentation_df[,age_group_id := "All Ages"]
} else {
  presentation_df <- merge(presentation_df, age_name, by = "age_group_id", all.x = T)
}

if (! "sex_id" %in% specific){
  presentation_df[,sex := "Both sexes"]
  presentation_df[,sex_id := 3]
} else {
  presentation_df <- merge(presentation_df, sex_name, by = "sex_id", all.x = T)
}

if (! "location_id" %in% specific){
  presentation_df[,location_id := 1]
} else {
  presentation_df <- merge(presentation_df, locs[,c("location_id", "location_name")], by = "location_id", all.x = T)
}
if (! "year_id" %in% specific){
  presentation_df[,year_id := 2019]
}

presentation_df <- presentation_df %>%
  .[,age_count := age_group_id - 7] %>%
  .[age_group_id %in% c(30:32),age_count := age_group_id - 16] %>%
  .[age_group_id %in% c(235),age_count := 17]

fwrite(presentation_df, 'FILEPATH', row.names = F)     
