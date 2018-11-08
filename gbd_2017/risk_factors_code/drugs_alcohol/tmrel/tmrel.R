#############################################
# Estimate TMREL
#############################################

#Grab packages
  library(data.table)
  library(plyr)
  library(dplyr)
  library(parallel)
  
  setwd("FILEPATH")
  source("get_draws.R")
  
  #Set options for TMREL calculation
  sex <- c(1,2)
  directory <- "FILEPATH"
  
  setwd(directory)
  
  files <- list.files(directory)
  
  causes <- as.numeric(unique(regmatches(files, regexpr("\\d{3}", files))))
  
  id_field <- rep("cause_id", length(causes))
  
  #Go get Daly draws from GBD 2016, reshape long, then by draw create weight factors as share of daly rates
  daly_draws <- data.table(get_draws(gbd_id_field=id_field, gbd_id=causes, source="Dalynator", location_ids=1, sex_ids=sex, 
                                     age_group_ids=27, year_ids=2016, measure_ids=2, metric_id=3, num_workers=5, 
                                     gbd_round_id=4, status='best')) %>%
    .[, c("measure_id", "metric_id") := NULL]
  
  daly_draws <- melt(daly_draws, c("location_id", "year_id", "age_group_id", "sex_id", "cause_id"), 
                     value.name ="dalys", variable.name="draw")
  
  daly_draws[, total_dalys := sum(.SD$dalys), by=c("draw")]
  daly_draws[, weight_factor := dalys/total_dalys, by=c("draw")]
  
  #Clean dataframe to match relative risks
  daly_draws <- daly_draws[, .(draw, cause_id, weight_factor, sex_id)]
  daly_draws[, draw:=as.numeric(gsub("draw_", "", draw))]
  
  grab_and_format <- function(filepath){
    
    df <- fread(filepath)
    df <- df[, .(exposure, draw, rr)]
    
    c <- regmatches(filepath, regexpr("\\d{3}", filepath))
    s <- as.numeric(gsub("^rr_\\d{3}|.csv|_", "", filepath))
    
    df <- df[, `:=`(sex_id = s, cause_id = c)]
    
    if (is.na(s)){
      
      df_male <- copy(df)
      df_male <- df_male[, sex_id := 1]
      
      df_female <- copy(df)
      df_female <- df_female[, sex_id := 2]
      
      df <- rbind(df_male, df_female)
    }
    
    return(df)
  }
  
  rr <- rbindlist(lapply(files, grab_and_format))
  
  #Expand long by draw, exposure, and cause. Merge with daly weights and calculate average rr for each exposure and draw.
  rr <- data.table(join(rr, daly_draws, by=c("draw", "cause_id", "sex_id")))
  
  rr <- rr[cause_id %in% causes,]
  rr[, weighted_rr := rr*weight_factor]
  rr[, all_cause_rr := sum(.SD$weighted_rr), by=c("draw", "exposure")]
  
  rr[, min_all_cause_rr := min(.SD$all_cause_rr), by="draw"]
  rr[, tmrel := .SD[all_cause_rr == min_all_cause_rr, exposure], by="draw"]
  
  all_cause_rr <- unique(rr[, .(exposure, draw, all_cause_rr)]) %>%
    .[, `:=`(mean_all_cause_rr = mean(.SD$all_cause_rr),
             lower_all_cause_rr = quantile(.SD$all_cause_rr, 0.025),
             upper_all_cause_rr = quantile(.SD$all_cause_rr, 0.975)),
      by = "exposure"] %>%
    .[, .(exposure, mean_all_cause_rr, lower_all_cause_rr, upper_all_cause_rr)] %>%
    unique
  
  tmrel_df <- unique(rr[, .(draw, tmrel, min_all_cause_rr)])
  write.csv(all_cause_rr, "FILEPATH/tmrel_summary.csv", row.names = F)
  