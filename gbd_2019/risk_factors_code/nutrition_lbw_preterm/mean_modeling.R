rm(list=ls())

os <- .Platform$OS.type

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/get_location_metadata.R"  )
source("FILEPATH/get_demographics.R"  )
source("FILEPATH/get_draws.R"  )
source("FILEPATH/find_non_draw_cols.R")

args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- "FILEPATH"

task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
param_map <- fread(param_map_filepath)

type <- param_map[task_id, type]
cut_off <- param_map[task_id, cut_off]
prev_me_id <- param_map[task_id, prev_me_id]
mean_me_id <- param_map[task_id, mean_me_id]
loc <- param_map[task_id, location_id]

lbwsg_ids <- fread("FILEPATH")

load_all_joint_data <- function(type){
  
  files <- list.files("FILEPATH", full.names = T)
  
  data <- lapply(files, function(filename){
    
    country_data <- readRDS(filename)
    return(country_data)
    
  }) %>% rbindlist(fill = T, use.names = T)
  
  data <- data[!(is.na(ga) | is.na(bw))]
  
  setnames(data, type, "data")
  
  data[, data := as.integer(data)]
  
  return(data)
  
}

collapse_by_mean_and_threshold <- function(microdata, type, cut_off){
  
  microdata[data < cut_off,  threshold := 1][data >= cut_off, threshold := 0] 

  collapse_mean <- microdata[, .(mean = mean(data)), by = list(location_id, year_id, sex_id)]
  collapse_prev <- microdata[, .(prev = sum(threshold) / .N), by = list(location_id, year_id, sex_id)]
  
  collapse <- merge(collapse_mean, collapse_prev, by = c("location_id", "year_id", "sex_id"))
  
  return(collapse)
  
}

remove_implausible <- function(collapse, type){
  
  if(type == "bw"){
    
    collapse <- collapse[mean < 5000 & mean > 1000 & prev > 0.005 & prev < 0.5]
    
  } else{
    
    collapse <- collapse[mean > 28 & mean < 44 & prev > 0.005 & prev < 0.5 ]
    
  }
  
  return(collapse)
  
}

load_bw_microdata <- function(){
  
  bw_data <- fread("FILEPATH")
  
  locs <- get_location_metadata(location_set_id = 9)
  
  bw_data <- merge(bw_data, locs[, list(ihme_loc_id, location_id)])
  
  setnames(bw_data, "birth_weight", "data")
  
  setnames(bw_data, "year_end", "year_id")
  
  bw_data[, data := data * 1000]
  
  bw_data <- bw_data[data > 0, ]
  
  return(bw_data)
  
}


plot_collapse <- function(){
  
  locs <- get_location_metadata(location_set_id = 9)
  
  ggdata <- merge(ggdata, locs[, list(location_id, super_region_name, location_name)], by = "location_id")

  ggplot(ggdata) + geom_point(aes(x = prev, y = mean, color = super_region_name), alpha = 0.5) #+ 
    facet_wrap(~sex_id) + 
    geom_line(data = preds.dt, aes(x, y))  

}



collapse <- fread(paste0(FILEPATH))

model <- lm(formula = mean ~ log(prev), data = collapse)

estimation_years <- get_demographics("epi")$year_id

to_fit <- get_draws(source = "epi",
                    gbd_id_type = "modelable_entity_id",
                    gbd_id = prev_me_id,
                    location_id = loc,
                    age_group_id = list(2),
                    year_id = estimation_years,
                    gbd_round_id = 6,
                    decomp = "step4",
                    status = "best",
                    num_workers = 4)

to_fit[, age_group_id := 164]
to_fit[, measure_id := 19]

to_fit <- to_fit[, -c("metric_id")]

non_draw_cols <- find_non_draw_cols(to_fit)

to_fit_long <- melt(to_fit, id.vars = non_draw_cols, variable.factor = F) 

setnames(to_fit_long, "value", "prev")

predicted = data.table(predicted = predict(model, to_fit_long))

predicted <- cbind(to_fit_long, predicted)

predicted_draws <- dcast(predicted, formula = paste0(paste(non_draw_cols, collapse = " + "), " ~ variable"), value.var = "predicted")

draw_dir <- paste0("FILEPATH")

if(dir.exists(draw_dir) == F){
  dir.create(draw_dir)
}

predicted_draws <- predicted_draws[, -c("modelable_entity_id")]

predicted_draws_agid2 <- copy(predicted_draws)
predicted_draws_agid2[, age_group_id := 2]
predicted_draws <- rbind(predicted_draws, predicted_draws_agid2)

write.csv(predicted_draws, paste0(draw_dir, loc, ".csv"), row.names = F, na = "")
  