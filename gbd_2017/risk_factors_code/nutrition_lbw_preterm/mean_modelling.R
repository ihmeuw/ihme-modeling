rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j <- FILEPATH
  h <- FILEPATH
  my_libs <- paste0(h, "/local_packages")
} else {
  j<- FILEPATH
  h<- FILEPATH
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(ggplot2)

args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- FILEPATH
 
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
param_map <- fread(param_map_filepath)

type <- param_map[task_id, type]
cut_off <- param_map[task_id, cut_off]
prev_me_id <- param_map[task_id, prev_me_id]
mean_me_id <- param_map[task_id, mean_me_id]


load_all_joint_data <- function(type){
  
  files <- list.files(FILEPATH, full.names = T)
  
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
  
  if(type == "ga"){   
    microdata[data < cut_off,  threshold := 1][data >= cut_off, threshold := 0] } 
  if(type == "bw"){   
    microdata[data <= cut_off, threshold := 1][data >  cut_off, threshold := 0] }

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
  
  bw_data <- fread(FILEPATH)
  
  source(FILEPATH)
  
  locs <- get_location_metadata(location_set_id = 9)
  
  bw_data <- merge(bw_data, locs[, list(ihme_loc_id, location_id)])
  
  setnames(bw_data, "birth_weight", "data")
  
  setnames(bw_data, "year_end", "year_id")
  
  bw_data[, data := data * 1000]
  
  bw_data <- bw_data[data > 0, ]
  
  return(bw_data)
  
}
 

plot_collapse <- function(){
  
  source(FILEPATH)
  
  locs <- get_location_metadata(location_set_id = 9)
  
  ggdata <- merge(ggdata, locs[, list(location_id, super_region_name, location_name)], by = "location_id")

  ggplot(ggdata) + geom_point(aes(x = prev, y = mean, color = super_region_name), alpha = 0.5) #+ 
    facet_wrap(~sex_id) + 
    geom_line(data = preds.dt, aes(x, y))  
 
}


 

dt <- load_all_joint_data(type)
 
if(type == "bw"){
  
  bw_microdata <- load_bw_microdata()
  
  dt <- rbindlist(list(dt[, .(data, location_id, year_id, sex_id)], 
                       bw_microdata[, .(data, location_id, year_id, sex_id)]), 
                       fill = T, use.names = T)
  
}

collapse <- collapse_by_mean_and_threshold(microdata = dt, type, cut_off)

collapse <- remove_implausible(collapse = collapse, type)

write.csv(collapse, FILEPATH, row.names = F, na = "")
 
model <- lm(formula = mean ~ log(prev), data = collapse)

source("FILEPATH/get_demographics.R"  )

estimation_locs <- get_demographics("epi")$location_id
estimation_years <- get_demographics("epi")$year_id

source("FILEPATH/get_draws.R"  )

to_fit <- get_draws(source = "epi", gbd_id_type = "modelable_entity_id", gbd_id = prev_me_id, 
                    location_id = estimation_locs, 
                    year_id = estimation_years,
                    gbd_round_id = 5,
                    status = "best")
 

model <- lm(formula = mean ~ log(prev), data = collapse)

source("FILEPATH/get_demographics.R"  )

estimation_locs <- get_demographics("epi")$location_id
estimation_years <- get_demographics("epi")$year_id

source("FILEPATH/get_draws.R")

to_fit <- get_draws(source = "epi", gbd_id_type = "modelable_entity_id", 
                    gbd_id = prev_me_id, 
                    location_id = estimation_locs,
                    age_group_id = 2,
                    year_id = estimation_years,
                    gbd_round_id = 5,
                    status = "best", 
                    num_workers = 4)


write.csv(to_fit, FILEPATH, row.names = F, na = "")

source("FILEPATH/find_non_draw_cols.R")

non_draw_cols <- find_non_draw_cols(to_fit)

to_fit_long <- melt(to_fit, id.vars = non_draw_cols, variable.factor = F) 

setnames(to_fit_long, "value", "prev")

predicted = data.table(predicted = predict(model, to_fit_long))

predicted <- cbind(to_fit_long, predicted)

predicted_draws <- dcast(predicted, formula = paste0(paste(non_draw_cols, collapse = " + "), " ~ variable"), value.var = "predicted")

draw_dir <- FILEPATH

predicted_draws <- predicted_draws[, -c("modelable_entity_id")]

for(loc in unique(predicted_draws$location_id)){
  
  write.csv(predicted_draws[location_id == loc], paste0(draw_dir, loc, ".csv"), row.names = F, na = "")
  
}

if(length(list.files(draw_dir)) == length(estimation_locs)){
  
  source("FILEPATH/save_results_epi.R"  )
  
  save_results_epi(input_dir = draw_dir, 
                   input_file_pattern = "{location_id}.csv", 
                   modelable_entity_id = mean_me_id, 
                   description = "predicted_mean_from_stgpr", 
                   gbd_round_id = 5, 
                   measure_id = 5, 
                   mark_best = T)
  
} else{
  
  stop("Wrong number of locations needed for save results!!! ")
  
}
