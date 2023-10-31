# --------------

#' 
# --------------

Sys.umask(mode = 002)

os <- .Platform$OS.type

library(data.table)
library(magrittr)
library(lme4)
library(ggplot2)

source("/FILEPATH/get_location_metadata.R"  )

## Move to parallel script
## Getting normal QSub arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]

## Retrieving array task_id
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
param_map <- fread(param_map_filepath)

type <- param_map[task_id, type]
cut_off <- param_map[task_id, cut_off]

run_interactively = 0

if(run_interactively == 1){
  type = "ga"
  cut_off = 37
}

microdata_location = "FILEPATH"
imputed_microdata_location = "FILEPATH"
results_location = "FILEPATH"

#' Loads all joint microdata
#'  Sets either ga or bw to be named "data"
#'  @param type Type of data being modelled: either ga or bw
load_all_joint_data <- function(type){
  files <- list.files(microdata_location, full.names = T)
  data <- lapply(files, function(filename){
    country_data <- readRDS(filename)
    return(country_data)
  }) %>% rbindlist(fill = T, use.names = T)
  
  data <- data[!(is.na(ga) | is.na(bw))]
  setnames(data, type, "data")
  data[, data := as.integer(data)]
  return(data)
}

#' Collapses the data variable to a table of mean and prevalence under a specified cut-off
collapse_by_mean_and_threshold <- function(microdata, type, cut_off){
  
  microdata[data < cut_off,  threshold := 1][data >= cut_off, threshold := 0] 
  
  collapse_mean <- microdata[, .(mean = mean(data), variance = var(data), sample_size = .N), by = list(location_id, year_id, sex_id)]
  collapse_prev <- microdata[, .(prev = sum(threshold) / .N), by = list(location_id, year_id, sex_id)]
  
  collapse <- merge(collapse_mean, collapse_prev, by = c("location_id", "year_id", "sex_id"))
  return(collapse)
}

#' Removes implausible prevalence or mean
remove_implausible <- function(dt, type){
  if(type == "bw"){
    dt <- dt[mean < 5000 & mean > 1000 & prev > 0.005 & prev < 0.5]
  } else{
    dt <- dt[mean > 28 & mean < 44 & prev > 0.005 & prev < 0.5 ]
  }
  
  dt <- dt[sex_id %in% 1:2]
  
  return(dt)
}

#' Loads all birthweight microdata
load_bw_microdata <- function(){
  # need to use new gbd2019 imputed dataset in Decomp 4
  bw_data <- fread(imputed_microdata_location)
  locs <- get_location_metadata(location_set_id = 9)
  bw_data <- merge(bw_data, locs[, list(ihme_loc_id, location_id)])
  
  setnames(bw_data, "birth_weight", "data")
  setnames(bw_data, "year_end", "year_id")
  
  bw_data[, data := data * 1000]
  bw_data <- bw_data[data > 0, ]
  return(bw_data)
}

dt <- load_all_joint_data(type)

if(type == "bw"){
  bw_microdata <- load_bw_microdata()
  dt <- rbindlist(list(dt[, .(data, location_id, year_id, sex_id)],
                       bw_microdata[, .(data, location_id, year_id, sex_id)]),
                  fill = T, use.names = T)
}

dt <- collapse_by_mean_and_threshold(microdata = dt, type, cut_off)
dt <- remove_implausible(dt, type)
saveRDS(dt, paste0(results_location, type, "_input_data.rds"))

model <- lmer(formula = mean ~ log(prev) + (1|location_id), data = dt)

saveRDS(model, paste0(results_location, type, '_model.rds'))


# ----- Visualize predictions 

preds <- data.table(prev = seq(0.01, 0.5, by=0.01))
preds$mean <- predict(model, newdata = preds, re.form = NA)

pdf(paste0(results_location, type, "_predict_plot.pdf"))

ggplot() + 
  geom_point(data = dt, aes(prev, mean)) + xlim(0,0.5) + #+ ylim(2000,4000) + #+ 
  geom_line(data = preds, aes(prev, mean)) 

dev.off()