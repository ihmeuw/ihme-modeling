# January 27, 2020

library(data.table)
library(fitdistrplus)
library(nloptr)
library(magrittr)
library(GoFKernel, lib = "FILEPATH")
library(dfoptim, lib = "FILEPATH")
source("FILEPATH/pdf_families.R")
source(file.path("FILEPATH/convergence_helper_functions.R"))
library(stats)
library(lubridate)


# ----- Set up parameters




debug = 0

if(debug == 1){
  
  
  initial_condition = 2
  me_type = "WAZ"
  optim_type = "sbplx"
  strategy = "m2"
  delete_iteration_csvs = FALSE
  maxeval = 3
  data_filepath = "FILEPATH"
  
  
  option = "thresholds_5"
  
  # Enter the actual threshold
  low_threshold <<- -3
  medium_threshold <<- -2
  high_threshold <<- -1
  
  #Enter the actual tail
  low_tail <<- .03
  middle_tail <<- .13
  high_tail <<- .25
  
  
  # if method is 4 or 5, enter weights for the weighted sum
  # low_weight refers to the furthest negative threshold/smallest proportion, high_weight refers to the closest to 0 threshold/highest proportion
  low_weight <<- (9/13)
  medium_weight <<- (3/13)
  high_weight <<- (1/13)
  
} else{
  
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  
  param_map <- fread(param_map_filepath)
  
  initial_condition = param_map[task_id, initial_condition]
  me_type = param_map[task_id, me_type]
  optim_type = param_map[task_id, optim_type]
  strategy = param_map[task_id, strategy]
  delete_iteration_csvs = param_map[task_id, delete_iteration_csvs]
  maxeval = param_map[task_id, maxeval]
  data_filepath = param_map[task_id, data_filepath]
  option = param_map[task_id, option]
  low_threshold = param_map[task_id, low_threshold]
  medium_threshold = param_map[task_id, medium_threshold]
  high_threshold = param_map[task_id, high_threshold]
  low_tail = param_map[task_id, low_tail]
  middle_tail = param_map[task_id, middle_tail]
  high_tail = param_map[task_id, high_tail]
  low_weight = param_map[task_id, low_weight]
  medium_weight = param_map[task_id, medium_weight]
  high_weight = param_map[task_id, high_weight]
  launch.date = param_map[task_id, launch.date]
  max.time = param_map[task_id, max.time]
  
}

# 
# # --------------------------------------------------------------------------------------------------------------------
# #Variables that have to be input on the parallel scripts!!!
# 
# # Enter the actual threshold
# low_threshold <<- -3
# medium_threshold <<- -2
# high_threshold <<- -1
# 
# # Enter the actual tails
# low_tail <<- .03
# middle_tail <<- .13
# high_tail <<- .25
# 
# # if method is 4 or 5, enter weights for the weighted sum
# # low_weight refers to the furthest negative threshold/smallest proportion
# low_weight <<- (9/13)
# medium_weight <<- (3/13)
# high_weight <<- (1/13)
# 
# #launch date format "MM_DD_YYYY"
# launch.date <<- "XXXXXX"
# 
# 
# # --------------------------------------------------------------------------------------------------------------------


#getting the time that the entire optimization starts, converting to underscores because of csv's
start.time <<- gsub("-", "_", Sys.time())
max.time <<- max.time

# ----- Function being optimized

min_KS_m2 <- function(allData, weights, initial_condition, all_CDF_Data_Params) { 
  
  allweightfits <- lapply(unique(allData$nid_loc_yr_index), function(nid_loc_yr_i){
    
    Data = allData[nid_loc_yr_index == nid_loc_yr_i]$data
    
    print(nid_loc_yr_i)
    
  
    XMAX <<- max(Data, na.rm = T)
    
    cdf.index <- which(unique(allData$nid_loc_yr_index) == nid_loc_yr_i)
    CDF_Data_Params <- all_CDF_Data_Params[[cdf.index]]
    
    

    
    weights <- find_bad_fits(weights, distlist, CDF_Data_Params)    
    
    weights <- rescale_to_one(weights)  
    
    den <- create_pdf(distlist, weights, CDF_Data_Params)
    
    
    CDF_Data_Params <- integrate_pdf(den, CDF_Data_Params)  
    
    
    weightfit <- goodness_of_fit_calc(Data, allData, CDF_Data_Params, weights, option, low_threshold, medium_threshold, high_threshold, low_tail, middle_tail, high_tail, low_weight, medium_weight, high_weight)
    
    iteration_weights_ks_tracking(out_dir, optim_type, initial_condition, weightfit, nid_loc_yr_i)
    
    return(weightfit)
    
  })
  
  KSs <- sapply(allweightfits, function(weightfit) return(weightfit$ks) ) 
  
  sum_KSs <- combine_fit(KSs) 
  
  global_ks_tracking(allData, sum_KSs)
  
  print(sum_KSs)
  
  
  #converting that original start time back to the dashes for math purposes
  comp.start.time <- gsub("_", "-", start.time)
  
  #getting current time in optimization
  current.time <- Sys.time()
  
  #converting both to the final math format for dates
  comp.start.time <- ymd_hms(comp.start.time)
  current.time <- ymd_hms(current.time)
  
  runtime <- comp.start.time %--% current.time
  runtime <- as.numeric(as.duration(runtime) /dhours(1))
  
  if (runtime > max.time) {
    return()
  }
  
  return(sum_KSs)
  
} 



allData = readRDS(data_filepath)


offset = 0
if (me_type == "HAZ") {
  allData$data <- allData$data + offset
} else if(me_type == "WAZ") {
  allData$data <- allData$data + offset
} else if(me_type == "WHZ") {
  allData$data <- allData$data + offset
}

distlist <- c(classA, classM) 
distlist$invgamma <- NULL
distlist$llogis <- NULL
set.seed(initial_condition)
out_dir = file.path("FILEPATH")


write_zero_csv(out_dir, optim_type, initial_condition)
message(paste(initial_condition, me_type, optim_type, strategy, delete_iteration_csvs, out_dir))



# ----- Prepping optimization

weights <- select_initial_weights(distlist, initial_condition)

all_CDF_Data_Params <- lapply(unique(allData$nid_loc_yr_index), function(nid_loc_yr_i) {
  
  Data = allData[nid_loc_yr_index == nid_loc_yr_i]$data
  
  XMAX <<- max(Data, na.rm = T)
  
  all.cdfs <- create_CDF(Data, nid_loc_yr_i)
  
  
  return(all.cdfs)
  
})

#creates a 1 meta-list with # of component lists = to the number of unique nid loc years
#each of these sub-lists has 8 elements
#total list components = 8 * number of NIDs

# ---- Running Optimization

if(optim_type == "sbplx"){
  
  optim_results <- try(sbplx(x0 = weights, 
                             fn = min_KS_m2, 
                             allData = allData, 
                             initial_condition = initial_condition,
                             all_CDF_Data_Params = all_CDF_Data_Params,
                             lower=rep(0, length(weights)), 
                             upper = rep(1, length(weights)), 
                             control = list(maxeval=maxeval)),
                       silent = F)
  
} else if (optim_type == "nmkb"){
  
  optim_results <- try(nmkb(weights, 
                            min_KS_m2, 
                            allData = allData, 
                            initial_condition = initial_condition,
                            all_CDF_Data_Params = all_CDF_Data_Params,
                            lower=0, 
                            upper=1, 
                            control=list(maxfeval=maxeval)),
                       silent=F)
  
  
} else if (optim_type == "nlminb"){ 
  optim_results <- try(nlminb(start = weights,
                              objective = min_KS_m2,
                              allData = allData,
                              initial_condition = initial_condition,
                              all_CDF_Data_Params = all_CDF_Data_Params,
                              control = list(iter.max = maxeval),
                              lower = rep(0, length(weights)),
                              upper = rep(1, length(weights))))
  
  
} else {
  
  message("wrong optim type specified:", optim_type)
  
}


print(optim_results)


save_max_it_data_m2 <- function(out_dir, nid_loc_yr_i_m2, optim_type, initial_condition, option, launch.date) {
  
  files <- list.files(paste0(out_dir, "/", nid_loc_yr_i_m2, "/", optim_type, "/", initial_condition))
  max_iteration = max(as.integer(unlist(lapply(files, function(f) { gsub(x = f, pattern = ".csv", replacement = "") } ))))
  
  max.weights <- fread(paste0(out_dir, "/", nid_loc_yr_i_m2, "/", optim_type, "/", initial_condition, "/", max_iteration, ".csv"))
  
  dir.create(paste0(out_dir, "/", nid_loc_yr_i_m2, "/", optim_type, "/", initial_condition, "_records" ), recursive = T)
  
  max.weights[, nid_loc_yr_i := nid_loc_yr_i_m2]
  max.weights[, initial_condition := initial_condition]
  max.weights[, option := option]
  
  write.csv(max.weights, paste0(out_dir, "/", nid_loc_yr_i_m2, "/", optim_type, "/", initial_condition, "_records/", option,"_weights_", launch.date, ".csv"))
  
}

for (nid_loc_yr_i_m2 in unique(allData$nid_loc_yr_index)) {
  
  save_max_it_data_m2(out_dir, nid_loc_yr_i_m2, optim_type, initial_condition, option, launch.date)
  
}

# ----- Clean up

for(nid_loc_yr_i in unique(allData$nid_loc_yr_index)) {
  
  aggregate_iterations(out_dir, nid_loc_yr_i, optim_type, initial_condition, option, launch.date)
  
}

if(delete_iteration_csvs == T){
  
  for(nid_loc_yr_i in unique(allData$nid_loc_yr_index)) {
    unlink(x = paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition), recursive = T) 
  }
  
}

#creating the log so we can see how long things run
end.time <- gsub("-", "_", Sys.time())

runtimes.log <- data.table(me.type = me_type, strategy = "m2", opt = option, initial.condition = initial_condition, start_time = start.time, end_time = end.time)
write.csv(runtimes.log, paste0("FILEPATH"), row.names = F)





