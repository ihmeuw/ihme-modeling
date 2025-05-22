# January 27, 2020

library(data.table)
library(fitdistrplus)
library(nloptr)
library(magrittr)
library(GoFKernel, lib = "FILEPATH")
library(dfoptim, lib = "FILEPATH")
source("FILEPATH")
source("FILEPATH")
library(stats)
library(lubridate)

# ----- Set up parameters



 
debug = 0

if(debug == 1){
  

  initial_condition = 2
  me_type = "HAZ"
  optim_type = "sbplx"
  strategy = "m2"
  delete_iteration_csvs = FALSE
  maxeval = 3
  data_filepath = "FILEPATH"
  

  option = "thresholds_5"
  
  low_threshold <<- -3
  medium_threshold <<- -2
  high_threshold <<- -1
  
  low_tail <<- .03
  middle_tail <<- .13
  high_tail <<- .25
  
  low_weight <<- (9/13)
  medium_weight <<- (3/13)
  high_weight <<- (1/13)
  
} else{
  
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  task_id <- ifelse(Sys.getenv('SLURM_ARRAY_TASK_ID') != '', as.integer(Sys.getenv('SLURM_ARRAY_TASK_ID')), NA)
  
  param_map <- fread(param_map_filepath)
  
  initial_condition = param_map[task_id, initial_condition]
  me_type = param_map[task_id, me_type]
  strategy = param_map[task_id, strategy]
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
  

  optim_type = "sbplx" # either sbplx, nmkb, or nlminb
  delete_iteration_csvs = FALSE
  
}



start.time <<- gsub("-", "_", Sys.time())
max.time <<- max.time

cli::cli_progress_step("Function being optimized...", msg_done = "Optimization function defined.")
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
  
  comp.start.time <- gsub("_", "-", start.time)
  
  current.time <- Sys.time()
  
  comp.start.time <- ymd_hms(comp.start.time)
  current.time <- ymd_hms(current.time)
  
  runtime <- comp.start.time %--% current.time
  runtime <- as.numeric(as.duration(runtime) /dhours(1))
  
  if (runtime > max.time) {
    return()
  }
  
  return(sum_KSs)
  
} 


cli::cli_progress_step("Reading microdata...", msg_done = "Microdata read.")
allData = readRDS(data_filepath)
allData = setDT(allData)

offset = 10
if (me_type == "HAZ") {
  allData$data <- allData$data + offset
} else if(me_type == "WAZ") {
  allData$data <- allData$data + offset
} else if(me_type == "WHZ") {
  allData$data <- allData$data + offset
} else if (me_type == "envir_lead_bone") {
  allData <- allData[data != 0]
  allData$data <- log(allData$data)
  allData$data <- ((allData$data-(mean(allData$data)))/sd(allData$data))
  allData$data <- allData$data + offset
  
}

distlist <- c(classA, classM) 

if(me_type %in% c("bw", "ga")){
  
  distlist$invgamma <- NULL
  distlist$llogis <- NULL
  
  
}



cli::cli_progress_step("Creating output dir...", msg_done = "Output dir created.")
set.seed(initial_condition)
out_dir = file.path("FILEPATH", Sys.Date(), me_type, strategy, option)


dir.create(out_dir, recursive = T, showWarnings = F)



write_zero_csv(out_dir, optim_type, initial_condition)
message(paste(initial_condition, me_type, optim_type, strategy, delete_iteration_csvs, out_dir))


allData <- allData[age_year < 5]
allData <- allData[, study.index := 1:.N, by = nid_loc_yr_index]
allData <- allData[, .SD[.N >= 2], by = nid_loc_yr_index]

no.of.loc.years <- length(unique(allData$nid_loc_yr_index))
length(unique(allData$nid_index))



if(option %like% "_1"){
  
  max.time = (2 * no.of.loc.years)/60
  
}

if(option %like% "_2"){
  
  max.time = (1.6 * no.of.loc.years)/60
  
}

if(option %like% "_3"){
  
  max.time = (1.8 * no.of.loc.years)/60
  
}

if(option %like% "_4"){
  
  max.time = (1.3 * no.of.loc.years)/60
  
}

if(option %like% "_5"){
  
  max.time = (1 * no.of.loc.years)/60
  
}





# ----- Prepping optimization
cli::cli_progress_step("Prepping optimization...", msg_done = "Optimization prepped.")
weights <- select_initial_weights(distlist, initial_condition)

all_CDF_Data_Params <- lapply(unique(allData$nid_loc_yr_index), function(nid_loc_yr_i) {
  
  Data = allData[nid_loc_yr_index == nid_loc_yr_i]$data
  
  XMAX <<- max(Data, na.rm = T)
  
  all.cdfs <- create_CDF(Data, nid_loc_yr_i)
  
  if (any(is.na(all.cdfs))) {
    warning(paste(nid_loc_yr_i, "has missing"))
    return(NULL)  
  }
  
  return(all.cdfs)
  
})


# ---- Running Optimization
cli::cli_progress_step("Running optimization...", msg_done = "Optimization ran.")
if(optim_type == "sbplx"){
  
  optim_results <- try(sbplx(x0 = weights, 
                             fn = min_KS_m2, 
                             allData = allData, 
                             initial_condition = initial_condition,
                             all_CDF_Data_Params = all_CDF_Data_Params,
                             lower = rep(0, length(weights)), 
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


cli::cli_progress_step("Saving max...", msg_done = "Max saved.")
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
cli::cli_progress_step("Aggregating iterations...", msg_done = "Iterations aggregated.")
for(nid_loc_yr_i in unique(allData$nid_loc_yr_index)) {
  
  aggregate_iterations(out_dir, nid_loc_yr_i, optim_type, initial_condition, option, launch.date)
  
}


cli::cli_progress_step("Deleting iterations...", msg_done = "Iterations deleted.")
if(delete_iteration_csvs == T){
  
  for(nid_loc_yr_i in unique(allData$nid_loc_yr_index)) {
    unlink(x = paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition), recursive = T) 
  }
  
}

#creating the log so we can see how long things run
end.time <- gsub("-", "_", Sys.time())

runtimes.log <- data.table(me.type = me_type, strategy = "m2", opt = option, initial.condition = initial_condition, start_time = start.time, end_time = end.time)
write.csv(runtimes.log, paste0("FILEPATH", optim_type, "/", launch.date, "_", me_type, "_runtime_records/", option, "_ic_", initial_condition, ".csv"), row.names = F)

cli::cli_progress_done()



