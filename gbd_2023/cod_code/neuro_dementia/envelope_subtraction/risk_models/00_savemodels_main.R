##########################################################################
### Author: USERNAME
### Project: GBD Nonfatal Estimation
### Purpose: SAVE ENVELOPE MODELS
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(mortcore, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

functions_dir <- paste0(functions_dir, "FILEPATH")
model_dt <- data.table(cause = c("ds", "stroke", "tbi", "pd"),
                      date = c("2019_07_02", "2019_06_26", "2019_07_03", "2019_07_24"),
                      me_id = c(IDs),
                      dir = c("FILEPATH", "FILEPATH", "FILEPATH", "FILEPATH"))
envelope_dir <- paste0("FILEPATH")
code_dir <- paste0("FILEPATH")
save_dir <- "FILEPATH"
for (c in model_dt[, cause]){
  dir.create(paste0(save_dir, c, "/", date))
}
draws <- paste0("draw_", 0:999)

# SOURCE FUNCTIONS --------------------------------------------------------

functs <- c("get_location_metadata.R", "get_age_metadata.R", "get_demographics_template.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))

floor_zero <- function(x){
  x[x < 0] <- 0
  return(x)
}

# GET DEMOGRAPHICS --------------------------------------------------------

age_dt <- get_age_metadata(12, gbd_round_id=7)
age_dt <- age_dt[age_group_id >= 13]
age_dt[age_group_years_end == 125, age_group_years_end := 100]
age_dt[, age := (age_group_years_start + age_group_years_end)/2]
demographic_dt <- get_demographics_template(gbd_team = "epi", gbd_round_id = 7)
loc_dt <- get_location_metadata(location_set_id = 35, gbd_round_id=7)
loc_dt <- loc_dt[(is_estimate == 1 & most_detailed == 1)]

# GET ALL DRAWS -----------------------------------------------------------

get_model_preds <- function(c){
  dir <- paste0(envelope_dir, model_dt[cause == c, dir])
  files <- list.files(dir)
  file <- files[grepl(paste0(model_dt[cause == c, date], "$"), files)]
  dt <- fread(paste0(dir, file, "/model_draws.csv"))
  dt <- unique(dt) 
  setnames(dt, names(dt)[grepl("age", names(dt))], "age")
  dt <- merge(dt, age_dt[, .(age, age_group_id)], by = "age")
  dt[, (draws) := lapply(.SD, floor_zero), .SDcols = draws]
  dt[, (draws) := lapply(.SD, exp), .SDcols = draws]
  dt <- dt[, c("age_group_id", draws), with = F]
  dt[, merge := 1]
  dt <- merge(dt, demographic_dt[, .(sex_id = unique(sex_id), merge = 1)], by = "merge", allow.cartesian = T)
  dt <- merge(dt, demographic_dt[, .(year_id = unique(year_id), merge = 1)], by = "merge", allow.cartesian = T)
  dt[, `:=` (measure_id = 11, metric_id = 3, merge = NULL)]
  return(dt)
}

model_draws <- lapply(model_dt[, cause], get_model_preds)
names(model_draws) <- model_dt[, cause]

# SAVE .CSV's -------------------------------------------------------------

save_files <- function(loc, c){
  dt <- model_draws[[c]]
  dt[, location_id := loc]
  write.csv(dt, paste0("FILEPATH"))
}

parallel::mclapply(loc_dt[, unique(location_id)], function(x) save_files(loc = x, c = "ds"), mc.cores = 9)
parallel::mclapply(loc_dt[, unique(location_id)], function(x) save_files(loc = x, c = "pd"), mc.cores = 9)
parallel::mclapply(loc_dt[, unique(location_id)], function(x) save_files(loc = x, c = "stroke"), mc.cores = 9)
parallel::mclapply(loc_dt[, unique(location_id)], function(x) save_files(loc = x, c = "tbi"), mc.cores = 9)


# Check for missing locs, print names of missing locations and return datatable containing missing location_ids/names
check_missing_locs<-function(indir,filepattern){
  setwd(indir)
  
  # which locations should you have? 
  loc_dt <- get_location_metadata(location_set_id = 35, gbd_round_id=7)
  loc_dt <- loc_dt[(is_estimate == 1 & most_detailed == 1)]
  locs<-loc_dt$location_id
  
  # which locations do you have 
  filepattern<-gsub(pattern = "\\..*",replacement = "",x = filepattern) #remove file extension 
  index<-grep(x = strsplit(x=strsplit(x = filepattern,split = "\\{")[[1]],split="\\}"),pattern = "location")-1
  saved_locs<-strsplit(x = gsub(pattern="\\..*",replacement="",x=list.files()),split = "_")
  saved_locs<-sapply(saved_locs, `[[`, index)
  
  missing<-setdiff(locs,saved_locs)
  loc_dt<-get_location_metadata(22)
  missing_loc_dt<-loc_dt[location_id%in%missing][,.(location_id,location_name)]
  if(length(missing)>=1){
    print(paste("Missing the following location IDs:",paste(missing_loc_dt[,location_id],collapse=",")))
    return(missing_loc_dt)
  }else{
    print("Not missing any locations")
    return(FALSE)
  }
}


missing <- check_missing_locs(indir = paste0("FILEPATH"), 
                              filepattern = "{location_id}.csv")

# JOBS TO SAVE MODELS -----------------------------------------------------

params <- data.table(cause = model_dt[, cause])
params[, task_num := 1:.N]
map_path <- paste0(save_dir, "task_map.csv")
write.csv(params, map_path, row.names = F)

array_qsub(jobname = "save_risk_models",
           shell = "FILEPATH",
           code = paste0(code_dir, "01_savemodels_child.R"),
           pass = list(map_path, save_dir),
           proj = "proj_yld",
           num_tasks = nrow(params),
           cores = 10, mem = 50, log = T, submit = T)
