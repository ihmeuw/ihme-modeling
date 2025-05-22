#########################################
### Apply severity splits to ID model ###

rm(list=ls())

# Source shared functions -------------------------------------------------
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_age_metadata.R")
library(openxlsx)
library(readstata13)

# Load metadata -----------------------------------------------------------
gbd_release <- 16
mes_map <- data.table(me_id=9423:9427, severity=c("borderline", "mild", "moderate", "severe", "profound"))
age_metadata <- get_age_metadata(age_group_set_id = 24, release_id = gbd_release)
location_metadata <- get_location_metadata(location_set_id=35, release_id = gbd_release)[is_estimate==1,]
output_folder <- "/FILEPATH/"

# Submit splitting job below to run this code on the cluster --------------
submit <- F
if(submit){
  slots <- 2
  mem <- paste0(1024 * slots*2, "M")
  shell <-"/FILEPATH/execRscript.sh -i /FILEPATH/rstudio/latest.img"
  script <- "-s /FILEPATH/id_severity_apply.R"
  project <- '' 
  sge_output_dir <- '-o /FILEPATH/%x.o%j -e /FILEPATH/errors/%x.o%j '
      # select incomplete jobs #
  filelist <- list.files(path="/FILEPATH/", pattern = "csv", full.names = TRUE)
  
  locations_done <- as.numeric(gsub(".csv", "", gsub('/FILEPATH/id_split_draws_', "", filelist)))
  locations_left <- location_metadata$location_id[!(location_metadata$location_id %in% locations_done)]
  
  #for (location in location_metadata$location_id){
  for (location in locations_left){
    job_name<- paste0('-J loc_',location)
    
    #sys_sub<- paste0("sbatch ", job_name, " --mem=", mem, " -c ", slots, " -t 30 -A ", project, " -p long.q -C archive ", sge_output_dir)
    sys_sub<- paste0("sbatch ", job_name, " --mem=", mem, " -c ", slots, " -t 30 -A ", project, " -p long.q ", sge_output_dir)
  
    system(paste(sys_sub, shell, script, location))
    print(paste(sys_sub, shell, script, location))
  }
  print("All causes submitted")
}

args<-commandArgs(trailingOnly = TRUE)
location <- args[1]


# Pull ID prevalence for location -----------------------------------------

prevalence <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 2420, source = "epi", sex_id = c(1, 2), age_group_id = age_metadata[most_detailed == 1, age_group_id],
                          location_id = location, measure_id = c(5), release_id = gbd_release)

prevalence <- melt.data.table(prevalence, id.vars = names(prevalence)[!(names(prevalence) %like% "draw")], value.name="id_prev", variable.name="draw")

# Load in meta-analysis results for severity ------------------------------
splits <- as.data.table(read.xlsx("/FILEPATH/sev_props.xlsx"))


# Split prevalence by severity --------------------------------------------

prevalence <- merge(prevalence, splits, by = c("draw"), allow.cartesian=TRUE)
prevalence[, sev_prev := id_prev * severity_prop]
prevalence[healthstate == 'id_bord', modelable_entity_id := 9423]
prevalence[healthstate == 'id_mild', modelable_entity_id := 9424]
prevalence[healthstate == 'id_mod', modelable_entity_id := 9425]
prevalence[healthstate == 'id_sev', modelable_entity_id := 9426]
prevalence[healthstate == 'id_prof', modelable_entity_id := 9427]
prevalence[, `:=` (model_version_id = NULL, id_prev = NULL, healthstate = NULL, severity_prop = NULL)]

prevalence <- dcast(prevalence, age_group_id + location_id + measure_id + sex_id + year_id + metric_id +modelable_entity_id ~ draw, value.var="sev_prev")

for(me in mes_map$me_id){
  write.csv(prevalence[modelable_entity_id == me,], paste0(output_folder, me, "/id_split_draws_", location, ".csv"), row.names=F)
}

