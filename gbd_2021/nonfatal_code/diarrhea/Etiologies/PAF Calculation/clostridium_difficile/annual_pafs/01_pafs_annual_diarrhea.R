##############################
### ANNUAL PAFs
##############################

args = commandArgs(trailingOnly=TRUE)
print(paste(args, collapse = ", "))
gbd_round <- args[1]
ds <- args[2]
param_map_path <- args[3]

# Set up libraries and source functions
library(plyr)
library(data.table)
source("/PATH/get_population.R")
source("/PATH/get_age_metadata.R")
source("/PATH/interpolate.R")

# get ID from array job
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
parameters <- fread(param_map_path)
location <- parameters[task_id, location_id]
m <- parameters[task_id, me_id]
print(paste("location", location))
print(paste("meid", m))

# Set variables
sex_id <- c(1,2)
year_start <- 1990
year_end <- 2022
cause_id <- 302
out_dir <- "/PATH"
eti_meta <- as.data.table(read.csv("/PATH/eti_rr_me_ids.csv"))

r <- eti_meta[modelable_entity_id == m]$rei_id
name <- as.character(eti_meta[modelable_entity_id == m]$name_colloquial)
eti_dir <- as.character(eti_meta[modelable_entity_id == m]$rei)
for(measure in c("yll", "yld")){
  print(paste("Interpolating years for", name, measure))
  ifelse(measure == "yll", measure_id <- 4, measure_id <- 3)
  df <- interpolate(gbd_id_type="rei_id"
                    , gbd_id=r, source='paf'
                    , measure_id=measure_id
                    , location_id=location
                    , sex_id=sex_id
                    , gbd_round_id=gbd_round
                    , decomp_step=ds
                    , reporting_year_start=year_start
                    , reporting_year_end=year_end
  )
  
  df$cause_id <- cause_id
  df$rei_id <- r
  df$modelable_entity_id <- m
  
  write.csv(df, paste0(out_dir, eti_dir, "/paf_annual_",measure,"_",location,".csv"), row.names=F)
  
  print(paste("Saved for", name, measure,"!"))
}