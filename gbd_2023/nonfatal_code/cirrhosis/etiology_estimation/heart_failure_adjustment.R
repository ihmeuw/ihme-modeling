################################################################################################
## Description: Calculates decompensated cirrhosis (=decompensated cirrhosis - decompensated cirrhosis with heart failure), 
## for a given location, age, sex, year.
## Output:  Decompensated cirrhosis without heart failure draws
################################################################################################

rm(list=ls())

pacman::p_load(data.table, openxlsx, ggplot2)
library(gridExtra)
library(grid) 
date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)

#SOURCE FUNCTIONS 
shared_functions <- "FILEPATH"
functions <- c("get_draws.R")
for (func in functions) {
  source(paste0(shared_functions, func))
}

# GET ARGS ----------------------------------------------------------------
args<-commandArgs(trailingOnly = TRUE)
out_dir <- args[1]
map_path <-args[2]
rel_id <- args[3]
version <- args[4]
# loc_id <- args[5]

locations <- fread(map_path)
task_id <- ifelse(is.na(as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))), 1, as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID")))
loc_id <- locations[task_num == task_id, location]


## Set IDs ----
#prevalence
measures <- c(5)

## Use shared functions ----
total_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                         gbd_id=OBJECT, 
                         source="epi", 
                         location_id=loc_id, 
                         measure_id=5, 
                         release_id = 16)

hf_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                      gbd_id=OBJECT, 
                      source="epi", 
                      location_id=loc_id, 
                      measure_id=5, 
                       release_id = 16)

## Drop columns that aren't needed ----
total_draws[,grep("mod", colnames(total_draws)) := NULL]
hf_draws[,grep("mod", colnames(hf_draws)) := NULL]


## Calculate without hf = decomp total - hf with decomp ----
new_draws <- copy(total_draws)

for (draw in draws) {
  new_draws[,draw := get(draw) - hf_draws[,get(draw)],with=F]
  new_draws[[draw]][new_draws[[draw]]<0]=0 # sets any negative draws to zero
}

outpath <- paste0(out_dir, "/", version, "/")
if (!dir.exists(outpath)) dir.create(outpath)
write.csv(new_draws, paste0(outpath, loc_id, ".csv"), row.names=FALSE)