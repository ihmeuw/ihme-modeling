################################################################################################
## Description: Calculates prevalence of compensated cirrhosis (=total cirrhosis - decompensated cirrhosis), 
## for a given location, age, sex, year.
## Output:  Compensated cirrhosis draws
################################################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "/home/j/"
  h <- "~/"
  l <- "/ihme/limited_use/"
} else {
  j <- "J:/"
  h <- "H:/"
  l <- "L:/"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(gridExtra)
library(grid) 
date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)

#SOURCE FUNCTIONS 
shared_functions <- FILEPATH
functions <- c("get_draws.R")
for (func in functions) {
  source(paste0(shared_functions, func))
}

# GET ARGS ----------------------------------------------------------------
args<-commandArgs(trailingOnly = TRUE)
out_dir <- FILEPATH
map_path <- FILEPATH
gbd_round_id <- OBJECT
decomp_step <- OBJECT

locations <- fread(map_path)
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
loc_id <- locations[task_num == task_id, location]


## Set IDs ----
#prevalence and incidence
measures <- c(5, 6)

## Use shared functions ----
total_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                         gbd_id=OBJECT, 
                         source="epi", 
                         location_id=loc_id, 
                         measure_id=measures, 
                         gbd_round_id = gbd_round_id, 
                         decomp_step= decomp_step)

symp_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                        gbd_id=OBJECT, 
                        source="epi", 
                        location_id=loc_id, 
                        measure_id=measures, 
                        gbd_round_id = gbd_round_id, 
                        decomp_step= decomp_step)


## Drop columns 
total_draws[,grep("mod", colnames(total_draws)) := NULL]
symp_draws[,grep("mod", colnames(symp_draws)) := NULL]


## Calculate asymptomatic = total - symptomatic ----
asymp_draws <- copy(total_draws)

for (draw in draws) {
  asymp_draws[,draw := get(draw) - symp_draws[,get(draw)],with=F]
  asymp_draws[[draw]][asymp_draws[[draw]]<0]=0 # sets any negative draws to zero
}

outpath <- FILEPATH
if (!dir.exists(outpath)) dir.create(outpath)
write.csv(asymp_draws, paste0(outpath, loc_id, ".csv"), row.names=FALSE)
