#----Set-up-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- commandArgs(trailingOnly = T)
  # if (length(arg)!=4) {
  #   arg <- c(2019, "41", 1000, 5) #toggle targeted run 
  # }
  
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  # arg <- c(2019, "41", 1000, 5) 
  
}

#----Functions and directories----------------------------------------------------------------------------------------------------------

library(data.table)
library(fst)
library(parallel)
library(magrittr)

loc_id <- arg[1]
year <- arg[2]
grid_version <- arg[3]
draws_required <- arg[4]

# # toggle for test run
# loc_id <- 197
# year <- 2019
# grid_version <- 48
# draws_required <- 1000

home_dir <- "FILEPATH"

ambient.grid.in <- file.path(home_dir,"FILEPATH",grid_version,"draws")
pred.cols <- paste0("pred_",1:draws_required)
draw.cols <- paste0("draw_",1:draws_required)

out.dir <- paste0(home_dir,"FILEPATH",grid_version)
dir.create(out.dir,recursive = T,showWarnings = F)

# Must change debug in script itself, since used for running line by line
debug <- F

# Output the settings into the log for later reference
warning(paste0("Location       = ", loc_id))
warning(paste0("Year           = ", year))
warning(paste0("Grid version   = ", grid_version))
warning(paste0("Draws req      = ", draws_required))


### LOAD PREPPED EXPOSURE DRAWS ###
# read in ambient grid

ambient <- read.fst(paste0(ambient.grid.in,"/",loc_id,"_",year,".fst")) %>% as.data.table
  
names(ambient) <- tolower(names(ambient))
setnames(ambient,pred.cols,draw.cols)
ambient <- ambient[,c("latitude","longitude","weight","pop",draw.cols),with=F]

warning("Ambient draws loaded")

# Drop any pixels with zero or missing populations #
ambient <- ambient[pop > 0 & is.na(pop)==F, ]

if (debug==T) ambientBkup <- copy(ambient)

warning("Zero pop gridcells dropped")

# multiply pop * weight to get the actual weight for PAF calculation
ambient[,pop_weight:=pop*weight]

### COLLAPSE POPULATION BY MEAN ANNUAL AMBIENT BINS ###
# To speed up computation time, we will collapse the gridcells into bins
# Our output will be a data.table with 1000 draws for each bin in the location, coupled with that bin's population

# We perform this collapse by draw (so that we don't run into problems due to too many rows)
collapse.draws <- function(draw) { 
  longAmbient <- copy(ambient)[,paste0("draw_",draw):=round(get(paste0("draw_",draw)),digits=2)] # rounds to 2 decimal places
  
  longAmbient <- longAmbient[, lapply(.SD, sum), by = .(get(paste0("draw_",draw))), .SDcols = "pop_weight"] # sums up pop per bin
  longAmbient[, draw := as.integer(draw)]
  
  return(longAmbient)
}

long <- do.call(rbind, mclapply(1:draws_required, collapse.draws))

setnames(long,"get","ambient")

warning("Reshape to long & collapse completed")

# Save the output #
write.csv(long, file = paste0(out.dir, "/", loc_id,"_",year, ".csv"), row.names = F)

warning("File exported -- done :)")



