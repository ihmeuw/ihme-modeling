#-----------------------------------------------------------------
# Purpose: calculate unmediated RR for current and former smoking
# Date: Sep 10, 2024
#-----------------------------------------------------------------

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"

# load library
library(data.table)
library(dplyr)

# number of levels
level_100 <- T
if(level_100){
  rr_dir <- FILEPATH
} else {
  rr_dir <- FILEPATH
}

rr_former_dir <- FILEPATH


# load the mediation factor draws for PAD
mf_matrix <- fread(FILEPATH)[rei_id==99 & cause_id==502]
mf_matrix <- melt(mf_matrix, id.vars = c('rei_id', 'med_id', 'cause_id'), 
                  measure.vars = paste0("draw_", 0:999))

setnames(mf_matrix, "value", "mf")
setnames(mf_matrix, "variable", "draw")

mf_matrix[, draw := 0:999]

# load the RR draws of PAD
rr_draws <- fread(paste0(FILEPATH, "peripheral_artery_disease.csv"))
rr_former_draws <- fread(FILEPATH)

# merge rr draws with mf draws
rr_draws <- merge(rr_draws, mf_matrix[,.(draw, mf)], by="draw", all.x = T)
rr_former_draws <- merge(rr_former_draws, mf_matrix[,.(draw, mf)], by="draw", all.x = T)

# calculate the unmediated RR draws for PAD
rr_draws[, rr := (rr-1)*(1-mf)+1]
rr_draws[, mf := NULL]

rr_former_draws[, rr := (rr-1)*(1-mf)+1]
rr_former_draws[, mf := NULL]

# save the unmediated RR draws
write.csv(rr_draws, paste0(FILEPATH, "peripheral_artery_disease_um.csv"), row.names = F)
write.csv(rr_former_draws, paste0(FILEPATH, "pad_former_um.csv"), row.names = F)

