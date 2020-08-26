rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "/home/j/"
  h <- "/homes/USERNAME/"
}

#################

source("FILEPATH")

arg <- commandArgs(trailingOnly = TRUE)
loc <- arg[1]



interp <- interpolate('rei_id', gbd_id = 95, source = 'exposure',
                    location_id = loc, gbd_round_id = 6, decomp_step = 'step4')

print("writing")

write.csv(interp, file= paste0("FILEPATH"), row.names = FALSE)




