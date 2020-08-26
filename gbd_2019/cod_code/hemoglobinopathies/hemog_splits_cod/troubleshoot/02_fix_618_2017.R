rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "/home/j/"
  h <- "/homes/chikeda/"
}

library("data.table")

locations <- read.csv(paste0(j, "temp/chikeda/2018/180110_Hemog/loc_metadata_35.csv"), header = TRUE, sep = ",")
sexes <- c(1,2)

datadir <- paste0(h, "repos/hemog/hemog_splits_cod/tmp/618/")

for (locs in locations$location_id[1]){
  for (sex in sexes[1]){
    indir <- paste0(datadir, sex, "/")
    print(paste0(indir, locs, "_2016_", sex, ".csv"))
    print(paste0(indir, locs, "_2017_", sex, ".csv"))
    main <- fread(paste0(indir, locs, "_2016_", sex, ".csv"))
    write.csv(main, paste0(indir, locs, "_2017_", sex, ".csv"), row.names = FALSE)
  }
}


