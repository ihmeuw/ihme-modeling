rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "/home/j/"
  h <- "/homes/"
}

library("stringr")

cause_ids <- c(614, 615, 616, 618)
sexes <- c(1,2)

savedir <- paste0(h, "/chikeda/repos/hemog/hemog_splits_cod/tmp/")
outdir <- paste0(j, "temp/chikeda/2018/180611_HemogCC/check_filenames/")

for (sex in sexes){
  for (cause in cause_ids){
    datadir <- paste0(savedir, cause, "/", sex, "/")
    filenames <- dir(datadir, pattern = ".csv")
    data <- as.data.frame(filenames)
    
    print(paste0(cause, "~~~", sex, "~~~", nrow(data)))
    
    # test <- str_split_fixed(data$filenames, "_", 3)
    
    # write.csv(test, paste0(savedir, cause,"_", sex, ".csv"), row.names = FALSE)
  }
}