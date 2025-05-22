 ####s script to find out how many files are present in a given directory
 #### useful for vetting runs of custom code, to verify that data is square prior to upload

library("stringr")
#################


#####Checks for square data

me_ids <- c(2583, 2582, 2584, 2336, 2335, 3093, 3092, 3091)

for (me in me_ids){
indir <- paste0("ADDRESS", me, "/01_draws/")
filenames <- list.files(indir, pattern = ".csv")
filenames <- as.data.frame(filenames)
filenames <- str_split_fixed(filenames$filenames, "_", 4)

print(paste0("there are ~ ", nrow(filenames), " ~ files present in me == ", me))

}

#####Checking to see which locations are present and which are missing
me_id <- 2336
locs <- get_location_metadata(35)
locs <- locs[most_detailed == 1]


indir <- paste0("FILEPATH/01_draws/")
filenames <- list.files(indir, pattern = ".csv")
filenames <- as.data.frame(filenames)
filenames <- str_split_fixed(filenames$filenames, "_", 4)
filenames <- as.data.table(filenames)

print(paste0("there are ~ ", nrow(filenames), " ~ files present in me == ", me_id))

present <- filenames[filenames$V3 == 2017]

present_locs <- unique(present$V2)
print(present_locs)

missing <- locs[!locs$location_id %in% present_locs]

print(missing$location_id)

scan <- table(present$V2)

print(scan)




