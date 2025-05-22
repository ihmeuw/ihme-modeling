# this script is a quick fix for the northern norway location of Troms og Finnmark
# v5 upper latitude limit is just below the southern tip of this region, so we don't have exposures for it
# the short fix is to copy over the exposures from the neighboring subnatinoal of Norway, which is Nordland (id 4926)

library(data.table)
library(fst)

version <- 'v5_4'

# need to create exposure fst files, results csv files, as well as the collaps PAF prep files

# exposure fst file prep
exp_files <- paste0('FILEPATH/gridded/',version,'/draws/')

# load in each Nordland file (filename starts with 4926) and save a new file with a different filename
nordland_exp_files <- list.files(exp_files, pattern = '^4926', full.names = T)

# only use files from 2005-2022? could replace all but after checking the values, they are so similar it doesn't matter
nordland_exp_files <- nordland_exp_files[8:25]

for (i in 1:length(nordland_exp_files)) {
  nordland <- read_fst(nordland_exp_files[i])
  nordland$location_id <- 60137
  nordland$location_name <- 'Troms og Finnmark'
  year <- nordland$year[1]
  new_filename <- paste0('60137_',year,'.fst')
  # new_filename <- paste0('60137_',year,'.csv') 
  # browser()
  write_fst(nordland, paste0(exp_files, new_filename))
}

# results csv file prep
exp_files <- paste0('FILEPATH/exp/results/', version, '/')

nordland_exp_files <- list.files(exp_files, pattern = '^4926', full.names = T)

for (i in 1:length(nordland_exp_files)) {
  nordland <- read.csv(nordland_exp_files[i])
  nordland$location_id <- 60137
  nordland$location_name <- 'Troms og Finnmark'
  year <- nordland$year[1]
  new_filename <- paste0('60137_',year,'.csv') 
  # browser()
  write.csv(nordland, paste0(exp_files, new_filename))
}

# collapse PAF prep files

nordland_collapse_files <- list.files(paste0('FILEPATH/exp/collapse/',version,'/'), pattern = '^4926', full.names = T)

# no id information in the file, just save it with the new name

years <- c(1990, 1995, 1998:2022)

for (i in 1:length(years)) {
  year <- years[i]
  nordland <- read.csv(nordland_collapse_files[i])
  new_filename <- paste0('60137_',year,'.csv') 
  # browser()
  write.csv(nordland, paste0('FILEPATH/exp/collapse/', version, '/', new_filename))
}
