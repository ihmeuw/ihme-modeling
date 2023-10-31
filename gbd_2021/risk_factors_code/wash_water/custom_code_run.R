### Author: [AUTHOR]
### Date: [DATE]
### Purpose: Run custom WaSH prep code
#####################################################################

## source function
source("FILEPATH/custom_code.R")

"%ni%" <- Negate("%in%")
"%unlike%" <- Negate("%like%")

args <- commandArgs(trailingOnly = T)
print(args)

limited_use <- args[1] %>% as.logical

## T if extracting from limited use directory, F otherwise
if (limited_use == T) {
  in.dir <- "FILEPATH"
} else {
  in.dir <- "FILEPATH"
}

## run prep code
files <- list.files(in.dir)
source_log <- fread("FILEPATH")
to_prep <- setdiff(files, source_log[reprep == 0, file_name])

if (length(to_prep) > 0) {
  x <- sapply(to_prep, prep_and_save, limited_use = limited_use)
} else {
  message("Nothing to prep!")
}
