### Purpose: Run custom WaSH prep code
#####################################################################

## source function
source("FILEPATH/prep.R")
"%ni%" <- Negate("%in%")
"%unlike%" <- Negate("%like%")

args <- commandArgs(trailingOnly = T)
print(args)

#Toggle these as needed
limited_use <- T
current_year <- 2022
batch1 <- "Batch1_022024"
batch2 <- "Batch2_032024"
batch3 <- "Batch3_subnatfix_042024"
batch4<-"Batch4_052024"
cw_batch<- "Batch4_052024/cw_file_check"
batch<-batch4
prep_all<- T # toggle T if you want to prep all your file in your in_dir regardless of their status in source_log, F if you want to adhere to you're source_log

## T if extracting from limited use directory, F otherwise
if (limited_use == T) {
  in_dir <- paste0("FILEPATH", current_year, "/FILEPATH/",batch)
} else {
  in_dir <- paste0("FILEPATH", current_year,"/FILEPATH/",batch)
}

## run prep code
files <- list.files(in_dir)
source_log <- fread(paste0("FILEPATH/b_prepped_log_2022.csv"))
ifelse(prep_all == T,
       to_prep <- files,
       to_prep <- setdiff(files, source_log[reprep == 0, file_name]))

if (length(to_prep) > 0) {
  x <- sapply(to_prep, prep_and_save, limited_use = limited_use, current_year = current_year)
} else {
  message("Nothing to prep!")
}
