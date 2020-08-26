## set directories
if (Sys.info()[1] == 'Windows') {
  library(mortdb)
  library(readr)
  library(data.table)
  h <- "FILEPATH"
} else {
  library(data.table)
  library(readr)
  library(RMySQL)
  library(mortdb, lib = "FILEPATH")
  library(mortcore, lib= "FILEPATH")
  h <- Sys.getenv("HOME")
}


args <- commandArgs(trailingOnly = T)
new_run_id <- as.numeric(args[1])
mark_best <- as.character(args[2])

master_folder <- "FILEPATH"
input_folder <- "FILEPATH"
output_folder <- "FILEPATH"

under_five_path <- paste0(output_folder, "/5q0_upload.csv")
under_five <- fread(paste0(output_folder, "/5q0_data.csv"))

under_five_2017_srs <- pull_srs_2017()

under_five_nonsrs<-under_five[(source=="srs" & year_id == 2017) | (source!="srs"),]


under_five<- rbindlist(list(under_five_nonsrs,under_five_2017_srs), use.names = T, fill=T)


under_five$adjustment <- 0
under_five$reference <- 0
under_five <- under_five[!is.na(nid),]

write_csv(under_five, under_five_path)
