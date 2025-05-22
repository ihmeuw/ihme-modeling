# load packages
require(data.table)
require(magrittr)
require(openxlsx)

user <- Sys.info()["user"]
code_dir <- paste0("FILEPATH",user,"/FILEPATH")

# source functions
source(paste0(code_dir, "/general_func_lib.R"))
invisible(sapply(list.files("FILEPATH/", full.names = T), source)) 
seq_track_path <- "FILEPATH/dummy_seq_remission_tracker.csv"
dummy_seq_tracker <- as.data.table(read.csv(seq_track_path))


# set objects
args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  args <- pass_args
}
out_dir <- file.path(args[1])
output_version <- as.character(args[2])
acause <- c(as.character(args[3]))
denom_bundle_ver <- as.numeric(args[4])
cross_walk_id <- as.numeric(args[5])
release_id <- as.numeric(args[6])
bun_id <- as.numeric(args[7])
xwalk_path <- as.character(args[8])

process_cv_ref_map_path <- paste0("FILEPATH/process_cv_reference_map.csv")

date <- gsub("-", "_", Sys.Date())

#-------------------------------------------------------------------------------------------------------------

# ---BODY-----------------------------------------------------------------------------------------------------
# Create list of locations that was parallelized over
location_dt <- get_location_metadata(location_set_id = 9, release_id = release_id)
location_dt <- location_dt[most_detailed == 1, list(location_id, location_name, location_type)]
locations <- location_dt[, location_id]

# check number of files in ouput directory
file_count <- list.files(out_dir, pattern = "\\.csv$") %>% length()
print(paste(timestamp(), "file count =", file_count))

# wait until number of files in output directory is equal to expected number of output files
while (file_count < length(locations)) {
  Sys.sleep(60)
  file_count <- list.files(out_dir, pattern = "\\.csv$") %>% length()
  print(paste(timestamp(), "file count =", file_count))
}

# once all files are present, append them and output .csv
if (file_count == length(locations)) {
  file_list <- list.files(out_dir, pattern = "\\.csv$")
  remission <- rbindlist(lapply(paste0(out_dir, "/", file_list), fread))
  
  # merge on location_name from the location_dt
  remission <- remission[age_start > 1]
  extraction <- merge(remission, location_dt[, -c("location_type"), with = F], by = "location_id", all.x = TRUE)
  xwalk_df <- get_crosswalk_version(cross_walk_id)
  
  # append to crosswalk
  data_to_add <- rbind(extraction, xwalk_df, fill = TRUE)
  
  # clear the seq column for all the data
  data_to_add$seq <- NA
  
  # save the xlsx of data
  dir.create(xwalk_path,recursive = TRUE)
  filepath <- paste0(xwalk_path, "/",output_version, "_with_remission_xwalk.xlsx")
  openxlsx::write.xlsx(data_to_add,filepath, sheetName = 'extraction', row.names = FALSE)
  
  # upload for xwalk
  # add to tracking file variables in save xwalk function
  # save file to head of scratch file structure 
  saved <- save_crosswalk_version(
    bundle_version_id = denom_bundle_ver,
    data_filepath = filepath,
    description = paste0(output_version, " with remission")
  )
  new_xwalk <- saved[,crosswalk_version_id]
  dummy_seq_tracker$crosswalk_id_w_ratios[dummy_seq_tracker$bundle_id == bun_id] <-  new_xwalk
  dummy_seq_tracker$date_xwalk_w_remmission_created[dummy_seq_tracker$bundle_id == bun_id] <-  date
  
  write.csv(dummy_seq_tracker, file = seq_track_path, row.names = FALSE)
  
  bvids<-fread(process_cv_ref_map_path)
  bvids$cvid_final[bvids$bundle_id==bun_id] <- new_xwalk
  fwrite(bvids, file = process_cv_ref_map_path, append = FALSE)
  
  print(paste0("done!: new xwalk : ", new_xwalk))
}
#-------------------------------------------------------------------------------------------------------------