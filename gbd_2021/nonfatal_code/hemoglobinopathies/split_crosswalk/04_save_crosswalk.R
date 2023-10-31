##' *************************************************************************************
##' Title: 04_save_crosswalk.R
##' *************************************************************************************

args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]
release_id <- args[2]
save_dir <- args[3]
cw_description <- args[4]


os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')

## Source all shared functions
invisible(lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  }))


'%ni%' <- Negate('%in%')

version_map <- read.csv(paste0("FILEPATH"))
bun_version <- version_map$bundle_version_id

print(bun_version)

final_data <- read.xlsx(paste0(save_dir, 'FILEPATH')) %>% as.data.table()

print(paste0("Bundle with outliers applied has ", length(unique(final_data$nid)), " unique NIDs and ", nrow(final_data), " rows"))


final_data$sex_id <- NULL
final_data$step2_location_year <- NULL
final_data <- final_data[group_review == 1 | is.na(group_review) | group_review == '']

final_data$crosswalk_parent_seq <- final_data$origin_seq
final_data$seq <- NA

final_data[is.na(is_outlier), is_outlier := 0]

print(paste0('Dropped ',nrow(final_data[upper >= 1]), ' rows where upper is > 1'))
final_data2 <- final_data[!(upper >= 1)]
final_data2 <- final_data2[lower <0, lower := 0]

print(paste0("Bundle cleaned for crosswalk validations has ", length(unique(final_data2$nid)), " unique NIDs and ", nrow(final_data2), "rows"))


write.xlsx(final_data2,
           file = paste0(save_dir, "FILEPATH"),
           row.names = FALSE, sheetName = "extraction")

result <- save_crosswalk_version(bundle_version_id = bun_version, 
                                 data_filepath = paste0(save_dir, "FILEPATH"),
                                 description = cw_description) 

print(result)
print('Done')



