rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- "USERNAME"
}

library('openxlsx')
library("stringr")
library("tidyverse")
#################

source(paste0("FILEPATH"))





test_version <- get_crosswalk_version(2486)

add_data <- read.csv('FILEPATH')
test_cols <- colnames(test_version)

missing <- c(!test_cols %in% colnames(add_data))


print(setdiff(test_cols, colnames(add_data)))
print(setdiff(colnames(add_data), test_cols))
print(setdiff(colnames(bun_206), colnames(add_data)))
print(setdiff(colnames(test_bun_step2), colnames(test_version)))
print(setdiff(colnames(all_data), colnames(bun_206)))


print(colnames(add_data))

## Steps to align columns(add_data columns -> test_version columns)
# bundle_id -> bundle_id_
colnames(add_data)[which(names(add_data) == "bundle_id")] <- "bundle_id_"

# location_ascii_name -> location_name
colnames(add_data)[which(names(add_data) == "location_ascii_name")] <- "location_name"

# note_SR -> note_sr
colnames(add_data)[which(names(add_data) == "note_SR")] <- "note_sr"

# drop data_sheet_filepath, file_path, age_group_id, year_id, sex_id, row_num, parent_id
add_data <- add_data %>%
  select(-data_sheet_filepath, -file_path, -age_group_id, -year_id, -sex_id, -parent_id, -row_num)


# add bundle_name column, modelable_entity_name
add_data <- add_data %>%
  mutate(modelable_entity_name = "Beta-thalassemia major parent", bundle_name = "")


# add original location, variance
add_data <- add_data %>%
  mutate(original_location = '', seq = NA)


# add cv columns to add_data
add_data <- add_data %>%
  mutate(cv_hospital = 0, cv_marketscan_all_2000 = 0, cv_marketscan_inp_2000 = 0,
         cv_marketscan_all_2010 = 0, cv_marketscan = 0, cv_marketscan_inp_2010 = 0,
         cv_marketscan_all_2012 = 0, cv_marketscan_inp_2012 = 0)



# add mean, lower, upper, standard_error
add_data <- add_data %>%
  mutate(mean = cases / sample_size, lower = NA, upper = NA)


# add step2_location_year
add_data <- add_data %>%
  mutate(step2_location_year = "Added VR data", clinical_data_type = "")


# add seq, crosswalk_parent_seq, origin_seq, parent_seq, origin_id
add_data <- add_data %>%
  mutate(seq = NA, crosswalk_parent_seq = NA, origin_seq = NA, parent_seq = NA, origin_id = NA)

add_data <- add_data %>%
  mutate(source_type = "Vital registration - national")

add_data <- add_data %>%
  mutate(variance = NA)

all_data <- rbind(test_version, add_data)

iter_bun_version <- rbind(bun_206, add_data)

#206 Outliers
all_data$is_outlier[all_data$nid == 285961] <- 1
all_data$is_outlier[all_data$age_start >= 80] <- 1

#209 outliers
all_data$is_outlier[all_data$nid == 96479 & all_data$sex == "Male"] <- 1
all_data$is_outlier[all_data$nid == 227966] <- 1
all_data$is_outlier[all_data$nid == 281461] <- 1



#211 outliers
all_data$is_outlier[all_data$nid == 281461] <- 1

libya <- all_data[all_data$location_name == "Libya"]
all_data$is_outlier[all_data$nid == 110023] <- 1


all_data$upper[all_data$upper >= 1] <- 0.9999
all_data$standard_error[all_data$standard_error >= 1] <- 0.9999
all_data$mean[is.na(all_data$mean)] <- 0
all_data$sample_size[all_data$sample_size == 0] <- 0


all_data$crosswalk_parent_seq <- all_data$seq

#all_data$seq <- NA

#seq_column <- all_data$seq

bun_209 <- bun_211 %>%
  select(seq)

all_data <- all_data[!all_data$nid == 346543]
all_data <- all_data[!all_data$nid == 348123]
all_data <- all_data[!all_data$nid == 348145]

all_data <- all_data[-c(56836:56890)]


write.xlsx(bun_209, 'FILEPATH', sheetName = 'extraction')

bun_209 <- get_bundle_data(209, 'step2')

write.xlsx(bun_209, 'FILEPATH', sheetName = 'extraction')

result <- upload_bundle_data(206, 'iterative', 'FILEPATH')


all_split2$crosswalk_parent_seq <- all_split2$parent_seq

print(setdiff(test_cols, colnames(bun_206)))
print(setdiff(colnames(bun_206), test_cols))
