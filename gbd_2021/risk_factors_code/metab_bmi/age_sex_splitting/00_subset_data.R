################################################################################
## DESCRIPTION: Subsets rows of data for age-pattern model in STGPR
## INPUTS: XLSX of cleaned data ##
## OUTPUTS: CSVs of gold-standard, GBD age-binned data ##
## AUTHOR:
## DATE:
################################################################################

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"


# Base filepaths
share_dir <- 'FILEPATH' # only accessible from the cluster
data_dir <- "FILEPATH"
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, 'FILEPATH'))
library(openxlsx, lib = paste0(h, "FILEPATH"))

# Version of age-split data
version <- 2

# Read in data
b_4790 <- read.xlsx(paste0(data_dir, "FILEPATH")) %>% as.data.table(.)
b_4793 <- read.xlsx(paste0(data_dir, "FILEPATH")) %>% as.data.table(.)

# Select measured rows only
ap_4790 <- b_4790[diagnostic == "measured"]
ap_4793 <- b_4793[diagnostic == "measured"]

# Select only rows that have 5 year age bins with age_start as multiple of 5 OR is for 2-4 year olDs
ap_4790 <- ap_4790[age_start == age_end - 5 & age_start %% 5 == 0 & age_start != 0| age_start == 2 & age_end == 5]
ap_4793 <- ap_4793[age_start == age_end - 5 & age_start %% 5 == 0 & age_start != 0| age_start == 2 & age_end == 5]

# Select rows with sample size greater than or equal to 20
ap_4790$sample_size <- round(ap_4790$sample_size)
ap_4793$sample_size <- round(ap_4793$sample_size)

ap_4790 <- ap_4790[sample_size >= 20]
ap_4793 <- ap_4793[sample_size >= 20]

# Only keep male and female data and create sex_id
ap_4790 <- ap_4790[sex %in% c("Male", "Female")][,sex_id := ifelse(sex == "Male", 1, 2)]
ap_4793 <- ap_4793[sex %in% c("Male", "Female")][,sex_id := ifelse(sex == "Male", 1, 2)]

# For rows with age start greater than or equal to 80, set the age start and age end to age group 21 inputs
ap_4790[age_start >= 80, `:=` (age_start = 80, age_end = 125, age_group_id = 21)]
ap_4793[age_start >= 80, `:=` (age_start = 80, age_end = 125, age_group_id = 21)]

# Use rule of 3 for sample proportion = 0 to get the upper confidence interval and then back calculate the variance
ap_4790[val == 0, `:=` (upper = -log(0.05)/sample_size, variance = (upper/qt(0.975, df = sample_size - 1))^2)]
ap_4793[val == 0, `:=` (upper = -log(0.05)/sample_size, variance = (upper/qt(0.975, df = sample_size - 1))^2)]

# Use rule of 3 for sample proportion = 1 to get the lower confidence interval and then back calculate the variance
ap_4793[val == 1, `:=` (lower = 1+log(0.05)/sample_size, variance = ((lower-1)/qt(0.025, df = sample_size - 1))^2)]

# Check that all lower and upper bounds are logical
if(nrow(ap_4790[lower > val | upper < val | lower > upper]) != 0){
  stop(message("Some lower and upper bounds are illogical."))
}

if(nrow(ap_4793[lower > val | upper < val | lower > upper]) != 0){
  stop(message("Some lower and upper bounds are illogical."))
}

# Save as CSVs
if(!dir.exists(paste0(data_dir, "FILEPATH", version))){
  dir.create(paste0(data_dir, "FILEPATH", version), mode = 000) # Should grant read and write access
  save_dir <- paste0(data_dir, "FILEPATH", version, "/")
} else {
  save_dir <- paste0(data_dir, "FILEPATH", version, "/")
}

fwrite(ap_4790, paste0(save_dir, "FILEPATH"))
fwrite(ap_4793, paste0(save_dir, "FILEPATH"))





####################################################################
## Outliering some observations
####################################################################

ap_4790 <- fread(paste0(save_dir, "ap_4790.csv"))
ap_4793 <- fread(paste0(save_dir, "ap_4793.csv"))

# For overweight: outliering the observations for males age-start = 60 less than same study age-start = 65
# 216 rows
ap_4790[,uniq_id := 1:.N]
ap_4790_problems <- ap_4790[age_start %in% c(60,65)&sex_id == 1]

ap_4790_65 <- ap_4790_problems[age_start %in% c(65)]
setnames(ap_4790_65, "val", "val.65")
ap_4790_60 <- ap_4790_problems[age_start %in% c(60)]
ap_4790_60 <- merge(ap_4790_60, ap_4790_65[,.(nid, location_id, year_id, sex_id, val.65)], by = c("nid", "location_id", "year_id", "sex_id"))
ap_4790_60[val < val.65, is_outlier := 1]
nrow(ap_4790_60[is_outlier == 1])
bad_ids_60 <- ap_4790_60[is_outlier == 1, uniq_id] # 216 (out of 490) rows

ap_4790[uniq_id %in% c(bad_ids_60), is_outlier := 1]
ap_4790[,uniq_id := NULL]

# For prop obese: outliering the observations for females age-start = 65 and year_id before 2000
# 95 rows
ap_4793[age_start == 65 & sex_id == 2 & year_id < 2000, is_outlier := 1]

# For prop obese: outliering the observations for males age-start = 45, 65

ap_4793[,uniq_id := 1:.N]
ap_4793_problems <- ap_4793[age_start %in% c(40,45,60,65)&sex_id == 1]

ap_4793_40 <- ap_4793_problems[age_start %in% c(40)]
setnames(ap_4793_40, "val", "val.40")
ap_4793_45 <- ap_4793_problems[age_start %in% c(45)]
ap_4793_45 <- merge(ap_4793_45, ap_4793_40[,.(nid, location_id, year_id, sex_id, val.40)], by = c("nid", "location_id", "year_id", "sex_id"))
ap_4793_45[val < val.40, is_outlier := 1]
nrow(ap_4793_45[is_outlier == 1])
bad_ids_45 <- ap_4793_45[is_outlier == 1, uniq_id] # 251 rows

ap_4793_65 <- ap_4793_problems[age_start %in% c(65)]
setnames(ap_4793_65, "val", "val.65")
ap_4793_60 <- ap_4793_problems[age_start %in% c(60)]
ap_4793_60 <- merge(ap_4793_60, ap_4793_65[,.(nid, location_id, year_id, sex_id, val.65)], by = c("nid", "location_id", "year_id", "sex_id"))
ap_4793_60[val < val.65, is_outlier := 1]
nrow(ap_4793_60[is_outlier == 1])
bad_ids_60 <- ap_4793_60[is_outlier == 1, uniq_id] # 172 rows

ap_4793[uniq_id %in% c(bad_ids_45, bad_ids_60), is_outlier := 1]
ap_4793[,uniq_id := NULL]
# 518 rows outliered

### Save them as new version
version = 3
if(!dir.exists(paste0(data_dir, "../age_pattern/data/v", version))){
  dir.create(paste0(data_dir, "../age_pattern/data/v", version)) # Should grant read and write access
  save_dir <- paste0(data_dir, "../age_pattern/data/v", version, "/")
} else {
  save_dir <- paste0(data_dir, "../age_pattern/data/v", version, "/")
}

fwrite(ap_4790, paste0(save_dir, "ap_4790.csv"))
fwrite(ap_4793, paste0(save_dir, "ap_4793.csv"))
