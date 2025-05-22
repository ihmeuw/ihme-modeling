
##' *************************************************************************************
##' Title: 03_apply_outliers.R
##' Purpose: Pulls in agesex split bundle version (or crosswalked dataset for bundle 212), 
##'          applies outliers to data according to outlier map. 
##' *************************************************************************************

args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]
release_id <- args[2]
outlier_save_dir <- args[3]


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
library('stringr')

## Source all shared functions
invisible(lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  }))


'%ni%' <- Negate('%in%')
measure_name <- 'prevalence'

print(paste0('Bundle ', bun_id))

#### Read in full age-sex split data (and crossalked data for 212)
if (bun_id == 212) {
  bun_data <- read.xlsx(paste0("FILEPATH")) %>% as.data.table() ##### THIS IS USING NEW CROSSWALK PACKAGE
  bun_data[, clinical_data_type := ""] #adding an arbitrary blank column for merging purposes
} else{
   bun_data <-  read.xlsx(paste0("FILEPATH")) %>% as.data.table
}

print(paste0("Age sex split bundle has ", length(unique(bun_data$nid)), " unique NIDs and ", nrow(bun_data), " rows"))

bun_data$age_group_id <- NULL
#### Pulling in Locations
locs <- get_location_metadata(35, release_id = release_id)
locs <- locs[, .(location_id, ihme_loc_id)]
bun_data$ihme_loc_id <- NULL
bun_data <- merge(bun_data, locs, by = 'location_id')
bun_data <- bun_data[, country_name := substr(ihme_loc_id, start = 1, stop = 3)]


#### Subset only to prevalence data for outliering
mort_data <- bun_data[measure != 'prevalence']
bun_data <- bun_data[measure == 'prevalence']

input_bun_data <- copy(bun_data)
print(paste0('Bundle has ', nrow(input_bun_data[is_outlier == 1]), ' outliers'))

#### Apply outliers
outlier_map <- read.xlsx('FILEPATH') %>% as.data.table()
outlier_map <- outlier_map[bundle_id == bun_id]
outlier_map <- outlier_map[,c('brief_reasoning', 'bundle_id', 'date_added', 'notes') := NULL]


outlier_combos <- unique(outlier_map[,.(merge_cols,threshold_cols)])
#### Iterate through each merge, and outlier rows based on the designated criteria --
#### Each merge outliers rows where the set_to_outlier column = 1 but first checks to see if there is a threshold value for doing so
#### For example, there may be rows from a given NID, location, and clinical data type, where only rows ABOVE a certain age should be outliered.
for (i in 1:nrow(outlier_combos)){
  
  id_cols <- unlist(str_split(outlier_combos[i]$merge_cols, ", "))
  print(paste0('Merging on ', id_cols))
  value_cols <- unlist(str_split(outlier_combos[i]$threshold_cols, ", "))
  
  if (value_cols !='none'){
    columns <- c(id_cols, value_cols, 'set_to_outlier')
  }else{
    columns <- c(id_cols, 'set_to_outlier')
  }  
  
  merge_outlier_map <- outlier_map[merge_cols == outlier_combos[i]$merge_cols & threshold_cols == outlier_combos[i]$threshold_cols,..columns]
  dt_1 <- merge(input_bun_data, merge_outlier_map, by = id_cols, all.x = TRUE)
  
  print(paste0("Outliering based on ", value_cols," values"))
  for (var in value_cols){
    print(var)
    if (var == 'age_floor'){
      dt_1 <- dt_1[age_start >= age_floor & set_to_outlier == 1, is_outlier :=1]
    }
    if (var == 'mean_floor'){
      dt_1 <- dt_1[mean >= mean_floor & set_to_outlier == 1, is_outlier :=1]
    }
    if (var == 'year_outlier'){
      dt_1 <- dt_1[year_start == year_outlier & set_to_outlier == 1, is_outlier :=1]
    }
    if (var == 'mean_zero'){
      dt_1 <- dt_1[mean == 0 & set_to_outlier == 1, is_outlier :=1] ## Different than if mean_floor is >= 0!! This is JUST zeros
    }
    if (var == 'none'){
      dt_1 <- dt_1[set_to_outlier == 1, is_outlier := 1]
    }
  }
  
  dt_1[, c('age_floor', 'mean_floor', 'year_outlier', 'mean_zero', 'set_to_outlier', 'threshold_cols') := NULL]
  input_bun_data <- dt_1
  print(paste0('Bundle has ', nrow(input_bun_data[is_outlier == 1]), ' outliers'))
}


#### Bundle 210 only: Remove outliers in clinical data over age 50-60 in US
if (bun_id == 210){
  gen_age_floor <- input_bun_data[clinical_data_type %in% c('claims', 'inpatient') & 
                                    age_start >= 50 & age_start <60 & 
                                    substr(ihme_loc_id, start = 1, stop = 3) == 'USA', is_outlier := 0]
  input_bun_data <- gen_age_floor
}

##### Bring back mortality data
mortality_data <- mort_data 
mortality_data_f <- mortality_data[measure != 'prevalence' & sex == 'Both']
mortality_data_f <- mortality_data_f[, sex := 'Female']
mortality_data_m <- mortality_data[measure != 'prevalence' & sex == 'Both']
mortality_data_m <- mortality_data_m[, sex := 'Male']
mortality_data_specific <- mortality_data[measure != 'prevalence' & sex != 'Both']
final_data <- rbind(input_bun_data, mortality_data_f, mortality_data_m, mortality_data_specific, fill = TRUE)


#### Apply mortality data outliers
if (bun_id == 206){
  final_data <- final_data[!(measure == 'mtexcess' & age_start >28 & nid == 110283)]
}

final_data$country_name <- NULL
print(paste0("Bundle with outliers applied and mortality data duplicated for male/female has ", length(unique(final_data$nid)), " unique NIDs and ", nrow(final_data), "rows"))

table(final_data$is_outlier)

write.xlsx(final_data,
           file = paste0(outlier_save_dir, bun_id, "_outliered.xlsx"),
           sheetName = "extraction", row.names = FALSE)

print(paste0('Wrote out outliered bundle data to ', paste0(outlier_save_dir, bun_id, '_outliered.xlsx')))


