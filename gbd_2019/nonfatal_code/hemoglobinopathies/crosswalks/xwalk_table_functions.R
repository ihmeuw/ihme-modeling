
######################################################
### Suite of functions for use in do_xwalk_table.R ###
######################################################

## ******************************************************************************

# read in me-to-bundle map, pull bundle names and merge
map_bundle_name <- function(map_path){
  map <- fread(map_path)
  
  # Get bundle names
  dbQuery <- function(host_name, query_string) {
    con <- dbConnect(dbDriver("MySQL"), #RMySQL::MySQL(),
                     username="USERNAME",
                     password="PASSWORD",
                     host=paste0("ADDRESS"))
    results <- dbGetQuery(con, query_string)
    dbDisconnect(con)
    return(data.table(results))
  }
  
  query_string <- "SELECT bundle_id, bundle_name FROM bundle.bundle"
  bn_map <- dbQuery("epi", query_string)
  
  map <- merge(map, bn_map, by = 'bundle_id', all.x = TRUE)
  
  return(map)
}

## ******************************************************************************

#subset data into separate sets by type of data (measure_id)
subset_by_measure <- function(measure_i, bundle) {
  bundle_name <- bundle[measure == measure_i]
  
  measure_map <- read.csv('FILEPATH')
  bundle_name <- merge(bundle_name, measure_map, by = 'measure')
}

## ******************************************************************************

#identify all unique combinations of covariates
data_combos <- function(measure_i, data, studlab_path, all_cv){
  data_temp <- data[[measure_i]]
  bun_id <- data_temp[1,bundle_id]
  meas_id <- data_temp[1,measure_id]
  # read in covariates used for that me
  me_path <- paste0("FILEPATH")
  cvs <- fread(paste0(me_path))
  cvs <- cvs[measure_id == meas_id & is.na(country_covariate_id)]
  cvs$study_covariate <- paste0("cv_", cvs$study_covariate)
  cvs$beta <- paste0(cvs$mean_effect, " (", cvs$lower_effect, " - ", cvs$upper_effect, ")")
  cvs <- cvs %>% select(study_covariate, beta) %>% rename(variable = study_covariate)
  
  # data_temp[, cv_sex := 1]
  # data_temp[sex %in% c("Male", "Female"), cv_sex := 0]
  
  if(all_cv == 1){
    cv_list <- grep("cv", colnames(data_temp), value = TRUE)
    
    
  } else{
    cv_list <- unique(cvs$variable)
    cv_list <- cv_list[!cv_list %in% c("cv_NA", 'cv_sex')]
    
  }
  
  keeps <- c('nid', 'age_start', 'age_end', 'location_id', 'year_start', 'year_end', 'sex', 'measure_id', 'measure',cv_list)
  
  data_temp <- data_temp %>%  select(keeps)
  
  cv_index <- grep("cv", colnames(data_temp))
  
  data_temp <- as.data.frame(data_temp)
  data_temp[,cv_index][is.na(data_temp[, cv_index])] <- 0
  
  data_temp <- as.data.table(data_temp)
  if (length(cv_list) == 0) {
    data_temp[, combo := sex]
    return(data_temp)
    
  } else {
    data_temp[, combo := do.call(paste0, .SD), .SDcols = c(cv_list, 'sex')] 
    return(data_temp)
  }
}

## ******************************************************************************

is_ref <- function(measure_i, data){
  ### determines if the cv combo is the reference definition
  ### Used in both all_cvs, and used_cvs
  data_temp <- data[[measure_i]]
  all_cvs <- grep("cv", colnames(data_temp), value = TRUE)
  slim_data <- data_temp %>% select(c(all_cvs, "combo"))
  slim_data <- unique(slim_data)
  
  cv_index <- grep("cv", colnames(slim_data))
  
  slim_data$is_reference <- 1
  
  #any combo with a 1 in it is not the reference defn
  ref_index <- grep('1',slim_data[,combo])
  slim_data[ref_index, is_reference := 0]
  
  slim_data <- slim_data %>% select(combo, is_reference)
  
  return(slim_data)
}

## ******************************************************************************

label_ref <- function(measure_i, data, ref_map) {
  data_temp <- data[[measure_i]]
  data_temp <- merge(data_temp, ref_map[[measure_i]], by = "combo")
  slim_data <- data_temp %>% select('nid', 'age_start', 'age_end', 'location_id', 'year_start', 'year_end', 'sex', 'combo', 'is_reference') 
}

## ******************************************************************************

unique_counts <- function(measure_i, data, ref_map){
  data_temp <- data[[measure_i]]
  
  counts <- data_temp %>% count(combo)
  # counts <- merge(counts, ref_map, by = 'combo', all.y = TRUE)
}

## ******************************************************************************

agesex_counts <- function(measure_i, data){
  
  source(paste0("FILEPATH"))
  
  data_temp <- data[[measure_i]]
  combo_list <- unique(data_temp$combo)
  
  summary <- data.table(combo = '', age_and_sex_specific_n = '', not_age_and_sex_specific_n = '')
  
  for (i in combo_list) {
    combo_data <- data_temp[combo == i]
    combo_counts <- divide_data(combo_data)
    
    summary_i <- data.table(combo = i, age_and_sex_specific_n = combo_counts[[1]], not_age_and_sex_specific_n = combo_counts[[2]])
    
    summary <- rbind(summary, summary_i)
  }
  
  summary <- summary[2:nrow(summary),]
  return(summary)
}

## ******************************************************************************
# reshape

pivot_table <- function(measure_i, data, counts, agesex, ref_map, studlab_path, bun_id){
  # measure_i <- 1
  # data <- combo_table_by_measure_used
  # counts <- counts_used
  # agesex <- agesex_counts_used
  # ref_map <- ref_map_by_measure_used
  # bun_id <- 92
  
  data_temp <- data[[measure_i]]
  meas_id <- data_temp[1,measure_id]
  # read in covariates used for that me
  me_path <- paste0(studlab_path, map[bundle_id == bun_id, me_id], ".csv")
  cvs <- fread(paste0(me_path))
  cvs <- cvs[measure_id == meas_id & is.na(country_covariate_id)]
  if(length(grep('cv', colnames(data_temp))) == 0){
    data_temp[, cv_sex := sex]    
    cvs$study_covariate <- paste0("cv_", cvs$study_covariate)
    cvs$beta <- paste0(cvs$mean_effect, " (", cvs$lower_effect, " - ", cvs$upper_effect, ")")
    cvs <- cvs %>% select(study_covariate, beta) %>% rename(variable = study_covariate) %>% filter(!variable %in% c('cv_NA'))
  } else{
    cvs$study_covariate <- paste0("cv_", cvs$study_covariate)
    cvs$beta <- paste0(cvs$mean_effect, " (", cvs$lower_effect, " - ", cvs$upper_effect, ")")
    cvs <- cvs %>% select(study_covariate, beta) %>% rename(variable = study_covariate) %>% filter(!variable %in% c('cv_sex', 'cv_NA'))
  }
  
  
  
  data_temp <- data_temp %>% select(sex, combo, measure, grep('cv', colnames(data_temp), value = TRUE))
  data_temp <- unique(data_temp)
  data_temp <- merge(data_temp, counts[[measure_i]], by = 'combo')
  data_temp <- merge(data_temp, agesex[[measure_i]], by = 'combo')
  data_temp <- merge(data_temp, ref_map[[measure_i]], by = 'combo')
  if(length(grep('cv_sex', colnames(data_temp))) > 0){
    data_temp <- data_temp[, alt_count:= NA]
  } else{
    data_temp <- data_temp[, alt_count := rowSums(.SD), .SDcols = grep('cv', colnames(data_temp))]
  }
  data_temp <- data_temp[order( -is_reference, alt_count),]
  data_temp <- data_temp[, newcase := .I]
  data_temp$me_id <- map[bundle_id == bun_id, me_id]
  data_temp$me_name <- map[bundle_id == bun_id, me_name]
  data_temp$bundle_id <- bun_id
  data_temp$bundle_name <- map[bundle_id == bun_id, bundle_name]
  
  
  ### Reshape happens here
  data_temp <- dcast(melt(data_temp, id.vars = "newcase"), variable ~ newcase)
  
  keep_rows <- c("bundle_id", "bundle_name", "me_id", "me_name", 'measure', 'combo', 'sex', 'is_reference', 'alt_count', 'n', 'age_and_sex_specific_n', 'not_age_and_sex_specific_n')
  
  for(i in c(1:nrow(data_temp))){
    if(data_temp[i, variable] %in% cvs$variable){
      data_temp[i, used := 'Yes']
    } else if(data_temp[i, variable] %in% keep_rows){
      data_temp[i, used := ""]
    } else{
      data_temp[i, used := 'No']
    }
  }
  
  
  data_temp <- merge(data_temp, cvs, by = 'variable', all.x = TRUE)
  
  ### Reorder columns
  setcolorder(data_temp, c(1, length(colnames(data_temp))-1, length(colnames(data_temp)), 2:(length(colnames(data_temp))-2)))
  
  ### Reorder rows
  yes_cvs <- data_temp[used== "Yes", variable]
  no_cvs <- data_temp[used== "No", variable]
  used_cvs <- c(yes_cvs, no_cvs)
  used_cvs <- used_cvs[!is.na(used_cvs)]
  
  target <- c(keep_rows, used_cvs)
  data_temp <- data_temp[match(target, data_temp$variable),]
}

## ******************************************************************************
# write to xlsx

save_sheets <- function(save_list, save_dir){
  count <- 0
  wb <- createWorkbook()
  for(save in save_list){
    if(count == 0){
      cv_type <- "used"
    } else{
      cv_type <- "all"
    }
    for(meas in c(1:length(save))){
      data <- save[[meas]]
      sheet_name <- paste0(data[5, 4], "_", cv_type)
      
      addWorksheet(wb, sheet_name)
      writeData(wb, sheet_name, data)
    }
    count <- count + 1
  }

  for(label in map[, unique(type)]){
    new_save <- paste0(save_dir, label, "/")
    ifelse(!dir.exists(new_save), dir.create(new_save), FALSE)
    
  }
  
  saveWorkbook(wb, file = paste0("FILEPATH"), overwrite = TRUE)
  
}


## ******************************************************************************
#paired_counts <- lapply(alt_combo_list, ref_to_alt, ref_data = ref_data, all_data = as.data.table(ref_data_by_measure[[i]]))
ref_to_alt <- function(combo_i, ref_data, all_data){
  
  alt_data_i <- all_data[combo == combo_i]
  
  alt_data_i[, year_mid := (year_start + year_end)/2]
  
  demo_match_cols <- c('year_mid', 'location_id', 'sex')
  
  paired_data <- merge(ref_data, alt_data_i, by = c('location_id','sex', 'age_start'), all.x = FALSE, suffixes = c('.ref', '.alt'), allow.cartesian = TRUE)
  paired_data <- paired_data[abs(year_mid.ref - year_mid.alt) < 6]
  
  pairs_count <- nrow(paired_data)
  
  return(pairs_count)
  
}
