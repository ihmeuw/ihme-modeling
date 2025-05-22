#############################################################
# Purpose: prep hosp (HCUP, NZL, PHL) CF inputs
#############################################################

# Get settings / controls ####
source("~/00a_prep_setup.R")
hosp_dt = data.frame()

# Hospital data ####
# variables to have in the data at the end
id.vars = c('age_start','age_end','sex_id','year_start','location_id','bundle_id','sample_size',
            'age_group_id','age_midpoint', 'source', 'parent_id', 'estimate')

if (prep_hosp == TRUE){
  ## Create write folder ####
  write_folder = paste0('FILEPATH')
  if (!dir.exists(write_folder)) dir.create(write_folder, recursive = TRUE)
  
  # Read in data ####
  # read in all HCUP/NZL/PHL files
  hosp_files = Sys.glob(paste0('FILEPATH'))
  
  filereader = lapply(hosp_files, function(filepath){
    a = tryCatch({
      df = fread(filepath)
      outcome = paste0(filepath, ' worked!')
      print(outcome)
      print(ncol(df))
      print(nrow(df))
      output = list(data = df, logs = outcome)
      return(output)
    },
    error = function(cond){
      outcome = paste0(filepath, ' broke')
      message(outcome)
      output = list(data = NULL, logs = outcome)
      return(output)
    })
  } )
  hosp_dt = rbindlist(lapply(filereader, function(list) list$data))
  
  # Save a log of which files were successfully read and which were empty
  logs = unlist(lapply(filereader, function(list) list$logs), use.names = FALSE)
  logs = data.frame(logs)
  write_csv(logs, paste0(write_folder, 'fileloading.csv'))
  
  # Replace all NAs in hosp_dt with 0s
  hosp_dt[is.na(hosp_dt)] = 0
  
  # Bin ages ####
  # add age_start and age_end columns
  hosp_dt = age_binner(hosp_dt, include_neonatal = TRUE)
  # sum cases columns by key values into age bins
  hosp_dt[,age := NULL]
  hosp_dt = hosp_dt[,lapply(.SD,sum),by=c('location_id','year_start','year_end','sex_id','bundle_id','age_start','age_end')]
  hosp_dt = hosp_dt[,lapply(.SD,as.numeric),by=c('location_id','year_start','year_end','sex_id','bundle_id','age_start','age_end')]

  # Reshape long by estimate type ####
  hosp_dt = melt(hosp_dt, id.vars = c('location_id','year_start','year_end','sex_id','bundle_id','age_start',
                                       'age_end'), variable.name = 'estimate_type', value.name = 'val')
  
  # Map on the estimate ids/dno classifications and keep just estimates we want for CFs
  hosp_ids = merge(hosp_dt, dno_mapping, by = 'estimate_type') 
  
  # Merge on age_group_id ####
  hosp_ids = merge(hosp_ids, ages[, c('age_start', 'age_group_id', 'age_end')], all.x = TRUE, by = c('age_start', 'age_end'))
  
  # Make a midpoint of ages
  hosp_ids[, age_midpoint := (age_start+age_end)/2]
  
  # Use location ids to merge on parent ids from gbd locations and include source column
  hosp_ids = merge(hosp_ids, locs[, c('location_id', 'parent_id')], all.x = TRUE, by = 'location_id') %>%
    .[parent_id == "ID", source := 'PHL'] %>%
    .[parent_id == "ID", source := 'NZL'] %>%
    .[parent_id == "ID", source := 'HCUP']
  
  # Drop the data from above that has NAs for age
  hosp_ids = hosp_ids[!is.na(age_start), ]
  
  # Sample size using population ####
  pops = get_population(year_id = unique(hosp_ids$year_start), 
                         sex_id = c(1,2), 
                         location_id = unique(hosp_ids$location_id), 
                         age_group_id = unique(hosp_ids$age_group_id)[!is.na(unique(hosp_ids$age_group_id))],
                         release_id = "ID") %>% setnames('year_id', 'year_start')
  
  hosp_ids = merge(hosp_ids, pops[, c('sex_id', 'age_group_id', 'year_start', 'location_id', 'population')], all.x = TRUE,
                   by = c('sex_id', 'age_group_id', 'year_start', 'location_id'))
  
  # Clean up ####
  setnames(hosp_ids, 'estimate_id', 'estimate')
  setnames(hosp_ids, 'year_start', 'year_id')
  hosp_ids[, year_end := NULL]
  hosp_ids[, estimate_type := NULL]
  
  # Save data by estimate ID ####
  lapply(unique(hosp_ids$estimate), function(x){
    write_data = hosp_ids[estimate == x, ]
    fwrite(write_data, paste0(write_folder,'/',x,'.csv'))
  })
  
}
