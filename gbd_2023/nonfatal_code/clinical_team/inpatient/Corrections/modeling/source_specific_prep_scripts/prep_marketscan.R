#############################################################
# Purpose: prep MS CF inputs
#############################################################
# Get global CF settings / controls ####
source("~/00a_prep_setup.R")
ms <- data.frame()

# Marketscan ####
if (prep_ms == TRUE){
  write_folder <- paste0('FILEPATH')
  if (!dir.exists(write_folder)) dir.create(write_folder)
  
  ## Read in data ####
  ms_files = Sys.glob(paste0('FILEPATH'))
  est_ids = unique(dno_mapping$estimate_id)
  files = lapply(est_ids, function(x){ ms_files[grepl(x, ms_files)]})
  ms_files = unlist(files)
  
  filereader = lapply(ms_files, function(filepath){
    a = tryCatch({
      df = read_parquet(filepath)
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
  ms = rbindlist(lapply(filereader, function(list) list$data))
  
  had_error = sapply(filereader, function(x) class(x)=="try-error")
  if(length(which(had_error == TRUE)) > 0) print("Check filereader, some files not read correctly")
  
  ms[, facility_id := NULL]
  
  # Remove year 2000
  ms <- ms[year_id != 2000]

  # Map on the estimate ids/dno classifications and keep just esimates we want for CFs
  ms[, estimate_id := paste0("estimate_", estimate_id)]
  ms[, estimate_type := NULL]
  ms_ids = merge(ms, dno_mapping, by = 'estimate_id', all.x = TRUE)
  
  # Bin ages ####
  # add age_start and age_end columns
  ms_ids = age_binner(ms_ids)
  # Sum cases columns by key values into age bins
  ms_ids[,age := NULL]
  idvars = names(ms_ids)[!names(ms_ids)%in%c('val')]
  ms_ids = ms_ids[,.(val = sum(val)), by=idvars]

  # Merge on age_group_id
  ms_ids = merge(ms_ids, ages[age_group_id > 4, c('age_start', 'age_group_id', 'age_end')], all.x = TRUE, by = c('age_start', 'age_end'))

  # Get sample size ####
  # sample size files by year
  n_files = list.files("FILEPATH", recursive = TRUE)
  n_dt = lapply(n_files, function(x) {
      year = sub(pattern = 'year=', '', x)
      year = strsplit(year, "/")[[1]][1]
      data = read_parquet(paste0("FILEPATH", x))
      data$year_id = as.integer(year)
      return(data)
    } 
  )
  n_dt = rbindlist(n_dt)
  n_dt = age_binner(n_dt)
  n_dt = n_dt[,.(population = sum(sample_size)), by = .(age_start, age_end, sex_id, location_id, year_id)]
  
  # Merge sample_size from enrollees table onto ms table
  ms_ids = merge(ms_ids, n_dt, by = c('age_start','age_end','sex_id','year_id','location_id'), all.x = TRUE)
  
  # Drop US national data, not representative for Marketscan (should all be coded by the state)
  ms_ids = ms_ids[location_id != 102] 
  
  if(nrow(unique(ms_ids[, .(age_start, age_end, sex_id, location_id, bundle_id, estimate_id, year_id)])) != nrow(ms_ids)) stop("Error: an aggregation step may be needed.")
  
  # Merge on age_group_id, calculate age_midpoint and add other columns for prep
  ms_ids[, age_midpoint := (age_start + age_end)/2]
  ms_ids[, source := 'Marketscan']
  ms_ids[, parent_id := 102]
  setnames(ms_ids, "estimate_id", "estimate")
  ms_ids[, estimate_type := NULL]

  # Save data by estimate ID ####
  lapply(unique(ms_ids$estimate), function(x){
    write_data = ms_ids[estimate == x, ]
    fwrite(write_data, paste0(write_folder,'/',x,'.csv'))
  })
  
}
