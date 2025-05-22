#############################################################
# Purpose: prep POL CF inputs

# Currently maintained by Audrey Batzel
#############################################################

# Get settings / controls ####
source("~/00a_prep_setup.R")

# Poland data ####
if (prep_pol == TRUE){
  ## Create write folder ####
  write_folder = paste0('FILEPATH')
  if (!dir.exists(write_folder)) dir.create(write_folder, recursive = TRUE)
  
  # Read in data ####
  pol_files = Sys.glob(paste0('FILEPATH'))
  est_ids = unique(dno_mapping$estimate_id)
  files = lapply(est_ids, function(x){ pol_files[grepl(x, pol_files)]})
  pol_files = unlist(files)
  
  pol_data = lapply(pol_files, function(x){
    pdt = read_parquet(x)
    return(pdt)
  })
  pol = rbindlist(pol_data, use.names = TRUE)

  # Bin ages ####
  pol = age_binner(pol)
  pol[, age := NULL]
  
  # Map on the estimate ids/dno classifications and keep just esimates we want for CFs
  pol[, estimate_id := paste0("estimate_", estimate_id)]
  pol[, estimate_type := NULL]
  pol_ids = merge(pol, dno_mapping, by = 'estimate_id', all.x = TRUE)

  # Sum cases columns by key values into age bins ####
  id_vars = names(pol_ids)[!names(pol_ids)%in% c('val')]
  pol_ids = pol_ids[, .(val = sum(val)), by = id_vars]
  
  # Merge on age_group_id ####
  pol_ids = merge(pol_ids, ages[age_group_id > 4, c('age_start','age_end','age_group_id')],
                   by=c('age_start','age_end'))
  
  # Make mid-point of ages
  pol_ids[, age_midpoint := (age_start + age_end)/2]
  
  # Sample size using population ####
  pol_pop = get_population(age_group_id = unique(pol_ids$age_group_id), 
                           year_id = unique(pol_ids$year_id), 
                           location_id = unique(pol_ids$location_id), 
                           sex_id = c(1,2), 
                           release_id=9)
  pol_pop[,run_id := NULL]
  pol_ids = merge(pol_ids, pol_pop, by = c('year_id','age_group_id','sex_id','location_id'), all.x = TRUE)
  
  # Clean up ####
  # Create columns for source/parent_id
  pol_ids[, source := 'POL'][, parent_id := 51]
  setnames(pol_ids, 'estimate_id', 'estimate')
  pol_ids[, estimate_type := NULL]
  
  # Save data by estimate ID ####
  lapply(unique(pol_ids$estimate), function(x){
    write_data = pol_ids[estimate == x, ]
    fwrite(write_data, paste0(write_folder,'/',x,'.csv'))
  })
  
}
