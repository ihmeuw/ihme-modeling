.get_one_year_populations <- function(model_inputs_path, 
                                      hierarchy=NULL
) {
  
  source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))
  source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))
  
  source("FILEPATH/get_location_metadata.R")
  source("FILEPATH/get_age_metadata.R")
  source("FILEPATH/get_population.R")
  
  if (is.null(hierarchy)) hierarchy <- gbd_data$get_covid_covariate_prep_hierarchy()
  
  covid_locs <- unique(hierarchy$location_id)
  
  pop_full <- .load_all_populations(model_inputs_root = model_inputs_path)
  pop_full <- pop_full[sex_id == 3,] # both sexes
  pop_full <- pop_full[pop_full$location_id %in% covid_locs,] # Lose extra GBD locations
  
  
  #-----------------------------------------------------------------------------
  # Get one year population intervals
  #-----------------------------------------------------------------------------
  
  # Get one year age intervals
  pop_one_year <- pop_full[pop_full$age_group_id %in% c(28, 49:142),]
  #pop_five_year <- pop_full[pop_full$age_group_id %in% c(1, 6:20, 30:32),]
  
  # Check 
  age_intervals <- pop_one_year$age_group_years_end - pop_one_year$age_group_years_start
  if (!all(age_intervals == 1)) stop('Must use one year age intervals')
  
  
  
  #-----------------------------------------------------------------------------
  # Build 95+ one year intervals
  #-----------------------------------------------------------------------------
  
  # Assuming uniform distribution is fine since these age groups are always aggregated
  pop_95_125 <- pop_full[pop_full$age_group_id == 235,]
  
  if (!all(pop_one_year$location_id %in% pop_95_125$location_id)) warning('95-125 may be missing for some locations. 95+ one year intervals may be low for some locations.')
  
  bins <- 95:125
  
  for (i in unique(pop_95_125$location_id)) {
    tmp <- pop_one_year[seq_along(bins),]
    tmp$location_id <- i
    tmp$age_group_id <- 999
    tmp$age_group_years_start <- bins
    tmp$age_group_years_end <- (bins) + 1
    tmp$population <- pop_95_125$population[pop_95_125$location_id == i] / length(bins)
    pop_one_year <- rbind(pop_one_year, tmp)
  }
  
  
  #-----------------------------------------------------------------------------
  # Infer missing locations
  #-----------------------------------------------------------------------------
  
  # The locations missing one year bins here:
  missing_locs <- covid_locs[!(covid_locs %in% pop_one_year$location_id)]
  hierarchy[location_id %in% missing_locs, .(location_id, location_name)]
  
  # Missing locs can be inferred using the relative proportions observed in the all age group
  
  aus <- hierarchy[parent_id == 71, .(location_id, location_name, parent_id)]
  
  aus$population <- c(431826,   # Australian Capital Territory
                      8176368,  # New South Wales
                      247023,   # Northern Territory
                      5206400,  # Queensland
                      1771703,  # South Australia
                      541965,   # Tasmania
                      6648564,  # Victoria
                      2675797)  # Western Australia  
  
  aus$prop <- aus$population / sum(aus$population)
  
  # These are Washington sub-state locs for all age - use to split each age group proportionaly
  
  wash <- pop_full[location_id %in% hierarchy[parent_id == 570, location_id] & age_group_id == 22, .(location_id, population)]
  wash <- merge(wash, hierarchy[,.(location_id, location_name, parent_id)])
  wash$prop <- wash$population / sum(wash$population)
  
  missing_locs <- rbind(aus, wash, fill=T)
  
  # Split out one year age intervals based on all-age proportion bt parent and child locations
  for (i in missing_locs$location_id) {
    for (j in unique(pop_one_year$age_group_id)) {
      
      sel <- missing_locs$location_id == i
      tmp <- pop_one_year[location_id == missing_locs$parent_id[sel] & age_group_id == j,]
      tmp$location_id <- i
      tmp$population <- tmp$population * missing_locs$prop[sel]
      pop_one_year <- rbind(pop_one_year, tmp)
      
    }
  }
  
  
  #-----------------------------------------------------------------------------
  # Hard-coded population adjustments
  #-----------------------------------------------------------------------------
  
  # Fix for Turkmenistan under-estimate
  # https://www.worldometers.info/world-population/turkmenistan-population/
  sel <- which(pop_one_year$location_id == 40)
  scalar <- 6172323/sum(pop_one_year$population[sel])
  pop_one_year$population[sel] <- pop_one_year$population[sel] * scalar
  
  # Fix for Canada under-estimate
  # https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1710000901
  sel <- which(pop_one_year$location_id == 101)
  scalar <- 38436447/sum(pop_one_year$population[sel])
  pop_one_year$population[sel] <- pop_one_year$population[sel] * scalar
  
  
  #-----------------------------------------------------------------------------
  # Checks
  #-----------------------------------------------------------------------------
  
  sel <- is.na(pop_one_year$population)
  if (sum(sel) > 0) warning('NAs recorded for population size in one year age intervals')
  
  for (i in unique(pop_one_year$location_id)) {
    tmp <- pop_one_year[location_id == i,]
    if (!all(0:125 %in% tmp$age_group_years_start)) warning(glue('One year age bins missing in location {i}'))
  }
  
  if (!all(covid_locs %in% pop_one_year$location_id)) stop('One year population still missing after Australia and Washington subnats were built')
  return(pop_one_year)
}


.get_age_group_populations <- function(age_starts,
                                       model_inputs_path,
                                       hierarchy=NULL,
                                       include_all_ages=FALSE
) {
  
  source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))
  source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))
  
  #age_ends <- c(age_starts[-1] - 1, 125)
  age_groups <- .get_age_groups(age_starts)
  all_age_group <- paste0(age_starts[1], '-', 125)
  if (include_all_ages) age_groups <- c(age_groups, all_age_group)
  if (is.null(hierarchy)) hierarchy <- gbd_data$get_covid_covariate_prep_hierarchy()
  
  covid_locs <- unique(hierarchy$location_id)
  
  # Get populations in one year intervals
  pop_one_year <- .get_one_year_populations(model_inputs_path = model_inputs_path, hierarchy = hierarchy)
  
  # Bin one year intervals into age groups
  pop_one_year$age_group <- cut(pop_one_year$age_group_years_start, breaks = c(age_starts, 126), right=F, include.lowest = T)
  
  # Calculate aggregate sums in age groups
  pop_age_group <- aggregate(population ~ location_id + age_group, data=pop_one_year, FUN=sum)
  
  # Make age groups labels
  tmp <- strsplit(as.character(pop_age_group$age_group), split="\\W+")
  tmp <- do.call(rbind, lapply(tmp, FUN=function(x) matrix(c(as.integer(x[2]), as.integer(x[3])-1), nrow=1)))
  colnames(tmp) <- c('age_start', 'age_end')
  pop_age_group <- cbind(pop_age_group, tmp)
  
  if (include_all_ages) {
    pop_all_age <- aggregate(population ~ location_id, data=pop_age_group, FUN=sum)
    pop_all_age$age_start <- age_starts[1]
    pop_all_age$age_end <- 125
    pop_all_age$age_group <- NA
    pop_age_group <- rbind(pop_age_group, pop_all_age)
  }
  
  pop_age_group$age_group <- paste(pop_age_group$age_start, pop_age_group$age_end, sep='-')
  
  # Check
  sel <- is.na(pop_age_group$population)
  if (sum(sel) > 0) warning('NAs recorded for population size in one year age intervals')
  
  for (i in unique(pop_age_group$location_id)) {
    tmp <- pop_age_group[pop_age_group$location_id == i,]
    if (!all(age_starts %in% tmp$age_start)) warning(glue('Population missing for some age starts in location {i}'))
  }
  
  if (!all(covid_locs %in% pop_age_group$location_id)) stop('Age group populations missing some locations')
  
  
  return(pop_age_group)
  
}

