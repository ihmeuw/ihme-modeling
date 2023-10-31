# Function to make lond data frame of population sizes for the specific age groups 
# we commonly use: 12-17, 18-64, 65+. Can include an 'all' age group which is 12+
# Hard coded age groups for now- would be ideal to abstract this to take a vector of age starts like the hesitancy funcs



.make_age_group_populations <- function(model_inputs_path = .model_inputs_path, 
                                        hierarchy=NULL,
                                        under_18=FALSE,
                                        over_0=TRUE, 
                                        over_5=TRUE, 
                                        over_12=TRUE, 
                                        include_all_ages=TRUE
                                        ) {
  
  if (over_0) over_5 <- TRUE
  if (over_5) over_12 <- TRUE
  if (under_18) over_4 <- over_5 <- over_12 <- FALSE
  
  source("FILEPATH/get_age_metadata.R")
  source("FILEPATH/get_population.R")
  
  if (is.null(hierarchy)) hierarchy <- gbd_data$get_covid_covariate_prep_hierarchy()
  
  pop_full <- .load_all_populations(model_inputs_root = model_inputs_path)
  pop_full <- pop_full[sex_id == 3,]
  
  covid_locs <- unique(hierarchy$location_id)
  #age_groups <- get_age_metadata(age_group_set_id=12, gbd_round_id=6)
  
  out <- data.table()
  
  #-------------------------------------------------------------------------------
  # 0-5
  #-------------------------------------------------------------------------------
  
  if (under_18) {
    
    tmp <- pop_full[age_group_id %in% c(1,6,7,8)]
    tmp[age_group_id == 8, population := population * (3/5)]
    
    tmp <- as.data.table(aggregate(population ~ location_id, tmp, sum))
    tmp$age_start <- 0
    tmp$age_end <- 17
    
    out <- rbind(out, tmp)
    
  } else {
    
    #-------------------------------------------------------------------------------
    # 0-4
    #-------------------------------------------------------------------------------
    
    if (over_0) {
      
      tmp <- pop_full[age_group_id == 1]
      
      tmp <- as.data.table(aggregate(population ~ location_id, tmp, sum))
      tmp$age_start <- 0
      tmp$age_end <- 4
      
      out <- rbind(out, tmp)
      
    }
    
    
    #-------------------------------------------------------------------------------
    # 5-11
    #-------------------------------------------------------------------------------
    
    if (over_5) {
      
      tmp <- pop_full[age_group_id %in% c(6,7)]
      tmp[age_group_id == 7, population := population * (2/5)]
      
      tmp <- as.data.table(aggregate(population ~ location_id, tmp, sum))
      tmp$age_start <- 5
      tmp$age_end <- 11
      
      out <- rbind(out, tmp)
      
    }
    
    
    #-------------------------------------------------------------------------------
    # 12-17
    #-------------------------------------------------------------------------------
    
    if (over_12) {
      
      tmp <- pop_full[age_group_id %in% c(7,8)]
      tmp[, population := population * (3/5)]
      
      tmp <- as.data.table(aggregate(population ~ location_id, tmp, sum))
      tmp$age_start <- 12
      tmp$age_end <- 17
      
      out <- rbind(out, tmp)
      
    }
    
  }
  
  
  #-------------------------------------------------------------------------------
  # 18-39
  #-------------------------------------------------------------------------------
  
  tmp <- pop_full[age_group_id %in% c(8:12)]
  tmp[age_group_id == 8, population := population * (2/5)]
  
  tmp <- as.data.table(aggregate(population ~ location_id, tmp, sum))
  tmp$age_start <- 18
  tmp$age_end <- 39
  
  out <- rbind(out, tmp)
  
  
  #-------------------------------------------------------------------------------
  # 40-64
  #-------------------------------------------------------------------------------
  
  tmp <- pop_full[age_group_id %in% c(13:17)]
  tmp[age_group_id == 8, population := population * (2/5)]
  
  tmp <- as.data.table(aggregate(population ~ location_id, tmp, sum))
  tmp$age_start <- 40
  tmp$age_end <- 64
  
  out <- rbind(out, tmp)
  
  
  #-------------------------------------------------------------------------------
  # 65+
  #-------------------------------------------------------------------------------
  
  
  tmp <- pop_full[age_group_id %in% c(18:20,30:32,235)]
  
  tmp <- as.data.table(aggregate(population ~ location_id, tmp, sum))
  tmp$age_start <- 65
  tmp$age_end <- 125
  
  out <- rbind(out, tmp)
  
  
  #-------------------------------------------------------------------------------
  # All ages
  #-------------------------------------------------------------------------------
  
  if (include_all_ages) {
    
    tmp <- as.data.table(aggregate(population ~ location_id, out, sum))
    tmp$age_start <- min(out$age_start)
    tmp$age_end <- max(out$age_end)
    out <- rbind(out, tmp)
    
  }

  
  out$age_group <- stringr::str_c(out$age_start, '-', out$age_end)
  out <- out[order(location_id, age_group)]
  sel <- c(colnames(out)[!colnames(out) == 'population'], 'population')
  out <- out[,..sel, with=F]
  
  
  #-------------------------------------------------------------------------------
  # Split out Australian subnationals
  #-------------------------------------------------------------------------------
  
  missing <- covid_locs[!(covid_locs %in% out$location_id)]
  
  if (length(missing) > 0 & all(hierarchy[location_id %in% missing, parent_id] == 71)) {
    
    missing <- hierarchy[location_id %in% missing, .(location_id, location_name)]
    
    missing$population <- c(
      431826,   # Australian Capital Territory
      8176368,  # New South Wales
      247023,   # Northern Territory
      5206400,  # Queensland
      1771703,  # South Australia
      541965,   # Tasmania
      6648564,  # Victoria
      2675797   # Western Australia  
    )
    
    missing$prop <- missing$population / sum(missing$population)
    
    for (i in missing$location_id) {
      
      tmp <- out[location_id == 71,]
      tmp$population <- tmp$population * missing$prop[missing$location_id == i]
      tmp$location_id <- i
      out <- rbind(out, tmp)
      
    }
    
  }
  
  # Check
  #for (i in unique(out$location_id)) if (nrow(out[location_id == i]) < 4) print(i)
  #out[is.na(out$population),]
  
  tmp <- hierarchy[location_id %in% covid_locs[!(covid_locs %in% out$location_id)],]
  
  if (nrow(tmp) > 0) {
    warning('Population object missing locations')
    print(tmp)
  }
  
  return(out)
}
