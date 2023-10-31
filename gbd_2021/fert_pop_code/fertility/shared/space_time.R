##############################
## Purpose: Space-time smoothing function
## Details: Run for one location at a time 
##          Smooth using beta distribution
###############################

space_time_beta <- function(data, loc, beta, zeta, min_year = 1950, max_year = 2020, 
                            loc_map_path = '', use_super_reg = F, regression = F,
                            data_density) {
  
  ##################################
  ## space_time_beta
  ## calculates space-time smoothed estimates
  ##
  ## inputs:
  ##    data: (data.table) containing all locations
  ##    loc: (character) ihme_loc_id of country to be smoothed    
  ##    beta: (numeric) value for shape parameters of beta distribution (both are equal b/c want a symmetric distribution)
  ##          increase beta for less time smoothing, decrease for more time smoothing 
  ##    zeta: (numberic) parameter for space smoothing
  ##          increase zeta to weight in-country data more, decrease to weight in-country more equally to out-of-country 
  ##    min_year: (numeric) earliest estimation year
  ##    max_year: (numeric) latest estimation year
  ##    loc_map_path: (character) file path for 
  ##    use_super_reg: (logical) whether to use super region data instead of region data for smoothing
  ##    data_density: (numeric) data density of a location
  ##    regression: (logical) whether to use constant or combined variant calculation
  ##
  ## outputs:
  ##    preds: (data.table) smoothed predictions for loc
  ##################################
  
  loc_map <- fread(paste0(loc_map_path))
  
  super_reg <- loc_map[ihme_loc_id == loc, super_region_id]
  reg       <- loc_map[ihme_loc_id == loc, region_id]
  
  ifelse(use_super_reg, region_data <- data[super_region_id == super_reg & !is.na(resid)], 
         region_data <- data[region_id == reg & !is.na(resid)])

  region_data[, in_country := (ihme_loc_id == loc)] 

  preds <- data.table()
  
  for (year in min_year:max_year) {
    
    region_data[, t := year_id - year]
    region_data[, w := dbeta((t+72)/144, shape1=beta, shape2=beta)/dbeta(0.5, beta, beta)]
    if (loc == 'CHN_44533' & year > 2010) {
      region_data[, w:= dbeta((t+72)/144, shape1=4000, shape2=4000)/dbeta(0.5, 4000, 4000)]
    }
    region_resids <- round(region_data[in_country==F, sum(w)], 10) != 0
    
    if (zeta < 1) {
      if (region_resids) {
        s <- (zeta/(1-zeta))*(region_data[in_country==F, sum(w)]/region_data[in_country==T, sum(w)])
        region_data[in_country==T, w := w*s]
      }
    } else {
      region_data[in_country==T, w := 0]
    }
    
    # fit variant 2: fixed effects local regression
    constant <- region_data[,resid %*% (w/sum(w))]
    
    ## keeping option to run with regression = T/F
    if(regression == T){
      preds <- rbind(preds, data.table(ihme_loc_id = loc, year = year, 
                                       resid = combined, stringsAsFactors = F))
    } else if (regression == F) {
      preds <- rbind(preds, data.table(ihme_loc_id = loc, year = year, 
                                       resid = constant, stringsAsFactors = F))
    }
    
    
    
  }
  
  return(preds)
}