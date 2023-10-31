duct_tape <- function(object) {
  
  message('Administering duct tape') # Duct tape (also called duck tape, from the cotton duck cloth it was originally made of) is cloth- or scrim-backed pressure-sensitive tape, often coated with polyethylene.
  
  #model_version <- .get_version_from_path(model_path)
  #object <- as.data.frame(fread(file.path(model_path, 'slow_scenario_vaccine_coverage.csv')))
  
  object <- as.data.frame(object)
  
  max_date <- as.Date(max(object$date))
  max_prop <- 0.99
  
  quantities <- c(
    'lr_unprotected',
    'lr_effective_protected_wildtype',
    'lr_effective_protected_variant',
    'lr_effective_wildtype',
    'lr_effective_variant',
    'hr_unprotected',
    'hr_effective_protected_wildtype',
    'hr_effective_protected_variant',
    'hr_effective_wildtype',
    'hr_effective_variant'
  )
  
  message('Checking that vaccination quantities do not exceed adult population...')
  
  for (i in unique(object$location_id)) {
    
    sel_loc <- which(object$location_id == i)
    
    tmp <- object[sel_loc, c('date', 'adult_population', quantities)]
    tmp[,-c(1,2)] <- apply(tmp[,-c(1,2)], MARGIN=2, FUN=function(x) cumsum(tidyr::replace_na(x, 0)))
    tmp$tot <- rowSums(tmp[,-c(1,2)])
    
    sel_date <- which(tmp$date == max_date)
    if (!is.na(tmp$adult_population[sel_date]) & tmp$tot[sel_date] > tmp$adult_population[sel_date]*max_prop) {

      dates_over <- tmp$date[tmp$tot > tmp$adult_population*max_prop]
      object[object$location_id == i & object$date %in% dates_over, quantities] <- 0
      warning(paste('Capped daily rates in', i))
    }
  }
  
  #par(mfrow=c(5,2))
  #
  #x <- object[object$location_id == i, ] 
  #
  #for (j in quantities) {
#
  #  plot(x$date, x[,j], type='l', main=i)
  #  abline(v=Sys.Date())
  #
  #  plot(x$date, cumsum(tidyr::replace_na(x[,j], 0)), type='l')
  #  abline(v=Sys.Date())
  #  
  #}
  
  #par(mfrow=c(1,1))
  
  
  
  quantities <- c(
    'cumulative_all_vaccinated',
    'cumulative_all_fully_vaccinated',
    'cumulative_all_effective'
  )
  
  for (i in unique(object$location_id)) {
    
    sel_loc <- which(object$location_id == i)
    
    if (all(!is.na(object[sel_loc, 'adult_population']))) {
      
      for (q in quantities) object[sel_loc, q] <- pmin(object[sel_loc, q], object[sel_loc, 'adult_population']*max_prop)
      warning(paste('Capped cumulative totals in', i))
    }
    
  }
  
  # Check
  #tmp <- object[object$location_id == 6,]
  #plot(tmp$date, tmp$cumulative_all_vaccinated)
  #points(tmp$date, tmp$cumulative_all_fully_vaccinated, col='red')
  #points(tmp$date, tmp$cumulative_all_effective, col='blue')
  #abline(h=tmp$adult_population)
  
  
  #write.csv(object, file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, model_version, 'slow_scenario_vaccine_coverage.csv'))
  return(as.data.table(object))
} 