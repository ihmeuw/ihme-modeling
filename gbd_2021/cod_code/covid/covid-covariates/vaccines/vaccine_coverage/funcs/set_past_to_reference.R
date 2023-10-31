

# Vaccination (reference) - 2022_01_31.03
# Vaccination (probably no) - 2022_01_31.05
#reference_version <- "2022_01_31.03"
#bjective_version <- "2022_01_31.05"

set_past_to_reference <- function(objective_output,
                                  reference_path, 
                                  stop_date = NULL,
                                  t_gap = 7,
                                  plots = FALSE
) {
  
  if (is.null(stop_date)) stop_date <- Sys.Date()
  
  reference_version <- .get_version_from_path(reference_path)
  #objective_version <- .get_version_from_path(objective_path)
  objective_version <- .output_version
  
  reference_output <- as.data.frame(fread(file.path(reference_path, 'slow_scenario_vaccine_coverage.csv')))
  #out <- objective_output <- as.data.frame(fread(file.path(objective_path, 'slow_scenario_vaccine_coverage.csv')))
  out <- objective_output <- as.data.frame(objective_output)
  
  max_date <- as.Date(max(reference_output$date))
  
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
  
  message(glue::glue('Setting vaccination quantities to reference prior to {stop_date}...'))
  
  for (i in unique(objective_output$location_id)) {
    for (j in quantities) {
      
      x <- reference_output[reference_output$location_id == i, c('date', j)] 
      y <- objective_output[objective_output$location_id == i, c('date', j)]
      tmp <- merge(x, y, by='date', all.y=T)
      
      # Set quantity of objective scenario to reference scenario
      sel_row <- which(objective_output$location_id == i & objective_output$date <= as.Date(stop_date))
      sel_col <- which(colnames(objective_output) == j)
      objective_output[sel_row, sel_col] <- tmp[tmp$date <= as.Date(stop_date), paste0(j, '.x')]
      
      # Knock out week prior and post stop_date
      sel_row <- which(objective_output$location_id == i & objective_output$date >= as.Date(stop_date) & objective_output$date <= as.Date(stop_date) + t_gap)
      objective_output[sel_row, sel_col] <- NA
      
      # "smooth" transition
      sel_row <- which(objective_output$location_id == i)
      out[sel_row, sel_col] <- zoo::na.approx(objective_output[sel_row, sel_col], na.rm=F)
      
    }
  }
  
  
  # Check
  message('Checking that reference and new scenario are identical...')
  for (i in unique(objective_output$location_id)) {
    for (j in quantities) {
      
      # Line up two quantities by date
      tmp <- merge(reference_output[reference_output$location_id == i & reference_output$date < as.Date(stop_date), c('date', j)], 
                   out[out$location_id == i & out$date < as.Date(stop_date), c('date', j)], 
                   by='date', 
                   all.y=T)
      
      if (!identical(tmp[,2], tmp[,3])) warning(glue::glue('{i} | {j} | objective version {objective_veresion} is not identical to refrence version {reference_version} prior to {stop_date}'))
      
    }
  }

  if (!identical(dim(out), dim(objective_output))) stop('Some observations have been lost when setting objective output to reference in the past')
  
  
  
  #file_name <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, objective_version, 'slow_scenario_vaccine_coverage.csv')
  #write.csv(out, file=file_name)
  #message(glue::glue('Past vaccination quantities set to reference version {reference_version} and saved here: {file_name}'))
  
  
  if (plots) {
    
    message('Plotting diagnostics...')
    
    pdf(file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, objective_version, glue::glue('check_set_past_to_reference_{reference_version}_{objective_version}.pdf')),
        height=11, width=9, onefile = T)
    
    message('Setting vaccination quantities to reference in past...')
    
    for (i in unique(objective_output$location_id)) {
      
      par(mfrow=c(5,3))
      for (j in quantities) {
        
        x <- reference_output[reference_output$location_id == i, c('date', j)] 
        y <- objective_output[objective_output$location_id == i, c('date', j)]
        
        plot(y$date, y[,j], type='l', col='red', main=i)
        lines(x$date, x[,j], type='l', col='blue')
        abline(v=stop_date)
        
        y <- out[out$location_id == i, c('date', j)]
        
        plot(y$date, y[,j], type='l', col='red', main=j)
        lines(x$date, x[,j], type='l', col='blue')
        abline(v=stop_date)
        
        plot(y$date, cumsum(tidyr::replace_na(y[,j], 0)), type='l', col='red')
        lines(x$date, cumsum(tidyr::replace_na(x[,j], 0)), type='l', col='blue')
        abline(v=stop_date)
        legend('bottomright', legend=c(paste('Objective', objective_version), paste('Reference', reference_version)),
               col=c("red", "blue"), lty=1, cex=0.8)
      }
      
      dev.off()
      par(mfrow=c(1,1))
      
    }
    
  }
  
  return(out)
  
}








