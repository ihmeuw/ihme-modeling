
apply_crosswalk <- function(dt, xwalk_mean, xwalk_se, add_note = NA) {

  
  # Where cases or sample size is missing but we have values that allow direct calculation, do that here
  dt[is.na(sample_size) == T & is.na(cases) == F & is.na(mean) == F, 
     sample_size := cases / mean]
  dt[is.na(mean) == T & is.na(cases) == F & is.na(sample_size) == F, 
     mean := cases / sample_size]
  
  
  # Where we have counts but not UIs or SEs, calculate UIs
  dt[is.na(lower) & is.na(standard_error), 
     lower := BinomCI(cases, sample_size, method = 'wilson')[, 2]]
  dt[is.na(upper) & is.na(standard_error), 
     upper := BinomCI(cases, sample_size, method = 'wilson')[, 3]]
  dt[mean == 0, lower := 0]
  dt[mean == 1, upper := 1]
  
  # Where we have UIs but not SEs, calculate SEs
  dt[, ui_prob := ifelse(is.na(uncertainty_type_value), 
                         0.975, 1 - (100 - uncertainty_type_value)/200)]
  dt[, lower_se := ifelse(is.na(lower), standard_error, 
                          (mean - lower)/qnorm(ui_prob))]
  dt[, upper_se := ifelse(is.na(upper), standard_error, 
                          (upper - mean)/qnorm(ui_prob))]
  
  dt[, lower_se := sqrt((xwalk_mean^2 * lower_se^2) + (mean^2 * xwalk_se^2))]
  dt[, upper_se := sqrt((xwalk_mean^2 * upper_se^2) + (mean^2 * xwalk_se^2))]
  
  dt[, mean := mean * xwalk_mean]
  dt[, lower := mean - lower_se * qnorm(0.975)]
  dt[, upper := mean + upper_se * qnorm(0.975)]
  
  
  ### CLEAN UP ###
  # Clear out SE, SS, and cases to ensure that new uncertainty and mean are used
  dt[, c('standard_error', 'sample_size', 'cases') := NA]
  dt[, c('lower_se', 'upper_se', 'ui_prob') := NULL]
  
  
  dt[!is.na(seq), crosswalk_parent_seq := seq]
  dt[, seq := NA]
  dt[, uncertainty_type_value := 95] 
  
  
  # Add note to note_modeler if needed
  if (!is.na(add_note)) {  
    if (!('note_modeler' %in% names(dt))) {
      dt[, note_modeler := '']
    }
    
    dt[, note_modeler := ifelse(note_modeler=='', add_note, 
                                paste(note_modeler, add_note, sep = ' | '))]
  }
  
  return(dt)
}
