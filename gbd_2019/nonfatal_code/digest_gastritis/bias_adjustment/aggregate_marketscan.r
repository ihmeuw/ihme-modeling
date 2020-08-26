# Aggregate Marketscan data to national level
# Written by Emma Nichols, modified by Jaimie Adelson, then Mae Dirac

aggregate_marketscan <- function(mark_dt){
  dt <- copy(mark_dt)
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  dt[, `:=` (location_id = 102, location_name = "United States", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  dt[measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  dt <- unique(dt, by = by_vars)
  
  return(dt)
}

