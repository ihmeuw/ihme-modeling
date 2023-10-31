## input : sex-split data table
## output : data table with 0 SE rows edited, also fix upper & lower
## method : Wolfson score method, using sample_size as effective_sample_size
## source: https://stash.ihme.washington.edu/projects/CC/repos/elmo/browse/elmo/uncertainty.py#226-295

## edit for INCIDENCE data, previous formula was only correct for proprtions

fix_zero_se <- function(data){
  z <- 1.96
  data[is.na(cases), cases := mean*sample_size]
  if ("proportion" %in% unique(data$measure)){
    data_prop <- data[measure == "proportion"]
    data_prop[standard_error == 0, standard_error := sqrt(mean * (1-mean) / sample_size + z^2 / (4*sample_size^2))]
    data_prop[standard_error == 0, lower := 0] # can't be negative 
    data_prop[standard_error == 0, upper := 1 / (1 + z**2 / sample_size) * (mean + z**2 / (2 * sample_size) + z * standard_error)]
  } else {
    data_prop <- data.table()
  }
  if (any(unique(data$measure) != "proportion")){
    data_other <- data[measure != "proportion"]
    data_other[standard_error == 0 & cases < 5, standard_error := ((5 - mean * sample_size) / sample_size + mean * sample_size * sqrt(5 / sample_size**2)) / 5]
    data_other[standard_error == 0 & cases > 5, standard_error := sqrt(mean/sample_size)]
  } else {
    data_other <- data.table()
  }
  data <- rbind(data_prop, data_other)
  return(data)
}





