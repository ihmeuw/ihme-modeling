# GET CASES FROM SAMPLE SIZE & MEAN OR VICE VERSA 

get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(sample_size) & !is.na(effective_sample_size), sample_size:= effective_sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[!is.na(cases) & is.na(sample_size), sample_size := cases / mean]
  return(dt)
}