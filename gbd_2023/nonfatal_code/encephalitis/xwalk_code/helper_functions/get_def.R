## GET DEFINITIONS (ALL BASED ON CV'S - WHERE ALL 0'S IS REFERENCE)

get_def <- function(ref_dt, ref_name, alt_names){
  dt <- copy(ref_dt)
  dt[, def := ""]
  for (alt in alt_names){
    dt[get(alt) == 1, def := paste0(def, "_", alt)]
  }
  dt[get(ref_name) == 1, def := "reference"]
  return(dt)
}