## HELPER FUNCTION TO ROUND TO NEAREST AGE START/AGE END IN GBD-LAND

get_closest_age <- function(start = T, age, age_dt) {
  # pull gbd age group mapping
  age_start_list <- unique(age_dt$gbd_age_start)
  age_end_list <- unique(age_dt$gbd_age_end)
  if (start == T) {
    index <- which.min(abs(age_start_list - age))
    gbd_age <- age_start_list[index]
  } else if (start == F) {
    index <- which.min(abs(age_end_list - age))
    gbd_age <- age_end_list[index]
  }
  return(gbd_age)
}