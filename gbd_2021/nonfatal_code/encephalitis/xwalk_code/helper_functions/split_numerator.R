
# splits numerator using global age pattern seen in clinical data
split_numerator <- function(raw_dt, age_dt, pattern_dt, most_detailed = F) {
  under1 <- age_dt[gbd_age_start<1]$age_group_id
  between_1_and_5 <- age_dt[gbd_age_start>=1 & gbd_age_start<5]$age_group_id
  over80 <- age_dt[gbd_age_start>=80]$age_group_id
  if (most_detailed == F){
    # add custom age group id 0 for years 0 to 1
    # also add custom age group id 80 to 95 +
    my_age_dt <- rbind(age_dt, 
                       data.table(age_group_id = 0, gbd_age_start = 0, gbd_age_end = 1),
                       data.table(age_group_id = 5, gbd_age_start = 1, gbd_age_end = 5),
                       data.table(age_group_id = 21, gbd_age_start = 80, gbd_age_end = 125))
    # remove unused age_group_ids
    my_age_dt <- my_age_dt[!age_group_id %in% c(under1, between_1_and_5, over80)]
  } else if (most_detailed == T){
    my_age_dt <- rbind(age_dt, data.table(age_group_id = 21, gbd_age_start = 80, gbd_age_end = 125))
    my_age_dt <- my_age_dt[!age_group_id %in% over80]
  }

  setorder(my_age_dt, "gbd_age_start")
  age_group_ids <- my_age_dt$age_group_id
  
  age_start_list <- sort(my_age_dt$gbd_age_start)
  age_end_list <- sort(my_age_dt$gbd_age_end)

  # find closest age_group_id to starting age_group_id
  row_age_group_id_start <- raw_dt[, age_group_id_start]
  if (!row_age_group_id_start %in% age_group_ids) {
    index <- which.min(abs(age_start_list - raw_dt$age_start))
    row_age_start <- age_start_list[index]
    row_age_group_id_start <- my_age_dt[gbd_age_start == row_age_start]$age_group_id
  }
  
  # find closest age_group_id to ending age_group_id
  row_age_group_id_end <- raw_dt[, age_group_id_end]
  if (!row_age_group_id_end %in% age_group_ids){
    # for 85 and younger, this will work
    if (raw_dt$age_end <= 85){
      index2 <- which.min(abs(age_end_list - raw_dt$age_end))
      row_age_end <- age_end_list[index2]
      row_age_group_id_end <- my_age_dt[gbd_age_end == row_age_end]$age_group_id
    } # need an exception for 85 and up which may not round up to 125 
    else if (raw_dt$age_end > 85){
      row_age_group_id_end <- 21
    }
  }

  # this only works if age_group_ids are SORTED by order of AGE 
  row_age_group_ids <- age_group_ids[which(row_age_group_id_start == age_group_ids):which(row_age_group_id_end == age_group_ids)]
  
  pattern_dt <- pattern_dt[age_group_id %in% row_age_group_ids]
  total_cases <- sum(pattern_dt$cases)
  pattern_dt[, age_percent:= cases / total_cases]
  row_age_group_id <- raw_dt[, age_group_id]
  age_percent <- pattern_dt[age_group_id == row_age_group_id, age_percent]
  raw_dt[, age_split_cases:= age_percent * cases]
  return(raw_dt)
}
