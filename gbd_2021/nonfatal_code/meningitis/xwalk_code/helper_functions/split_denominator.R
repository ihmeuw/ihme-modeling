# splits denominator using location-specific population distribution
# most detailed argument - should you split to most detailed ages for GBD 2020?
# T = yes
# F = no, split to GBD 2019 age groups
# F is used for "step2" causes and T is used for "on rotation" causes.

split_denominator <- function(raw_dt, age_dt, pop_dt, most_detailed = F) {
  under1 <- age_dt[gbd_age_start<1]$age_group_id
  between_1_and_5 <- age_dt[gbd_age_start>=1 & gbd_age_start<5]$age_group_id
  over80 <- age_dt[gbd_age_start>=80]$age_group_id
  # add custom age group id 0 for years 0 to 1
  # same age group splits for all causes, no custom age groups. 
  if (most_detailed == F){
    # also add custom age group id 80 to 95 +
    my_age_dt <- rbind(age_dt,
                       data.table(age_group_id = 0, gbd_age_start = 0, gbd_age_end = 1),
                       data.table(age_group_id = 5, gbd_age_start = 1, gbd_age_end = 5),
                       data.table(age_group_id = 21, gbd_age_start = 80, gbd_age_end = 125))
    # remove unused age_group_ids
    my_age_dt <- my_age_dt[!age_group_id %in% c(under1, between_1_and_5, over80)]
  } else if (most_detailed == T){
    my_age_dt <- rbind(age_dt,
                       data.table(age_group_id = 21, gbd_age_start = 80, gbd_age_end = 125))
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
  
  row_location_id <- raw_dt[, location_id]
  row_year_start <- raw_dt[, year_start]
  row_year_end <- raw_dt[, year_end]
  # use population from year_match (midway between start and end)
  if(!"year_match" %in% names(raw_dt)) raw_dt[,year_match := year_floor]
  row_year_id <- raw_dt[, year_match] 
  row_sex_id <- ifelse(raw_dt[, sex] == "Male", 1, 2)

  # for all ages of interest within the location/sex/year
  pop <- pop_dt[age_group_id %in% row_age_group_ids &
                  location_id == row_location_id &
                  year_id == row_year_id &
                  sex_id == row_sex_id]
  if (most_detailed == F){
    # also pull under5 and above 85 if applicable
    if (raw_dt$age_start == 0) {
      pop2 <- pop_dt[age_group_id %in% c(2,3,388,389,238,34) &
                       location_id == row_location_id &
                       year_id == row_year_id &
                       sex_id == row_sex_id]
    } else {pop2 <- data.table()}
    pop <- rbind(pop, pop2)
    pop[age_group_id %in% under1, age_group_id:= 0] # sum under 1 age groups together
    pop[age_group_id %in% between_1_and_5, age_group_id:= 5] # sum 1-5 age groups together
  }
  # pop[age_group_id %in% over80, age_group_id:= 21] # sum 80 - 95 + age groups together
  pop <- pop[, .(population = sum(population)), by = "age_group_id"]
  total_pop <- sum(pop$population)
  pop[, age_percent:= population / total_pop ]
  
  row_sample_size <- raw_dt[, sample_size]
  pop[, age_split_sample_size:= age_percent * row_sample_size]
  raw_dt$match_key <- 1
  pop$match_key <- 1
  age_split_dt <- merge(raw_dt, 
                        pop[, .(age_group_id, age_split_sample_size, match_key)], 
                        by ="match_key", 
                        allow.cartesian = T)
  
  cols.remove <- c("match_key", "gbd_age_start", "gbd_age_end")
  age_split_dt[, (cols.remove):= NULL]
  age_split_dt <- merge(age_split_dt, my_age_dt, by = "age_group_id")
  if(!all(is.na(age_split_dt$seq))) {
    age_split_dt$crosswalk_parent_seq <- age_split_dt$seq
  } # else age_split_dt$crosswalk_parent_seq leave it alone
  return(age_split_dt)
}
