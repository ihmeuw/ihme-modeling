#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Back-cast education quality estimates to fill in all age groups
#***********************************************************************************************************************


#----FUNCTION-----------------------------------------------------------------------------------------------------------
### function to back-calculate EQ for all ages and years based on 1970-year_end estimates of standardized test scores for 10-14 year-olds
backcast_edu_quality <- function(version, year_start=1990, age_group_start=7, save=FALSE) {
  
  ### read data location files (draws)
  gpr_file <- file.path("/share/covariates/ubcov/model/output", version, "draws_temp_1", csv_name)
  edu_q <- gpr_file %>% fread
  edu_q[, measure_id := NULL]
  
  ### save draws and quantiles for publication
  if (save) {
    write.csv(edu_q, file.path(eq_pub_path, "draws", csv_name), row.names=FALSE)
    write.csv(collapse_point(input_file=edu_q, draws_name="draw"), file.path(eq_pub_path, "quantiles", csv_name), row.names=FALSE)
  }
  
  ### keep just age_group 10-14
  edu_q <- edu_q[age_group_id==age_group_start, ]
  
  ### back-cast learning to 1880 assuming same score as observed in most recent available model estimate
  oldest_year <- min(edu_q$year_id)
  edu_q_back <- merge(x=data.frame(copy(edu_q[year_id==oldest_year])[, year_id := NULL]), y=data.frame(year_id=1880:(oldest_year - 1)), by=NULL) %>% as.data.table
  edu_q <- rbind(edu_q, edu_q_back)
  
  ### back-calculate edu_q estimate for older age groups using 10-14 scores (age_group_start can be changed)
  age_list <- c((age_group_start + 1):20, 30:32, 235)
  age_map <- data.table(age_group_id=age_list, age_seq=seq(1, length(age_list)))
  # prep data table
  edu_q_all <- NULL %>% as.data.table
  for (age_group in age_list) {
    # propagate ages through times series
    edu_q_back <- copy(edu_q)
    edu_q_back[, age_group_id := age_group]
    edu_q_back[, year_id := year_id + (5 * age_map[age_group_id==age_group, age_seq])]
    edu_q_back <- edu_q_back[year_id >= year_start & year_id <= year_end, ]
    # save
    edu_q_all <- rbind(edu_q_all, edu_q_back)
  }
  
  ### save location csv for all ages and all years
  if (save) {
    write.csv(edu_q_all, file.path(eq_out_path, "draws", csv_name), row.names=FALSE)
    ### save quantiles
    edu_q_all_quantiles <- collapse_point(input_file=edu_q_all, draws_name="draw")
    write.csv(edu_q_all_quantiles, file.path(eq_out_path, "quantiles", csv_name), row.names=FALSE)
  }
  
}
#***********************************************************************************************************************