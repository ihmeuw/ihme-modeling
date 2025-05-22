
# RETURNS TRUE IF CASES AND SAMPLE SIZE ARE AGE-SPLIT CORRECTLY AND FALSE OW
bool_check_age_splits <- function(dt) {
  age_split_dt <- dt[, .(total_age_split_cases = sum(age_split_cases), 
                         total_age_split_sample_size = sum(age_split_sample_size)),
                     by = c("crosswalk_parent_seq", "sex")]
  original_dt <- unique(dt[, .(crosswalk_parent_seq, sex, cases, sample_size)])
  age_split_dt <- merge(age_split_dt, original_dt, by = c("crosswalk_parent_seq", "sex"))
  check_agg <- nrow(age_split_dt[abs(total_age_split_cases - cases) > 1]) == 0 & 
    nrow(age_split_dt[abs(total_age_split_sample_size - sample_size) > 1]) == 0
  check_age_ranges <- nrow(dt[gbd_age_end - gbd_age_start > 45]) == 0
  return(check_agg & check_age_ranges)
}
