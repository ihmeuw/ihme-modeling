library(DescTools)

age_split_data <- function(dt, row, age_pattern, pops, age_meta, 
                           description = "age split off DisMod", width = 20) {

  split_row <- copy(dt)[row, ]
  split_row[sex == "Male", sex_id := 1
            ][sex == "Female", sex_id := 2
              ][sex == "Both", sex_id := 3]
  
  split_row[is.na(mean), mean := cases / sample_size]
  
  if (is.na(split_row$upper)) {
    row_ci <- BinomCI(split_row$mean * split_row$sample_size, 
                      split_row$sample_size )
    split_row[, lower := row_ci[2]]
    split_row[, upper := row_ci[3]]
  }
  
  is_one <- split_row$mean == 1
  split_row[, refOdds := ifelse(mean == 1, 1, mean / (1 - mean))]
  
  ages <- age_meta[age_group_years_start < split_row$age_end & 
                     age_group_years_end > split_row$age_start, age_group_id]

  pops <- pops[location_id == split_row[, location_id] & 
                 year_id == split_row[, gbd_year] & 
                 sex_id == split_row[, sex_id] & 
                 age_group_id %in% ages,]
  
  age_pattern <- age_pattern[location_id == split_row[, pattern_loc_id] & 
                               year_id == split_row[, gbd_year] & 
                               sex_id == split_row[, sex_id] & 
                               age_group_id %in% ages,]
  age_pattern[, location_id := NULL]
  
  age_pattern <- merge(age_pattern, pops, 
                       by = c("year_id", "sex_id", "age_group_id"), all.x = T)
  age_pattern <- merge(age_pattern, age_meta, 
                       by = c("age_group_id"), all.x = T)
  setDT(age_pattern)
  setnames(age_pattern, 
           c("age_group_years_start", "age_group_years_end"), 
           c("age_start", "age_end"))
  
  age_pattern <- age_pattern[, .(mean, population, age_start, age_end)]
  age_pattern[, age_cat := floor(age_start/width)*width]
  age_pattern[, `:=` (age_start = min(age_start), age_end = max(age_end)), 
              by = age_cat]
  age_pattern[, mean := weighted.mean(mean, w = population), by = age_cat]
  age_pattern[, population := sum(population), by = age_cat]
  age_pattern <- unique(age_pattern)
  
  
  if (is_one) {
    age_pattern[, odds := 1]
  } else {
    age_pattern[, odds := mean/(1-mean)]
  }
  
  age_pattern[, meanOdds := weighted.mean(odds, w = population)]
  
  age_split_row <- age_pattern[, .(odds, meanOdds, age_start, age_end, 
                                   population)][, merge := 1]
  split_row[, c("cases", "sample_size", "age_start", "age_end") := NULL
            ][, merge := 1]
  split_row <- merge(split_row, age_split_row, by = "merge", all = T
                     )[, merge := NULL]
  
  split_row[, oddsOfMean := odds * refOdds / meanOdds]
  split_row[, mean := ifelse(mean==1, 1, oddsOfMean / (1 + oddsOfMean))]
  
  split_row[!is.na(lower), lower := mean - sqrt((sum(population)/population) * 
                                                  ((mean - lower)/qnorm(0.975))^2) * 
              qnorm(0.975)]
  split_row[!is.na(lower) & (lower < 0 | mean == 0), lower := 0]
  split_row[!is.na(upper), upper := mean + sqrt((sum(population)/population) * 
                                                  ((upper - mean)/qnorm(0.975))^2) * 
              qnorm(0.975)]
  split_row[!is.na(upper) & (upper > 1 | mean == 1), upper := 1]
  
  split_row[!is.na(standard_error), 
            standard_error := sqrt((split_row[, standard_error]^2) * 
                                     sum(population)/population)]
  split_row[!is.na(standard_error) & standard_error>1, standard_error := 1]
  
  split_row <- split_row[, c("odds", "oddsOfMean", "refOdds", "meanOdds", "population") := NULL]
  

  if (!('note_modeler' %in% names(split_row))) split_row[, note_modeler := '']
  split_row[, note_modeler := paste(note_modeler, description, sep = ' | ')]
  split_row[!is.na(seq), crosswalk_parent_seq := seq]
  split_row[, seq := NA]
  
  return(split_row)
}
