### TYPHOID ONLY ADJUSTMENT ---------------------------------------------------------------------------

typh_only_adjust <- function(dt, row, age_pattern, pops) {
  split_row <- dt[row,]  
  
  split_row[sex == "Male", sex_id := 1
            ][sex == "Female", sex_id := 2
              ][sex == "Both", sex_id := 3]
  
  if (split_row[, sex_id] == 3) {
    sex_pull <- 1:2
  } else {
    sex_pull <- split_row[, sex_id]
  }
  
  ages <- age_meta[age_group_years_start < split_row$age_end & 
                     age_group_years_end > split_row$age_start, age_group_id]
  
  pops <- pops[location_id == split_row[, location_id] & 
                 year_id==split_row[, gbd_year] & 
                 sex_id %in% sex_pull & age_group_id %in% ages,]
  
  inc <- age_pattern[location_id == split_row[, location_id] & 
                       year_id == split_row[, gbd_year] & 
                       sex_id %in% sex_pull & age_group_id %in% ages,]
  
  inc <- merge(inc, pops, 
               by = c("location_id", "year_id", "sex_id", "age_group_id"), 
               all.x = T)
  setDT(inc)
  
  prop_draws <- as.numeric(inc[, lapply(.SD, function(x) {
    1 - weighted.mean(x, w = population)}), .SDcols = paste0("draw_", 0:999)])
  
  u <- split_row[, mean]
  s <- split_row[, standard_error]
  
  a <- u * (u - u^2 - s^2) / s^2 
  b  <- a * (1 -u) / u
  
  mean_draws <- rbeta(1000, a, b)
  
  adjMean_draws <- mean_draws / prop_draws
  adjMean_draws <- adjMean_draws * (u/mean(prop_draws)) / mean(adjMean_draws)

  split_row[mean>0, `:=` (mean = mean(adjMean_draws), standard_error = sd(adjMean_draws))]
  split_row[, `:=` (note_modeler = paste(note_modeler, "typhoid-only adjustment off DisMod proportion model"))]
  
  return(split_row)
}

                