# Helper functions for custom generating flu from LRI
#inputs: new value, old value
#outputs: percent change
pct_change <- function(new, old) {
  pct = (new-old)/abs(old)
  return(pct)
}

#inputs: data table, columns named as draw_0:999 required
#outputs: 95% confidence intervals and mean value
confidence_intervals <- function(dt, n_draws) {
  end <- n_draws-1
  summaries <- dt[, lower := apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = (paste0("draw_", 0:end))]
  summaries <- dt[, mean  := rowMeans(.SD), .SDcols = (paste0("draw_", 0:end))]
  summaries <- dt[, upper := apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = (paste0("draw_", 0:end))]
  
  clean_summary <- copy(summaries[, paste0("draw_",0:end):=NULL])
  return(clean_summary)
}

# inputs: proportions = draw-wise data table of proportions, overall_nums = draw-wise data table of overall counts or rate
# outputs: rate or count by etiology
get_counts <- function(proportions, overall_nums, n_draws) {
  end <- n_draws-1
  #change draw names for consistency
  setnames(proportions,old=paste0("draw_",0:end), new=paste0("prop_",0:end))
  setnames(overall_nums,old=paste0("draw_",0:end), new=paste0("ov_",0:end))
  combined <- merge(proportions, overall_nums, 
                    by = c("location_id", "age_group_id", "sex_id", "year_id", "measure_id"))
  #calculate counts by etiology
  combined <- combined[, paste0("draw_",0:end) := lapply(0:end, function(x) {get(paste0("prop_", x)) * get(paste0("ov_",x))})]
  combined <- combined[,c(paste0("prop_",0:end), paste0("ov_",0:end)):=NULL]
  return(combined)
}

neaten <- function(dt, rei_map, loc_map) {
  dt <- merge(dt, rei_map[,.(rei_id, rei_name)], by = "rei_id")
  dt <- merge(dt, loc_map[,.(location_name, location_id)], by = "location_id")
  if ("age_group_id" %in% names(dt) & !all(dt$age_group_id == 22)) dt <- merge(dt, age_meta[,.(age_group_name, age_group_id)], by = "age_group_id")
  return(dt)
}