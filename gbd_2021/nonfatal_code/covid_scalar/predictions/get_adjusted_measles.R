# Function that will multiply the caculated ratio (from spline cascade or RegMod)
# by the "expected" (GBD counterfactual) measles estimates to get adjusted all-age, both-sex case counts

get_adjusted_measles <- function(expected_dir, ratio_dt, years, time, seas_path, end = 999){
  custom_nf_results <- fread(expected_dir)
  expected <- custom_nf_results[year_id %in% years & location_id %in% unique(ratio_dt$location_id)]
  if (time == "monthly"){
    # split expected out to monthly
    seas <- as.data.table(read.xlsx(seas_path))
    expected <- get_monthly(expected, seas, drawnames = names(expected)[names(expected)%like%"draw_"])
  }
  if (time == "monthly") merge_cols <- c("location_id", "year_id", "month") else merge_cols <- c("location_id", "year_id")
  expected_counts <- merge(expected, ratio_dt, by=merge_cols)
  # DO NOT duplicate for subnationals OR age/sex since we are in count space
  expected_counts <- expected_counts[, paste0("inc_draw_",0:end) := lapply(0:end, function(x) {get(paste0("ratio_draw_", x)) * get(paste0("case_draw_",x))})]
  expected_counts <- expected_counts[,c(paste0("ratio_draw_",0:end), paste0("case_draw_",0:end)):=NULL]
  return(expected_counts)
}