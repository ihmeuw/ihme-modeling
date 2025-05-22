# This function takes a ggplot and adds red circles around
# points which were not present in the GBD 2019 data

add_data_history <- function(plot_data,
                             plot,
                             prev_round_census_nids) {
  data_sources <- plot_data[source == "data" &
                              detailed_source != "backwards ccmpp" &
                              outlier_type == "not outliered"]
  data_sources[prev_round_census_nids, new_source := FALSE, on = c("nid", "underlying_nid")]
  data_sources[is.na(new_source), new_source := TRUE]
  
  # add red circles around data points which are new
  new_sources <- data_sources[new_source == TRUE]
  
  p <- p +
    geom_point(data = new_sources, aes(x = year_id, y = mean), shape = 1, color = "red", inherit.aes = FALSE, size = 6)
  
  return(p)
}