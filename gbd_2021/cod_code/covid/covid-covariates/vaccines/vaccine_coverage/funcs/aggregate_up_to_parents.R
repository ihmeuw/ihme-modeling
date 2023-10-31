#' @name aggregate_up_to_parents 
#' @description Aggregates child values up to a parent
#' 
#' @param dt [data.frame] Data table to operate on. It must have the columns 'parent_id', 'date', and agg_cols. 
#' @param hierarchy [data.frame] Hierarchy to aggregate up. 
#' It must have the columns "location_id","location_name","parent_id","region_name","level","most_detailed". 
#' @param interpolate [logical] Do you want to interpolate missing child values? 
#' @param agg_cols [character] Vector of columns to aggregate values for
#' @param interp_cols [vector(character)] Names of columns to interpolate. Defaults are for vaccine_projections_functions.R, only place used
#' 
#' @return Data frame with values aggregated up hierarchy.
aggregate_up_to_parents <- function(
  dt,
  hierarchy,
  interpolate,
  agg_cols,
  interp_cols = c("reported_vaccinations", "people_vaccinated", "fully_vaccinated")
){
  # AGGREGATION LOOPS up to NATIONAL LEVEL - replaces prior code that treated nations and WA state separately, they are now combined
  # If the desired level to aggregate UP TO changes, adjust the two lines below
  
  # REQUIREMENT: Data must be uniquely identified by location_id and date at this point 
  if (nrow(dt) != nrow(unique(dt[, .(location_id, date)]))){
    stop("Data must be uniquely identified by location_id and date!")
  }
  
  levels_rev <- sort(unique(hierarchy$level), decr = T) # reverse hierarchy levels for bottom-up aggregation
  levels_rev <- seq(max(levels_rev), 4, by = -1)        # aggregating only levels x:4, up to country level (3)
  
  for (l in levels_rev){
    
    # Outer loop: for each hierarchy level, starting at leaf nodes and going up
    # to a pre-specified level (3, counties), aggregate all children to their
    # parent level, then repeat and roll those up to the next parent level
    
    parents_of_level <- hierarchy[level == l - 1 & most_detailed == 0, location_id]
    
    for (i in parents_of_level){
      
      # Inner loop: Find children, aggregate selected columns for all children
      # of one parent location, reset the location_id to the parent_id, perform
      # some weighting calculations, then merge back on the temp data.frame
      
      children_locs <- hierarchy[parent_id == i, location_id]
      
      if (length(children_locs) > 0){
        part <- dt[location_id %in% children_locs]
        for (col in agg_cols){
          if (any(is.na(part[, get(col)]))){
            # Hugely missing (488444 rows with !complete.cases() in make_scenario_priority) - changing 'error' to 'warning'
            warning(sprintf("There cannot be NA values in leaf nodes! Found NA for agg_col %s with parent ID %i.", col, i))
          }
        }
        if (interpolate) part <- .fix_aggregates(
          agg_dt = part,
          interp_cols = interp_cols
        )
        part <- part[, lapply(.SD, function(x) sum(x)), by=c("parent_id", "date"), .SDcols = agg_cols] # In the future, you might want to make these by-vars dynamic, because if you 
        # have any other stratification, you will lose if for some location IDS (specifically, parents of children).
        setnames(part, "parent_id", "location_id")
        
        part <- merge(part, 
                      hierarchy[,c("location_id","location_name","parent_id","region_name","level","most_detailed")],
                      by = "location_id",
                      all.x = TRUE)
        
        dt <- rbind(dt[!location_id == unique(part$location_id)], part, fill = T)
      } 
      
    }
  }
  
  # Diagnostic check
  # loc <- 6 # should now be able to see China
  # dt %>% 
  #   filter(location_id == loc) %>% 
  #   select(plot_cols) %>% 
  #   tidyr::pivot_longer(plot_cols_piv) %>% 
  #   ggplot(aes(x=date, y=value, color=name)) +
  #   geom_line() +
  #   # facet_wrap(~name, scales = 'free') +
  #   theme_minimal() +
  #   ggtitle(unique(dt[location_id==loc, location_name]))
  
  return(dt)
  
}