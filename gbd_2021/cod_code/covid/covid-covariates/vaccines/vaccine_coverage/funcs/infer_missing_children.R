
# Impute missing most-detailed locations that are missing by dis-aggregating parent locations
infer_missing_children <- function(object,             # A data.table with at least location_id and columns to disaggregate. Function with loop through age_group if it exists
                                   objective_cols,     # A vector of the column names to disaggregate
                                   population,         # data.table of population sizes produced by the .make_age_group_populations function
                                   hierarchy,          # data.table of spatial hierarchy
                                   verbose=FALSE       # Logical indicating whether to print info in each loop
) {
  
  object <- as.data.table(object)
  no_age <- !('age_group' %in% colnames(object))
  if (no_age) object$age_group <- 'NA'
  
  # Get most detailed that are missing
  most_detailed <- hierarchy[parent_id %in% object$location_id & most_detailed == 1, .(location_id, location_name, parent_id)]
  missing <- most_detailed[!(most_detailed$location_id %in% object$location_id)]
  
  # Get the parent locations of the missing most detailed locations
  
  #national_parents <- unique(hierarchy[level == 4, parent_id])
  all_parents <- unique(hierarchy$parent_id)
  parents_of_missing <- all_parents[all_parents %in% missing$parent_id]
  
  if (verbose) {
    msg <- paste('parent', 'child', 'age_group', 'quantity', sep=' | ')
    message(msg)  
    message(rep('-', nchar(msg)))
  }
  
  out <- data.table()
  for (i in parents_of_missing) {
    
    children <- hierarchy[parent_id == i, location_id]
    
    for (j in children) {
      for (k in unique(object$age_group)) {
        
        tmp <- object[location_id == i & age_group == k,]
        
        # Population sizes of parent and child locations (parent location populations are the sum of all children)
        pop_parent <- population[population$location_id == i & population$age_group == k, 'population']
        pop_child <- population[population$location_id == j & population$age_group == k, 'population']
        
        for (l in objective_cols) {
          
          if (verbose) message(paste(i, j, k, l, sep = ' | '))
          sel <- which(colnames(tmp) == l) # Index of current objective column
          tmp[,sel] <- tmp[,..sel] * (pop_child/pop_parent) # Calculate population weighted value
          
        }
        
        # Update any spatial information
        spatial_cols <- colnames(object)[colnames(object) %in% colnames(hierarchy)]
        tmp <- as.data.frame(tmp)
        
        for (s in spatial_cols) {
          sel <- which(hierarchy[,..s] == j)
          tmp[,which(colnames(tmp) == s)] <- hierarchy[sel, ..s]
        }
        
        out <- rbind(out, tmp)
      }
    }
  }
  
  if (any(is.na(out$location_name))) {
    
    out <- merge(out[,location_name := NULL], hierarchy[,.(location_id, location_name)], by='location_id', all.x=T)
    
  }
  
  if (no_age) out[,age_group := NULL]
  return(out) 
}
