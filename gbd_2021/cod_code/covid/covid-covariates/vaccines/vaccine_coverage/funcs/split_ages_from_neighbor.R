
split_ages_from_neighbor <- function(loc_x, # data frame of all age location to be split into age groups
                                     loc_y, # data frame of age stratified neighbor
                                     observed_age,
                                     model_inputs_root,
                                     hierarchy,
                                     vax_quantities=NULL
) {
  
  if (!(loc_y %in% observed_age$location_id)) stop('loc_y not in observed_age')
  if(is.null(vax_quantities)) vax_quantities <- colnames(observed_age)[which(colnames(observed_age) %like% 'vaccinated' | colnames(observed_age) %like% 'administered')]
  
  message(glue('Inferring age-stratified data for [{hierarchy$location_name[hierarchy$location_id == loc_x]}] using observed proportions in [{hierarchy$location_name[hierarchy$location_id == loc_y]}]'))
  
  # Get age-stratified data from template location (loc_y)
  observed_age_y <- as.data.frame(observed_age[observed_age$location_id == loc_y,])
  
  # Tally populations in appropriate age groups
  pop <- .get_age_group_populations(age_starts=unique(observed_age_y$age_start),
                                    model_inputs_path=model_inputs_root,
                                    hierarchy=hierarchy,
                                    include_all_ages=T)
  
  pop_x <- pop[pop$location_id == loc_x,]
  pop_y <- pop[pop$location_id == loc_y,]
  
  # Calculate population proportions for each age group in template location
  observed_age_y <- merge(observed_age_y, pop_y, by=c('location_id', 'age_start', 'age_end', 'age_group'))
  for (v in vax_quantities) observed_age_y[,v] <- observed_age_y[,v] / observed_age_y[,'population']
  
  # Calculate counts in inferred location using loc_x population sizes
  out <- observed_age_y[,!(colnames(observed_age_y) == 'population')]
  out$location_id <- loc_x
  out$location_name <- hierarchy$location_name[hierarchy$location_id == loc_x]
  
  out <- merge(out, pop_x, by=c('location_id', 'age_start', 'age_end', 'age_group'))
  for (v in vax_quantities) out[,v] <- out[,v] * out[,'population']
  
  # Clean up
  
  out <- out[,!(colnames(out) == 'population')]
  out$observed <- 0
  
  if (F) {
    plot_raw_data(observed_age_y)
    plot_raw_data(out)
  }
  
  return(out)
}