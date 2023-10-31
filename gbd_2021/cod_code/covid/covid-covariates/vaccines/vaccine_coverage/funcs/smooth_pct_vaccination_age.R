
smooth_pct_vaccination_age <- function(vaccine_output_root, 
                                       hierarchy,
                                       population,
                                       time_varying_bias=FALSE
) {
  
  .print_header_message('Modeling bias-corrected vaccination coverage by age')
  .t <- Sys.time()
  
  # Get global args
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_path <- model_parameters$model_inputs_path
  
  age_starts <- model_parameters$age_starts
  include_all_ages <- model_parameters$include_all_ages
  n_cores <- model_parameters$n_cores
  
  age_groups <- .get_age_groups(age_starts)
  all_age_group <- paste0(age_starts[1], '-', 125)
  
  
  #-----------------------------------------------------------------------------
  # Load data
  #-----------------------------------------------------------------------------
  message('Loading data and models...')

  # Get Facebook vaccination coverage data. Produced by smooth_vaccine_hesitancy_age()
  fb <- fread(file.path(vaccine_output_root, 'observed_survey_data_age.csv'))
  fb$date <- as.Date(fb$date)
  
  # Get observed NON-age-stratified vaccination coverage data
  observed_all_age <- vaccine_data$load_observed_vaccinations(vaccine_output_root)
  observed_all_age$date <- as.Date(observed_all_age$date)
  
  # Load modeled vaccination coverage from all-age vaccination data
  mods_all_age <- fread(file.path(vaccine_output_root, "smooth_pct_vaccinated.csv"))
  mods_all_age$date <- as.Date(mods_all_age$date)
  
  # Get observed vaccination coverage by age from (CDC)
  observed_age <- fread(file.path(vaccine_output_root, "observed_vaccination_data_age.csv"))
  observed_age$date <- as.Date(observed_age$date)
  
  dups <- unique(observed_age[duplicated(observed_age[,.(location_id, date, age_group)]), location_id])
  if (length(dups) > 0) stop(glue('There are duplicate locations in observed_age: {dups}'))
  
  # Load modeled vaccination coverage by age (using FB survey data)
  mods_age <- fread(file.path(vaccine_output_root, "smooth_pct_vaccinated_age.csv"))
  mods_age$date <- as.Date(mods_age$date)
  
  dups <- unique(mods_age[duplicated(mods_age[,.(location_id, date, age_group)]), location_id])
  if (length(dups) > 0) stop(glue('There are duplicate locations in mods_age: {dups}'))
  
  mods_age <- merge(mods_age, population[,c('location_id', 'age_group', 'population')], by=c('location_id', 'age_group'), all.x=T)
  
  observed_all_age <- merge(observed_all_age[,.(location_id, date, people_vaccinated)], 
                            population[population$age_group == all_age_group,], 
                            by=c('location_id'))
  
  observed_all_age$pct_vaccinated <- observed_all_age$people_vaccinated/observed_all_age$population
  
  
  # Merge age group populations
  population <- population[,!colnames(population) %in% c('age_start', 'age_end')]
  observed_age <- merge(observed_age, population, by=c('location_id', 'age_group'))
  
  observed_age$pct_vaccinated <- observed_age$initially_vaccinated/observed_age$population
  #observed_age$pct_vaccinated <- .do_manual_bounds_proportion(observed_age$pct_vaccinated) # Lots of out-of-bounds: need to check source for population sizes
  
  fb$pct_vaccinated <- fb$vaccinated/fb$total_respondents_vaccination
  fb$pct_vaccinated[fb$total_respondents_vaccination == 0] <- 0
  fb$pct_vaccinated[is.nan(fb$pct_vaccinated)] <- NA
  fb$pct_vaccinated <- .do_manual_bounds_proportion(fb$pct_vaccinated)
  
  
  observed_bias <- fread(file.path(vaccine_output_root, 'observed_survey_bias_age.csv'))
  mods_bias <- fread(file.path(vaccine_output_root, 'smooth_survey_bias_age.csv'))
  
  
  
  #test <- mods_age[location_id == 218 & age_group == '12-17',]
  #plot(test$date, test$smooth_pct_vaccinated)
  
  #sel <- c(1, 31, 32, 45, 570, 218, 71, 11, 92, 86)
  #ggplot() +
  #geom_point(data = fb[fb$location_id %in% sel,], 
  #           aes(x=date, y=pct_vaccinated)) +
  #  geom_line(data = mods_age[mods_age$location_id %in% sel,], 
  #            aes(x=date, y=smooth_pct_vaccinated), col='red') +
  #  facet_grid(rows=vars(age_group), cols=vars(location_name))
  
  
  
  #---------------------------------------------------------
  # Perform bias correction for all locations and age groups 
  #---------------------------------------------------------
  
  .print_header_message('Applying bias corrections')
  
  # Could be helpful to split these out and list by super-region to see how the model will substitute things...
  method_1_locs <- unique(observed_age$location_name)
  message(glue('Method 1 will be applied the following {length(method_1_locs)} locations which have sufficient age-stratified data:'))
  message(paste(method_1_locs, collapse=' | '))
  
  tmp <- unique(mods_age$age_group)
  age_groups_order <- c(all_age_group, tmp[!(tmp == all_age_group)]) # All ages must be first for method 2
  merge_cols <- c('location_id', 'date', 'age_group')
  
  
  # Setup parallel environments
  export_packages <- c('mgcv', 'scam', 'zoo', 'R.utils', 'glue', 'data.table')
  export_objects <- c('.get_column_val',
                      '.make_cumulative',
                      '.extend_end_values',
                      '.do_sqrt_interp',
                      '.fit_scam',
                      '.fit_time_dependent_bias',
                      '.prior_line_MR_BRT')
  
  bind_func <- function(a, b) rbind(a, b, fill=TRUE)
  
  cl <- parallel::makeCluster(n_cores) # Cannot figure out how to suppress the verbose messages
  doParallel::registerDoParallel(cl)
  
  
  mods_bias_corrected <- 
    foreach(i = hierarchy[level >= 3, location_id], .combine=bind_func) %:%
    foreach(j = unique(mods_age$age_group),  .combine=bind_func, .verbose = FALSE,
            .export = export_objects, .packages = export_packages) %dopar% {
              
              #message(i, j)
              #tryCatch( { 
              
              tmp_observed_all_age <- observed_all_age[observed_all_age$location_id == i,] 
              setnames(tmp_observed_all_age, 'people_vaccinated', 'initially_vaccinated') # Must agree with age-stratified nomenclature
              
              tmp_mod_age <- mods_age[mods_age$location_id == i & mods_age$age_group == j,]
              tmp_mod_all_age <- mods_all_age[mods_all_age$location_id == i,]
              tmp_bias <- mods_bias[mods_bias$location_id == i & mods_bias$age_group == j,]
              
              
              
              # Sequentially prioritize the three methods based on available data
              if (i %in% unique(observed_age$location_id)) {
                
                method <- 1 
                tmp_observed <- observed_age[observed_age$location_id == i & observed_age$age_group == j,]
                
              } else {
                
                method <- 2 
                tmp_observed <- observed_all_age[observed_all_age$location_id == i,] # try non-age-stratified data
                setnames(tmp_observed, 'people_vaccinated', 'initially_vaccinated') # Must agree with age-stratified nomenclature
                
                check <- nrow(tmp_observed) == 0 | all(is.na(tmp_observed$pct_vaccinated)) | sum(tmp_observed$pct_vaccinated, na.rm=TRUE) == 0
                if (check) method <- 3 
                
              }
              
              #if (i %in% force_method_1) method <- 1
              #if (i %in% force_method_2) method <- 2
              #if (i %in% force_method_3) method <- 3
              
              message(paste(i, hierarchy[hierarchy$location_id == i, location_name], j, glue('Method {method}'), sep=' | '))
              
              
              # Do bias correction based on the method
              if (method == 1) {
                
                if (j == all_age_group) {
                  
                  tmp <- merge(tmp_observed_all_age[,c(merge_cols, 'initially_vaccinated'), with=F],
                               tmp_mod_all_age[,c(merge_cols, 'smooth_pct_vaccinated'), with=F],
                               by = merge_cols,
                               all.y=T)
                  
                } else {
                  
                  tmp <- merge(tmp_observed[,c(merge_cols, 'initially_vaccinated'), with=F],
                               tmp_mod_age[,c(merge_cols, 'smooth_pct_vaccinated'), with=F],
                               by = merge_cols)
                }
                
                tmp$population <- population[population$location_id == i & population$age_group == j, 'population']
                tmp$pct_vaccinated <- tmp$initially_vaccinated / tmp$population
                tmp$smooth_raw_vaccinated <- tmp$smooth_pct_vaccinated * tmp$population
                
                
                # Get non-age stratified data
                tmp_observed_all_age <- observed_all_age[observed_all_age$location_id == i,] # try non-age-stratified data
                setnames(tmp_observed_all_age, 'people_vaccinated', 'initially_vaccinated_all_age') # Must agree with age-stratified nomenclature
                
                # Impute missing observations based on ratio of age-group to all-age (will rake later)
                imputed <- merge(tmp[,c('date', 'initially_vaccinated')], 
                                 tmp_observed_all_age[,c('date', 'initially_vaccinated_all_age')], 
                                 by='date',
                                 all=T)
                
                # Match zeros
                imputed$initially_vaccinated_all_age <- .extend_end_values(imputed$initially_vaccinated_all_age, lead=T, trail=F)
                sel <- which(imputed$initially_vaccinated_all_age == 0)
                imputed$initially_vaccinated[sel] <- 0
                
                imputed[, imputed := is.na(initially_vaccinated)]
                imputed$ratio_to_all_age <- imputed$initially_vaccinated / imputed$initially_vaccinated_all_age
                imputed$ratio_to_all_age[is.nan(imputed$ratio_to_all_age)] <- 0
                imputed$ratio_to_all_age <- zoo::na.approx(imputed$ratio_to_all_age, na.rm=FALSE)
                imputed$ratio_to_all_age <- .extend_end_values(imputed$ratio_to_all_age)
                
                imputed[is.na(initially_vaccinated), initially_vaccinated := initially_vaccinated_all_age * ratio_to_all_age]
                
                tmp <- merge(tmp[,initially_vaccinated := NULL],
                             imputed[,c('date', 'initially_vaccinated', 'ratio_to_all_age', 'imputed')],
                             by='date',
                             all.x=T)
                
                tmp$pct_vaccinated_adjusted <- tmp$initially_vaccinated / tmp$population
                
                max_date <- as.Date(max(imputed$date[!is.na(imputed$initially_vaccinated)]))
                
                tmp_mod <- .fit_scam(data=tmp,
                                     response='pct_vaccinated_adjusted',
                                     min_date=as.Date('2020-12-16'),
                                     max_date = max_date,
                                     time_out=20,
                                     use_weights=FALSE,
                                     se_fit=F,
                                     return_data_frame=T)
                
                tmp <- merge(tmp, tmp_mod[,c('date', 'response')], by='date', all.x=T)
                setnames(tmp, 'response', 'smooth_pct_vaccinated_adjusted')
                tmp$smooth_raw_vaccinated_adjusted <- tmp$smooth_pct_vaccinated_adjusted * tmp$population
                
                tmp$smooth_pct_vaccinated <- NA
                tmp$smooth_raw_vaccinated <- NA
                
                #par(mfrow=c(1,3))
                #plot(imputed$date, imputed$initially_vaccinated_all_age, col='blue')
                #points(imputed$date[!imputed$imputed], imputed$initially_vaccinated[!imputed$imputed])
                ##
                #plot(imputed$date, imputed$ratio_to_all_age, col='green3', ylim=c(0,1))
                #points(imputed$date[!imputed$imputed], imputed$ratio_to_all_age[!imputed$imputed], pch=19)
                #
                #plot(tmp$date, tmp$pct_vaccinated_adjusted, col='green4', ylim=c(0,1))
                #abline(h=1, lty=2)
                #points(tmp$date, tmp$pct_vaccinated, pch=19)
                #lines(tmp$date, tmp$smooth_pct_vaccinated_adjusted, col='red', lwd=3)
                ##
                #mtext(paste(i, hierarchy[hierarchy$location_id == i, location_name], j, sep=' | '), outer=T, line=-2)
                #par(mfrow=c(1,1))
                
                
              } else if (method == 2) {
                
                # Method 2: when age-stratified vaccination data are not available
                # Infers vaccination is each age group based on reported FB vaccination coverage and 
                # observed bias between FB survey vaccination coverage and reported vaccination coverage 
                # in other locations
                
                if (j == all_age_group) {
                  
                  tmp <- merge(tmp_observed_all_age[,c(merge_cols, 'initially_vaccinated'), with=F],
                               tmp_mod_all_age[,c(merge_cols, 'smooth_pct_vaccinated'), with=F],
                               by = merge_cols,
                               all.y=T)
                  
                  tmp <- merge(tmp,
                               tmp_mod_age[,c(merge_cols, 'smooth_pct_vaccinated'), with=F],
                               by = merge_cols,
                               all.y=T)
                  
                  tmp <- merge(tmp, 
                               tmp_bias[,c(merge_cols,'smooth_bias', 'smooth_ratio'), with=F],
                               by = merge_cols,
                               all.x=T)
                  
                  tmp$smooth_bias <- tmp$smooth_pct_vaccinated.y - tmp$smooth_pct_vaccinated.x
                  tmp$smooth_ratio <- (tmp$smooth_pct_vaccinated.y - tmp$smooth_bias) / tmp$smooth_pct_vaccinated.y
                  tmp$smooth_ratio[tmp$smooth_pct_vaccinated.y == 0] <- 0
                  tmp$smooth_pct_vaccinated_adjusted <- tmp$smooth_pct_vaccinated.y - tmp$smooth_bias
                  tmp$population <- population[population$location_id == i & population$age_group == j, 'population']
                  
                  names(tmp)[which(names(tmp) == 'smooth_pct_vaccinated.y')] <- 'smooth_pct_vaccinated'
                  tmp[,'smooth_pct_vaccinated.x' := NULL]
                  
                  #plot(tmp$date, tmp$smooth_pct_vaccinated.y, col='blue')
                  #points(tmp$date, tmp$smooth_pct_vaccinated.x, col='red')
                  #lines(tmp$date, tmp$smooth_pct_vaccinated_adjusted, col='blue', lwd=2)
                  
                } else {
                  
                  tmp <- merge(tmp_observed[,c(merge_cols, 'initially_vaccinated'), with=F],
                               tmp_mod_all_age[,c(merge_cols, 'smooth_pct_vaccinated'), with=F],
                               by = merge_cols,
                               all.y=T)
                  
                  tmp$age_group <- j
                  
                  tmp <- merge(tmp_mod_age[,c(merge_cols, 'smooth_pct_vaccinated'), with=F], 
                               tmp_bias[,c(merge_cols,'smooth_bias', 'smooth_ratio'), with=F],
                               by = merge_cols,
                               all.x=T)
                  
                  tmp$smooth_ratio <- .extend_end_values(tmp$smooth_ratio)
                  if (!time_varying_bias) tmp$smooth_ratio <- tmp$smooth_ratio[length(tmp$smooth_ratio)]
                  
                  if (sum(tmp$smooth_pct_vaccinated, na.rm=T) == 0) {
                    tmp$pct_vaccinated_adjusted <- tmp$smooth_pct_vaccinated - tmp$smooth_bias
                  } else {
                    tmp$pct_vaccinated_adjusted <- tmp$smooth_pct_vaccinated * tmp$smooth_ratio
                  }
                  
                  tmp$population <- population[population$location_id == i & population$age_group == j, 'population']
                  
                  max_date <- tmp$date[max(which(!is.na(tmp$pct_vaccinated_adjusted)))]
                  
                  tmp_mod <- .fit_scam(data=tmp,
                                       response='pct_vaccinated_adjusted',
                                       max_date=max_date,
                                       use_weights=F,
                                       se_fit=F,
                                       time_out=20,
                                       return_data_frame = T)
                  
                  tmp <- merge(tmp, tmp_mod[,c('date', 'response')], by='date', all.y=T)
                  setnames(tmp, 'response', 'smooth_pct_vaccinated_adjusted')
                  
                }
                
                tmp[which(tmp$smooth_pct_vaccinated_adjusted > 1), 'smooth_pct_vaccinated_adjusted'] <- 1
                tmp$smooth_raw_vaccinated_adjusted <- tmp$smooth_pct_vaccinated_adjusted * tmp$population
                tmp$pct_vaccinated <- NA
                
                #par(mfrow=c(1,3))
                #plot(tmp$date, tmp$smooth_pct_vaccinated, ylim=c(0,1.25))
                #points(tmp$date, tmp$pct_vaccinated_adjusted, col='pink')
                #lines(tmp$date, tmp$smooth_pct_vaccinated_adjusted, col='purple', lwd=2)
                #plot(tmp$date, tmp$smooth_bias)
                #plot(tmp$date, tmp$smooth_ratio)
                #par(mfrow=c(1,1))
                #
                #plot(tmp_observed$date, tmp_observed$pct_vaccinated, ylim=c(0,1), main=paste(i, j))
                #points(tmp$date, tmp$pct_vaccinated_adjusted, pch=2)
                #lines(tmp_mod_all_age$date, tmp_mod_all_age$smooth_pct_vaccinated, col='green', lwd=2)
                #lines(tmp$date, tmp$smooth_pct_vaccinated_adjusted, col='dodgerblue', lwd=2)
                
                
              } else if (method == 3) {
                
                # Method 3 is used when no observed data are available
                # This uses spatial aggregates of the ratios in method 1 to infer what the bias correction should be
                
                tmp <- merge(data.table(location_id=i,
                                        date=as.Date(tmp_mod_age$date),
                                        age_group=j,
                                        pct_vaccinated=NA),
                             tmp_mod_age[,c(merge_cols, 'smooth_pct_vaccinated'), with=F],
                             by = merge_cols)
                
                tmp <- merge(tmp, 
                             tmp_bias[,c(merge_cols,'smooth_bias', 'smooth_ratio'), with=F],
                             by = merge_cols,
                             all.x=T)
                
                tmp$smooth_ratio <- .extend_end_values(tmp$smooth_ratio)
                if (!time_varying_bias) tmp$smooth_ratio <- tmp$smooth_ratio[length(tmp$smooth_ratio)]
                
                if (sum(tmp$smooth_pct_vaccinated, na.rm=T) == 0) {
                  tmp$pct_vaccinated_adjusted <- tmp$smooth_pct_vaccinated - tmp$smooth_bias
                } else {
                  tmp$pct_vaccinated_adjusted <- tmp$smooth_pct_vaccinated * tmp$smooth_ratio
                }
                
                max_date <- tmp$date[max(which(!is.na(tmp$pct_vaccinated_adjusted)))]
                
                tmp_mod <- .fit_scam(data=tmp,
                                     response='pct_vaccinated_adjusted',
                                     max_date=max_date,
                                     use_weights=F,
                                     se_fit=F,
                                     time_out=20,
                                     return_data_frame = T)
                
                tmp <- merge(tmp, tmp_mod[,c('date', 'response')], by='date', all.y=T)
                setnames(tmp, 'response', 'smooth_pct_vaccinated_adjusted')
                
                tmp[which(tmp$smooth_pct_vaccinated_adjusted > 1), 'smooth_pct_vaccinated_adjusted'] <- 1
                tmp$population <- population[population$location_id == i & population$age_group == j, 'population']
                tmp$smooth_raw_vaccinated_adjusted <- tmp$smooth_pct_vaccinated_adjusted * tmp$population
                tmp$pct_vaccinated <- NA
                
                #plot(tmp$date, tmp$smooth_pct_vaccinated, ylim=c(0,1))
                #points(tmp$date, tmp$smooth_pct_vaccinated_adjusted, ylim=c(0,1), col='red')
                
              } else {
                warning(glue('{i} | {j} | Could not identify which bias correction method to use'))
                next
              }
              
              tmp$date <- as.Date(tmp$date)
              tmp$age_start <- .get_column_val(tmp_mod_age$age_start)
              tmp$age_end <- .get_column_val(tmp_mod_age$age_end)
              tmp$method <- method
              tmp
              
              #}, error = function(e) {
              #  
              #  message(paste(conditionMessage(e)), '\n')
              #  
              #})
            }
  
  stopCluster(cl)
  
  
  # Check
  #tmp <- mods_bias_corrected[location_id == 71,]
  #
  #grid.arrange(
  #  ggplot() +
  #    geom_line(data = tmp, 
  #              aes(x=date, y=smooth_raw_vaccinated_adjusted)) +
  #    facet_grid(rows=vars(age_group), cols=vars(location_id)),
  #  
  #  ggplot() +
  #    geom_line(data = tmp, 
  #              aes(x=date, y=smooth_pct_vaccinated_adjusted)) +
  #    facet_grid(rows=vars(age_group), cols=vars(location_id)),
  #  ncol=2
  #)
  
  
  
  
  #-------------------------------------------------------------------------------
  # Confirm proportional split worked
  # If sums off - apply rake factor
  
  x <- mods_bias_corrected[location_id == 71,]
  
  message('Doing final raking of counts')
  
  .rake_model_output <- function(x) {
    
    x <- as.data.frame(x)
    age_groups <- unique(x$age_group)
    all_age_group <- paste0(min(x$age_start), '-', max(x$age_end))
    age_groups <- age_groups[!(age_groups == all_age_group)]
    
    #par(mfrow=c(1,5))
    #plot(x$date, x$smooth_raw_vaccinated_adjusted)
    #plot(x$date, x$smooth_pct_vaccinated_adjusted)
    
    # Adjust each age group by raking factor
    wide <- reshape2::dcast(x, date ~ age_group, value.var="smooth_raw_vaccinated_adjusted")
    wide$rake_factor <- wide[,all_age_group] / rowSums(wide[,age_groups])
    wide$rake_factor[wide$rake_factor == Inf | is.nan(wide$rake_factor)] <- 0
    for (j in age_groups) wide[,j] <- wide[,j] * wide$rake_factor
    
    x_raked <- data.frame(
      location_id=unique(x$location_id),
      reshape2::melt(data=wide, 
                     id.vars='date', 
                     measure.vars=c(all_age_group, as.character(age_groups)), 
                     variable.name='age_group',
                     value.name='smooth_raw_vaccinated_adjusted_raked'),
      stringsAsFactors = FALSE
    )
    
    #plot(x_raked$date, x_raked$smooth_raw_vaccinated_adjusted_raked)
    
    #y <- x_raked[x_raked$age_group == '65-125',]
    
    # Check for non-cumulative behavior and correct
    x_raked <- do.call(rbind,
                       lapply(split(x_raked, f=x_raked$age_group),
                              FUN=function(y) {
                                
                                check <- diff(y$smooth_raw_vaccinated_adjusted_raked) < 0
                                if (any(check[!is.na(check)])) y$smooth_raw_vaccinated_adjusted_raked <- .make_cumulative(y$smooth_raw_vaccinated_adjusted_raked)
                                
                                # Might want to cap at age group population here....
                                
                                return(y)
                                
                              }))
    
    #ggplot(x_raked) +
    #  geom_line(aes(x=date, y=smooth_raw_vaccinated_adjusted_raked)) +
    #  facet_grid(rows=vars(age_group), cols=vars(location_id))
    
    
    # Final adjustment of all age sum
    # Adjust each age group by raking factor
    wide <- reshape2::dcast(x_raked, date ~ age_group, value.var="smooth_raw_vaccinated_adjusted_raked")
    wide[,all_age_group] <- rowSums(wide[,age_groups])
    
    x_raked <- data.frame(
      location_id=unique(x$location_id),
      reshape2::melt(data=wide, 
                     id.vars='date', 
                     measure.vars=c(all_age_group, as.character(age_groups)), 
                     variable.name='age_group',
                     value.name='smooth_raw_vaccinated_adjusted_raked'),
      stringsAsFactors = FALSE
    )
    
    #plot(x_raked$date, x_raked$smooth_raw_vaccinated_adjusted_raked)
    
    # Check
    wide <- reshape2::dcast(x_raked, date ~ age_group, value.var="smooth_raw_vaccinated_adjusted_raked")
    wide$rake_factor <- wide[,all_age_group] / rowSums(wide[,age_groups])
    
    check <- !(wide$rake_factor == 1)
    if (any(check[!is.na(check)])) warning(glue('{unique(x$location_id)} | Age groups did not rake perfectly'))
    
    return(x_raked)
  }
  
  mods_bias_corrected_raked <- .apply_func_location(object = mods_bias_corrected, func = .rake_model_output, n_cores=n_cores)
  
  
  # Check which locations are missing
  message('After raking the following locations are missing:')
  most_detailed_locs <- hierarchy[most_detailed == 1, location_id]
  sel <- most_detailed_locs %in% mods_bias_corrected_raked$location_id
  missing_locs <- most_detailed_locs[!sel]
  hierarchy[location_id %in% missing_locs, .(location_id, location_name)]
  
  
  mods_bias_corrected_raked <- merge(mods_bias_corrected, 
                                     mods_bias_corrected_raked, 
                                     by=c('location_id', 'date', 'age_group'))
  
  mods_bias_corrected_raked$smooth_pct_vaccinated_adjusted_raked <- mods_bias_corrected_raked$smooth_raw_vaccinated_adjusted_raked / mods_bias_corrected_raked$population
  
  # Raking can nudge proportions over 1 in some age groups, so final quantity here must be capped at 1
  sel <- which(mods_bias_corrected_raked$smooth_pct_vaccinated_adjusted_raked > 1)
  
  if (length(sel) > 0) {
    
    message('WARNING: smooth_pct_vaccinated_adjusted_raked > 1 (likely from raking) in the following locations and age groups. Capping at 1.')
    problem_locs <- unique(mods_bias_corrected_raked$location_id[sel])
    print(unique(mods_bias_corrected_raked[location_id %in% problem_locs, .(location_id, age_group)]))
    mods_bias_corrected_raked[sel, 'smooth_pct_vaccinated_adjusted_raked'] <- 1
  }
  
  message('Calculating daily vaccination rate')
  f <- .calc_daily_rate_age <- function(x) {
    x$daily_rate <- c(NA, diff(x$smooth_raw_vaccinated_adjusted_raked))
    x
  }
  
  mods_bias_corrected_raked <- .apply_func_location_age(object=mods_bias_corrected_raked, func=.calc_daily_rate_age)

  
  # Check
  #tmp <- mods_bias_corrected_raked[location_id == 33,]
  #
  #grid.arrange(
  #  ggplot() +
  #    geom_line(data = tmp, 
  #              aes(x=date, y=smooth_raw_vaccinated_adjusted_raked)) +
  #    facet_grid(rows=vars(age_group), cols=vars(location_id)),
  #  
  #  ggplot() +
  #    geom_line(data = tmp, 
  #              aes(x=date, y=smooth_pct_vaccinated_adjusted_raked)) +
  #    facet_grid(rows=vars(age_group), cols=vars(location_id)),
  #  ncol=2
  #)
  
  
  # Save
  tmp <- file.path(vaccine_output_root, 'smooth_pct_vaccinated_age_adjusted.csv')
  write.csv(mods_bias_corrected_raked, file=tmp, row.names = F)
  message(paste0('Bias correction applied to vaccination coverage by age and saved here: ', tmp))
  print(Sys.time() - .t)
  
  
}

