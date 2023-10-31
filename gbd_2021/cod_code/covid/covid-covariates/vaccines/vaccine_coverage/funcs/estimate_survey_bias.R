
estimate_survey_bias <- function(vaccine_output_root,
                                 hierarchy,
                                 population,
                                 knock_out_locations=NULL,
                                 plots=FALSE
) {
  
  .print_header_message('Estimating bias in vaccination coverage from FB survey models')
  
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
  
  
  # Merge age group populations
  population <- population[,!colnames(population) %in% c('age_start', 'age_end')]
  observed_age <- merge(observed_age, population, by=c('location_id', 'age_group'))
  observed_age$pct_vaccinated <- observed_age$initially_vaccinated/observed_age$population
  
  
  
  
  #-----------------------------------------------------------------------------
  # Estimate bias
  #-----------------------------------------------------------------------------
  
  message('Calculating observed bias...')
  
  # Calculate ratio of vaccination coverage based on FB data to observed vaccination coverage from other data sources (OWiD, CDC etc)
  merge_cols <- c('location_id', 'date', 'age_group')
  tmp <- merge(observed_age[, c(merge_cols, 'pct_vaccinated'), with=F], 
               mods_age[, c(merge_cols, 'smooth_pct_vaccinated', 'population'), with=F], 
               by=merge_cols,
               all.y = T)
  
  tmp$date <- as.Date(tmp$date)
  tmp <- as.data.frame(tmp)
  
  observed_bias <- foreach(i = unique(tmp$location_id), .combine=rbind) %:%
    foreach (j = unique(tmp$age_group), .combine=rbind, .errorhandling='remove') %do% {
      
      x <- as.data.frame(tmp[tmp$location == i & tmp$age_group == j,])
      
      # A patch to keep things from getting weird if EUA dates are missing
      sel <- x$date < as.Date('2020-12-16') + 7
      if (!all(is.na(x$pct_vaccinated)) & all(is.na(x$pct_vaccinated[sel]))) x$pct_vaccinated[sel] <- 0 

      x$bias <- x$smooth_pct_vaccinated - x$pct_vaccinated
      x$ratio <- (x$smooth_pct_vaccinated - x$bias) / x$smooth_pct_vaccinated
      x$ratio[x$smooth_pct_vaccinated == 0] <- 0
      x
      
      #plot(x$date, x$smooth_pct_vaccinated)
      #points(x$date, x$pct_vaccinated, col='blue')
      
    }
  
  observed_bias <- observed_bias[observed_bias$bias <= 1 & observed_bias$bias >= -1,]
  observed_bias <- observed_bias[!(is.na(observed_bias$age_group)),]
  observed_bias <- .add_spatial_metadata(data=observed_bias, hierarchy = hierarchy)
  
  .print_header_message('Locations with observed bias')
  for (j in age_groups) {
    l <- length(unique(observed_bias$location_id[observed_bias$age_group == j]))
    message(glue('{l} locations in {j} age group'))
  }
  
  
  
  
  #-----------------------------------------------------------------------------
  # Do manual knock-outs: these locations will inherit a parent/region level model
  #-----------------------------------------------------------------------------
  
  if (!is.null(knock_out_locations)) {
    message('Manual knock out of bias for the following locations (these will inherit a parent-level model):')
    message(paste(knock_out_locations, collapse=' | '))
    observed_bias <- observed_bias[!(observed_bias$location_id %in% knock_out_locations),]
  }
  
  
  
  #-----------------------------------------------------------------------------
  # Time-varying bias
  #-----------------------------------------------------------------------------
  
  message('Estimating time-varying bias functions...')
  
  .fit_bias_models <- function(data,               # data.frame long by location_id and age_group with data columns pct_vaccinated and total_respondents_vaccination
                               level,              # level of spatial aggregation (expects 'global', 'super_region', 'region', 'parent', or 'location')
                               hierarchy=NULL,     # covid spatial hierarchy 
                               n_cores
  ) {
    
    message(level)
    t_start <- Sys.time()
    if (!inherits(data$date, 'Date')) data$date <- as.Date(data$date)
    if (is.null(hierarchy)) hierarchy <- gbd_data$get_covid_covariate_prep_hierarchy()
    
    
    if (level == 'parent') {
      sel <- data$parent_id %in% data$location_id
      duplicate_parents <- unique(data$parent_id[sel])
      message('The following parent locations have location-level vaccination data. Parent-level aggregate models will not be fitted:')
      message(paste(duplicate_parents, collapse = ' | '))
    }
    
    data <- set_spatial_scale(data = data, level = level, hierarchy = hierarchy) # pool data based on the spatial level
    
    if (level == 'parent') data <- data[!(data$location_id %in% duplicate_parents),]

    suppressMessages(cl <- parallel::makeCluster(n_cores))
    doParallel::registerDoParallel(cl)
    
    fitted_models <- 
      foreach(i = unique(data$location_id), .combine=rbind) %:%
      foreach(j = unique(data$age_group), .combine=rbind, 
              .packages = c('mgcv', 'data.table'), .export = c('.get_column_val', '.extend_end_values')) %dopar% {
                
                x <- data[data$location_id == i & data$age_group == j,]
                
                tryCatch({
                  
                  out <- data.frame(date =                       seq.Date(as.Date("2020-12-16"), as.Date(max(data$date)), by=1),
                                    location_id =               .get_column_val(x$location_id),
                                    location_name =             .get_column_val(x$location_name),
                                    parent_id =                 .get_column_val(x$parent_id),
                                    region_id =                 .get_column_val(x$region_id),
                                    region_name =               .get_column_val(x$region_name),
                                    super_region_id =           .get_column_val(x$super_region_id),
                                    super_region_name =         .get_column_val(x$super_region_name),
                                    age_start =                 .get_column_val(x$age_start),
                                    age_end =                   .get_column_val(x$age_end),
                                    age_group =                 .get_column_val(x$age_group),
                                    smooth_bias =                NA,
                                    smooth_bias_L95 =            NA,
                                    smooth_bias_H95 =            NA,
                                    smooth_ratio =               NA,
                                    smooth_ratio_L95 =           NA,
                                    smooth_ratio_H95 =           NA,
                                    fit_level =                  level,
                                    model_type =                 'gam_gaussian',
                                    R2_bias =                    NA,
                                    R2_ratio =                   NA)
                  
                  mod <- mgcv::gam(bias ~ s(as.integer(date), bs='cr'), 
                                   data=x,
                                   weights=x$population/mean(x$population),
                                   family='gaussian')
                  
                  pred <- predict(mod, 
                                  newdata=data.frame(date=as.integer(out$date)), 
                                  type='link', 
                                  se.fit=T)
                  
                  out$smooth_bias <- pred$fit
                  out$smooth_bias_H95 <- pred$fit + pred$se.fit*2
                  out$smooth_bias_L95 <- pred$fit - pred$se.fit*2
                  
                  x$ratio[x$ratio == 0] <- 1e-03
                  mod <- mgcv::gam(ratio ~ s(as.integer(date), bs='cr'), 
                                   data=x,
                                   weights=x$population/mean(x$population),
                                   family='Gamma')
                  
                  pred <- predict(mod, 
                                  newdata=data.frame(date=as.integer(out$date)), 
                                  type='response', 
                                  se.fit=T)
                  
                  out$smooth_ratio <- pred$fit
                  out$smooth_ratio_H95 <- pred$fit + pred$se.fit*2
                  out$smooth_ratio_L95 <- pred$fit - pred$se.fit*2
                  
                  # Carry model value forward from last observed date
                  sel <- out$date > max(x$date[!is.na(x$bias)])
                  
                  out$smooth_bias[sel] <- NA
                  out$smooth_bias_H95[sel] <- NA
                  out$smooth_bias_L95[sel] <- NA
                  
                  out$smooth_bias <- .extend_end_values(out$smooth_bias)
                  out$smooth_bias_L95 <- .extend_end_values(out$smooth_bias_L95)
                  out$smooth_bias_H95 <- .extend_end_values(out$smooth_bias_H95)
                  
                  out$R2 <- summary(mod)$r.sq
                  out
                  
                }, error = function(e) {
                  
                  message(paste('ERROR:', i, hierarchy[location_id == i, 'location_name'], j, conditionMessage(e)))
                  
                })
                
                #par(mfrow=c(1,3))
                #plot(x$date, x$smooth_pct_vaccinated, ylim=c(0,1.25), main='Est v obs')
                #points(x$date, x$pct_vaccinated, col='grey50')
                #
                #plot(x$date, x$bias, ylim=c(-1,1), main='Absolute bias')
                #abline(h=0)
                #lines(out$date, out$smooth_bias, col='red')
                #lines(out$date, out$smooth_bias_L95, col='red', lty=2)
                #lines(out$date, out$smooth_bias_H95, col='red', lty=2)
                #
                #plot(x$date, x$ratio, main='Bias ratio')
                #lines(out$date, out$smooth_ratio, col='blue')
                #lines(out$date, out$smooth_ratio_H95, col='blue')
                #lines(out$date, out$smooth_ratio_L95, col='blue')
                #par(mfrow=c(1,3))
                
              }
    
    stopCluster(cl)
    
    row.names(fitted_models) <- NULL
    message('Done.')
    print(Sys.time() - t_start)
    return(fitted_models)
  }
  
  # Fit vaccination coverage by age at each spatial scale
  mods_bias <- rbind(
    .fit_bias_models(data=observed_bias, level='global', hierarchy=hierarchy, n_cores=length(age_starts)),
    .fit_bias_models(data=observed_bias, level='super_region', hierarchy=hierarchy, n_cores=pmin(length(age_starts)*7, n_cores)),
    .fit_bias_models(data=observed_bias, level='region', hierarchy=hierarchy, n_cores=n_cores),
    .fit_bias_models(data=observed_bias, level='parent', hierarchy=hierarchy, n_cores=n_cores),
    .fit_bias_models(data=observed_bias, level='location', hierarchy=hierarchy, n_cores=n_cores)
  )
  
  .print_header_message('Locations with enough data to fit a time-varying bias function:')
  for (j in age_groups) {
    l <- length(unique(mods_bias$location_id[mods_bias$age_group == j]))
    message(glue('{l} locations in {j} age group'))
  }
  
  
  
  #-----------------------------------------------------------------------------
  # Extrapolate bias for unobserved locations / age groups
  #-----------------------------------------------------------------------------
  
  variable <- 'smooth_bias'
  variable_col <- which(colnames(mods_bias) == variable)
  if (length(variable_col) == 0) stop('variable not found in mods')
  if (is.null(hierarchy)) hierarchy <- gbd_data$get_covid_covariate_prep_hierarchy()
  
  check <- function(x) nrow(x) == 0 | all(is.na(x[,variable_col]))
  
  cl <- parallel::makeCluster(n_cores)
  doParallel::registerDoParallel(cl)
  
  mods_bias <- 
    foreach(i = hierarchy[, location_id], .combine=rbind) %:% 
    foreach(j = c(all_age_group, age_groups), .combine=rbind, .packages=c('data.table', 'glue')) %dopar% {
      
      #message(paste(i, hierarchy[location_id == i, location_name], j, sep=' | '))
      x <- mods_bias[mods_bias$location_id == i & mods_bias$age_group == j,]
      if (check(x)) x <- mods_bias[mods_bias$location_id == hierarchy[location_id == i, parent_id] & mods_bias$age_group == j,]
      if (check(x)) x <- mods_bias[mods_bias$location_id == hierarchy[location_id == i, region_id] & mods_bias$age_group == j,]
      if (check(x)) x <- mods_bias[mods_bias$location_id == hierarchy[location_id == i, super_region_id] & mods_bias$age_group == j,]
      if (check(x)) x <- mods_bias[mods_bias$location_id == 1 & mods_bias$age_group == j,]
      if (check(x)) warning(glue('{i} | {j} | cannot find model'))
      
      x$location_id <- i
      x
      
    }
  
  stopCluster(cl)
  
  mods_bias <- .add_spatial_metadata(data=mods_bias, hierarchy=hierarchy)
  row.names(mods_bias) <- NULL
  
  
  
  # Check for duplicates (there can be overlap between location and parent aggregates)
  dups <- which(duplicated(mods_bias[,c('location_id', 'date', 'age_group')], fromLast = TRUE))
  
  if (length(dups) > 0) {
    
    dup_locs <- unique(mods_bias[dups, 'location_id'])
    
    message(glue('Found these duplicated locations in mods_age:'))
    message(paste(dup_locs, collapse=' | '))
    message('Filtering for location level fits...')
    
    for (i in dup_locs) {
      for (j in unique(mods_bias$age_group)) {
        
        message(paste(i, j, sep=' | '))
        sel <- mods_bias$location_id == i & mods_bias$age_group == j
        tmp <- mods_bias[sel,]
        
        if ('location' %in% unique(tmp$fit_level)) {
          
          mods_bias <- rbind(mods_bias[!sel,], tmp[tmp$fit_level == 'location',])
        } else {
          warning(glue('Location {i} | age group {j} is duplicated but there is no model fit at location level'))
        }
        
      }
    }
    
    dup_locs <- unique(mods_bias[duplicated(mods_bias[,c('location_id', 'date', 'age_group')]), 'location_id'])
    if (length(dup_locs) > 0) warning(glue("Could not resolve location-age duplicates for: {paste(dup_locs, collapse=' ')}"))

  }
  
  .print_header_message('Locations with time-varying bias after spatial extrapolation:')
  for (j in age_groups) {
    l <- length(unique(mods_bias$location_id[mods_bias$age_group == j]))
    message(glue('{l} locations in {j} age group'))
  }


  # Check
  sel <- is.na(mods_bias$smooth_bias)
  if (sum(sel) > 0) warning('NAs recorded for population size in one year age intervals')
  
  for (i in unique(mods_bias$location_id)) {
    tmp <- mods_bias[mods_bias$location_id == i,]
    if (!all(age_groups %in% tmp$age_group)) warning(glue('Population missing for some age starts in location {i}'))
  }
  
  if (!all(unique(hierarchy$location_id) %in% mods_bias$location_id)) stop('Age group populations missing some locations')
  
  
  # Save
  tmp <- file.path(vaccine_output_root, 'observed_survey_bias_age.csv')
  write.csv(observed_bias, file=tmp, row.names = F)
  message(paste0('Observed bias saved here: ', tmp))
  
  tmp <- file.path(vaccine_output_root, 'smooth_survey_bias_age.csv')
  write.csv(mods_bias, file=tmp, row.names = F)
  message(paste0('Models of time-varying bias saved here: ', tmp))


  if (plots) {
    
    message('Plotting bias diagnostics...')
    
    observed_bias$age_group <- .factor_age_groups(observed_bias$age_group)
    mods_bias$age_group <- .factor_age_groups(mods_bias$age_group)
    
    pdf(file.path(vaccine_output_root, glue("bias_check.pdf")), width=6, height=10, onefile = TRUE)


    # Global
    p <- ggplot(mods_bias[mods_bias$location_id == 1,]) +
      geom_point(data=observed_bias, aes(x=date, y=bias), shape=1, alpha=0.3) +
      geom_hline(yintercept=0) +
      geom_line(aes(x=date, y=smooth_bias), color='dodgerblue', size=1) +
      geom_line(aes(x=date, y=smooth_bias_H95), color='dodgerblue', size=1, linetype=2) +
      geom_line(aes(x=date, y=smooth_bias_L95), color='dodgerblue', size=1, linetype=2) +
      facet_grid(rows=vars(age_group)) +
      theme_bw() +
      labs(title=paste0(hierarchy[location_id == 1, location_name], ' (1) | n = ', length(unique(observed_bias$location_id))))
    
    print(p)
    
    # Super region
    sr <- unique(mods_bias$super_region_id)
    for (i in sr[!is.na(sr)]) {
      
      tmp <- observed_bias[observed_bias$super_region_id == i,]
      n <- length(unique(tmp$location_id))
      
      p <- ggplot(mods_bias[mods_bias$location_id == i,]) +
        geom_point(data=tmp, aes(x=date, y=bias), shape=1, alpha=0.3) +
        geom_hline(yintercept=0, lty=2, color='green3') +
        geom_line(aes(x=date, y=smooth_bias), color='dodgerblue', size=1) +
        geom_line(aes(x=date, y=smooth_bias_H95), color='dodgerblue', size=1, linetype=2) +
        geom_line(aes(x=date, y=smooth_bias_L95), color='dodgerblue', size=1, linetype=2) +
        facet_grid(vars(age_group)) +
        theme_bw() +
        labs(title = paste0(hierarchy[location_id == i, location_name], ' (', i, ') | n = ', n),
             caption = paste0('Fit level: ', mods_bias$fit_level[mods_bias$location_id == i]))
      
      print(p)
    }
    
    # Region
    r <- unique(mods_bias$region_id)
    for (i in r[!is.na(r)]) {
      
      tmp <- observed_bias[observed_bias$region_id == i,]
      n <- length(unique(tmp$location_id))
      
      p <- ggplot(mods_bias[mods_bias$location_id == i,]) +
        geom_point(data=tmp, aes(x=date, y=bias), shape=1, alpha=0.3) +
        geom_hline(yintercept=0, lty=2, color='green3') +
        geom_line(aes(x=date, y=smooth_bias), color='dodgerblue', size=1) +
        geom_line(aes(x=date, y=smooth_bias_H95), color='dodgerblue', size=1, linetype=2) +
        geom_line(aes(x=date, y=smooth_bias_L95), color='dodgerblue', size=1, linetype=2) +
        facet_grid(vars(age_group)) +
        theme_bw() +
        labs(title = paste0(hierarchy[location_id == i, location_name], ' (', i, ') | n = ', n),
             caption = paste0('Fit level: ', mods_bias$fit_level[mods_bias$location_id == i]))
      
      print(p)
    }
    
    # Location
    
    for (i in hierarchy[level >= 3, location_id]) {
      
      tmp <- observed_bias[observed_bias$location_id == i,]
      n <- length(unique(tmp$location_id))
      
      p <- ggplot(mods_bias[mods_bias$location_id == i,]) +
        geom_point(data=tmp, aes(x=date, y=bias), shape=1, alpha=0.3) +
        geom_hline(yintercept=0, lty=2, color='green3') +
        geom_line(aes(x=date, y=smooth_bias), color='dodgerblue', size=1) +
        geom_line(aes(x=date, y=smooth_bias_H95), color='dodgerblue', size=1, linetype=2) +
        geom_line(aes(x=date, y=smooth_bias_L95), color='dodgerblue', size=1, linetype=2) +
        facet_grid(vars(age_group)) +
        theme_bw() +
        labs(title = paste0(hierarchy[location_id == i, location_name], ' (', i, ') | n = ', n),
             caption = paste0('Fit level: ', mods_bias$fit_level[mods_bias$location_id == i]))
      
      print(p)
    }
    
    dev.off()
    message('Done.')
  }
  
  
}

