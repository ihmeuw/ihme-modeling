
smooth_vaccine_hesitancy_age <- function(vaccine_output_root,
                                         hierarchy,
                                         population,
                                         child_method='weighted_mean', # Either 'weighted_mean' (take weighted mean across all time points based on observed data) or 'glm' (child hesitancy tracks adult vax and hesitancy via a GLM)
                                         remove_wave_12_trial=FALSE,
                                         knock_out_locations=NULL
) {
  
  .print_header_message('Fitting hesitancy models by age')
  
  # Get global args
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_path <- model_parameters$model_inputs_path
  
  age_starts <- model_parameters$age_starts
  include_all_ages <- model_parameters$include_all_ages
  n_cores <- model_parameters$n_cores
  
  age_groups <- .get_age_groups(age_starts)
  child_age_groups <- age_groups[age_starts < 18]
  
  surv <- read.csv(file.path(vaccine_output_root, 'observed_survey_data_age.csv'), stringsAsFactors = F)
  surv$date <- as.Date(surv$date)
  
  # Need to clean this up: somehow survey data now has NAs going back to April 2020? A data extraction thing?
  surv <- surv[surv$date >= as.Date('2020-12-16'),]
  
  # Load modeled vaccination coverage by age (using FB survey data)
  mods_vaccination_age <- fread(file.path(vaccine_output_root, "smooth_pct_vaccinated_age.csv"))
  mods_vaccination_age$date <- as.Date(mods_vaccination_age$date)
  mods_vaccination_age <- merge(mods_vaccination_age, population[,c('location_id', 'age_group', 'population')], by=c('location_id', 'age_group'))
  mods_vaccination_age <- as.data.frame(mods_vaccination_age)

  
  #-------------------------------------------------------------------------------

  .fit_hesitancy_age_models <- function(data,       # data.frame with location-age hesitancy data 'survey_yes'
                                        level,      # level of spatial aggregation (expects 'region' or 'parent' or 'location' currently)
                                        n_cores,    # number of cores to use when running in parallel
                                        hierarchy   # covid spatial hierarchy 
  ) {
    
    message(level)
    if (!inherits(data$date, 'Date')) data$date <- as.Date(data$date)
    level_id <- paste0(level, '_id')
    level_name <- paste0(level, '_name')
    
    data <- .add_spatial_metadata(data=data, hierarchy=hierarchy)
    
    
    if (level == 'global') {
      data$location_id <- data$parent_id <- data$region_id <- data$super_region_id <- data$global_id <- 1
      data$location_name <- data$parent_name <- data$region_name <- data$super_region_name <- data$global_name <- 'global'
    }
    
    
    if (level == 'parent') {
      sel <- data$parent_id %in% data$location_id | data$parent_id %in% data$region_id 
      duplicate_parents <- unique(data$parent_id[sel])
      data <- data[!(data$parent_id %in% duplicate_parents),]
      message('The following parent locations have location-level hesitancy data. Parent-level aggregate models will not be fitted:')
      message(paste(duplicate_parents, collapse = ' | '))
    }
    
    #x <- data[data$region_id == 100 & data$age_group == '18-39',]
    #x <- data[data$location_id == 11 & data$age_group == '0-125',]
    #plot(x$date, x$survey_yes)
    
    do.call(
      rbind,
      pbmcapply::pbmclapply(
        X=split(data, 
                list(data[,which(colnames(data) == level_id)], 
                     data$age_group)), 
        mc.cores=n_cores, 
        FUN=function(x) {
            
            tryCatch( {
              
              
              out <- data.frame(date =                  seq.Date(as.Date("2020-12-16"), as.Date(max(x$date, na.rm=T)), by=1),
                                location_id =           .get_column_val(x[,which(colnames(x) == level_id)]),
                                location_name =         ifelse(level == 'parent', NA, .get_column_val(x[,which(colnames(x) == level_name)])),
                                parent_id =             hierarchy[location_id == .get_column_val(x[,which(colnames(x) == level_id)]), parent_id],
                                region_id =             .get_column_val(x$region_id),
                                region_name =           .get_column_val(x$region_name),
                                super_region_id =       .get_column_val(x$super_region_id),
                                super_region_name =     .get_column_val(x$super_region_name),
                                age_start =             .get_column_val(x$age_start),
                                age_end =               .get_column_val(x$age_end),
                                age_group =             .get_column_val(x$age_group),
                                survey_yes =            NA,
                                sample_size =           NA,
                                smooth_survey_yes =     NA,
                                smooth_survey_yes_H95 = NA,
                                smooth_survey_yes_L95 = NA,
                                fit_level=level)
              
              
              if (level == 'location') {
                out$survey_yes <- x$survey_yes
                out$sample_size <- x$sample_size
              }
              
              if (all(is.na(x$survey_yes))) {
                
                warning(paste('Location', unique(out$location_id), 'all survey_yes NA'))
                return(out)
                
              } else {
                
                x <- x[x$sample_size > 0,]
                
                if (.get_column_val(x$age_end) < 18) { # Methods for child age groups (data are very sparse)
                  
                  if (child_method == 'weighted_mean') {
                    
                    sel <- x$date >= as.Date(ifelse(remove_wave_12_trial, '2021-12-19', '2020-12-16'))
                    
                    #if (sum(!is.na(x$survey_yes[sel])) > 5) { # Only get weighted mean for locations where n >= 5
                    ci <- Hmisc::wtd.quantile(x=x$survey_yes[sel], w=x$sample_size[sel], probs=c(0.0275,0.975), na.rm=T)
                    out$smooth_survey_yes <- weighted.mean(x=x$survey_yes[sel], w=x$sample_size[sel], na.rm=T)
                    out$smooth_survey_yes_H95 <- ci[2]
                    out$smooth_survey_yes_L95 <- ci[1]
                    #}
                    
                  }
                  
                } else { # Adult age group methods
                  
                  mod <- mgcv::gam(survey_yes ~ as.integer(date) + s(as.integer(date), bs="gp"),
                                   data=x, 
                                   family='quasibinomial',
                                   weights=x$sample_size)
                  rm(x)
                  
                  pred <- predict(mod, 
                                  newdata=data.frame(date=as.integer(out$date)), 
                                  type='response', 
                                  se.fit=T)
                  rm(mod)
                  
                  out$smooth_survey_yes <- pred$fit
                  out$smooth_survey_yes_H95 <- pred$fit+pred$se.fit*1.96
                  out$smooth_survey_yes_L95 <- pred$fit-pred$se.fit*1.96
                  
                }
                
                return(out)
              }
              #}
              
              # Check
              #plot(x$date, x$survey_yes)
              #lines(out$date, out$smooth_survey_yes, col='red', cex=5)
              #lines(out$date, out$smooth_survey_yes_H95, col='red', cex=5, lty=2)
              #lines(out$date, out$smooth_survey_yes_L95, col='red', cex=5, lty=2)
              
            }, warning = function(w) {
              
              message(paste('WARNING:', unique(x$location_id), unique(x$age_group), conditionMessage(w)), '\n')
              
            }, error = function(e) {
              
              message(paste('ERROR:', unique(x$location_id), unique(x$age_group), conditionMessage(e)), '\n')
              
            })
        })
    )
  }
  
  # Fit hesitancy models at each spatial scale
  mods_hesitancy_age <- rbind(
    .fit_hesitancy_age_models(data=surv, level='global', n_cores=n_cores, hierarchy=hierarchy),
    .fit_hesitancy_age_models(data=surv, level='super_region', n_cores=n_cores, hierarchy=hierarchy),
    .fit_hesitancy_age_models(data=surv, level='region', n_cores=n_cores, hierarchy=hierarchy),
    .fit_hesitancy_age_models(data=surv, level='parent', n_cores=n_cores, hierarchy=hierarchy),
    .fit_hesitancy_age_models(data=surv, level='location', n_cores=n_cores, hierarchy=hierarchy)
  )
  
  row.names(mods_hesitancy_age) <- NULL

  # Check
  #test <- .fit_hesitancy_age_models(data=surv, level='region', n_cores=n_cores, hierarchy=hierarchy)
  #test <- mods_hesitancy_age[mods_hesitancy_age$location_id == 100 & mods_hesitancy_age$age_group == '0-125',]
  #
  #ggplot(test) +
  #  geom_line(aes(x=date, y=smooth_survey_yes), size=2) +
  #  facet_grid(vars(age_group),
  #             vars(location_name)) +
  #  theme_bw()
  
  
  
  ################################################################################
  # Do manual knock-outs: these locations will inherit a parent/region level model
  ################################################################################
  
  if (!is.null(knock_out_locations)) {
    message('Knocking out the following locations (these will inherit a parent-level model):')
    message(paste(knock_out_locations, collapse=' | '))
    sel <- mods_hesitancy_age$location_id %in% knock_out_locations
    mods_hesitancy_age <- mods_hesitancy_age[!sel,]
  }
  
  ################################################################################
  ################################################################################
  
  
  #----------------------------------------------------------------------------------------
  # Using GLMs to model child hesitancy must be done after the adults models are completed
  
  if (child_method == 'glm') {
    
    message('Estimating child vaccine hesitancy using GLMs...')
    
    #spatial_level <- 'super_region'
    data_survey <- surv
    mods_vaccination <- mods_vaccination_age
    mods_hesitancy <- mods_hesitancy_age
    
    .fit_child_hesitancy_glm <- function(child_age_groups,
                                         parent_age_group='18-64',
                                         spatial_level,
                                         data_survey,
                                         mods_vaccination,
                                         mods_hesitancy,
                                         hierarchy,
                                         min_samp_size=5,
                                         remove_wave_12_trial=FALSE, # Remove dates prior to full launch of wave 12 survey (December 19, 2021)
                                         verbose=FALSE
    ) {
      
      if (verbose) message(spatial_level)
      glm_list <- list()
      merge_cols <- c('location_id', 'date')
      
      # Get models of vaccination and hesitancy for the adult/parent age group
      mods_vaccination_parent <- mods_vaccination[!(is.na(mods_vaccination$smooth_pct_vaccinated)) & mods_vaccination$age_group == parent_age_group, 
                                                  c(merge_cols, 'smooth_pct_vaccinated')]
      
      mods_vaccination_child <- mods_vaccination[!(is.na(mods_vaccination$smooth_pct_vaccinated)) & mods_vaccination$age_group == j, 
                                                 c(merge_cols, 'smooth_pct_vaccinated')]
      colnames(mods_vaccination_child)[colnames(mods_vaccination_child) == 'smooth_pct_vaccinated'] <- 'smooth_pct_vaccinated_child'
      
      mods_hesitancy_parent <- mods_hesitancy[!(is.na(mods_hesitancy$smooth_survey_yes)) & mods_hesitancy$age_group == parent_age_group, 
                                              c(merge_cols, 'smooth_survey_yes')]
      
      
      for (j in child_age_groups) {
        
        # Get survey responses for child age group wherever they exist
        data_survey_child <- data_survey[!(is.na(data_survey$survey_yes)) & data_survey$age_group == j, 
                                         c(merge_cols, 'survey_yes', 'sample_size')]
        
        # Merge child data (survey_yes) with parent models: smooth_pct_vaccinated and smooth_survey_yes
        tmp <- merge(data_survey_child, mods_vaccination_parent, by=merge_cols, all=TRUE)
        tmp <- merge(tmp, mods_vaccination_child, by=merge_cols, all=TRUE)
        tmp <- merge(tmp, mods_hesitancy_parent, by=merge_cols, all=TRUE)
        
        # Aggregate survey data and models to the desired spatial scale
        tmp <- set_spatial_scale(data=tmp, level=spatial_level, hierarchy = hierarchy)
        
        # Fit GLM for each location/aggregate
        locations <- unique(tmp$location_id)
        for (i in locations[!is.na(locations)]) {
          
          covariate_names <- c(merge_cols, 
                               'smooth_pct_vaccinated',       # Parent model of vaccination coverage
                               'smooth_pct_vaccinated_child', # Child model of vaccination coverage
                               'smooth_survey_yes',           # Parent model of vaccine acceptance
                               'survey_yes',                  # Observed survey responses of parents indicating they will vaccinate child in age group j
                               'sample_size')                 # Number of respondent used as observation weights  
          
          tmp_loc <- tmp[tmp$location_id == i, covariate_names] # Number of respondent used as observation weights  
          tmp_loc <- tmp_loc[complete.cases(tmp_loc),]
          
          # Check
          
          model_formula <- as.formula('survey_yes ~ smooth_survey_yes + smooth_pct_vaccinated + smooth_pct_vaccinated_child + as.integer(date)')
          wave_12_trial <- tmp_loc$date < as.Date('2021-12-19') # Date of wave 12 full launch
          
          if (remove_wave_12_trial) {
            tmp_loc <- tmp_loc[!wave_12_trial,] # All dates prior to full wave 12 launch are removed
          } else {
            tmp_loc$wave_12_trial <- wave_12_trial # Wave 12 trial dates kept and binomial variable added for before/after full launch date
            model_formula <- as.formula('survey_yes ~ smooth_survey_yes + smooth_pct_vaccinated + smooth_pct_vaccinated_child + wave_12_trial + as.integer(date)')
          }
          
          
          if (nrow(tmp_loc) > min_samp_size) {
            
            par(mfrow=c(2,3))
            for (k in covariate_names[-c(1,6)]) plot(tmp_loc[,k], tmp_loc$survey_yes, xlab=k, main=paste(i, j))
            corrplot::corrplot(cor(tmp_loc[,-c(1:2)]), method = 'number', diag = FALSE, 
                               type = 'upper', pch.cex = 0.8 , tl.col='black', 
                               tl.cex = 0.7, tl.srt = 45, tl.pos='n')
            
            tryCatch( { 
              
              # Relationship between child acceptance and parent acceptance + parent vaccination coverage + time trend
              mod <- glm(formula=model_formula, data=tmp_loc, family='quasibinomial', weights=tmp_loc$sample_size)
              mod$spatial_level <- spatial_level
              
              # Append fitted model to list
              sel <- length(glm_list) + 1
              glm_list[[sel]] <- mod
              names(glm_list)[sel] <- paste(j, i, sep='-')
              
              if (verbose) message(paste(j, i, 'Model successful', sep=' | '))
              
            }, error = function(e) {
              
              if (verbose) message(paste(j, i, conditionMessage(e), sep=' | '))
              
            })
            
          } else {
            
            if (verbose) message(paste(j, i, 'Not enough data', sep=' | '))
            
          }
          
        }
      }
      
      return(glm_list)
    }
    
    parent_age_group <- '18-64'
    
    mods_child <- foreach(k = c('global', 'super_region', 'region', 'location'), .combine=c) %do% {
      
      .fit_child_hesitancy_glm(child_age_groups = child_age_groups,
                               parent_age_group = parent_age_group,
                               spatial_level = k,
                               data_survey = surv,
                               mods_vaccination = mods_vaccination_age,
                               mods_hesitancy = mods_hesitancy_age,
                               hierarchy = hierarchy,
                               min_samp_size=20,
                               remove_wave_12_trial=T,
                               verbose=T)
      
    }
    
    length(mods_child)
    
    
    
    
    message('Predicting child hesitancy using GLMs fitted to parent survey responses...')
    
    #suppressMessages(cl <- parallel::makeCluster(n_cores))
    #doParallel::registerDoParallel(cl)
    
    mods_hesitancy_child <- 
      foreach(j = child_age_groups, .combine=rbind) %:%
      foreach(i = unique(mods_hesitancy_age$location_id), .combine=rbind) %do% {
        
        # Get the fitted GLM. If missing, go up hierarchy of aggregates until one is found
        tmp_glm <- mods_child[[paste(j, i, sep='-')]]
        if (is.null(tmp_glm)) tmp_glm <- mods_child[[paste(j, hierarchy[location_id == i, parent_id], sep='-')]]
        if (is.null(tmp_glm)) tmp_glm <- mods_child[[paste(j, hierarchy[location_id == i, region_id], sep='-')]]
        if (is.null(tmp_glm)) tmp_glm <- mods_child[[paste(j, hierarchy[location_id == i, super_region_id], sep='-')]]
        if (is.null(tmp_glm)) tmp_glm <- mods_child[[paste(j, 1, sep='-')]]
        
        if (is.null(tmp_glm)) { 
          warning(glue("{j} | {i} | {hierarchy[location_id == i, location_name]} | ERROR: Could not find GLM"))
        } else { 
          message(glue("{j} | {i} | {hierarchy[location_id == i, location_name]} | GLM fit at {tmp_glm$spatial_level} level"))
        }
        
        # Gather covariates
        merge_cols <- c('location_id', 'date')
        tmp <- merge(mods_hesitancy_age[mods_hesitancy_age$age_group == parent_age_group & mods_hesitancy_age$location_id == i,],
                     mods_vaccination_age[mods_vaccination_age$age_group == parent_age_group & mods_vaccination_age$location_id == i, c(merge_cols, 'smooth_pct_vaccinated')],
                     by=merge_cols,
                     all.x=T)
        
        tmp_child <- mods_vaccination_age[mods_vaccination_age$age_group == j & mods_vaccination_age$location_id == i, c(merge_cols, 'smooth_pct_vaccinated')]
        colnames(tmp_child)[colnames(tmp_child) == 'smooth_pct_vaccinated'] <- 'smooth_pct_vaccinated_child'
        tmp <- merge(tmp, tmp_child, by=merge_cols, all.x=T); rm(tmp_child)
        
        # If observations of survey_yes exist for this location and age group, add them
        tmp$survey_yes <- NA
        tmp_data <- surv[surv$age_group == j & surv$location_id == i,]
        if (nrow(tmp_data) > 0) {
          tmp <- merge(tmp[,!colnames(tmp) %in% c('survey_yes', 'sample_size')], 
                       tmp_data[,c(merge_cols, 'survey_yes', 'sample_size')], 
                       by=merge_cols, 
                       all.x=T)
        }
        
        tmp$wave_12_trial <- tmp$date < as.Date('2021-12-19') # Date of wave 12 full launch
        
        pred <- tryCatch( { 
          
          predict(object=tmp_glm, newdata=tmp, type='response', se.fit=TRUE)
          
        }, error = function(e) {
          
          message('Model prediction failed. Finding an aggregate model...') # Hoping this will not be necessary after more data arrive
          # In some cases there is not enough pattern/data in a locations to fit and predict a model
          # If a model fails at prediction, try a model from higher level of the hierarchy/aggregate
          tmp_glm <- mods_child[[paste(j, hierarchy[location_id == i, region_id], sep='-')]]
          if (is.null(tmp_glm)) tmp_glm <- mods_child[[paste(j, hierarchy[location_id == i, super_region_id], sep='-')]]
          if (is.null(tmp_glm)) tmp_glm <- mods_child[[paste(j, 1, sep='-')]]
          predict(object=tmp_glm, newdata=tmp, type='response', se.fit=TRUE)
          
        })
        
        tmp$fit_level <- tmp_glm[['spatial_level']]
        
        tmp$smooth_survey_yes <- pred$fit
        tmp$smooth_survey_yes_H95 <- pred$fit+pred$se.fit*1.96
        tmp$smooth_survey_yes_L95 <- pred$fit-pred$se.fit*1.96
        
        tmp$age_group <- j
        a <- unlist(strsplit(j, split='[-]'))
        class(a) <- class(tmp$age_start)
        tmp$age_start <- a[1]
        tmp$age_end <- a[2]
        
        plot(tmp$date, tmp$smooth_survey_yes, ylim=c(0,1), type='l', 
             main=glue("{j} | {i} | {hierarchy[location_id == i, location_name]}"))
        lines(tmp$date, tmp$smooth_survey_yes_H95, lty=2)
        lines(tmp$date, tmp$smooth_survey_yes_L95, lty=2)
        
        tmp <- tmp[,colnames(tmp) %in% colnames(mods_hesitancy_age)]
        tmp
      }
    
    #stopCluster(cl)
    
    # Add child models to 
    mods_hesitancy_age <- rbind(mods_hesitancy_age, mods_hesitancy_child)
    
  }
  
  
  
  
  #-----------------------------------------------------------------------------
  message('Inferring missing locations in each age group...')
  
  cl <- parallel::makeCluster(n_cores)
  doParallel::registerDoParallel(cl)
  
  check <- function(a) nrow(x) == 0 | all(is.na(a$smooth_survey_yes))
  
  mods_hesitancy_age <- 
    foreach(i = hierarchy[, location_id], .combine=rbind) %:% 
    foreach(j = unique(mods_hesitancy_age$age_group), .combine=rbind, .packages=c('data.table', 'glue')) %dopar% {
      
      x <- mods_hesitancy_age[mods_hesitancy_age$location_id == i & mods_hesitancy_age$age_group == j,]
      
      # If the location is missing from models, find a parent model as substitute
      if (check(x)) x <- mods_hesitancy_age[mods_hesitancy_age$location_id == hierarchy[location_id == i, parent_id] & mods_hesitancy_age$age_group == j,]
      if (check(x)) x <- mods_hesitancy_age[mods_hesitancy_age$location_id == hierarchy[location_id == i, region_id] & mods_hesitancy_age$age_group == j,]
      if (check(x)) x <- mods_hesitancy_age[mods_hesitancy_age$location_id == hierarchy[location_id == i, super_region_id] & mods_hesitancy_age$age_group == j,]
      if (check(x)) x <- mods_hesitancy_age[mods_hesitancy_age$location_id == 1 & mods_hesitancy_age$age_group == j,]
      if (check(x)) warning(glue('{i} | {j} | Hesitancy model missing'))
      
      x$location_id <- i
      x
      
    }
  
  stopCluster(cl)
  
  mods_hesitancy_age <- .add_spatial_metadata(data=mods_hesitancy_age, hierarchy=hierarchy)
  
  
  
  
  
  # Check
  
  for (j in unique(mods_hesitancy_age$age_group)) {
    
    if (!all(hierarchy[level >= 1, location_id] %in% mods_hesitancy_age$location_id[mods_hesitancy_age$age_group == j])) {
      stop(glue('Location(s) missing for age group {j}'))
    } else {
      message(glue('Hesitancy models present for all locations in age group {j}'))
    }
  }
  
  #x1 <- surv[surv$location_id == 81 & surv$age_group == '0-125',]
  #x2 <- mods_hesitancy_age[mods_hesitancy_age$location_id == 81 & mods_hesitancy_age$age_group == '0-125',]
  
  #plot(x1$date, x1$survey_yes)
  #lines(x2$date, x2$smooth_survey_yes)
  
  sel <- c(1, 31, 32, 33, 48, 81, 100, 102, 570)
  g <- ggplot() +
    geom_point(data = surv[surv$location_id %in% sel,], 
               aes(x=date, y=survey_yes)) +
    geom_line(data = mods_hesitancy_age[mods_hesitancy_age$location_id %in% sel,], 
              aes(x=date, y=smooth_survey_yes), col='purple') +
    facet_grid(rows=vars(age_group), cols=vars(location_name)) +
    scale_x_date(labels = date_format("%b \n %Y")) +
    theme_classic()
  print(g)
  
  
  write.csv(surv, file=file.path(vaccine_output_root, 'observed_survey_data_age.csv'), row.names = F)
  write.csv(mods_hesitancy_age, file=file.path(vaccine_output_root, 'smooth_survey_yes_age.csv'), row.names = F)
  message(glue('Hesitancy results written to {vaccine_output_root}'))
  
}

