.build_child_hesitancy_data <- function(vaccine_output_root,
                                        hierarchy,
                                        split_under_18=FALSE, # Survey data come as 0-17. If this arg is TRUE, it will use questions P3 and J4 to proporionaly split into finer age groups
                                        n_cores=1
) {
  
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_path <- model_parameters$model_inputs_path
  use_limited_use <- model_parameters$use_limited_use
  
  child_age_start <- 0
  child_age_end <- 17
  child_age_group <- '0-17'
  
  #-----------------------------------------------------------------------------
  message('Compiling US and Global CTIS data for under-18 age group...')
  .symptom_surveys <- model_inputs_data$load_symptom_survey_data(model_inputs_path, use_limited_use)
  
  us <- .symptom_surveys$us_states
  global <- .symptom_surveys$global
  us[, date := as.Date(date)]
  global[, date := as.Date(date)]
  
  us <- us[us$variable %in% paste0('P', 2:3)]
  global <- global[global$variable %in% paste0('J', 4:5)]
  
  # Child survey responses
  # Global: J3-J5
  #
  # J3: Are you the parent or legal guardian of any children under age 18?
  # Responses: yes (1), no (2)
  #
  # J4: Thinking about your oldest child under age 18, how old are they?
  # Responses: Under 5 years old (1), 5 to 11 years old (2), 12 to 15 years old (3), 16 to 17 years old (4)
  #
  # J5: Thinking about your oldest child under age 18, will you choose to get them vaccinated against COVID-19 when they are eligible?
  # Responses: They are already vaccinated for COVID-19 (5), Yes definitely (1), Yes probably (2), No probably not (3), No definitely not (4) 
  
  # US: P1-P3
  #
  # P1:	Are you the parent or legal guardian of any children under age 18?	
  # Responses: Yes (1), No (2)
  #
  # P2:	Thinking about your oldest child under age 18, how old are they?	
  # Responses: Under 5 years old (1), 5 to 11 years old (2), 12 to 15 years old (3), 16 to 17 years old (4) 
  #
  # P3: Thinking about your oldest child under age 18, will you choose to get them vaccinated against COVID-19 when they are eligible?	
  # Responses: They are already vaccinated for COVID-19 (5) Yes definitely (1) Yes probably (2)  No probably not (3)  No definitely not (4)
  
  # Column 'obs': The number of respondents who answered with the specified answer for the specified question (e.g. 
  # The number of participants who responded ‘yes’ to the question ‘Have you received the Covid-19 vaccine?)
  
  
  #-----------------------------------------------------------------------------
  # Build child vaccinations
  
  message('Building vaccination and hesitancy data for under-18 age group')
  
  #df <- us[us$variable == 'P3',]
  #x <- us[us$location_id == 523 & us$date == as.Date('2021-10-07') & us$variable == 'P3',]
  
  .get_vaccination_responses <- function(df) {
    
    df <- as.data.frame(df)
    df <- df[df$value %in% as.character(1:5),]
    
    out <- do.call(
      rbind,
      pbmcapply::pbmclapply(
        X=split(df, list(df$location_id, df$date)),
        mc.cores=n_cores,
        FUN=function(x) {
          
          tryCatch( {
            
            data.frame(location_id = .get_column_val(x$location_id),
                       date = as.Date(unique(x$date)[1]),
                       age_start = child_age_start,
                       age_end = child_age_end,
                       age_group = child_age_group,
                       yes = ifelse(1 %in% x$value, x$obs[x$value == 1], 0),
                       yes_prob = ifelse(2 %in% x$value, x$obs[x$value == 2], 0),
                       no_prob = ifelse(3 %in% x$value, x$obs[x$value == 3], 0),
                       no = ifelse(4 %in% x$value, x$obs[x$value == 4], 0),
                       total_respondents_hesitant = sum(x$obs[x$value != 5], na.rm=T),
                       vaccinated = ifelse(5 %in% x$value, x$obs[x$value == 5], 0),
                       not_vaccinated = sum(x$obs[x$value != 5], na.rm=T),
                       unknown_vaccinated = 0,
                       total_respondents_vaccination = sum(x$obs, na.rm=T),
                       total_surveyed = sum(x$obs, na.rm=T))
            
          }, error = function(e) {
            
            message(paste('ERROR:', unique(x$location_id), unique(x$date), conditionMessage(e)))
            
          })
        }
      )
    )
    
    out <- out[complete.cases(out),]
    row.names(out) <- NULL
    return(out)
  }
  
  surv_child <- rbind(.get_vaccination_responses(df=us[us$variable == 'P3',]),
                      .get_vaccination_responses(df=global[global$variable == 'J5',]))
  
  
  
  
  
  if (split_under_18 == FALSE) {
    
    return(surv_child)
    
  } else if (split_under_18) {
    
    #-----------------------------------------------------------------------------
    # Split 0-17 age group
    
    message('Splitting under-18 age group into component age groups...')
    
    # Under 5 years old (1), 5 to 11 years old (2), 12 to 15 years old (3), 16 to 17 years old (4)
    age_group_key <- data.frame(value=as.character(1:4),
                                age_start=c(0,5,12,16),
                                age_end=c(4,11,15,17))
    
    age_group_key$age_group <- paste(age_group_key$age_start, age_group_key$age_end, sep='-')
    
    #tmp <- surv_child[surv_child$location_id == 523 & surv_child$date == as.Date('2021-10-07'),]
    #x <- us[us$location_id == 524 & us$date == as.Date('2021-10-07') & us$variable == 'P2',]
    
    .get_child_age_groups <- function(df) {
      
      df <- as.data.frame(df)
      df <- df[df$value %in% as.character(1:4),]
      
      out <- do.call(
        rbind,
        pbmcapply::pbmclapply(
          X=split(df, list(df$location_id, df$date)),
          mc.cores=n_cores,
          FUN=function(x) {
            
            tryCatch( {
              
              x <- merge(x, age_group_key, by='value', all=T)
              
              data.frame(location_id = .get_column_val(x$location_id),
                         date = as.Date(unique(x$date)[1]),
                         age_start = x$age_start,
                         age_end = x$age_end,
                         age_group = x$age_group,
                         obs = x$obs,
                         total_respondents = sum(x$obs, na.rm=TRUE))

            }, error = function(e) {
              
              message(paste('ERROR:', unique(x$location_id), unique(x$date), conditionMessage(e)))
              
            })
          }
        )
      )
      
      out <- out[complete.cases(out[,c('location_id', 'date')]),]
      out$obs[is.na(out$obs)] <- 0
      out$prop <- out$obs/out$total_respondents
      row.names(out) <- NULL
      return(out)
    }
    
    
    surv_child_age_groups_observed <- rbind(.get_child_age_groups(df = us[us$variable == 'P2',]),
                                            .get_child_age_groups(df = global[global$variable == 'J4',]))
    
    surv_child_age_groups_observed <- merge(surv_child_age_groups_observed, 
                                            hierarchy[,.(location_id, location_name, parent_id, region_id, region_name, super_region_id, super_region_name)], 
                                            by='location_id', 
                                            all.x=TRUE)
    
    # There are not enough observations for the age group questions, so we must calculate some averages to use at different spatial scales
    #spatial_level <- 'super_region_id'
    #x <- surv_age_groups[surv_age_groups$super_region_id == 64,]
    
    .aggregate_child_age_groups <- function(df, spatial_level) {
      
      message(spatial_level)
      if (spatial_level == 'global') df$global <- 1
      
      out <- do.call(
        rbind,
        pbmcapply::pbmclapply(
          X=split(df, list(df[,spatial_level])),
          mc.cores=n_cores,
          FUN=function(x) {
            
            tryCatch( {
              
              tmp <- aggregate(x$obs, by=list(x$age_group), FUN=function(x) sum(x, na.rm=TRUE))
              tmp <- cbind(do.call(rbind, strsplit(tmp[,1], split='-')), tmp)
              names(tmp) <- c('age_start', 'age_end', 'age_group', 'obs')
              tmp$total_respondents <- sum(x$total_respondents, na.rm=TRUE) / length(unique(x$age_group))
              tmp$spatial_level <- spatial_level
              
              if (sum(tmp$obs) != .get_column_val(tmp$total_respondents)) {
                
                stop('Aggregation failed')
                
              } else if (.get_column_val(tmp$total_respondents) < 30) {
                
                stop('Sample size < 30')
                
              } else {
                
                data.frame(location_id = .get_column_val(x[,spatial_level]), tmp)
                
              }
              
            }, error = function(e) {
              
              message(paste('ERROR:', unique(x$location_id), unique(x$date), conditionMessage(e)))
              
            })
          }
        )
      )
      
      out$obs[is.na(out$obs)] <- 0
      out$prop <- out$obs/out$total_respondents
      row.names(out) <- NULL
      return(out)
    }
    
    # Numbers of each age group aggregated across date and location (just date in the case of location-level)
    tmp <- rbind(
      .aggregate_child_age_groups(df=surv_child_age_groups_observed, spatial_level = 'global'),
      .aggregate_child_age_groups(df=surv_child_age_groups_observed, spatial_level = 'super_region_id'),
      .aggregate_child_age_groups(df=surv_child_age_groups_observed, spatial_level = 'region_id'),
      .aggregate_child_age_groups(df=surv_child_age_groups_observed, spatial_level = 'parent_id'),
      .aggregate_child_age_groups(df=surv_child_age_groups_observed, spatial_level = 'location_id')
    )
    
    surv_child_age_groups_aggregates <- 
      foreach(i = hierarchy[most_detailed == 1, location_id], .combine=rbind) %do% {
        
        x <- tmp[tmp$location_id == i,] # If location level is missing, go up the hierarchy until aggregate found
        if (nrow(x) == 0) x <- tmp[tmp$location_id == hierarchy[location_id == i, parent_id],]
        if (nrow(x) == 0) x <- tmp[tmp$location_id == hierarchy[location_id == i, region_id],]
        if (nrow(x) == 0) x <- tmp[tmp$location_id == hierarchy[location_id == i, super_region_id],]
        if (nrow(x) == 0) x <- tmp[tmp$location_id == 1,] # Global
        x$location_id <- i
        x
        
      }
    
    if (!all(hierarchy[most_detailed == 1, location_id] %in% surv_child_age_groups_aggregates$location_id)) {
      stop(glue('Location(s) missing from surv_child_age_groups'))
    } 
    
    
    # Do proportional split of 0-17 age group (by date where possible, by aggregate proportions elsewhere)
    
    #i <- 523
    
    surv_child_split <- 
      foreach(i=unique(surv_child$location_id), .combine=rbind) %do% {
      
      x <- surv_child[surv_child$location_id == i,]
      a <- surv_child_age_groups_observed[surv_child_age_groups_observed$location_id == i,]
      
      if (nrow(a) == 0) {
        a <- surv_child_age_groups_aggregates[surv_child_age_groups_aggregates$location_id == i,]
        a <- foreach(j=x$date, .combine=rbind) %do% data.frame(date=as.Date(j), a)
      }
      
      # Survey responses to be split by age (leave totals alone)
      response_cols <- c("yes", "yes_prob", "no_prob", "no", "total_respondents_hesitant",   
                         "vaccinated", "not_vaccinated", "unknown_vaccinated", "total_respondents_vaccination",
                         "total_surveyed")
      
      age_cols <- c("age_start", "age_end", "age_group")
      
      tmp <- merge(x[,c('location_id', 'date', response_cols)], 
                   a[,c('date', age_cols, 'prop')], 
                   by='date',
                   all=T)
      
      for (k in response_cols) tmp[,k] <- tmp[,k] * tmp$prop
      tmp <- tmp[,colnames(tmp) != 'prop']
      tmp
      
    }
    
    return(surv_child_split)
    
  }
  
}