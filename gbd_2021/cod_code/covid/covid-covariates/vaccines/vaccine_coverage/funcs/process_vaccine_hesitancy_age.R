
process_survey_data_age <- function(vaccine_output_root,
                                    hierarchy,
                                    split_under_18 = FALSE  # Child hesitancy is reported as 0-17, when split_under_18 is TRUE, this function uses additional survey questions to split into finer age groups
) {
  
  # Get global args
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_path <- model_parameters$model_inputs_path
  
  age_starts <- model_parameters$age_starts
  include_all_ages <- model_parameters$include_all_ages
  n_cores <- model_parameters$n_cores
  
  use_limited_use <- model_parameters$use_limited_use
  yes_responses <- model_parameters$survey_yes_responses
  use_appointment <- model_parameters$survey_use_appointment
  
  
  #-----------------------------------------------------------------------------
  message('Loading adult hesitancy data')
  surv_raw <- as.data.frame(.load_vaccine_hesitancy_data(model_inputs_root=model_inputs_path, use_data_intake=TRUE))
  
  # Temporary fix until data extraction changes names
  tmp <- colnames(surv_raw)
  if ('age_from' %in% tmp) tmp[which(tmp == 'age_from')] <- 'age_start'
  if ('age_to' %in% tmp) tmp[which(tmp == 'age_to')] <- 'age_end'
  colnames(surv_raw) <- tmp
  
  # Add age-group IDs
  surv_raw$age_group <- stringr::str_c(surv_raw$age_start, '-', surv_raw$age_end)
  
  # Remove locations not used in COVID hierarchy
  sel <- surv_raw$location_id %in% hierarchy$location_id # Using ihme_loc_id because some location_id are missing
  tmp <- unique(surv_raw$location_name[!sel])
  surv_raw <- surv_raw[sel,]
  message(paste(length(tmp), 'locations are not in the COVID hierarchy and were removed'))
  
  # Check location in COVID hierarchy that are missing in survey data
  tmp <- !(hierarchy$location_id %in% surv_raw$location_id)
  message(paste(sum(tmp), 'locations in the COVID hierarchy are not present in survey data and must be inferred'))

  
  #-----------------------------------------------------------------------------
  # Build child data
  
  surv_child <- .build_child_hesitancy_data(vaccine_output_root = vaccine_output_root,
                                            hierarchy = hierarchy,
                                            split_under_18 = split_under_18,
                                            n_cores = n_cores)
  
  # This breaks the appointment arg, but we don't/won't use this anyway
  #surv$spatial_level <- 'location_id'
  surv <- rbind(surv_raw[,which(colnames(surv_raw) %in% colnames(surv_child))], surv_child)
  surv <- surv[!(is.na(surv$age_group) | surv$age_group == '18-125'),]
  


  #-----------------------------------------------------------------------------
  # Redistribute age groups 
  
  survey_cols <- c("yes", "yes_prob", "no_prob", "no", "total_respondents_hesitant")
  vax_cols <- c("vaccinated", "not_vaccinated", "unknown_vaccinated", "total_respondents_vaccination")
  app_cols <- c("appointment", "total_respondents_appointment")
  
  keep_cols <- c(survey_cols, vax_cols)
  if (use_appointment) keep_cols <- c(keep_cols, app_cols)
  surv <- surv[, c('location_id', 'date', 'age_start', 'age_end', 'age_group', keep_cols)]
  
  surv_rebin <- run_redistribute_ages(df=surv, 
                                      age_starts=age_starts,
                                      objective_columns=keep_cols,
                                      include_all_ages=include_all_ages,
                                      n_cores=n_cores)
  
  
 #tmp <- surv_rebin[surv_rebin$location_id %in% sel,]
 #ggplot(tmp) +
 #  geom_point(aes(x=date, y=total_respondents_hesitant), color = 'red', size=2) +
 #  geom_point(aes(x=date, y=total_respondents_vaccination), color ='blue', size=2) +
 #  facet_grid(vars(age_group),
 #             vars(location_id)) +
 #  theme_bw()


  # Add location info
  surv <- .add_spatial_metadata(data=surv_rebin, hierarchy=hierarchy)
  
  
  #-----------------------------------------------------------------------------
  message("Calculating proportion vaccinated")
  
  surv$pct_vaccinated <- surv$vaccinated / surv$total_respondents_vaccination
  surv[surv$sample_size == 0, 'pct_vaccinated'] <- NA
  
  tmp <- surv$pct_vaccinated[!is.na(surv$pct_vaccinated)]
  if (any(tmp > 1 | tmp < 0)) warning('Proportion vaccinated out of bounds in FB survey data')

  
  
  
  
  #-----------------------------------------------------------------------------
  message(paste("Calculating", yes_responses, "hesitancy (survey_yes)"))
  
  if(yes_responses == "default"){
    
    num <- if (use_appointment) surv$appointment + surv$yes + surv$yes_prob else surv$yes + surv$yes_prob
    
  } else if (yes_responses == "definitely"){
    
    num <- if (use_appointment) surv$appointment + surv$yes else surv$yes
    
  } else if (yes_responses == "not_no"){
    
    num <-  if (use_appointment) surv$appointment + surv$yes + surv$yes_prob + surv$no_prob else surv$yes + surv$yes_prob + surv$no_prob
    
  } else stop('yes_responses not recognized')
  
  den <- if (use_appointment) surv$total_respondents_hesitant + surv$total_respondents_appointment else surv$total_respondents_hesitant
  surv$survey_yes <- num / den
  surv$sample_size <- den
  
  surv[is.nan(surv$pct_vaccinated), 'pct_vaccinated'] <- NA
  surv[is.nan(surv$survey_yes), 'survey_yes'] <- NA
  
  #plot_surv_data(surv[surv$location_id == 570,])
  
  
  
  default_par <- par()
  par(mfrow=c(1,3))
  hist(surv$survey_yes, 
       xlab="Proportion of respondents answering 'yes'",
       main='Overall distribution of survey_yes',
       col='lightblue')
  
  
  #-----------------------------------------------------------------------------
  message('Squaring dates across location-age')
  
  sel <- which(!is.na(surv$survey_yes))
  date_start <- as.Date(min(surv$date[sel], na.rm=T))
  date_stop <- as.Date(max(surv$date[sel], na.rm=T))

  message(paste('Date range of survey_yes:', date_start, "--", date_stop))
  
  fill_cols <- c("age_start", "age_end", "age_group",
                 "location_id", "location_name",
                 "parent_id", 
                 "region_id", "region_name")  
  
  surv <- do.call(
    rbind,
    pbapply::pblapply(split(surv, list(surv$location_id, surv$age_group)), function(x) {
      
      tryCatch( { 
        
        date_series <- data.frame(date=seq.Date(date_start, date_stop, by=1))
        
        if (any(!(date_series$date %in% x$date))) {
          
          x <- merge(x, date_series, by='date', all.y=TRUE) # Add any skipped dates
          for (i in fill_cols) x[,i] <- .get_column_val(x[,i]) # Fill static column vals
          
        }
        
      }, error=function(e){
        
        cat("Warning :", unique(x$tmp), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
  # Set unobserved sample sizes to zero - these are used as weights in model
  sel <- which(is.na(surv$survey_yes))
  surv[sel, 'total_respondents_hesitant'] <- 0
  surv[sel, 'total_respondents_appointment'] <- 0
  
  
  
  #-------------------------------------------------------------------------------
  # Adjust survey responses for the shift from wave 10 (pre-May20) to wave 11 (post-May20)
  # Take mean of "survey_yes" for last few days prior to May-20 and get difference from
  # few days mean post May-20. Use this bias to scale "survey_yes" post May-20

  message('Fixing shift from wave 10 -> wave 11')
  surv <- do.call(
    rbind,
    pbapply::pblapply(split(surv, list(surv$location_id, surv$age_group)), function(x) {
      
      #message(unique(x$location_id))
      
      tryCatch( { # if data break model - return NAs and fill in after
        
        t <- as.Date("2021-05-20")
        delta_t <- 7 # match mean of week prior to mean of week post
        mean_prior <- mean(x$survey_yes[x$date >= t-delta_t & x$date <= t], na.rm=TRUE)
        mean_post <- mean(x$survey_yes[x$date > t & x$date <= t+delta_t], na.rm=TRUE)
        bias <- mean_prior - mean_post
        sel <- x$date > t
        if (!is.nan(bias)) x$survey_yes[sel] <- x$survey_yes[sel] + bias
        
      }, error = function(e){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$age_group), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
  row.names(surv) <- NULL
  
  hist(surv$survey_yes, 
       xlab="Proportion of respondents answering 'yes'",
       main='After wave 10 -> wave 11 fix')
  abline(v=c(0,1), lty=2, col='red', lwd=2)
  
  # Quick check
  #tmp <- surv$survey_yes[!is.na(surv$survey_yes)]
  #if (any(tmp > 1 | tmp < 0)) {
  #  warning('Making manual adjustment to proportion hesitant out of bounds')
  #  surv[which(surv$survey_yes > 1), 'survey_yes'] <- 1
  #  surv[which(surv$survey_yes < 0), 'survey_yes'] <- 0
  #} 
  
  surv$survey_yes <- .do_manual_bounds_proportion(surv$survey_yes)


  hist(surv$survey_yes, 
       xlab="Proportion of respondents answering 'yes'",
       main='Bounded to [0,1]',
       col='lightgreen')
  suppressWarnings(par(default_par))


  tmp_name <- file.path(vaccine_output_root, 'observed_survey_data_age.csv')
  write.csv(surv, file=tmp_name, row.names = F)
  message(glue('Processed hesitancy data written to: {tmp_name}'))

}




plot_surv_data <- function(x) {
  
  if (!('location_name' %in% colnames(x))) {
    h <- .get_covid_modeling_hierarchy()
    x <- merge(x, h[,.(location_id, location_name)], by='location_id', all.x=T)
  }
  
  x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
  
  pal <- .get_pal(9)
  ggplot(x) +
    geom_point(aes(x=date, y=survey_yes), color = pal[1], size=2) +
    geom_point(aes(x=date, y=pct_vaccinated), color = pal[2], size=2) +
    facet_grid(vars(age_group),
               vars(location_name)) +
    theme_bw()
  
}