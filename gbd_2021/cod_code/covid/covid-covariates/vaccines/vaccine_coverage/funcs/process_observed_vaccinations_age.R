process_observed_vaccinations_age <- function(vaccine_output_root, 
                                              hierarchy,
                                              population
) {
  
  .print_header_message('Processing age-stratified vaccination data')
  
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_root <- model_parameters$model_inputs_path
  
  age_starts <- model_parameters$age_starts
  include_all_ages <- model_parameters$include_all_ages
  n_cores <- model_parameters$n_cores
  
  all_age_start <- age_starts[1]
  all_age_end <- 125
  all_age_group <- paste0(all_age_start, '-', all_age_end)
  
  age_groups <- .get_age_groups(age_starts)
  
  message('Loading all-age vaccination data...')
  observed_all_age <- vaccine_data$load_observed_vaccinations(vaccine_output_root)
  observed_all_age$date <- as.Date(observed_all_age$date)
  
  message('Loading age-stratified vaccination data...')
  observed_age <- model_inputs_data$load_new_vaccination_data(model_inputs_root=model_inputs_root, type='age')
  observed_age$age_group <- paste0(observed_age$age_start, '-', observed_age$age_end)
  
  # Load data on date of Emergency Use Authorization for younger age groups
  eua_dates <- fread(file.path(vaccine_output_root, 'emergency_use_authorizations.csv'))
  
  vax_quantities <- colnames(observed_age)[which(colnames(observed_age) %like% 'vaccinated' | colnames(observed_age) %like% 'administered')]
  
  
  if (!('hongkong' %in% observed_age$source)) {
    
    message('Adding Hong Kong manually from data_intake')
    tmp <- fread('FILEPATH/hongkong_vaccination_age.csv')
    tmp$source <- 'hongkong'
    tmp$date <- .convert_dates(tmp$date)
    tmp$age_group <- paste(tmp$age_start, tmp$age_end, sep='-')
    colnames(tmp)[colnames(tmp) == 'location'] <- 'location_name'
    tmp <- tmp[, nid := NULL]
    observed_age <- rbind(observed_age, tmp)
    rm(tmp)
    
  }
  
  observed_age$observed <- 1
  
  #---------------------------------------------------------------------------------------------
  # For select countries: split all-age proportionally based on age data from alternate location
  #---------------------------------------------------------------------------------------------
  
  tmp <- split_ages_from_neighbor(loc_x = 6, # China
                                  loc_y = 354, # Hong Kong Special Administrative Region of China
                                  observed_age = observed_age,
                                  model_inputs_root = model_inputs_root,
                                  hierarchy = hierarchy,
                                  vax_quantities = vax_quantities)
  
  observed_age <- rbind(observed_age, tmp)
  
  tmp <- split_ages_from_neighbor(loc_x = 44533, # China (not Hong Kong or Macao)
                                  loc_y = 354, # Hong Kong Special Administrative Region of China
                                  observed_age = observed_age,
                                  model_inputs_root = model_inputs_root,
                                  hierarchy = hierarchy,
                                  vax_quantities = vax_quantities)
  
  observed_age <- rbind(observed_age, tmp)
  
  tmp <- split_ages_from_neighbor(loc_x = 361, # China (not Hong Kong or Macao)
                                  loc_y = 354, # Hong Kong Special Administrative Region of China
                                  observed_age = observed_age,
                                  model_inputs_root = model_inputs_root,
                                  hierarchy = hierarchy,
                                  vax_quantities = vax_quantities)
  
  observed_age <- rbind(observed_age, tmp)
  
  
  
  #-----------------------------------------------------------------------------
  # Split by source and apply source-specific processing via functions below
  #-----------------------------------------------------------------------------
  
  message('Age-stratified data by sources:')
  print(unique(observed_age$source))
  
  #x <- observed_age[source == 'switzerland',]
  
  out <- do.call(
    rbind,
    lapply(split(observed_age, by='source'), function(x) {
      
      tryCatch( {
        
        if (na.omit(x$source)[1] == 'cdc') {
          
          .process_observed_cdc_age(observed_cdc = x, 
                                    age_starts = age_starts,
                                    eua_dates = eua_dates,
                                    vax_quantities = vax_quantities,
                                    hierarchy = hierarchy,
                                    vaccine_output_root = vaccine_output_root,
                                    n_cores = n_cores)
          
        } else if (na.omit(x$source)[1] == 'coveragedb') {
          
          .process_observed_mpl_age(observed_mpl = x,
                                    age_starts = age_starts,
                                    vax_quantities = vax_quantities,
                                    eua_dates = eua_dates,
                                    age_groups = age_groups,
                                    hierarchy = hierarchy,
                                    vaccine_output_root = vaccine_output_root,
                                    n_cores = n_cores) 
          
        } else if (na.omit(x$source)[1] == 'europe') {
          
          .process_observed_europe_age(observed_eur = x, 
                                       age_starts = age_starts,
                                       vax_quantities = vax_quantities,
                                       hierarchy = hierarchy,
                                       vaccine_output_root = vaccine_output_root,
                                       n_cores = n_cores)
          
        } else if (na.omit(x$source)[1] == 'italy') {
          
          .process_observed_italy_age(observed_ita = x, 
                                      age_starts = age_starts,
                                      vax_quantities = vax_quantities,
                                      hierarchy = hierarchy,
                                      vaccine_output_root = vaccine_output_root,
                                      n_cores = n_cores)
          
        } else if (na.omit(x$source)[1] == 'canada') {
          
          .process_observed_canada_age(observed_can = x,
                                       age_starts = age_starts,
                                       vax_quantities = vax_quantities,
                                       eua_dates = eua_dates,
                                       population = population,
                                       hierarchy = hierarchy, 
                                       vaccine_output_root = vaccine_output_root,
                                       n_cores = n_cores)
          
        } else if (na.omit(x$source)[1] == 'brazil') {
          
          .process_observed_brazil_age(observed_brz = x,
                                       age_starts=age_starts,
                                       vax_quantities = vax_quantities,
                                       eua_dates = eua_dates,
                                       population = population,
                                       hierarchy = hierarchy, 
                                       vaccine_output_root = vaccine_output_root,
                                       n_cores = n_cores)
          
        } else if (na.omit(x$source)[1] == 'india') {
          
          .process_observed_india_age(observed_ind = x, 
                                      observed_all_age = observed_all_age,
                                      age_starts = age_starts,
                                      vax_quantities = vax_quantities,
                                      eua_dates = eua_dates,
                                      vaccine_output_root = vaccine_output_root,
                                      n_cores = n_cores)
          
        }  else if (na.omit(x$source)[1] == 'southafrica') {
          
          .process_observed_southafrica_age(observed_saf = x, 
                                            observed_all_age = observed_all_age,
                                            age_starts = age_starts,
                                            vax_quantities = vax_quantities,
                                            eua_dates = eua_dates,
                                            hierarchy = hierarchy, 
                                            vaccine_output_root = vaccine_output_root,
                                            n_cores = n_cores) 
          
        } else if (na.omit(x$source)[1] == 'ecuador') {
          
          .process_observed_ecuador_age(observed_ecu = x, 
                                        age_starts = age_starts,
                                        vax_quantities = vax_quantities,
                                        eua_dates = eua_dates,
                                        hierarchy = hierarchy, 
                                        vaccine_output_root = vaccine_output_root,
                                        n_cores = n_cores) 
          
        } else if (na.omit(x$source)[1] %in% c('switzerland', 'peru', 'hongkong')) {
          
          .process_observed_default_age(observed_def = x, 
                                        age_starts = age_starts,
                                        vax_quantities = vax_quantities,
                                        eua_dates = eua_dates,
                                        hierarchy = hierarchy, 
                                        vaccine_output_root = vaccine_output_root,
                                        n_cores = n_cores) 
          
        } else if (na.omit(x$source)[1] == 'some_other_source') {
          
          
        }
        
      }, error = function(e){
        
        cat("ERROR |", unique(x$source), "|", conditionMessage(e), "\n")
        
      }) 
    })
  )
  
  
  
  
  
  
  #############################################################################################
  
  #############################################################################################
  
  # CDC data are okay except for Northern Mariana Islands
  gandalf_locs <- c(376, 380) # Northern Mariana Islands, Palau
  out <- out[!(out$source == 'cdc' & out$location_id %in% gandalf_locs),]
  
  # Max Planck
  # Need to get more EUAs for these locations
  gandalf_locs <- c('United States of America')
  sel <- out$source == 'coveragedb' & out$location_name %in% gandalf_locs
  out <- out[!(out$source == 'coveragedb' & out$location_name %in% gandalf_locs),]
  
  #############################################################################################
  #############################################################################################
  
  #vax_quantities <- colnames(out)[which(colnames(out) %like% 'vaccinated' | colnames(out) %like% 'administered')]
  
  # Add an all ages group that is the sum of all age groups 
  
  if (include_all_ages) {
    
    message(glue('Making an all-age group ({all_age_group}) from the sum of each age group'))
    
    # Some sources (CDC) already have 12-125 age group
    out <- out[!(out$age_group == all_age_group),]
    
    tmp <- out %>%
      group_by(location_id, date) %>%
      summarize_at(.vars=vax_quantities, .funs=sum) 
    
    tmp$age_start <- all_age_start
    tmp$age_end <- all_age_end
    tmp$age_group <- stringr::str_c(tmp$age_start, '-', tmp$age_end)
    tmp$brand <- 'All'
    tmp <- merge(tmp, hierarchy[,.(location_id, location_name)], by='location_id', all.x=T)
    
    sel <- which(colnames(out) %in% colnames(tmp))
    out <- rbind(out[,sel], tmp); rm(tmp)
    row.names(out) <- NULL
  }
  
  
  
  #-----------------------------------------------------------------------------
  # Infer missing child locations by dis-aggregating parent locations
  # Only dis-aggregates parent location if not all child locations are present
  #-----------------------------------------------------------------------------
  
  missing_children <- infer_missing_children(object = out,
                                             objective_cols = vax_quantities,
                                             population = population,
                                             hierarchy = hierarchy,
                                             verbose = TRUE)
  
  out <- rbind(out, missing_children)
  
  
  #-----------------------------------------------------------------------------
  # Make aggregate locations
  #-----------------------------------------------------------------------------
  
  
  
  # Check
  #plot_raw_data(out[out$location_id %in% c(570, 60886, 3539, 60887),])
  #plot_raw_data(out[out$location_id %in% c(48, 71, 81, 570, 98, 523),])
  
  # Check for duplicates
  dups <- duplicated(out[,c('location_id', 'date', 'age_group')])
  if (sum(dups) > 0) {
    message('Had to remove duplicates after data processing')
    out <- out[!dups,]
  }
  
  # Save
  tmp_name <- file.path(vaccine_output_root, "observed_vaccination_data_age.csv")
  write.csv(out, tmp_name, row.names = F)
  message(glue('Processed age-stratified data saved here: {tmp_name}'))
}





################################################################################
# Functions for specific data sets
################################################################################

# Quick plotting method for age-stratified data dataframe
plot_raw_data <- function(x) {
  
  if (!('location_name' %in% colnames(x))) {
    h <- .get_covid_modeling_hierarchy()
    x <- merge(x, h[,.(location_id, location_name)], by='location_id', all.x=T)
  }
  
  x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
  
  pal <- .get_pal(9)
  ggplot(x) +
    geom_point(aes(x=date, y=initially_vaccinated), color = pal[1], size=2) +
    geom_point(aes(x=date, y=fully_vaccinated), color = pal[2], size=2) +
    geom_point(aes(x=date, y=total_administered), color = pal[3], size=2) +
    geom_point(aes(x=date, y=boosters_administered), color = pal[4], size=2) +
    facet_grid(vars(age_group),
               vars(location_name)) +
    theme_bw()
  
}


# Standarized methods for setting vaccination quantities to zero berfore EUA date
# WHen EUA date not available, function will infer the regional average
# For adult age groups, first data >0 in observed all-age data is used
# Expects a dataframe or datatable long by location-age





.process_observed_cdc_age <- function(observed_cdc = observed_age[source == 'cdc'], 
                                      age_starts,
                                      observed_all_age,
                                      eua_dates,
                                      vax_quantities,
                                      hierarchy, 
                                      vaccine_output_root,
                                      n_cores) {
  
  .print_header_message('CDC')
  #vax_quantities <- colnames(observed_cdc)[which(colnames(observed_cdc) %like% 'vaccinated' | colnames(observed_cdc) %like% 'administered')]
  observed_cdc[, date := date - 3] # Shift states by 3 day lag (per INDIVIDUAL_NAME)
  
  pdf(file.path(vaccine_output_root, glue("data_raw_age_cdc.pdf")), width=6, height=9, onefile = TRUE)
  for (i in unique(observed_cdc$location_id)) {
    p <- plot_raw_data(observed_cdc[location_id == i,])
    print(p)
  }
  dev.off()
  
  message(glue('Raw CDC age groups: ', paste(unique(observed_cdc$age_group), collapse=' | ')))
  message('Calculating discrete age groups...')
  
  #x <- observed_cdc[location_id == '523']
  
  get_age_group_difference <- function(x, age_1, age_2) {
    
    x <- as.data.table(x)
    
    out <- data.table(date=x[age_start == age_1, date])
    
    # Do age group differences for each of the vaccination quantities (initial, fully, total)
    for (i in vax_quantities) {
      
      sel <- c('date', i)
      
      # Line up the two age groups by date
      tmp <- as.data.frame(merge(x[age_start == age_1, ..sel], 
                                 x[age_start == age_2, ..sel], 
                                 by='date', 
                                 all.y=T))
      
      vax_cols <- c(paste0(i, '.x'), paste0(i, '.y'))    # Which columns are the vaccination quantities
      tmp$new <- tmp[,vax_cols[1]] - tmp[, vax_cols[2]]  # Get difference
      tmp$new[tmp$new < 0] <- 0
      colnames(tmp)[which(colnames(tmp) == 'new')] <- i  # Add appropriate name
      tmp <- tmp[, !(colnames(tmp) %in% vax_cols)]       # Drop merge columns
      out <- merge(out, tmp, by='date', all.x=TRUE)      # Collect adjusted vaccination quantities
    }
    
    out$age_start <- age_1
    out$age_end <- age_2 - 1
    return(out) 
  }
  
  
  out <- data.table()
  for (i in unique(observed_cdc$location_id)) {
    
    #message(i)
    x <- observed_cdc[location_id == i,]
    
    x_loc <- .get_column_val(x$location_id)
    x$location_name <- hierarchy[location_id == x_loc]$location_name # Many of the two-name locs are messed up
    date_range <- c(as.Date("2020-12-01"), max(x$date))
    
    tmp <- rbind(get_age_group_difference(x, age_1=5, age_2=12), # Currently no 0-4 age group
                 get_age_group_difference(x, age_1=12, age_2=18),
                 
                 get_age_group_difference(x, age_1=18, age_2=65),
                 x[age_start == 65,],
                 fill=TRUE)
    
    if (nrow(tmp) == 0) {
      
      message(glue('Cannot calculate age group differences for location {i} | {x$location_name[1]}'))
      next 
      
    } else {
      
      tmp$age_group <- stringr::str_c(tmp$age_start, '-', tmp$age_end)
      
      tmp <- .knock_out_rows_by_date(object=tmp, date1='2021-03-17')
      tmp <- .knock_out_rows_by_date(object=tmp, date1='2021-03-21')
      
      # Ensure no dates missing
      tmp <- do.call(
        rbind,
        lapply(split(tmp, by='age_group'), function(x) {
          
          x <- merge(x, 
                     data.table(date=as.Date(date_range[1]:date_range[2])), 
                     by='date', all=T)
          
          x$age_start <- .get_column_val(x$age_start)
          x$age_end <- .get_column_val(x$age_end)
          return(x)
        }))
      
      tmp$age_group <- stringr::str_c(tmp$age_start, '-', tmp$age_end)
      tmp$location_id <- .get_column_val(x$location_id)
      tmp$location_name <- .get_column_val(x$location_name)
      tmp$brand <- .get_column_val(x$brand)
      tmp$source <- .get_column_val(x$source)
      
      out <- rbind(out, tmp, fill=T)
      
    }
  }
  
  out_rebin <- run_redistribute_ages(df=out, 
                                     age_starts=age_starts,
                                     objective_columns=vax_quantities, 
                                     population_weighted = FALSE,
                                     n_cores=n_cores)
  
  f <- function(x) {
    for (i in vax_quantities) x[[i]][x[[i]] == 0] <- NA
    return(x)
  }
  
  out_rebin <- .apply_func_location_age(object=out_rebin, func=f, n_cores=n_cores)
  
  out <- set_eua_dates(object=out_rebin,
                       eua_dates=eua_dates,
                       vax_quantities=vax_quantities)
  
  plot_raw_data(out[out$location_id == 570,])
  
  
  pdf(file.path(vaccine_output_root, glue("data_proc_age_cdc.pdf")), width=6, height=9, onefile = TRUE)
  for (i in unique(out$location_id)) {
    p <- plot_raw_data(out[out$location_id == i,])
    print(p)
  }
  dev.off()
  
  out$source <- 'cdc'
  row.names(out) <- NULL
  return(out)
}


.process_observed_mpl_age <- function(observed_mpl = observed_age[source == 'coveragedb'],
                                      age_starts,
                                      vax_quantities,
                                      eua_dates,
                                      age_groups,
                                      hierarchy, 
                                      vaccine_output_root,
                                      n_cores
) {
  
  .print_header_message('Max Planck')
  
  observed_mpl <- observed_mpl[which(!(observed_mpl$location_name == 'United States of America')),]
  
  message('Plotting raw data...')
  pdf(file.path(vaccine_output_root, glue("data_raw_age_max_planck.pdf")), width=6, height=9, onefile = TRUE)
  for (i in unique(observed_mpl$location_id)) {
    p <- plot_raw_data(observed_mpl[location_id == i,])
    print(p)
  }
  dev.off()
  
  rebin_mpl <- run_redistribute_ages(df=observed_mpl, 
                                     age_starts=age_starts,
                                     objective_columns=vax_quantities, 
                                     n_cores=n_cores)
  
  rebin_mpl <- as.data.table(rebin_mpl)
  rebin_mpl$age_group <- factor(rebin_mpl$age_group, levels=age_groups)
  rebin_mpl <- rebin_mpl[order(age_group),]
  
  if (F) {
    x <- rebin_mpl[location_id == 4749 & age_group == '65-125',]
    x <- rebin_mpl[location_name == 'Uruguay' & age_group == '18-39',]
    plot_raw_data(x)
  }
  
  message('Imputing missing quantities and enforcing EUA dates...')
  pdf(file.path(vaccine_output_root, glue("data_proc_age_max_planck.pdf")), width=11, height=8, onefile = TRUE)
  par(mfrow=c(3, length(unique(rebin_mpl$age_group))))
  
  out <- do.call(
    rbind,
    lapply(split(rebin_mpl, by=c('location_id', 'age_group')), function(x) {
      
      tryCatch( {
        
        x_loc <- .get_column_val(x$location_id)
        x_age <- .get_column_val(x$age_group)
        
        x$location_name <- hierarchy[location_id == x_loc]$location_name # Many of the two-name locs are messed up
        date_range <- c(as.Date("2020-12-16"), as.Date(Sys.Date()))
        
        x <- merge(x, 
                   data.table(date=as.Date(date_range[1]:date_range[2])), 
                   by='date', 
                   all=T)
        
        x$age_start <- .get_column_val(x$age_start)
        x$age_end <- .get_column_val(x$age_end)
        x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
        x$location_id <- .get_column_val(x$location_id)
        x$location_name <- .get_column_val(x$location_name)
        x$brand <- .get_column_val(x$brand)
        x$source <- .get_column_val(x$source)
        
        x <- set_eua_dates(object = x,
                           eua_dates = eua_dates,
                           vax_quantities = vax_quantities)
        
        prop_first <- x$initially_vaccinated / x$total_administered
        prop_first[is.nan(prop_first)] <- 0
        x$fully_vaccinated <- x$total_administered * (1 - prop_first)
        
        for (i in vax_quantities) x[[i]] <- .remove_non_cumulative(x[[i]])
        
        # Adjust for cases when initially vaccinated > fully vaccinated
        check <- x$initially_vaccinated < x$fully_vaccinated
        if (any(check[!is.na(check)])) x$initially_vaccinated <- pmax(x$initially_vaccinated, x$fully_vaccinated)
        
        plot(x$date, x$total_administered, 
             ylab='Total vaccinated',
             main=paste(x$location_name[1], x$age_group[1], sep=' | '))
        abline(v=eua_dates[eua_dates$location_id == x_loc & eua_dates$age_group == x_age, 'date'], lty=2)
        points(x$date, x$initially_vaccinated, col='blue')
        points(x$date, x$fully_vaccinated, col='red')
        points(x$date, x$boosters_administered, col='green3')
        
        return(x)
        
      }, error=function(e){
        
        cat("ERROR :", unique(x$location_id), ":", unique(x$location_name), ":", unique(x$age_group), ":", conditionMessage(e), "\n")
        
      })
    }))
  
  dev.off()
  par(mfrow=c(1,1))
  
  row.names(out) <- NULL
  return(out)
}


.process_observed_europe_age <- function(observed_eur = observed_age[source == 'europe'], 
                                         age_starts,
                                         vax_quantities,
                                         hierarchy, 
                                         vaccine_output_root,
                                         n_cores) {
  
  .print_header_message('Europe')
  
  pdf(file.path(vaccine_output_root, glue("data_raw_age_europe.pdf")), width=6, height=9, onefile = TRUE)
  for (i in unique(observed_eur$location_id)) {
    p <- plot_raw_data(observed_eur[location_id == i,])
    print(p)
  }
  dev.off()
  
  message(glue('Raw Europe age groups: ', paste(unique(observed_eur$age_group), collapse=' | ')))
  
  #x <- observed_eur[location_id == '82']
  #plot_raw_data(x)
  
  out <- do.call(
    rbind,
    lapply(split(observed_eur, by='location_id'), function(x) {
      
      tryCatch( {
        
        x_loc <- .get_column_val(x$location_id)
        date_range <- c(as.Date("2020-12-16"), max(x$date))
        
        if (x_loc == 82) {
          extra_age_groups <- c("0-17", "0-59", "0-125", "60-125")
          x <- x[!(x$age_group %in% extra_age_groups),]
        }
        
        if (x_loc == 89) {
          
          # Build adult age group for Denmark; age groups overlap 0-17, 0-125
          get_adult_age_group_denmark <- function(x) {
            
            out <- data.table(date=x[age_end == 125, date])
            
            for (i in vax_quantities) {
              
              sel <- c('date', i)
              tmp <- as.data.frame(merge(x[age_end == 125, ..sel], 
                                         x[age_end == 17, ..sel], 
                                         by='date', 
                                         all.y=T))
              
              vax_cols <- c(paste0(i, '.x'), paste0(i, '.y'))    # Which columns are the vaccination quantities
              tmp$new <- tmp[,vax_cols[1]] - tmp[, vax_cols[2]]  # Get difference
              tmp$new[tmp$new < 0] <- 0
              colnames(tmp)[which(colnames(tmp) == 'new')] <- i  # Add appropriate name
              tmp <- tmp[, !(colnames(tmp) %in% vax_cols)]       # Drop merge columns
              out <- merge(out, tmp, by='date', all.x=TRUE)      # Collect adjusted vaccination quantities
            }
            
            out$age_start <- 18
            out$age_end <- 125
            return(out) 
          }
          
          # Calculate discrete age groups
          x <- rbind(x[x$age_end == 17,],
                     get_adult_age_group_denmark(x),
                     fill=TRUE)
          
          x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
          
        }
        
        # Ensure no dates missing
        tmp <- do.call(
          rbind,
          lapply(split(x, by='age_group'), function(x) {
            
            x <- merge(x, 
                       data.table(date=as.Date(date_range[1]:date_range[2])), 
                       by='date', 
                       all=T)
            
            x$age_start <- .get_column_val(x$age_start)
            x$age_end <- .get_column_val(x$age_end)
            return(x)
          }))
        
        # Fill other data
        tmp$age_group <- stringr::str_c(tmp$age_start, '-', tmp$age_end)
        tmp$location_id <- .get_column_val(x$location_id)
        tmp$location_name <- hierarchy[location_id == x_loc]$location_name
        tmp$brand <- .get_column_val(x$brand)
        tmp$source <- .get_column_val(x$source)
        x <- tmp; rm(tmp)
        
        return(x)
        
      }, error=function(e){
        
        cat("ERROR :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        return(NULL)
        
      })
    })
  )
  
  out <- run_redistribute_ages(df=out[!(out$age_group == '0-125'),], 
                               age_starts=age_starts,
                               objective_columns=vax_quantities, 
                               n_cores=n_cores)
  
  # Fix sums
  f <- function(x) {
    for (i in vax_quantities) x[[i]] <- .remove_non_cumulative(x[[i]])
    return(x)
  }
  
  message('Removing non-cumulative observations...')
  out <- .apply_func_location_age(object=out, func=f, n_cores=n_cores)
  
  p <- plot_raw_data(out)
  print(p)
  
  pdf(file.path(vaccine_output_root, glue("data_proc_age_europe.pdf")), width=6, height=9, onefile = TRUE)
  for (i in unique(out$location_id)) {
    p <- plot_raw_data(out[out$location_id == i,])
    print(p)
  }
  dev.off()
  
  
  out$source <- 'europe'
  row.names(out) <- NULL
  return(out)
}





.process_observed_italy_age <- function(observed_ita = observed_age[source == 'italy'], 
                                        age_starts,
                                        vax_quantities,
                                        hierarchy, 
                                        vaccine_output_root,
                                        n_cores) {
  
  .print_header_message('Italy')
  
  pdf(file.path(vaccine_output_root, glue("data_raw_age_italy.pdf")), width=6, height=9, onefile = TRUE)
  for (i in unique(observed_ita$location_id)) {
    p <- plot_raw_data(observed_ita[location_id == i,])
    print(p)
  }
  dev.off()
  
  
  message(glue('Raw Italy age groups: ', paste(unique(observed_ita$age_group), collapse=' | ')))
  
  #x <- observed_eur[location_id == '82']
  #plot_raw_data(x)
  
  out <- do.call(
    rbind,
    lapply(split(observed_ita, by='location_id'), function(x) {
      
      tryCatch( {
        
        x_loc <- .get_column_val(x$location_id)
        date_range <- c(as.Date("2020-12-16"), max(x$date))
        
        tmp <- do.call(
          rbind,
          lapply(split(x, by='age_group'), function(x) {
            
            x <- merge(x, 
                       data.table(date=as.Date(date_range[1]:date_range[2])), 
                       by='date', 
                       all=T)
            
            x$age_start <- .get_column_val(x$age_start)
            x$age_end <- .get_column_val(x$age_end)
            return(x)
          }))
        
        # Fill other data
        tmp$age_group <- stringr::str_c(tmp$age_start, '-', tmp$age_end)
        tmp$location_id <- .get_column_val(x$location_id)
        tmp$location_name <- hierarchy[location_id == x_loc]$location_name
        tmp$brand <- .get_column_val(x$brand)
        tmp$source <- .get_column_val(x$source)
        x <- tmp; rm(tmp)
        
        return(x)
        
      }, error=function(e){
        
        cat("ERROR :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        return(NULL)
        
      })
    })
  )
  
  out <- run_redistribute_ages(df=out, 
                               age_starts=age_starts,
                               objective_columns=vax_quantities, 
                               n_cores=n_cores)
  
  f <- function(x) {
    for (i in vax_quantities) x[[i]] <- .remove_non_cumulative(x[[i]])
    return(x)
  }
  
  message('Removing non-cumulative observations...')
  out <- .apply_func_location_age(object=out, func=f, n_cores=n_cores)
  
  
  pdf(file.path(vaccine_output_root, glue("data_proc_age_italy.pdf")), width=6, height=9, onefile = TRUE)
  for (i in unique(out$location_id)) {
    p <- plot_raw_data(out[out$location_id == i,])
    print(p)
  }
  dev.off()
  
  
  out$source <- 'italy'
  row.names(out) <- NULL
  return(out)
}





.process_observed_canada_age <- function(observed_can = observed_age[source == 'canada'], 
                                         age_starts,
                                         vax_quantities,
                                         eua_dates,
                                         population,
                                         hierarchy, 
                                         vaccine_output_root,
                                         n_cores
) {
  
  .print_header_message('Canada')
  
  pdf(file.path(vaccine_output_root, glue("data_raw_age_canada.pdf")), width=6, height=9, onefile = TRUE)
  p <- plot_raw_data(observed_can); print(p)
  dev.off()
  
  message(glue('Raw Canada age groups: ', paste(unique(observed_can$age_group), collapse=' | ')))
  
  date_range <- c(as.Date("2020-12-16"), max(observed_can$date))
  
  
  nat <- do.call(
    rbind,
    lapply(split(observed_can, by=c('location_id', 'age_group')), function(x) {
      
      x <- merge(x, 
                 data.table(date=as.Date(date_range[1]:date_range[2])), 
                 by='date', 
                 all=T)
      
      x$age_start <- .get_column_val(x$age_start)
      x$age_end <- .get_column_val(x$age_end)
      x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
      x$location_id <- .get_column_val(x$location_id)
      x$location_name <- hierarchy[location_id == x$location_id[1], 'location_name']
      x$brand <- .get_column_val(x$brand)
      x$source <- .get_column_val(x$source)
      
      return(x)
      
    }))
  
  # Appears that current data extraction has data that are over-estimating vaccinations by a lot
  # Scaling by latest observation
  all_age_tot <- 6694616 # Jan 01 2022
  age_tot <- sum(nat[nat$date == as.Date('2022-01-01'), 'initially_vaccinated'], na.rm=T)
  ratio_adjust <- all_age_tot/age_tot
  nat$initially_vaccinated <- nat$initially_vaccinated * ratio_adjust
  nat$fully_vaccinated <- nat$fully_vaccinated * ratio_adjust
  nat$boosters_administered <- nat$boosters_administered * ratio_adjust
  nat$total_administered <- nat$total_administered * ratio_adjust
  
  nat_rebin <- run_redistribute_ages(df=nat, 
                                     age_starts=age_starts,
                                     objective_columns=vax_quantities, 
                                     n_cores=n_cores)
  
  tmp <- nat_rebin[,c('initially_vaccinated', 'fully_vaccinated', 'boosters_administered')]
  nat_rebin$total_administered <- rowSums(tmp, na.rm=T)
  
  for (i in vax_quantities) nat_rebin[[i]][nat_rebin[[i]] == 0] <- NA
  
  nat_rebin <- set_eua_dates(object=nat_rebin,
                             eua_dates=eua_dates,
                             vax_quantities=vax_quantities)
  
  plot_raw_data(nat_rebin)
  plot_raw_data(nat)
  
  message('Splitting Canada national into sub-national locations...')
  subnats <- infer_missing_children(object = nat_rebin, 
                                    objective_cols = vax_quantities, 
                                    population = population, 
                                    hierarchy = hierarchy, 
                                    verbose=F)
  
  out <- rbind(nat_rebin, subnats)
  
  #plot_raw_data(nat)
  #plot_raw_data(out) 
  
  pdf(file.path(vaccine_output_root, glue("data_proc_age_canada.pdf")), width=6, height=9, onefile = TRUE)
  for (i in unique(out$location_id)) {
    p <- plot_raw_data(out[out$location_id == i,])
    print(p)
  }
  dev.off()
  
  
  out$source <- 'Canada'
  row.names(out) <- NULL
  return(out)
}




.process_observed_brazil_age <- function(observed_brz = observed_age[source == 'brazil'], 
                                         age_starts,
                                         vax_quantities,
                                         eua_dates,
                                         population,
                                         hierarchy, 
                                         vaccine_output_root,
                                         n_cores) {
  
  .print_header_message('Brazil')
  
  pdf(file.path(vaccine_output_root, glue("data_raw_age_brazil.pdf")), width=6, height=9, onefile = TRUE)
  p <- plot_raw_data(observed_brz); print(p)
  dev.off()
  
  message(glue('Raw Brazil age groups: ', paste(unique(observed_brz$age_group), collapse=' | ')))
  
  date_range <- c(as.Date("2020-12-16"), max(observed_brz$date))
  
  observed_brz <- .knock_out_rows_by_date(object=observed_brz, date1='2021-08-08')
  observed_brz <- .knock_out_rows_by_date(object=observed_brz, date1='2021-08-15')
  
  nat <- do.call(
    rbind,
    lapply(split(observed_brz, by=c('location_id', 'age_group')), function(x) {
      
      x <- merge(x, 
                 data.table(date=as.Date(date_range[1]:date_range[2])), 
                 by='date', 
                 all=T)
      
      x$age_start <- .get_column_val(x$age_start)
      x$age_end <- .get_column_val(x$age_end)
      x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
      x$location_id <- .get_column_val(x$location_id)
      x$location_name <- hierarchy[location_id == x$location_id[1], 'location_name']
      x$brand <- .get_column_val(x$brand)
      x$source <- .get_column_val(x$source)
      
      return(x)
      
    }))
  
  nat_rebin <- run_redistribute_ages(df=nat, 
                                     age_starts=age_starts,
                                     objective_columns=vax_quantities, 
                                     n_cores=n_cores)
  
  tmp <- nat_rebin[,c('initially_vaccinated', 'fully_vaccinated', 'boosters_administered')]
  nat_rebin$total_administered <- rowSums(tmp, na.rm=T)
  
  for (i in vax_quantities) nat_rebin[[i]][nat_rebin[[i]] == 0] <- NA
  
  nat_rebin <- set_eua_dates(object=nat_rebin,
                             eua_dates=eua_dates,
                             vax_quantities=vax_quantities)
  
  plot_raw_data(nat_rebin)
  plot_raw_data(nat)
  
  message('Splitting Brazil national into sub-national locations...')
  subnats <- infer_missing_children(object = nat_rebin, 
                                    objective_cols = vax_quantities, 
                                    population = population, 
                                    hierarchy = hierarchy, 
                                    verbose=F)
  
  out <- rbind(nat_rebin, subnats)
  
  
  pdf(file.path(vaccine_output_root, glue("data_proc_age_brazil.pdf")), width=6, height=9, onefile = TRUE)
  for (i in unique(out$location_id)) {
    p <- plot_raw_data(out[out$location_id == i,])
    print(p)
  }
  dev.off()
  
  
  out$source <- 'Brazil'
  row.names(out) <- NULL
  return(out)
}





.process_observed_spain_age <- function(observed_esp = observed_age[source == 'spain'], 
                                        age_starts,
                                        vax_quantities,
                                        eua_dates,
                                        population,
                                        hierarchy, 
                                        vaccine_output_root,
                                        n_cores) {
  
  .print_header_message('Spain')
  
  pdf(file.path(vaccine_output_root, glue("data_raw_age_spain.pdf")), width=6, height=9, onefile = TRUE)
  p <- plot_raw_data(observed_esp); print(p)
  dev.off()
  
  message(glue('Raw Spain age groups: ', paste(unique(observed_esp$age_group), collapse=' | ')))
  date_range <- c(as.Date("2020-12-16"), max(observed_esp$date))
  
  nat <- do.call(
    rbind,
    lapply(split(observed_esp, by=c('location_id', 'age_group')), function(x) {
      
      x <- merge(x, 
                 data.table(date=as.Date(date_range[1]:date_range[2])), 
                 by='date', 
                 all=T)
      
      x$age_start <- .get_column_val(x$age_start)
      x$age_end <- .get_column_val(x$age_end)
      x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
      x$location_id <- .get_column_val(x$location_id)
      x$location_name <- hierarchy[location_id == x$location_id[1], 'location_name']
      x$brand <- .get_column_val(x$brand)
      x$source <- .get_column_val(x$source)
      return(x)
      
    }))
  
  nat_rebin <- run_redistribute_ages(df=nat, 
                                     age_starts=age_starts,
                                     objective_columns=vax_quantities, 
                                     n_cores=n_cores)
  
  tmp <- nat_rebin[,c('initially_vaccinated', 'fully_vaccinated', 'boosters_administered')]
  nat_rebin$total_administered <- rowSums(tmp, na.rm=T)
  
  for (i in vax_quantities) nat_rebin[[i]][nat_rebin[[i]] == 0] <- NA
  
  nat_rebin <- set_eua_dates(object=nat_rebin,
                             eua_dates=eua_dates,
                             vax_quantities=vax_quantities)
  
  pdf(file.path(vaccine_output_root, glue("data_proc_age_spain.pdf")), width=6, height=9, onefile = TRUE)
  for (i in unique(out$location_id)) {
    p <- plot_raw_data(out[out$location_id == i,])
    print(p)
  }
  dev.off()
  
  out$source <- 'Spain'
  row.names(out) <- NULL
  return(out)
}







.process_observed_india_age <- function(observed_ind = observed_age[source == 'india'], 
                                        observed_all_age,
                                        age_starts,
                                        vax_quantities,
                                        eua_dates,
                                        vaccine_output_root,
                                        n_cores
) {
  
  .print_header_message('India')
  
  pdf(file.path(vaccine_output_root, glue("data_raw_age_india.pdf")), width=12, height=9, onefile = TRUE)
  p <- plot_raw_data(observed_ind); print(p)
  dev.off()
  
  message(glue('Raw India age groups: ', paste(unique(observed_ind$age_group), collapse=' | ')))
  
  date_range <- c(as.Date("2020-12-16"), max(observed_ind$date))
  
  message('Building India national aggregate')
  nat <- aggregate(formula = total_administered ~ date + age_group, data=observed_ind, FUN=function(x) sum(x, na.rm=T))
  
  #t <- do.call(
  #rbind,
  #lapply(split(nat, list(nat$age_group)), function(x) {
  #  
  #  x <- merge(x, 
  #             data.table(date=as.Date(date_range[1]:date_range[2])), 
  #             by='date', 
  #             all=T)
  #  
  #  x$age_group <- .get_column_val(x$age_group)
  #  
  #  return(x)
  #  
  #}))
  
  tmp <- do.call(rbind, strsplit(nat$age_group, split='-'))
  nat$age_start <- tmp[,1]
  nat$age_end <- tmp[,2]
  nat$brand <- 'All'
  nat$location_id <- 163
  nat$location_name <- 'India'
  nat$initially_vaccinated <- NA
  nat$fully_vaccinated <- NA
  nat$boosters_administered <- NA
  nat$source <- 'india'
  
  observed_ind <- rbind(observed_ind, nat, fill=T)
  
  observed_ind_rebin <- run_redistribute_ages(df=observed_ind, 
                                              age_starts=age_starts,
                                              objective_columns=vax_quantities, 
                                              n_cores=n_cores)
  
  for (i in vax_quantities) observed_ind_rebin[[i]][observed_ind_rebin[[i]] == 0] <- NA
  
  observed_ind_rebin <- as.data.table(observed_ind_rebin)
  
  observed_ind_rebin <- do.call(
    rbind,
    lapply(split(observed_ind_rebin, by=c('location_id', 'age_group')), function(x) {
      
      x <- merge(x, 
                 data.table(date=as.Date(date_range[1]:date_range[2])), 
                 by='date', 
                 all=T)
      
      x$age_start <- .get_column_val(x$age_start)
      x$age_end <- .get_column_val(x$age_end)
      x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
      x$location_id <- .get_column_val(x$location_id)
      x$location_name <- hierarchy[location_id == x$location_id[1], 'location_name']
      x$brand <- .get_column_val(x$brand)
      x$source <- .get_column_val(x$source)
      
      return(x)
      
    }))
  
  observed_ind_rebin <- set_eua_dates(object=observed_ind_rebin,
                                      eua_dates=eua_dates,
                                      vax_quantities=vax_quantities)
  
  # Only total_administered is given, so must built out other quantities based on all-age proportions
  tmp <- observed_all_age[observed_all_age$location_id == 163,]
  tmp$prop_initial <- tmp$people_vaccinated / tmp$reported_vaccinations
  tmp$prop_fully <- tmp$fully_vaccinated / tmp$reported_vaccinations
  tmp$prop_boost <- tmp$boosters_administered / tmp$reported_vaccinations
  
  #x <- observed_ind_rebin[observed_ind_rebin$location_id == 163 & observed_ind_rebin$age_group == '12-17',]
  
  message('Splitting total_administered based on all-age proportions...')
  
  f <- function(x) {
    
    x <- merge(x, tmp[,c('date', 'prop_initial', 'prop_fully', 'prop_boost')], by='date', all.x=T)
    
    x$prop_initial <- .extend_end_values(x$prop_initial)
    x$prop_fully <- .extend_end_values(x$prop_fully)
    x$prop_boost <- .extend_end_values(x$prop_boost)
    
    x$initially_vaccinated <- x$total_administered * x$prop_initial
    x$fully_vaccinated <- x$total_administered * x$prop_fully
    x$boosters_administered <- x$total_administered * x$prop_boost
    x[, c('prop_initial', 'prop_fully', 'prop_boost') := NULL]
    
  }
  
  out <- .apply_func_location_age(object=observed_ind_rebin, func = f, n_cores = n_cores)
  
  pdf(file.path(vaccine_output_root, glue("data_proc_age_india.pdf")), width=12, height=9, onefile = TRUE)
  for (i in unique(out$location_id)) {
    p <- plot_raw_data(out[out$location_id == i,])
    print(p)
  }
  dev.off()
  
  out$source <- 'India'
  row.names(out) <- NULL
  return(out)
}



.process_observed_southafrica_age <- function(observed_saf = observed_age[source == 'southafrica'], 
                                              observed_all_age = observed_all_age,
                                              age_starts,
                                              vax_quantities,
                                              eua_dates,
                                              hierarchy, 
                                              vaccine_output_root,
                                              n_cores) {
  
  .print_header_message('South Africa')
  
  pdf(file.path(vaccine_output_root, glue("data_raw_age_southafrica.pdf")), width=6, height=9, onefile = TRUE)
  p <- plot_raw_data(observed_saf); print(p)
  dev.off()
  
  message(glue('Raw South Africa age groups: ', paste(unique(observed_saf$age_group), collapse=' | ')))
  date_range <- c(as.Date("2020-12-16"), max(observed_saf$date)) 
  
  observed_saf <- do.call(
    rbind,
    lapply(split(observed_saf, by=c('location_id', 'age_group')), function(x) {
      
      x <- merge(x, 
                 data.table(date=as.Date(date_range[1]:date_range[2])), 
                 by='date', 
                 all=T)
      
      x$age_start <- .get_column_val(x$age_start)
      x$age_end <- .get_column_val(x$age_end)
      x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
      x$location_id <- .get_column_val(x$location_id)
      x$location_name <- hierarchy[location_id == x$location_id[1], 'location_name']
      x$brand <- .get_column_val(x$brand)
      x$source <- .get_column_val(x$source)
      return(x)
      
    }))
  
  observed_saf_rebin <- run_redistribute_ages(df=observed_saf, 
                                              age_starts=age_starts,
                                              objective_columns=vax_quantities, 
                                              n_cores=n_cores)
  
  for (i in vax_quantities) observed_saf_rebin[[i]][observed_saf_rebin[[i]] == 0] <- NA
  
  message('Splitting total_administered based on all-age proportions...')
  f <- function(x) {
    
    tmp <- observed_all_age[observed_all_age$location_id == 196,]
    tmp$prop_initial <- tmp$people_vaccinated / tmp$reported_vaccinations
    tmp$prop_fully <- tmp$fully_vaccinated / tmp$reported_vaccinations
    tmp$prop_boost <- tmp$boosters_administered / tmp$reported_vaccinations
    
    x <- merge(x, tmp[,c('date', 'prop_initial', 'prop_fully', 'prop_boost')], by='date', all.x=T)
    x$initially_vaccinated <- x$total_administered * x$prop_initial
    x$fully_vaccinated <- x$total_administered * x$prop_fully
    x$boosters_administered <- x$total_administered * x$prop_boost
    x[, -which(colnames(x) %in% c('prop_initial', 'prop_fully', 'prop_boost'))]
    
  }
  
  observed_saf_rebin <- f(observed_saf_rebin)
  
  out <- set_eua_dates(object=observed_saf_rebin,
                       eua_dates=eua_dates,
                       vax_quantities=vax_quantities)
  
  pdf(file.path(vaccine_output_root, glue("data_proc_age_southafrica.pdf")), width=12, height=9, onefile = TRUE)
  for (i in unique(out$location_id)) {
    p <- plot_raw_data(out[out$location_id == i,])
    print(p)
  }
  dev.off()
  
  out$source <- 'South Africa'
  row.names(out) <- NULL
  return(out)
  
}






.process_observed_ecuador_age <- function(observed_ecu = observed_age[source == 'ecuador'], 
                                          age_starts,
                                          vax_quantities,
                                          eua_dates,
                                          population,
                                          hierarchy, 
                                          vaccine_output_root,
                                          n_cores) {
  
  if (.get_column_val(observed_ecu$location_id) != 122) observed_ecu$location_id <- 122
  if (.get_column_val(observed_ecu$location_name) != 'Ecuador') observed_ecu$location_name <- 'Ecuador' 
  
  .print_header_message('Ecuador')
  
  pdf(file.path(vaccine_output_root, glue("data_raw_age_ecuador.pdf")), width=6, height=9, onefile = TRUE)
  p <- plot_raw_data(observed_ecu); print(p)
  dev.off()
  
  message(glue('Raw Ecuador age groups: ', paste(unique(observed_ecu$age_group), collapse=' | ')))
  date_range <- c(as.Date("2020-12-01"), max(observed_ecu$date))
  
  age_groups <- .get_age_groups(age_starts)
  
  rebin_ecu <- run_redistribute_ages(df=observed_ecu, 
                                     age_starts=age_starts,
                                     objective_columns=vax_quantities, 
                                     n_cores=n_cores)
  
  rebin_ecu <- as.data.table(rebin_ecu)
  rebin_ecu$age_group <- factor(rebin_ecu$age_group, levels=age_groups)
  rebin_ecu <- rebin_ecu[order(age_group),]
  
  #x <- rebin_ecu[location_name == 'Ecuador' & age_group == '18-39',]
  
  message('Imputing missing quantities and enforcing EUA dates...')
  pdf(file.path(vaccine_output_root, glue("data_proc_age_ecuador.pdf")), width=11, height=8, onefile = TRUE)
  par(mfrow=c(3, length(unique(rebin_ecu$age_group))))
  
  out <- do.call(
    rbind,
    lapply(split(rebin_ecu, by=c('location_id', 'age_group')), function(x) {
      
      tryCatch( {
        
        x_loc <- .get_column_val(x$location_id)
        x_age <- .get_column_val(x$age_group)
        
        x$location_name <- hierarchy[location_id == x_loc]$location_name # Many of the two-name locs are messed up
        date_range <- c(as.Date("2020-12-16"), as.Date(Sys.Date()))
        
        x <- merge(x, 
                   data.table(date=as.Date(date_range[1]:date_range[2])), 
                   by='date', 
                   all=T)
        
        x$age_start <- .get_column_val(x$age_start)
        x$age_end <- .get_column_val(x$age_end)
        x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
        x$location_id <- .get_column_val(x$location_id)
        x$location_name <- .get_column_val(x$location_name)
        x$brand <- .get_column_val(x$brand)
        x$source <- .get_column_val(x$source)
        
        x <- set_eua_dates(object = x,
                           eua_dates = eua_dates,
                           vax_quantities = vax_quantities)
        
        prop_first <- x$initially_vaccinated / x$total_administered
        prop_first[is.nan(prop_first)] <- 0
        x$fully_vaccinated <- x$total_administered * (1 - prop_first)
        
        for (i in vax_quantities) x[[i]] <- .remove_non_cumulative(x[[i]])
        
        # Adjust for cases when initially vaccinated > fully vaccinated
        check <- x$initially_vaccinated < x$fully_vaccinated
        if (any(check[!is.na(check)])) x$initially_vaccinated <- pmax(x$initially_vaccinated, x$fully_vaccinated)
        
        plot(x$date, x$total_administered, 
             ylab='Total vaccinated',
             main=paste(x$location_name[1], x$age_group[1], sep=' | '))
        abline(v=eua_dates[eua_dates$location_id == x_loc & eua_dates$age_group == x_age, 'date'], lty=2)
        points(x$date, x$initially_vaccinated, col='blue')
        points(x$date, x$fully_vaccinated, col='red')
        points(x$date, x$boosters_administered, col='green3')
        
        return(x)
        
      }, error=function(e){
        
        cat("ERROR :", unique(x$location_id), ":", unique(x$location_name), ":", unique(x$age_group), ":", conditionMessage(e), "\n")
        
      })
    }))
  
  dev.off()
  par(mfrow=c(1,1))
  
  row.names(out) <- NULL
  return(out)
  
  
  
}



.process_observed_default_age <- function(observed_def = observed_age[source %in% c('hongkong')], 
                                          age_starts,
                                          vax_quantities,
                                          eua_dates,
                                          population,
                                          hierarchy, 
                                          vaccine_output_root,
                                          n_cores) {
  
  source_name <- na.omit(observed_def$source)[1]
  .print_header_message(glue('{source_name} (default)'))
  
  pdf(file.path(vaccine_output_root, glue("data_raw_age_{source_name}.pdf")), width=6, height=9, onefile = TRUE)
  p <- plot_raw_data(observed_def); print(p)
  dev.off()
  
  message(glue('Raw default age groups: ', paste(unique(observed_def$age_group), collapse=' | ')))
  date_range <- c(as.Date("2020-12-01"), max(observed_def$date))
  
  age_groups <- .get_age_groups(age_starts)
  
  rebin_def <- run_redistribute_ages(df=observed_def, 
                                     age_starts=age_starts,
                                     objective_columns=vax_quantities, 
                                     n_cores=n_cores)
  
  rebin_def <- as.data.table(rebin_def)
  rebin_def$age_group <- factor(rebin_def$age_group, levels=age_groups)
  rebin_def <- rebin_def[order(age_group),]
  
  #x <- rebin_def[location_name == 'China' & age_group == '18-39',]
  
  message('Imputing missing quantities and enforcing EUA dates...')
  pdf(file.path(vaccine_output_root, glue("data_proc_age_{source_name}.pdf")), width=11, height=8, onefile = TRUE)
  par(mfrow=c(3, length(unique(rebin_def$age_group))))
  
  out <- do.call(
    rbind,
    lapply(split(rebin_def, by=c('location_id', 'age_group')), function(x) {
      
      tryCatch( {
        
        x_loc <- .get_column_val(x$location_id)
        x_age <- .get_column_val(x$age_group)
        
        x$location_name <- hierarchy[location_id == x_loc]$location_name # Many of the two-name locs are messed up
        date_range <- c(as.Date("2020-12-01"), as.Date(Sys.Date()))
        
        x <- merge(x, 
                   data.table(date=as.Date(date_range[1]:date_range[2])), 
                   by='date', 
                   all=T)
        
        x$age_start <- .get_column_val(x$age_start)
        x$age_end <- .get_column_val(x$age_end)
        x$age_group <- stringr::str_c(x$age_start, '-', x$age_end)
        x$location_id <- .get_column_val(x$location_id)
        x$location_name <- .get_column_val(x$location_name)
        x$brand <- .get_column_val(x$brand)
        x$source <- .get_column_val(x$source)
        
        x <- set_eua_dates(object = x,
                           eua_dates = eua_dates,
                           vax_quantities = vax_quantities)
        
        prop_first <- x$initially_vaccinated / x$total_administered
        prop_first[is.nan(prop_first)] <- 0
        x$fully_vaccinated <- x$total_administered * (1 - prop_first)
        
        for (i in vax_quantities) x[[i]] <- .remove_non_cumulative(x[[i]])
        
        # Adjust for cases when initially vaccinated > fully vaccinated
        check <- x$initially_vaccinated < x$fully_vaccinated
        if (any(check[!is.na(check)])) x$initially_vaccinated <- pmax(x$initially_vaccinated, x$fully_vaccinated)
        
        plot(x$date, x$total_administered, 
             ylab='Total vaccinated',
             main=paste(x$location_name[1], x$age_group[1], sep=' | '))
        abline(v=eua_dates[eua_dates$location_id == x_loc & eua_dates$age_group == x_age, 'date'], lty=2)
        points(x$date, x$initially_vaccinated, col='blue')
        points(x$date, x$fully_vaccinated, col='red')
        points(x$date, x$boosters_administered, col='green3')
        
        return(x)
        
      }, error=function(e){
        
        cat("ERROR :", unique(x$location_id), ":", unique(x$location_name), ":", unique(x$age_group), ":", conditionMessage(e), "\n")
        
      })
    }))
  
  dev.off()
  par(mfrow=c(1,1))
  
  out$source <- source_name
  row.names(out) <- NULL
  return(out)
  
  
  
}

