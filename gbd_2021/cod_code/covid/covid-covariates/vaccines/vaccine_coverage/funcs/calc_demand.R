
calc_demand <- function(vaccine_output_root, 
                        plot_maps=FALSE,
                        plot_timeseries=FALSE
) {
  
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_path <- model_parameters$model_inputs_path
  previous_best_path <- model_parameters$previous_best_path
  
  start_date <- as.Date(model_parameters$data_start_date) # error, needs origin, but seems unused later
  yes_responses <- model_parameters$survey_yes_responses
  
  
  ##-------------------------------------------------------------------
  ## Import data ##
  message('Importing data')
  hierarchy <- gbd_data$get_covid_covariate_prep_hierarchy()
  
  total_population <- model_inputs_data$load_total_population(model_inputs_path)
  setnames(total_population, 'population', 'total_population')
  population <- model_inputs_data$load_adult_population(model_inputs_path, model_parameters)
  setnames(population, 'adult_population', 'population')
  
  o5_locs <- c(6, 101, 156, 102) # China, Canada, UAE, US
  over5_population <- model_inputs_data$load_o5_population(model_inputs_path)
  setnames(over5_population, 'over5_population', 'population')
  sel <- which(population$location_id %in% o5_locs)
  population <- rbind(population[-sel,], over5_population[sel,])
  population <- population[order(location_id),]
  
  hesitancy <- .load_time_series_vaccine_hesitancy(vaccine_output_root, 'default')
  #hesitancy <- merge(hesitancy, population, by='location_id', all.x=T)
  hesitancy$pct_vaccinated[hesitancy$pct_vaccinated > 1] <- 1
  
  mod <- vaccine_data$load_scenario_forecast(vaccine_output_root, 'slow')
  mod <- mod[, c("location_id", "date", "cumulative_all_vaccinated", "adult_population")]
  mod <- mod[date <= max(hesitancy$date),]
  mod$pct_vaccinated <- mod$cumulative_all_vaccinated / mod$adult_population
  
  hesitancy <- merge(hesitancy,
                     mod[,c('location_id', 'date', 'adult_population')],
                     by=c('location_id', 'date'))
  
  #x <- hesitancy[hesitancy$location_id == 10,]
  
  demand <- do.call(
    rbind,
    lapply(split(hesitancy, by='location_id'), function(x) {
      
      tryCatch( { 
        
        if (any(is.na(x$pct_vaccinated))) x$pct_vaccinated <- zoo::na.approx(x$pct_vaccinated, na.rm=F)
        x <- x[complete.cases(x[,c('smooth_combined_yes', 'pct_vaccinated'),])]
        x$max_date <- max(x$date)
        
        x$smooth_combined_yes <- pmax(x$smooth_combined_yes, x$pct_vaccinated)
        x$demand_pct <- x$smooth_combined_yes - x$pct_vaccinated
        x$demand_raw <- x$demand_pct * x$adult_population
        
        return(x)
        
      }, error=function(e){
        
        cat("Error :", unique(x$location_id), ":", unique(x$location_name), "\n")
        
      })
    })
  )
  
  
  demand <- merge(demand, total_population, by='location_id', all.x=T)
  demand <- .add_spatial_metadata(data=demand, hierarchy=hierarchy)
  demand[, max_date := as.Date(max_date)]
  demand_pt <- demand[date == max_date,]
  
  # REGIONAL AGGREGATES ----
  # set of hierarchies that need regional aggregates for Briefs
  hier_who_euro118 <- .get_who_european_hierarchy() # aggregate to level 1
  hier_who_covid123 <- .get_who_covid_hierarchy() # aggregate to level 1
  hier_eu_covid124 <- .get_european_union_hierarchy() # aggregate to level 0 (all EU)
  agg_cols <- c("demand_raw", "total_population")
  # Requirement: are all most-detailed locs present?
  md_118 <- hier_who_euro118[most_detailed==1, location_id]
  md_123 <- hier_who_covid123[most_detailed == 1, location_id]
  md_124 <- hier_eu_covid124[most_detailed == 1, location_id]
  # Neither model nor GBD need Aus states. Set Australia
  # to most detailed, or update lsid 115, which may have downstream effects on
  # other covariates.
  aus_children <- children_of_parents(parent_loc_ids = 71, hierarchy = hierarchy, output = "loc_ids")
  hierarchy <- hierarchy[!location_id %in% aus_children]
  hierarchy[location_id == 71, most_detailed := 1]
  md_hier <- hierarchy[most_detailed == 1, location_id]
  
  if (!all(unique(c(md_118, md_123, md_124, md_hier)) %in% demand_pt$location_id) | # most detailed locations present?
      # any required data missing? (also some quantities necessary to calculate demand_raw that caused prior issues)
      any(is.na(demand_pt[location_id %in% unique(c(md_118, md_123, md_124, md_hier)), c(..agg_cols, "adult_population", "demand_pct")])) 
  ) {
    stop("calc_demand.R: Some most-detailed locations/values are absent from demand_pt, and you cannot completely aggregate up to regional levels")
  }
  
  regions <- rbindlist(
    list(# must choose the correct level for regions from each hierarchy
      .aggregate_regions(hierarchy = hier_who_euro118, demand_pt = demand_pt, region_level = 1, agg_cols = agg_cols),
      .aggregate_regions(hierarchy = hier_who_covid123, demand_pt = demand_pt, region_level = 1, agg_cols = agg_cols),
      .aggregate_regions(hierarchy = hier_eu_covid124, demand_pt = demand_pt, region_level = 0, agg_cols = agg_cols),
      .aggregate_regions(hierarchy = hierarchy, demand_pt = demand_pt, region_level = 0, agg_cols = agg_cols) # global
    )
  )
  
  if(any(regions$location_id %in% demand_pt$location_id)) {
    stop("calc_demand.R: One of the regions already exists in the demand_pt data frame - please check `regions` before appending to main data frame")
  }
 
  # sanity check
  regions %>% mutate(pct_unmet_demand = round(demand_raw/total_population*100,1))
  # many columns are intentionally left NA since this data frame is a blend of 4 incompatible hierarchies
  demand_pt <- rbind(demand_pt, regions, fill = T)
  
  if (F) {
    
    tmp <- demand[demand$location_id == 44533,]
    
    par(mfrow=c(1,3))
    plot(as.Date(tmp$date), tmp$pct_vaccinated, ylim=c(0,1))
    plot(as.Date(tmp$date), tmp$survey_yes, ylim=c(0,1))
    lines(as.Date(tmp$date), tmp$smooth_survey_yes)
    plot(as.Date(tmp$date), tmp$smooth_combined_yes, ylim=c(0,1))
    par(mfrow=c(1,1))
    
  }
  
  
  
  write.csv(demand, file=file.path(vaccine_output_root, glue('demand_time_series_{yes_responses}.csv')), row.names = F)
  write.csv(demand_pt, file=file.path(vaccine_output_root, glue('demand_point_est_{yes_responses}.csv')), row.names = F)
  message(glue('Demand results written to {vaccine_output_root}'))
  
  
  
  
  if (F) {
    
    get_demand_file_path <- function(vaccine_output_root, prefix) {
      tmp <- list.files(vaccine_output_root, full.names = T)
      tmp <- tmp[grep(prefix, tmp)]
      tmp <- tmp[grep('.csv', tmp)]
      read.csv(tmp)
    }

    tmp <- "FILEPATH"
    demand <- get_demand_file_path(vaccine_output_root=tmp, prefix='demand_time_series')
    demand_pt <- get_demand_file_path(vaccine_output_root=tmp, prefix='demand_point_est')

  }
  
  
  
  
  if (plot_maps) {
    
    # Prep spatial data
    shp_covid <- st_read('FILEPATH/covid_simp_2_v2.shp')
    shp_covid <- shp_covid[shp_covid$loc_id %in% hierarchy$location_id,]
    names(shp_covid)[names(shp_covid) == 'loc_id'] <- 'location_id'
    shp_covid$location_id <- as.integer(shp_covid$location_id)
    sf_plot <- left_join(shp_covid, demand_pt, by = "location_id")
    
    
    # Map of props
    pdf(file.path(vaccine_output_root, glue('demand_maps_{yes_responses}.pdf')), height=8, width=12, onefile = T)
    
    message('Plotting percent map')
    pal <- colorRampPalette(c('white', rev(viridis::viridis_pal(option='magma')(10)[-c(9,10)])))(20) 
    #scales::show_col(pal)
    
    p <- sf_plot %>% 
      ggplot(aes(fill=demand_raw/total_population, col='Test')) +
      geom_sf(size=0.15, color='black') +
      ggtitle(paste("Estimated proporiton of the total population that is NOT vaccinated but willing to be vaccinated as of", format(as.Date(demand_pt$max_date[1]), '%B %d, %Y'))) +
      scale_fill_gradientn(colours = pal, na.value = 'grey75') + 
      theme_void() +
      theme(legend.position='bottom',
            strip.text.x = element_text(size=12),
            plot.title = element_text(hjust = 0.5, vjust=0.5),
            plot.subtitle = element_text(hjust = 0.5, vjust=0.5)) +
      guides(fill = guide_colourbar(title = 'Proportion of total population',
                                    title.position = 'top',
                                    barwidth = 20, 
                                    barheight = 0.6,
                                    frame.colour = "black", 
                                    ticks.colour = "black"))
    
    print(p)
    
    
    
    
    # Map of counts
    message('Plotting raw count map')

    breaks <- c(0, 10e03, 100e03, 1e06, 10e06, 50e06) + 1
    global_tot <- format(sum(demand_pt$demand_raw, na.rm=T), big.mark = ',', scientific = F)
    global_pct <- round((sum(demand_pt$demand_raw, na.rm=T) / sum(demand_pt$total_population, na.rm=T))*100, 1)
    title_text <- glue("Estimated global unmet demand is {global_tot} ({global_pct}%) as of {format(hesitancy$max_date[1], '%B %d, %Y')}")
    
    pal <- colorRampPalette(c('white', rev(viridis::viridis_pal(option='magma')(10))[-c(5:8,10)]))(60) 
    
    p <- sf_plot %>% 
      ggplot(aes(fill=demand_raw+1)) +
      geom_sf(size=0.15, color='black') +
      ggtitle(title_text) +
      scale_fill_gradientn(colours = pal, 
                           na.value = 'grey75',
                           trans = "log",
                           breaks = breaks, 
                           labels = format(as.numeric(breaks-1), big.mark = ',', scientific = F)) + 
      theme_void() +
      theme(legend.position='bottom',
            strip.text.x = element_text(size=12),
            plot.title = element_text(hjust = 0.5, vjust=0.5)) +
      guides(fill = guide_colourbar(title='Total count', 
                                    title.position = 'top',
                                    barwidth = 20, 
                                    barheight = 0.6,
                                    frame.colour = "black", 
                                    ticks.colour = "black",
                                    label.theme = element_text(angle=325, vjust=0.8, hjust=0.2)
                                    ))
    
    print(p)
    
    dev.off()
    
  }
  
  if (plot_timeseries) {
    
  message('Plotting time series')
  sel_locs <- c(6, 102)
  sel_locs <- hierarchy[location_id %in% sel_locs, location_name]
  sel_locs <- c(sel_locs, 'Bahrain', 'Israel')
  
  pdf(file.path(vaccine_output_root, glue('demand_time_series_{yes_responses}.pdf')), height=5, width=10, onefile = T)
  
  # PRoportion remaining
  
  ggplot(demand, aes(x=date, y=demand_pct)) +
    geom_line(aes(group=location_id), alpha=0.5, size=0.5) +
    geom_line(data=demand[location_name %in% sel_locs,], aes(group=location_id, color=factor(location_name)), size=1.5) +
    ggtitle('Proportion of willing yet to be vaccianted') +
    ylim(0,1) +
    theme_classic()
  

  # Counts remaining
  
  breaks <- c(0, 1e03, 10e03, 100e03, 1e06, 10e06, 100e06, 1e09) + 1
  
  ggplot(demand) +
    geom_line(aes(x=date, y=demand_raw+1, group=location_id), alpha=0.5, size=0.5) +
    geom_line(data=demand[location_name %in% sel_locs,],
              aes(x=date, y=demand_raw+1, group=location_id, color=factor(location_name)),
              size=1.5) +
    scale_y_continuous(trans='log',
                       breaks=breaks,
                       labels=format(as.numeric(breaks-1), big.mark = ',', scientific = F)) +
    ggtitle('Raw vaccine demand (total number adults willing and yet to be vaccinated)') +
    ylab('Total number of willing adults that are unvaccinated') +
    theme_classic()
  
  dev.off()
  
  
  }
  
  
  
  
  
  
}

# Function to aggregate regions up from most detailed locations, up to a regional level
# Requires: hierarchy, raw demand data, specify which level your desired regions are set, agg_cols are present in data
# Returns: data frame with all regional aggregates, and names drawn from the input hierarchy (which may not match the name structure from the demand data's hierarchy)
#' Title
#'
#' @param hierarchy [data.table] location hierarchy for which you will aggregate demand values values
#' @param demand_pt [data.table] point estimates of vaccine demand on the max date in the data
#' @param region_level [integer] what level are regions assigned to in the chosen hierarchy
#' @param agg_cols [character] which columns will you aggregate
#'
#' @return [data.table] summary aggregates of chosen columns, all others will be NA
.aggregate_regions <- function(hierarchy, demand_pt, region_level, agg_cols){
  
  df <- demand_pt[, .(location_id, location_name, demand_raw, total_population)] # remove for the loop since this changes every hierarchy
  hier_refill <- hierarchy[, .(location_id, location_name)]
  hier_subset <- hierarchy[, .(location_id, region_name, level, most_detailed, path_to_top_parent, parent_id)]
  df <- merge(hier_subset, df, by = "location_id", all.x = T)
  
  df_refill <- df[,.(location_id, location_name, parent_id)] # for later renaming
  df_refill <- merge(df_refill, hier_subset[, .(location_id, level)])
  
  rev_levels <- sort(unique(hier_subset$level), decr = T)
  
  for (l in rev_levels[rev_levels > 0]) { # do not use 0 in the loop (tries to aggregate up to level "-1")
    
    # Outer loop: for each hierarchy level, starting at leaf nodes, aggregate all children to their
    # parent level, then repeat and roll those up to the next parent level
    parents_of_level <- hier_subset[level == l - 1 & most_detailed == 0, location_id]
    
    for (i in parents_of_level) {
      
      # Inner loop: aggregate all leaf locations up the their parent.
      children_locs <- hier_subset[parent_id == i & !location_id %in% parents_of_level, location_id] # why are regions parents of themselves?
      
      children_df <- df[location_id %in% children_locs]
      children_df <- children_df[which(!is.na(parent_id)), lapply(.SD, function(x) sum(x, na.rm = T)), by = .(parent_id), .SDcols = agg_cols]
      parent_agg_row <- setnames(children_df, "parent_id", "location_id")
      parent_agg_row <- merge(parent_agg_row, df_refill, by = "location_id")
      df <- rbind(df[!location_id %in% unique(parent_agg_row$location_id)], parent_agg_row, fill = T)
      
      # print(tail(df, 20)) # for scoping in
      
    }
    
  }
  # get essential numbers, get locations names from hierarchy
  df <- df[level==region_level, .(location_id, parent_id, demand_raw, total_population)]
  df <- merge(df, hier_refill, by = "location_id")
  # Refill dates, most_detailed
  demand_pt_date <- as.Date(max(demand_pt[, max_date], na.rm = T))
  df[, `:=` (date = demand_pt_date, max_date = demand_pt_date)]
  df[, most_detailed := 0]

  return(df)
}
