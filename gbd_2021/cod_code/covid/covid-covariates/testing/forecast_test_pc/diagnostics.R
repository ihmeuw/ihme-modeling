library(ggplot2)
library(dplyr)
library(ggpubr)

# Plots for pdf -----------------------

plot_prod_comp5 <- function(current_version,
                            previous_best_version,
                            hierarchy=NULL,
                            out_path
) {
  
  if (is.null(hierarchy)) hierarchy <- get_location_metadata(location_set_id = lsid_covar, location_set_version_id = lsvid_covar, release_id = release_covar)
  
  latest_name <- paste("Latest:", current_version)
  previous_name <- paste("Previous:", previous_best_version)
  
  message("Getting reported testing data for previous best and latest")
  latest_data <- fread(glue("FILEPATH/data_smooth.csv"))
  latest_data$date <- as.Date(latest_data$date)
  latest_data$version <- latest_name
  
  best_data <- fread(glue("FILEPATH/data_smooth.csv"))
  best_data$date <- as.Date(best_data$date)
  best_data$version <- previous_name
  
  message("Getting models of testing percent for previous best and latest")
  latest_mod <- fread(glue("FILEPATH/forecast_raked_test_pc_simple.csv"))
  latest_mod[, date := as.Date(date)]
  latest_mod$version <- latest_name       
  
  best_mod <- fread(glue("FILEPATH/forecast_raked_test_pc_simple.csv"))
  best_mod[, date := as.Date(date)]
  best_mod$version <- previous_name
  
  model_dt <- rbind(best_mod[location_id %in% unique(latest_mod$location_id)], latest_mod, fill = T)
  sorted <- ihme.covid::sort_hierarchy(hierarchy[location_id %in% unique(model_dt$location_id)])
  
  # daily_reported_total doesn't account for missing days, interpolate by dividing by
  # the number of missing days inbetween. transformation is visual only
  
  latest_data_daily <- latest_data %>% 
    select(date, location_name, location_id, daily_total_reported, population, version) %>% 
    # Removing NA days allows us to calc the date difference = # missing days between
    filter(!is.na(daily_total_reported)) %>% 
    mutate(
      # Gets number of missing days between last reported
      na_between = as.numeric(date - lag(date)),
      # helps with transitions between one loc to another
      prev_loc = lag(location_id),
      # take the daily_reported_total and divide by number of days between last report
      # if days between not 0 or there was not a location change
      daily_total_reported_imputed = ifelse(is.na(prev_loc), 
                                            daily_total_reported, 
                                            ifelse(location_id == prev_loc, 
                                                   daily_total_reported / as.numeric(na_between),
                                                   daily_total_reported)))
  
  best_data_daily <- best_data %>% 
    select(date, location_name, location_id, daily_total_reported, population, version) %>% 
    filter(!is.na(daily_total_reported)) %>% 
    mutate(
      na_between = (date - lag(date)),
      prev_loc = lag(location_id),
      daily_total_reported_imputed = ifelse(na_between == 0, 
                                            daily_total_reported, 
                                            ifelse(location_id == prev_loc, 
                                                   daily_total_reported / as.numeric(na_between),
                                                   daily_total_reported)))
  
  message(glue("Getting testing sources from filename report"))
  
  # Read in latest filename report to add testing source in plot titles
  file_report <- fread(glue('FILEPATH/filename_report.csv'))
  
  message(glue("Plotting testing diagnostics here: {out_path}"))
  
  pal <- RColorBrewer::brewer.pal(9, "Set1")
  
  pdf(out_path, width = 12, height = 11, onefile = TRUE)
  
  for(loc_id in unique(sorted$location_id)) {
    
    fron <- unique(model_dt[location_id == loc_id]$frontier)
    fron <- unique(round(fron * 1e5, 2))
    fron <- ifelse(is.na(fron), "None", paste0(fron, " per 100k"))
    
    # Get source of testing from filename report
    df <- file_report %>% 
      filter(location_id == loc_id)
    
    tot_sources <- df$total_tests_data_filename
    daily_sources <- df$daily_tests_data_filename
    
    # Combine and remove NA's or empty ''
    all_sources <- na.omit(unique(c(tot_sources, daily_sources)))
    all_sources <- all_sources[all_sources != '']
    
    # If latest data available and it is lower than previous best then set the 
    # comparison plot ylim to latest
    if(any(latest_data$location_id == loc_id)) {
      
      # Get the higher of the two between latest data and latest model
      max_latest_data <- max(latest_data_daily$daily_total_reported_imputed[latest_data_daily$location_id == loc_id], na.rm = TRUE) / latest_data_daily$population[latest_data_daily$location_id == loc_id][1] * 1e5
      max_latest_mod <- max(latest_mod$test_pc[latest_mod$location_id == loc_id], na.rm = TRUE) * 1e5
      
      max_latest <- max(max_latest_data, max_latest_mod)
      
      # Get the higher of the two between best data and best model
      max_best <- max(c(max(best_data_daily$daily_total_reported_imputed[best_data_daily$location_id == loc_id], na.rm = TRUE) / best_data_daily$population[best_data_daily$location_id == loc_id][1] * 1e5,
                        max(best_mod$test_pc[best_mod$location_id == loc_id], na.rm = TRUE) * 1e5))
      
      # If the max latest data/model is lower than the max prev best data/model 
      # or if max prev best is NA and max latest is not, set ylim to latest 
      if (max_latest < max_best | (is.na(max_best) & !is.na(max_latest))) {
        ylim_switch <- TRUE
        
        # Get latest mod max to set first plot scale to 
        latest_max_y <- max_latest
        
      } else {
        ylim_switch <- FALSE
      }
      
      # latest data not available set scale to max latest mod
    } else {
      ylim_switch <- TRUE
      latest_max_y <- max(latest_mod$test_pc[latest_mod$location_id == loc_id], na.rm = TRUE) * 1e5
    }
    
    # Get the earliest date between all models and dates to set plots to 
    # standardize to
    x_min_mod <- min(c(latest_mod$date, best_mod$date), na.rm = TRUE)
    x_min_data <- min(c(latest_data$date, best_data$date), na.rm = TRUE)
    x_min <- min(c(x_min_mod, x_min_data))
    
    # Common template for observed data and model plots
    g <- ggplot() + 
      geom_vline(xintercept=max(best_data$date), lty=2, color='grey50') +
      theme_bw() +
      theme(legend.position = "none") +
      scale_x_date(limits = c(x_min, Sys.Date() + 90))
    
    # Combined plot including observed data + model
    p1 <- ggplot() +
      geom_point(data = latest_data[location_id == loc_id], 
                 aes(x = date, y = daily_total_reported / population * 1e5, shape=version), size=2.5) +
      geom_point(data = best_data[location_id == loc_id], 
                 aes(x = date, y = daily_total_reported / population * 1e5, shape=version), size=2.5) +
      geom_line(data = latest_mod[location_id == loc_id & date < Sys.Date() + 90], 
                aes(x = date, y = test_pc * 1e5, linetype = observed == 0, color = version), lwd=2.5) +
      geom_line(data = best_mod[location_id == loc_id & date < Sys.Date() + 90], 
                aes(x = date, y = test_pc * 1e5, linetype = observed == 0, color = version), lwd=2) +
      scale_shape_manual('data', values = c(1,19)) +
      scale_color_manual('model', values = c(pal[1], pal[2])) +
      theme_bw() +
      theme(legend.position = "bottom",
            legend.text=element_text(size=8)) +
      xlab("Date") +
      ylab("Tests per 100k") 
      # Conditionally add ylim if latest data available and less than prev best
      
      # Removed to see all data on first plot to give context for outliers - reintroduce if a well-scaled model is ever desired
      # {if(ylim_switch)ylim(NA, latest_max_y)} +
      # guides(linetype = "none") +
      # scale_x_date(limits = c(x_min, Sys.Date() + 90))
    
    # Combined plot including observed data + model transformed by dividing
    # days since last reported; the idea is for this plot to minimize false visual outliers
    p2 <- ggplot() +
      
      geom_point(data = latest_data_daily[location_id == loc_id], 
                 aes(x = date, y = daily_total_reported_imputed / population * 1e5, shape=version), size=2.5) +
      
      geom_point(data = best_data_daily[location_id == loc_id], 
                 aes(x = date, y = daily_total_reported_imputed / population * 1e5, shape=version), size=2.5) +
      
      geom_line(data = latest_mod[location_id == loc_id & date < Sys.Date() + 90], 
                aes(x = date, y = test_pc * 1e5, linetype = observed == 0, color = version), lwd=2.5) +
      
      geom_line(data = best_mod[location_id == loc_id & date < Sys.Date() + 90], 
                aes(x = date, y = test_pc * 1e5, linetype = observed == 0, color = version), lwd=2) +
      
      scale_shape_manual('data', values = c(1,19)) +
      scale_color_manual('model', values = c(pal[1], pal[2])) +
      theme_bw() +
      theme(legend.position = "bottom",
            legend.text=element_text(size=8)) +
      xlab("Date") +
      ylab("Tests per 100k / # Days since last reported test") +
      # Conditionally add ylim if latest data available and less than prev best for model clarity
      {if(ylim_switch) ylim(NA, latest_max_y)} +
      guides(linetype = "none") +
      scale_x_date(limits = c(x_min, Sys.Date() + 90))
    
    # Observed previous best data plot
    p3 <- g + geom_point(data = best_data[location_id == loc_id], 
                         aes(x = date, y = daily_total_reported / population * 1e5), shape=19, size=2.5) +
      ylab('daily_total_reported / population * 1e5')
    
    # Observed latest data plot
    p4 <- g + geom_point(data = latest_data[location_id == loc_id], 
                         aes(x = date, y = daily_total_reported / population * 1e5), shape=1, size=2.5) +
      ylab('daily_total_reported / population * 1e5')
    
    # Previous best model plot
    p5 <- g + geom_line(data = best_mod[location_id == loc_id & date < Sys.Date() + 90], 
                        aes(x = date, y = test_pc * 1e5, linetype = observed == 0), color = pal[2], lwd=2)
    
    # Latest model plot
    p6 <- g +  geom_line(data = latest_mod[location_id == loc_id & date < Sys.Date() + 90], 
                         aes(x = date, y = test_pc * 1e5, linetype = observed == 0), color = pal[1], lwd=2.5)
    
    # Getting the title to share across two plots 
    labs(title = paste0(hierarchy[location_id == loc_id, location_name], " (", loc_id, ")"),
         subtitle = paste0("Frontier was ", fron, "\n", paste(all_sources ,collapse = ", ")))
    
    title <- paste0(paste0(hierarchy[location_id == loc_id, location_name], "\n (", loc_id, ") "),
                    paste0("Frontier was ", fron[1], "\n"), paste(all_sources ,collapse = ", "))
    
    # Arrange the plots together
    grid.arrange(
      ggarrange(
        # Top = both versions of full plot with shared legend, and shared title
        annotate_figure(ggarrange(p1, p2, 
                                  common.legend = TRUE, legend = "bottom"),
                        top = text_grob(title)), 
        # Bottom = each individual observed data, model plots
        ggarrange(p3, p4, p5, p6, ncol = 2, nrow = 2), 
        nrow = 2), 
      newpage = TRUE)
    
  }
  
  dev.off()
  message(glue("Plotting complete."))
}


# Plot Scenarios  ----------------------

#' Plot the reference, better, and worse scenarios
plot_scenarios <- function(out_dt, in_dt, out_path) {
  melt_dt <- melt(out_dt[, .(
    location_id, location_name, date,
    raked_ref_test_pc,
    better_fcast, worse_fcast,
    total_pc_100, observed
  )],
  id.vars = c("location_id", "location_name", "date", "total_pc_100", "observed")
  )
  melt_dt[grepl("raked_ref", variable), scenario := "reference"]
  melt_dt[grepl("better", variable), scenario := "better"]
  melt_dt[grepl("worse", variable), scenario := "worse"]
  melt_dt[variable %in% c("raked_ref_test_pc", "better_fcast", "worse_fcast"), value := value * 1e5]
  
  melt_dt[, plot_name := paste0(location_id, " - ", location_name)]
  pdf(out_path, width = 12, height = 9)
  for (i in seq(ceiling(length(sort(unique(melt_dt$location_id))) / 12))) {
    loc_list <- sort(unique(melt_dt$location_id))[((i - 1) * 12 + 1):(min(i * 12, length(sort(unique(melt_dt$location_id)))))]
    plot_dt <- melt_dt[location_id %in% loc_list]
    plot_dt[observed == FALSE, total_pc_100 := NA]
    
    gg <- ggplot() +
      geom_line(data = plot_dt[!is.na(value)], aes(x = date, y = value, color = scenario), alpha = 0.5) +
      geom_point(data = in_dt[location_id %in% loc_list & !is.na(daily_total_reported)], aes(x = date, y = daily_total_reported / pop * 1e5), color = "black", size = 0.1) +
      ylab("Test  per 100k") +
      xlab("Date") +
      scale_linetype_manual(values = c("dashed", "solid")) +
      theme_bw() +
      facet_wrap(~location_name, nrow = 3, scale = "free") +
      theme(legend.position = "bottom")
    print(gg)
  }
  invisible(dev.off())
}


