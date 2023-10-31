.plot_hesitancy_comparison <- function(
  vaccine_output_root,
  version_1,
  version_2,
  name_1=NULL,
  name_2=NULL
) {
  
  hierarchy <- gbd_data$get_covid_covariate_prep_hierarchy()
  
  if (is.null(name_1)) name_1 <- version_1
  if (is.null(name_2)) name_2 <- version_2
  
  path_version_1 <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, version_1)
  path_version_2 <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, version_2)
  
  out1 <- vaccine_data$load_scenario_forecast(path_version_1, scenario = 'slow')
  #out2 <- vaccine_data$load_scenario_forecast(path_version_2, scenario = 'slow')
  
  hes1 <- vaccine_data$load_time_series_vaccine_hesitancy(path_version_1, "default")
  hes1$name <- name_1
  hes2 <- vaccine_data$load_time_series_vaccine_hesitancy(path_version_2, "default")
  hes2$name <- name_2
  
  hes <- rbind(hes1, hes2)
  
  message('Plotting hesitancy comparison plots')

  pal <- RColorBrewer::brewer.pal(9, 'Set1')
  plot_locs <- unique(hes$location_id)
  
  euro_scenario <- FALSE
  
  if (euro_scenario) {
    
    # Get location_id in covid hierarchy that corresponds to WHO Euro region
    who_euro_hierarchy <- get_location_metadata(location_set_id = 57, location_set_version_id = 765, release_id = 9)
    euro_locs <- hierarchy[location_name %in% who_euro_hierarchy[level > 0, location_name], location_id] # Get covid location_id
    euro_locs <- hierarchy[location_id %in% euro_locs | parent_id %in% euro_locs, location_id] # Keep all subnationals
    
    # Confirm we have all locations in WHO European region
    sel <- who_euro_hierarchy[level > 0, location_name] %in% hierarchy[location_id %in% euro_locs, location_name]
    if (!all(sel)) warning('Missing WHO locations in Euro scenario')
    
    plot_locs <- euro_locs
  }
  
  
  #-----------------------------------------------------------------------------
  # Plot scatter plots
  
  # Mutual maximum date
  t_max <- min(hes1$max_date[1], hes2$max_date[1])

  # Scatter plot
  tmp <- merge(hes1[date == t_max, .(location_id, location_name, date, smooth_combined_yes)],
               hes2[date == t_max, .(location_id, location_name, date, smooth_combined_yes)],
               by=c('location_id', 'location_name', 'date'))
  
  tmp <- tmp[location_id %in% plot_locs,]
  axis_range <- range(c(tmp$smooth_combined_yes.x, tmp$smooth_combined_yes.y))
  
  ratio <- tmp$smooth_combined_yes.x/tmp$smooth_combined_yes.y
  cutoff <- quantile(ratio, probs=c(0.0275, 0.975))
  sel_labels <- which(ratio < cutoff[1] | ratio > cutoff[2])

  pdf(file.path(vaccine_output_root, glue("vaccine_hesitancy_scatter_{version_1}_{version_2}.pdf")), height=9.5, width=9.5, onefile=TRUE)
 
   tmp <- ggplot(data=tmp) + 
    geom_text_repel(data=tmp[sel_labels,], aes(x=smooth_combined_yes.x, y=smooth_combined_yes.y, label = location_name), color='grey30', size=3, segment.size=0.2) +
    geom_point(aes(x=smooth_combined_yes.x, y=smooth_combined_yes.y), size=2) +
    geom_abline(intercept = 0, slope = 1) +
    scale_x_continuous(name_1, limits = axis_range) +
    scale_y_continuous(name_2, limits = axis_range) + 
    ggtitle(glue('Comparison of hesitancy scenarios on {t_max}')) +
    theme_bw()
   
   print(tmp)
   
   tmp <- hes1[date == hes1$max_date[1], ]
   sel_labels <- which(tmp$pct_vaccinated > tmp$smooth_combined_yes)
   
   tmp <- ggplot(data=tmp) + 
     geom_text_repel(data=tmp[sel_labels,], aes(x=smooth_combined_yes, y=pct_vaccinated, label = location_name), color='grey30', size=3, segment.size=0.2) +
     geom_point(aes(x=smooth_combined_yes, y=pct_vaccinated), size=2) +
     geom_abline(intercept = 0, slope = 1) +
     xlab('pct_vaccinted + willing') +
     ylab('pct_vaccinted') +
     ggtitle(glue('Version {version_1}: pct_vaccianted vs pct_vaccinted + willing on {hes1$max_date[1]}')) +
     theme_bw()
   
   print(tmp)
  
  dev.off()
  
  
  
  
  #-----------------------------------------------------------------------------
  # Plot comparison by location
  
  pdf(file.path(vaccine_output_root, glue("vaccine_hesitancy_compare_{version_1}_{version_2}.pdf")), height=5, width=11, onefile = TRUE)
  
  for (loc in plot_locs) {
    
    g <- ggplot(hes[location_id == loc], aes(x=date)) + ylim(0,1) + xlim(range(hes1$date)) + theme_minimal() + theme(legend.position = 'none') + scale_shape(name='', solid=F)
    
    cols <- c(pal[2], pal[5])
    names(cols) <- c(name_1, name_2)
    
    leg <- cowplot::get_legend(
      g +
        geom_point(aes(y=combined_yes, shape=name), size=2.5) +
        geom_line(aes(y=smooth_combined_yes, color=name), size=2) +
        scale_color_manual(name="", values=cols) +
        theme(legend.position = 'bottom')
    )
    
    tmp <- plot_grid(
      g +
        geom_point(data=out1[location_id == loc], aes(x=date, y=people_vaccinated/adult_population), size=2.5) +
        ylab('Proportion of the adult (12+) population') +
        ggtitle('Cumulative proportion vaccinated'),
      
      g +
        geom_point(aes(y=survey_yes, shape=name), size=2.5) +
        ylab('') +
        ggtitle('Acceptance in unvaccinated'),
      
      g +
        geom_point(aes(y=combined_yes, shape=name), size=2.5) +
        geom_line(aes(y=smooth_combined_yes, color=name), size=2) +
        scale_color_manual(name="Scenario", values=cols) +
        ylab('') +
        ggtitle('Vaccinated + willing'),
      
      ncol=3,
      nrow=1
    )
    
    grid.arrange(grid::textGrob(hes[location_id == loc, location_name][1]),
                 tmp, 
                 as_grob(leg), 
                 ncol=1, 
                 nrow=3,
                 heights=c(1,8,1))

  }
  
  dev.off()
}

  