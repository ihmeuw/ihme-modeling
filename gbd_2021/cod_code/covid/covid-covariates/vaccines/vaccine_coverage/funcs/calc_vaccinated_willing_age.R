
calc_vaccinated_willing_age <- function(vaccine_output_root,
                                        hierarchy,
                                        plots=FALSE
                                        ) {
  
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  age_starts <- model_parameters$age_starts
  include_all_ages <- model_parameters$include_all_ages
  
  #-----------------------------------------------------------------------------
  # Load data
  #-----------------------------------------------------------------------------
  
  message('Loading data')

  mods_bias_corrected_age <- fread(file.path(vaccine_output_root, 'smooth_pct_vaccinated_age_adjusted.csv'))
  observed_survey <- fread(file.path(vaccine_output_root, "observed_survey_data_age.csv"))
  smooth_survey <- fread(file.path(vaccine_output_root, "smooth_survey_yes_age.csv"))
  
  mods_bias_corrected_age$date <- as.Date(mods_bias_corrected_age$date)
  observed_survey$date <- as.Date(observed_survey$date)
  smooth_survey$date <- as.Date(smooth_survey$date)
  
  
  
  message('Calculating vaccinated plus willing')
  
  merge_cols <- c('location_id', 'date', 'age_group')
  mods <- merge(mods_bias_corrected_age[,c(merge_cols, 'pct_vaccinated', 'smooth_pct_vaccinated_adjusted_raked'), with=F],
                smooth_survey[,c(merge_cols, 'smooth_survey_yes'), with=F],
                by=merge_cols)
  
  
  
  mods$smooth_combined_yes <- mods$smooth_pct_vaccinated_adjusted_raked + (1 - mods$smooth_pct_vaccinated_adjusted_raked) * mods$smooth_survey_yes

  
  check <- mods$smooth_pct_vaccinated_adjusted_raked > mods$smooth_combined_yes
  
  if (any(check[!is.na(check)])) {
    
    message('WARNING: pct_vaccinated > pct_vaccinated + willing in the following locations:')
    problem_locs <- mods$location_id[which(check)]
    print(hierarchy[location_id %in% problem_locs, .(location_id, location_name)])

  } else {
    message('pct_vaccinated < pct_vaccinated + willing in all locations')
  }
  
  
  # Save
  tmp_name <- file.path(vaccine_output_root, 'vaccinated_and_willing.csv')
  write.csv(mods, file=tmp_name, row.names = F)
  message(glue('Output: {tmp_name}'))
  
  
  
  
  if (plots) {
    
    pal <- RColorBrewer::brewer.pal(9, 'Set1')
    
    #loc <- 523
    #age <- '12-125'
    
    tmp_name <- file.path(vaccine_output_root, glue("vaccinated_and_willing.pdf"))
    pdf(tmp_name, height=8, width=10, onefile=TRUE)
    
    for (loc in unique(mods$location_id)) {
      
      plot_list <- list()
      
      for (age in unique(mods$age_group)) {
        
        tmp_mod <- mods[location_id == loc & age_group == age,]
        
        sel_max_date <- which.max(tmp_mod$date[!is.na(tmp_mod$smooth_pct_vaccinated_adjusted_raked) & !is.na(tmp_mod$smooth_survey_yes)])
        if (length(sel_max_date) == 0) sel_max_date <- NA
        
        g <- ggplot(tmp_mod, aes(x=date)) + ylim(0,1) + theme_minimal() 
        
        g <- plot_grid(
          
          g + 
            geom_hline(aes(yintercept=smooth_pct_vaccinated_adjusted_raked[sel_max_date]), color=pal[1], lty=2) +
            geom_hline(aes(yintercept=smooth_combined_yes[sel_max_date]), color=pal[4], lty=2) +
            geom_point(aes(y=pct_vaccinated), size=3, alpha=0.4) +
            geom_line(aes(y=smooth_pct_vaccinated_adjusted_raked), color=pal[1], size=2) +
            ylab('Proportion vaccinated'),
          
          g + 
            geom_point(data=observed_survey[location_id == loc & age_group == age,], 
                       aes(y=survey_yes, size=sample_size), alpha=0.4) +
            geom_line(aes(y=smooth_survey_yes), color=pal[2], size=2) +
            theme(legend.position = 'none') +
            ylab('Proportion unvaccinated willing'),
          
          g + 
            geom_hline(aes(yintercept=smooth_combined_yes[sel_max_date]), color=pal[4], lty=2) +
            geom_line(aes(y=smooth_combined_yes), color=pal[4], size=2) +
            ylab('Proportion vaccinated OR willing'),
          
          ncol=3
        )
        
        g <- plot_grid(grid::textGrob(age),
                       g,
                       ncol=1, 
                       rel_heights=c(0.05, 1))
        
        plot_list <- c(plot_list, list(g))
        
      }
      
      p <- plot_grid(ggdraw() + draw_label(glue("{hierarchy[location_id == loc, location_name]} ({loc})"), fontface='bold'), 
                     plot_grid(plotlist=plot_list, ncol=1), 
                     ncol=1, 
                     rel_heights=c(0.05, 1)) 
      
      print(p)
      
    }
    
    graphics.off()
    message(glue('Plots: {tmp_name}'))
    
  }
  

}

