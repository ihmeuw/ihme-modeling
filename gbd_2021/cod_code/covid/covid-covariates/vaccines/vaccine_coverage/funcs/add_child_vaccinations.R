
add_child_vaccinations <- function(
  vaccine_output_root,
  model_output_12_plus=NULL,                       # Output of uptake model output (with adult pop set to 12+ in US)
  eua_date = '2021-11-03',                         # Date of Emergency Use Authorization in US
  eua_date_lag = 7,                                # Number of days after EUA that vaccinations begin
  delivery_schedule = 365/4,                       # Default is delivery rate required to vaccinated all children in state in 6 months        
  dosing_period=21,                                # Number of days between first and second dose
  prop_parents_willing = 0.45,                     # Proportion of parents willing to vaccinate their children reported by KFF
  use_hesitancy = FALSE,                           # If TRUE and prop_parents_willing = NULL, then willingness in children is set to adult vaccinated + willing in each state
  lag_days_partial = 7,                            # Number of days until partial efficacy after first dose                       
  lag_days_full = 14,                              # Number days until full efficacy after second dose
  loss_followup = 0.1,                             # Proportion of people that do not follow up to get second dose
  efficacy_partial = 0.6,                          # Efficacy after one dose
  scenario_suffix = NULL                           # Suffix to add to file name
){
  
  # lr_vaccinated: daily low-risk (<65) people vaccinated
  # lr_unproteced: daily low-risk (<65) people who receive no benefit from vaccine
  # lr_effective_variant: daily low-risk (<65) people immune and protected from wildtype and variant
  # lr_effective_protected_variant: daily low-risk (<65) people protected severe disease from wildtype and variant
  # lr_effective_wildtype: daily low-risk (<65) people immune and protected from wildtype only
  # lr_effective_protected_wildtype: daily low-risk (<65) people protected severe disease from wildtype only
  
  message('Loading data')
  eua_date <- as.Date(eua_date, format='%Y-%m-%d')
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  
  # Scenario is for US only
  
  us_locs <- c(102, hierarchy[parent_id == 102, location_id])
  
  population <- .make_age_group_populations(model_inputs_path = 'FILEPATH', hierarchy = hierarchy, over_5 = TRUE)
  
  #empirical_lags <- fread(file.path(vaccine_output_root, 'empirical_lag_days.csv'))
  
  # Efficacy for Pfizer
  ve_table <- vaccine_data$load_vaccine_efficacy(vaccine_output_root)
  efficacy_infection_wildtype <- ve_table[candidate == 'BNT-162', prop_protected_not_infectious]
  efficacy_infection_variant <- ve_table[candidate == 'BNT-162', variant_infection]
  efficacy_disease_wildtype <- ve_table[candidate == 'BNT-162', efficacy]
  efficacy_disease_variant <- ve_table[candidate == 'BNT-162', variant_efficacy]
  
  hesitancy <- fread(file.path(vaccine_output_root, "time_point_vax_hesitancy_USA.csv"))
  
  # Uptake model output (with adult pop set to 12+)
  if (is.null(model_output_12_plus)) model_output_12_plus <- fread(file.path(vaccine_output_root, "slow_scenario_vaccine_coverage.csv"))


  message('Constructing child vaccination quantities for each location')

  out <- data.table()
  for (i in us_locs){
      
    print(i)
    
    x <- model_output_12_plus[location_id == i,]
    
    child_population <- population[location_id == i & age_group == '5-11', population]
    
    if (use_hesitancy == FALSE & !(is.null(prop_parents_willing))) {
      
      willing_child_population <- child_population * prop_parents_willing
      
    } else if (use_hesitancy & is.null(prop_parents_willing)) {
      
      willing_child_population <- child_population * hesitancy[location_id == i, vax_and_willing]
      
    } else {
      
      stop('Do you want to use prop_parents_willing or adult hesitancy by state?')
    }
    
    delivery_per_day <- willing_child_population/delivery_schedule

    
    
    # Set vaccine uptake schedule (shots in arms) for children
    tmp <- data.frame(date=as.Date(x$date), delivery_per_day=delivery_per_day)
    tmp[tmp$date <= (eua_date + eua_date_lag), 'delivery_per_day'] <- 0
    tmp[cumsum(tmp$delivery_per_day) > willing_child_population, 'delivery_per_day'] <- 0
    
    # Calculate vaccination status
    tmp$children_vaccinated_1d <- tmp$delivery_per_day
    tmp$children_vaccinated_2d <- shift(tmp$children_vaccinated_1d, n=dosing_period) * (1-loss_followup)
    
    # Efficacy that protects against infection
    tmp$children_effective_1d <- shift(tmp$children_vaccinated_1d, n=lag_days_partial) * efficacy_partial * efficacy_infection_wildtype
    tmp$children_effective_2d <- shift(tmp$children_vaccinated_2d, n=lag_days_full) * efficacy_infection_wildtype
    
    tmp$children_effective <- tmp$children_effective_1d + tmp$children_effective_2d
    tmp$children_unprotected <- tmp$children_effective_1d - tmp$children_effective
    
    tmp$children_effective_variant <- shift(tmp$children_vaccinated_2d, n=lag_days_full) * efficacy_infection_variant
    tmp$children_effective_wildtype <- tmp$children_effective - tmp$children_effective_variant
    
    # Efficacy that protects against severe disease
    tmp$children_effective_protected_1d <- shift(tmp$children_vaccinated_1d, n=lag_days_partial) * efficacy_partial * efficacy_disease_wildtype
    tmp$children_effective_protected_2d <- shift(tmp$children_vaccinated_2d, n=lag_days_full) * efficacy_disease_wildtype
    tmp$children_effective_protected <- tmp$children_effective_protected_1d + tmp$children_effective_protected_2d
    
    tmp$children_effective_protected_variant <- shift(tmp$children_vaccinated_2d, n=lag_days_full) * efficacy_disease_variant
    tmp$children_effective_protected_wildtype <- tmp$children_effective_protected - tmp$children_effective_protected_variant
    
    # Make mutually exclusive
    tmp$children_effective_protected_variant <- tmp$children_effective_protected_variant - tmp$children_effective_variant 
    tmp$children_effective_protected_wildtype <- tmp$children_effective_protected_wildtype - tmp$children_effective_wildtype
    
    
    # Clean up
    for (j in which(!(colnames(tmp) == 'date'))) {
      sel <- is.na(tmp[,j])
      tmp[sel,j] <- 0
    }
    
    setnames(tmp, 
             c('children_vaccinated_1d', 'children_vaccinated_2d'), 
             c('children_vaccinated', 'children_fully_vaccinated'))
    
    quants <- c(
      'children_vaccinated',
      'children_fully_vaccinated',
      'children_effective',
      'children_unprotected',
      'children_effective_variant',
      'children_effective_wildtype',
      'children_effective_protected_variant',
      'children_effective_protected_wildtype'
    )
    
    for (j in quants) {
      tmp_cumulative <- data.frame(cumsum(tmp[,j]))
      names(tmp_cumulative) <- paste0('cumulative_', j)
      tmp <- cbind(tmp, tmp_cumulative)
    }
    
    # Append child quantities to model output
    sel <- colnames(tmp) %in% c('date', quants, paste0('cumulative_', quants))
    x <- merge(x, tmp[,sel], by='date')
    
    
    #### Recalculate relevant quantities in output
    x$lr_vaccinated <- x$lr_vaccinated + x$children_vaccinated
    x$lr_unprotected <- x$lr_unprotected + x$children_unprotected
    x$lr_effective_variant <- x$lr_effective_variant + x$children_effective_variant 
    x$lr_effective_wildtype <- x$lr_effective_wildtype + x$children_effective_wildtype
    x$lr_effective_protected_wildtype <- x$lr_effective_protected_wildtype + x$children_effective_protected_wildtype
    x$lr_effective_protected_variant <- x$lr_effective_protected_variant + x$children_effective_protected_variant
    
    x$cumulative_lr_vaccinated <- x$cumulative_lr_vaccinated + x$cumulative_children_vaccinated
    x$cumulative_lr_unprotected <- x$cumulative_lr_unprotected + x$cumulative_children_unprotected
    x$cumulative_lr_effective_variant <- x$cumulative_lr_effective_variant + x$cumulative_children_effective_variant 
    x$cumulative_lr_effective_wildtype <- x$cumulative_lr_effective_wildtype + x$cumulative_children_effective_wildtype
    x$cumulative_lr_effective_protected_wildtype <- x$cumulative_lr_effective_protected_wildtype + x$cumulative_children_effective_protected_wildtype
    x$cumulative_lr_effective_protected_variant <- x$cumulative_lr_effective_protected_variant + x$cumulative_children_effective_protected_variant
    
    x$cumulative_all_vaccinated <- x$cumulative_all_vaccinated + x$cumulative_children_vaccinated
    x$cumulative_all_fully_vaccinated <- x$cumulative_all_fully_vaccinated + x$cumulative_children_fully_vaccinated
    x$cumulative_all_effective <- x$cumulative_all_effective + x$cumulative_children_effective
    
    x$elderly_vaccinated <- c(0, diff(x$cumulative_elderly_effective))
    
    
    #Check
    #plot(x$date, x$cumulative_all_effective)
    #test <- model_output_12_plus[location_id == i,]
    #points(test$date, test$cumulative_all_effective, col='blue')
    
    out <- rbind(out, x)
    
  }
  
  
  
  #### Make model output
  #message('Saving new model output')
  model_output_5_plus <- rbind(model_output_12_plus[!(location_id %in% us_locs),], out, fill=TRUE)
  
  #if (is.null(scenario_suffix)) {
  #  tmp_name <- file.path(vaccine_output_root, glue("slow_scenario_vaccine_coverage.csv"))
  #} else {
  #  tmp_name <- file.path(vaccine_output_root, glue("slow_scenario_vaccine_coverage_children_{scenario_suffix}.csv"))
  #}
  #  
  #write.csv(model_output_5_plus, file = tmp_name)
  #message(tmp_name)
  
  
  
  #### Make some dumb plots
  message('Making some dumb plots')
  
  if (is.null(scenario_suffix)) {
    tmp_path <- file.path(vaccine_output_root, glue("child_vacination_schedule_{.get_version_from_path(vaccine_output_root)}.pdf"))
  } else {
    tmp_path <- file.path(vaccine_output_root, glue("child_vacination_schedule_{scenario_suffix}_{.get_version_from_path(vaccine_output_root)}.pdf"))
  }
  
  pdf(tmp_path, height=9, width=9, onefile = TRUE)
  
  for (i in us_locs) {
    
    child_population <- population[location_id == i & age_group == '5-11', population]
    
    if (use_hesitancy == FALSE & !(is.null(prop_parents_willing))) {
      
      willing_child_population <- child_population * prop_parents_willing
      
    } else if (use_hesitancy & is.null(prop_parents_willing)) {
      
      willing_child_population <- child_population * hesitancy[location_id == i, vax_and_willing]
      
    }
    
    
    g <- ggplot(data=model_output_5_plus[location_id == i,], aes(x=as.Date(date))) + xlab('Date') + theme_bw() + theme(legend.position = "none")
    
    leg <- cowplot::get_legend(
      g +
        geom_line(aes(y = children_vaccinated, color = "Vaccinated"), size=0.7) +
        geom_line(aes(y = children_effective_wildtype, col = "Infection (wildtype)"), size=0.7) +
        geom_line(aes(y = children_effective_variant, color = "Infection (variant)"), size=0.7) +
        geom_line(aes(y = children_effective_protected_wildtype, col = "Disease (wildtype)"), size=0.7) +
        geom_line(aes(y = children_effective_protected_variant, col = "Disease (variant)"), size=0.7) + 
        scale_color_discrete("") +
        xlab("Date") + ylab("Daily other adults") + 
        theme_bw() + theme(legend.position = "bottom")
    )
    
    ggrid <- grid.arrange(
      
      g + geom_line(aes(y=children_vaccinated), size=1) +
        geom_vline(xintercept = eua_date, lty=2, color='dodgerblue') +
        ylab('Age 5-11 vaccinated per day'),
      
      g + geom_line(aes(y=adults_vaccinated), size=1) +
        ylab('Age 12-64 vaccinated per day'),
      
      g + geom_line(aes(y=elderly_vaccinated), size=1) +
        ylab('Age 65+ vaccinated per day'),
      
      g + 
        geom_hline(yintercept = child_population, lty=2) +
        geom_hline(yintercept = willing_child_population, lty=2, color='red') +
        geom_line(aes(y=cumulative_children_vaccinated), size=1) +
        geom_vline(xintercept = eua_date, lty=2, color='dodgerblue') +
        ylab('Cumulative 5-11 vaccinated'),
      
      g + 
        geom_line(aes(y=cumulative_adults_vaccinated), size=1) +
        ylab('Cumulative 12-64 vaccinated'),
      
      g + 
        geom_line(aes(y=cumulative_elderly_vaccinated), size=1) +
        ylab('Cumulative 65+ vaccinated'),
      
      g +
        geom_line(aes(y = children_vaccinated, color = "Vaccinated"), size=0.7) +
        geom_line(aes(y = children_effective_wildtype, col = "Infection (wildtype)"), size=0.7) +
        geom_line(aes(y = children_effective_variant, color = "Infection (variant)"), size=0.7) +
        geom_line(aes(y = children_effective_protected_wildtype, col = "Disease (wildtype)"), size=0.7) +
        geom_line(aes(y = children_effective_protected_variant, col = "Disease (variant)"), size=0.7) + 
        scale_color_discrete("") +
        ylab("Daily 5-11 vaccination status"),
      
      g +
        geom_line(aes(y = lr_vaccinated, color = "Vaccinated"), size=0.7) +
        geom_line(aes(y = lr_effective_wildtype, col = "Infection (wildtype)"), size=0.7) +
        geom_line(aes(y = lr_effective_variant, color = "Infection (variant)"), size=0.7) +
        geom_line(aes(y = lr_effective_protected_wildtype, col = "Disease (wildtype)"), size=0.7) +
        geom_line(aes(y = lr_effective_protected_variant, col = "Disease (variant)"), size=0.7) + 
        scale_color_discrete("") +
        ylab("Daily 12-64 vaccination status"),
      
      g +
        geom_line(aes(y = hr_vaccinated, color = "Vaccinated"), size=0.7) +
        geom_line(aes(y = hr_effective_wildtype, col = "Infection (wildtype)"), size=0.7) +
        geom_line(aes(y = hr_effective_variant, color = "Infection (variant)"), size=0.7) +
        geom_line(aes(y = hr_effective_protected_wildtype, col = "Disease (wildtype)"), size=0.7) +
        geom_line(aes(y = hr_effective_protected_variant, col = "Disease (variant)"), size=0.7) + 
        scale_color_discrete("") +
        ylab("Daily 65+ vaccination status"),
      
      nrow = 3, 
      ncol = 3,
      top = hierarchy[location_id == i, location_name]
    )
    
    plot_grid(ggrid, leg, nrow=2, rel_heights = c(12,1))
    
    
  }
  
  dev.off()
  
  
  
  if (is.null(scenario_suffix)) {
    tmp_path <- file.path(vaccine_output_root, glue("child_vacination_diff_{.get_version_from_path(vaccine_output_root)}.pdf"))
  } else {
    tmp_path <- file.path(vaccine_output_root, glue("child_vacination_diff_{scenario_suffix}_{.get_version_from_path(vaccine_output_root)}.pdf"))
  }
  
  
  pdf(tmp_path, height=4, width=10, onefile = TRUE)
  
  for (i in us_locs) {
    
    delta <- round(model_output_5_plus[location_id == i & date == '2023-12-31', 'cumulative_all_vaccinated'] - model_output_12_plus[location_id == i & date == '2023-12-31', 'cumulative_all_vaccinated'])
    
    ymax <- max(model_output_5_plus[location_id == i, cumulative_all_vaccinated])
    
    g <- ggplot(data=model_output_5_plus[location_id == i,], aes(x=as.Date(date))) + ylim(0, ymax) + xlab('Date') + theme_bw() + theme(legend.position = "none")
    
    leg <- cowplot::get_legend(
   
      g + 
        geom_line(aes(y=cumulative_all_vaccinated, color='5+'), size=1) +
        geom_line(data = model_output_12_plus[location_id == i,],
                  aes(y=cumulative_all_vaccinated, color='12+'), size=1) +
        theme(legend.position = 'bottom') + scale_color_discrete("")
      
    )
    
    ggrid <- grid.arrange(

      g + 
        geom_line(aes(y=cumulative_all_vaccinated, color='5+'), size=1) +
        geom_line(data = model_output_12_plus[location_id == i,],
                  aes(y=cumulative_all_vaccinated, color='12+'), size=1) +
        annotate(geom = 'text', label = glue('Difference = {delta}'), x = as.Date('2021-12-31'), y = ymax*0.5, hjust = 0, vjust = 1) + 
        ylab('Cumulative total vaccinated') +
        ggtitle('Initially vaccinated'),
      
      g + 
        geom_line(aes(y=cumulative_all_fully_vaccinated, color='5+'), size=1) +
        geom_line(data = model_output_12_plus[location_id == i,],
                  aes(y=cumulative_all_fully_vaccinated, color='12+'), size=1) +
        ylab('Cumulative total vaccinated') +
        ggtitle('Fully vaccinated'),
      
      g + 
        geom_line(aes(y=cumulative_all_effective, color='5+'), size=1) +
        geom_line(data = model_output_12_plus[location_id == i,],
                  aes(y=cumulative_all_effective, color='12+'), size=1) +
        ylab('Cumulative total vaccinated') +
        ggtitle('Effectively vaccinated'),
      
      
      nrow = 1, 
      ncol = 3,
      top = hierarchy[location_id == i, location_name]
    )
    
    plot_grid(ggrid, leg, nrow=2, rel_heights = c(12,1))
    
    
  }
  
  dev.off()
  
  message('Done')
  return(model_output_5_plus)
}



#scenario_path_1 <- "FILEPATH/slow_scenario_vaccine_coverage_children_low.csv"
#scenario_path_2 <- "FILEPATH/slow_scenario_vaccine_coverage_children_high.csv"

plot_children_scenario_compare <- function(vaccine_output_root,
                                           scenario_path_1, 
                                           scenario_path_2,
                                           scenario_name_1=NULL,
                                           scenario_name_2=NULL,
                                           hierarchy
) {
  

  
  s1 <- fread(scenario_path_1)
  s2 <- fread(scenario_path_2)
  
  if (is.null(scenario_name_1)) scenario_name_1 <- 'Scenario 1'
  if (is.null(scenario_name_2)) scenario_name_2 <- 'Scenario 2'
  
  s1$scenario <- scenario_name_1
  s2$scenario <- scenario_name_2
  
  # US only
  
  us_locs <- c(102, hierarchy[parent_id == 102, location_id])
  
  s1 <- s1[location_id %in% us_locs,]
  s2 <- s2[location_id %in% us_locs,]
  
  s <- rbind(s1, s2, fill=TRUE)
  
  # Need to fix later
  #tmp_path <- "FILEPATH/child_vaccination_scenario_compare_2021_11_04.07.pdf"
  tmp_path <- file.path(vaccine_output_root, 'child_vaccination_scenario_compare.pdf')
  
  pdf(tmp_path, height=4, width=15, onefile = TRUE)
  
  for (i in us_locs) {
    
    delta <- round(s2[location_id == i & date == '2023-12-31', 'cumulative_all_vaccinated'] - s1[location_id == i & date == '2023-12-31', 'cumulative_all_vaccinated'])
    delta_all <- paste0('(', format(delta, big.mark=','), ifelse(delta > 0, ' increase', 'decrease'), ')')
    
    delta <- round(s2[location_id == i & date == '2023-12-31', 'cumulative_all_fully_vaccinated'] - s1[location_id == i & date == '2023-12-31', 'cumulative_all_fully_vaccinated'])
    delta_full <- paste0('(', format(delta, big.mark=','), ifelse(delta > 0, ' increase', 'decrease'), ')')
    
    delta <- round(s2[location_id == i & date == '2023-12-31', 'cumulative_all_effective'] - s1[location_id == i & date == '2023-12-31', 'cumulative_all_effective'])
    delta_eff <- paste0('(', format(delta, big.mark=','), ifelse(delta > 0, ' increase', 'decrease'), ')')
    
    ymax <- max(s2[location_id == i, cumulative_all_vaccinated])
    
    g <- ggplot(data=s[location_id == i,], aes(x=as.Date(date))) + 
      scale_y_continuous("Cumulative vaccianted", label = comma, limits = c(0, ymax)) +
      xlab('Date') + theme_bw() +
      theme(panel.grid.major = element_blank(), 
            panel.grid.minor = element_blank(),
            legend.position = "none") +
      scale_color_manual("Scenario", values = c("#377EB8", "#E41A1C"))
    
    leg <- cowplot::get_legend(
      g + geom_line(aes(y=cumulative_all_vaccinated, color=scenario), size=1) +
        theme(legend.position = 'bottom') 
    )
    
    grid.arrange(
      
      g + 
        geom_line(aes(y=cumulative_all_vaccinated, color=scenario), size=1) +
        geom_line(data = s2[location_id == i,],
                  aes(y=cumulative_all_vaccinated, color=scenario), size=1) +
        ggtitle(glue('Initially vaccinated {delta_all}')),
      
      g + 
        geom_line(aes(y=cumulative_all_fully_vaccinated, color=scenario), size=1) +
        geom_line(data = s2[location_id == i,],
                  aes(y=cumulative_all_fully_vaccinated, color=scenario), size=1) +
        ggtitle(glue('Fully vaccinated {delta_full}')),
      
      g + 
        geom_line(aes(y=cumulative_all_effective, color=scenario), size=1) +
        geom_line(data = s2[location_id == i,],
                  aes(y=cumulative_all_effective, color=scenario), size=1) +
        theme(legend.position = c(0.85, 0.2)) +
        ggtitle(glue('Effectively vaccinated {delta_eff}')),
      
      nrow = 1, 
      ncol = 3,
      top = hierarchy[location_id == i, location_name]
    )
  
    #plot_grid(ggrid, leg, nrow=2, rel_heights = c(10,1))
    
  }
  
  dev.off()
  
  message('Done')
  
}

