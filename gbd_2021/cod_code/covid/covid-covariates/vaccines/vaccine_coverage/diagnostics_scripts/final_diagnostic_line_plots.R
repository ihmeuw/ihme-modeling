
rm(list=ls())

library(argparse)
library(data.table)
library(ggplot2)
library(scales)
library(gridExtra)
library(RColorBrewer)

source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))

source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))

source(file.path(CODE_PATHS$VACCINE_CODE_ROOT, "vaccine_projection_functions.R"))

## Setup and background
  # Set up command line argument parser
  parser <- argparse::ArgumentParser(
    description = 'Launch a location-specific COVID results brief',
    allow_abbrev = FALSE
  )
  
  parser$add_argument('--version', help = 'Full path to vaccine covariate version')
  parser$add_argument('--scenario_name', help = 'Vaccine scenario')
  
  message(paste(c('COMMAND ARGS:',commandArgs(TRUE)), collapse = ' '))
  args <- parser$parse_args(commandArgs(TRUE))
  
  vaccine_path <- args$version
  scenario_name <- args$scenario_name
  
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()

## Read in data and make plots
  plot_dt <- vaccine_data$load_scenario_forecast(vaccine_path, scenario_name)
  plot_dt[, cumulative_all_vaccinated := cumsum(lr_vaccinated + hr_vaccinated), by = "location_id"]
  #plot_dt <- clean_up(plot_dt)
  loc_order <- hierarchy[location_id %in% unique(plot_dt$location_id)]
  location_plots <- loc_order[order(sort_order)]$location_id
  observed <- vaccine_data$load_observed_vaccinations(vaccine_path)
  observed[, date := as.Date(date)]
  
  pdf(file.path(vaccine_path, glue("{scenario_name}_scenario_vaccine_doses_single_line.pdf")), height = 8, width = 5)
  
  for(loc_id in location_plots){
    
    print(loc_id)
    plot_dt[, prop_vaccinated := cumulative_all_vaccinated / adult_population]
    plot_dt[, scale1 := cumulative_all_vaccinated / prop_vaccinated]
    scale1 <- plot_dt[location_id == loc_id, mean(scale1, na.rm = TRUE)]
    
    if(max(plot_dt[location_id == loc_id]$cumulative_all_vaccinated) == 0) scale1 <- 100
    
    p <- ggplot(plot_dt[location_id == loc_id & date < "2022-01-01"]) + 
      geom_line(aes(x=as.Date(date), y=cumulative_all_vaccinated, col = "Received vaccine")) + 
      geom_line(aes(x=as.Date(date), y=cumulative_all_effective, col = "Effectively vaccinated")) + 
      geom_point(data = observed[location_id == loc_id], aes(x = date, y = reported_vaccinations - fully_vaccinated), size = 3, pch = 1) +
      scale_color_manual("", values = c("#bfe0ff", "#142f69")) + 
      theme_bw() + theme(legend.position = "bottom") +  
      ggtitle("Full year 2021") + 
      scale_y_continuous("People", labels = comma, sec.axis = sec_axis(~./scale1, name="Percent of adult population", labels = percent)) +
      xlab("")
      #scale_y_continuous("Cumulative people vaccinated", labels = comma) + xlab("")
    
    q <- ggplot(plot_dt[location_id == loc_id & date < "2021-05-01"]) + 
      geom_line(aes(x=as.Date(date), y=cumulative_all_vaccinated, col = "Received vaccine")) +
      geom_line(aes(x=as.Date(date), y=cumulative_all_effective, col = "Effectively vaccinated")) + 
      geom_point(data = observed[location_id == loc_id], aes(x = date, y = reported_vaccinations), size = 2, shape = 3) +
      geom_point(data = observed[location_id == loc_id], aes(x = date, y = fully_vaccinated), size = 2, shape = 2) +
      geom_point(data = observed[location_id == loc_id], aes(x = date, y = reported_vaccinations - fully_vaccinated), size = 3, shape = 1) +
      #scale_color_manual("", values = c("#ee8080", "#980000")) +  
      scale_color_manual("", values = c("#bfe0ff", "#142f69")) +
      theme_bw() + theme(legend.position = "bottom") +
      ggtitle("Through next month") + 
      scale_y_continuous("People", labels = comma, sec.axis = sec_axis(~./scale1, name="Percent of adult population", labels = percent)) +
      xlab("")

    grid.arrange(q, p, top = unique(plot_dt[location_id==loc_id]$location_name))
    
  }
  
  dev.off()

  # pdf(paste0("FILEPATH", version, "/", scenario_name,"_scenario_vaccine_subtype_line.pdf"), height = 6, width = 12)
  #   for(loc_id in location_plots){
  #     elderly <- ggplot(plot_dt[location_id == loc_id & date < "2021-12-31"], aes(x = as.Date(date))) +
  #       geom_line(aes(y = cumulative_elderly_vaccinated, col = "vaccinated")) +
  #       geom_line(aes(y = cumulative_elderly_effective, col = "effective")) +
  #       geom_line(aes(y = cumulative_elderly_effective_variant, col = "variant, immune")) +
  #       geom_line(aes(y = cumulative_elderly_effective_wildtype, col = "wildtype, immune")) +
  #       geom_line(aes(y = cumulative_elderly_effective_protected_wildtype, col = "wildtype, severe disease")) +
  #       geom_line(aes(y = cumulative_elderly_effective_protected_variant, col = "variant, severe disease")) +
  #       geom_line(aes(y = cumulative_elderly_effective_variant + cumulative_elderly_effective_wildtype +
  #                       cumulative_elderly_effective_protected_wildtype + cumulative_elderly_effective_protected_variant,
  #                     col = "sum subgroups"), lty = 2) + scale_color_discrete("") +
  #       theme_bw() + xlab("Date") + ylab("Cumulative elderly") + theme(legend.position = "bottom")
  #     adults <- ggplot(plot_dt[location_id == loc_id & date < "2021-12-31"], aes(x = as.Date(date))) +
  #       geom_line(aes(y = cumulative_adults_vaccinated, col = "vaccinated")) +
  #       geom_line(aes(y = cumulative_adults_effective, col = "effective")) +
  #       geom_line(aes(y = cumulative_adults_effective_variant, col = "variant, immune")) +
  #       geom_line(aes(y = cumulative_adults_effective_wildtype, col = "wildtype, immune")) +
  #       geom_line(aes(y = cumulative_adults_effective_protected_wildtype, col = "wildtype, severe disease")) +
  #       geom_line(aes(y = cumulative_adults_effective_protected_variant, col = "variant, severe disease")) +
  #       geom_line(aes(y = cumulative_adults_effective_variant + cumulative_adults_effective_wildtype +
  #                       cumulative_adults_effective_protected_wildtype + cumulative_adults_effective_protected_variant,
  #                     col = "sum subgroups"), lty = 2) + scale_color_discrete("") +
  #       theme_bw() + xlab("Date") + ylab("Cumulative adults") + theme(legend.position = "bottom")
  #     essential <- ggplot(plot_dt[location_id == loc_id & date < "2021-12-31"], aes(x = as.Date(date))) +
  #               geom_line(aes(y = cumulative_essential_vaccinated, col = "vaccinated")) +
  #               geom_line(aes(y = cumulative_essential_effective, col = "effective")) +
  #               geom_line(aes(y = cumulative_essential_effective_variant, col = "variant, immune")) +
  #               geom_line(aes(y = cumulative_essential_effective_wildtype, col = "wildtype, immune")) +
  #               geom_line(aes(y = cumulative_essential_effective_protected_wildtype, col = "wildtype, severe disease")) +
  #               geom_line(aes(y = cumulative_essential_effective_protected_variant, col = "variant, severe disease")) +
  #               geom_line(aes(y = cumulative_essential_effective_variant + cumulative_essential_effective_wildtype +
  #                                 cumulative_essential_effective_protected_wildtype + cumulative_essential_effective_protected_variant,
  #                             col = "sum subgroups"), lty = 2) + scale_color_discrete("") +
  #       theme_bw() + xlab("Date") + ylab("Cumulative essential") + theme(legend.position = "bottom")
  #     grid.arrange(elderly, essential, adults, nrow = 1, top = unique(plot_dt[location_id==loc_id]$location_name))
  # }
  # dev.off()
  
  plot_dt[, cumulative_all_effective_variant := 
            cumulative_elderly_effective_variant + cumulative_essential_effective_variant + cumulative_adults_effective_variant]
  plot_dt[, cumulative_all_effective_wildtype := 
            cumulative_elderly_effective_wildtype + cumulative_essential_effective_wildtype + cumulative_adults_effective_wildtype]
  plot_dt[, cumulative_all_effective_protected_variant := 
            cumulative_elderly_effective_protected_variant + cumulative_essential_effective_protected_variant + cumulative_adults_effective_protected_variant]
  plot_dt[, cumulative_all_effective_protected_wildtype := 
            cumulative_elderly_effective_protected_wildtype + cumulative_essential_effective_protected_wildtype + cumulative_adults_effective_protected_wildtype]
  
  pdf(file.path(vaccine_path, glue("{scenario_name}_scenario_vaccine_subtype_line.pdf")), height = 6, width = 12)
  
  for(loc_id in location_plots){
    
    plot_dt[, prop_vaccinated := cumulative_all_vaccinated / adult_population]
    plot_dt[, scale1 := cumulative_all_vaccinated / prop_vaccinated]
    scale1 <- plot_dt[location_id == loc_id, mean(scale1, na.rm = TRUE)]
    
    if(max(plot_dt[location_id == loc_id]$cumulative_all_vaccinated) == 0) scale1 <- 100
    
    hr <- ggplot(plot_dt[location_id == loc_id & date < "2021-12-31"], aes(x = as.Date(date))) +
      geom_line(aes(y = cumulative_all_vaccinated, col = "Vaccinated")) +
      geom_line(aes(y = cumulative_all_effective_variant, col = "Immune variant")) +
      geom_line(aes(y = cumulative_all_effective_wildtype +
                      cumulative_all_effective_variant, col = "Immune wildtype")) +
      geom_line(aes(y = cumulative_all_effective_protected_variant +
                      cumulative_all_effective_variant, col = "Effective\nvariant disease")) +
      geom_line(aes(y = cumulative_all_effective_variant + cumulative_all_effective_wildtype +
                      cumulative_all_effective_protected_wildtype + cumulative_all_effective_protected_variant,
                    col = "Effective\nwildtype disease")) + 
      theme_bw() + xlab("Date") + 
      scale_y_continuous("Cumulative adults", labels = comma, 
                         sec.axis = sec_axis(~./scale1, name="Percent of adult population", labels = percent)) + 
      theme(legend.position = "bottom") + scale_color_discrete("", guide = guide_legend(nrow = 2)) +
      ggtitle(unique(plot_dt[location_id == loc_id, location_name]))
    
    print(hr)
    
  }
  
  dev.off()
  
  pdf(file.path(vaccine_path, glue("{scenario_name}_scenario_vaccine_subtype_daily_line.pdf")), height = 6, width = 12)
  
  for(loc_id in location_plots){
    
    hr <- ggplot(plot_dt[location_id == loc_id & date < "2021-12-31"], aes(x = as.Date(date))) +
      geom_line(aes(y = hr_vaccinated, col = "vaccinated")) +
      geom_line(aes(y = hr_effective_variant, col = "variant, immune")) +
      geom_line(aes(y = hr_effective_wildtype, col = "wildtype, immune")) +
      geom_line(aes(y = hr_effective_protected_wildtype, col = "wildtype, severe disease")) +
      geom_line(aes(y = hr_effective_protected_variant, col = "variant, severe disease")) +
      geom_line(aes(y = hr_effective_variant + hr_effective_wildtype +
                      hr_effective_protected_wildtype + hr_effective_protected_variant,
                    col = "sum subgroups"), lty = 2) + scale_color_discrete("") +
      theme_bw() + xlab("Date") + ylab("Daily high risk") + theme(legend.position = "bottom")
    
    lr <- ggplot(plot_dt[location_id == loc_id & date < "2021-12-31"], aes(x = as.Date(date))) +
      geom_line(aes(y = lr_vaccinated, col = "vaccinated")) +
      geom_line(aes(y = lr_effective_variant, col = "variant, immune")) +
      geom_line(aes(y = lr_effective_wildtype, col = "wildtype, immune")) +
      geom_line(aes(y = lr_effective_protected_wildtype, col = "wildtype, severe disease")) +
      geom_line(aes(y = lr_effective_protected_variant, col = "variant, severe disease")) +
      geom_line(aes(y = lr_effective_variant + lr_effective_wildtype +
                      lr_effective_protected_wildtype + lr_effective_protected_variant,
                    col = "sum subgroups"), lty = 2) + scale_color_discrete("") +
      theme_bw() + xlab("Date") + ylab("Daily other adults") + theme(legend.position = "bottom")
    
    grid.arrange(hr, lr, nrow = 1, top = unique(plot_dt[location_id==loc_id]$location_name))
    
  }
  
  dev.off()