
rm(list = ls(all.names = TRUE))

library(argparse)
library(data.table)
library(ggplot2)
library(scales)
library(gridExtra)
library(RColorBrewer)
library(glue)

source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "utils.R"))

#-------------------------------------------------------------------------------
# Get arguments

parser <- argparse::ArgumentParser(
  description = 'Launch a location-specific COVID results brief',
  allow_abbrev = FALSE
)

parser$add_argument('--current_version', help = 'Full path to vaccine covariate version')
parser$add_argument('--compare_version', help = 'Full path to vaccine covariate comparison version')
message(paste(c('COMMAND ARGS:',commandArgs(TRUE)), collapse = ' '))
args <- parser$parse_args(commandArgs(TRUE))


current_version <- args$current_version
compare_version <- args$compare_version

current_path <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, current_version)
compare_path <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, compare_version)

model_parameters <- yaml::read_yaml(file.path(current_path, 'model_parameters.yaml'))
#end_date <- as.Date("2021-12-31")
end_date <- model_parameters$end_date

#-------------------------------------------------------------------------------
# Load data

hierarchy <- gbd_data$get_covid_modeling_hierarchy()

observed <- vaccine_data$load_observed_vaccinations(current_path)
observed[, date := as.Date(date)]
observed[, cumulative_first := cumsum(daily_first_vaccinated), by = "location_id"]

slow_scale_up <- vaccine_data$load_scenario_forecast(current_path, 'slow')
slow_compare <- vaccine_data$load_scenario_forecast(compare_path, 'slow')
slow_scale_up[, version := paste0("New: ", current_version)]
slow_compare[, version := paste0("Baseline: ", compare_version)]
slow_scale_up[, name := "Reference"]
slow_compare[, name := "Reference"]

loc_order <- hierarchy[location_id %in% unique(slow_scale_up$location_id)]
location_plots <- loc_order[order(sort_order)]$location_id

# Stupid patch to lose nonsense columns
dup <- colnames(slow_scale_up)[duplicated(colnames(slow_scale_up))]
slow_scale_up <- slow_scale_up[,!which(colnames(slow_scale_up) %in% dup), with=F]

dup <- colnames(slow_compare)[duplicated(colnames(slow_compare))]
slow_compare <- slow_compare[,!which(colnames(slow_compare) %in% dup), with=F]



plot_dt <- rbind(slow_scale_up, slow_compare, fill = T)
plot_dt[scale_up_delivery < 0, scale_up_delivery := 0]




#-------------------------------------------------------------------------------
# Plot

pdf(file.path(current_path, glue("scale_up_doses_{current_version}_{compare_version}.pdf")))

for (loc_id in location_plots) {
  
  adult_pop <- unique(plot_dt$adult_population[plot_dt$location_id == loc_id])[1]
  
  plot_dt[, prop_vaccinated := cumulative_all_vaccinated / adult_population]
  plot_dt[, scale1 := cumulative_all_vaccinated / prop_vaccinated]
  #scale1 <- plot_dt[location_id == loc_id & name == "Reference", mean(scale1, na.rm = TRUE)]
  
  if (max(plot_dt[location_id == loc_id & name == "Reference"]$cumulative_all_vaccinated) == 0) scale1 <- 100
  
  a <- ggplot() +
    geom_line(data = slow_scale_up[location_id == loc_id & date < end_date], 
              aes(x = date, y = supply_and_delivery_doses), col = "black", size=1) +
    geom_line(data = plot_dt[location_id == loc_id & date < end_date], 
              aes(x = date, y = scale_up_delivery, col = version), size=1.25) +
    xlab("") + scale_y_continuous("Delivery capacity per day", label = comma) +
    scale_color_discrete("Scenario") + 
    theme_classic() + geom_vline(xintercept = Sys.Date(), lty = 2, alpha = 0.5)
  
  if(nrow(observed[location_id == loc_id]) > 0){
    
    b <- ggplot(data = plot_dt[location_id == loc_id & date < end_date], aes(x = date)) +
      geom_line(aes(y = cumulative_all_vaccinated, col = version, linetype = '1'), size=1.25) + 
      geom_line(aes(y = cumulative_all_fully_vaccinated, col = version, linetype = '2'), size=1.25) + 
      geom_line(aes(y = cumulative_all_effective, col = version, linetype = '3'), size=1.25) + 
      geom_hline(yintercept = adult_pop) +
      xlab("") + 
      geom_point(data = observed[location_id == loc_id], aes(x = date, y = reported_vaccinations, shape = "1"), size = 2.5) +
      geom_point(data = observed[location_id == loc_id], aes(x = date, y = fully_vaccinated, shape = "2"), size = 2.5) +
      geom_point(data = observed[location_id == loc_id], aes(x = date, y = people_vaccinated, shape = "3"), size = 3.25) +
      scale_shape_manual(values = c(3,2,1), 
                         labels = c("Total vaccinations",
                                    "Fully vaccinated",
                                    "Any vaccinated")) +
      scale_linetype_manual(values=c(1,2,3),
                            labels=c("Initiated vaccination",
                                     "Fully vaccinated",
                                     "Effectively vaccinated")) +
      scale_y_continuous("Cumulative people vaccinated", 
                         label = comma,sec.axis = sec_axis(~./adult_pop, name="Percent of total population", labels = percent)) +
      scale_color_discrete("Scenario") + 
      theme_classic() + geom_vline(xintercept = Sys.Date(), lty = 2, alpha = 0.5)
    
  } else {
    
    b <- ggplot(data = plot_dt[location_id == loc_id & date < end_date], 
                aes(x = date, y = cumulative_all_vaccinated)) +
      geom_line(aes(col = version)) + 
      xlab("") + 
      scale_y_continuous("Cumulative people vaccinated", label = comma,sec.axis = sec_axis(~./scale1, name="Percent of adult population", labels = percent)) +
      scale_color_discrete("Scenario") + 
      theme_classic() + geom_vline(xintercept = Sys.Date(), lty = 2, alpha = 0.5)
    
  }
  
  grid.arrange(a, b, top = unique(plot_dt[location_id == loc_id, location_name]))
  
}

dev.off()
