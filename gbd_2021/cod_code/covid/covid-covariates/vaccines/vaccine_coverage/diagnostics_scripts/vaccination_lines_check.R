
rm(list = ls())

library(argparse)
library(data.table)
library(ggplot2)
library(scales)
library(gridExtra)
library(RColorBrewer)
library(glue)

source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))

parser <- argparse::ArgumentParser(
  description = 'Launch a location-specific COVID results brief',
  allow_abbrev = FALSE
)

parser$add_argument('--current_version', help = 'Current covariate version for plotting')
parser$add_argument('--compare_version', help = 'Full path to vaccine covariate comparison version')
message(paste(c('COMMAND ARGS:', commandArgs(TRUE)), collapse = ' '))
args <- parser$parse_args(commandArgs(TRUE))


#-------------------------------------------------------------------------------
# Load data

hierarchy <- gbd_data$get_covid_modeling_hierarchy()
plot_dt <- vaccine_data$load_scenario_forecast(file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, args$current_version), 'slow')
plot_dt$date <- as.Date(plot_dt$date)

#plot_dt <- plot_dt[date <= as.Date('2022-05-01'),]

loc_order <- hierarchy[location_id %in% plot_dt$location_id]
location_plots <- loc_order[order(sort_order)]$location_id


#-------------------------------------------------------------------------------
# Plot

pdf(
  file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, 
            args$current_version,
            glue("vaccination_lines_check_{args$current_version}.pdf")), 
  height=4, 
  width=10, 
  onefile=TRUE
)

for (loc_id in location_plots) {

  tmp <- plot_dt[location_id == loc_id]
  
  tmp$cumulative_all_fully_vaccinated

   p <- ggplot(data = tmp, aes(x = date)) +
      geom_line(aes(y = cumulative_all_vaccinated, col = 'Initial'), size=2.5) + 
      geom_line(aes(y = cumulative_all_fully_vaccinated, col = 'Fully'), size=2.5) + 
      geom_line(aes(y = cumulative_all_effective, col = 'Effective'), size=2.5) + 
      xlab("") + ggtitle(tmp$location_name[1]) +
      scale_color_discrete("Vaccination") + 
      scale_y_continuous("Cumulative count", 
                         label = comma, sec.axis = sec_axis(~./tmp$adult_population[1], name="Percent of adult population", labels = percent)) +
      theme_minimal() + geom_vline(xintercept = Sys.Date(), lty = 2, alpha = 0.5)

   print(p)
}

dev.off()
