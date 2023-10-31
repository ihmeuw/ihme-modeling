
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
# Set args

current_version <- args$current_version
compare_version <- args$compare_version

current_version_path <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, current_version)
compare_version_path <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, compare_version)



#-------------------------------------------------------------------------------
# Load data

model_parameters <- yaml::read_yaml(file.path(current_version_path, 'model_parameters.yaml'))
model_inputs_path <- model_parameters$model_inputs_path
hierarchy <- gbd_data$get_covid_covariate_prep_hierarchy()



#-------------------------------------------------------------------------------
# Plot

plot_name <- glue("some_plot_name_{compare_version}_{current_version}.pdf")

pdf(file.path(current_version_path, plot_name), onefile=TRUE)


# plotting code


dev.off()
