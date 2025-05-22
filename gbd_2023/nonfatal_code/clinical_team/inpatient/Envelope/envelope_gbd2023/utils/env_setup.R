# Title: envelope run setup
# Purpose: run the first part of the pipeline corresponding to run_id_{run_id} to produce a bundle version


# Defines file paths to source and save intermediate files and vetting tools
if (Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else {
  username <- Sys.getenv("USERNAME")
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}

# Specify directories and set working directory
home_dir <- "FILEPATH"
code_dir <- "FILEPATH"
setwd(home_dir)

# Install all libraries for the pipeline
library(pacman)
required_packages <- 
c(
  # Data processing and visualization
  "data.table",
  "dplyr",
  "tidyr",
  "readxl",
  "xlsx",
  "openxlsx",
  "magrittr",
  # Databases
  "RMySQL",
  "DBI",
  # Colorful output
  "crayon",
  # Data manipulation
  "Hmisc",
  # String operations
  "stringi", 
  "stringr", 
  "lubridate", # for date operations
  "rlang", # for tidy eval
  "ggplot2", # for plotting
  "ggrepel", # for text labels
  "ggthemes", # for themes
  "viridis", # for color scales
  'gridExtra',
  "styler"
)
pacman::p_load(names = required_packages, character.only = TRUE)


# Source shared functions - all .R files in the dir
shared_functions_dir <- "FILEPATH"

functs <- list.files(shared_functions_dir, 
                     # pattern = any file ending with .R
                     pattern = "\\.R$",
                     full.names = FALSE)

invisible(lapply(functs, function(x) source(paste0(shared_functions_dir, x))))
