#################################################################################
##                                                                             ##
## Description: Graphs diagnostics from MR-BRT crosswalking                    ##
##                                                                             ##
#################################################################################

Sys.umask(mode = "0002")

library(crosswalk002)
library(data.table)

args <- commandArgs(trailingOnly = TRUE)
settings_dir <- args[1]

if (interactive()) {
  version_estimate <- 999
  main_std_def <- "20_weeks"
  settings_dir <- "FILEPATH"
}

load(settings_dir)
list2env(new_settings, envir = environment())

if (model == "SBR") crosswalk_dir <- "FILEPATH"
if (model == "SBR/NMR") crosswalk_dir <- "FILEPATH"
if (model == "SBR + NMR") crosswalk_dir <- "FILEPATH"

# Read in required inputs from 01e_crosswalking.Rd

df_matched <- fread("FILEPATH")
df_orig3 <- fread("FILEPATH")

fit <- py_load_object(filename = "FILEPATH", pickle = "dill")

# Regenerate CWData

df <- crosswalk002::CWData(
  df = df_matched,            # dataset for metaregression
  obs = "log_diff",           # column name for the observation mean
  obs_se = "log_diff_se",     # column name for the observation standard error
  alt_dorms = "dorm_alt",     # column name of the variable indicating the alternative method
  ref_dorms = "dorm_ref",     # column name of the variable indicating the reference method
  covs = list("sev_gest_bw"), # names of columns to be used as covariates later
  study_id = "id"             # name of the column indicating group membership, usually the matching groups
)

############################
## Additional Diagnostics ##
############################

repl_python() # type 'exit' to get back to the R interpreter

plots <- import("crosswalk.plots")

# Funnel Plots

if (main_std_def == "28 weeks") {
  
  alt_definitions <- data.table(
    std_def_short = sort(unique(df_orig3$std_def_short)),
    letter = c("F", "A", "B", "G", "I", "C", "D", "MAIN", "H", "J", "E")
  )
  
} else {
  
  alt_definitions <- data.table(
    std_def_short = sort(unique(df_orig3$std_def_short)),
    letter = c("F", "MAIN", "A", "G", "I", "B", "C", "D", "H", "J", "E")
  )  
  
}

alt_definitions <- alt_definitions[std_def_short != main_std_def]

for (def in unique(alt_definitions$std_def_short)) {

  plots$funnel_plot(
    cwmodel = fit,
    cwdata = df,
    continuous_variables = list("sev_gest_bw"),
    obs_method = def,
    plot_note = "Funnel plot",
    plots_dir = crosswalk_dir,
    file_name = "FILEPATH",
    write_file = TRUE
  )

}

# Dose-Response Plots

for (def in unique(alt_definitions$std_def_short)) {

  plots$dose_response_curve(
    dose_variable = "sev_gest_bw",
    cwmodel = fit,
    cwdata = df,
    continuous_variables = list(),
    obs_method = def,
    plot_note = paste0(alt_definitions[std_def_short == def]$letter, ": Dose-Response Plot (", gsub("_", " ", def), ")"),
    plots_dir = crosswalk_dir,
    file_name = "FILEPATH",
    write_file = TRUE
  )
}
