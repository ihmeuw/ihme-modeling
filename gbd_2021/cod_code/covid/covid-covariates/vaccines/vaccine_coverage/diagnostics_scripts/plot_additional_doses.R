

# Arguments. ###################################################################

# out_fp = "FILEPATH/additional_doses_plots.pdf"
out_fp = "FILEPATH/test_3crs_additional_doses_plots.pdf"
scenario_files = c(
  "FILEPATH/test_uncorrected_last_shots_in_arm_by_brand_w_booster_reference.csv",
  "FILEPATH/test_uncorrected_last_shots_in_arm_by_brand_w_booster_optimal.csv"
)


if (!interactive()) {
  parser <- argparse::ArgumentParser(
    description = 'Plot vaccine courses by location and scenario.',
    allow_abbrev = FALSE
  )
  
  parser$add_argument(
    '--out_fp',
    type = "character",
    help = 'Filepath to write to.'
  )
  parser$add_argument(
    '--scenario_files',
    type = "character",
    help = 'Comma-separated filepaths to the data to be plotted, one for each scenario.'
  )
  
  message(paste(c('COMMAND ARGS:',commandArgs(TRUE)), collapse = ' '))
  args <- parser$parse_args()
  
  for (arg in names(args)) {
    message(paste0(arg, ": ", get(arg), "\n"))
  }
  
  out_fp = args$out_fp
  scenario_files = unlist(strsplit(args$scenario_files, ","))
}


# Set up. ######################################################################

library(data.table)
library(ggplot2)

source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "utils.R"))


# Set data. ####################################################################

both_scenarios_dt = data.table()

scenario_list = list(
  "Reference_scenario" = fread(scenario_files[1]),
  "Optimal_scenario" = fread(scenario_files[2])
)

scenarios = names(scenario_list)

for (scen in scenarios) {
  scenario_list[[scen]][, Scenario := scen]
  
  both_scenarios_dt = rbind(
    both_scenarios_dt,
    scenario_list[[scen]]
  )
}

both_scenarios_dt = .strip_indices(df = both_scenarios_dt)

id_cols = c("location_id", "date", "vaccine_course", "risk_group", "Scenario")
setkeyv(x = both_scenarios_dt, cols = id_cols)

both_scenarios_dt = melt(
  data = both_scenarios_dt,
  id.vars = id_cols,
  measure_vars = brand_cols,
  variable.name = "Brand",
  value.name = "Daily Shots in Arms"
)

both_scenarios_dt[
  ,
  `:=`(
    vaccine_course = factor(vaccine_course, ordered = T),
    risk_group = factor(risk_group),
    Scenario = factor(Scenario),
    Brand = factor(Brand)
  )
]

setkeyv(x = both_scenarios_dt, cols = c(id_cols, "Brand"))


# Plot. ########################################################################

hierarchy = gbd_data$get_covid_modeling_hierarchy()


pdf(file = out_fp)

message("Plotting to: ", out_fp)

for (loc_id in unique(both_scenarios_dt$location_id)) {
  
  loc_name = unique(hierarchy[location_id == loc_id, location_name])
  
  for (scen in unique(both_scenarios_dt$Scenario)) {
    
    # message(loc_name, " (", loc_id, ") ", scen)
    
    print(
      ggplot(
        data = both_scenarios_dt[location_id == loc_id & Scenario == scen],
        mapping = aes(
          x = date,
          y = `Daily Shots in Arms`,
          color = vaccine_course
        )
      ) + 
        geom_line(alpha = 0.5) + 
        theme_classic() + 
        facet_grid(rows = vars(Brand), cols = vars(risk_group)) + 
        ggtitle(paste0(loc_name, " (", loc_id, ") ", scen))
    )
  }
}

dev.off()