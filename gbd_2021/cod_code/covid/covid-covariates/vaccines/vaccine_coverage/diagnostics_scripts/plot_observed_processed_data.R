
# Parser ----

if (!interactive()) {
  parser <- argparse::ArgumentParser(
    description = 'Plot processed vaccine data.',
    allow_abbrev = FALSE
  )
  
  parser$add_argument(
    '--output_path',
    type = "character",
    help = 'Full path to vaccine covariate version'
  )
  
  parser$add_argument(
    '--previous_best_path',
    type = "character",
    help = 'Full path to last production vaccine covariate version'
  )
  
  message(paste(c('COMMAND ARGS:',commandArgs(TRUE)), collapse = ' '))
  args <- parser$parse_args(commandArgs(TRUE))
  
  for (key in names(args)) {
    assign(key, args[[key]])
  }
  
  for (arg in names(args)) {
    message(paste0(arg, ": ", get(arg), "\n"))
  }
  
}

# Setup ----

# SCOPING ----
# last prod with raw SA data - 2022_06_06.01
# first prod w/o raw SA data - 2022_07_14.02
# 
# output_path <- .output_path
# previous_best_path <- .previous_best_path
# 
# output_path <- "FILEPATH"
# previous_best_path <- "FILEPATH"

library(data.table)
library(dplyr)
library(ggplot2)
source(file.path("FILEPATH", Sys.info()["user"], "FILEPATH/paths.R"))
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))

# Paths ----
write_out_path <- file.path(output_path, 'observed_data_processed_plots.pdf')

# Data & hierarchy ----
observed_data <- fread(file.path(output_path, "observed_data.csv"))
previous_data <- fread(file.path(previous_best_path, "observed_data.csv"))
hierarchy <- gbd_data$get_covid_modeling_hierarchy()

# version ID column for plotting
observed_version <- strsplit(output_path, split = "/")[[1]][length(strsplit(output_path, split = "/")[[1]])]
previous_version <- strsplit(previous_best_path, split = "/")[[1]][length(strsplit(output_path, split = "/")[[1]])]
observed_data[, version_id := paste0("Current ", "(", observed_version, ")")]
previous_data[, version_id := paste0("Previous best ", "(", previous_version, ")")]

# Merge and clean
all_data <- rbind(previous_data, observed_data, fill = T)
all_data <- merge(all_data, hierarchy[,.(location_id, location_name)], by = "location_id", all.x = T)
all_data[, location_name.x := NULL]
setnames(all_data, "location_name.y", "location_name")
all_data[, date := as.Date(date)]

# Plot ----

Locs <- unique(all_data$location_id)

pdf(file = file.path(write_out_path), width = 12, onefile = T)

for (i in 1:length(Locs)) {
  
  # SCOPING ----
  # i <- which(Locs == 571) # West Virginia
  
  one_loc <- all_data[location_id == Locs[i]]
  # ensure clean filenames
  x <- one_loc$data_filename
  one_loc$data_filename <- paste(unique(as.vector(x)[!is.na(as.vector(x))]), collapse = " | ")
  
  p <-
    one_loc %>% as_tibble() %>% 
    dplyr::rename(c("1_reported_vaccinations" = "reported_vaccinations",
                    "2_people_vaccinated" =     "people_vaccinated",
                    "3_fully_vaccinated" =      "fully_vaccinated",
                    "4_boosters_administered" = "boosters_administered",
                    "5_booster_1" =             "booster_1",
                    "6_booster_2" =             "booster_2")) %>% 
    tidyr::pivot_longer(cols = c("1_reported_vaccinations",
                                 "2_people_vaccinated",
                                 "3_fully_vaccinated",
                                 "4_boosters_administered",
                                 "5_booster_1",
                                 "6_booster_2")) %>% 
    # filter(!(name == "6_booster_2" & value == 0)) # if you need to see only non-zero booster_2
      ggplot(aes(x = date, y = value, 
                 color = name, 
                 alpha = version_id,
                 size = version_id
                 )) +
      scale_alpha_manual("Version ID", values = c(1,0.15))+
      scale_size_manual("Version ID", values = c(0,2))+
      scale_color_brewer("Vaccine Course", palette = "Dark2") +
    scale_x_date(limits = c(min(one_loc$date), Sys.Date()), 
                 date_breaks = "2 months",
                 date_minor_breaks = "1 month",
                 date_labels = "%b '%y") +
    geom_point() + 
    ggtitle(paste(unique(one_loc$location_name), ":", unique(one_loc$location_id)),
            paste(unique(observed_data$version_id), "vs.", 
                  unique(previous_data$version_id))) +
    labs(caption = unique(one_loc$data_filename)) +
    theme_minimal() +
    facet_wrap(~name, nrow = 2) +
    theme(
      legend.position = 0,
      axis.text.x = element_text(angle = 60, vjust = 1, hjust = 1)
    )
  
  if (nrow(observed_data[location_id == Locs[i]]) == 0) {
    
    p <- 
      p + 
      annotate("text", x=as.Date(mean(one_loc$date, na.rm = T)), 
               y=mean(one_loc$reported_vaccinations, na.rm=T),
               label="Missing current data",
               color="red")
    
  }
    
  print(p)
  
}

dev.off()

