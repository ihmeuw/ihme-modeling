#### Generate MLT envelope plots ####


library(mortdb, lib.loc = "FILEPATH")
library(mortcore, lib.loc = "FILEPATH")
library(data.table)
library(ggplot2)
library(argparse)

parser <- ArgumentParser()
parser$add_argument('--mlt_version', type='integer', required=TRUE,
                    help='The MLT life table estimate version of this run')
parser$add_argument('--mlt_env_version', type='integer', required=TRUE,
                    help='The MLT envelope estimate version of this run')
parser$add_argument('--vr_version', type='integer', required=TRUE,
                    help='The empirical deaths version to compare to')
parser$add_argument('--gbd_year', type='integer', required=TRUE,
                    help = "GBD year of analysis")


args <- parser$parse_args()
mlt_version <- args$mlt_version
mlt_env_version <- args$mlt_env_version
vr_version <- args$vr_version
gbd_year <- args$gbd_year


# Pull Envelope -----------------------------------------------------------

env_file <- paste0("FILEPATH", mlt_version,
                   "FILEPATH", mlt_env_version, '.csv')

env <- fread(env_file)

# Drop unneeded columns
env[, c('estimate_stage_id', 'lower', 'upper') := NULL]

# Pull previous year's best envelope
prev_gbd_year <- get_gbd_year(get_gbd_round(gbd_year) - 1)
prev_year_env <- mortdb::get_mort_outputs(model_name = 'mlt death number', model_type = 'estimate',
                                          run_id = 'best', gbd_year = prev_gbd_year)
prev_year_env <- prev_year_env[, .(location_id, year_id, sex_id, age_group_id, prev_env = mean)]

# Pull VR -----------------------------------------------------------------

vr <- mortdb::get_mort_outputs(model_name = "death number empirical", model_type = "data",
                               run_id = vr_version)
vr <- vr[outlier==0, .(location_id, sex_id, age_group_id, year_id, source_name, detailed_source, vr=mean)]


# Merge and add age/location map -------------------------------------------

all_data <- merge(env, vr, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'), all.x=T) # Merge on VR and see what sticks. Some locations/ages won't have VR, that's OK
all_data <- merge(all_data, prev_year_env, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'), all.x=T) # New years/age groups/locations won't have values for the previous year, also OK

ages <- mortdb::get_age_map(gbd_year = gbd_year, type = 'gbd') # Subset to standard ages only. Drops in below merge expected
locations <- mortdb::get_locations(gbd_year = gbd_year, level = 'all')

all_data <- merge(all_data, ages[, .(age_group_id, age_group_name, age_group_years_start, age_group_years_end)], by='age_group_id')
all_data <- merge(all_data, locations[, .(location_id, ihme_loc_id, location_name)], by='location_id')

# Write out the envelope file
fwrite(all_data, "FILEPATH", nThread = 1) # Hardcode single threading to avoid corruptions from race conditions

# Create Plots ------------------------------------------------------------

# Make graphing directory
graphs_dir <- paste0("FILEPATH", mlt_version, '/diagnostics/location_specific')
if (!(dir.exists(graphs_dir))) dir.create(graphs_dir)

# Create color scale
colors <- c("blue", "red", "purple")
names(colors) <- c("Current Envelope",
                   paste0("GBD", prev_gbd_year, " Envelope"),
                   paste0("VR v", vr_version))

# Create location specific plots

options(warn = -1) # Turn off warnings lest we get spammed with tons of "Removed 3 rows containing missing values" messages, which are OK
for (iso3 in unique(all_data$ihme_loc_id)) {
  print(iso3)
  pdf(paste0(graphs_dir, '/', iso3, '.pdf'), width=15, height = 15)

  # 1 plot per sex
  for (sex in 1:3) {

    # Subset data
    temp <- all_data[ihme_loc_id==iso3 & sex_id==sex]

    sex_name <- ifelse(sex==1, 'Male', ifelse(sex==2, 'Female', 'Both Sex'))

    p <- ggplot(data=temp, aes(x=year_id)) +
      theme_bw() +
      geom_line(aes(y=mean, colour='Current Envelope')) + # line graph series of our MLT envelope
      geom_line(aes(y=prev_env, colour=paste0("GBD", prev_gbd_year, " Envelope"))) + # line graph series of previous year's best envelope
      geom_point(aes(y=vr, colour=paste0("VR v", vr_version))) + # scatter VR data points
      facet_wrap(facets =  ~ factor(reorder(age_group_name, age_group_years_start)), nrow=5, ncol = 5, scales = "free") + # Create grid of ages
      xlab("Year") +
      ylab("Deaths") +
      ggtitle(paste0("No shock with HIV deaths ", iso3, " ", temp[1, location_name], ", ", sex_name)) +
      scale_colour_manual(name = "Type", values = colors) +
      theme(legend.position = "right")
    plot(p)

  }

  dev.off()
}

# Run above code again to create main plots. Annoying. But since we're running outside of the main pipeline, time isn't quite as important.
pdf(paste0(graphs_dir, '/../MLT_Envelope_plots.pdf'), width=15, height=15)

for (iso3 in unique(all_data$ihme_loc_id)) {

  # 1 plot per sex
  for (sex in 1:3) {

    # Subset data
    temp <- all_data[ihme_loc_id==iso3 & sex_id==sex]

    sex_name <- ifelse(sex==1, 'Male', ifelse(sex==2, 'Female', 'Both Sex'))

    p <- ggplot(data=temp, aes(x=year_id)) +
      theme_bw() +
      geom_line(aes(y=mean, colour='Current Envelope')) + # line graph series of our MLT envelope
      geom_line(aes(y=prev_env, colour=paste0("GBD", prev_gbd_year, " Envelope"))) + # line graph series of previous year's best envelope
      geom_point(aes(y=vr, colour=paste0("VR v", vr_version))) + # scatter VR data points
      facet_wrap(facets =  ~ factor(reorder(age_group_name, age_group_years_start)), nrow=5, ncol = 5, scales = "free") + # Create grid of ages
      xlab("Year") +
      ylab("Deaths") +
      ggtitle(paste0("No shock with HIV deaths ", iso3, " ", temp[1, location_name], ", ", sex_name)) +
      scale_colour_manual(name = "Type", values = colors) +
      theme(legend.position = "right")
    plot(p)
  }

}

mortdb::send_slack_message(paste0("MLT Envelope Graphs Ready, at ", graphs_dir),
                          channel = "#mortality", icon = ":man-playing-water-polo:", botname = "MLTBot")