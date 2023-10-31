
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
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "utils.R"))

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

vaccine_output_root <- args$current_version
model_parameters <- yaml::read_yaml(file.path(vaccine_output_root, 'model_parameters.yaml'))
model_inputs_path <- model_parameters$model_inputs_path
hierarchy <- gbd_data$get_covid_modeling_hierarchy()

surv <- read.csv(file.path(vaccine_output_root, 'observed_survey_data_age.csv'))
mods <- read.csv(file.path(vaccine_output_root, 'smooth_survey_yes_age.csv'))

surv$date <- as.Date(surv$date)
mods$date <- as.Date(mods$date)

#-------------------------------------------------------------------------------
# Plot

message('Plotting region-age models')

pdf(file.path(vaccine_output_root, 'vaccine_hesitancy_region_age_combo.pdf'), onefile=TRUE, width=12, height=8)

regions <- unique(hierarchy$region_id)
regions <- regions[!is.na(regions)]

tmp_surv <- surv[surv$region_id %in% regions,]
tmp_surv <- tmp_surv[!is.na(tmp_surv$region_name),]
tmp_mods <- mods[mods$location_id %in% regions,]

p <- ggplot() +
  geom_point(data=tmp_surv, aes(x=date, y=survey_yes, size=sample_size), alpha=0.4) +
  geom_line(data=tmp_mods, aes(x=date, y=smooth_survey_yes, color= region_name), size=1.5) +
  geom_line(data=tmp_mods, aes(x=date, y=smooth_survey_yes_H95, color=region_name), size=0.5) +
  geom_line(data=tmp_mods, aes(x=date, y=smooth_survey_yes_L95, color=region_name), size=0.5) +
  ylab("Proportion unvaccinated willing to be vaccinated (source: FB survey)") +
  xlab('') +
  ylim(0, 1) + theme_minimal() +
  facet_grid(vars(age_group), 
             vars(region_name),
             labeller=labeller(facet_category=label_wrap_gen(width = 16, multi_line = TRUE))) +
  theme(legend.position = 'none',
        strip.text.x = element_text(angle=90, hjust=0),
        strip.text.y = element_text(angle=0),
        axis.text.x = element_text(angle=90)) +
  scale_x_date(date_labels = "%b %y")

print(p)
dev.off()


pdf(file.path(vaccine_output_root, 'vaccine_hesitancy_region_age.pdf'), onefile=TRUE, width=11, height=8)

for (i in c(1, regions)) {
  
  x <- surv[surv$region_id == i,]
  xr <- mods[mods$location_id == i,]
  
  x <- x[!is.na(x$age_group),]
  xr <- xr[!is.na(xr$age_group),]
  
  xr$location_name <- factor(xr$location_name, levels=unique(surv$region_name))
  
  p <- ggplot() +
    geom_point(data=x, aes(x=date, y=survey_yes, size=sample_size), alpha=0.4) +
    geom_line(data=xr, aes(x=date, y=smooth_survey_yes, color= location_name), size=1.5) +
    geom_line(data=xr, aes(x=date, y=smooth_survey_yes_H95, color=location_name), size=0.5) +
    geom_line(data=xr, aes(x=date, y=smooth_survey_yes_L95, color=location_name), size=0.5) +
    ylim(0, 1) + theme_minimal() +
    ggtitle(.get_column_val(x$region_name)) +
    scale_colour_discrete(drop=FALSE) +
    facet_wrap(~ age_group) +
    scale_size_continuous(name='Sample size', breaks=c(0, 100, 500, 1000)) +
    guides(color=FALSE)
  
  print(p)
}

dev.off()



#-------------------------------------------------------------------------------
message('Plotting location-age models')

location_order <- hierarchy[location_id %in% unique(mods$location_id)]
location_order <- location_order[order(sort_order)]$location_id

pdf(file.path(vaccine_output_root, 'vaccine_hesitancy_location_age.pdf'), onefile=TRUE, width=11, height=8)

for (i in location_order) {
  
  if (i == 102) {
    x <- surv[surv$parent_id == i,]
  } else {
    x <- surv[surv$location_id == i,]
  }
  
  xl <- mods[mods$location_id == i,]
  
  x$region_name <- factor(x$region_name, levels=unique(surv$region_name))
  xl$region_name <- factor(xl$region_name, levels=unique(surv$region_name))

  p <- 
    ggplot() +
    geom_point(data=x, aes(x=date, y=survey_yes, size=sample_size), alpha=0.4) +
    geom_line(data=xl, aes(x=date, y=smooth_survey_yes, color=region_name), size=1.5) +
    geom_line(data=xl, aes(x=date, y=smooth_survey_yes_H95, color=region_name), size=0.5) +
    geom_line(data=xl, aes(x=date, y=smooth_survey_yes_L95, color=region_name), size=0.5) +
    xlab("Proportion unvaccinated willing to be vaccinated (source: FB survey)") +
    ylab('Date') +
    ylim(0, 1) + theme_minimal() +
    ggtitle(paste0(.get_column_val(xl$location_name), ' (', i, ')')) + #,
            #subtitle = paste('Model fit at', unique(xl$fit_level), 'level')) +
    scale_colour_discrete(drop=FALSE) +
    facet_wrap(~ age_group) +
    scale_size_continuous(name='Sample size', breaks=c(0, 100, 500, 1000)) +
    guides(color=FALSE)
  
  print(p)
}

dev.off()



#-------------------------------------------------------------------------------
message('Plotting cleaned data')

pdf(file.path(vaccine_output_root, 'vaccine_hesitancy_FB_data.pdf'), onefile=TRUE, width=10, height=10)

for (i in unique(surv$location_id)) {
  
  tmp <- surv[surv$location_id == i,]
  
  g <- ggplot(data=tmp) +
    theme_minimal() +
    facet_wrap(~ age_group, nrow=1) +
    xlab("Date")
  
  grid.arrange(
    g + geom_point(aes(x=date, y=vaccinated), pch=1) + ylab('Vaccinated'),
    g + geom_point(aes(x=date, y=not_vaccinated), pch=1) + ylab('Not vaccinated'),
    g + geom_point(aes(x=date, y=unknown_vaccinated), pch=1) + ylab('Unknown vaccinated'),
    g + geom_point(aes(x=date, y=total_respondents_vaccination), pch=1) + ylab('Total respondents'),
    g + geom_point(aes(x=date, y=pct_vaccinated), pch=1) + ylab("Proportion vaccinated"),
    ncol=1,
    top=grid::textGrob(paste0(.get_column_val(tmp$location_name), ' (', i, ')'))
  )
  
}

dev.off()


