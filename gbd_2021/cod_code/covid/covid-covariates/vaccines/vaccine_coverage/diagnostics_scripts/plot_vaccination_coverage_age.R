
library(argparse)
library(data.table)
library(ggplot2)
library(scales)
library(grid)
library(gridExtra)
library(RColorBrewer)
library(glue)
library(cowplot)

source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "utils.R"))
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "population_funcs.R"))

parser <- argparse::ArgumentParser(description = 'Plot vaccination coverage by age')
parser$add_argument('--current_version', help = 'Current covariate version for plotting')
parser$add_argument('--compare_version', help = 'Full path to vaccine covariate comparison version')
message(paste(c('COMMAND ARGS:', commandArgs(TRUE)), collapse = ' '))
args <- parser$parse_args(commandArgs(TRUE))



#-------------------------------------------------------------------------------
# Set args

current_version <- args$current_version
current_version_path <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, current_version)

model_parameters <- vaccine_data$load_model_parameters(current_version_path)

all_age_group <- '0-125'
age_starts <- model_parameters$age_starts
include_all_ages <- model_parameters$include_all_ages

#-------------------------------------------------------------------------------
# Load data

model_parameters <- yaml::read_yaml(file.path(current_version_path, 'model_parameters.yaml'))
model_inputs_path <- model_parameters$model_inputs_path
hierarchy <- gbd_data$get_covid_modeling_hierarchy()

population <- .get_age_group_populations(age_starts = age_starts,
                                         model_inputs_path = model_inputs_path,
                                         hierarchy = hierarchy,
                                         include_all_ages = include_all_ages)

population <- population[,!colnames(population) %in% c('age_start', 'age_end')]
population$age_group <- .factor_age_groups(population$age_group)

# Load modeled vaccination coverage by age (using FB survey data)
mods_age <- fread(file.path(current_version_path, "smooth_pct_vaccinated_age.csv"))
mods_age$date <- as.Date(mods_age$date)
mods_age <- merge(mods_age, population[,c('location_id', 'age_group', 'population')], by=c('location_id', 'age_group'))
mods_age$age_group <- .factor_age_groups(mods_age$age_group)

# Load modeled vaccination coverage from all-age vaccination data
mods_all_age <- fread(file.path(current_version_path, "smooth_pct_vaccinated.csv"))
mods_all_age$date <- as.Date(mods_all_age$date)
mods_all_age$age_group <- .factor_age_groups(mods_all_age$age_group)

# Load bias corrected model output
mods_bias_corrected_raked <- fread(file.path(current_version_path, 'smooth_pct_vaccinated_age_adjusted.csv'))
mods_bias_corrected_raked$age_group <- .factor_age_groups(mods_bias_corrected_raked$age_group)

# Get observed vaccination coverage by age from (CDC)
observed_age <- fread(file.path(current_version_path, "observed_vaccination_data_age.csv"))
observed_age$date <- as.Date(observed_age$date)
observed_age <- merge(observed_age, population, by=c('location_id', 'age_group'))
observed_age$pct_vaccinated <- observed_age$initially_vaccinated/observed_age$population
observed_age$age_group <- .factor_age_groups(observed_age$age_group)

# Get observed NON-age-stratified vaccination coverage data
observed_all_age <- vaccine_data$load_observed_vaccinations(current_version_path)
observed_all_age$date <- as.Date(observed_all_age$date)
observed_all_age$age_group <- .factor_age_groups(observed_all_age$age_group)

observed_all_age <- merge(observed_all_age[,.(location_id, date, people_vaccinated)], 
                          population[population$age_group == all_age_group,], 
                          by=c('location_id'))

observed_all_age$pct_vaccinated <- observed_all_age$people_vaccinated/observed_all_age$population


observed_bias <- fread(file.path(current_version_path, 'observed_survey_bias_age.csv'))
mods_bias <- fread(file.path(current_version_path, 'smooth_survey_bias_age.csv'))
observed_bias$age_group <- .factor_age_groups(observed_bias$age_group)
mods_bias$age_group <- .factor_age_groups(mods_bias$age_group)


# Get Facebook vaccination coverage data. Produced by smooth_vaccine_hesitancy_age()
fb <- fread(file.path(current_version_path, 'observed_survey_data_age.csv'))
fb$date <- as.Date(fb$date)
fb$age_group <- .factor_age_groups(fb$age_group)

fb$pct_vaccinated <- fb$vaccinated/fb$total_respondents_vaccination
fb$pct_vaccinated[fb$total_respondents_vaccination == 0] <- 0
fb$pct_vaccinated[is.nan(fb$pct_vaccinated)] <- NA
fb$pct_vaccinated <- .do_manual_bounds_proportion(fb$pct_vaccinated)






#-------------------------------------------------------------------------------
# Plot

#pal <- RColorBrewer::brewer.pal(9, 'Set1')

#i <- 33

plot_name <- glue("vaccination_coverage_age_{current_version}.pdf")

pdf(file.path(current_version_path, plot_name), height=13.5, width=11.5, onefile=TRUE)

for (i in unique(hierarchy[level >= 3, location_id])) {
  
  tryCatch( { 
    
    # Age-stratified models
    x_mod_age <- mods_age[mods_age$location_id == i,]
    
    # All age model from non-age stratified data
    x_mod_all <- mods_all_age[mods_all_age$location_id == i,]
    
    # Bias corrected models that are raked so that sum of age groups == all age
    x_mod_adj <- mods_bias_corrected_raked[mods_bias_corrected_raked$location_id == i,]
    
    # Time varying bias
    x_obs_bias <- observed_bias[observed_bias$location_id == i,]
    x_mod_bias <- mods_bias[mods_bias$location_id == i,]
    
    # Observed age-stratified data
    x_obs_age <- observed_age[observed_age$location_id == i,]
    
    # Observed all-age data
    x_obs_all <- observed_all_age[observed_all_age$location_id == i,]
    
    # Use all-age data when age-stratified not available (method 2)
    #if (unique(x_mod_adj$method) == 2) x_obs_age <- observed_all_age[observed_all_age$location_id == i,]
    
    # Raw FB survey data
    x_obs_fb <- fb[fb$location_id == i,]
    
    #x_prop <- prop_total[location_id == i,]
    
    method <- unique(x_mod_adj$method)
    if (length(method) == 0) method <- 3
    
    if (nrow(x_obs_age) == 0) {
      
      x_obs_age <- data.frame(location_id=i,
                              date=x_mod_age$date,
                              age_group=x_mod_age$age_group,
                              pct_vaccinated=as.numeric(NA))
    }
    
    #date_range <- range(c(x_obs_age$date, x_obs_fb$date, x_mod_age$date, x_mod_adj$date))
    date_range <- range(as.Date('2020-12-16'), Sys.Date())
    
    #if (!(i %in% c(25, 298, 351)) & all(nrow(x_mod_age) > 0)) {
    
    # Default plot settings for proportions
    g <- ggplot(data=x_mod_age) +
      geom_hline(yintercept=0, size=0.25) + 
      geom_hline(yintercept=1, size=0.25) +
      ylim(0, 1.2) +
      scale_x_date(labels = date_format("%b"), limits = date_range) +
      theme_minimal() +
      facet_wrap(~ age_group, nrow=1) +
      xlab("Date") + ylab(glue("Proportion vaccinated"))
    
    
    plot_title <- paste0(hierarchy[location_id == i, location_name], ' (', i, ')', ' | Method ', method)
    message(plot_title)
    
    
    if (method == 1) {
      
      adjusted_mod_color <- 'red'
      
      glist <- list(
        
      g +
        geom_point(data=x_obs_age, aes(x=date, y=pct_vaccinated), pch=1, size=2.5) +
        facet_wrap(~ age_group, nrow=1) +
        ggtitle('Reported proportion of age group with at least one dose (e.g. CDC, OWiD)'),
      
      ggplot() +
        geom_point(data=x_obs_all, aes(x=date, y=people_vaccinated), pch=1, size=2.5, color='darkcyan') +
        geom_point(data=x_obs_age, aes(x=date, y=initially_vaccinated), pch=1, size=2.5) +
        scale_x_date(labels = date_format("%b")) +
        facet_wrap(~ age_group, nrow=1) +
        xlab("Date") + ylab("Total vaccinated") +
        theme_minimal() + theme(legend.position = 'none') +
        ggtitle('Total counts compared to non-age-stratified data'),
      
      ggplot() +
        geom_point(data=x_mod_adj, aes(x=date, y=ratio_to_all_age), size=2.5, pch=4, col='cyan3') +
        geom_point(data=x_mod_adj[!x_mod_adj$imputed], aes(x=date, y=ratio_to_all_age), pch=1, size=2.5) +
        scale_x_date(labels = date_format("%b"), limit=date_range) + 
        xlab("Date") + ylab("Ratio") +
        facet_wrap(~ age_group, nrow=1) +
        theme_minimal() + theme(legend.position = 'none') +
        ggtitle('Ratio of each age group to all-age data'),
      
      g +
        geom_point(data=x_obs_all, aes(x=date, y=pct_vaccinated), pch=1, size=2.5) +
        geom_point(data=x_mod_adj, aes(x=date, y=pct_vaccinated_adjusted), pch=4, size=2.5, color='cyan3') +
        geom_point(data=x_mod_adj, aes(x=date, y=pct_vaccinated), pch=1, size=2.5) +
        geom_line(data=x_mod_adj, aes(x=date, y=smooth_pct_vaccinated_adjusted), size=1.5, color=adjusted_mod_color) +
        theme(legend.position = 'none') +
        ggtitle('Monotonic spline with data imputed using ratio to all-age')
      
      )

      
    }
    
    
    if (method %in% c(2,3)) {
      
      adjusted_mod_color <- 'purple'
      
      glist <- list(
        
      g +
        geom_point(data=x_obs_all, aes(x=date, y=pct_vaccinated), pch=1, size=2.5) +
        facet_wrap(~ age_group, nrow=1) +
        ggtitle('Reported vaccination coverage (e.g. CDC, OWiD)') +
        geom_line(data=x_mod_all, aes(x=date, y=smooth_pct_vaccinated), color='green4', size=1.5),
      
      g +
        geom_point(data=x_obs_fb, aes(x=date, y=pct_vaccinated, size=total_respondents_vaccination), alpha=0.4) +
        geom_line(data=x_mod_age, aes(x=date, y=smooth_pct_vaccinated), color='goldenrod', size=1.5) +
        scale_size_continuous(name='Sample size', breaks=c(0, 100, 500, 1000, 5000)) +
        theme(legend.position = 'none') +
        ggtitle('Vaccination coverage estimated used FB survey responses'),
      
      ggplot() +
        geom_point(data=x_obs_bias, 
                   aes(x=date, y=bias), shape=1, alpha=0.3) +
        geom_hline(yintercept=0) +
        geom_line(data=x_mod_bias[x_mod_bias$location_id == i,], 
                  aes(x=date, y=smooth_bias), color='dodgerblue', size=1.75, alpha=0.7) +
        geom_line(data=x_mod_adj, aes(x=date, y=smooth_bias), size=1, color='blue3') +
        ylim(-1,1) +
        xlab("Date") + ylab('Bias') +
        scale_x_date(labels = date_format("%b")) +
        facet_wrap(~ age_group, nrow=1) +
        theme_minimal() +
        ggtitle(paste0('Estimated bias (survey estimate vs observed) | Fit level: ', x_mod_bias$fit_level[1])),

      g +
        geom_point(data=x_obs_age, aes(x=date, y=pct_vaccinated), pch=1, alpha=0.4, size=2.5) +
        geom_point(data=x_mod_adj, aes(x=date, y=pct_vaccinated), pch=1, alpha=0.4, size=2.5) +
        geom_line(data=x_mod_age, aes(x=date, y=smooth_pct_vaccinated), color='goldenrod', size=1.5, alpha=0.5) +
        geom_line(data=x_mod_adj, aes(x=date, y=smooth_pct_vaccinated_adjusted, color='Uptake (bias corrected)'), size=1.5) +
        geom_line(data=x_mod_all, aes(x=date, y=smooth_pct_vaccinated), color='green4', size=1.5) +
        scale_color_manual(name='Model', values=c('Uptake (FB)' = 'goldenrod',
                                                  'Uptake (bias corrected)' = adjusted_mod_color)) +
        theme(legend.position = 'none') +
        ggtitle('Bias-adjusted coverage based on reported all-age (green) and FB survey (purple) data')
      
      )
      
    }
    
    
    # Last two rows are common to all methods
    glist <- c(glist,
      list(
      ggplot() +
        geom_line(data=x_mod_adj, aes(x=date, y=smooth_raw_vaccinated_adjusted_raked), size=1.5) +
        geom_hline(data=x_mod_adj, aes(yintercept=population), linetype=2, size=0.4, col='grey40') +
        scale_x_date(labels = date_format("%b"), limit=date_range) +
        xlab("Date") + ylab("Total vaccinated") +
        facet_wrap(~ age_group, nrow=1) +
        theme_minimal() + theme(legend.position = 'none') +
        ggtitle('Smoothed counts raked to all-age'),
      
      ggplot() +
        geom_line(data=x_mod_adj, aes(x=date, y=daily_rate), size=2) +
        scale_x_date(labels = date_format("%b"), limit=date_range) +
        xlab("Date") + ylab("Daily rate") +
        facet_wrap(~ age_group, nrow=1) +
        theme_minimal() + theme(legend.position = 'none') +
        ggtitle('Daily rate of vaccination')
      )
    )
    
    grid.arrange(grobs=glist,
                 nrow=length(glist),
                 ncol=1,
                 top=grid::textGrob(plot_title))
    
  }, error = function(e) {
    
    message(paste('ERROR:', i, conditionMessage(e)), '\n')
    grid.arrange(grid::textGrob(plot_title))
    
  })
}

dev.off()




