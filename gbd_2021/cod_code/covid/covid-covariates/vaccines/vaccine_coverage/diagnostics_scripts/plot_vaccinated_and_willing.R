
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

parser <- argparse::ArgumentParser(description = 'Plot vaccinated and willing by age')
parser$add_argument('--current_version', help = 'Current covariate version for plotting')
parser$add_argument('--compare_version', help = 'Full path to vaccine covariate comparison version')
args <- parser$parse_args(commandArgs(TRUE))



#-------------------------------------------------------------------------------
# Set args

current_version <- args$current_version
compare_version <- args$compare_version
if (identical(current_version, compare_version)) compare_version <- NULL

current_version_path <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, current_version)
compare_version_path <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, compare_version)

all_age_group <- '0-125'



#-------------------------------------------------------------------------------
# Load data

model_parameters <- yaml::read_yaml(file.path(current_version_path, 'model_parameters.yaml'))
model_inputs_path <- model_parameters$model_inputs_path
hierarchy <- gbd_data$get_covid_modeling_hierarchy()

# Get observed survey data
survey_current <- fread(file.path(current_version_path, "observed_survey_data_age.csv"))
survey_current$date <- as.Date(survey_current$date)


# Get current and compare model output
mods_current <- fread(file.path(current_version_path, 'vaccinated_and_willing.csv'))

#if (!is.null(compare_version)) {
#  
#   tryCatch({
#    
#    mods_compare <- fread(file.path(compare_version_path, 'vaccinated_and_willing.csv'))
#    
#  }, error = function(e) {
#    
#    cat(conditionMessage(e), "\n")
#    cat("===> Grabbing 'time_series_vaccine_hesitancy.csv' instead")
#    mods_compare <- fread(file.path(compare_version_path, 'time_series_vaccine_hesitancy.csv'))
#    mods_compare$age_group <- all_age_group
#    
#  })
#  
#}

mods_compare <- fread(file.path(compare_version_path, 'time_series_vaccine_hesitancy.csv'))
mods_compare$age_group <- all_age_group


#-------------------------------------------------------------------------------
# Plot

pal <- RColorBrewer::brewer.pal(9, 'Set1')

plot_name <- glue("vaccinated_and_willing_age_{current_version}.pdf")
if (!is.null(compare_version)) plot_name <- glue("vaccinated_and_willing_age_{current_version}_{compare_version}.pdf")


#loc <- 523
#age <- '12-125'

pdf(file.path(current_version_path, plot_name), height=12, width=10, onefile=TRUE)

for (loc in hierarchy[level >= 3, location_id]) {
  
  plot_list <- list()
  
  for (age in unique(mods_current$age_group)) {
    
    tmp_mod_current <- mods_current[location_id == loc & age_group == age,]
    tmp_mod_compare <- mods_compare[location_id == loc & age_group == age,]
    
    sel_max_date <- which.max(tmp_mod_current$date[!is.na(tmp_mod_current$smooth_pct_vaccinated_adjusted_raked) & !is.na(tmp_mod_current$smooth_survey_yes)])
    if (length(sel_max_date) == 0) sel_max_date <- NA
    
    g <- ggplot(data=tmp_mod_current, aes(x=date)) + ylim(0,1) + theme_minimal() 
    
    g <- plot_grid(
      
      g + 
        geom_hline(aes(yintercept=smooth_pct_vaccinated_adjusted_raked[sel_max_date]), color=pal[1], lty=2) +
        geom_hline(aes(yintercept=smooth_combined_yes[sel_max_date]), color=pal[4], lty=2) +
        geom_point(aes(y=pct_vaccinated), size=3, alpha=0.4) +
        geom_line(aes(y=smooth_pct_vaccinated_adjusted_raked), color=pal[1], size=2) +
        ylab('Proportion vaccinated'),
      
      g + 
        geom_point(data=survey_current[location_id == loc & age_group == age,], 
                   aes(y=survey_yes, size=sample_size), alpha=0.4) +
        geom_line(aes(y=smooth_survey_yes), color='dodgerblue', size=2) +
        theme(legend.position = 'none') +
        ylab('Proportion unvaccinated willing'),
      
      g + 
        geom_hline(aes(yintercept=smooth_combined_yes[sel_max_date]), color=pal[4], lty=2) +
        geom_line(data=tmp_mod_compare, aes(y=smooth_combined_yes), color='green3', alpha=0.5, size=2) +
        geom_line(aes(y=smooth_combined_yes), color=pal[4], size=2) +
        ylab('Proportion vaccinated OR willing'),
      
      ncol=3
    )
    
    g <- plot_grid(grid::textGrob(age),
                   g,
                   ncol=1, 
                   rel_heights=c(0.05, 1))
    
    plot_list <- c(plot_list, list(g))
    
  }
  
  p <- plot_grid(ggdraw() + draw_label(glue("{hierarchy[location_id == loc, location_name]} ({loc})"), fontface='bold'), 
                 plot_grid(plotlist=plot_list, ncol=1), 
                 ncol=1, 
                 rel_heights=c(0.05, 1)) 
  
  print(p)
  
}


dev.off()
