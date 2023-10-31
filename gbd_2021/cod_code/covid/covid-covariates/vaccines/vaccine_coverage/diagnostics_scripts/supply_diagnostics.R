################################################################
## Diagnostic plots for vaccine dose supply estimates
################################################################

library(data.table)
library(ggplot2)
library(gridExtra)
library(scales)
library(boot)
library(zoo)
library(cowplot)
library(ggpubr)
library(glue)

source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))

source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))
source(CODE_PATHS$MAPPING_FUNCTION_PATH)



## Setup and background
# Set up command line argument parser
parser <- argparse::ArgumentParser(
  description = 'Diagnostics for vaccine supply',
  allow_abbrev = FALSE
)

parser$add_argument('--current_version', help = 'Full path to vaccine covariate version')
parser$add_argument('--compare_version', help = 'Full path to ccompare version (if comparing, can be NULL)')

message(paste(c('COMMAND ARGS:',commandArgs(TRUE)), collapse = ' '))
args <- parser$parse_args(commandArgs(TRUE))


#-------------------------------------------------------------------------------
# Set args

vaccine_output_root <- args$current_version

model_parameters <- yaml::read_yaml(file.path(vaccine_output_root, 'model_parameters.yaml'))
model_inputs_path <- model_parameters$model_inputs_path
gavi_scenario <- model_parameters$gavi_dose_scenario

## Grab final_dt and purchase_candidates
hierarchy <- gbd_data$get_covid_modeling_hierarchy()
final_dt <- vaccine_data$load_final_doses(vaccine_output_root)
purchase_candidates <- vaccine_data$load_purchase_candidates(vaccine_output_root)
#purchase_candidates <- merge(purchase_candidates[,location_name := NULL], hierarchy[,c('location_id', 'location_name')], by='location_id', all=TRUE)

levels_manufacturer <- levels(as.factor(purchase_candidates$manufacturer))


#-------------------------------------------------------------------------------
# Define funcs

# Function to make large number of distinct colors for manufacturers
get_pal <- function(x) colorRampPalette(brewer.pal(9, "Set1"))(x)

# Function to plot bar plot for a subset of purchase candidates
plot_bar_func <- function(dt, plot_title=NULL, leg=FALSE) {
  
  if (!('source' %in% colnames(dt))) dt$source <- 'All'
  dt$manufacturer <- factor(dt$manufacturer, levels=levels_manufacturer)
  
  p <- ggplot(dt, aes(x=factor(effective_date), y=sum_location_secured_daily_doses, fill = manufacturer)) + 
    geom_bar(stat="identity", col = "black") + 
    facet_grid(cols=vars(source), drop=F) +
    xlab("Effective start date doses available") + 
    scale_y_continuous("Doses", labels = comma) + 
    scale_fill_manual(name="Manufacturer",
                      values=get_pal(length(levels_manufacturer)),
                      drop=FALSE) +
    ggtitle(plot_title, 
            subtitle = paste0(format(round(sum(dt$sum_location_secured_daily_doses, na.rm = T), 0), big.mark = ","), " total probable secured doses")) + 
    theme_minimal() + 
    theme(axis.text.x = element_text(angle = 90, hjust = 1),
          legend.position=ifelse(leg, 'right', 'none'))
  
  
  return(p)
}

get_cumulative_doses <- function(dt) {
  
  tryCatch( {
    
    dt <- as.data.table(aggregate(sum_location_secured_daily_doses ~ effective_date, data=dt, FUN=function(x) sum(x, na.rm=TRUE)))
    dt$effective_date <- as.Date(dt$effective_date)
    dt$sum_location_secured_daily_doses <- cumsum(dt$sum_location_secured_daily_doses)
    dt <- merge(dt, data.table(effective_date=as.Date(seq(min(tmp$effective_date), max(tmp$effective_date), by=1))), all.y=TRUE)
    dt$sum_location_secured_daily_doses <- zoo::na.locf(dt$sum_location_secured_daily_doses, na.rm=F)
    return(dt)
    
  }, error=function(e){
    
    dt <- data.table(effective_date=as.Date(seq(min(tmp$effective_date), max(tmp$effective_date), by=1)),
                     sum_location_secured_daily_doses=0)
    return(dt)
  })
}

p <- plot_bar_func(purchase_candidates, leg=T)
leg1 <- get_legend(p)


#-------------------------------------------------------------------------------
# Plot for COVAX locs and ALL locs

for (covax_only in c(FALSE, TRUE)) {

if (covax_only) {
  covax_locs <- model_inputs_data$load_covax_locations(model_inputs_path)
  final_dt <- final_dt[location_id %in% covax_locs$location_id,]
  purchase_candidates <- purchase_candidates[location_id %in% covax_locs$location_id,]
}
  
file_name <- ifelse(covax_only, 
                    glue("supply_diagnostics_gavi_{gavi_scenario}_covax_locs.pdf"), 
                    glue("supply_diagnostics_gavi_{gavi_scenario}.pdf"))

pdf(file.path(vaccine_output_root, file_name), height = 10, width = 11, onefile = TRUE)

for(loc in unique(purchase_candidates$location_id)){
  
  tmp <- purchase_candidates[location_id == loc & !is.na(date),]
  #tmp[is.na(sum_location_secured_daily_doses), sum_location_secured_daily_doses := location_secured_daily_doses * effective_days]
  
  sel_gavi <- which(tmp$location == 'GAVI')
  sel_other_covax <- which(tmp$location %in% c('COVAX', 'COVAX AMC'))
  
  sources <- c(glue('GAVI scenario: {gavi_scenario}'),
               'Other COVAX',
               'Purchased and other donations')
  
  tmp[sel_gavi, source := sources[1]]
  tmp[sel_other_covax, source := sources[2]]
  tmp[-c(sel_gavi, sel_other_covax), source := sources[3]]
  
  tmp$source <- factor(tmp$source, levels=sources)
  tmp$manufacturer <- factor(tmp$manufacturer, levels=levels_manufacturer)
  
  plot_title <- paste0(hierarchy[location_id == loc, location_name], ' (', loc, ')')
  p1 <- plot_bar_func(tmp, plot_title)

  tmp_gavi <- get_cumulative_doses(tmp[source == sources[1],])
  tmp_other_covax <- get_cumulative_doses(tmp[source == sources[2],])
  tmp_other <- get_cumulative_doses(tmp[source == sources[3],])
  tmp_tot <- get_cumulative_doses(tmp)
  
  tmp_gavi[, source := sources[1]]
  tmp_other_covax[, source := sources[2]]
  tmp_other[, source := sources[3]]
  tmp_tot[, source := 'Total']

  tmp_cumulative <- rbind(
    tmp_gavi,
    tmp_other_covax,
    tmp_other,
    tmp_tot 
  )
  
  setnames(tmp_cumulative, 'sum_location_secured_daily_doses', 'cumulative_doses')
  
  p2 <- ggplot(tmp_cumulative, aes(x=effective_date, y=cumulative_doses, color=source)) +
    geom_line(size=2) +
    xlab("Effective start date doses available") + 
    scale_y_continuous("Total doses received", labels = comma) + 
    scale_color_manual(name="Source", values=c("darkred", "#E69F00", "#56B4E9", 'black')) +
    ggtitle('Cumulative doses received by source') +
    theme_minimal()
  
  
  leg2 <- get_legend(p2)
  
  p <- plot_grid(
    p1, 
    leg1,
    p2 + theme(legend.position='none'), 
    leg2,
    ncol=2,
    nrow=2,
    rel_widths=c(8,2),
    rel_heights=c(6,5),
    align='h'
  )
  
  print(p)
  
}

dev.off()

}
