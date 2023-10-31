################################################################
## Diagnostic plots for vaccine dose supply estimates
################################################################

library(data.table)
library(ggplot2)
library(gridExtra)
library(scales)
library(boot)
library(zoo)

source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))

source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))
source(CODE_PATHS$MAPPING_FUNCTION_PATH)

hierarchy <- gbd_data$get_covid_modeling_hierarchy()

## Setup and background
# Set up command line argument parser
  parser <- argparse::ArgumentParser(
    description = 'Diagnostics for vaccine supply',
    allow_abbrev = FALSE
  )
  
  parser$add_argument('--version', help = 'Full path to vaccine covariate version')
  
  message(paste(c('COMMAND ARGS:',commandArgs(TRUE)), collapse = ' '))
  args <- parser$parse_args(commandArgs(TRUE))
  
  vaccine_path <- args$version

## Grab final_dt and purchase_candidates
  final_dt <- vaccine_data$load_final_doses(vaccine_path)
  purchase_candidates <- vaccine_data$load_purchase_candidates(vaccine_path)
  
## Plot them at a leisurely pace while not interrupting the rest of the pipeline :upside-down-face:
# Doses by manufacturer/quarter/location
pdf(file.path(vaccine_path, "doses_by_location_bars.pdf"), height = 6, width = 8)

 for(loc in unique(purchase_candidates$location_id)){
   
    p <- ggplot(purchase_candidates[location_id == loc & !is.na(date)], aes(x=factor(effective_date), y=sum_location_secured_daily_doses, fill = manufacturer)) + 
      geom_bar(stat="identity", col = "black") + xlab("Start date doses available") + scale_y_continuous("Doses", labels = comma) + 
      scale_fill_discrete("") + 
      ggtitle(unique(purchase_candidates[location_id == loc, location]), 
              subtitle = paste0(format(round(sum(purchase_candidates[location_id == loc]$sum_location_secured_daily_doses, na.rm = T), 0), big.mark = ","), 
                                " total probable secured doses")) + 
      theme_bw() +
      facet_wrap(~location) + theme(axis.text.x = element_text(angle = 90, hjust = 1))
    
    print(p)
    
 }

dev.off()
  
pdf(file.path(vaccine_path, "doses_by_location_lines.pdf"), height = 8, width = 6)

  for(loc in unique(final_dt$location_id)){
    
    p <- ggplot(final_dt[location_id == loc], aes(x=as.Date(date))) +
          geom_line(aes(y = secured_daily_doses, col = "Secured")) +
          geom_line(aes(y = secured_daily_doses * weighted_efficacy_location, col = "Effective wildtype")) +
          geom_line(aes(y = secured_daily_doses * weighted_variant_efficacy_location, col = "Effective variant")) +
          geom_line(aes(y = secured_daily_doses * weighted_protected_location, col = "Immune")) +
          theme_bw() + xlab("") + scale_y_continuous("Doses per day", labels = comma) + scale_color_discrete("")
    
    q <- ggplot(final_dt[location_id == loc], aes(x=as.Date(date))) +
          geom_line(aes(y = cumulative_doses, col = "Secured")) +
          theme_bw() + xlab("") + scale_y_continuous("Cumulative doses", labels = comma) + scale_color_manual("", values = "blue")
    
    grid.arrange(p, q, top = unique(final_dt[location_id == loc, location_name]))
    
  }

dev.off()

## Make a diagnostic map
# Map results

lsvid <- 771
location_id <- 1
loc_id <- 1

map_dt <- final_dt[!(location_id %in% c(102, 163, 135, 130, 95, 86, 101, 165, 81, 92))]
map_dt[, bin := cut(cumulative_doses, c(0,1e5,1e6,1e7,1e8,1e12),
                    labels = c("<100,000","100k-1M","1M-10M","10M-100M",">100M"))]
total_secured[, date := Sys.Date()]
if(!(57 %in% total_secured$location_id)){
  total_secured <- rbind(total_secured,
                         data.table(location_id = 57, secured_location = NA, likely_location = NA, population = NA), fill = T)
}
colors <- c("gray", (brewer.pal(5, "RdYlBu")))

pdf(file.path(vaccine_path, "doses_by_location_maps.pdf"), height = 5.5, width = 11)
  # Map of the secured doses
  map_dt[, bin := cut(cumulative_doses, c(0,1e6,5e6,1e7,5e7,1e8,1e12),
                      labels = c("<1M","1M-5M","5M-10M","10M-50M","50M-100M",">100M"))]
  generic_map_function(map_dt[date == "2021-12-31"], title = "Cumulative secured doses (December 31 2021)", colors = c("gray", (brewer.pal(6, "RdYlBu"))))

  # Map of the secured doses per capita
  map_dt[, bin := cut(cumulative_doses/population * 100000, c(0,5000,10000,50000,100000,250000,1000000),
                      labels = c("<5,000","5,000-9,000","10,000-49,000","50,000-99,000","100,000-249,000",">250,000"))]
  generic_map_function(map_dt[date == "2021-12-31"], title = "Secured effective doses per 100,000 (Cumulative, December 31, 2021)", colors = c("gray", (brewer.pal(6, "RdYlBu"))))

  # Map of total secured doses per capita
  total_secured[, bin := cut(secured_location/population * 100000, c(0,5000,10000,50000,100000,250000,1000000),
                      labels = c("<5,000","5,000-9,000","10,000-49,000","50,000-99,000","100,000-249,000",">250,000"))]
  generic_map_function(total_secured, title = "Purchased or secured doses per 100,000", colors = c("gray", rev(brewer.pal(6, "RdYlBu"))))

  total_secured[, bin := cut(likely_location/population * 100000, c(0,5000,10000,50000,100000,250000,1000000),
                             labels = c("<5,000","5,000-9,000","10,000-49,000","50,000-99,000","100,000-249,000",">250,000"))]
  generic_map_function(total_secured, title = "Likely secured doses per 100,000", colors = c("gray", rev(brewer.pal(6, "RdYlBu"))))

dev.off()
