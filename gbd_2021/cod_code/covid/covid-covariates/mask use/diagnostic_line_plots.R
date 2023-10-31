#############################################################
## Diagnostic plots for mask use 
#############################################################
library(ggplot2)
library(ggrepel)
library(boot)
library(gridExtra)
library(data.table)
library(scales)
library(glue)

source(file.path("FILEPATH/get_location_metadata.R"))
hierarchy <- get_location_metadata(location_set_id = 115, location_set_version_id = 1051, release_id = 9)

## Setup and background
# Set up command line argument parser
  parser <- argparse::ArgumentParser(
    description='Launch a location-specific COVID results brief',
    allow_abbrev=FALSE
  )
  
  parser$add_argument('--version', help='New mask version')
  parser$add_argument('--compare_version', help='Compare (best) mask version')
  
  message(paste(c('COMMAND ARGS:',commandArgs(TRUE)), collapse=' '))
  args <- parser$parse_args(commandArgs(TRUE))
  
  version <- args$version
  best_version <- args$compare_version

## Import data
  cur_best <- fread(paste0("FILEPATH", best_version,"/mask_use.csv"))
  cur_best[, version := paste0("Compare: ", best_version)]
  cur_best$date <- as.Date(cur_best$date)
  
  prev_data <- fread(paste0("FILEPATH",best_version,"/used_data.csv"))
  prev_data[, version := paste0("Compare: ", best_version)]
  prev_data$date <- as.Date(prev_data$date)
  
  out_dt <- fread(paste0("FILEPATH", version,"/mask_use.csv"))
  out_dt[, version := paste0("New: ", version)]
  out_dt[, date := as.Date(date)]
  
  worse_dt <- fread(paste0("FILEPATH", version,"/mask_use_worse.csv"))
  worse_dt[, version := paste0("New: worse")]
  worse_dt[, date := as.Date(date)]
  
  high_dt <- fread(paste0("FILEPATH", version, "/mask_use_best.csv"))
  high_dt[, version := paste0("New: best")]
  high_dt[, date := as.Date(date)]
  
  dt <- fread(paste0("FILEPATH", version,"/used_data.csv"))
  dt[, version := paste0("New: ", version)]
  dt$date <- as.Date(dt$date)

## Bind data
  both_dt <- rbind(out_dt, cur_best, fill=T)
  data_dt <- rbind(dt[,c("location_id","date","prop_always","version","source")], 
                   prev_data[,c("location_id","date","prop_always","version","source")], fill=T)
  
  scat_dt <- merge(cur_best, out_dt, by=c("date","location_id"))
  scat_dt <- subset(scat_dt, date %in% as.Date(c("2020-09-01","2021-01-01","2021-04-01",
                                                 "2021-05-01","2021-06-01","2021-10-01", 
                                                 "2021-12-01", as.character(Sys.Date()))))

  data_dt$source[is.na(data_dt$source)] <- "Set 0"
  #data_dt$version <- ifelse(data_dt$source %in% c("Set 0", "KFF"), "New version", data_dt$version)

##---------------------------------------------------------------------- 
# Plot
# (This takes a while)
  
loc_list <- hierarchy[location_id %in% unique(cur_best$location_id)]
loc_list <- loc_list[order(sort_order)]  
 
pdf(paste0("FILEPATH", version, "/mask_use_scatter_",version,"_",best_version,".pdf"), width = 12, height = 12)

ggplot(scat_dt[location_id %in% loc_list$location_id], aes(x=mask_use.x, y=mask_use.y)) + geom_point() +
  geom_abline(intercept=0, slope=1, col="red", lty=2) + xlab("Previous Best") + ylab("New model") +
  ggtitle("Predicted mask use") + theme_minimal() + facet_wrap(~date) +
  geom_text_repel(data=scat_dt[location_id %in% loc_list$location_id & abs(mask_use.x - mask_use.y) > 0.1], aes(label=location_name.x))

dev.off()

plot_end_date <- as.character(Sys.Date() + 120)


pdf(paste0("FILEPATH", version, "/mask_use_plots_",version,"_",best_version,".pdf"), width = 9, height = 6, onefile = TRUE)
  
for(loc in loc_list$location_id) {
  
  #print(loc)
  
  gg <- ggplot() +
    geom_point(data = data_dt[location_id == loc], aes(x = date, y = prop_always * 100, shape=source, col=version), size=3, alpha = 0.6) +
    geom_line(data = both_dt[location_id == loc], aes(x = date, y = mask_use * 100, col=version), size=2) +
    geom_line(data = worse_dt[location_id == loc], aes(x = date, y = mask_use * 100, col = "Worse"), size=2) +
    geom_line(data = high_dt[location_id == loc], aes(x = date, y = mask_best * 100, col = "Best"), size=2) +
    scale_x_date("Date", limits = as.Date(c("2020-01-01",plot_end_date))) + ylab("Percent always mask use") +
    #facet_wrap(~ location_name) + 
    ggtitle(glue("{hierarchy[location_id == loc, location_name]} ({loc})")) +
    ylim(c(0, 100)) +
    scale_color_manual("", values = c("dodgerblue", "purple", "goldenrod", "firebrick")) + 
    theme_minimal() + theme(legend.position = 'bottom')
  
  #pp <- ggplot() +
  #  geom_point(data = data_dt[location_id == loc & date > "2021-03-01" & date < Sys.Date() + 14],
  #             aes(x = date, y = prop_always * 100, shape=source, col=version), size=2, alpha = 0.65) +
  #  geom_line(data = both_dt[location_id == loc & date > "2021-03-01" & date < Sys.Date() + 14],
  #            aes(x = date, y = mask_use * 100, col=version)) +
  #  geom_line(data = worse_dt[location_id == loc & date > "2021-03-01" & date < Sys.Date() + 14],
  #            aes(x = date, y= mask_use * 100, col = "Worse")) +
  #  scale_x_date("Date") + ylab("Percent always mask use") +
  #  facet_wrap(~ location_name) + ylim(c(0, 100)) +
  #  theme_bw() + scale_color_manual("", values = c("purple", "goldenrod","firebrick"))
  
  print(gg)
  #grid.arrange(gg, pp)
  
}
dev.off()


## Do you want a set of clean plots?
#pdf(paste0("/ihme/covid-19-2/mask-use-outputs/", version, "/clean_mask_use_timeseries.pdf"))
#  for(loc in loc_list$location_id) {
#    gg <- ggplot() +
#      geom_line(data = out_dt[location_id == loc], aes(x = date, y = mask_use * 100)) +
#      geom_point(data = dt[location_id == loc], aes(x = date, y = prop_always * 100), size=2) +
#      scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-12-31"))) + ylab("Percent always mask use") +
#      facet_wrap(~ location_name) + ylim(c(0, 100)) +
#      theme_bw()
#    print(gg)
#  }
#dev.off()