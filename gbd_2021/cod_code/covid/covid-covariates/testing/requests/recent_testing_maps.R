### Make maps of smoothed testing per capita on most recent date for each location

## Setup
library(data.table)
library(ggplot2)
library(data.table)
library(rgdal)
library(dplyr)
library(grid)
library(gridExtra)
library(maptools)
library(RColorBrewer)
source("FILEPATH/get_location_metadata.R")

## Arguments
release <- "best"
breaks <- c(0, 5, 10, 20, 40, 70, 100, 150, 200, 300, 99999)
breaks_labels <- c('0 - 4', '5 - 9', '10 - 19', '20 - 39', '40 - 69', '70 - 99', '100 - 149', '150 - 199', '200 - 299', '300+')
colors <- brewer.pal((length(breaks_labels)), "RdYlBu")

## Paths
data_path <- file.path("FILEPATH", release, "forecast_raked_test_pc_simple.csv")
globe_shp_path <- 'FILEPATH/figure_shape_w_subnats.shp'
us_shp_path <- 'FILEPATH/GBD2019_mapping_final.shp'
out_path <-file.path("FILEPATH", release, "latest_testing_map.pdf")

## Read 
in_dt <- fread(data_path)
globe_shp <- readOGR(globe_shp_path)
shp1 = subset(globe_shp, globe_shp@data$ihme_lc_id %in% NA)
shp2 = subset(globe_shp, globe_shp@data$ihme_loc_d %in% NA)
shp2@data$ihme_loc_d = shp2@data$ihme_lc_id
globe_shp = rbind(shp1, shp2)
us_shp <- readOGR(us_shp_path)
us_locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 6)[parent_id == 102 | location_id == 102]

## Process
in_dt[, date := as.Date(date)]
df <- rbindlist(lapply(unique(in_dt$location_id), function(loc) {
  # Grab
  loc_dt <- in_dt[location_id == loc & !is.na(test_pc)]
  loc_dt[observed == 1][date == max(date), .(location_id, location_name, date, test_pc, pop)]
}))
df[, value:= test_pc * 1e5]
df[, bin := cut(value, breaks, right = F, labels = breaks_labels)]

## Plot

plot_global <- function(df, shp_plt, plot_title, leg_title) {
  shp_plt = subset(shp_plt, (shp_plt@data$loc_id != 102 & shp_plt@data$loc_id %in% df$location_id))
  shp_plt@data = data.frame(shp_plt@data, df[match(shp_plt@data$loc_id, df$location_id),])
  
  shp_plt@data$id <- rownames(shp_plt@data)
  shp_plt_points <- fortify(shp_plt, region = "id")
  # create data frame
  shp_plt_DF <- merge(shp_plt_points, shp_plt@data, by = "id")
  setorder(shp_plt_DF, 'group')
  
  # create map theme
  map_theme_main <- theme(panel.background = element_rect(fill="white"),
                          panel.border = element_rect(color="white", fill=NA, size=rel(1)),
                          axis.text.x = element_blank(),
                          axis.title.x = element_blank(),
                          axis.ticks.x = element_blank(),
                          axis.text.y = element_blank(),
                          axis.title.y = element_blank(),
                          plot.title = element_text(size = 16, hjust = 0.5),
                          legend.text = element_text(size = 12),
                          legend.title = element_text(size = 14),
                          axis.ticks.y = element_blank(),
                          panel.grid = element_blank(),
                          legend.key.size = unit(.8, 'cm'),
                          panel.grid.major = element_line(color = 'white'))
  ## Global
  gg_global <- ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                             fill = bin)) +
    geom_polygon() + geom_path(size = 0.1) +
    scale_fill_manual(values = colors,
                      name = paste0(leg_title), na.translate = F) +
    ggtitle(paste0(plot_title)) +
    map_theme_main + theme(legend.position = c(0.15, 0.3), legend.direction = 'vertical') + guides(fill = guide_legend(ncol = 1))
  
  return(gg_global)
}

plot_us <- function(df, shp_plt, plot_title, leg_title, us_locs) {
  map_theme_main <- theme(panel.background = element_rect(fill="white"),
                          panel.border = element_rect(color="white", fill=NA, size=rel(1)),
                          axis.text.x = element_blank(),
                          axis.title.x = element_blank(),
                          axis.ticks.x = element_blank(),
                          axis.text.y = element_blank(),
                          axis.title.y = element_blank(),
                          plot.title = element_text(size = 16, hjust = 0.5),
                          #legend.position = "bottom",
                          #legend.justification = c(0,0),
                          legend.text = element_text(size = 12),
                          legend.title = element_text(size = 14),
                          axis.ticks.y = element_blank(),
                          panel.grid = element_blank(),
                          legend.key.size = unit(.8, 'cm'),
                          panel.grid.major = element_line(color = 'white'))
  map_theme_insets <- theme(panel.background = element_rect(fill="white"),
                            panel.border = element_rect(color="black", fill=NA, size=rel(1)),
                            axis.text.x = element_blank(),
                            axis.title.x = element_blank(),
                            axis.ticks.x = element_blank(),
                            axis.text.y = element_blank(),
                            axis.title.y = element_blank(),
                            legend.position = "none",
                            legend.justification = c(0,0),
                            plot.margin=grid::unit(c(0,0,0,0), "mm"),
                            axis.ticks.y = element_blank(),
                            panel.grid = element_blank(),
                            legend.key.size = unit(.5, 'cm'),
                            panel.grid.major = element_line(color = 'white'))
  shp_plt = subset(shp_plt, (shp_plt@data$loc_id != 102 & shp_plt@data$loc_id %in% us_locs$location_id))
  shp_plt@data = data.frame(shp_plt@data, df[match(shp_plt@data$loc_id, df$location_id),])
  
  shp_plt@data$id <- rownames(shp_plt@data)
  shp_plt_points <- fortify(shp_plt, region = "id")
  # create data frame
  shp_plt_DF <- merge(shp_plt_points, shp_plt@data, by = "id")
  setorder(shp_plt_DF, 'group')
  
  main = ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                       fill = bin)) +
    geom_polygon() + geom_path(size = 0.1) +
    scale_fill_manual(values = colors[breaks_labels %in% unique(shp_plt_DF$bin)],
                      name = paste0(leg_title),
                      breaks = levels(shp_plt_DF$bin)) +
    coord_cartesian(xlim = c(-127, -60), ylim = c(22, 50)) +
    ggtitle(paste0(plot_title)) +
    map_theme_main + theme(legend.position = c(0.85, 0.3), legend.direction = 'vertical') + guides(fill = guide_legend(ncol = 1))
  
  hi <- ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                      fill =bin)) +
    geom_polygon() + geom_path(size = 0.1) +
    scale_fill_manual(values = colors[breaks_labels %in% unique(shp_plt_DF$bin)],
                      breaks = levels(shp_plt_DF$bin)) +
    coord_cartesian(xlim = c(-161.5, -154.4), ylim = c(18.5, 22.4)) +
    map_theme_insets + theme(legend.position = 'none')
  
  ak <- ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                      fill =bin)) +
    geom_polygon() + geom_path(size = 0.1) +
    scale_fill_manual(values = colors[breaks_labels %in% unique(shp_plt_DF$bin)],
                      breaks = levels(shp_plt_DF$bin)) +
    coord_cartesian(xlim = c(-184, -129), ylim = c(50, 71.7)) +
    map_theme_insets+ theme(legend.position = 'none')
  
  
  ak_grb <- ggplotGrob(ak)
  hi_grb <- ggplotGrob(hi)
  
  us_map <- main +
    annotation_custom(grob = ak_grb,
                      xmin = -130,
                      xmax = -118,
                      ymin = 22,
                      ymax = 30) +
    annotation_custom(grob = hi_grb,
                      xmin = -117,
                      xmax = -104,
                      ymin = 22,
                      ymax = 27)
  
  return(us_map)
}

gg_global <- plot_global(df, globe_shp, 
                         plot_title = "Daily testing per capita",
                         leg_title = "Tests per 100k\n population:")
gg_us <- plot_us(df, us_shp,
                 plot_title = "Daily testing per capita (United States)",
                 leg_title = "Tests per 100k population:",
                 us_locs)

## Save
pdf(out_path, width = 12.5, height = 8)
  print(gg_global)
  print(gg_us)
dev.off()