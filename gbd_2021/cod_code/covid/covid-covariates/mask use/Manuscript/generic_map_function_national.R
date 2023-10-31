######################################################################
## Generic Mapping function: Only plot GBD values
## Expects a data.table with location_id, a binned variable to be mapped
#####################################################################3

library(maptools)
library(RColorBrewer)
library(reshape2)
library(sp)
library(ggplot2)
library(grid)
library(gridExtra)
library(dplyr)
library(rgdal)
library(scales)
library(timeDate)
library(lubridate)
library(lattice)
library(viridis)
library(zoo)
library(ggrepel)
library(data.table)
library(cowplot)
#library(ggplotify, lib = '/ihme/homes/scottg16/R')
library(TTR)
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_ids.R")

## Pull location hierarchy  ##
hierarchy <- get_location_metadata(115, 746, release_id = 9)

locnms = get_ids('location') #this never gets referenced - can delete?
locs_us = hierarchy[parent_id == 102 & location_id != 102]
locs_mex = hierarchy[parent_id == 130 & location_id != 130]
lvl3_locs = hierarchy[level == 3]

map_theme_main <- theme(panel.background = element_rect(fill="white"),
                        panel.border = element_rect(color="white", fill=NA, size=rel(1)),
                        axis.text.x = element_blank(),
                        axis.title.x = element_blank(),
                        axis.ticks.x = element_blank(),
                        axis.text.y = element_blank(),
                        axis.title.y = element_blank(),
                        plot.title = element_text(size = 16, hjust = .5),
                        plot.caption = element_text(size = 8, hjust = 0),
                        #legend.position = "bottom",
                        #legend.justification = c(0,0),
                        legend.text = element_text(size = 10),
                        legend.position = "right",
                        legend.title = element_text(size = 14),
                        axis.ticks.y = element_blank(),
                        panel.grid = element_blank(),
                        legend.key.size = unit(.6, 'cm'),
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
paho_coords_nuscn = coord_cartesian(xlim = c(-120, -35), ylim = c(-60, 30))
paho_coords = coord_cartesian(xlim = c(-180, 0), ylim = c(-60, 70))
emro_coords =  coord_cartesian(xlim = c(-10, 75), ylim = c(0, 40))
euro_coords = coord_cartesian(xlim = c(-20, 85), ylim = c(30, 70))
searo_coords = coord_cartesian(xlim = c(70, 140), ylim = c(-15, 44))
afro_coords = coord_cartesian(xlim = c(-20, 50), ylim = c(-35, 35))
wpro_coords = coord_cartesian(xlim = c(75, 180), ylim = c(-50, 50))


colors_default = colorRampPalette(brewer.pal(9, 'RdYlBu'))

# Read in shapefiles
us_shp <- readOGR('FILEPATH/covid_simp_2.shp')
dspt <- readOGR("/FILEPATH/GBD_WITH_INSETS_NOSUBS.shp")
dspt <- subset(dspt, ID %in% c(313, 143, 228)) #
dspt@data$id <- rownames(dspt@data)
dspt_df <- fortify(dspt, region = "id")
# create data frame
dspt_df <- merge(dspt_df, dspt@data, by = "id")
dspt_df$loc_id <- dspt_df$ID
dspt_df <- data.table(dspt_df)
dspt_df[, location_id := loc_id]

generic_map_function <- function(df, title, colors = "default", map_parent = NULL, loc_id = 1, china_subs = F, level = 0){

  setorder(df, "date")
  list_national_parents <- hierarchy[level == 3 & most_detailed == 0, location_id]
  has_national_est <- setdiff(list_national_parents, df$location_id)
  has_national_est <- list_national_parents[!(list_national_parents %in% has_national_est)]
  has_subnational_est <- list_national_parents[list_national_parents %in% has_national_est]

  #df = df[!location_id %in% c(102, 135, 130, 163, 165, 70, 86)]

  df$location_name = NULL
  df = unique(df)

  missing_est <- setdiff(hierarchy$location_id, c(df$location_id, 570))
  # A bit manual, but we don't want to plot Hubei, Indonesia subs
  missing_est <- missing_est[missing_est > 0]
  missing_est <- missing_est[missing_est != 503]
  missing_est <- missing_est[!(missing_est %in% hierarchy[parent_id %in% has_national_est]$location_id)]
  # Drop US, Spain, Italy, Germany, Indonesia
  # missing_est <- missing_est[!missing_est %in% c(102, 163, 92, 81, 86)]

  df_tmp <- df

  shp_plt <- us_shp
  shp_plt = subset(shp_plt, shp_plt@data$loc_id %in% c(unique(df_tmp$location_id), 424,349,195,338,189, 40, 6, 7, missing_est))
  #shp_plt <- subset(shp_plt, level < 2)
  shp_plt@data = data.frame(shp_plt@data, df_tmp[match(shp_plt@data$loc_id, df_tmp$location_id),])

  shp_plt@data$id <- rownames(shp_plt@data)

  # I don't know how these still get through
  wa_chn <- subset(shp_plt, loc_id %in% c(6,570))
  if(china_subs == T){
    shp_plt <- subset(shp_plt, !(parent_id %in% c(570)))
  } else {
    shp_plt <- subset(shp_plt, !(parent_id %in% c(6,570)))
  }

  shp_plt <- rbind(shp_plt, wa_chn)

  shp_plt_points <- fortify(shp_plt, region = "id")
  # create data frame
  shp_plt_DF <- merge(shp_plt_points, shp_plt@data, by = "id")
  shp_plt_DF = as.data.table(shp_plt_DF)


  # Annoying and not a very robust way to do this, but if mapping for a specific country, don't add disputed territories
  if(level == 0){
    shp_plt_DF <- rbind(shp_plt_DF, dspt_df, fill = T)
  }
  #shp_plt_DF <- rbind(shp_plt_DF, dspt_df, fill = T)
  shp_plt_DF <- subset(shp_plt_DF, loc_id != 376) # Drop Northern Mariana Islands
  if(570 %in% unique(df_tmp$location_id)){
    shp_plt_DF <- subset(shp_plt_DF, !loc_id %in% hierarchy[parent_id == 570, location_id])
  }
  shp_plt_DF <- shp_plt_DF[!is.na(location_id)]
  # zoom in to the region of interest
  #map_locs <- if()
  #shp_plt_DF <- subset(shp_plt_DF, )
  shp_plt_DF[is.na(bin), bin := "No data"]
  setorder(shp_plt_DF, 'group')
  map_colors <- colors

  if(loc_id %in% c(102, locs_us$location_id)){
    shp_plt_DF = shp_plt_DF[location_id %in% c(locs_us$location_id, hierarchy[parent_id == 570, location_id])]

    main = ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                         fill =bin)) +
      geom_polygon() + geom_path(size = 0.1) +
      scale_fill_manual('', values = rev(map_colors),
                        drop = F
      ) +
      coord_cartesian(xlim = c(-127, -70), ylim = c(22, 50)) +
      map_theme_main +
       guides(fill = guide_legend(ncol=1))

    hi <- ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                        fill =bin)) +
      geom_polygon() + geom_path(size = 0.1) +
      scale_fill_manual('', values = rev(map_colors),
                        drop = F
      ) +
      coord_cartesian(xlim = c(-161.5, -154.4), ylim = c(18.5, 22.4)) +
      map_theme_insets

    ak <- ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                        fill =bin)) +
      geom_polygon() + geom_path(size = 0.1) +
      scale_fill_manual('', values = rev(map_colors),
                        drop = F
      ) +
      coord_cartesian(xlim = c(-184, -129), ylim = c(50, 71.7)) +
      map_theme_insets


    ak_grb <- as_grob(ak)
    hi_grb <- as_grob(hi)

    main <- main +
      annotation_custom(grob = ak_grb,
                        xmin = -130,
                        xmax = -118,
                        ymin = 22,
                        ymax = 30) +
      annotation_custom(grob = hi_grb,
                        xmin = -118,
                        xmax = -105,
                        ymin = 22,
                        ymax = 27) +
      ggtitle(title)

  }else{

    main = ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                         fill =bin)) +
      geom_polygon() + geom_path(size = 0.1) +
      scale_fill_manual('', values = rev(map_colors),
                        drop = F) +
      map_theme_main  +
      guides(fill = guide_legend(ncol=1)) +
      labs(title = title)
    if(loc_id == 44566)
      main = main + euro_coords
    }
    if(loc_id == 44564){
        main = main + paho_coords
    }

  main <- main + theme(legend.key = element_rect(color="black"))
  plot(main)

}
