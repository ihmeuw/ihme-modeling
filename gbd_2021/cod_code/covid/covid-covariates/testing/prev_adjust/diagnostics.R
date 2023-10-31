library(ggplot2)

#' Plot the reference, better, and worse scenarios
plot_scenarios <- function(out_dt, in_dt, out_path) {
    melt_dt <- melt(out_dt[, .(
        location_id, location_name, date,
        raked_ref_test_pc,
        better_fcast, worse_fcast,
        total_pc_100, observed
    )],
    id.vars = c("location_id", "location_name", "date", "total_pc_100", "observed")
    )
    melt_dt[grepl("raked_ref", variable), scenario := "reference"]
    melt_dt[grepl("better", variable), scenario := "better"]
    melt_dt[grepl("worse", variable), scenario := "worse"]
    melt_dt[variable %in% c("raked_ref_test_pc", "better_fcast", "worse_fcast"), value := value * 1e5]
    
    melt_dt[, plot_name := paste0(location_id, " - ", location_name)]
    pdf(out_path, width = 12, height = 9)
    for (i in seq(ceiling(length(sort(unique(melt_dt$location_id))) / 12))) {
        loc_list <- sort(unique(melt_dt$location_id))[((i - 1) * 12 + 1):(min(i * 12, length(sort(unique(melt_dt$location_id)))))]
        plot_dt <- melt_dt[location_id %in% loc_list]
        plot_dt[observed == FALSE, total_pc_100 := NA]
        
        gg <- ggplot() +
            geom_line(data = plot_dt[!is.na(value)], aes(x = date, y = value, color = scenario), alpha = 0.5) +
            geom_point(data = in_dt[location_id %in% loc_list & !is.na(daily_total_reported)], aes(x = date, y = daily_total_reported / pop * 1e5), color = "black", size = 0.1) +
            ylab("Test  per 100k") +
            xlab("Date") +
            scale_linetype_manual(values = c("dashed", "solid")) +
            theme_bw() +
            facet_wrap(~location_name, nrow = 3, scale = "free") +
            theme(legend.position = "bottom")
        print(gg)
    }
    invisible(dev.off())
}
#' Plot comparison with previous best
plot_prod_comp <- function(simple_data_path, raw_data_path, seir_testing_reference, out_path, hierarchy) {
    simple_dt <- fread(simple_data_path)
    simple_dt[, date := as.Date(date)]
    raw_dt <- fread(raw_data_path)
    raw_dt[, date := as.Date(date)]
    best_dt <- fread(file.path(seir_testing_reference))
    setnames(best_dt, "testing_reference", "test_pc")
    best_dt <- merge(best_dt, unique(simple_dt[, .(location_id, location_name)]))
    best_dt[, version := "previous best"]
    best_dt[, date := as.Date(date)]
    simple_dt[, version := paste0("latest - ", release)]
    dt <- rbind(best_dt[location_id %in% unique(simple_dt$location_id)], simple_dt, fill = T)
    dt <- dt[location_id %in% location_id]
    
    sorted <- ihme.covid::sort_hierarchy(hierarchy[location_id %in% unique(dt$location_id)])
    pdf(out_path, width = 12, height = 8)
    for(loc_id in unique(sorted$location_id)) {
        loc_dt <- dt[location_id == loc_id]
        gg <- ggplot() +
            geom_line(data = loc_dt[!is.na(test_pc)], aes(x = date, y = test_pc * 1e5, color = version, linetype = observed == 1)) +
            geom_point(data = raw_dt[location_id == loc_id & !is.na(daily_total_reported)], aes(x = date, y = daily_total_reported / pop * 1e5), color = "black") +
            theme_bw() +
            theme(legend.position = "bottom") +
            xlab("Date") +
            ylab("Tests per 100k") +
            guides(linetype = F) + 
            ggtitle(unique(loc_dt$location_name))
        print(gg)
    }
    invisible(dev.off())
}

## Mapping code
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
breaks <- c(0, 5, 10, 20, 40, 70, 100, 150, 200, 300, 99999)
breaks_labels <- c('0 - 4', '5 - 9', '10 - 19', '20 - 39', '40 - 69', '70 - 99', '100 - 149', '150 - 199', '200 - 299', '300+')
colors <- brewer.pal((length(breaks_labels)), "RdYlBu")

## Paths
globe_shp_path <- 'FILEPATH/GBD2017_mapping_final.shp'

## Read 
globe_shp <- readOGR(globe_shp_path)


plot_global <- function(df, value, shp_plt, plot_title, leg_title) {
    df$bin <- df[,..value]
    df$bin <- df$bin * 1e5
    #df$bin <- cut(df$bin, breaks=breaks)
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
        scale_fill_continuous(name = paste0(leg_title)) +
        ggtitle(paste0(plot_title)) +
        theme(legend.position = c(0.15, 0.3), legend.direction = 'vertical') + guides(fill = guide_legend(ncol = 1))
    
    return(gg_global)
}
