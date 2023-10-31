## Make maps of testing rates ##

rm(list=ls(all=TRUE))
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
library(ggplotify, lib = 'FILEPATH')
library(TTR)

## Arguments
release <- "best"
breaks <- c(0, 5, 10, 20, 40, 70, 100, 150, 200, 300, 99999)
breaks_labels <- c('0 - 4', '5 - 9', '10 - 19', '20 - 39', '40 - 69', '70 - 99', '100 - 149', '150 - 199', '200 - 299', '300+')
colors <- c(brewer.pal((length(breaks_labels)), "RdYlBu"), "gray")

## Input data
data_path <- file.path("FILEPATH", release, "forecast_raked_test_pc_simple.csv")
in_dt <- fread(data_path)
## Process
in_dt[, date := as.Date(date)]
# df <- rbindlist(lapply(unique(in_dt$location_id), function(loc) {
#   # Grab
#   loc_dt <- in_dt[location_id == loc & !is.na(test_pc)]
#   loc_dt[observed == 1][date == max(date), .(location_id, location_name, date, test_pc, pop)]
# }))
df <- in_dt
df[, value:= test_pc * 1e5]
df[, bin := cut(value, breaks, right = F, labels = breaks_labels)]

setorder(df, "date")
df = df[!location_id %in% c(102, 101, 135, 130, 163, 165, 11, 196, 70, 92, 81, 86)]
#df[location_id %in% loc_met[parent_id == 570, location_id], location_id := 570]
df$location_name = NULL
df = unique(df)

df <- df[date=="2020-04-01"]

# Drop WA non/king-county
df <- subset(df, !(location_id %in% c(570, 3539, 60886)))
df$location_id <- ifelse(df$location_id==60887, 570, df$location_id)

dir = "FILEPATH"
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")

locnms = get_ids('location')
loc_met = get_location_metadata(location_set_version_id = 720, location_set_id = 111)
locs_us = loc_met[parent_id == 102 & location_id != 102]
locs_mex = loc_met[parent_id == 130 & location_id != 130]
lvl3_locs = loc_met[level == 3]


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

us_shp <- readOGR('FILEPATH/shape_covid_simp.shp')

library(data.table)
library(ggplot2)
library(viridis)

out_dir <- "FILEPATH"

colors6 = rev(c("#006837", "#31A354", "#ADDD8E", "#F4E73D",
                "#E7C626", "#C1A32F", "#C9CACC"))

colors9 = rev(c("#006837", "#31A354", "#ADDD8E",
                "#E1EFA5", "#FFFFCC",
                "#FBF597", "#F4E73D", "#E7C626",
                "#C1A32F", "#C9CACC"))

colors9 = rev(c("#0F6966", "#1C9A95", "#27BDBA",
                "#9ED8C8", "#D6F2D9",
                "#FFFDE9", "#FFFFCC", "#FBF597",
                "#F4E73D", "#C9CACC"))

colors7 = rev(c("#0F6966", "#1C9A95", "#27BDBA",
                #"#9ED8C8",
                #"#FFFDE9",
                "#FFFFCC", "#FBF597",
                "#F4E73D", "#C9CACC"))


dspt <- readOGR("FILEPATH/GBD_WITH_INSETS_NOSUBS.shp")
dspt <- subset(dspt, ID %in% c(313, 143, 228)) #
dspt@data$id <- rownames(dspt@data)
dspt_df <- fortify(dspt, region = "id")
# create data frame
dspt_df <- merge(dspt_df, dspt@data, by = "id")
dspt_df$loc_id <- dspt_df$ID
dspt_df <- data.table(dspt_df)

#in_dt = in_dt[date == '2020-07-05']
admin0 <- setdiff(unique(in_dt$location_id), as.numeric(us_shp@data$loc_id[us_shp@data$level==0]))
admin0 <- hierarchy[level==3 & most_detailed == 1]

#mask_use = mask_use[date == "2020-06-03" | date == "2020-10-01" | date == '2020-06-13']
pdf(file.path(out_dir, "map_testing_04-01.pdf"), width = 11, height = 5.5)

### Global Map ###

df_tmp <- df

shp_plt <- us_shp
shp_plt = subset(shp_plt, shp_plt@data$loc_id %in% c(unique(in_dt$location_id), admin0$location_id, 424, 570, 435, 182, 349, 338,189, 40, 6, 7, locs_us$location_id))
shp_plt <- subset(shp_plt, level < 2)
shp_plt@data = data.frame(shp_plt@data, df_tmp[match(shp_plt@data$loc_id, df_tmp$location_id),])

shp_plt@data$id <- rownames(shp_plt@data)
shp_plt_points <- fortify(shp_plt, region = "id")
# create data frame
shp_plt_DF <- merge(shp_plt_points, shp_plt@data, by = "id")
shp_plt_DF = as.data.table(shp_plt_DF)

shp_plt_DF <- rbind(shp_plt_DF, dspt_df, fill = T)
shp_plt_DF <- subset(shp_plt_DF, loc_id != 376) # Drop Northern Mariana Islands
shp_plt_DF <- subset(shp_plt_DF, !is.na(location_id))

shp_plt_DF[is.na(bin), bin := "No data"]

setorder(shp_plt_DF, 'group')


main = ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                     fill =bin)) +
  geom_polygon() + geom_path(size = 0.1) +
  scale_fill_manual('', values = colors,
                    drop = F) +
  #coord_cartesian(xlim = c(-127, -60), ylim = c(22, 50)) +
  map_theme_main  +
  labs(title = paste0("Diagnostic testing per capita\nJuly 5"))
plot(main)

### US Map ###

shp_plt = us_shp
shp_plt = subset(shp_plt, shp_plt@data$loc_id %in% c(locs_us$location_id))
shp_plt@data = data.frame(shp_plt@data, df_tmp[match(shp_plt@data$loc_id, df_tmp$location_id),])

shp_plt@data$id <- rownames(shp_plt@data)
shp_plt_points <- fortify(shp_plt, region = "id")
# create data frame
shp_plt_DF <- merge(shp_plt_points, shp_plt@data, by = "id")
shp_plt_DF = as.data.table(shp_plt_DF)

shp_plt_DF <- rbind(shp_plt_DF, dspt_df, fill = T)
shp_plt_DF <- subset(shp_plt_DF, loc_id != 376) # Drop Northern Mariana Islands

shp_plt_DF[is.na(bin_lab_2), bin_lab_2 := "No data"]
shp_plt_DF$bin_lab_2 = factor(shp_plt_DF$bin_lab_2,
                              levels = rev(c("No data", "< 10%", "10-20%", "20-30%", "30-40%", "40-50%", "50-60%",  "60-70%",  "70-80%" , "> 80%")),
                              labels = rev(c("No data", "< 10%", "10-20%", "20-30%", "30-40%", "40-50%", "50-60%",  "60-70%",  "70-80%", "> 80%")))
shp_plt_DF$bin_alt <- cut(shp_plt_DF$mask_use*100, c(0,25,30,35,40,50,100),
                          labels=c("< 25%","25-29%","30-34%","35-39%","40-49%","> 50%"))
shp_plt_DF$bin_alt = factor(shp_plt_DF$bin_alt,
                            levels = rev(c("< 25%","25-29%","30-34%","35-39%","40-49%","> 50%")),
                            labels = rev(c("< 25%","25-29%","30-34%","35-39%","40-49%","> 50%")))
shp_plt_DF <- subset(shp_plt_DF, !is.na(bin_alt))
setorder(shp_plt_DF, 'group')

main = ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                     fill = bin_alt)) +
  geom_polygon() + geom_path(size = 0.1) +
  scale_fill_manual('', values = rev(colors7),
                    drop = F) +
  coord_cartesian(xlim = c(-127, -60), ylim = c(22, 50)) +
  map_theme_main  +
  labs(title = "Percent who say they always wear a mask when going out\nJuly 5",
       caption = "Source: PREMISE")

hi <- ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                    fill = bin_alt)) +
  geom_polygon() + geom_path(size = 0.1) +
  scale_fill_manual('', values = rev(colors7),
                    drop = F
  ) +
  coord_cartesian(xlim = c(-161.5, -154.4), ylim = c(18.5, 22.4)) +
  map_theme_insets

ak <- ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                    fill = bin_alt)) +
  geom_polygon() + geom_path(size = 0.1) +
  scale_fill_manual('', values = rev(colors7),
                    drop = F
  ) +
  coord_cartesian(xlim = c(-184, -129), ylim = c(50, 71.7)) +
  map_theme_insets


ak_grb <- as.grob(ak)
hi_grb <- as.grob(hi)

map_als <- main +
  annotation_custom(grob = ak_grb,
                    xmin = -130,
                    xmax = -118,
                    ymin = 22,
                    ymax = 30) +
  annotation_custom(grob = hi_grb,
                    xmin = -118,
                    xmax = -105,
                    ymin = 22,
                    ymax = 27)
plot(map_als)


dev.off()
