
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

# source("/ihme/code/covid-19/user/ctroeger/covid-beta-inputs/src/covid_beta_inputs/mask_use/Manuscript/map_mask_use.R")

dir = "FILEPATH"
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")

mask_version <- "best"
plot_date <- as.Date("2021-05-12")


locnms = get_ids('location')
loc_met = get_location_metadata(location_set_version_id = 746, location_set_id = 115, release_id = 9)
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

# Arguments
## Paths
out_dir <- "FILEPATH"

colors7 = rev(c("#0F6966", "#1C9A95", "#27BDBA",
                "#9ED8C8", "#D6F2D9",
                "#FFFDE9", "#FFFFCC", "#FBF597",
                "#F4E73D", "#C9CACC"))

colors7 = rev(c("#0F6966", "#1C9A95", "#27BDBA",
                #"#9ED8C8",
                #"#FFFDE9",
                "#FFFFCC", "#FBF597",
                "#F4E73D", "#C9CACC"))

##54445C,#505E7A,#417B90,#309799,#3FB295,#6ECB87,#AADF74,#EFEE68
colors_yl_pr <- rev(c(#"#3D384C", # Dark purple
                      "#54445C", # Lighter dark purple
                      "#505E7A","#417B90","#309799","#3FB295","#6ECB87","#AADF74",
                      "#EFEE68", # Faint yellow
                      "#F5D45B", # Darker yellow
                      "#C9CACC")) # Gray
  
  
mask_use_2 = fread(paste0('FILEPATH',mask_version,'/mask_use.csv'))
mask_use_2$date = as.Date(mask_use_2$date)

mask_use_2[,value := mask_use]
mask_use = as.data.table(mask_use_2)

mask_use[value < .10, bin_lab_2 := "< 10%"]
mask_use[value >= .10, bin_lab_2 := "10-25%"]
mask_use[value >= .25,  bin_lab_2 := "25-40%"]
# mask_use[value >= .30, bin_lab_2 := "30-40%"]
mask_use[value >= .4, bin_lab_2 := "40-50%"]
mask_use[value >= .5, bin_lab_2 := "50-60%"]
mask_use[value >= .60, bin_lab_2 := "60-70%"]
mask_use[value >= .7, bin_lab_2 := "70-80%"]
mask_use[value >= .8, bin_lab_2 := "80-95%"]
mask_use[value >= .95, bin_lab_2 := ">= 95%"]

dspt <- readOGR("FILEPATH/GBD_WITH_INSETS_NOSUBS.shp")
dspt <- subset(dspt, ID %in% c(313, 143, 228)) #
dspt@data$id <- rownames(dspt@data)
dspt_df <- fortify(dspt, region = "id")
# create data frame
dspt_df <- merge(dspt_df, dspt@data, by = "id")
dspt_df$loc_id <- dspt_df$ID
dspt_df <- data.table(dspt_df)

setorder(mask_use, "date")
mask_use = mask_use[!location_id %in% c(102, 101, 135, 130, 163, 165, 70, 92, 81, 86, 6)]
mask_use[location_id %in% loc_met[parent_id == 570, location_id], location_id := 570]
mask_use <- mask_use[!(location_id %in% loc_met[parent_id %in% c(196, 11), location_id])]
mask_use$location_name = NULL
mask_use = unique(mask_use)
mask_use <- merge(mask_use, loc_met, by="location_id")

hist(mask_use[level == 1]$mask_use)
quantile(mask_use[level==1]$mask_use, 0.95, na.rm = T)
ecdf(mask_use$mask_use)(0.85)


## we can just print the most recent one
### Global Map ###
pdf(paste0(out_dir, "/sequence_map_mask_",plot_date,".pdf"), width = 11, height = 5.5)
  # for(p in c(as.Date(plot_date),as.Date(plot_date) - 7, as.Date(plot_date) - seq(14,112,14))){
  for(p in as.Date(plot_date)){
    p <- as.Date(p)
    df_tmp <- mask_use[date == p]
    
    shp_plt <- us_shp
    shp_plt = subset(shp_plt, shp_plt@data$loc_id %in% c(unique(df_tmp$location_id), 424, 570, 349, 338, 189, 40, 6, 7, locs_us$location_id))
    shp_plt <- subset(shp_plt, level < 2)
    shp_plt@data = data.frame(shp_plt@data, df_tmp[match(shp_plt@data$loc_id, df_tmp$location_id),])
    
    shp_plt@data$id <- rownames(shp_plt@data)
    shp_plt_points <- fortify(shp_plt, region = "id")
    # create data frame
    shp_plt_DF <- merge(shp_plt_points, shp_plt@data, by = "id")
    shp_plt_DF = as.data.table(shp_plt_DF)
    
    shp_plt_DF <- rbind(shp_plt_DF, dspt_df, fill = T)
    shp_plt_DF <- subset(shp_plt_DF, loc_id != 376) # Drop Northern Mariana Islands
    #shp_plt_df <- shp_plt_DF[loc_id != "Australia"] # Manually drop Australia national
    
    shp_plt_DF[is.na(bin_lab_2), bin_lab_2 := "No data"]
    shp_plt_DF$bin_lab_2 = factor(shp_plt_DF$bin_lab_2,
                                  levels = rev(c("No data", "< 10%", "10-25%", "25-40%", "40-50%", "50-60%",  "60-70%",  "70-80%" ,"80-95%",">= 95%")),
                                  labels = rev(c("No data", "< 10%", "10-24%", "25-39%", "40-49%", "50-59%",  "60-69%",  "70-79%", "80-94%",">= 95%")))
    setorder(shp_plt_DF, 'group')
    
    
    main = ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                         fill =bin_lab_2)) +
      geom_polygon() + geom_path(size = 0.1) +
      scale_fill_manual('', values = rev(colors_yl_pr),
                        drop = F) +
      #coord_cartesian(xlim = c(-127, -60), ylim = c(22, 50)) +
      map_theme_main  +
      labs(title = paste0("Percent who say they always wear a mask when going out\n",format(p, "%B %d %Y")),
           caption = 'PREMISE/Facebook Global symptom survey (This research is based on survey results from University of Maryland Social Data Science Center)')
    main <- main + theme(legend.key = element_rect(color="black"))
    plot(main)
  }
dev.off()

# ### US Map ###
pdf(paste0(out_dir, "/sequence_us_map_mask_",plot_date,".pdf"), width = 8, height = 5.5)
  #for(p in c(as.Date(plot_date),as.Date(plot_date) - 7, as.Date(plot_date) - seq(14,112,14))){
  for(p in as.Date(plot_date)){ 
    p <- as.Date(p)
    df_tmp <- mask_use[date == p]
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
    shp_plt_DF$bin_alt <- cut(shp_plt_DF$mask_use*100, c(0,30,35,40,45,50,55,100),
                              labels=c("< 30%","30-34%","35-39%","40-44%","45-49","50-54%","> 55%"))
    shp_plt_DF$bin_alt = factor(shp_plt_DF$bin_alt,
                                levels = rev(c("< 30%","30-34%","35-39%","40-44%","45-49","50-54%","> 55%")),
                                labels = rev(c("< 30%","30-34%","35-39%","40-44%","45-49","50-54%","> 55%")))

    ## Wider range to show Nancy's suggestion >95% target
    shp_plt_DF$bin_alt <- cut(shp_plt_DF$mask_use*100, c(0,20,30,40,50,60,70,95,100),
                              labels=c("< 20%","21-29%","30-39%","40-49","50-59%","60-69","70-94",">= 95%"))
    shp_plt_DF$bin_alt = factor(shp_plt_DF$bin_alt,
                                levels = rev(c("< 20%","21-29%","30-39%","40-49","50-59%","60-69","70-94",">= 95%")),
                                labels = rev(c("< 20%","21-29%","30-39%","40-49%","50-59%","60-69%","70-94%",">= 95%")))
    
    
    shp_plt_DF <- subset(shp_plt_DF, !is.na(bin_alt))
    setorder(shp_plt_DF, 'group')
    
    col_us <- colors_yl_pr[c(1,2,3,4,5,6,7,8,9)]
    
    main = ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                         fill = bin_alt)) +
      geom_polygon() + geom_path(size = 0.1) +
      scale_fill_manual('', values = rev(col_us),
                        drop = F) +
      coord_cartesian(xlim = c(-127, -60), ylim = c(22, 50)) +
      map_theme_main  +
      labs(title = paste0("Percent who say they always wear a mask when going out\n", format(p, "%B %d %Y")),
           caption = 'PREMISE/Facebook Global symptom survey (This research is based on survey results from University of Maryland Social Data Science Center)')
    
    hi <- ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                        fill = bin_alt)) +
      geom_polygon() + geom_path(size = 0.1) +
      scale_fill_manual('', values = rev(col_us),
                        drop = F
      ) +
      coord_cartesian(xlim = c(-161.5, -154.4), ylim = c(18.5, 22.4)) +
      map_theme_insets
    
    ak <- ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                        fill = bin_alt)) +
      geom_polygon() + geom_path(size = 0.1) +
      scale_fill_manual('', values = rev(col_us),
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
    map_als <- map_als + theme(legend.key = element_rect(color="black"))
    plot(map_als)
  }
dev.off()
