######################################################################
## Map deaths averted on Jan 1 2021
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
library(ggplotify, lib = 'FILEPATH')
library(TTR)
source("FILEPATH/get_location_metadata.R")

## Pull location hierarchy  ##
lsvid <- 720
hierarchy <- get_location_metadata(111, lsvid)

# Read in deaths averted
df <- fread("FILEPATH/mask_use_deaths_averted_9-23.csv")
df$date = as.Date(df$date)
#df$value <- df$deaths_mean / df$cuml_deaths
df$value <- df$pct_mean

# Add on Hubei
if(!(503 %in% unique(df$location_id))){
  df <- rbind(df, data.table(location_id=503, location_name = "Hubei", pct_val = NA), fill = T)
}

# Round about way to get missing locations filled
pop <- fread("FILEPATH/2020_07_29.01/pops.csv")
population <- aggregate(population ~ location_id, data=pop, function(x) sum(x))

missing_est <- setdiff(population$location_id, df$location_id)
# A bit manual, but we don't want to plot Hubei, Indonesia subs
missing_est <- missing_est[missing_est > 0]
missing_est <- missing_est[missing_est != 503]
missing_est <- missing_est[!(missing_est %in% hierarchy[parent_id==11]$location_id)] # Indonesia
missing_est <- missing_est[!(missing_est %in% hierarchy[parent_id==196]$location_id)] # South Africa
missing_est <- missing_est[!(missing_est %in% hierarchy[parent_id==6]$location_id)] # South Africa


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

# Arguments
## Paths
out_dir <- "FILEPATH"

colors9 = rev(c("#006837", #"#31A354",
                "#ADDD8E",
                "#E1EFA5", "#FFFFCC",
                "#FBF597", "#F4E73D", "#E7C626",
                "#C1A32F", "#C9CACC"))

# colors_bl_yl = rev(c("#0F6966", "#1C9A95", "#27BDBA",
#                 "#9ED8C8", "#D6F2D9",
#                 "#FFFDE9", #"#FFFFCC",
#                 "#FBF597",
#                 "#F4E73D",
#                 "#C9CACC"))
colors_yl_pr <- rev(c(#"#3D384C", # Dark purple
  #"#54445C", # Lighter dark purple
  "#505E7A","#417B90","#309799",
  #"#3FB295",
  "#6ECB87","#AADF74",
  "#EFEE68", # Faint yellow
  "#F5D45B", # Darker yellow
  "#C9CACC")) # Gray

df[value := pct_mean]
df[value < (-.75), bin_lab_2 := "> 75%"]
df[value >= (-0.75), bin_lab_2 := "50 to 75%"]
df[value >= (-0.5),  bin_lab_2 := "30 to 50%"]
df[value >= (-0.3), bin_lab_2 := "20 to 30%"]
df[value >= (-0.2), bin_lab_2 := "10 to 20%"]
df[value >= (-0.1), bin_lab_2 := "5 to 10%"]
df[value >= (-0.05), bin_lab_2 := "< 5%"]

dspt <- readOGR("FILEPATH/GBD_WITH_INSETS_NOSUBS.shp")
dspt <- subset(dspt, ID %in% c(313, 143, 228)) #
dspt@data$id <- rownames(dspt@data)
dspt_df <- fortify(dspt, region = "id")
# create data frame
dspt_df <- merge(dspt_df, dspt@data, by = "id")
dspt_df$loc_id <- dspt_df$ID
dspt_df <- data.table(dspt_df)

setorder(df, "date")
df = df[!location_id %in% c(102, 101, 135, 130, 163, 165, 70, 92, 81, 86, 6)]
df[location_id %in% loc_met[parent_id == 570, location_id], location_id := 570]
df$location_name = NULL
df = unique(df)

#mask_use = mask_use[date == "2020-06-03" | date == "2020-10-01" | date == '2020-06-13']
pdf(file.path(out_dir, "map_mask_averted_pct_revision.pdf"), width = 11, height = 5.5)

### Global Map ###

#df_tmp = mask_use[date == date_i]
df_tmp <- df

shp_plt <- us_shp
shp_plt = subset(shp_plt, shp_plt@data$loc_id %in% c(unique(df_tmp$location_id),424,570,349,195,338,189,6,40,7,missing_est))
shp_plt <- subset(shp_plt, level < 2)
shp_plt@data = data.frame(shp_plt@data, df_tmp[match(shp_plt@data$loc_id, df_tmp$location_id),])

shp_plt@data$id <- rownames(shp_plt@data)
shp_plt_points <- fortify(shp_plt, region = "id")
# create data frame
shp_plt_DF <- merge(shp_plt_points, shp_plt@data, by = "id")
shp_plt_DF = as.data.table(shp_plt_DF)

# Why are Chinese states showing up?
china_df <- subset(shp_plt_DF, loc_id == 6)
# shp_plt_DF <- subset(shp_plt_DF, parent_id != 6) 
#shp_plt_DF <- rbind(shp_plt_DF, china_df)
shp_plt_DF <- subset(shp_plt_DF, loc_id != 6)
shp_plt_DF <- rbind(shp_plt_DF, dspt_df, fill = T)
shp_plt_DF <- subset(shp_plt_DF, loc_id != 376) # Drop Northern Mariana Islands

shp_plt_DF[is.na(bin_lab_2), bin_lab_2 := "No estimates"]
shp_plt_DF$bin_lab_2 = factor(shp_plt_DF$bin_lab_2,
                              levels = (c("> 75%", "50 to 75%", "30 to 50%", "20 to 30%", "10 to 20%", "5 to 10%",  "< 5%","No estimates")),
                              labels = (c("> 75%", "50 to 75%", "30 to 50%", "20 to 30%", "10 to 20%", "5 to 10%",  "< 5%", "No estimates")))
setorder(shp_plt_DF, 'group')


main = ggplot(data = shp_plt_DF, aes(x=long, y=lat, group = group,
                                     fill =bin_lab_2)) +
  geom_polygon() + geom_path(size = 0.1) +
  scale_fill_manual('', values = rev(colors_yl_pr),
                    drop = F) +
  map_theme_main  +
  labs(title = "Percent difference in deaths universal mask scenario to reference\nJanuary 1, 2021")
plot(main)

dev.off()
