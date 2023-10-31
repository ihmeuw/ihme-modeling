################################################################
## Aggregations of mask use in Facebook data by age, sex, urbanicity
########################################################################
library(data.table)
library(ggplot2)
source(file.path("FILEPATH/get_location_metadata.R"))

hierarchy <- get_location_metadata(111, 727)
df <- fread('FILEPATH/mask_ts_tabulations.csv')

df <- merge(df, hierarchy[,c("location_id","region_name","super_region_name","level","parent_id")], by="location_id")

df$mask_use <- df$mask_7days_all
df$date <- as.Date(df$date, format = "%d.%m.%Y")

# Probably need to remove people who haven't left their homes in 7 days.
df$mask_use <- df$mask_7days_all - df$mask_7days_no_public

colors_grbu <- c("#20313E","#255662","#267E7F","#38A891","#64D398","#A2FB95")
colors_bark <- c("#519834","#2E361A","#514343","#B97172","#7B3453","#A00265")

pdf("FILEPATH/mask_tabulation_line_plots.pdf", height=8, width=12)
  ## Super-region aggregates by sex
  tab_sr_sex <- aggregate(cbind(N, mask_use) ~ super_region_name + gender + date, data = df, function(x) sum(x))
  location_levels <- c("Global", unique(tab_sr_sex$super_region_name))
  global <- aggregate(cbind(N, mask_use) ~ gender + date, data = df, function(x) sum(x))
  global$super_region_name <- "Global"
  tab_sr_sex <- rbind(tab_sr_sex, global)
  tab_sr_sex$name <- factor(tab_sr_sex$super_region_name, levels = location_levels)
  tab_sr_sex <- subset(tab_sr_sex, gender %in% c("male","female"))
  tab_sr_sex$proportion <- tab_sr_sex$mask_use / tab_sr_sex$N
  ggplot(tab_sr_sex, aes(x=date, y=proportion, col = gender)) + 
    #geom_line() + 
    stat_smooth(method = "loess", se = F) + 
    facet_wrap(~name) + theme_minimal() +
    ylab("Always wear a mask proportion") + scale_color_manual("Self-reported\ngender", labels = c("Female","Male"), values = colors_bark) + 
    scale_x_date("", date_breaks = "1 month", date_labels = "%b") + 
    ggtitle("Mask use by gender")
  
  ## Super-region aggregates by age
  df$age_group <- ifelse(df$age_group %in% c("65-74", "> 75"), "> 65", as.character(df$age_group))
  tab_sr_age <- aggregate(cbind(N, mask_use) ~ super_region_name + age_group + date, data = df, function(x) sum(x))
  global <- aggregate(cbind(N, mask_use) ~ age_group + date, data = df, function(x) sum(x))
  global$super_region_name <- "Global"
  tab_sr_age <- rbind(tab_sr_age, global)
  tab_sr_age$name <- factor(tab_sr_age$super_region_name, levels = location_levels)
  tab_sr_age$age_group <- factor(tab_sr_age$age_group, c("18-24","25-34","35-44","45-54","55-64","> 65"))
  tab_sr_age$proportion <- tab_sr_age$mask_use / tab_sr_age$N

  ggplot(tab_sr_age[tab_sr_age$N > 100, ], aes(x=date, y=proportion, col = age_group)) + 
    # geom_line() + 
    stat_smooth(method = "loess", se = F) +
    facet_wrap(~name) + theme_minimal() +
    ylab("Always wear a mask proportion") + scale_x_date("", date_breaks = "1 month", date_labels = "%b") +  
    scale_color_manual("Age group", values = colors_bark) + 
    ggtitle("Mask use by age")
  
  ## Super-region aggregates by urbanicity
  tab_sr_urban <- aggregate(cbind(N, mask_use) ~ super_region_name + urbanicity + date, data = df, function(x) sum(x))
  global <- aggregate(cbind(N, mask_use) ~ urbanicity + date, data = df, function(x) sum(x))
  global$super_region_name <- "Global"
  tab_sr_urban <- rbind(tab_sr_urban, global)
  tab_sr_urban$name <- factor(tab_sr_urban$super_region_name, location_levels)
  tab_sr_urban$proportion <- tab_sr_urban$mask_use / tab_sr_urban$N
  colors_grbu <- c("#20313E","#255662","#267E7F","#38A891","#64D398","#A2FB95")
  ggplot(tab_sr_urban[tab_sr_urban$N > 100 & tab_sr_urban$urbanicity != "", ], aes(x=date, y=proportion, col = urbanicity)) + 
    # geom_line() + 
    stat_smooth(method = "loess", se = F) + 
    facet_wrap(~name) + theme_minimal() + scale_color_manual("", values = colors_bark[c(1,2,5)], labels = c("City","Town","Rural/Village")) + 
    ylab("Always wear a mask proportion") + scale_x_date("", date_breaks = "1 month", date_labels = "%b") +  
    ggtitle("Mask use by urbanicity")
  
  ## Super-region aggregates by contacts
  tab_sr_contacts <- aggregate(cbind(N, mask_use) ~ super_region_name + contacts_24h + date, data = df, function(x) sum(x))
  global <- aggregate(cbind(N, mask_use) ~ contacts_24h + date, data = df, function(x) sum(x))
  global$super_region_name <- "Global"
  tab_sr_contacts <- rbind(tab_sr_contacts, global)
  tab_sr_contacts$name <- factor(tab_sr_contacts$super_region_name, levels = location_levels)
  tab_sr_contacts$proportion <- tab_sr_contacts$mask_use / tab_sr_contacts$N
  colors_grbu <- c("#20313E","#255662","#267E7F","#38A891","#64D398","#A2FB95")
  tab_sr_contacts$contacts_24h <- factor(tab_sr_contacts$contacts_24h, c("1-4","5-9","10-19",">20"))
  ggplot(tab_sr_contacts[tab_sr_contacts$N > 100 & tab_sr_contacts$contacts_24h != "", ], aes(x=date, y=proportion, col = contacts_24h)) + 
    #geom_line() + 
    stat_smooth(method = "loess", se = F) + 
    facet_wrap(~name) + theme_minimal() +
    ylab("Always wear a mask proportion") + scale_x_date("", date_breaks = "1 month", date_labels = "%b") +  
    scale_color_manual("Number of contacts", values = colors_bark[c(1,2,3,4)]) + 
    ggtitle("Mask use by number of contacts in 24 hours")
  
  # tab_sr <- aggregate(cbind(N, mask_use) ~ super_region_name + date, data = df, function(x) sum(x))
  # tab_sr$proportion <- tab_sr$mask_use / tab_sr$N
  # ggplot(tab_sr[tab_sr$super_region_name %like% "Middle", ], aes(x=date, y=proportion)) + geom_line() +
  #   theme_minimal() + ggtitle("North Africa and Middle East", subtitle = "Eid al Fitr May 23") +
  #   geom_vline(xintercept = as.Date("2020-05-23"), lty=2)
dev.off()
