library(data.table)
library(tidyverse)
library(dplyr)
library(ggplot2)
library(formattable)
library(gridExtra)
library(forestplot)
library(ggpubr)
library(grid)

# Importing in data
# infections
inf_ancest_omi <- fread("FILEPATH/inf_ancest_omi.csv")
inf_omi_omi <- fread("FILEPATH/inf_omi_omi.csv")

past_inf_ancest_omi <- fread("FILEPATH/past_inf_ancest_omi.csv")
past_inf_omi_omi <- fread("FILEPATH/past_inf_omi_omi.csv")

inf_ancest_non_omi <- fread("FILEPATH/inf_ancest_non_omi.csv")
past_inf_ancest_non_omi <- fread("FILEPATH/past_inf_ancest_non_omi.csv")

# severe
past_sev_ancest_omi <- fread("FILEPATH/past_sev_ancest_omi.csv")
sev_ancest_non_omi <- fread("FILEPATH/sev_ancest_non_omi.csv")
past_sev_ancest_non_omi <- fread("FILEPATH/past_sev_ancest_non_omi.csv")
sev_ancest_omi <- fread("FILEPATH/sev_ancest_omi.csv")

# all data curve data
inf_all_data <- fread("FILEPATH/inf_all_data.csv")
past_inf_all_data <- fread("FILEPATH/past_inf_all_data.csv")

sev_all_data <- fread("FILEPATH/sev_all_data.csv")
past_sev_all_data <- fread("FILEPATH/past_sev_all_data.csv")

################################################################################
# INFECTION NATURAL IMMUNITY PLOTS
################################################################################

f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

################################################################################
# omicron infections current variant
infection_all_variants <- ggplot() + 
  # natural immunity lines ancestral/omi
  geom_line(data = subset(past_inf_ancest_omi, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_omi, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_ancest_omi, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Omicron", size = insesqua), 
             show.legend = FALSE) +
  ##############################################################################
  # natural immunity lines acestral omi/omi
  geom_line(data = subset(past_inf_omi_omi, mid_point < 18), mapping = aes(x = mid_point, y = pinf, color = "Omicron/Omicron"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_omi_omi, mid_point > 18), mapping = aes(x = mid_point, y = pinf, color = "Omicron/Omicron"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_omi_omi, mapping = aes(x = mid_point, y = e_mean, color = "Omicron/Omicron", size = insesqua), 
             show.legend = FALSE) +
  ##############################################################################
  # natural immunity lines ancestral/non-omi
  geom_line(data = subset(past_inf_ancest_non_omi, mid_point < 18), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Non-Omicron (includes Ancestral)"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_non_omi, mid_point > 18), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Non-Omicron (includes Ancestral)"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_ancest_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Non-Omicron (includes Ancestral)", size = insesqua), 
             show.legend = FALSE) +
# all data curve
  geom_line(data = subset(past_inf_all_data, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "All Data"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_all_data, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "All Data"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_all_data, mapping = aes(x = mid_point, y = e_mean, color = "All Data", size = insesqua), 
             show.legend = FALSE) +

scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c(
    "Ancestral/Omicron" = 'red2',
    "Omicron/Omicron" = 'purple',
    "Ancestral/Non-Omicron (includes Ancestral)" = '#ABA300',
    "All Data" = 'green4')
  ) +
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Infections)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(infection_all_variants)

################################################################################
# SEVERE PLOTS
################################################################################

severe_all_variants <- ggplot() + 
  # natural immunity lines ancestral/omi
  geom_line(data = subset(past_sev_ancest_omi, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_sev_ancest_omi, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = sev_ancest_omi, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Omicron", size = insesqua), 
             show.legend = FALSE) +
  ##############################################################################
# natural immunity lines acestral omi/omi
  geom_line(data = subset(past_sev_ancest_non_omi, mid_point < 18), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Non-Omicron"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_sev_ancest_non_omi, mid_point > 18), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Non-Omicron"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = sev_ancest_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Non-Omicron", size = insesqua), 
             show.legend = FALSE) +
  ##############################################################################
# all data curve
geom_line(data = subset(past_sev_all_data, mid_point < 58), mapping = aes(x = mid_point, y = pinf, color = "All Data"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_sev_all_data, mid_point > 58), mapping = aes(x = mid_point, y = pinf, color = "All Data"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = sev_all_data, mapping = aes(x = mid_point, y = e_mean, color = "All Data", size = insesqua), 
             show.legend = FALSE) +
  
  
  scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c(
    "Ancestral/Omicron" = 'red2',
    "Ancestral/Non-Omicron" = '#ABA300',
    "All Data" = 'green4')
  ) +
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Severe)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(severe_all_variants)









################################################################################
# COMBINING PLOTS

grid.arrange(infection_all_variants, severe_all_variants, nrow = 2, 
             top = textGrob("Wanning Immunity", gp= gpar(fontsize = 20)))
