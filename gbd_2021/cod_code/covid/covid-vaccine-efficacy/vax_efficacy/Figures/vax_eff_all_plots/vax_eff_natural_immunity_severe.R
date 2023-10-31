library(data.table)
library(tidyverse)
library(dplyr)
library(ggplot2)
library(formattable)
library(gridExtra)
library(forestplot)
library(ggpubr)
library(grid)

# SEVERE NATURAL IMMUNITY PLOTS
# Importing data
sev_ancest_ancest <- fread("FILEPATH/sev_ancest_ancest.csv")
sev_ancest_delta <- fread("FILEPATH/sev_ancest_delta.csv")
sev_ancest_omi <- fread("FILEPATH/sev_ancest_omi.csv")

past_sev_ancest_ancest <- fread("FILEPATH/past_sev_ancest_ancest.csv")
past_sev_ancest_delta <- fread("FILEPATH/past_sev_ancest_delta.csv")
past_sev_ancest_omi <- fread("FILEPATH/past_sev_ancest_omi.csv")

################################################################################
f1 <- "Times"
shp_col <- c("#87b35c","#e22527")
################################################################################

# ancestral severe current variant
severe_ancest <- ggplot() + 
  # natural immunity lines 
  geom_line(data = subset(past_sev_ancest_ancest, mid_point < 43), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Ancestral"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_sev_ancest_ancest, mid_point > 43), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Ancestral"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = sev_ancest_ancest, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Ancestral", size = insesqua), 
             show.legend = FALSE) +
  scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c(
    "Ancestral/Ancestral" = 'red2')
  ) +
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Ancestral)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(severe_ancest)

################################################################################

# delta severe current variant
severe_delta <- ggplot() + 
  # natural immunity lines 
  geom_line(data = subset(past_sev_ancest_delta, mid_point < 39), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Delta"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_sev_ancest_delta, mid_point > 39), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Delta"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = sev_ancest_delta, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Delta", size = insesqua), 
             show.legend = FALSE) +
  scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c(
    "Ancestral/Delta" = 'red2')
  ) +
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Delta)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(severe_delta)

################################################################################

# omicron severe current variant
severe_omi <- ggplot() + 
  # natural immunity lines 
  geom_line(data = subset(past_sev_ancest_omi, mid_point < 58), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_sev_ancest_omi, mid_point > 58), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = sev_ancest_omi, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Omicron", size = insesqua), 
             show.legend = FALSE) +
  scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c(
    "Ancestral/Omicron" = 'red2')
  ) +
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(severe_omi)

################################################################################
# COMBINING PLOTS

grid.arrange(severe_ancest, severe_delta, severe_omi, nrow = 1, 
             top = textGrob("Severe Wanning Immunity", gp= gpar(fontsize = 20)))
