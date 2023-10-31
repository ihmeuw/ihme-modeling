library(data.table)
library(tidyverse)
library(dplyr)
library(ggplot2)
library(formattable)
library(gridExtra)
library(forestplot)
library(ggpubr)
library(grid)

# load data
# original ancestral/omicron data
inf_ancest_omi <- fread("FILEPATH/inf_ancest_omi.csv")
past_inf_ancest_omi <- fread("FILEPATH/past_inf_ancest_omi.csv")

# adjusting e_mean values ancestral/omicron data
inf_ancest_omi_adjusted <- fread("FILEPATH/inf_ancest_omi_adjusted.csv")
past_inf_ancest_omi_adjusted <- fread("FILEPATH/past_inf_ancest_omi_adjusted.csv")


f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

################################################################################
# omicron infections current variant
infection_ancest_omi_vs_adjusted_variants <- ggplot() + 
  # natural immunity lines ancestral/omi original data
  geom_line(data = subset(past_inf_ancest_omi, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron Original"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_omi, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron Original"), 
            size = 1, linetype = 2) +
  # # natural immunity points
  geom_point(data = inf_ancest_omi, mapping = aes(x = mid_point, y = efficacy_mean, color = "Ancestral/Omicron Original", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################
# natural immunity lines ancestral/omi adjusted 
geom_line(data = subset(past_inf_ancest_omi_adjusted, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron Adjusted"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_omi_adjusted, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron Adjusted"), 
            size = 1, linetype = 2) +
  # # natural immunity points
  geom_point(data = inf_ancest_omi_adjusted, mapping = aes(x = mid_point, y = efficacy_mean, color = "Ancestral/Omicron Adjusted", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################


scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c(
    "Ancestral/Omicron Original" = 'red2',
    "Ancestral/Omicron Adjusted" = 'blue')
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

print(infection_ancest_omi_vs_adjusted_variants)