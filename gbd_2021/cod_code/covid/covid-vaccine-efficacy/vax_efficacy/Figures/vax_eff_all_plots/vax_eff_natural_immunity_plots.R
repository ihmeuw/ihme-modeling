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
# ancestral current variant
inf_ancest_ancest <- fread("FILEPATH/inf_ancest_ancest.csv")
past_inf_ancest_ancest <- fread("FILEPATH/past_inf_ancest_ancest.csv")
# alpha current variant
inf_ancest_alpha <- fread("FILEPATH/inf_ancest_alpha.csv")
past_inf_ancest_alpha <- fread("FILEPATH/past_inf_ancest_alpha.csv")
# beta current variant
inf_ancest_beta <- fread("FILEPATH/inf_ancest_beta.csv")
past_inf_ancest_beta <- fread("FILEPATH/past_inf_ancest_beta.csv")
# delta current variant
inf_ancest_delta <- fread("FILEPATH/inf_ancest_delta.csv")
past_inf_ancest_delta <- fread("FILEPATH/past_inf_ancest_delta.csv")
# omicron current variant
inf_ancest_omi <- fread("FILEPATH/inf_ancest_omi.csv")
inf_omi_omi <- fread("FILEPATH/inf_omi_omi.csv")
past_inf_ancest_omi <- fread("FILEPATH/past_inf_ancest_omi.csv")
past_inf_omi_omi <- fread("FILEPATH/past_inf_omi_omi.csv")

################################################################################
# INFECTION NATURAL IMMUNITY PLOTS
################################################################################

f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

# ancestral infections current variant
infection_ancest <- ggplot() + 
# natural immunity lines 
  geom_line(data = subset(past_inf_ancest_ancest, mid_point < 78), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Ancestral"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_ancest, mid_point > 78), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Ancestral"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_ancest_ancest, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Ancestral", size = insesqua), 
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

print(infection_ancest)

################################################################################
# INFECTIONS ALPHA

# alpha infections current variant
infection_alpha <- ggplot() + 
  # natural immunity lines 
  geom_line(data = subset(past_inf_ancest_alpha, mid_point < 45), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Alpha"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_alpha, mid_point > 45), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Alpha"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_ancest_alpha, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Alpha", size = insesqua), 
             show.legend = FALSE) +
  scale_size_continuous(range = c(0.3,3.5)) +
    scale_color_manual(values=c(
      "Ancestral/Alpha" = 'red2')
    ) +
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Alpha)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(infection_alpha)

# beta infections current variant
infection_beta <- ggplot() + 
  # natural immunity lines 
  geom_line(data = subset(past_inf_ancest_beta, mid_point < 45), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Beta"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_beta, mid_point > 45), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Beta"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_ancest_beta, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Beta", size = insesqua), 
             show.legend = FALSE) +
scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c(
    "Ancestral/Beta" = 'red2')
  ) +
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Beta)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(infection_beta)

################################################################################
# delta infections current variant
infection_delta <- ggplot() + 
  # natural immunity lines
  geom_line(data = subset(past_inf_ancest_delta, mid_point < 64), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Delta"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_delta, mid_point > 64), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Delta"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_ancest_delta, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Delta", size = insesqua), 
             show.legend = FALSE) +
  #############################################################################3
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

print(infection_delta)

################################################################################
# omicron infections current variant
infection_omi <- ggplot() + 
  # natural immunity lines 
  geom_line(data = subset(past_inf_ancest_omi, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_omi, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "Ancestral/Omicron"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_ancest_delta, mapping = aes(x = mid_point, y = e_mean, color = "Ancestral/Omicron", size = insesqua), 
             show.legend = FALSE) +
  ##############################################################################
# natural immunity lines 
geom_line(data = subset(past_inf_omi_omi, mid_point < 18), mapping = aes(x = mid_point, y = pinf, color = "Omicron/Omicron"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_omi_omi, mid_point > 18), mapping = aes(x = mid_point, y = pinf, color = "Omicron/Omicron"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_omi_omi, mapping = aes(x = mid_point, y = e_mean, color = "Omicron/Omicron", size = insesqua), 
             show.legend = FALSE) +
  ##############################################################################
  scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c(
    "Ancestral/Omicron" = 'red2',
    "Omicron/Omicron" = 'blue2')
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

print(infection_omi)


################################################################################
# COMBINING PLOTS

grid.arrange(infection_ancest, infection_alpha, infection_beta, infection_delta, infection_omi, nrow = 2, 
             top = textGrob("Infection Wanning Immunity", gp= gpar(fontsize = 20)))
