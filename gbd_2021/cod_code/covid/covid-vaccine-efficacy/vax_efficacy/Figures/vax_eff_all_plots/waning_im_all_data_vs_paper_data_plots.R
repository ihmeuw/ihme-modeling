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
# infection
inf_all_data_5knots_lt_true <- fread("FILEPATH/inf_all_data_5knots_lt_true.csv")
past_inf_all_data_5knots_lt_true <- fread("FILEPATH/past_inf_all_data_5knots_lt_true.csv")

inf_all_data_8knots_lt_true <- fread("FILEPATH/inf_all_data_8knots_lt_true.csv")
past_inf_all_data_8knots_lt_true <- fread("FILEPATH/past_inf_all_data_8knots_lt_true.csv")

# severe
sev_all_data_5knots_lt_true <- fread("FILEPATH/sev_all_data_5knots_lt_true.csv")
past_sev_all_data_5knots_lt_true <- fread("FILEPATH/past_sev_all_data_5knots_lt_true.csv")

sev_all_data_8knots_lt_true <- fread("FILEPATH/sev_all_data_8knots_lt_true.csv")
past_sev_all_data_8knots_lt_true <- fread("FILEPATH/past_sev_all_data_8knots_lt_true.csv")

# paper data
past_inf <- fread("FILEPATH/past_inf.csv")
past_infection <- fread("FILEPATH/past_infection.csv")

past_inf_o <- fread("FILEPATH/past_inf_o.csv")
past_infection_o <- fread("FILEPATH/past_infection_o.csv")


f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

################################################################################
# infections all data 5 knots lines
infection_all_data_knots_lt <- ggplot() + 
  # infections all data 5 knots lines
  geom_line(data = subset(past_inf_all_data_5knots_lt_true, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "5 knots/linear tail = T"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_all_data_5knots_lt_true, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "5 knots/linear tail = T"), 
            size = 1, linetype = 2) +
  # nfections all data 5 knots points
  geom_point(data = inf_all_data_5knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "5 knots/linear tail = T", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################
  #  infections all data 8 knots lines
  geom_line(data = subset(past_inf_all_data_8knots_lt_true, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "8 knots/linear tail = T"), 
            size = 1, linetype = 1) +
  #  infections all data 8 knots lines
  geom_line(data = subset(past_inf_all_data_8knots_lt_true, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "8 knots/linear tail = T"), 
            size = 1, linetype = 2) +
  #  infections all data 8 knots points
  geom_point(data = inf_all_data_8knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "8 knots/linear tail = T", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################
  # waning natural immunity from paper past infections non omicron lines 
  geom_line(data = subset(past_inf, mid_point < 78), mapping = aes(x = mid_point, y = pinf, color = "Infections Non-Omicron (paper data)"), 
            size = 1, linetype = 1) +
  # waning natural immunity from paper past infections non omicron lines 
  geom_line(data = subset(past_inf, mid_point > 78), mapping = aes(x = mid_point, y = pinf, color = "Infections Non-Omicron (paper data)"), 
            size = 1, linetype = 2) +
  # waning natural immunity from paper past infections non omicron points 
  geom_point(data = past_infection, mapping = aes(x = mid_point, y = efficacy_mean, color = "Infections Non-Omicron (paper data)", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################
  # waning natural immunity from paper past infections omicron lines
  geom_line(data = subset(past_inf_o, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "Infections Omicron (paper data)"),
            size = 1, linetype = 1) +
  # waning natural immunity from paper past infections omicron lines
  geom_line(data = subset(past_inf_o, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "Infections Omicron (paper data)"),
            size = 1, linetype = 2) +
  # waning natural immunity from paper past infections omicron points
  geom_point(data = past_infection_o, mapping = aes(x = mid_point, y = efficacy_mean, color = "Infections Omicron (paper data)", size = 0.5),
             show.legend = FALSE) +
  ##############################################################################

  scale_size_continuous(range = c(0.3,3.5)) +
    scale_color_manual(values=c(
      "5 knots/linear tail = T" = 'red2',
      "8 knots/linear tail = T" = 'blue',
      "Infections Non-Omicron (paper data)" = '#ABA300',
      "Infections Omicron (paper data)" = 'green4')
    ) +
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Infections All Data)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(infection_all_data_knots_lt)

################################################################################
# SEVERE
################################################################################
# severe all data 5 knots lines
severe_all_data_knots <- ggplot() + 
  # infections all data 5 knots lines
  geom_line(data = subset(past_sev_all_data_5knots_lt_true, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "5 knots/linear tail = T"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_sev_all_data_5knots_lt_true, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "5 knots/linear tail = T"), 
            size = 1, linetype = 2) +
  # nfections all data 5 knots points
  geom_point(data = sev_all_data_5knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "5 knots/linear tail = T", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################
  #  infections all data 8 knots lines
  geom_line(data = subset(past_sev_all_data_8knots_lt_true, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "8 knots/linear tail = T"), 
            size = 1, linetype = 1) +
  #  infections all data 8 knots lines
  geom_line(data = subset(past_sev_all_data_8knots_lt_true, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "8 knots/linear tail = T"), 
            size = 1, linetype = 2) +
  #  infections all data 8 knots points
  geom_point(data = sev_all_data_8knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "8 knots/linear tail = T", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################

scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c(
    "5 knots/linear tail = T" = 'red2',
    "8 knots/linear tail = T" = 'blue')
  ) +
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Severe All Data)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(severe_all_data_knots)

################################################################################

grid.arrange(infection_all_data_knots_lt, severe_all_data_knots, nrow = 2)

