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
# anc/omi adjusted data 5 knotes linear tail false
inf_ancest_omi_adjusted_5knots_lt_false <- fread("FILEPATH/inf_ancest_omi_adjusted_5knots_lt_false.csv")
past_inf_ancest_omi_adjusted_5knots_lt_false <- fread("FILEPATH/past_inf_ancest_omi_adjusted_5knots_lt_false.csv")

# anc/omi adjusted data 5 knotes linear tail true
inf_ancest_omi_adjusted_5knots_lt_true <- fread("FILEPATH/inf_ancest_omi_adjusted_5knots_lt_true.csv")
past_inf_ancest_omi_adjusted_5knots_lt_true <- fread("FILEPATH/past_inf_ancest_omi_adjusted_5knots_lt_true.csv")

# anc/omi adjusted data 8 knotes linear tail false
inf_ancest_omi_adjusted_8knots_lt_false <- fread("FILEPATH/inf_ancest_omi_adjusted_8knots_lt_false.csv")
past_inf_ancest_omi_adjusted_8knots_lt_false <- fread("FILEPATH/past_inf_ancest_omi_adjusted_8knots_lt_false.csv")

# anc/omi adjusted data 8 knotes linear tail true
inf_ancest_omi_adjusted_8knots_lt_true <- fread("FILEPATH/inf_ancest_omi_adjusted_8knots_lt_true.csv")
past_inf_ancest_omi_adjusted_8knots_lt_true <- fread("FILEPATH/past_inf_ancest_omi_adjusted_8knots_lt_true.csv")





f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

################################################################################
# omicron infections current variant
infection_ancest_omi_knots_lt <- ggplot() + 
  # natural immunity lines ancestral/omi 5 knots linear tail = F
  geom_line(data = subset(past_inf_ancest_omi_adjusted_5knots_lt_false, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "5 knots/linear tail = F"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_omi_adjusted_5knots_lt_false, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "5 knots/linear tail = F"), 
            size = 1, linetype = 2) +
  # # natural immunity points ancestral/omi 5 knots linear tail = F
  geom_point(data = inf_ancest_omi_adjusted_5knots_lt_false, mapping = aes(x = mid_point, y = efficacy_mean, color = "5 knots/linear tail = F", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################
# natural immunity lines ancestral/omi 5 knots linear tail = T 
geom_line(data = subset(past_inf_ancest_omi_adjusted_5knots_lt_true, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "5 knots/linear tail = T"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_omi_adjusted_5knots_lt_true, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "5 knots/linear tail = T"), 
            size = 1, linetype = 2) +
  # natural immunity points ancestral/omi 5 knots linear tail = T
  geom_point(data = inf_ancest_omi_adjusted_5knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "5 knots/linear tail = T", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################
# natural immunity lines ancestral/omi 8 knots linear tail = F 
geom_line(data = subset(past_inf_ancest_omi_adjusted_8knots_lt_false, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "8 knots/linear tail = F"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_omi_adjusted_8knots_lt_false, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "8 knots/linear tail = F"), 
            size = 1, linetype = 2) +
  # natural immunity points ancestral/omi 8 knots linear tail = F
  geom_point(data = inf_ancest_omi_adjusted_8knots_lt_false, mapping = aes(x = mid_point, y = efficacy_mean, color = "8 knots/linear tail = F", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################
# natural immunity lines ancestral/omi 8 knots linear tail = T 
geom_line(data = subset(past_inf_ancest_omi_adjusted_8knots_lt_true, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "8 knots/linear tail = T"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_ancest_omi_adjusted_8knots_lt_true, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "8 knots/linear tail = T"), 
            size = 1, linetype = 2) +
  # natural immunity points ancestral/omi 8 knots linear tail = T 
  geom_point(data = inf_ancest_omi_adjusted_8knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "8 knots/linear tail = T", size = insesqua),
             show.legend = FALSE) +
  ##############################################################################
  ##############################################################################


scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c(
    "5 knots/linear tail = F" = 'red2',
    "5 knots/linear tail = T" = 'blue',
    "8 knots/linear tail = F" = '#ABA300',
    "8 knots/linear tail = T" = 'green4')
  ) +
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Infections Ancestral/Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(infection_ancest_omi_knots_lt)
