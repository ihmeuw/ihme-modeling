library(data.table)
library(tidyverse)
library(dplyr)
library(ggplot2)
library(formattable)
library(gridExtra)
library(ggpubr)
library(forestplot)

# reading in data
# mRNA infections non omicron
mRNA_vaxes_inf_non_omi <- fread("FILEPATH/mRNA_vaxes_inf_non_omi.csv")
mRNA_vaxes_inf_104w_non_omi <- fread("FILEPATH/mRNA_vaxes_inf_104w_non_omi.csv")
# non mRNA non omicron
non_mRNA_vaxes_inf_non_omi <- fread("FILEPATH/non_mRNA_vaxes_inf_non_omi.csv")
non_mRNA_vaxes_inf_104w_non_omi <- fread("FILEPATH/non_mRNA_vaxes_inf_104w_non_omi.csv")
# mRNA infection omicron
mRNA_vaxes_inf_omi <- fread("FILEPATH/mRNA_vaxes_inf_omi.csv")
mRNA_vaxes_inf_104w_omi <- fread("FILEPATH/mRNA_vaxes_inf_104w_omi.csv")
# non mRNA infections omicron
# non_mRNA_vaxes_inf_104w_omi <- fread("FILEPATH/non_mRNA_vaxes_inf_104w_omi.csv")
# non_mRNA_vaxes_inf_omi <- fread("FILEPATH/non_mRNA_vaxes_inf_omi.csv")


# mRNA severe non omicron
mRNA_vaxes_sev_non_omi <- fread("FILEPATH/mRNA_vaxes_sev_non_omi.csv")
mRNA_vaxes_sev_104w_non_omi <- fread("FILEPATH/mRNA_vaxes_sev_104w_non_omi.csv")
# non mRNA severe non omicron
non_mRNA_vaxes_sev_non_omi <- fread("FILEPATH/non_mRNA_vaxes_sev_non_omi.csv")
non_mRNA_vaxes_sev_104w_non_omi <- fread("FILEPATH/non_mRNA_vaxes_sev_104w_non_omi.csv")
# mRNA severe omicron
mRNA_vaxes_sev_omi <- fread("FILEPATH/mRNA_vaxes_sev_omi.csv")
mRNA_vaxes_sev_104w_omi <- fread("FILEPATH/mRNA_vaxes_sev_104w_omi.csv")
# non mRNA severe omicron
non_mRNA_vaxes_sev_104w_omi <- fread("FILEPATH/non_mRNA_vaxes_sev_104w_omi.csv")
non_mRNA_vaxes_sev_omi <- fread("FILEPATH/non_mRNA_vaxes_sev_omi.csv")






f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

################################################################################
# NON_OMICRON INFECTIONS
################################################################################

Vaccines_infection_non_omicron <- ggplot() + 
  # mRNA vaccines lines
  geom_line(data = subset(mRNA_vaxes_inf_104w_non_omi, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mRNA_vaxes_inf_104w_non_omi, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # mRNA vaccines points
  geom_point(data = mRNA_vaxes_inf_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "mRNA Vaccines", size = in_var)) + #shape = variant,
  
  # non mRNA vaccines lines 
  geom_line(data = subset(non_mRNA_vaxes_inf_104w_non_omi, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(non_mRNA_vaxes_inf_104w_non_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # non mRNA vaccines points
  geom_point(data = non_mRNA_vaxes_inf_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "Non-mRNA Vaccines", size = in_var)) +
 
scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c("mRNA Vaccines" = "purple", 
                              "Non-mRNA Vaccines" = "green4")
  ) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Non-Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(Vaccines_infection_non_omicron)

################################################################################
# OMICRON INFECTIONS
################################################################################

Vaccines_infection_omicron <- ggplot() + 
  # mRNA vaccines lines
  geom_line(data = subset(mRNA_vaxes_inf_104w_omi, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mRNA_vaxes_inf_104w_omi, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # mRNA vaccines points
  geom_point(data = mRNA_vaxes_inf_omi, mapping = aes(x = mid_point, y = e_mean, color = "mRNA Vaccines", size = in_var)) + #shape = variant,
  
  # non mRNA vaccines lines 
  # geom_line(data = subset(non_mRNA_vaxes_inf_104w_omi, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
  #           size = 1, linetype = 1) +
  # geom_line(data = subset(non_mRNA_vaxes_inf_104w_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
  #           size = 1, linetype = 2) +
  # # non mRNA vaccines points
  # geom_point(data = non_mRNA_vaxes_inf_omi, mapping = aes(x = mid_point, y = e_mean, color = "Non-mRNA Vaccines", size = in_var)) +
  
scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c("mRNA Vaccines" = "purple")
  ) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(Vaccines_infection_omicron)

################################################################################
# NON OMICRON SEVERE 
################################################################################

Vaccines_severe_non_omicron <- ggplot() + 
  # mRNA vaccines lines
  geom_line(data = subset(mRNA_vaxes_sev_104w_non_omi, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mRNA_vaxes_sev_104w_non_omi, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # mRNA vaccines points
  geom_point(data = mRNA_vaxes_sev_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "mRNA Vaccines", size = in_var)) + #shape = variant,
  
  # non mRNA vaccines lines 
  geom_line(data = subset(non_mRNA_vaxes_sev_104w_non_omi, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(non_mRNA_vaxes_sev_104w_non_omi, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # non mRNA vaccines points
  geom_point(data = non_mRNA_vaxes_sev_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "Non-mRNA Vaccines", size = in_var)) +
  
scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c("mRNA Vaccines" = "purple", 
                              "Non-mRNA Vaccines" = "green4")
  ) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Non-Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(Vaccines_severe_non_omicron)

################################################################################
# OMICRON SEVERE 
################################################################################

Vaccines_severe_omicron <- ggplot() + 
  # mRNA vaccines lines
  geom_line(data = subset(mRNA_vaxes_sev_104w_omi, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mRNA_vaxes_sev_104w_omi, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # mRNA vaccines points
  geom_point(data = mRNA_vaxes_sev_omi, mapping = aes(x = mid_point, y = e_mean, color = "mRNA Vaccines", size = in_var)) + #shape = variant,
  
  # non mRNA vaccines lines 
  geom_line(data = subset(non_mRNA_vaxes_sev_104w_omi, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(non_mRNA_vaxes_sev_104w_omi, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # non mRNA vaccines points
  geom_point(data = non_mRNA_vaxes_sev_omi, mapping = aes(x = mid_point, y = e_mean, color = "Non-mRNA Vaccines", size = in_var)) +
  
scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c("mRNA Vaccines" = "purple", 
                              "Non-mRNA Vaccines" = "green4")
  ) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity (Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(Vaccines_severe_omicron)

################################################################################
# COMBINING PLOTS
################################################################################

grid.arrange(Vaccines_infection_non_omicron, Vaccines_infection_omicron, 
             Vaccines_severe_non_omicron, Vaccines_severe_omicron, nrow = 2, 
             top = textGrob("Vaccine Wanning Immunity (Infections)", gp= gpar(fontsize = 20)),
             bottom = textGrob("Vaccine Wanning Immunity (Severe)", gp= gpar(fontsize = 20)))
