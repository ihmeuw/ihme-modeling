library(data.table)
library(tidyverse)
library(dplyr)
library(ggplot2)
library(formattable)
library(gridExtra)
library(ggpubr)
library(forestplot)

# reading in data
# NON OMICRON INFECTIONS
# asta
astrainf_non_omi <- fread("FILEPATH/astrainf_non_omi.csv")
ast_inf_104w_non_omi <- fread("FILEPATH/ast_inf_104w_non_omi.csv")
# cor_sino
cor_sino_inf_104w_non_omi <- fread("FILEPATH/cor_sino_inf_104w_non_omi.csv")
cor_sino_inf_non_omi <- fread("FILEPATH/cor_sino_inf_non_omi.csv")
# jj
jj_inf_104w_non_omi <- fread("FILEPATH/jj_inf_104w_non_omi.csv")
jjinf_non_omi <- fread("FILEPATH/jjinf_non_omi.csv")
# moderna
mod_inf_104w_non_omi <- fread("FILEPATH/mod_inf_104w_non_omi.csv")
modinf_non_omi <- fread("FILEPATH/modinf_non_omi.csv")
# pfizer
pfizer_inf_104w_non_omi <- fread("FILEPATH/pfizer_inf_104w_non_omi.csv")
pfizinf_non_omi <- fread("FILEPATH/pfizinf_non_omi.csv")

# OMICRON INFECTIONS
# astra
astrainf_omi <- fread("FILEPATH/astrainf_omi.csv")
ast_inf_104w_omi <- fread("FILEPATH/ast_inf_104w_omi.csv")
# cor_sino
cor_sino_inf_104w_omi <- fread("FILEPATH/cor_sino_inf_104w_omi.csv")
cor_sino_inf_omi <- fread("FILEPATH/cor_sino_inf_omi.csv")
# jj
jj_inf_104w_omi <- fread("FILEPATH/jj_inf_104w_omi.csv")
jjinf_omi <- fread("FILEPATH/jjinf_omi.csv")
# moderna
mod_inf_104w_omi <- fread("FILEPATH/mod_inf_104w_omi.csv")
modinf_omi <- fread("FILEPATH/modinf_omi.csv")
# pfizer
pfizer_inf_104w_omi <- fread("FILEPATH/pfizer_inf_104w_omi.csv")
pfizinf_omi <- fread("FILEPATH/pfizinf_omi.csv")

# NON OMICRON SEVERE
# asta
astrasev_non_omi <- fread("FILEPATH/astrasev_non_omi.csv")
ast_sev_104w_non_omi <- fread("FILEPATH/ast_sev_104w_non_omi.csv")
# cor_sino
cor_sino_sev_104w_non_omi <- fread("FILEPATH/cor_sino_sev_104w_non_omi.csv")
cor_sino_sev_non_omi <- fread("FILEPATH/cor_sino_sev_non_omi.csv")
# jj
jj_sev_104w_non_omi <- fread("FILEPATH/jj_sev_104w_non_omi.csv")
jjsev_non_omi <- fread("FILEPATH/jjsev_non_omi.csv")
# moderna
mod_sev_104w_non_omi <- fread("FILEPATH/mod_sev_104w_non_omi.csv")
modsev_non_omi <- fread("FILEPATH/modsev_non_omi.csv")
# pfizer
pfizer_sev_104w_non_omi <- fread("FILEPATH/pfizer_sev_104w_non_omi.csv")
pfizsev_non_omi <- fread("FILEPATH/pfizsev_non_omi.csv")

# OMICRON SEVERE
# astra
astrasev_omi <- fread("FILEPATH/astrasev_omi.csv")
ast_sev_104w_omi <- fread("FILEPATH/ast_sev_104w_omi.csv")
# cor_sino
cor_sino_sev_104w_omi <- fread("FILEPATH/cor_sino_sev_104w_omi.csv")
cor_sino_sev_omi <- fread("FILEPATH/cor_sino_sev_omi.csv")
# pfizer
pfizer_sev_104w_omi <- fread("FILEPATH/pfizer_sev_104w_omi.csv")
pfizsev_omi <- fread("FILEPATH/pfizsev_omi.csv")






f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

# NON_OMICRON INFECTIONS

infection_non_omicron <- ggplot() + 
  # pfizer lines 
  geom_line(data = subset(pfizer_inf_104w_non_omi, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(pfizer_inf_104w_non_omi, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 2) +
  # pfizer points
  geom_point(data = pfizinf_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  # moderna lines 
  geom_line(data = subset(mod_inf_104w_non_omi, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mod_inf_104w_non_omi, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 2) +
  # moderna points
  geom_point(data = modinf_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
  # astra lines 
  geom_line(data = subset(ast_inf_104w_non_omi, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1, linetype = 1) +
  geom_line(data = subset(ast_inf_104w_non_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1, linetype = 2) +
  # astra points
  geom_point(data = astrainf_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
  # jj lines 
  geom_line(data = subset(jj_inf_104w_non_omi, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
            size = 1, linetype = 1) +
  geom_line(data = subset(jj_inf_104w_non_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
            size = 1, linetype = 2) +
  # jj points
  geom_point(data = jjinf_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", size = in_var)) +
  # corona sac lines 
  geom_line(data = subset(cor_sino_inf_104w_non_omi, mid_point < 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
            size = 1, linetype = 1) +
  geom_line(data = subset(cor_sino_inf_104w_non_omi, mid_point > 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
            size = 1, linetype = 2) +
  # corona sac points
  geom_point(data = cor_sino_inf_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", size = in_var)) +
  #############################################################################
scale_size_continuous(range = c(0.3,3.5)) +
scale_color_manual(values=c("Pfizer & BioNTech" = "purple", 
                            "Moderna" = "blue", 
                            "AstraZeneca" = "orange",
                            "Johnson & Johnson" = "green4",
                            "CoronaVac/SinoVac" = "red")
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

print(infection_non_omicron)


# OMI INFECTIONS 
infection_omi <- ggplot() + 
  # pfizer lines (midpoint max = 38.5)
  geom_line(data = subset(pfizer_inf_104w_omi, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(pfizer_inf_104w_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 2) +
  # pfizer points
  geom_point(data = pfizinf_omi, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  # moderna lines (midpoint max = 44.5)
  geom_line(data = subset(mod_inf_104w_omi, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mod_inf_104w_omi, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 2) +
  # moderna points
  geom_point(data = modinf_omi, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
  # # astra lines (midpoint max = 30)
  geom_line(data = subset(ast_inf_104w_omi, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1, linetype = 1) +
  geom_line(data = subset(ast_inf_104w_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1, linetype = 2) +
  # astra points
  geom_point(data = astrainf_omi, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
############################################################################
scale_size_continuous(range = c(0.3,3.5)) +
scale_color_manual(values=c("Pfizer & BioNTech" = "purple", 
                            "Moderna" = "blue", 
                            "AstraZeneca" = "orange")
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
print(infection_omi)


# NON_OMICRON SEVERE
severe_non_omicron <- ggplot() + 
  # pfizer lines 
  geom_line(data = subset(pfizer_sev_104w_non_omi, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(pfizer_sev_104w_non_omi, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 2) +
  # pfizer points
  geom_point(data = pfizsev_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  # moderna lines 
  geom_line(data = subset(mod_sev_104w_non_omi, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mod_sev_104w_non_omi, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 2) +
  # moderna points
  geom_point(data = modsev_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
  # astra lines 
  geom_line(data = subset(ast_sev_104w_non_omi, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1, linetype = 1) +
  geom_line(data = subset(ast_sev_104w_non_omi, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1, linetype = 2) +
  # astra points
  geom_point(data = astrasev_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
  # jj lines 
  geom_line(data = subset(jj_sev_104w_non_omi, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
            size = 1, linetype = 1) +
  geom_line(data = subset(jj_sev_104w_non_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
            size = 1, linetype = 2) +
  # jj points
  geom_point(data = jjsev_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", size = in_var)) +
  # corona sac lines 
  geom_line(data = subset(cor_sino_sev_104w_non_omi, mid_point < 33), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
            size = 1, linetype = 1) +
  geom_line(data = subset(cor_sino_sev_104w_non_omi, mid_point > 33), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
            size = 1, linetype = 2) +
  # corona sac points
  geom_point(data = cor_sino_sev_non_omi, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", size = in_var)) +
  #############################################################################
  scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c("Pfizer & BioNTech" = "purple", 
                              "Moderna" = "blue", 
                              "AstraZeneca" = "orange",
                              "Johnson & Johnson" = "green4",
                              "CoronaVac/SinoVac" = "red")
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

print(severe_non_omicron)


# OMI SEVERE 

severe_omi <- ggplot() + 
  # pfizer lines (midpoint max = 38.5)
  geom_line(data = subset(pfizer_sev_104w_omi, mid_point < 36), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(pfizer_sev_104w_omi, mid_point > 36), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 2) +
  # pfizer points
  geom_point(data = pfizsev_omi, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  # # astra lines (midpoint max = 30)
  geom_line(data = subset(ast_sev_104w_omi, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1, linetype = 1) +
  geom_line(data = subset(ast_sev_104w_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1, linetype = 2) +
  # astra points
  geom_point(data = astrasev_omi, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
  # corona sac lines 
  geom_line(data = subset(cor_sino_sev_104w_omi, mid_point < 33), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
            size = 1, linetype = 1) +
  geom_line(data = subset(cor_sino_sev_104w_omi, mid_point > 33), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
            size = 1, linetype = 2) +
  # corona sac points
  geom_point(data = cor_sino_sev_omi, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", size = in_var)) +
  ############################################################################
scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c("Pfizer & BioNTech" = "purple", 
                              "AstraZeneca" = "orange",
                              "CoronaVac/SinoVac" = "red")
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
print(severe_omi)

################################################################################

grid.arrange(infection_non_omicron, infection_omi, severe_non_omicron, severe_omi, nrow = 2, 
             top = textGrob("Vaccine Wanning Immunity (Infections)", gp= gpar(fontsize = 20)),
             bottom = textGrob("Vaccine Wanning Immunity (Severe)", gp= gpar(fontsize = 20)))
