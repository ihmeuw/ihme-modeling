library(data.table)
library(tidyverse)
library(dplyr)
library(ggplot2)
library(formattable)
library(gridExtra)
library(ggpubr)
library(forestplot)

# reading in all data
# INFECTIONS
# NATURAL IMMUNITY
# alpha
past_inf_alpha <- fread("FILEPATH/past_inf_alpha.csv")
inf_alpha <- fread("FILEPATH/inf_alpha.csv")
# beta
past_inf_beta <- fread("FILEPATH/past_inf_beta.csv")
inf_beta <- fread("FILEPATH/inf_beta.csv")
# delta
past_inf_delta <- fread("FILEPATH/past_inf_delta.csv")
inf_delta <- fread("FILEPATH/inf_delta.csv")
# omicron
past_inf_omi <- fread("FILEPATH/past_inf_omi.csv")
inf_omi <- fread("FILEPATH/inf_omi.csv")

# AstraZeneca
# alpha
ast_inf_104w_alpha <- fread("FILEPATH/ast_inf_104w_alpha.csv")
astrainf_alpha <- fread("FILEPATH/astrainf_alpha.csv")
# beta (no beta data)
# ast_inf_104w_beta <- fread("FILEPATH/ast_inf_104w_beta.csv")
# astrainf_beta <- fread("FILEPATH/astrainf_beta.csv")
# delta
ast_inf_104w_delta <- fread("FILEPATH/ast_inf_104w_delta.csv")
astrainf_delta <- fread("FILEPATH/astrainf_delta.csv")
# omicron
ast_inf_104w_omi <- fread("FILEPATH/ast_inf_104w_omi.csv")
astrainf_omi <- fread("FILEPATH/astrainf_omi.csv")

# CoronaVac/SinoVac
# alpha
cor_sino_inf_104w_alpha <- fread("FILEPATH/cor_sino_inf_104w_alpha.csv")
cor_sino_inf_alpha <- fread("FILEPATH/cor_sino_inf_alpha.csv")
# beta (no beta data)
# cor_sino_inf_104w_beta <- fread("FILEPATH/cor_sino_inf_104w_beta.csv")
# cor_sino_inf_beta <- fread("FILEPATH/cor_sino_inf_beta.csv")
# delta
cor_sino_inf_104w_delta <- fread("FILEPATH/cor_sino_inf_104w_delta.csv")
cor_sino_inf_delta <- fread("FILEPATH/cor_sino_inf_delta.csv")
# omicron
cor_sino_inf_104w_omi <- fread("FILEPATH/cor_sino_inf_104w_omi.csv")
cor_sino_inf_omi <- fread("FILEPATH/cor_sino_inf_omi.csv")

# Johnson & Johnson
# alpha
jj_inf_104w_alpha <- fread("FILEPATH/jj_inf_104w_alpha.csv")
jjinf_alpha <- fread("FILEPATH/jjinf_alpha.csv")
# beta (no beta data)
# jj_inf_104w_beta <- fread("FILEPATH/jj_inf_104w_beta.csv")
# jjinf_beta <- fread("FILEPATH/jjinf_beta.csv")
#delta
jj_inf_104w_delta <- fread("FILEPATH/jj_inf_104w_delta.csv")
jjinf_delta <- fread("FILEPATH/jjinf_delta.csv")
# omi
jj_inf_104w_omi <- fread("FILEPATH/jj_inf_104w_omi.csv")
jjinf_omi <- fread("FILEPATH/jjinf_omi.csv")

# Moderna
# alpha
mod_inf_104w_alpha <- fread("FILEPATH/mod_inf_104w_alpha.csv")
modinf_alpha <- fread("FILEPATH/modinf_alpha.csv")
# beta (no beta data)
# mod_inf_104w_beta <- fread("FILEPATH/mod_inf_104w_beta.csv")
# modinf_beta <- fread("FILEPATH/modinf_beta.csv")
# delta
mod_inf_104w_delta <- fread("FILEPATH/mod_inf_104w_delta.csv")
modinf_delta <- fread("FILEPATH/modinf_delta.csv")
# omi
mod_inf_104w_omi <- fread("FILEPATH/mod_inf_104w_omi.csv")
modinf_omi <- fread("FILEPATH/modinf_omi.csv")

# Pfizer & BioNTech
# alpha
pfizer_inf_104w_alpha <- fread("FILEPATH/pfizer_inf_104w_alpha.csv")
pfizinf_alpha <- fread("FILEPATH/pfizinf_alpha.csv")
# beta (no beta data)
# pfizer_inf_104w_beta <- fread("FILEPATH/pfizer_inf_104w_beta.csv")
# pfizinf_beta <- fread("FILEPATH/pfizinf_beta.csv")
# delta
pfizer_inf_104w_delta <- fread("FILEPATH/pfizer_inf_104w_delta.csv")
pfizinf_delta <- fread("FILEPATH/pfizinf_delta.csv")
# omi
pfizer_inf_104w_omi <- fread("FILEPATH/pfizer_inf_104w_omi.csv")
pfizinf_omi <- fread("FILEPATH/pfizinf_omi.csv")


###############################################################################
# plotting 
###############################################################################
# ALPHA

f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

# new linetype variable (DOESNT WORK FOR NOW)
# pfizer_sev_104w_line <- mutate(pfizer_sev_104w, line = ifelse(mid_point < 17, "dashed", "solid"))
# mod_sev_104_line <- mutate(mod_sev_104, line = ifelse(mid_point < 17, "dashed", "solid"))
# ast_sev_104w_line <- mutate(ast_sev_104w, line = ifelse(mid_point < 17, "dashed", "solid"))
# jj_sev_104_line <- mutate(jj_sev_104, line = ifelse(mid_point < 17, "dashed", "solid"))
# cor_sino_sev_104_line <- mutate(cor_sino_sev_104, line = ifelse(mid_point < 17, "dashed", "solid"))

# alpha infection Panel A1
infection_alpha <- ggplot() + 
  # pfizer lines (midpoint max = 38.5)
  geom_line(data = subset(pfizer_inf_104w_alpha, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(pfizer_inf_104w_alpha, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 2) +
  # pfizer points
  geom_point(data = pfizinf_alpha, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", alpha = (in_var*0.75))) + #shape = variant,
  # moderna lines (midpoint max = 44.5)
  geom_line(data = subset(mod_inf_104w_alpha, mid_point < 23), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mod_inf_104w_alpha, mid_point > 23), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 2) +
  # # moderna points 
  # geom_point(data = modinf_alpha, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
  # # astra lines (midpoint max = 30)
  # geom_line(data = subset(ast_inf_104w_alpha, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
  #           size = 1, linetype = 1) +
  # geom_line(data = subset(ast_inf_104w_alpha, mid_point > 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
  #           size = 1, linetype = 2) +
  # # astra points
  # geom_point(data = astrainf_alpha, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
  # # jj lines (midpoint max = 30)
  # geom_line(data = subset(jj_inf_104w_alpha, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
#           size = 1, linetype = 1) +
# geom_line(data = subset(jj_inf_104w_alpha, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
#           size = 1, linetype = 2) +
# # jj points
# geom_point(data = jjinf_alpha, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", size = in_var)) +
# # corona sac lines (midpoint max = 14.5)
# geom_line(data = subset(cor_sino_inf_104w_alpha, mid_point < 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), 
#           size = 1, linetype = 1) +
# geom_line(data = subset(cor_sino_inf_104w_alpha, mid_point > 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), 
#           size = 1, linetype = 2) +
# # corona sac points
# geom_point(data = cor_sino_inf_alpha, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", size = in_var)) +
#############################################################################
# natural immunity lines (midpoint = 86)
geom_line(data = subset(past_inf_alpha, mid_point < 34), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_alpha, mid_point > 34), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_alpha, mapping = aes(x = mid_point, y = e_mean, color = "Natural Immunity", alpha = (insesqua*0.75)), 
             show.legend = FALSE) +
  #############################################################################

scale_color_manual(values=c("Pfizer & BioNTech" = "purple", 
                            "Moderna" = "blue", 
                            # "AstraZeneca" = "orange", 
                            # "Johnson & Johnson" = "green4",
                            # "CoronaVac/SinoVac" = "red", 
                            "Natural Immunity" = '#ABA300')
) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  # geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  ggtitle("Waning Immunity (Alpha)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)
print(infection_alpha)

###############################################################################
# BETA
###############################################################################

# f1 <- "Times"
# shp_col <- c("#87b35c","#e22527")



infection_beta <- ggplot() + 
  # # pfizer lines (midpoint max = 38.5)
  # geom_line(data = subset(pfizer_sev_104w, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
  #           size = 1, linetype = 1) +
  # geom_line(data = subset(pfizer_sev_104w, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
  #           size = 1, linetype = 2) +
  # # pfizer points
  # geom_point(data = pfiz, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  # # moderna lines (midpoint max = 44.5)
  # geom_line(data = subset(mod_sev_104, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
  #           size = 1, linetype = 1) +
  # geom_line(data = subset(mod_sev_104, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
  #           size = 1, linetype = 2) +
  # # moderna points 
  # geom_point(data = mod, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
  # # astra lines (midpoint max = 30)
  # geom_line(data = subset(ast_sev_104w, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
  #           size = 1, linetype = 1) +
  # geom_line(data = subset(ast_sev_104w, mid_point > 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
  #           size = 1, linetype = 2) +
  # # astra points
  # geom_point(data = astra, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
  # # jj lines (midpoint max = 30)
  # geom_line(data = subset(jj_sev_104, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
  #           size = 1, linetype = 1) +
  # geom_line(data = subset(jj_sev_104, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
  #           size = 1, linetype = 2) +
  # # jj points
  # geom_point(data = jj, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", size = in_var)) +
  # # corona sac lines (midpoint max = 14.5)
  # geom_line(data = subset(cor_sino_sev_104, mid_point < 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), 
  #           size = 1, linetype = 1) +
  # geom_line(data = subset(cor_sino_sev_104, mid_point > 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), 
  #           size = 1, linetype = 2) +
  # # corona sac points
  # geom_point(data = cor_sino, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", size = in_var)) +
  #############################################################################
# natural immunity lines (midpoint = 86)
geom_line(data = subset(past_inf_beta, mid_point < 45), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_beta, mid_point > 45), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
            size = 1, linetype = 2) +
  # geom_line(data = past_inf_detla, mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), size = 1) +
  
  
  # natural immunity points
  geom_point(data = inf_beta, mapping = aes(x = mid_point, y = e_mean, color = "Natural Immunity", alpha = (insesqua*0.75)), 
             show.legend = FALSE) +
  #############################################################################

scale_color_manual(values=c(
                            # "Pfizer & BioNTech" = "purple", 
                            # "Moderna" = "blue", 
                            # "AstraZeneca" = "orange", 
                            # "Johnson & Johnson" = "green4",
                            # "CoronaVac/SinoVac" = "red", 
                            "Natural Immunity" = '#ABA300')
                   ) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
  # geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  ggtitle("Waning Immunity (Beta)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)
print(infection_beta)

################################################################################
# DELTA
################################################################################

# f1 <- "Times"
# shp_col <- c("#87b35c","#e22527")

infection_delta <- ggplot() + 
  # pfizer lines (midpoint max = 38.5)
  geom_line(data = subset(pfizer_inf_104w_delta, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(pfizer_inf_104w_delta, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 2) +
  # pfizer points
  geom_point(data = pfizinf_delta, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", alpha = (in_var*0.75))) + #shape = variant,
  # moderna lines (midpoint max = 44.5)
  geom_line(data = subset(mod_inf_104w_delta, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mod_inf_104w_delta, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 2) +
  # moderna points
  geom_point(data = modinf_delta, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", alpha = (in_var*0.75))) +
  # astra lines (midpoint max = 30)
  geom_line(data = subset(ast_inf_104w_delta, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
          size = 1, linetype = 1) +
  geom_line(data = subset(ast_inf_104w_delta, mid_point > 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
          size = 1, linetype = 2) +
  # astra points
  geom_point(data = astrainf_delta, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", alpha = (in_var*0.75))) +
  # jj lines (midpoint max = 30)
  geom_line(data = subset(jj_inf_104w_delta, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
          size = 1, linetype = 1) +
  geom_line(data = subset(jj_inf_104w_delta, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
          size = 1, linetype = 2) +
  # jj points
  geom_point(data = jjinf_delta, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", alpha = (in_var*0.75))) +
  # corona sac lines (midpoint max = 14.5)
  geom_line(data = subset(cor_sino_inf_104w_delta, mid_point < 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
          size = 1, linetype = 1) +
  geom_line(data = subset(cor_sino_inf_104w_delta, mid_point > 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
          size = 1, linetype = 2) +
  # corona sac points
  geom_point(data = cor_sino_inf_delta, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", alpha = (in_var*0.75))) +
  #############################################################################
  # natural immunity lines (midpoint = 86)
  geom_line(data = subset(past_inf_delta, mid_point < 64), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_delta, mid_point > 64), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_delta, mapping = aes(x = mid_point, y = e_mean, color = "Natural Immunity", alpha = (insesqua*0.75)), 
             show.legend = FALSE) +
  #############################################################################

  scale_color_manual(values=c("Pfizer & BioNTech" = "purple", 
                              "Moderna" = "blue", 
                              "AstraZeneca" = "orange",
                              "Johnson & Johnson" = "green4",
                              "CoronaVac/SinoVac" = "red",
                              "Natural Immunity" = '#ABA300')
                                ) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  # geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  ggtitle("Waning Immunity (Delta)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(infection_delta)

################################################################################
# OMI
################################################################################

# f1 <- "Times"
# shp_col <- c("#87b35c","#e22527")


infection_omi <- ggplot() + 
  # pfizer lines (midpoint max = 38.5)
  geom_line(data = subset(pfizer_inf_104w_omi, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(pfizer_inf_104w_omi, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 2) +
  # pfizer points
  geom_point(data = pfizinf_omi, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", alpha = (in_var*0.75))) + #shape = variant,
  # moderna lines (midpoint max = 44.5)
  geom_line(data = subset(mod_inf_104w_omi, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mod_inf_104w_omi, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 2) +
  # moderna points
  # geom_point(data = modinf_omi, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
  # # astra lines (midpoint max = 30)
  geom_line(data = subset(ast_inf_104w_omi, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1, linetype = 1) +
  geom_line(data = subset(ast_inf_104w_omi, mid_point > 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1, linetype = 2) +
  # astra points
  geom_point(data = astrainf_omi, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", alpha = (in_var*0.75))) +
  # jj lines (midpoint max = 30)
  # geom_line(data = subset(jj_inf_104w_omi, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
  #           size = 1, linetype = 1) +
  # geom_line(data = subset(jj_inf_104w_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
  #           size = 1, linetype = 2) +
  # # jj points
  # geom_point(data = jjinf_omi, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", size = in_var)) +
  # corona sac lines (midpoint max = 14.5)
  # geom_line(data = subset(cor_sino_inf_104w_omi, mid_point < 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
  #           size = 1, linetype = 1) +
  # geom_line(data = subset(cor_sino_inf_104w_omi, mid_point > 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
  #           size = 1, linetype = 2) +
  # # corona sac points
  # geom_point(data = cor_sino_inf_omi, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", size = in_var)) +
  #############################################################################
# natural immunity lines (midpoint = 86)
geom_line(data = subset(past_inf_omi, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(past_inf_omi, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
            size = 1, linetype = 2) +
  # natural immunity points
  geom_point(data = inf_omi, mapping = aes(x = mid_point, y = e_mean, color = "Natural Immunity", alpha = (insesqua*0.75)), 
             show.legend = FALSE) +
  #############################################################################

scale_color_manual(values=c("Pfizer & BioNTech" = "purple", 
                            "Moderna" = "blue", 
                            "AstraZeneca" = "orange",
                            # "Johnson & Johnson" = "green4",
                            # "CoronaVac/SinoVac" = "red",
                            "Natural Immunity" = '#ABA300')
) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  # geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  ggtitle("Waning Immunity (Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)
print(infection_omi)

################################################################################
# COMBINING PLOTS

grid.arrange(infection_alpha, infection_beta ,infection_delta, infection_omi, nrow = 2, 
             top = textGrob("Infection Wanning Immunity", gp= gpar(fontsize = 20)))


