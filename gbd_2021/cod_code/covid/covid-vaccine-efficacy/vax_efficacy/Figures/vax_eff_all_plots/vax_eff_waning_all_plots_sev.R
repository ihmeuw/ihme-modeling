library(data.table)
library(tidyverse)
library(dplyr)
library(ggplot2)
library(formattable)
library(gridExtra)

# reading in all data
# SEVERE
# NATURAL IMMUNITY
# alpha
past_sev_alpha <- fread("FILEPATH/past_sev_alpha.csv")
sev_alpha <- fread("FILEPATH/sev_alpha.csv")
# beta
# past_sev_beta <- fread("FILEPATH/past_sev_beta.csv")
# sev_beta <- fread("FILEPATH/sev_beta.csv")
# delta
past_sev_delta <- fread("FILEPATH/past_sev_delta.csv")
sev_delta <- fread("FILEPATH/sev_delta.csv")
# omicron
past_sev_omi <- fread("FILEPATH/past_sev_omi.csv")
sev_omi <- fread("FILEPATH/sev_omi.csv")

# AstraZeneca
# alpha
ast_sev_104w_alpha <- fread("FILEPATH/ast_sev_104w_alpha.csv")
astrasev_alpha <- fread("FILEPATH/astrasev_alpha.csv")
# beta (no beta data)
# ast_inf_104w_beta <- fread("FILEPATH/ast_inf_104w_beta.csv")
# astrainf_beta <- fread("FILEPATH/astrainf_beta.csv")
# delta
ast_sev_104w_delta <- fread("FILEPATH/ast_sev_104w_delta.csv")
astrasev_delta <- fread("FILEPATH/astrasev_delta.csv")
# omicron
ast_sev_104w_omi <- fread("FILEPATH/ast_sev_104w_omi.csv")
astrasev_omi <- fread("FILEPATH/astrasev_omi.csv")

# CoronaVac/SinoVac
# alpha
cor_sino_sev_104w_alpha <- fread("FILEPATH/cor_sino_sev_104w_alpha.csv")
cor_sino_sev_alpha <- fread("FILEPATH/cor_sino_sev_alpha.csv")
# beta (no beta data)
# cor_sino_sev_104w_beta <- fread("FILEPATH/cor_sino_sev_104w_beta.csv")
# cor_sino_sev_beta <- fread("FILEPATH/cor_sino_sev_beta.csv")
# delta
cor_sino_sev_104w_delta <- fread("FILEPATH/cor_sino_sev_104w_delta.csv")
cor_sino_sev_delta <- fread("FILEPATH/cor_sino_sev_delta.csv")
# omicron
cor_sino_sev_104w_omi <- fread("FILEPATH/cor_sino_sev_104w_omi.csv")
cor_sino_sev_omi <- fread("FILEPATH/cor_sino_sev_omi.csv")

# Johnson & Johnson
# alpha
jj_sev_104w_alpha <- fread("FILEPATH/jj_sev_104w_alpha.csv")
jjsev_alpha <- fread("FILEPATH/jjsev_alpha.csv")
# beta (no beta data)
# jj_sev_104w_beta <- fread("FILEPATH/jj_sev_104w_beta.csv")
# jjsev_beta <- fread("FILEPATH/jjsev_beta.csv")
#delta
jj_sev_104w_delta <- fread("FILEPATH/jj_sev_104w_delta.csv")
jjsev_delta <- fread("FILEPATH/jjsev_delta.csv")
# omi
jj_sev_104w_omi <- fread("FILEPATH/jj_sev_104w_omi.csv")
jjsev_omi <- fread("FILEPATH/jjsev_omi.csv")

# Moderna
# alpha
mod_sev_104w_alpha <- fread("FILEPATH/mod_sev_104w_alpha.csv")
modsev_alpha <- fread("FILEPATH/modsev_alpha.csv")
# beta (no beta data)
# mod_sev_104w_beta <- fread("FILEPATH/mod_sev_104w_beta.csv")
# modsev_beta <- fread("FILEPATH/modsev_beta.csv")
# delta
mod_sev_104w_delta <- fread("FILEPATH/mod_sev_104w_delta.csv")
modsev_delta <- fread("FILEPATH/modsev_delta.csv")
# omi
# mod_sev_104w_omi <- fread("FILEPATH/mod_sev_104w_omi.csv")
# modsev_omi <- fread("FILEPATH/modsev_omi.csv")

# Pfizer & BioNTech
# alpha
pfizer_sev_104w_alpha <- fread("FILEPATH/pfizer_sev_104w_alpha.csv")
pfizsev_alpha <- fread("FILEPATH/pfizsev_alpha.csv")
# beta (no beta data)
# pfizer_sev_104w_beta <- fread("FILEPATH/pfizer_sev_104w_beta.csv")
# pfizsev_beta <- fread("FILEPATH/pfizsev_beta.csv")
# delta
pfizer_sev_104w_delta <- fread("FILEPATH/pfizer_sev_104w_delta.csv")
pfizsev_delta <- fread("FILEPATH/pfizsev_delta.csv")
# omi
pfizer_sev_104w_omi <- fread("FILEPATH/pfizer_sev_104w_omi.csv")
pfizsev_omi <- fread("FILEPATH/pfizsev_omi.csv")



################################################################################
# SEVERE PLOTS
################################################################################

f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

# alpha severe Panel A1
severe_alpha <- ggplot() + 
  # pfizer lines (midpoint max = 38.5)
  geom_line(data = subset(pfizer_sev_104w_alpha, mid_point < 23), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1.5, linetype = 1) +
  geom_line(data = subset(pfizer_sev_104w_alpha, mid_point > 23), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1.5, linetype = 2) +
  # pfizer points
  geom_point(data = pfizsev_alpha, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  # moderna lines (midpoint max = 44.5)
  geom_line(data = subset(mod_sev_104w_alpha, mid_point < 23), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1.5, linetype = 1) +
  geom_line(data = subset(mod_sev_104w_alpha, mid_point > 23), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1.5, linetype = 2) +
  # # moderna points 
  geom_point(data = modsev_alpha, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
  # # astra lines (midpoint max = 30)
  # geom_line(data = subset(ast_inf_104w_alpha, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
  #           size = 1.5, linetype = 1) +
  # geom_line(data = subset(ast_inf_104w_alpha, mid_point > 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
  #           size = 1.5, linetype = 2) +
  # # astra points
  # geom_point(data = astrainf_alpha, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
  # # jj lines (midpoint max = 30)
  # geom_line(data = subset(jj_inf_104w_alpha, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
#           size = 1.5, linetype = 1) +
# geom_line(data = subset(jj_inf_104w_alpha, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
#           size = 1.5, linetype = 2) +
# # jj points
# geom_point(data = jjinf_alpha, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", size = in_var)) +
# # corona sac lines (midpoint max = 14.5)
# geom_line(data = subset(cor_sino_inf_104w_alpha, mid_point < 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), 
#           size = 1.5, linetype = 1) +
# geom_line(data = subset(cor_sino_inf_104w_alpha, mid_point > 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), 
#           size = 1.5, linetype = 2) +
# # corona sac points
# geom_point(data = cor_sino_inf_alpha, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", size = in_var)) +
#############################################################################
# natural immunity lines (midpoint = 86)
geom_line(data = subset(past_sev_alpha, mid_point < 43), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
          size = 1.5, linetype = 1) +
  geom_line(data = subset(past_sev_alpha, mid_point > 43), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
            size = 1.5, linetype = 2) +
  # natural immunity points
  geom_point(data = sev_alpha, mapping = aes(x = mid_point, y = e_mean, color = "Natural Immunity", size = insesqua), 
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
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  ggtitle("Waning Immunity (Alpha)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)
print(severe_alpha)

###############################################################################
# BETA
###############################################################################

# f1 <- "Times"
# shp_col <- c("#87b35c","#e22527")



# severe_beta <- ggplot() + 
  # # pfizer lines (midpoint max = 38.5)
  # geom_line(data = subset(pfizer_sev_104w, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
  #           size = 1.5, linetype = 1) +
  # geom_line(data = subset(pfizer_sev_104w, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
  #           size = 1.5, linetype = 2) +
  # # pfizer points
  # geom_point(data = pfiz, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  # # moderna lines (midpoint max = 44.5)
  # geom_line(data = subset(mod_sev_104, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
  #           size = 1.5, linetype = 1) +
  # geom_line(data = subset(mod_sev_104, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
#           size = 1.5, linetype = 2) +
# # moderna points 
# geom_point(data = mod, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
# # astra lines (midpoint max = 30)
# geom_line(data = subset(ast_sev_104w, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
#           size = 1.5, linetype = 1) +
# geom_line(data = subset(ast_sev_104w, mid_point > 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
#           size = 1.5, linetype = 2) +
# # astra points
# geom_point(data = astra, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
# # jj lines (midpoint max = 30)
# geom_line(data = subset(jj_sev_104, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
#           size = 1.5, linetype = 1) +
# geom_line(data = subset(jj_sev_104, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
#           size = 1.5, linetype = 2) +
# # jj points
# geom_point(data = jj, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", size = in_var)) +
# # corona sac lines (midpoint max = 14.5)
# geom_line(data = subset(cor_sino_sev_104, mid_point < 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), 
#           size = 1.5, linetype = 1) +
# geom_line(data = subset(cor_sino_sev_104, mid_point > 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), 
#           size = 1.5, linetype = 2) +
# # corona sac points
# geom_point(data = cor_sino, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", size = in_var)) +
#############################################################################
# natural immunity lines (midpoint = 86)
# geom_line(data = subset(past_sev_beta, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
#           size = 1.5, linetype = 1) +
#   geom_line(data = subset(past_sev_beta, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
#             size = 1.5, linetype = 2) +
  # geom_line(data = past_inf_detla, mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), size = 1) +
  
  
  # # natural immunity points
  # geom_point(data = sev_beta, mapping = aes(x = mid_point, y = e_mean, color = "Natural Immunity", size = insesqua), 
  #            show.legend = FALSE) +
  #############################################################################

# scale_color_manual(values=c(
  # "Pfizer & BioNTech" = "purple", 
  # "Moderna" = "blue", 
  # "AstraZeneca" = "orange", 
  # "Johnson & Johnson" = "green4",
  # "CoronaVac/SinoVac" = "red", 
#   "Natural Immunity" = '#ABA300')
# ) +
#   
#   
#   ylim(c(0, 1)) +
#   xlim(c(0, 110)) +
#   theme_classic() +
#   theme(text=element_text(size=20, family = f1),
#         legend.position = "bottom") +
#   guides(linetype = FALSE) +
#   labs(y="Natural immunity effectiveness", x = "Weeks after infection", color = "Vaccine") + #Week after second dose #shape = "Variant"
#   geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
#   ggtitle("Waning Immunity (Beta)") +
#   theme(plot.title = element_text(size = 18, hjust = 0.5)) +
#   guides(size = FALSE)
# print(sev_beta)

################################################################################
# DELTA
################################################################################

# f1 <- "Times"
# shp_col <- c("#87b35c","#e22527")

severe_delta <- ggplot() + 
  # pfizer lines (midpoint max = 38.5)
  geom_line(data = subset(pfizer_sev_104w_delta, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1.5, linetype = 1) +
  geom_line(data = subset(pfizer_sev_104w_delta, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1.5, linetype = 2) +
  # pfizer points
  geom_point(data = pfizsev_delta, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  # moderna lines (midpoint max = 44.5)
  geom_line(data = subset(mod_sev_104w_delta, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1.5, linetype = 1) +
  geom_line(data = subset(mod_sev_104w_delta, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1.5, linetype = 2) +
  # moderna points
  geom_point(data = modsev_delta, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
  # astra lines (midpoint max = 30)
  geom_line(data = subset(ast_sev_104w_delta, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1.5, linetype = 1) +
  geom_line(data = subset(ast_sev_104w_delta, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1.5, linetype = 2) +
  # astra points
  geom_point(data = astrasev_delta, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
  # jj lines (midpoint max = 30)
  geom_line(data = subset(jj_sev_104w_delta, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
            size = 1.5, linetype = 1) +
  geom_line(data = subset(jj_sev_104w_delta, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
            size = 1.5, linetype = 2) +
  # jj points
  geom_point(data = jjsev_delta, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", size = in_var)) +
  # corona sac lines (midpoint max = 14.5)
  geom_line(data = subset(cor_sino_sev_104w_delta, mid_point < 33), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
            size = 1.5, linetype = 1) +
  geom_line(data = subset(cor_sino_sev_104w_delta, mid_point > 33), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
            size = 1.5, linetype = 2) +
  # corona sac points
  geom_point(data = cor_sino_sev_delta, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", size = in_var)) +
  #############################################################################
# natural immunity lines (midpoint = 86)
geom_line(data = subset(past_sev_delta, mid_point < 39), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
          size = 1.5, linetype = 1) +
  geom_line(data = subset(past_sev_delta, mid_point > 39), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
            size = 1.5, linetype = 2) +
  # natural immunity points
  geom_point(data = sev_delta, mapping = aes(x = mid_point, y = e_mean, color = "Natural Immunity", size = insesqua), 
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
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  ggtitle("Waning Immunity (Delta)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)
print(severe_delta)

################################################################################
# OMI
################################################################################

# f1 <- "Times"
# shp_col <- c("#87b35c","#e22527")


severe_omi <- ggplot() + 
  # pfizer lines (midpoint max = 38.5)
  geom_line(data = subset(pfizer_sev_104w_omi, mid_point < 36), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1.5, linetype = 1) +
  geom_line(data = subset(pfizer_sev_104w_omi, mid_point > 36), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1.5, linetype = 2) +
  # pfizer points
  geom_point(data = pfizsev_omi, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  # moderna lines (midpoint max = 44.5)
  # geom_line(data = subset(mod_sev_104w_omi, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
  #           size = 1.5, linetype = 1) +
  # geom_line(data = subset(mod_sev_104w_omi, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
  #           size = 1.5, linetype = 2) +
  # moderna points
  # geom_point(data = modinf_omi, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
  # # astra lines (midpoint max = 30)
  geom_line(data = subset(ast_sev_104w_omi, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1.5, linetype = 1) +
  geom_line(data = subset(ast_sev_104w_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"),
            size = 1.5, linetype = 2) +
  # astra points
  geom_point(data = astrasev_omi, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
  # jj lines (midpoint max = 30)
  # geom_line(data = subset(jj_inf_104w_omi, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
  #           size = 1.5, linetype = 1) +
  # geom_line(data = subset(jj_inf_104w_omi, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
  #           size = 1.5, linetype = 2) +
  # # jj points
  # geom_point(data = jjinf_omi, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", size = in_var)) +
  # corona sac lines (midpoint max = 14.5)
  # geom_line(data = subset(cor_sino_inf_104w_omi, mid_point < 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
  #           size = 1.5, linetype = 1) +
  # geom_line(data = subset(cor_sino_inf_104w_omi, mid_point > 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"),
#           size = 1.5, linetype = 2) +
# # corona sac points
# geom_point(data = cor_sino_inf_omi, mapping = aes(x = mid_point, y = e_mean, color = "CoronaVac/SinoVac", size = in_var)) +
#############################################################################
# natural immunity lines (midpoint = 86)
geom_line(data = subset(past_sev_omi, mid_point < 58), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
          size = 1.5, linetype = 1) +
  geom_line(data = subset(past_sev_omi, mid_point > 58), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
            size = 1.5, linetype = 2) +
  # natural immunity points
  geom_point(data = sev_omi, mapping = aes(x = mid_point, y = e_mean, color = "Natural Immunity", size = insesqua), 
             show.legend = FALSE) +
  #############################################################################

scale_color_manual(values=c("Pfizer & BioNTech" = "purple", 
                            # "Moderna" = "blue", 
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
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  ggtitle("Waning Immunity (Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)
print(severe_omi)

################################################################################
# COMBINING PLOTS

grid.arrange(severe_alpha,severe_delta, severe_omi, nrow = 3, 
             top = textGrob("Severe Wanning Immunity", gp= gpar(fontsize = 20)))
