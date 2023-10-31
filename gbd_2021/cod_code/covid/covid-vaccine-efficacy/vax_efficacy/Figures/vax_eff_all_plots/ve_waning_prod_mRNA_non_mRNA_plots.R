library(data.table)
library(tidyverse)
library(dplyr)
library(ggplot2)
library(formattable)
library(gridExtra)
library(ggpubr)
library(forestplot)

# reading in data
# non-mRNA infection non omi
non_mRNA_vaxes_inf_104w_non_omi_5knots_lt_true <- fread("FILEPATH/non_mRNA_vaxes_inf_104w_non_omi_5knots_lt_true.csv")
non_mRNA_vaxes_inf_non_omi_5knots_lt_true <- fread("FILEPATH/non_mRNA_vaxes_inf_non_omi_5knots_lt_true.csv")

# mRNA infection non omi
mRNA_vaxes_inf_104w_non_omi_5knots_lt_true <- fread("FILEPATH/mRNA_vaxes_inf_104w_non_omi_5knots_lt_true.csv")
mRNA_vaxes_inf_non_omi_5knots_lt_true <- fread("FILEPATH/mRNA_vaxes_inf_non_omi_5knots_lt_true.csv")

# # non-mRNA infection omi(no data)
# non_mRNA_vaxes_inf_104w_omi_5knots_lt_true
# non_mRNA_vaxes_inf_omi_5knots_lt_true

# mRNA infection omi
mRNA_vaxes_inf_104w_omi_5knots_lt_true <- fread("FILEPATH/mRNA_vaxes_inf_104w_omi_5knots_lt_true.csv")
mRNA_vaxes_inf_omi_5knots_lt_true <- fread("FILEPATH/mRNA_vaxes_inf_omi_5knots_lt_true.csv")

# non-mRNA severe non omi
non_mRNA_vaxes_sev_104w_non_omi_5knots_lt_true <- fread("FILEPATH/non_mRNA_vaxes_sev_104w_non_omi_5knots_lt_true.csv")
non_mRNA_vaxes_sev_non_omi_5knots_lt_true <- fread("FILEPATH/non_mRNA_vaxes_sev_non_omi_5knots_lt_true.csv")

# mRNA severe non omi
mRNA_vaxes_sev_104w_non_omi_5knots_lt_true <- fread("FILEPATH/mRNA_vaxes_sev_104w_non_omi_5knots_lt_true.csv")
mRNA_vaxes_sev_non_omi_5knots_lt_true <- fread("FILEPATH/mRNA_vaxes_sev_non_omi_5knots_lt_true.csv")

# severe omicron all data
all_vaxes_sev_omi_5knots_lt_true_adjusted <- fread("FILEPATH/all_vaxes_sev_omi_5knots_lt_true_adjusted.csv")
all_vaxes_sev_104w_omi_5knots_lt_true_adjusted <- fread("FILEPATH/all_vaxes_sev_104w_omi_5knots_lt_true_adjusted.csv")

# mRNA sev omi
mRNA_vaxes_sev_omi_5knots_lt_true_adjusted <- fread("FILEPATH/mRNA_vaxes_sev_omi_5knots_lt_true_adjusted.csv")
mRNA_vaxes_sev_104w_omi_5knots_lt_true_adjusted <- fread("FILEPATH/mRNA_vaxes_sev_104w_omi_5knots_lt_true_adjusted.csv")

non_mRNA_vaxes_sev_omi_5knots_lt_true_adjusted <- fread("FILEPATH/non_mRNA_vaxes_sev_omi_5knots_lt_true_adjusted.csv")
non_mRNA_vaxes_sev_104w_omi_5knots_lt_true_adjusted <- fread("FILEPATH/non_mRNA_vaxes_sev_104w_omi_5knots_lt_true_adjusted.csv")


# paper data
# non-omicron
astrainfec <- fread("FILEPATH/astrainfec.csv")
ast_inf_30w <- fread("FILEPATH/ast_inf_30w.csv")

astrasev <- fread("FILEPATH/astrasev.csv")
ast_sev_40w <- fread("FILEPATH/ast_sev_40w.csv")

jjinfec <- fread("FILEPATH/jjinfec.csv")
jj_inf_30w <- fread("FILEPATH/jj_inf_30w.csv")

jjsev <- fread("FILEPATH/jjsev.csv")
jj_sev_30 <- fread("FILEPATH/jj_sev_30.csv")

modinfec <- fread("FILEPATH/modinfec.csv")
mod_inf_45w <- fread("FILEPATH/mod_inf_45w.csv")

modsev <- fread("FILEPATH/modsev.csv")
mod_sev_40 <- fread("FILEPATH/mod_sev_40.csv")

pfizinfec <- fread("FILEPATH/pfizinfec.csv")
pfizer_inf_40w <- fread("FILEPATH/pfizer_inf_40w.csv")

pfizsev <- fread("FILEPATH/pfizsev.csv")
pfizer_sev_40w <- fread("FILEPATH/pfizer_sev_40w.csv")

# omicron
modinfec_o <- fread("FILEPATH/modinfec_o.csv")
mod_inf_o_45w <- fread("FILEPATH/mod_inf_o_45w.csv")

pfizinfec_o <- fread("FILEPATH/pfizinfec_o.csv")
pfizer_inf_o_27w <- fread("FILEPATH/pfizer_inf_o_27w.csv")

pfizer_sev_o_36w <- fread("FILEPATH/pfizer_sev_o_36w.csv")
pfizsev_o <- fread("FILEPATH/pfizsev_o.csv")



################################################################################

f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

################################################################################
# NON_OMICRON INFECTIONS
################################################################################

Vaccines_infection_non_omicron <- ggplot() + 
  # mRNA vaccines lines
  geom_line(data = subset(mRNA_vaxes_inf_104w_non_omi_5knots_lt_true, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mRNA_vaxes_inf_104w_non_omi_5knots_lt_true, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # mRNA vaccines points
  geom_point(alpha = 0.5, data = mRNA_vaxes_inf_non_omi_5knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "mRNA Vaccines", size = in_var)) + #shape = variant,
  
  # non mRNA vaccines lines 
  geom_line(data = subset(non_mRNA_vaxes_inf_104w_non_omi_5knots_lt_true, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(non_mRNA_vaxes_inf_104w_non_omi_5knots_lt_true, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # non mRNA vaccines points
  geom_point(alpha = 0.5, data = non_mRNA_vaxes_inf_non_omi_5knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "Non-mRNA Vaccines", size = in_var)) +
  ##############################################################################
# astra vaccines lines 
geom_line(data = subset(ast_inf_30w, mid_point < 16), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(ast_inf_30w, mid_point > 16), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
            size = 1, linetype = 2) +
  # astra vaccines points
  geom_point(alpha = 0.5, data = astrainfec, mapping = aes(x = mid_point, y = efficacy_mean, color = "AstraZeneca", size = in_var)) +
################################################################################
# # jj vaccines lines 
geom_line(data = subset(jj_inf_30w, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
          size = 1, linetype = 1) +
  # geom_line(data = subset(jj_inf_30w, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"),
  #           size = 1, linetype = 2) +
  # jj vaccines points
  geom_point(alpha = 0.5, data = jjinfec, mapping = aes(x = mid_point, y = efficacy_mean, color = "Johnson & Johnson", size = in_var)) +
################################################################################
# moderna vaccines lines
geom_line(data = subset(mod_inf_45w, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"),
          size = 1, linetype = 1) +
#   geom_line(data = subset(mod_inf_45w, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"),
#             size = 1, linetype = 2) +
  # moderna vaccines points
  geom_point(alpha = 0.5, data = pfizinfec, mapping = aes(x = mid_point, y = efficacy_mean, color = "Moderna", size = in_var)) +
###############################################################################
# pfizer vaccines lines
geom_line(data = subset(pfizer_inf_40w, mid_point < 27), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"),
          size = 1, linetype = 1) +
  geom_line(data = subset(pfizer_inf_40w, mid_point > 27), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"),
            size = 1, linetype = 2) +
  # pfizer vaccines points
  geom_point(alpha = 0.5, data = pfizinfec, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech", size = in_var)) +
  ################################################################################

  scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c("mRNA Vaccines" = "purple", 
                              "Non-mRNA Vaccines" = "green4",
                              "AstraZeneca" = 'red2',
                              "Johnson & Johnson" = 'blue',
                              "Moderna" = 'black',
                              "Pfizer & BioNTech" = 'orange3')
  ) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity Infections (Non-Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(Vaccines_infection_non_omicron)

################################################################################
# OMICRON INFECTIONS
################################################################################

Vaccines_infection_omicron <- ggplot() + 
  # mRNA vaccines lines
  geom_line(data = subset(mRNA_vaxes_inf_104w_omi_5knots_lt_true, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mRNA_vaxes_inf_104w_omi_5knots_lt_true, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # mRNA vaccines points
  geom_point(alpha = 0.5, data = mRNA_vaxes_inf_omi_5knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "mRNA Vaccines", size = in_var)) + #shape = variant,
  ##############################################################################
# moderna vaccines lines 
geom_line(data = subset(mod_inf_o_45w, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
          size = 1, linetype = 1) +
  # geom_line(data = subset(mod_inf_o_45w, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
  #           size = 1, linetype = 2) +
  # moderna vaccines points
  geom_point(alpha = 0.5, data = modinfec_o, mapping = aes(x = mid_point, y = efficacy_mean, color = "Moderna", size = in_var)) +
  ################################################################################
# pfizer vaccines lines 
geom_line(data = subset(pfizer_inf_o_27w, mid_point < 27), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
          size = 1, linetype = 1) +
  # geom_line(data = subset(pfizer_inf_o_27w, mid_point > 27), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
  #           size = 1, linetype = 2) +
  # pfizer vaccines points
  geom_point(alpha = 0.5, data = pfizinfec_o, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech", size = in_var)) +


  scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c("mRNA Vaccines" = "purple",
                              "Moderna" = 'black',
                              "Pfizer & BioNTech" = 'orange3')
  ) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity Infections (Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(Vaccines_infection_omicron)

################################################################################
# NON OMICRON SEVERE 
################################################################################

Vaccines_severe_non_omicron <- ggplot() + 
  # mRNA vaccines lines
  geom_line(data = subset(mRNA_vaxes_sev_104w_non_omi_5knots_lt_true, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mRNA_vaxes_sev_104w_non_omi_5knots_lt_true, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # mRNA vaccines points
  geom_point(alpha = 0.5, data = mRNA_vaxes_sev_non_omi_5knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "mRNA Vaccines", size = in_var)) + #shape = variant,
  
  # non mRNA vaccines lines 
  geom_line(data = subset(non_mRNA_vaxes_sev_104w_non_omi_5knots_lt_true, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(non_mRNA_vaxes_sev_104w_non_omi_5knots_lt_true, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 2) +
  # non mRNA vaccines points
  geom_point(alpha = 0.5, data = non_mRNA_vaxes_sev_non_omi_5knots_lt_true, mapping = aes(x = mid_point, y = efficacy_mean, color = "Non-mRNA Vaccines", size = in_var)) +
  ##############################################################################
  # astra vaccines lines 
  geom_line(data = subset(ast_sev_40w, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(ast_sev_40w, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
            size = 1, linetype = 2) +
  # astra vaccines points
  geom_point(alpha = 0.5, data = astrasev, mapping = aes(x = mid_point, y = efficacy_mean, color = "AstraZeneca", size = in_var)) +
  ################################################################################
# jj vaccines lines 
geom_line(data = subset(jj_sev_30, mid_point < 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
          size = 1, linetype = 1) +
  # geom_line(data = subset(jj_sev_30, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
  #           size = 1, linetype = 2) +
  # jj vaccines points
  geom_point(alpha = 0.5, data = jjsev, mapping = aes(x = mid_point, y = efficacy_mean, color = "Johnson & Johnson", size = in_var)) +
  ################################################################################
# moderna vaccines lines 
geom_line(data = subset(mod_sev_40, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(mod_sev_40, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1, linetype = 2) +
  # moderna vaccines points
  geom_point(alpha = 0.5, data = modsev, mapping = aes(x = mid_point, y = efficacy_mean, color = "Moderna", size = in_var)) +
  ################################################################################
# pfizer vaccines lines 
geom_line(data = subset(pfizer_sev_40w, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
          size = 1, linetype = 1) +
  geom_line(data = subset(pfizer_sev_40w, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 2) +
  # pfizer vaccines points
  geom_point(alpha = 0.5, data = pfizsev, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech", size = in_var)) +
  ################################################################################
  
  
  scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c("mRNA Vaccines" = "purple", 
                              "Non-mRNA Vaccines" = "green4",
                              "AstraZeneca" = 'red2',
                              "Johnson & Johnson" = 'blue',
                              "Moderna" = 'black',
                              "Pfizer & BioNTech" = 'orange3')
  ) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity Severe (Non-Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(Vaccines_severe_non_omicron)

################################################################################
# OMICRON SEVERE 
################################################################################

Vaccines_severe_omicron <- ggplot() + 
  # All vaccines lines
  geom_line(data = subset(all_vaxes_sev_104w_omi_5knots_lt_true_adjusted, mid_point < 36), mapping = aes(x = mid_point, y = ve, color = "All Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(all_vaxes_sev_104w_omi_5knots_lt_true_adjusted, mid_point > 36), mapping = aes(x = mid_point, y = ve, color = "All Vaccines"), 
            size = 1, linetype = 2) +
  # All vaccines points
  geom_point(alpha = 0.5, data = all_vaxes_sev_omi_5knots_lt_true_adjusted, mapping = aes(x = mid_point, y = efficacy_mean, color = "All Vaccines", size = in_var)) + #shape = variant,
  ##############################################################################
# pfizer vaccines lines 
  geom_line(data = subset(pfizer_sev_o_36w, mid_point < 36), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1, linetype = 1) +
  # geom_line(data = subset(pfizer_sev_o_36w, mid_point > 36), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
  #           size = 1, linetype = 2) +
  # pfizer vaccines points
  geom_point(alpha = 0.5, data = pfizsev_o, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech", size = in_var)) +
  ##############################################################################
  # mRNA vaccines lines 
  geom_line(data = subset(mRNA_vaxes_sev_104w_omi_5knots_lt_true_adjusted, mid_point < 36), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(mRNA_vaxes_sev_104w_omi_5knots_lt_true_adjusted, mid_point > 36), mapping = aes(x = mid_point, y = ve, color = "mRNA Vaccines"),
            size = 1, linetype = 2) +
  # mRNA vaccines points
  geom_point(alpha = 0.5, data = mRNA_vaxes_sev_omi_5knots_lt_true_adjusted, mapping = aes(x = mid_point, y = efficacy_mean, color = "mRNA Vaccines", size = in_var)) +
  ##############################################################################
  # non_mRNA vaccines lines 
  geom_line(data = subset(non_mRNA_vaxes_sev_104w_omi_5knots_lt_true_adjusted, mid_point < 33), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"), 
            size = 1, linetype = 1) +
  geom_line(data = subset(non_mRNA_vaxes_sev_104w_omi_5knots_lt_true_adjusted, mid_point > 33), mapping = aes(x = mid_point, y = ve, color = "Non-mRNA Vaccines"),
            size = 1, linetype = 2) +
  # non-mRNA vaccines points
  geom_point(alpha = 0.5, data = non_mRNA_vaxes_sev_omi_5knots_lt_true_adjusted, mapping = aes(x = mid_point, y = efficacy_mean, color = "Non-mRNA Vaccines", size = in_var)) +


  scale_size_continuous(range = c(0.3,3.5)) +
  scale_color_manual(values=c("All Vaccines" = "black",
                              "Pfizer & BioNTech" = 'orange3',
                              "mRNA Vaccines" = 'purple',
                              "Non-mRNA Vaccines" = 'green4')
  ) +
  
  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  ggtitle("Waning Immunity Severe (Omicron)") +
  theme(plot.title = element_text(size = 18, hjust = 0.5)) +
  guides(size = FALSE)

print(Vaccines_severe_omicron)

################################################################################
# COMBINING PLOTS
################################################################################

grid.arrange(Vaccines_infection_non_omicron, Vaccines_infection_omicron, 
             Vaccines_severe_non_omicron, Vaccines_severe_omicron, nrow = 2, 
             top = textGrob("Vaccine Wanning Immunity (Infections) Adjusted with 5knots linear tail = T", gp= gpar(fontsize = 18)),
             bottom = textGrob("Vaccine Wanning Immunity (Severe) Adjusted with 5knots linear tail = T", gp= gpar(fontsize = 18)))
