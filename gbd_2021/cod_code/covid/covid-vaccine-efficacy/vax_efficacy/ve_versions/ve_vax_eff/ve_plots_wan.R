#plots waning Vaccine effectiveness

library(ggplot2)
library(formattable)
f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

#delta infection Panel A1
infection <- ggplot() + 
  #geom_ribbon(data = pfizer_inf_40w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = mod_inf_45w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = ast_inf_30w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = jj_inf_30w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_line(data = pfizer_sev_104w, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), size = 1.5) +
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  geom_line(data = mod_sev_104, mapping = aes(x = mid_point, y = ve, color = "Moderna"), size = 1.5) + 
  geom_point(data = mod, mapping = aes(x = mid_point, y = e_mean, color = "Moderna", size = in_var)) +
  geom_line(data = ast_sev_104w, mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), size = 1.5) + 
  geom_point(data = astra, mapping = aes(x = mid_point, y = e_mean, color = "AstraZeneca", size = in_var)) +
  geom_line(data = jj_sev_104, mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), size = 1.5) + 
  geom_point(data = jj, mapping = aes(x = mid_point, y = e_mean, color = "Johnson & Johnson", size = in_var)) +
  ###############################################
  geom_line(data = cor_sino_sev_104, mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), size = 1.5) +
  geom_point(data = cor_sino, mapping = aes(x = mid_point, y = efficacy_mean, color = "CoronaVac/SinoVac", size = in_var)) +
  ###############################################
  scale_color_manual(values=c("Pfizer & BioNTech" = "purple", "Moderna" = "blue", "AstraZeneca" = "orange", "Johnson & Johnson" = "green4",
                              "CoronaVac/SinoVac" = "red")) +
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  #scale_color_manual(values = shp_col, name = "Variant") +
  guides(linetype = "none") +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  guides(size = "none")
print(infection)

#saving figures in pdf for submission
#outfile <- "/ihme/homes/cstein87/vaceff/FiguresVE/"
#ggsave(file = paste0(outfile, "Fig3PanelA1", ".pdf"), infection, device = cairo_pdf,
#       width = 105.5, height = 91.5, units = "mm", limitsize = T,
#       dpi = 320)

#ggsave(file = paste0(outfile, "Fig3PanelA1", ".pdf"), infection, device = cairo_pdf,
#       width = 7.5, height = 4.15, pointsize = 1.5, limitsize = T,
#       dpi = 320)

#delta symptomatic Panel A3
f1 <- "Times"
symp <- ggplot() + 
  geom_ribbon(data = pfizer_symp_43w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_ribbon(data = mod_symp_43w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_ribbon(data = ast_symp_27w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_line(data = pfizer_symp_43w, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), size = 1.5) +
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  geom_line(data = mod_symp_43w, mapping = aes(x = mid_point, y = ve, color = "Moderna"), size = 1.5) + 
  geom_point(data = mod, mapping = aes(x = mid_point, y = efficacy_mean, color = "Moderna", size = in_var)) +
  geom_line(data = ast_symp_27w, mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), size = 1.5) + 
  geom_point(data = astra, mapping = aes(x = mid_point, y = efficacy_mean, color = "AstraZeneca", size = in_var)) +
  scale_color_manual(values=c("Pfizer & BioNTech" = "purple", "Moderna" = "blue", "AstraZeneca" = "orange")) +
  ylim(c(0, 1)) +
  xlim(c(0, 45)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  #scale_color_manual(values = shp_col, name = "Variant") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  guides(size = FALSE)
print(symp)

#delta severe Panel A5
f1 <- "Times"
sev <- ggplot() + 
  geom_ribbon(data = pfizer_sev_104w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_ribbon(data = mod_sev_104, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_ribbon(data = ast_sev_104w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_ribbon(data = jj_sev_30, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_line(data = pfizer_sev_104w, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), size = 1.5) +
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  geom_line(data = mod_sev_104, mapping = aes(x = mid_point, y = ve, color = "Moderna"), size = 1.5) + 
  geom_point(data = mod, mapping = aes(x = mid_point, y = efficacy_mean, color = "Moderna", size = in_var)) +
  geom_line(data = ast_sev_104w, mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), size = 1.5) + 
  geom_point(data = astra, mapping = aes(x = mid_point, y = efficacy_mean, color = "AstraZeneca", size = in_var)) +
  geom_line(data = jj_sev_104, mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), size = 1.5) + 
  geom_point(data = jj, mapping = aes(x = mid_point, y = efficacy_mean, color = "Johnson & Johnson", size = in_var)) +
  scale_color_manual(values=c("Pfizer & BioNTech" = "purple", "Moderna" = "blue", "AstraZeneca" = "orange", "Johnson & Johnson" = "green4")) +
  ylim(c(0, 1)) +
  xlim(c(0, 45)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  #scale_color_manual(values = shp_col, name = "Variant") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  guides(size = FALSE)
print(sev)

#omicron infection Panel B1
f1 <- "Times"
inf_o <- ggplot() + 
  #geom_ribbon(data = pfizer_inf_o30w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = mod_inf_o_45w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_line(data = pfizer_inf_o30w, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), size = 1.5) +
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  geom_line(data = mod_inf_o_45w, mapping = aes(x = mid_point, y = ve, color = "Moderna"), size = 1.5) + 
  geom_point(data = mod, mapping = aes(x = mid_point, y = efficacy_mean, color = "Moderna", size = in_var)) +
  scale_color_manual(values=c("Pfizer & BioNTech" = "purple", "Moderna" = "blue")) +
  ylim(c(0, 1)) +
  xlim(c(0, 45)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  #scale_color_manual(values = shp_col, name = "Variant") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  guides(size = FALSE)
print(inf_o)

#omicron symp Panel B3
f1 <- "Times"
symp_o <- ggplot() + 
  #geom_ribbon(data = pfizer_symp_o_43, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = mod_symp_o_43, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_line(data = pfizer_symp_o_43, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), size = 1.5) +
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  geom_line(data = mod_symp_o_43, mapping = aes(x = mid_point, y = ve, color = "Moderna"), size = 1.5) + 
  geom_point(data = mod, mapping = aes(x = mid_point, y = efficacy_mean, color = "Moderna", size = in_var)) +
  scale_color_manual(values=c("Pfizer & BioNTech" = "purple", "Moderna" = "blue")) +
  ylim(c(0, 1)) +
  xlim(c(0, 45)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  #scale_color_manual(values = shp_col, name = "Variant") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  guides(size = FALSE)
print(symp_o)

#omicron sev Panel B5
f1 <- "Times"
sev_o <- ggplot() + 
  #geom_ribbon(data = pfizer_sev_o_34w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = ast_sev_o_30, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_line(data = pfizer_sev_o_34w, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), size = 1.5) +
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  geom_line(data = ast_sev_o_30, mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), size = 1.5) + 
  geom_point(data = astra, mapping = aes(x = mid_point, y = efficacy_mean, color = "AstraZeneca", size = in_var)) +
  scale_color_manual(values=c("Pfizer & BioNTech" = "purple", "AstraZeneca" = "orange")) +
  ylim(c(0, 1)) +
  xlim(c(0, 45)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  #scale_color_manual(values = shp_col, name = "Variant") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  guides(size = FALSE)
print(sev_o)


