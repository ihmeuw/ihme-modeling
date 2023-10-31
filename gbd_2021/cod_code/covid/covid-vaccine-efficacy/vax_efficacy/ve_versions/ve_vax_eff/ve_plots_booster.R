#plots booster_log

library(ggplot2)
library(formattable)
f1 <- "Times"
shp_col <- c("#87b35c","#e22527")
colors()

#Delta Infection A2
f1 <- "Times"
infection <- ggplot() + 
  #geom_ribbon(data = pfipfi_inf_15w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = modmod_inf_12w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_line(data = pfipfi_inf_15w, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech/Booster: Pfizer & BioNTech"), size = 1.5) +
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech/Booster: Pfizer & BioNTech", size = in_var)) + 
  geom_line(data = modmod_inf_12w, mapping = aes(x = mid_point, y = ve, color = "Moderna/Booster: Moderna"), size = 1.5) + 
  geom_point(data = mod, mapping = aes(x = mid_point, y = efficacy_mean, color = "Moderna/Booster: Moderna", size = in_var)) +
  scale_color_manual(values=c("Pfizer & BioNTech/Booster: Pfizer & BioNTech" = "coral2", "Moderna/Booster: Moderna" = "cyan2")) +
  ylim(c(0, 1)) +
  xlim(c(0, 20)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after booster dose", color = "Vaccine") +
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  guides(size = FALSE)
print(infection)


#Delta Severe A6
f1 <- "Times"
sev_delta <- ggplot() + 
  geom_ribbon(data = pfipfi_sev_15w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_ribbon(data = pfimod_sev_11w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_ribbon(data = astpfi_sev_15w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_line(data = pfipfi_sev_15w, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech/Booster: Pfizer & BioNTech"), size = 1.5) +
  geom_point(data = pfipfi_sev, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech/Booster: Pfizer & BioNTech", size = in_var)) + 
  geom_line(data = pfimod_sev_11w, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech/Booster: Moderna"), size = 1.5) + 
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech/Booster: Moderna", size = in_var)) +
  geom_line(data = astpfi_sev_15w, mapping = aes(x = mid_point, y = ve, color = "AstraZeneca/Booster: Pfizer & BioNTech"), size = 1.5) + 
  geom_point(data = astra, mapping = aes(x = mid_point, y = efficacy_mean, color = "AstraZeneca/Booster: Pfizer & BioNTech", size = in_var)) +
  scale_color_manual(values=c("Pfizer & BioNTech/Booster: Pfizer & BioNTech" = "coral2", "Pfizer & BioNTech/Booster: Moderna" = "plum2", "AstraZeneca/Booster: Pfizer & BioNTech" = "seagreen2")) +
  ylim(c(0, 1)) +
  xlim(c(0, 20)) +
  theme_classic() +
  theme(text=element_text(size=15, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after booster dose", color = "Vaccine") +
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  guides(size = FALSE)
print(sev_delta)

#Omicron Symptomatic B4
f1 <- "Times"
symp_omi <- ggplot() + 
  #geom_ribbon(data = pfipfi_symp_o_17w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = astpfi_symp_o_17, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = astast_symp_o_17w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_line(data = pfipfi_symp_o_17w, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech/Booster: Pfizer & BioNTech"), size = 1.5) +
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech/Booster: Pfizer & BioNTech", size = in_var)) + 
  geom_line(data = astpfi_symp_o_17, mapping = aes(x = mid_point, y = ve, color = "AstraZeneca/Booster: Pfizer & BioNTech"), size = 1.5) + 
  geom_point(data = astpfi_symp_o, mapping = aes(x = mid_point, y = efficacy_mean, color = "AstraZeneca/Booster: Pfizer & BioNTech", size = in_var)) +
  geom_line(data = astast_symp_o_17w, mapping = aes(x = mid_point, y = ve, color = "AstraZeneca/Booster: AstraZeneca"), size = 1.5) + 
  geom_point(data = astra, mapping = aes(x = mid_point, y = efficacy_mean, color = "AstraZeneca/Booster: AstraZeneca", size = in_var)) +
  scale_color_manual(values=c("Pfizer & BioNTech/Booster: Pfizer & BioNTech" = "coral2", "AstraZeneca/Booster: Pfizer & BioNTech" = "royalblue2", "AstraZeneca/Booster: AstraZeneca" = "orchid2")) +
  ylim(c(0, 1)) +
  xlim(c(0, 20)) +
  theme_classic() +
  theme(text=element_text(size=15, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after booster dose", color = "Vaccine") +
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  guides(size = FALSE)
print(symp_omi)

#Omicron Severe B6
f1 <- "Times"
sev_omi <- ggplot() + 
  #geom_ribbon(data = pfipfi_sev_o_20, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = pfiast_sev_o_20, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = modastsev_o_13, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  #geom_ribbon(data = astpfi_sev_o_15w, aes(x=mid_point, ymin=ve_lower, ymax=ve_upper), alpha=0.4, fill="pink") +
  geom_line(data = pfipfi_sev_o_20, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech/Booster: Pfizer & BioNTech"), size = 1.5) +
  geom_point(data = pfipfi_sev_o, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech/Booster: Pfizer & BioNTech", size = in_var)) + 
  geom_line(data = pfiast_sev_o_20, mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech/Booster: AstraZeneca"), size = 1.5) + 
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = efficacy_mean, color = "Pfizer & BioNTech/Booster: AstraZeneca", size = in_var)) +
  geom_line(data = modastsev_o_13, mapping = aes(x = mid_point, y = ve, color = "Moderna/Booster: AstraZeneca"), size = 1.5) + 
  geom_point(data = mod, mapping = aes(x = mid_point, y = efficacy_mean, color = "Moderna/Booster: AstraZeneca", size = in_var)) +
  geom_line(data = astpfi_sev_o_15w, mapping = aes(x = mid_point, y = ve, color = "AstraZeneca/Booster: Pfizer & BioNTech"), size = 1.5) + 
  geom_point(data = astra, mapping = aes(x = mid_point, y = efficacy_mean, color = "AstraZeneca/Booster: Pfizer & BioNTech", size = in_var)) +
  scale_color_manual(values=c("Pfizer & BioNTech/Booster: Pfizer & BioNTech" = "coral2", "AstraZeneca/Booster: Pfizer & BioNTech" = "royalblue2", "Pfizer & BioNTech/Booster: AstraZeneca" = "yellow2", "Moderna/Booster: AstraZeneca" = "grey50")) +
  ylim(c(0, 1)) +
  xlim(c(0, 20)) +
  theme_classic() +
  theme(text=element_text(size=12, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after booster dose", color = "Vaccine") +
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  guides(size = FALSE)
print(sev_omi)
