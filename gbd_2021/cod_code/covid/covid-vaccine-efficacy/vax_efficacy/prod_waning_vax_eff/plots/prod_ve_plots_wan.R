library(ggplot2)
library(formattable)

past_inf_delta <- fread("FILEPATH/past_inf_delta.csv")
inf_delta <- fread("FILEPATH/inf_delta.csv")

f1 <- "Times"
shp_col <- c("#87b35c","#e22527")

# new linetype variable (DOESNT WORK FOR NOW)
# pfizer_sev_104w_line <- mutate(pfizer_sev_104w, line = ifelse(mid_point < 17, "dashed", "solid"))
# mod_sev_104_line <- mutate(mod_sev_104, line = ifelse(mid_point < 17, "dashed", "solid"))
# ast_sev_104w_line <- mutate(ast_sev_104w, line = ifelse(mid_point < 17, "dashed", "solid"))
# jj_sev_104_line <- mutate(jj_sev_104, line = ifelse(mid_point < 17, "dashed", "solid"))
# cor_sino_sev_104_line <- mutate(cor_sino_sev_104, line = ifelse(mid_point < 17, "dashed", "solid"))

#delta infection Panel A1
infection_delta <- ggplot() + 
  # pfizer lines (midpoint max = 38.5)
  geom_line(data = subset(pfizer_sev_104w, mid_point < 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1.5, linetype = 1) +
  geom_line(data = subset(pfizer_sev_104w, mid_point > 39), mapping = aes(x = mid_point, y = ve, color = "Pfizer & BioNTech"), 
            size = 1.5, linetype = 2) +
  # pfizer points
  geom_point(data = pfiz, mapping = aes(x = mid_point, y = e_mean, color = "Pfizer & BioNTech", size = in_var)) + #shape = variant,
  # moderna lines (midpoint max = 44.5)
  geom_line(data = subset(mod_sev_104, mid_point < 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1.5, linetype = 1) +
  geom_line(data = subset(mod_sev_104, mid_point > 45), mapping = aes(x = mid_point, y = ve, color = "Moderna"), 
            size = 1.5, linetype = 2) +
  # moderna points 
  geom_point(data = mod, mapping = aes(x = mid_point, y = efficacy_mean, color = "Moderna", size = in_var)) +
  # astra lines (midpoint max = 30)
  geom_line(data = subset(ast_sev_104w, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
            size = 1.5, linetype = 1) +
  geom_line(data = subset(ast_sev_104w, mid_point > 31), mapping = aes(x = mid_point, y = ve, color = "AstraZeneca"), 
            size = 1.5, linetype = 2) +
  # astra points
  geom_point(data = astra, mapping = aes(x = mid_point, y = efficacy_mean, color = "AstraZeneca", size = in_var)) +
  # jj lines (midpoint max = 30)
  geom_line(data = subset(jj_sev_104, mid_point < 31), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
            size = 1.5, linetype = 1) +
  geom_line(data = subset(jj_sev_104, mid_point > 30), mapping = aes(x = mid_point, y = ve, color = "Johnson & Johnson"), 
            size = 1.5, linetype = 2) +
  # jj points
  geom_point(data = jj, mapping = aes(x = mid_point, y = efficacy_mean, color = "Johnson & Johnson", size = in_var)) +
  # corona sac lines (midpoint max = 14.5)
  geom_line(data = subset(cor_sino_sev_104, mid_point < 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), 
            size = 1.5, linetype = 1) +
  geom_line(data = subset(cor_sino_sev_104, mid_point > 15), mapping = aes(x = mid_point, y = ve, color = "CoronaVac/SinoVac"), 
            size = 1.5, linetype = 2) +
  # corona sac points
  geom_point(data = cor_sino, mapping = aes(x = mid_point, y = efficacy_mean, color = "CoronaVac/SinoVac", size = in_var)) +
  #############################################################################
  # natural immunity lines (midpoint = 86)
  geom_line(data = subset(past_inf_delta, mid_point < 86), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
          size = 1.5, linetype = 1) +
  geom_line(data = subset(past_inf_delta, mid_point > 86), mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), 
            size = 1.5, linetype = 2) +
# geom_line(data = past_inf_detla, mapping = aes(x = mid_point, y = pinf, color = "Natural Immunity"), size = 1) +
  
  
  # natural immunity points
  geom_point(data = inf_delta, mapping = aes(x = mid_point, y = e_mean, color = "Natural Immunity", size = insesqua), 
             show.legend = FALSE) +
  #############################################################################
  
scale_color_manual(values=c("Pfizer & BioNTech" = "purple", "Moderna" = "blue", "AstraZeneca" = "orange", "Johnson & Johnson" = "green4",
                            "CoronaVac/SinoVac" = "red", "Natural Immunity" = '#ABA300')) +

  
  ylim(c(0, 1)) +
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1),
        legend.position = "bottom") +
  guides(linetype = FALSE) +
  labs(y="Vaccine effectiveness", x = "Week after second dose", color = "Vaccine") + #Week after second dose #shape = "Variant"
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow")) +
  ggtitle("Waning Immunity (Delta)") +
  theme(plot.title = element_text(size = 22, hjust = 0.5)) +
  guides(size = FALSE)
print(infection_delta)
