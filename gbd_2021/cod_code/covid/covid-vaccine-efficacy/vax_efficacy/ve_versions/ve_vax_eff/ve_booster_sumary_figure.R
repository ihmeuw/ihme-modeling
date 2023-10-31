#meta_analysis results_vaccine effectiveness

library(tidyverse)
library(reshape2)
library(data.table)

#vaccine effectiveness
meta_ve <- read.csv("FILEPATH/ve_meta.csv")

#subset
#inf <- subset(meta_ve, severity == "infection")
#sym <- subset(meta_ve, severity == "symptomatic")
sev <- subset(meta_ve, severity == "severe")

#colors()

library(ggplot2)
library(scales)
gg1 <- ggplot(sev, aes(variant, vaccine)) +
  geom_tile(aes(fill = efficacy, height=1, width = 0.65)) + 
  geom_text(aes(label = ve_low_upp), size = 5) +
  scale_fill_gradient(low = "peachpuff2", high = "royalblue", na.value = "white", breaks=c(0, 25, 50, 75, 100), limits=c(0,100)) +
  #scale_colour_gradient2(low = muted("red"), mid = "white", high = muted("blue"), midpoint = 0.50, na.value = "grey92") +
  #scale_fill_brewer(palette = "RdYlBu") +
  labs(x= "Variant", y= "Vaccine", fill = "Vaccine effectiveness") +
  theme_grey(base_size=14) +
  theme(legend.text=element_text(face="bold"), 
        legend.key.size = unit(4, 'cm'), #change legend key size
        legend.key.height = unit(1, 'cm'),
        legend.key.width = unit(3, 'cm'), #change legend key width
        #legend.title = element_text(size=20),
        plot.background=element_blank(),
        panel.border=element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(), 
        panel.background = element_blank(),
        axis.ticks = element_blank()) +
  theme(legend.position="bottom") +
  ggtitle(" ") +
  theme(plot.title = element_text(hjust = 0.5))
print(gg1)

#saving figures in pdf for submission
#  outfile <- "FILEPATH"
#  ggsave(file = paste0(outfile, "Fig2PanelA", ".pdf"), gg1, device = cairo_pdf, #Fig2PanelA #Fig2PanelB #Fig2PanelC
#         width = 105.5, height = 91.5, units = "mm", limitsize = T,
#         dpi = 320)
  

#booster dose effectiveness
meta_booster <- read.csv("FILEPATH/booster_meta.csv")

#subset
#inf <- subset(meta_booster, severity == "infection")
#sym <- subset(meta_booster, severity == "symptomatic")
sev <- subset(meta_booster, severity == "severe")

#colors()

library(ggplot2)
library(scales)
gg1 <- ggplot(sev, aes(variant, vaccine)) +
  geom_tile(aes(fill = efficacy, height=1, width = 0.65)) + 
  geom_text(aes(label = ve_low_upp), size = 5) +
  scale_fill_gradient(low = "peachpuff2", high = "royalblue", na.value = "white", breaks=c(0, 25, 50, 75, 100), limits=c(0,100)) +
  #scale_colour_gradient2(low = muted("red"), mid = "white", high = muted("blue"), midpoint = 0.50, na.value = "grey92") +
  #scale_fill_brewer(palette = "RdYlBu") +
  labs(x= "Variant", y= "Vaccine", fill = "Booster dose effectiveness") +
  theme_grey(base_size=14) +
  theme(legend.text=element_text(face="bold"), 
        legend.key.size = unit(4, 'cm'), #change legend key size
        legend.key.height = unit(1, 'cm'),
        legend.key.width = unit(3, 'cm'), #change legend key width
        #legend.title = element_text(size=20),
        plot.background=element_blank(),
        panel.border=element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(), 
        panel.background = element_blank(),
        axis.ticks = element_blank()) +
  theme(legend.position="bottom") +
  ggtitle(" ") +
  theme(plot.title = element_text(hjust = 0.5))
print(gg1)

