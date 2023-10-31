##Prev inf meta-analysis
meta_pinf <- fread("FILEPATH/past_inf_meta.csv")

#subset
#inf <- subset(meta_pinf, severity == "infection")
#sym <- subset(meta_pinf, severity == "symptomatic")
#sev <- subset(meta_pinf, severity == "severe")

#colors()
library(ggplot2)
library(scales)

gg1 <-  ggplot(meta_pinf, aes(x = factor(severity, level = c("infection", "symptomatic", "severe")), y = variant)) +
  geom_tile(aes(fill = efficacy, height=1, width = 0.7)) + 
  geom_text(aes(label = ve_low_upp), size = 8) +
  scale_fill_gradient(low = "peachpuff2", high = "royalblue", na.value = "white", breaks=c(0, 25, 50, 75, 100), limits=c(0,100)) +
  #scale_colour_gradient2(low = muted("red"), mid = "white", high = muted("blue"), midpoint = 0.50, na.value = "grey92") +
  #scale_fill_brewer(palette = "RdYlBu") +
  labs(x = " ", y= " ", fill = "Previous infection protection") + #Previous infection protection
  theme_grey(base_size=20) +
  theme(legend.text=element_text(face="bold"), 
        legend.key.size = unit(4, 'cm'), #change legend key size
        legend.key.height = unit(2, 'cm'),
        legend.key.width = unit(3, 'cm'), #change legend key width
        #legend.title = element_text(size=20),
        plot.background=element_blank(),
        panel.border=element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(), 
        panel.background = element_blank(),
        axis.ticks = element_blank())+
  scale_x_discrete(labels=c("Panel A: Protection against reinfection ", "Panel B: Protection against symptomatic disease", "Panel C: Protection against severe disease")) +
  theme(legend.position="bottom") +
  #ggtitle("Symptomatic disease") +
  theme(plot.title = element_text(hjust = 0.5))    
print(gg1)

#saving figures in pdf for submission
outfile <- "FILEPATH"
ggsave(file = paste0(outfile, "Figure2", ".pdf"), gg1, device = cairo_pdf, #Fig1PanelA #Fig1PanelB
       width = 105.5, height = 91.5, units = "mm", limitsize = T,
       dpi = 320)
