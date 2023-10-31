#Data availability

#PREVIOUS INFECTION by variant and outcome

#Fig1PanA: First on prev_inf_baseline_log code need run lines from 7 to 49 
#Fig1PanelB: First on prev_inf_waning_log code need run lines from 7 to 57

ve2a <- distinct(ve2, author, sev_severity , prim_variant_name, variant_name,   )
ve2_t <- aggregate(ve2a$author, by=list(ve2a$sev_severity,ve2a$prim_variant_name,ve2a$variant_name), FUN=length)

library(stringi)
ve2_t$Group.4 = stri_join(ve2_t$Group.2,ve2_t$Group.3,sep=" and ")

#ONLY for Fig 1 Panel A show Omicron and Omicron
ve2_t_panA <- as.data.table(ve2_t)
ve2_t_panA[Group.4 == "Omicron and Omicron", Group.5 :=Group.4 ]
ve2_t_panA[Group.4 != "Omicron and Omicron", Group.5 :=Group.3 ]


#Fig1PanA use ve2_t_panA and Group.5 / Fig1PanelB use ve2_t and Group.3
gg1 <- ggplot(ve2_t, aes(x = factor(Group.1, level = c("infection", "symptomatic", "severe")), y = Group.3,
                         fill = factor(x), color = factor(x), height=0.5, width = 0.9)) + #, width = 0.5
  geom_tile(colour="white",size=0.1) +
  #scale_fill_brewer(palette = "RdYlBu") +
  scale_fill_manual(values = c("1" = "#8E063B", "2" = "#B97281", "3" = "#CCA1A9", "4" = "peachpuff2", "5" = "lightyellow2", 
                               "6" = "lavender", "7" = "#D4B6BB", "8" = "#DFD6D7", "9" = "plum4", "10" = "#E2E0E0",
                               "11" = "#E0E0E1", "12" = "#D7D7DE", "13" = "#C9CBD8", "14" = "#ECD836", "15" = "#A6ABCA",
                               "16" = "#9198C1", "17" = "#7984B8", "18" = "#606EAE", "19" = "#4157A7", "20" = "#023FA5", "21" = "blue4")) +
  labs(x= "Outcome", y= " ", fill = "Number of studies") +
  theme_grey(base_size=8)+
  theme(legend.text=element_text(face="bold"), 
        plot.background=element_blank(),
        panel.border=element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(), 
        panel.background = element_blank(),
        axis.ticks = element_blank()) +
  scale_x_discrete(labels=c("Infection", "Symptomatic", "Severe"))
#legend.position = "bottom")
print(gg1)

#saving figures in pdf for submission
outfile <- "FILEPATH"
ggsave(file = paste0(outfile, "Fig1PanelB", ".pdf"), gg1, device = cairo_pdf, #Fig1PanelA #Fig1PanelB
       width = 105.5, height = 91.5, units = "mm", limitsize = T,
       dpi = 320)
