#data availability

library(tidyverse)
library(reshape2)
library(data.table)

#total number of unique study_id included
#first keep in each dataset the variants and vaccines of interest
basve <- read.csv("FILEPATH/data_tracking2.csv")
basve <- subset(basve, variant == "B.1.1.7" | variant == "B.1.351" | variant == "B.1.617.2" |variant == "B.1.1.529" | 
                variant == "B.1.1.529.1" | variant == "B.1.1.529.2")
basve <- subset(basve, vaccine_developer == "Pfizer & BioNTech"  | vaccine_developer == "Moderna" | 
                vaccine_developer == "AstraZeneca" | vaccine_developer == "Johnson & Johnson")
basve <- subset(basve, exclude == 0)
basve <- basve[!(basve$efficacy_mean < 0), ] #values below 0 did not run in logit space
#basve <- subset(basve, booster == 0) #booster == 0 or 1
#length(unique(basve$study_id))
#length(unique(basve$location_id))
basve <- select(basve, study_id, location_id, location_name, booster)

#first keep in each dataset the variants and vaccines of interest
tsvacc <- read.csv("FILEPATH/tsvacc.csv")
tsvacc <- subset(tsvacc, variant == "B.1.617.2" |variant == "B.1.1.529" | 
                  variant == "B.1.1.529.1" | variant == "B.1.1.529.2")
tsvacc <- subset(tsvacc, vaccine_developer == "Pfizer & BioNTech"  | vaccine_developer == "Moderna" | 
                  vaccine_developer == "AstraZeneca" | vaccine_developer == "Johnson & Johnson")
tsvacc <- subset(tsvacc, exclude == 0)
tsvacc <- tsvacc[!(tsvacc$efficacy_mean < 0), ] #values below 0 did not run in logit space
#tsvacc <- subset(tsvacc, booster == 0)
#length(unique(tsvacc$study_id))
#length(unique(tsvacc$location_id))
#tsvacc <- subset(tsvacc, vemt == 0)
tsvacc <- select(tsvacc, study_id, location_id, location_name, booster)

#merge both datasets
total <- rbind(basve, tsvacc)

#total unique study_ids included in VE paper
length(unique(total$study_id))
length(unique(total$location_id))
table(total$location_name)
table(total$location_id)

#total only booster studies
total <- subset(total, booster == 1) #booster == 0 or 1
length(unique(total$study_id))


#PLOTS data availability by dataset, variants, vaccines and outcomes

#basve
basve <- read.csv(" FILEPATH/data_tracking2.csv")

ve2 <- subset(basve, select = c(exclude, study_id, study_id2, author, location_id, location_name, vac_before_booster, vaccine_developer, 
                                variant, symptom_severity, Severity, age_start, age_end, age_mean, sample_size, booster,
                                efficacy_mean, efficacy_lower, efficacy_upper))

#tsvacc
tsvacc <- read.csv("FILEPATH/tsvacc.csv")
ve2 <- subset(tsvacc, select = c(exclude, study_id, author, X1st.dose.only., location_id, location_id2, location_name, location_name2, 
                                 vac_before_booster, vaccine_developer, variant, symptom_severity, severity,
                                 sample_size, efficacy_mean, efficacy_lower, efficacy_upper, booster, vemt, start_interval, end_interval))

#for both datasets rename severity variable
ve2 <- mutate(ve2, sev_severity = if_else(symptom_severity == "Severe", "severe",
                                  if_else(symptom_severity == "severe", "severe",
                                  if_else(symptom_severity == "Infection", "infection",
                                  if_else(symptom_severity == "infection", "infection",
                                  if_else(symptom_severity == "asymptomatic, Mild, Moderate, Severe", "symptomatic", 
                                  if_else(symptom_severity == "Asymptompatic, Mild, Moderate + Severe", "symptomatic",
                                  if_else(symptom_severity == "Mild, Moderate", "symptomatic",
                                  if_else(symptom_severity == "Mild,Moderate", "symptomatic",
                                  if_else(symptom_severity == "Mild, Moderate + Severe", "symptomatic",
                                  if_else(symptom_severity == "Moderate, Severe", "symptomatic", "NA")))))))))))
              
#keep only included studies
ve2 <- subset(ve2, exclude == 0)

#VACCINE EFFECTIVENESS
#subset of studies not booster
ve2 <- subset(ve2, booster == 0) # 0 no booster 1 with booster
ve2 <- subset(ve2, variant == "B.1.1.7" | variant == "B.1.351" | variant == "B.1.617.2" |variant == "B.1.1.529" | 
                   variant == "B.1.1.529.1" | variant == "B.1.1.529.2")
ve2 <- subset(ve2, vaccine_developer == "Pfizer & BioNTech"  | vaccine_developer == "Moderna" | 
                vaccine_developer == "AstraZeneca" | vaccine_developer == "Johnson & Johnson")
#ONLY for time_since_vacc dataset
#ve2 <- subset(ve2, vemt == 0) #0 =week after second dose and 1=average time

length(unique(ve2$study_id))
length(unique(ve2$location_id))
table(ve2$location_name)
length(unique(ve2$location_name))

library(ggplot2)

#subset by variant Alpha B.1.1.7   Beta B.1.351	  Delta B.1.617.2   Omicron #B.1.1.529	#B.1.1.529.1	#B.1.1.529.2
#ve3 <- subset(ve2, variant == "B.1.617.2")
ve3 <- subset(ve2, variant == "B.1.1.529" | variant == "B.1.1.529.1" | variant == "B.1.1.529.2")

ve2a <- distinct(ve3, author, sev_severity , vaccine_developer,  )
ve2_t <- aggregate(ve2a$author, by=list(ve2a$sev_severity,ve2a$vaccine_developer), FUN=length)

gg1 <- ggplot(ve2_t, aes(x = factor(Group.1, level = c("infection", "symptomatic", "severe")), y = Group.2, 
                  fill = factor(x), color = factor(x), height=0.7, width = 0.9)) + #, width = 0.5 #, height=0.5, width = 0.9
  geom_tile(colour="white",size=0.1) +
  scale_fill_manual(values = c("1" = "#8E063B", "2" = "#B97281", "3" = "#CCA1A9", "4" = "peachpuff2", "5" = "lightyellow2", 
                               "6" = "lavender", "7" = "#D4B6BB", "8" = "#DFD6D7", "9" = "plum4", "10" = "#E2E0E0",
                               "11" = "#E0E0E1", "12" = "#D7D7DE", "13" = "#C9CBD8", "14" = "#ECD836", "15" = "#A6ABCA",
                               "16" = "#9198C1", "17" = "#7984B8", "18" = "#606EAE", "19" = "#4157A7", "20" = "#023FA5", "21" = "blue4",
                               "24" = "darkblue" )) + 
  #scale_fill_brewer(palette = "RdYlBu") +
  labs(x= "Outcome", y= "Vaccine", fill = "Number of studies") +
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
ggsave(file = paste0(outfile, "Fig1PanelJj", ".pdf"), gg1, device = cairo_pdf, #Fig1PanelA #Fig1PanelB #Fig1PanelC #Fig1PanelD
                                                                              #Fig1PanelG #Fig1PanelH
       width = 105.5, height = 91.5, units = "mm", limitsize = T,
       dpi = 320)


#BOOSTER dose effectiveness
ve2 <- subset(ve2, booster == 1)
ve2 <- subset(ve2, variant == "B.1.1.7" | variant == "B.1.351" | variant == "B.1.617.2" |variant == "B.1.1.529" | 
                variant == "B.1.1.529.1" | variant == "B.1.1.529.2")
ve2 <- subset(ve2, vaccine_developer == "Pfizer & BioNTech"  | vaccine_developer == "Moderna" | 
                vaccine_developer == "AstraZeneca" | vaccine_developer == "Johnson & Johnson")

library(ggplot2)

#subset by variant Alpha B.1.1.7   Beta B.1.351	  Delta B.1.617.2   Omicron #B.1.1.529	#B.1.1.529.1	#B.1.1.529.2
#ve3 <- subset(ve2, variant == "B.1.617.2")
ve3 <- subset(ve2, variant == "B.1.1.529" | variant == "B.1.1.529.1" | variant == "B.1.1.529.2")

ve2a <- distinct(ve3, author, sev_severity, vac_before_booster, vaccine_developer,  )
ve2_t <- aggregate(ve2a$author, by=list(ve2a$sev_severity,ve2a$vac_before_booster,ve2a$vaccine_developer), FUN=length)

library(stringi)
ve2_t$Group.4 = stri_join(ve2_t$Group.2,ve2_t$Group.3,sep=" and ")

#display.brewer.pal(n = 11, name = 'RdYlBu')

gg1 <- ggplot(ve2_t, aes(x = factor(Group.1, level = c("infection", "symptomatic", "severe")), y = Group.4, 
                  fill = factor(x), color = factor(x), height=0.5, width = 0.9)) + #, width = 0.5
  geom_tile(colour="white",size=0.1) +
  #scale_fill_brewer(palette = "RdYlBu") +
  scale_fill_manual(values = c("1" = "#8E063B", "2" = "#B97281", "3" = "#CCA1A9", "4" = "peachpuff2", "5" = "lightyellow2", 
                               "6" = "lavender", "7" = "#D4B6BB", "8" = "#DFD6D7", "9" = "plum4", "10" = "#E2E0E0",
                               "11" = "#E0E0E1", "12" = "#D7D7DE", "13" = "#C9CBD8", "14" = "#ECD836", "15" = "#A6ABCA",
                               "16" = "#9198C1", "17" = "#7984B8", "18" = "#606EAE", "19" = "#4157A7", "20" = "#023FA5", "21" = "blue4")) +
  labs(x= "Outcome", y= "Primary vaccine and Booster", fill = "Number of studies") +
  theme_grey(base_size=8)+
  theme(legend.text=element_text(face="bold"), #, size = 8),
        #legend.title = element_text(size = 8),
        axis.text = element_text(size = 5),
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
ggsave(file = paste0(outfile, "Fig1PanelJ__", ".pdf"), gg1, device = cairo_pdf,  #Fig1PanelE #Fig1PanelF
                                                                               #Fig1PanelI #Fig1PanelIi
                                                                               #Fig1PanelJ #Fig1PanelJj
       width = 105.5, height = 91.5, units = "mm", limitsize = T,
       dpi = 320)

