## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Sweden Data 
## Date 2/1/2021
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

version <- 0

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0("ROOT" ,"FILEPATH/get_location_metadata.R"))
source(paste0("ROOT" ,"FILEPATH/get_age_metadata.R"))

## --------------------------------------------------------------------- ----
# Pull Swedish data
library(readxl)
input <- read_excel('FILEPATH/SWE_ICU_data_input.xlsx',
                 sheet="Blad1")[,c('Sex(f=1, m=2)','agegroup','Agegroup 65',"Fatigue (MFI00:general fatigue > 5)","Physical fatigue","Depression (PHQ > 9)",
                                   "Anxiety (GAD, ≥10 anxiety disorder)", "EQ5 Pain/discomfort", "EQ5 Anxiety/Depression",
                                   "Cognitive dysfunction (MOCA < 26)","Difficulties concentrating","Memory problems","Problem finding words","EQ5 usual activities",
                                   "Shortness of breath","Cough or sore throat")]

#convert character columns to numeric
cols.num <- c("Fatigue (MFI00:general fatigue > 5)", "Depression (PHQ > 9)", "Anxiety (GAD, ≥10 anxiety disorder)")
input[cols.num] <- sapply(input[cols.num],as.numeric)

#getting the number of patients by age and by sex 
sample_n <-input %>% 
  group_by(`Sex(f=1, m=2)`,`Agegroup 65`) %>%
  summarise(N=n()) 

#getting the number of patients by sex 
sample_n_bysex <-input %>% 
  group_by(`Sex(f=1, m=2)`) %>%
  summarise(N=n()) 

#Fatigue
input <- mutate(input, post_acute = ifelse((`Fatigue (MFI00:general fatigue > 5)` %in% 1) &
                                          (`Physical fatigue` %in% 1 | `Depression (PHQ > 9)` %in% 1 | `Anxiety (GAD, ≥10 anxiety disorder)` %in% 1) &
                                          (`EQ5 Pain/discomfort`+ `EQ5 Anxiety/Depression`>=4 ), 1,0))

table(input$post_acute)
summary(input$post_acute)

# colnames(input)
#mild_cog
input <- mutate(input, cog_mild = ifelse((`Cognitive dysfunction (MOCA < 26)` %in% c(1,'NA')) &
                                          (`EQ5 usual activities` %in% c(2,3)) &
                                          ((`Difficulties concentrating`+ `Memory problems`+ `Problem finding words`)>=1 ), 1,0))
table(input$cog_mild)
summary(input$cog_mild)

#moderate_cog
input <- mutate(input, cog_moderate = ifelse((`Cognitive dysfunction (MOCA < 26)` %in% 1) &
                                       (`EQ5 usual activities` %in% c(4,5)) &
                                       ((`Difficulties concentrating`+ `Memory problems`+ `Problem finding words`)>=1 ), 1,0))
table(input$cog_moderate)
summary(input$cog_moderate)

#combine the cog mild and cog moderate together
input <- mutate(input, cognitive = ifelse((cog_mild %in% 1 | cog_moderate %in% 1), 1, 0))
table(input$cognitive)
summary(input$cognitive)

#res_mild
input <- mutate(input, res_mild = ifelse(((`Shortness of breath` %in% 1 | `Cough or sore throat` %in% 1) & `EQ5 usual activities` %in% 2 ) | 
                                          ((`Shortness of breath` %in% 1 | `Cough or sore throat` %in% 1) & `EQ5 usual activities` %in% 3 & 
                                             (`Fatigue (MFI00:general fatigue > 5)`+`Depression (PHQ > 9)`+ `Anxiety (GAD, ≥10 anxiety disorder)`)<2),1,0))
table(input$res_mild)
summary(input$res_mild)

#res_moderate
input <- mutate(input, res_moderate = ifelse(((`Shortness of breath` %in% 1 | `Cough or sore throat` %in% 1) & (`EQ5 usual activities` %in% 3) &  
                                                (`Fatigue (MFI00:general fatigue > 5)`+`Depression (PHQ > 9)`+ `Anxiety (GAD, ≥10 anxiety disorder)`)>1) | 
                                              ((`Shortness of breath` %in% 1 | `Cough or sore throat` %in% 1) & 
                                                (`EQ5 usual activities` %in% 4) &  ((`Fatigue (MFI00:general fatigue > 5)`+`Depression (PHQ > 9)`+ `Anxiety (GAD, ≥10 anxiety disorder)`)<2)),1,0))
#zero patients in this category
table(input$res_moderate)
summary(input$res_moderate)

#res_severe
input <- mutate(input, res_severe = ifelse(((`Shortness of breath` %in% 1 | `Cough or sore throat` %in% 1) & `EQ5 usual activities` %in% 4 &  
                                                (`Fatigue (MFI00:general fatigue > 5)`+`Depression (PHQ > 9)`+ `Anxiety (GAD, ≥10 anxiety disorder)`)>1) | 
                                               ((`Shortness of breath` %in% 1 | `Cough or sore throat` %in% 1) & 
                                                  (`EQ5 usual activities` %in% 5) &  ((`Fatigue (MFI00:general fatigue > 5)`+`Depression (PHQ > 9)`+ `Anxiety (GAD, ≥10 anxiety disorder)`)>0)),1,0))
#zero patients in this category
table(input$res_severe)
summary(input$res_severe)


#combine the cog mild and cog moderate together
a3 <- mutate(input, res_combine = ifelse((res_mild %in% 1 | res_moderate %in% 1 | res_severe %in% 1), 1, 0))
table(a3$res_combine)
summary(a3$res_combine)

#combine the resp, cognitive and fatigue categories 
a3 <- mutate(a3, Cog_Res = ifelse((cognitive %in% 1 & res_combine %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Fat = ifelse((cognitive %in% 1 & post_acute %in% 1), 1, 0 ))
a3 <- mutate(a3, Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1
                                       & cognitive %in% 1), 1, 0 ))
#all long term category
a3 <- mutate(a3, long_term = ifelse((cognitive %in% 1 | res_combine %in% 1 | post_acute %in% 1), 1, 0 ))
a3$overlap <- rowSums(a3[,c("res_mild","res_moderate","res_severe")])

#Handling the cases that the patients falling in more than one(mild, moderate, severe) category
#they always move to the more severe state
DT = as.data.table(a3)
DT[(overlap >1 & res_mild==res_moderate), res_mild := 0]
DT[(overlap >1 & res_moderate== res_severe), res_moderate := 0]
write.csv(DT, paste0("FILEPATH/Swe_all_data_marked_v", version, ".csv"))

#Change wide to long, fill in NAs
inter <- DT[,c('Sex(f=1, m=2)',"Agegroup 65", "post_acute","cog_mild","cog_moderate","cognitive","res_mild","res_moderate","res_severe","res_combine","Cog_Res",
               "Cog_Fat","Res_Fat","Cog_Res_Fat","long_term")]
inter[is.na(inter)] <- 0
data_long <- gather(inter , measure, value, c(post_acute:long_term) )

#by age >65 and Sex only
#since res_severe and res_moderate does not have any counts, the two columns were dropped
cocurr_mu <-data_long %>%
  group_by(`Sex(f=1, m=2)`, `Agegroup 65`,measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0
write.csv(cocurr_mu, paste0("FILEPATH/Swe_byAgeSex_", version, ".csv"))

#table by sex only 
cocurr_sex <-data_long %>% 
  group_by(`Sex(f=1, m=2)`,measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var) 

cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0
write.csv(cocurr_sex, paste0("FILEPATH/Swe_bySex_v", version, ".csv"))
