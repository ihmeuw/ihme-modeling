## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Get Proprotion from different datasets(Russia, Iran, Italy)
## Contributors: NAME
## Date 2/25/2021
## Time Stamp: change the denomenator of cognition(moderate and mild) and respiratory 
## to the sum of cognition_all
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
version <- 2

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k ,"FILEPATH/get_location_metadata.R"))
source(paste0(roots$k ,"FILEPATH/get_age_metadata.R"))

#Read in CoFlow
Co_flow <- read_excel("FILEPATH/long_covid_extraction_02.24.21_COFLOW.xlsx")
Co_flow$mean <- Co_flow$cases/Co_flow$sample_size
Co_flow_sim <- Co_flow[,c('symptom_cluster',"mean")]

#Read in the Russia proportion
Rus_pre <- read.csv("FILEPATH/Russia_bySex_v10.csv")
Rus_wo_pre <- read.csv("FILEPATH/Russia_bySex_wo_comparison_pre_v0.csv")

#this function converts the wide data to long and collapse to both sex
convert <- function(filename, prefix, start, end) {
  Temp_file <- subset(filename, select=-c(X))
  Temp_total<-Temp_file %>% 
    summarize_if(is.numeric, sum, na.rm=TRUE) %>%
    bind_rows(Temp_file) %>%
    mutate_at(vars(-N, -Sex), funs(. / N))
  
  data_long <- gather(Temp_total, condition, measurement, start:end, factor_key=TRUE)
  names(data_long)[names(data_long) == "measurement"] <- paste0("measurement",prefix)
  return(data_long)
}

Rus_pre<-convert(Rus_pre, "_pre", "Cog_Fat", "res_severe" )
Rus_wo_pre <- convert(Rus_wo_pre, "_wo_pre","Cog_Fat", "res_severe" )

#combine the pre and post into one dataset 
Russia<-cbind(Rus_pre, Rus_wo_pre[4])
Russia$source <-"Russia"
  
#read in Iran Proportion
Iran_pre <- read.csv("FILEPATH/Iran_bySex_v1.csv")
Iran_wo_pre <- read.csv("FILEPATH/Iran_without_comparison_bySex_v0.csv")
names(Iran_pre)[names(Iran_pre) == "Dm3"] <- "Sex"
names(Iran_wo_pre)[names(Iran_wo_pre) == "Dm3"] <- "Sex"

Iran_pre <-convert(Iran_pre, "_pre", "Cog_Fat", "res_severe" )
Iran_wo_pre <- convert(Iran_wo_pre, "_wo_pre", "Cog_Fat", "res_severe" )

Iran<-cbind(Iran_pre, Iran_wo_pre[4])
Iran$source <-"Iran"

##read in Italy Pediatric Proportion
Ita_ped_pre <- read.csv("FILEPATH/Italy_ped_bySex_v5.csv")
Ita_ped_wo_pre <- read.csv("FILEPATH/Italy_ped_bySex_wo_comparison_v0.csv")
setDT(Ita_ped_pre)[, sex:= dplyr::recode(sex, `1` = "male",`2`="female")]
setDT(Ita_ped_pre)[, sex:= dplyr::recode(sex, `1` = "male",`2`="female")]
names(Ita_ped_pre)[names(Ita_ped_pre) == "sex"] <- "Sex"
names(Ita_ped_wo_pre)[names(Ita_ped_wo_pre) == "sex"] <- "Sex"

Ita_ped_pre <-convert(Ita_ped_pre, "_pre","cog","Res_Fat")
Ita_ped_wo_pre <- convert(Ita_ped_wo_pre, "_wo_pre","cog","Res_Fat")

Ita_ped<-cbind(Ita_ped_pre , Ita_ped_wo_pre[5])
Ita_ped<-subset(Ita_ped, select=-c(follow_up_time))
Ita_ped$source <-"Italy Pediatric"

##read in Italy adult Proportion
Ita_adult_pre <- read.csv("FILEPATH/Italy_adult_bySex_v1.csv")
Ita_adult_wo_pre <- read.csv("FILEPATH/Italy_adult_bySex_wo_comparison_v1.csv")
setDT(Ita_adult_pre)[, sex:= dplyr::recode(sex, `1` = "male",`2`="female")]
setDT(Ita_adult_wo_pre)[, sex:= dplyr::recode(sex, `1` = "male",`2`="female")]
Ita_adult_wo_pre$Cog_Res <-0

setcolorder(Ita_adult_wo_pre, c("X","sex","cog","Cog_Res","long_term","res","N","follow_up_time"))
names(Ita_adult_pre)[names(Ita_adult_pre) == "sex"] <- "Sex"
names(Ita_adult_wo_pre)[names(Ita_adult_wo_pre) == "sex"] <- "Sex"
#took the line out because it does not exist in the post one
#Ita_adult_pre <-subset(Ita_adult_pre, select=-c(Cog_Res))

Ita_adult_pre <-convert(Ita_adult_pre, "_pre","cog","res")
Ita_adult_wo_pre <- convert(Ita_adult_wo_pre, "_wo_pre","cog","res")

Ita_adult<-cbind(Ita_adult_pre, Ita_adult_wo_pre[5])
Ita_adult<-subset(Ita_adult, select=-c(follow_up_time))
Ita_adult$source <-"Italy Adult"

Italy <- rbind(Ita_ped, Ita_adult)
Italy <- Italy[c("N","Sex","condition","measurement_pre","measurement_wo_pre","source")]

#bind all datasets together
all <- rbind(Italy, Russia, Iran)
all$Sex <- all$Sex %>% replace_na("Both")

#transform 
all$ratio <- all$measurement_pre/all$measurement_wo_pre
all$log_ratio <- log(all$measurement_pre/all$measurement_wo_pre)
colnames(all)
all$pre_cases <-all$N*all$measurement_pre
all$wo_pre_cases <- all$N*all$measurement_wo_pre

# setwd("FILEPATH")
# write.csv(all, "proportion.csv")
