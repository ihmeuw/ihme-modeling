## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Extract Zurich Data with pre and post
## Contributors: NAME
## Date:3/11/2021
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
version <- 0

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k, 'FILEPATH/get_ids.R'))
source(paste0(roots$k, 'FILEPATH/get_location_metadata.R'))
source(paste0(roots$k, 'FILEPATH/get_population.R'))

setwd("FILEPATH")

#import the pre and post extraction from the huge spreadsheet
full_extract <- read.xlsx('FILEPATH/long_covid_extraction_03.08.2021.xlsx', 
                        sheet='Sheet1')

zurich<- full_extract[(full_extract$title %in% "Zurich" & full_extract$sex %in% "Both" & full_extract$age_start %in% 0 & full_extract$age_end %in% 99),
                      c('title','sex','symptom_cluster','cases','sample_size','follow_up_value','follow_up_units','sample_population')]

zurich$follow_up_time <- paste0(zurich$follow_up_value,"-",zurich$follow_up_units)

zurich <- subset(zurich, select=-c(follow_up_value, follow_up_units, title))
#getting the sum
zurich$cases <-as.numeric(zurich$cases)
zurich_total<-zurich %>% 
  group_by(sex, follow_up_time, symptom_cluster, sample_population) %>%
  summarize_if(is.numeric, sum, na.rm=TRUE) 
zurich_total$measurement <- zurich_total$cases/zurich_total$sample_size
zurich_total$Group <- "measurement_pre"


#import the data without pre, with post only
post_only <- read.xlsx('FILEPATH/long_covid_extraction_02.23.21 Zurich without pre post subtraction.xlsx')
zurich_post<- post_only[(post_only$title %in% "Zurich" & post_only$sex %in% "Both" & post_only$age_start %in% 0 & post_only$age_end %in% 99),
                      c('title','sex','symptom_cluster','cases','sample_size','follow-up_value','follow-up_units','sample_population')]
zurich_post$follow_up_time <- paste0(zurich_post$`follow-up_value`,"-",zurich_post$`follow-up_units`)
zurich_post <- subset(zurich_post, select=-c(`follow-up_value`, `follow-up_units`, title))

zurich_post_total<-zurich_post %>% 
  group_by(sex, follow_up_time, symptom_cluster, sample_population) %>%
  summarize_if(is.numeric, sum, na.rm=TRUE) 
zurich_post_total$measurement <- zurich_post_total$cases/zurich_post_total$sample_size
zurich_post_total$Group <- "measurement_wo_pre"

zurich_all<-rbind(zurich_total,zurich_post_total )
#write.csv(zurich_all, "extracted_proportion_Zurich.csv")

#append the zurich data to the proportion data
zurich_all$source <-'Zurich'
zurich_all<- zurich_all[,c('cases','sex','symptom_cluster','Group','measurement','source','follow_up_time','sample_population')]


#import proportion data
proportion <- read.xlsx('FILEPATH/proportion.xlsx', sheet='What_we_want')
proportion$follow_up_time<-'' 
proportion$sample_population <-''

proportion_v1 <- rbindlist(list(proportion, zurich_all))
#setwd("FILEPATH")
#write.csv(proportion_v1, "proportion_v1.csv")
                       
