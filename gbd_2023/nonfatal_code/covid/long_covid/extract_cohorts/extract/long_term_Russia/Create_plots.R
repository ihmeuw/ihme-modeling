## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Create plots for Russian Data
## Contributors: NAME
## Date 2/1/2021
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

#library(scales)
version <- 0

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k ,"FILEPATH/get_location_metadata.R"))
source(paste0(roots$k ,"FILEPATH/get_age_metadata.R"))

data <- read.csv('FILEPATH/Russian_byAgeSex_10.csv')

full <- read.csv('FILEPATH/Russia_all_data_marked_v10.csv')
table(full$Sex, full$post_acute)
table(full$Sex, full$cognitive)
table(full$Sex, full$res_combine)

dt <- data[,c('age_group_name', "Sex", "post_acute", 'cognitive', "res_combine","N")]
dt_long <- gather(dt, condition, measurement, post_acute:res_combine)
dt_long$percent <-dt_long$measurement/dt_long$N


getwd()

pdf(file= 'Russia_data.pdf', height=11, width=8.5)
#combined categories
Post <- ggplot(data =data) +
  geom_col(mapping = aes(x=age_group_name, y=post_acute)) +
  facet_wrap('Sex') +
  labs(x='', y='Post Fatigue Syndrome', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Cog <-ggplot(data = data) +
  geom_col(mapping = aes(x=age_group_name, y=cognitive)) +
  facet_wrap('Sex') +
  labs(x='', y='Cognition', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Res <- ggplot(data = data) +
  geom_col(mapping = aes(x=age_group_name, y=res_combine)) +
  facet_wrap('Sex') +
  labs(x='', y='Respiratory', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))


grid.arrange(Post, Cog, Res, ncol=1)

#stack bar, and three bars 
stack_bar_all_percent <- ggplot(data = dt_long) +
  geom_col(mapping = aes(x=age_group_name, y=percent, fill=condition)) +
  facet_wrap('Sex') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

Stack_bar_all_count <- ggplot(data = dt_long) +
  geom_col(mapping = aes(x=age_group_name, y=measurement, fill=condition)) +
  facet_wrap('Sex') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

grid.arrange(stack_bar_all_percent, Stack_bar_all_count, ncol=1)

three_bars_percent <- ggplot(data= dt_long, aes(x=age_group_name, y=percent, fill= condition))+
  geom_bar(stat="identity", position=position_dodge()) +
  facet_wrap('Sex') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

three_bars_discret <- ggplot(data= dt_long, aes(x=age_group_name, y=measurement, fill= condition))+
  geom_bar(stat="identity", position=position_dodge()) +
  facet_wrap('Sex') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

grid.arrange(three_bars_percent, three_bars_discret, ncol=1)

#use individual level 
full <- merge(full, data[,c('age_group_name', 'N','Sex')], all.x = TRUE)

full$cognitive_v <- factor(ifelse(full$cog_mild %in% 1, 'Cognitive_mild',
                                ifelse(full$cog_moderate %in% 1, 'Cognitive_moderate', 'None')),
                         levels=c('Cognitive_mild','Cognitive_moderate', 'None'))

full$res_v <- factor(ifelse(full$res_mild %in% 1, 'Res_mild',
                            ifelse(full$res_moderate%in% 1, 'Res_moderate',
                                   ifelse(full$res_severe %in% 1, 'Res_severe', 'None'))),
                     levels=c('Res_mild','Res_moderate','Res_severe'))


data_short <- data[, c('age_group_name','N', 'Sex','cog_mild','cog_moderate','res_mild','res_moderate','res_severe','res_combine')]
data_short$cognition <- factor(ifelse(data_short$cog_mild %in% 1, 'Cognitive_mild',
                                  ifelse(data_short$cog_moderate %in% 1, 'Cognitive_moderate', 'None')),
                           levels=c('Cognitive_mild','Cognitive_moderate', 'None'))

data_short$respiratory <- factor(ifelse(data_short$res_mild %in% 1, 'Res_mild',
                            ifelse(data_short$res_moderate%in% 1, 'Res_moderate',
                                   ifelse(data_short$res_severe %in% 1, 'Res_severe', 'None'))),
                     levels=c('Res_mild','Res_moderate','Res_severe'))



ggplot(data = full) +
  geom_col(mapping = aes(x=age_group_name, y=cognitive/N, fill=cognitive_v)) +
  facet_wrap('Sex') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())+
  labs(x='', y='Percent', fill='cognition')

ggplot(data = full) +
  geom_col(mapping = aes(x=age_group_name, y=cognitive, fill=cognitive_v)) +
  facet_wrap('Sex') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  labs(x='', y='counts', fill='cognition')

ggplot(data = full) +
  geom_col(mapping = aes(x=age_group_name, y=res_combine/N, fill=res_v)) +
  facet_wrap('Sex') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())+
  labs(x='', y='Percent', fill='respiratory')

ggplot(data = full) +
  geom_col(mapping = aes(x=age_group_name, y=res_combine, fill=res_v)) +
  facet_wrap('Sex') +
  theme(axis.text.x = element_text(hjust=1, angle=45))
dev.off()


