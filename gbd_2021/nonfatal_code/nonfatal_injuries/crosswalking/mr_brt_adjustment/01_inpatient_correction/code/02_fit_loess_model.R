# team: GBD Injuries
# project: crosswalking GBD 2020
# script: fit loess model to get smooth age pattern for admittance proportions

# load packages
library(gtools)
library('carData', lib.loc = 'FILEPATH')
library('car', lib.loc = 'FILEPATH')
library(locfit)
library(ggrepel)

# import proportions prepped in 01_find_proportions.py
props <- read.csv('FILEPATH')
props_agg <- props[,c('sex_id', 'age_start', 'proportion')]
props_agg <- aggregate(props_agg$proportion, by = list(sex_id = props_agg$sex_id, age_start = props_agg$age_start), FUN = sum)
colnames(props_agg)[colnames(props_agg) == 'x'] <- 'proportion'

# create a prediction data frame to use after building loess model
sex_id <- c(1, 2)
age_start <- c(1, seq(5, 95, 5))
year_id <- c(2002, 2003, 2004)
pred_df <- expand.grid(year_id, age_start, sex_id)
colnames(pred_df) <- c('year_id', 'age_start', 'sex_id')
pred_df$admitted <- NA
pred_df$total_ed <- NA
pred_df$proportion <- NA
pred_df$age_end <- ifelse(pred_df$age_start == 1, pred_df$age_start + 3, pred_df$age_start + 4)
pred_df <- pred_df[,c('sex_id', 'age_start', 'age_end', 'year_id', 'admitted', 'total_ed', 'proportion')]

# logit transform to keep all proportions between 0 and 1
props_agg$proportion <- logit(props_agg$proportion)

# fit loess model, predict proportions, and back-transfrom out of logit space
loess_mod <- loess(proportion ~ age_start + sex_id, data = props_agg, control=loess.control(surface="direct"), span = 0.5) 
pred_df$pred <- predict(loess_mod, newdata = pred_df)
props_agg$proportion <- expit(props_agg$proportion)
pred_df$pred <- expit(pred_df$pred)

# save smoothed proportions
write.csv(pred_df, 'FILEPATH', row.names = F)

# set-up for visualization
props_agg$sex <- ifelse(props_agg$sex_id == 1, 'Males', 'Females')
pred_df$sex <- ifelse(pred_df$sex_id == 1, 'Males', 'Females')

# plot raw data and proportions with information about cases and sample size
g <- ggplot() + 
  geom_point(data = props_agg, aes(x = age_start, y = proportion)) + 
  facet_grid(~ sex)  + 
  labs(x = 'Age', y = 'Proportion of cases admitted to the hospital', colour = 'Year') + theme(text = element_text(size = 15)) +
  geom_line(data = pred_df, aes(x = age_start, y = pred))
g

ggsave('FILEPATH')
