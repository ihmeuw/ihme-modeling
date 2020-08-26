################
# 7. Model TMREL
################

#Grab packages
library(data.table)
library(plyr)
library(dplyr)
library(parallel)
  
setwd('FILEPATH')
source("get_draws.R")
  
#Set options for TMREL calculation
sex <- c(1,2)
directory <- 'FILEPATH'
  
setwd(directory)
  
files <- list.files(directory)
  
causes <- as.numeric(unique(regmatches(files, regexpr("\\d{3}", files))))
  
id_field <- rep("cause_id", length(causes))
  
daly_draws <- NULL
for (i in 1:23){
  for (j in 1:2){

    #Go get Daly draws from GBD 2016, reshape long, then by draw create weight factors as share of daly rates
    single_cause <- get_draws(id_field[i], gbd_id=causes[i], source="dalynator", location_id=1, sex_id=sex[j], 
                                age_group_id=27, year_id=2016, measure_id=2, metric_id=3, num_workers=5, 
                                gbd_round_id=4, status='best') %>% 
                    data.table %>%
                    .[, c("measure_id", "metric_id") := NULL] 
    daly_draws <- rbind(daly_draws, single_cause)
}}
  

daly_draws <- melt(daly_draws, c("location_id", "year_id", "age_group_id", "sex_id", "cause_id"), 
                   value.name ="dalys", variable.name="draw")

daly_draws[, total_dalys := sum(.SD$dalys), by=c("draw")]
daly_draws[, weight_factor := dalys/total_dalys, by=c("draw")]

#Clean dataframe to match relative risks
daly_draws <- daly_draws[, .(draw, cause_id, weight_factor, sex_id)]
daly_draws[, draw:=as.numeric(gsub("draw_", "", draw))]

grab_and_format <- function(filepath){
  
  df <- fread(filepath)
  df <- df[, .(exposure, draw, rr)]
  
  c <- regmatches(filepath, regexpr("\\d{3}", filepath))
  s <- as.numeric(gsub("^rr_\\d{3}|.csv|_", "", filepath))
  
  df <- df[, `:=`(sex_id = s, cause_id = c)]
  
  if (is.na(s)){
    
    df_male <- copy(df)
    df_male <- df_male[, sex_id := 1]
    
    df_female <- copy(df)
    df_female <- df_female[, sex_id := 2]
    
    df <- rbind(df_male, df_female)
  }
  
  return(df)
}

rr <- rbindlist(lapply(files, grab_and_format))

#Expand long by draw, exposure, and cause. Merge with daly weights and calculate average rr for each exposure and draw.
rr <- data.table(join(rr, daly_draws, by=c("draw", "cause_id", "sex_id")))

rr <- rr[cause_id %in% causes,]
rr[, weighted_rr := rr*weight_factor]
rr[, all_cause_rr := sum(.SD$weighted_rr), by=c("draw", "exposure")]

rr[, min_all_cause_rr := min(.SD$all_cause_rr), by="draw"]
rr[, tmrel := .SD[all_cause_rr == min_all_cause_rr, exposure], by="draw"]

all_cause_rr <- unique(rr[, .(exposure, draw, all_cause_rr)]) %>%
  .[, `:=`(mean_all_cause_rr = mean(.SD$all_cause_rr),
           lower_all_cause_rr = quantile(.SD$all_cause_rr, 0.025),
           upper_all_cause_rr = quantile(.SD$all_cause_rr, 0.975)),
    by = "exposure"] %>%
  .[, .(exposure, mean_all_cause_rr, lower_all_cause_rr, upper_all_cause_rr)] %>%
  unique

tmrel_df <- unique(rr[, .(draw, tmrel, min_all_cause_rr)])

write.csv(all_cause_rr, 'FILEPATH', row.names = F)

library(ggplot2)

plotty <- ggplot(all_cause_rr, aes(y=mean_all_cause_rr, x=exposure)) + 
  geom_line() + 
  geom_ribbon(aes(ymin=lower_all_cause_rr, ymax=upper_all_cause_rr), fill='grey', alpha=0.6) + 
  labs(x="G/day of pure alcohol", y="Weighted \nall-cause risk", title="Weighted all-cause risk \nin g/day of pure alcohol") +
  theme_classic() +
  theme(plot.title = element_text(hjust=0.5), axis.title.y = element_text(angle=0, vjust=0.5)) 

plotty

#Plot density of TMREL

sum_stats <- summary(tmrel_df$tmrel)

ggplot(tmrel_df, aes(x=tmrel)) + 
  geom_histogram(bins=20, color='black') +
  labs(x="TMREL \n(in drinks per day)", y="Number of draws", title="Distribution of TMREL \n in g/day") +
  theme(plot.title = element_text(hjust=0.5), axis.title.y = element_text(angle=0, vjust=0.5))
