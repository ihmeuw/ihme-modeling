#--------------------------------------------------------------
# Name: 
# Date: 27 Feb 2021
# Project: GBD nonfatal COVID
# Purpose: estimate long COVID duration among mild/moderate cases and among hospital cases
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())
setwd("FILEPATH")

# load packages
pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)
library(plyr)


folder <- 'FILEPATH'
outputfolder <- 'FILEPATH'

# main symptom clusters
version_a <- 78
# overlap
version_b <- 78
# severities
version_sev <- 78

save <- 1

estimation_years <- c(2020, 2021, 2022, 2023)
follow_up_days_keep <- ifelse((((estimation_years - 1) %% 4) == 0), (estimation_years-2020) * 366, (estimation_years - 2020) * 365)
follow_up_days_keep[2:length(estimation_years)] <- round(follow_up_days_keep[2:4] - (366 / 2), 0)
follow_up_days_keep_c <- 90 - 9
follow_up_days_keep_h <- 90 - 30
follow_up_days_keep_i <- 90 - 37
follow_up_days_keep <- c(0, follow_up_days_keep_c, follow_up_days_keep_h, follow_up_days_keep_i)

for(out in c('any', 'cog', 'fat', 'rsp')) {
  model_dir <- paste0(out, '_v', version_a, '/')
  draws1 <- data.table(read.csv(paste0(outputfolder, model_dir, 'predictions_draws_hospital_icu_all.csv')))
  draws2 <- data.table(read.csv(paste0(outputfolder, model_dir, 'predictions_draws_community_all.csv')))
  draws1 <- unique(draws1[follow_up_days %in% follow_up_days_keep & (male==1 | female==1)])
  draws2 <- draws2[follow_up_days %in% follow_up_days_keep]
  if (!('children' %in% names(draws1))) {
    temp <- copy(draws1)
    temp$children <- 0
    draws1$children <- 1
    draws1 <- rbind(draws1, temp)
    rm(temp)
  } 
  table(draws1$children, draws1$female)
  table(draws2$children, draws2$female)
  draws2_kids <- copy(draws2[draws2$children==1 & draws2$female==0,])
  draws2_kids$female <- 1
  draws2 <- rbind(draws2[(draws2$children==1 & draws2$female==0) | draws2$children==0,], draws2_kids)
  table(draws2$children, draws2$female)
  
  draws <- rbind(draws1, draws2)
  draws$outcome <- out
  try(draws$follow_up_days_comm <- NULL)
  try(draws$follow_up_days_hosp <- NULL)
  try(draws$male <- NULL)
  try(draws$other_list <- NULL)
  try(draws$memory_problems <- NULL)
  try(draws$fatigue <- NULL)
  try(draws$administrative <- NULL)
  try(draws$cough <- NULL)
  try(draws$shortness_of_breath <- NULL)
  
  if(out=='any') df <- copy(draws)
  else df <- rbind(df, draws)
}
  table(df$children, df$female)
  draws_clusters <- copy(df)
  df <- copy(draws_clusters)
  

  draws_overlap <- data.table()
for(out in c('cog_rsp', 'fat_cog', 'fat_rsp', 'fat_cog_rsp')) {
  model_dir <- paste0(out, 'FILEPATH')
  draws <- data.table(read.csv(paste0(outputfolder, model_dir, 'predictions_draws.csv')))
#  if (version_b < 53) {
#    draws <- draws[draws$follow_up_days %in% follow_up_days_keep,]
#  } 
  draws$outcome <- out
  try(draws$follow_up_days_comm <- NULL)
  try(draws$follow_up_days_hosp <- NULL)
  try(draws$other_list <- NULL)
  try(draws$memory_problems <- NULL)
  try(draws$fatigue <- NULL)
  try(draws$administrative <- NULL)
  try(draws$cough <- NULL)
  try(draws$shortness_of_breath <- NULL)
  draws$female <- 0
  draws2 <- copy(draws)
  draws2$female <- 1
  draws <- rbind(draws, draws2)
  
  drawsbind <- data.table()
  for (d in follow_up_days_keep) {
    draws$follow_up_days <- d
    drawsbind <- rbind(draws, drawsbind)
  }
  draws <- copy(drawsbind)
  
  draws$hospital <- draws$hospital_icu
  draws$icu <- 0
  draws$hospital_icu <- NULL

  # add children
  draws$children <- 0
  draws2 <- copy(draws)
  draws2$children <- 1
  draws <- rbind(draws, draws2)
  
  # add icu
  temp <- draws[hospital==1]
  temp$icu <- 1
  temp$hospital <- 0
  draws <- rbind(draws, temp)
  draws_overlap <- rbind(draws_overlap, draws)
}
  df <- rbind(df, draws_overlap)
  draws_overlap[1:10,c(1:10,999:1007)]
  df[1:10,c(1:10,999:1007)]
  table(draws_overlap$outcome, draws_overlap$children)

  draws_sev <- data.table()
for(out in c('mild_cog', 'mod_cog', 'mild_rsp', 'mod_rsp', 'sev_rsp')) {
  model_dir <- paste0(out, 'FILEPATH')
  draws <- data.table(read.csv(paste0(outputfolder, model_dir, 'predictions_draws.csv')))
  draws$outcome <- out
  try(draws$follow_up_days_comm <- NULL)
  try(draws$follow_up_days_hosp <- NULL)
  try(draws$other_list <- NULL)
  try(draws$memory_problems <- NULL)
  try(draws$fatigue <- NULL)
  try(draws$administrative <- NULL)
  try(draws$cough <- NULL)
  try(draws$shortness_of_breath <- NULL)
  draws$female <- 0
  draws2 <- copy(draws)
  draws2$female <- 1
  
  draws <- rbind(draws, draws2)
  
  drawsbind <- data.table()
  for (d in follow_up_days_keep) {
    draws$follow_up_days <- d
    drawsbind <- rbind(draws, drawsbind)
  }
  drawsbind$hospital <- drawsbind$hospital_icu
  drawsbind$hospital_icu <- NULL
  drawsbind$icu <- 0
  temp <- drawsbind[hospital==1]
  temp$icu <- 1
  temp$hospital <- 0
  temp$hospital_icu <- NULL
  drawsbind <- rbind(drawsbind, temp)

  drawsbind$children <- 0
  drawsbind2 <- copy(drawsbind)
  drawsbind2$children <- 1
  drawsbind <- rbind(drawsbind, drawsbind2)
  
  draws_sev <- rbind(draws_sev, drawsbind)
}
  dim(draws_sev)
  dim(unique(draws_sev))
  df <- rbind(df, draws_sev)
draws_sev[1:30,c(1:10,999:1007)]


# keep relevant follow-up points for each initial severity
df <- data.table(df)
df <- df[(hospital==0 & icu==0 & follow_up_days %in% c(0, 81)) |
           (hospital==1 & icu==0 & follow_up_days %in% c(0, 60)) |
           (hospital==0 & icu==1 & follow_up_days %in% c(0, 53))]

df$X <- NULL
df <- melt(df, id.vars = c('hospital', 'icu', 'female', 'outcome', 'children', 'follow_up_days'))
df <- unique(df)
df <- reshape(df, idvar = c('hospital', 'icu', 'female', 'children', 'variable', 'follow_up_days'), timevar = 'outcome', direction = 'wide', sep='_')

df$mild_cog <- df$value_mild_cog / (df$value_mild_cog + df$value_mod_cog)
df$mod_cog <- df$value_mod_cog / (df$value_mild_cog + df$value_mod_cog)
df$mild_rsp <- df$value_mild_rsp / (df$value_mild_rsp + df$value_mod_rsp + df$value_sev_rsp)
df$mod_rsp <- df$value_mod_rsp / (df$value_mild_rsp + df$value_mod_rsp + df$value_sev_rsp)
df$sev_rsp <- df$value_sev_rsp / (df$value_mild_rsp + df$value_mod_rsp + df$value_sev_rsp)

df_sev <- df[, c('hospital', 'icu', 'female', 'children', 'follow_up_days', 'variable', 'mild_cog', 'mod_cog', 'mild_rsp', 'mod_rsp', 'sev_rsp')]


df$value_any[df$value_any>1] <- 1
# put all proportions in "among COVID patients" space rather than "among long COVID cases"
df$fcr <- df$value_fat_cog_rsp * df$value_any
df$fr <- df$value_fat_rsp * df$value_any
df$fc <- df$value_fat_cog * df$value_any
df$cr <- df$value_cog_rsp * df$value_any

# subtract to make all outcome mutually exclusive
if (version_a < 40) {
  df$fr <- df$fr - df$fcr
  df$fc <- df$fc - df$fcr
  df$cr <- df$cr - df$fcr
  df$f <- df$value_fat - df$fc - df$fr - df$fcr
  df$r <- df$value_rsp - df$cr - df$fr - df$fcr
  df$c <- df$value_cog - df$fc - df$cr - df$fcr
} else {
  df$f <- df$value_fat - df$fc - df$fr - df$fcr
  df$r <- df$value_rsp - df$cr - df$fr - df$fcr
  df$c <- df$value_cog - df$fc - df$cr - df$fcr
}


# check for negative draws and cap at 0
for (v in c('fcr', 'fc', 'fr', 'cr', 'f', 'c', 'r')) {
  # count rows below min
  n <- nrow(df[get(v) < 0])
  df[, eval(paste0(v, 'low')) := 0]
  df[get(v) < 0, eval(paste0(v, 'low')) := 1]
  df[get(v) < 0, eval(v) := 0]
  nlow <- nrow(df[get(paste0(v, 'low')) == 1])
}

table(df$fcrlow, df$follow_up_days)
table(df$frlow, df$follow_up_days)
table(df$fclow, df$follow_up_days)
table(df$crlow, df$follow_up_days)
table(df$flow, df$follow_up_days)
table(df$rlow, df$follow_up_days)
table(df$clow, df$follow_up_days)
table(df$flow[df$hospital==0 & df$icu==0 & df$female==0 & df$children==0], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==0 & df$children==0])
table(df$flow[df$hospital==0 & df$icu==0 & df$female==0 & df$children==1], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==0 & df$children==1])
table(df$flow[df$hospital==0 & df$icu==0 & df$female==1 & df$children==0], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==1 & df$children==0])
table(df$flow[df$hospital==0 & df$icu==0 & df$female==1 & df$children==1], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==1 & df$children==1])
table(df$flow[df$hospital==1 & df$female==0 & df$children==0], df$follow_up_days[df$hospital==1 & df$female==0 & df$children==0])
table(df$flow[df$hospital==1 & df$female==0 & df$children==1], df$follow_up_days[df$hospital==1 & df$female==0 & df$children==1])
table(df$flow[df$hospital==1 & df$female==1 & df$children==0], df$follow_up_days[df$hospital==1 & df$female==1 & df$children==0])
table(df$flow[df$hospital==1 & df$female==1 & df$children==1], df$follow_up_days[df$hospital==1 & df$female==1 & df$children==1])
table(df$flow[df$icu==1 & df$female==0 & df$children==1], df$follow_up_days[df$icu==1 & df$female==0 & df$children==0])
table(df$flow[df$icu==1 & df$female==0 & df$children==0], df$follow_up_days[df$icu==1 & df$female==0 & df$children==1])
table(df$flow[df$icu==1 & df$female==1 & df$children==1], df$follow_up_days[df$icu==1 & df$female==1 & df$children==0])
table(df$flow[df$icu==1 & df$female==1 & df$children==0], df$follow_up_days[df$icu==1 & df$female==1 & df$children==1])
table(df$rlow[df$hospital==0 & df$icu==0 & df$female==0 & df$children==0], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==0 & df$children==0])
table(df$rlow[df$hospital==0 & df$icu==0 & df$female==0 & df$children==1], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==0 & df$children==1])
table(df$rlow[df$hospital==0 & df$icu==0 & df$female==1 & df$children==0], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==1 & df$children==0])
table(df$rlow[df$hospital==0 & df$icu==0 & df$female==1 & df$children==1], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==1 & df$children==1])
table(df$rlow[df$hospital==1 & df$female==0 & df$children==0], df$follow_up_days[df$hospital==1 & df$female==0 & df$children==0])
table(df$rlow[df$hospital==1 & df$female==0 & df$children==1], df$follow_up_days[df$hospital==1 & df$female==0 & df$children==1])
table(df$rlow[df$hospital==1 & df$female==1 & df$children==0], df$follow_up_days[df$hospital==1 & df$female==1 & df$children==0])
table(df$rlow[df$hospital==1 & df$female==1 & df$children==1], df$follow_up_days[df$hospital==1 & df$female==1 & df$children==1])
table(df$rlow[df$icu==1 & df$female==0 & df$children==1], df$follow_up_days[df$icu==1 & df$female==0 & df$children==0])
table(df$rlow[df$icu==1 & df$female==0 & df$children==0], df$follow_up_days[df$icu==1 & df$female==0 & df$children==1])
table(df$rlow[df$icu==1 & df$female==1 & df$children==1], df$follow_up_days[df$icu==1 & df$female==1 & df$children==0])
table(df$rlow[df$icu==1 & df$female==1 & df$children==0], df$follow_up_days[df$icu==1 & df$female==1 & df$children==1])
table(df$clow[df$hospital==0 & df$icu==0 & df$female==0 & df$children==0], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==0 & df$children==0])
#table(df$clow[df$hospital==0 & df$icu==0 & df$female==0 & df$children==1], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==0 & df$children==1])
table(df$clow[df$hospital==0 & df$icu==0 & df$female==1 & df$children==0], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==1 & df$children==0])
#table(df$clow[df$hospital==0 & df$icu==0 & df$female==1 & df$children==1], df$follow_up_days[df$hospital==0 & df$icu==0 & df$female==1 & df$children==1])
table(df$clow[df$hospital==1 & df$female==0 & df$children==0], df$follow_up_days[df$hospital==1 & df$female==0 & df$children==0])
table(df$clow[df$hospital==1 & df$female==0 & df$children==1], df$follow_up_days[df$hospital==1 & df$female==0 & df$children==1])
table(df$clow[df$hospital==1 & df$female==1 & df$children==0], df$follow_up_days[df$hospital==1 & df$female==1 & df$children==0])
table(df$clow[df$hospital==1 & df$female==1 & df$children==1], df$follow_up_days[df$hospital==1 & df$female==1 & df$children==1])
table(df$clow[df$icu==1 & df$female==0 & df$children==1], df$follow_up_days[df$icu==1 & df$female==0 & df$children==0])
table(df$clow[df$icu==1 & df$female==0 & df$children==0], df$follow_up_days[df$icu==1 & df$female==0 & df$children==1])
table(df$clow[df$icu==1 & df$female==1 & df$children==1], df$follow_up_days[df$icu==1 & df$female==1 & df$children==0])
table(df$clow[df$icu==1 & df$female==1 & df$children==0], df$follow_up_days[df$icu==1 & df$female==1 & df$children==1])


df$sum <- df$fcr + df$fr + df$fc + df$cr + df$f + df$r + df$c

summary_pre <- df[,.(mean(fcr),mean(fr),mean(fc), mean(cr), mean(value_fat), mean(value_rsp), mean(value_cog), mean(value_any), mean(f), mean(r), mean(c), mean(sum)), 
                  by = c('hospital', 'icu', 'female', 'children', 'follow_up_days')]
setnames(summary_pre, c('V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10', 'V11', 'V12'), 
         c('fcr', 'fr', 'fc', 'cr', 'value_fat', 'value_rsp', 'value_cog', 'value_any', 'f', 'r', 'c', 'sum'))

write.csv(summary_pre, paste0(outputfolder, 'FILEPATH', version_a, '.csv'))

#df_pre <- df[,c('follow_up_days', 'hospital', 'icu', 'female', 'variable', 'f', 'c', 'r', 'fc', 'fr', 'cr', 'fcr', 'value_any', 'sum')]
#df_pre <- melt(df_pre, id.vars = c('follow_up_days', 'hospital', 'icu', 'female', 'variable'))
#setnames(df_pre, 'variable.1', 'outcome')
#setnames(df_pre, 'value', 'proportion')
#summary_pre <- df_pre[,.(mean(proportion),quantile(proportion,0.025, na.rm=TRUE),quantile(proportion,0.975, na.rm=TRUE)), 
#                      by = c('hospital', 'icu', 'female', 'follow_up_days', 'outcome')]
#setnames(summary_pre, c('V1', 'V2', 'V3'), c('mean', 'lower', 'upper'))
#write.csv(summary_pre, paste0(outputfolder, 'FILEPATH/proportions_pre_post_adjustment2.csv'))


df$fcr <- df$fcr * df$value_any / df$sum
df$fc <- df$fc * df$value_any / df$sum
df$fr <- df$fr * df$value_any / df$sum
df$cr <- df$cr * df$value_any / df$sum
df$f <- df$f * df$value_any / df$sum
df$c <- df$c * df$value_any / df$sum
df$r <- df$r * df$value_any / df$sum



df$sum2 <- df$fcr + df$fr + df$fc + df$cr + df$f + df$r + df$c

summary <- df[,.(mean(sum),mean(sum2)), 
              by = c('hospital', 'icu', 'female', 'children', 'follow_up_days')]
setnames(summary, c('V1', 'V2'), 
         c('raw_sum', 'adjusted_sum'))

plot(summary$raw_sum, summary$adjusted_sum)
plot(df$sum2, df$sum)

df$c_mild <- df$c * df$mild_cog
df$c_mod <- df$c * df$mod_cog
df$r_mild <- df$r * df$mild_rsp
df$r_mod <- df$r * df$mod_rsp
df$r_sev <- df$r * df$sev_rsp
df$fc_mild <- df$fc * df$mild_cog
df$fc_mod <- df$fc * df$mod_cog
df$fr_mild <- df$fr * df$mild_rsp
df$fr_mod <- df$fr * df$mod_rsp
df$fr_sev <- df$fr * df$sev_rsp
df$cr_mild_mild <- df$cr * df$mild_cog * df$mild_rsp
df$cr_mild_mod <- df$cr * df$mild_cog * df$mod_rsp
df$cr_mild_sev <- df$cr * df$mild_cog * df$sev_rsp
df$cr_mod_mild <- df$cr * df$mod_cog * df$mild_rsp
df$cr_mod_mod <- df$cr * df$mod_cog * df$mod_rsp
df$cr_mod_sev <- df$cr * df$mod_cog * df$sev_rsp
df$fcr_mild_mild <- df$fcr * df$mild_cog * df$mild_rsp
df$fcr_mild_mod <- df$fcr * df$mild_cog * df$mod_rsp
df$fcr_mild_sev <- df$fcr * df$mild_cog * df$sev_rsp
df$fcr_mod_mild <- df$fcr * df$mod_cog * df$mild_rsp
df$fcr_mod_mod <- df$fcr * df$mod_cog * df$mod_rsp
df$fcr_mod_sev <- df$fcr * df$mod_cog * df$sev_rsp

df$sum3 <- df$f + df$c_mild + df$c_mod + df$r_mild + df$r_mod + df$r_sev + df$fc_mild + df$fc_mod + df$fr_mild + df$fr_mod + df$fr_sev + 
  df$cr_mild_mild + df$cr_mild_mod + df$cr_mild_sev + df$cr_mod_mild +  df$cr_mod_mod+ df$cr_mod_sev + 
  df$fcr_mild_mild + df$fcr_mild_mod + df$fcr_mild_sev + df$fcr_mod_mild + df$fcr_mod_mod + df$fcr_mod_sev

plot(df$sum2, df$sum3)


#df[follow_up_days > 0, follow_up_days := follow_up_days + (366 / 2)]

final_draws <- df[,c('follow_up_days', 'hospital', 'icu', 'female', 'children', 'variable', 'f', 'c_mild', 'c_mod', 'r_mild', 'r_mod', 'r_sev', 'fc_mild', 'fc_mod', 'fr_mild', 'fr_mod', 'fr_sev',
                     'cr_mild_mild', 'cr_mild_mod', 'cr_mild_sev', 'cr_mod_mild', 'cr_mod_mod', 'cr_mod_sev', 'fcr_mild_mild', 'fcr_mild_mod', 'fcr_mild_sev', 
                     'fcr_mod_mild', 'fcr_mod_mod', 'fcr_mod_sev')]

final_draws <- melt(final_draws, id.vars = c('follow_up_days', 'hospital', 'icu', 'female', 'children', 'variable'))
setnames(final_draws, 'variable.1', 'outcome')
setnames(final_draws, 'value', 'proportion')

summary <- final_draws[,.(mean(proportion),quantile(proportion,0.025, na.rm=TRUE),quantile(proportion,0.975, na.rm=TRUE)), 
                       by = c('hospital', 'icu', 'female', 'children', 'follow_up_days', 'outcome')]
setnames(summary, c('V1', 'V2', 'V3'), c('mean', 'lower', 'upper'))

if (save == 1) {
  write.csv(summary, paste0(outputfolder, 'final_proportion_summary.csv'))
}
summary
#View(summary)


#final_draws <- reshape(final_draws, idvar = c('hospital', 'icu', 'female', 'children', 'follow_up_days', 'outcome'), timevar = 'variable', direction = 'wide')
#setnames(final_draws, paste0('proportion.draw_', c(0:999)), paste0('draw_', c(0:999)))


head(final_draws)
dim(final_draws)
write.csv(final_draws, paste0(outputfolder, 'final_proportion_draws_v', version_a, '.csv'))

if (save == 1) {
  write.csv(final_draws, paste0(outputfolder, 'final_proportion_draws.csv'))
  write.csv(final_draws, file =paste0("FILEPATH/final_proportion_draws.csv"))
}


final_any <- df[,c('follow_up_days', 'hospital', 'icu', 'female', 'children', 'variable', 'value_any')]
final_any <- melt(final_any, id.vars = c('follow_up_days', 'hospital', 'icu', 'female', 'children', 'variable'))
setnames(final_any, 'variable.1', 'outcome')
setnames(final_any, 'value', 'proportion')

summary <- final_any[,.(mean(proportion),quantile(proportion,0.025, na.rm=TRUE),quantile(proportion,0.975, na.rm=TRUE)), 
                       by = c('hospital', 'icu', 'female', 'children', 'follow_up_days', 'outcome')]
setnames(summary, c('V1', 'V2', 'V3'), c('mean', 'lower', 'upper'))

if (save == 1) {
  write.csv(summary, paste0(outputfolder, 'final_proportion_summary_any.csv'))
}
summary
head(final_any)
dim(final_any)
write.csv(final_any, paste0(outputfolder, 'final_proportion_draws_any_v', version_a, '.csv'))

if (save == 1) {
  write.csv(final_any, paste0(outputfolder, 'final_proportion_draws_any.csv'))
  write.csv(final_any, file =paste0("FILEPATH/final_proportion_draws_any.csv"))
}




