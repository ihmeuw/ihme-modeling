#--------------------------------------------------------------
# Name: NAME (USERNAME)
# Date: 27 Feb 2021
# Project: GBD nonfatal COVID
# Purpose: estimate long COVID duration among mild/moderate cases and among hospital cases
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())
setwd("FILEPATH")

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH'
  h_root <- '~/'
} else {
  j_root <- 'FILEPATH/'
  h_root <- 'FILEPATH/'
}


# load packages
pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)
library(plyr)


folder <- 'FILEPATH'
outputfolder <- 'FILEPATH'
#outputfolder <- 'FILEPATH'

# main symptom clusters
version_a <- 117
# overlap
version_b <- 117
# severities
version_sev <- 117

save <- 1

estimation_years <- c(2020, 2021, 2022, 2023, 2024)

follow_up_days_keep <- ifelse(
  (((estimation_years - 1) %% 4) == 0), 
  (estimation_years-2020) * 366, 
  (estimation_years - 2020) * 365)

follow_up_days_keep[2:length(estimation_years)] <- round(
  follow_up_days_keep[2:4] - (366 / 2), 0)

follow_up_days_subtract_c <- 9
follow_up_days_subtract_h <- 30
follow_up_days_subtract_i <- 37

follow_up_days_keep_pipeline <- c(
  0, 90-follow_up_days_subtract_c, 90-follow_up_days_subtract_h, 90-follow_up_days_subtract_i,
  (365 - follow_up_days_subtract_c), (365 - follow_up_days_subtract_h), (365 - follow_up_days_subtract_i)
)

follow_up_days_keep <- c(
  0, 90-follow_up_days_subtract_c, 90-follow_up_days_subtract_h, 90-follow_up_days_subtract_i,
  (365 - follow_up_days_subtract_c), (365 - follow_up_days_subtract_h), (365 - follow_up_days_subtract_i),
  (366 / 2 - follow_up_days_subtract_c), (366 / 2 - follow_up_days_subtract_h), (366 / 2 - follow_up_days_subtract_i)
)

for(out in c('any_main', 'cog', 'fat', 'rsp')) {
  
  model_dir <- paste0(out, '_v', version_a, '/')
  
  draws1 <- fread(
    file = file.path(outputfolder, model_dir, 
                     "predictions_draws_hospital_icu_all.csv")
  )
  
  draws2 <- fread(
    file = file.path(outputfolder, model_dir, 
                     "predictions_draws_community_all.csv")
  )
  
  draws1 <- unique(draws1[follow_up_days %in% follow_up_days_keep & 
                            (male == 1 | female == 1)])
  
  draws2 <- draws2[follow_up_days %in% follow_up_days_keep]
  
  if (!('children' %in% names(draws1))) {
    temp <- copy(draws1)
    
    temp[, children := 0]
    draws1[, children := 1]
    
    draws1 <- rbind(draws1, temp)
    rm(temp)
  } 
  
  table(draws1$children, draws1$female)
  table(draws2$children, draws2$female)
  
  draws2_kids <- copy(draws2[children == 1 & female == 0])
  draws2_kids[, female := 1]
  
  draws2 <- rbind(
    draws2[(children == 1 & female == 0) | children == 0],
    draws2_kids
  )
  
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
  try(draws$proportion.data_id <- NULL)
  try(draws$proportion.mean <- NULL)
  try(draws$proportion.standard_error <- NULL)
  
  
  if(out == "any_main") {
    df <- copy(draws)
  } else {
    df <- rbind(df, draws)
  }
}

table(df$children, df$female)

# keep relevant follow-up points for each initial severity
df <- df[
  (hospital == 0 & icu == 0 & follow_up_days %in% c(0, 90-follow_up_days_subtract_c, (366 / 2 - follow_up_days_subtract_c), 
                                                    (365 - follow_up_days_subtract_c))) |
    (hospital == 1 & icu == 0 & follow_up_days %in% c(0, 90-follow_up_days_subtract_h, (366 / 2 - follow_up_days_subtract_h), 
                                                      (365 - follow_up_days_subtract_h))) |
    (hospital == 0 & icu == 1 & follow_up_days %in% c(0, 90-follow_up_days_subtract_i, (366 / 2 - follow_up_days_subtract_i), 
                                                      (365 - follow_up_days_subtract_i)))]

draws_overlap <- data.table()

for(out in c('cog_rsp', 'fat_cog', 'fat_rsp', 'fat_cog_rsp')) {
  
  model_dir <- paste0(out, '_v', version_b, '/')
  
  draws <- fread(
    file = file.path(outputfolder, model_dir, 'predictions_draws.csv'))
  

  draws[, outcome := out]
  
  try(draws$follow_up_days_comm <- NULL)
  try(draws$follow_up_days_hosp <- NULL)
  try(draws$other_list <- NULL)
  try(draws$memory_problems <- NULL)
  try(draws$fatigue <- NULL)
  try(draws$administrative <- NULL)
  try(draws$cough <- NULL)
  try(draws$shortness_of_breath <- NULL)
  try(draws$proportion.data_id <- NULL)
  try(draws$proportion.mean <- NULL)
  try(draws$proportion.standard_error <- NULL)
  
  draws[, female := 0]
  
  draws2 <- copy(draws)
  draws2[, female := 1]
  
  draws <- rbind(draws, draws2)
  
  drawsbind <- data.table()
  
  for (d in follow_up_days_keep) {
    draws[, follow_up_days := d]
    drawsbind <- rbind(draws, drawsbind)
  }
  
  draws <- copy(drawsbind)
  
  draws[, hospital := hospital_icu]
  draws[, icu := 0]
  draws[, hospital_icu := NULL]
  
  # add children
  draws[, children := 0]
  draws2 <- copy(draws)
  draws2[, children := 1]
  draws <- rbind(draws, draws2)
  
  # add icu
  temp <- draws[hospital == 1]
  temp[, icu := 1]
  temp[, hospital := 0]
  
  draws <- rbind(draws, temp)
  draws_overlap <- rbind(draws_overlap, draws)
}

df <- rbind(df, draws_overlap)
df <- df[(hospital==0 & icu==0 & follow_up_days %in% c(0, 90-follow_up_days_subtract_c, (366/2-follow_up_days_subtract_c), 
                                                       (365-follow_up_days_subtract_c))) |
           (hospital==1 & icu==0 & follow_up_days %in% c(0, 90-follow_up_days_subtract_h, (366/2-follow_up_days_subtract_h), 
                                                         (365-follow_up_days_subtract_h))) |
           (hospital==0 & icu==1 & follow_up_days %in% c(0, 90-follow_up_days_subtract_i, (366/2-follow_up_days_subtract_i), 
                                                         (365-follow_up_days_subtract_i)))]

draws_overlap[1:10, c(1:10, 999:1007)]
df[1:10, c(1:6, 1004:1007)]

table(draws_overlap$outcome, draws_overlap$children)

draws_sev <- data.table()

for(out in c('mild_cog', 'mod_cog', 'mild_rsp', 'mod_rsp', 'sev_rsp')) {
  
  model_dir <- paste0(out, '_v', version_sev, '/')
  draws <- fread(
    file = file.path(outputfolder, model_dir, 'predictions_draws.csv'))
  
  draws[, outcome := out]
  
  try(draws$follow_up_days_comm <- NULL)
  try(draws$follow_up_days_hosp <- NULL)
  try(draws$other_list <- NULL)
  try(draws$memory_problems <- NULL)
  try(draws$fatigue <- NULL)
  try(draws$administrative <- NULL)
  try(draws$cough <- NULL)
  try(draws$shortness_of_breath <- NULL)
  try(draws$proportion.data_id <- NULL)
  try(draws$proportion.mean <- NULL)
  try(draws$proportion.standard_error <- NULL)
  
  draws[, female := 0]
  draws2 <- copy(draws)
  draws2[, female := 1]
  
  draws <- rbind(draws, draws2)
  
  drawsbind <- data.table()
  for (d in follow_up_days_keep) {
    draws[, follow_up_days := d]
    drawsbind <- rbind(draws, drawsbind)
  }
  
  drawsbind[, hospital := hospital_icu]
  drawsbind[, hospital_icu := NULL]
  drawsbind[, icu := 0]
  
  temp <- drawsbind[hospital == 1]
  temp[, icu := 1]
  temp[, hospital := 0]
  temp[, hospital_icu := NULL]
  
  drawsbind <- rbind(drawsbind, temp)
  
  drawsbind[, children := 0]
  drawsbind2 <- copy(drawsbind)
  drawsbind2[, children := 1]
  
  drawsbind <- rbind(drawsbind, drawsbind2)
  
  draws_sev <- rbind(draws_sev, drawsbind)
}

dim(draws_sev)

df <- rbind(df, draws_sev)

df <- df[
  (hospital == 0 & icu == 0 & follow_up_days %in% c(0, 90-follow_up_days_subtract_c, (366 / 2 - follow_up_days_subtract_c), 
                                                    (365 - follow_up_days_subtract_c))) |
    (hospital == 1 & icu == 0 & follow_up_days %in% c(0, 90-follow_up_days_subtract_h, (366 / 2 - follow_up_days_subtract_h), 
                                                      (365 - follow_up_days_subtract_h))) |
    (hospital == 0 & icu == 1 & follow_up_days %in% c(0, 90-follow_up_days_subtract_i, (366 / 2 - follow_up_days_subtract_i), 
                                                      (365 - follow_up_days_subtract_i)))]

draws_sev[1:30, c(1:10, 999:1007)]


df[, V1 := NULL]

df <- melt(df,
           id.vars = c("hospital", "icu", "female", "outcome", "children", 
                       "follow_up_days"))

df <- unique(df)

df <- reshape(df,
              idvar = c("hospital", "icu", "female", "children", "variable", 
                        "follow_up_days"),
              timevar = "outcome", direction = "wide", sep = "_")

df[, mild_cog := value_mild_cog / (value_mild_cog + value_mod_cog)]
df[, mod_cog := value_mod_cog / (value_mild_cog + value_mod_cog)]
df[, mild_rsp := value_mild_rsp / (value_mild_rsp + value_mod_rsp + value_sev_rsp)]
df[, mod_rsp := value_mod_rsp / (value_mild_rsp + value_mod_rsp + value_sev_rsp)]
df[, sev_rsp := value_sev_rsp / (value_mild_rsp + value_mod_rsp + value_sev_rsp)]

df_sev <- df[, c('hospital', 'icu', 'female', 'children', 'follow_up_days', 
                 'variable', 'mild_cog', 'mod_cog', 'mild_rsp', 'mod_rsp', 'sev_rsp')]

df[value_any_main  > 1, value_any_main  := 1]

# put all proportions in "among COVID patients" space rather than "among long COVID cases"
df[, fcr := value_fat_cog_rsp * value_any_main]
df[, fr := value_fat_rsp * value_any_main]
df[, fc := value_fat_cog * value_any_main]
df[, cr := value_cog_rsp * value_any_main]

# subtract to make all outcome mutually exclusive
df[, f := value_fat - fc - fr - fcr]
df[, r := value_rsp - cr - fr - fcr]
df[, c := value_cog - fc - cr - fcr]


# check for negative draws and cap at 0
for (v in c('fcr', 'fc', 'fr', 'cr', 'f', 'c', 'r')) {
  
  # count rows below min
  n <- nrow(df[get(v) < 0])
  df[, eval(paste0(v, 'low')) := 0]
  df[get(v) < 0, eval(paste0(v, 'low')) := 1]
  df[get(v) < 0, eval(v) := 0]
  
  # TODO nlow never referenced again what is its purpose
  nlow <- nrow(df[get(paste0(v, 'low')) == 1])
}

table(df$fcrlow, df$follow_up_days)
table(df$frlow, df$follow_up_days)
table(df$fclow, df$follow_up_days)
table(df$crlow, df$follow_up_days)
table(df$flow, df$follow_up_days)
table(df$rlow, df$follow_up_days)
table(df$clow, df$follow_up_days)

table(df[hospital == 0 & icu == 0 & female == 0 & children == 0, flow], 
      df[hospital == 0 & icu == 0 & female == 0 & children == 0, follow_up_days])

table(df[hospital == 0 & icu == 0 & female == 0 & children == 1, flow], 
      df[hospital == 0 & icu == 0 & female == 0 & children == 1, follow_up_days])

table(df[hospital == 0 & icu == 0 & female == 1 & children == 0, flow], 
      df[hospital == 0 & icu == 0 & female == 1 & children == 0, follow_up_days])

table(df[hospital == 0 & icu == 0 & female == 1 & children == 1, flow], 
      df[hospital == 0 & icu == 0 & female == 1 & children == 1, follow_up_days])

table(df[hospital == 1 & female == 0 & children == 0, flow], 
      df[hospital == 1 & female == 0 & children == 0, follow_up_days])

table(df[hospital == 1 & female == 0 & children == 1, flow], 
      df[hospital == 1 & female == 0 & children == 1, follow_up_days])

table(df[hospital == 1 & female == 1 & children == 0, flow], 
      df[hospital == 1 & female == 1 & children == 0, follow_up_days])

table(df[hospital == 1 & female == 1 & children == 1, flow], 
      df[hospital == 1 & female == 1 & children == 1, follow_up_days])

table(df[icu == 1 & female == 0 & children == 1, flow], 
      df[icu == 1 & female == 0 & children == 1, follow_up_days])

table(df[icu == 1 & female == 0 & children == 0, flow], 
      df[icu == 1 & female == 0 & children == 0, follow_up_days])

table(df[icu == 1 & female == 1 & children == 1, flow], 
      df[icu == 1 & female == 1 & children == 1, follow_up_days])

table(df[icu == 1 & female == 1 & children == 0, flow], 
      df[icu == 1 & female == 1 & children == 0, follow_up_days])

table(df[hospital == 0 & icu == 0 & female == 0 & children == 0, rlow], 
      df[hospital == 0 & icu == 0 & female == 0 & children == 0, follow_up_days])

table(df[hospital == 0 & icu == 0 & female == 0 & children == 1, rlow], 
      df[hospital == 0 & icu == 0 & female == 0 & children == 1, follow_up_days])

table(df[hospital == 0 & icu == 0 & female == 1 & children == 0, rlow], 
      df[hospital == 0 & icu == 0 & female == 1 & children == 0, follow_up_days])

table(df[hospital == 0 & icu == 0 & female == 1 & children == 1, rlow], 
      df[hospital == 0 & icu == 0 & female == 1 & children == 1, follow_up_days])

table(df[hospital == 1 & female == 0 & children == 0, rlow], 
      df[hospital == 1 & female == 0 & children == 0, follow_up_days])

table(df[hospital == 1 & female == 0 & children == 1, rlow], 
      df[hospital == 1 & female == 0 & children == 1, follow_up_days])

table(df[hospital == 1 & female == 1 & children == 0, rlow], 
      df[hospital == 1 & female == 1 & children == 0, follow_up_days])

table(df[hospital == 1 & female == 1 & children == 1, rlow], 
      df[hospital == 1 & female == 1 & children == 1, follow_up_days])

table(df[icu == 1 & female == 0 & children == 1, rlow], 
      df[icu == 1 & female == 0 & children == 1, follow_up_days])

table(df[icu == 1 & female == 0 & children == 0, rlow], 
      df[icu == 1 & female == 0 & children == 0, follow_up_days])

table(df[icu == 1 & female == 1 & children == 1, rlow], 
      df[icu == 1 & female == 1 & children == 1, follow_up_days])

table(df[icu == 1 & female == 1 & children == 0, rlow], 
      df[icu == 1 & female == 1 & children == 0, follow_up_days])

table(df[hospital == 0 & icu == 0 & female == 0 & children == 0, clow], 
      df[hospital == 0 & icu == 0 & female == 0 & children == 0, follow_up_days])

# table(df[hospital == 0 & icu == 0 & female == 0 & children == 1, clow], 
#       df[hospital == 0 & icu == 0 & female == 0 & children == 1, follow_up_days])

table(df[hospital == 0 & icu == 0 & female == 1 & children == 0, clow], 
      df[hospital == 0 & icu == 0 & female == 1 & children == 0, follow_up_days])

# table(df[hospital == 0 & icu == 0 & female == 1 & children == 1, clow], 
#       df[hospital == 0 & icu == 0 & female == 1 & children == 1, follow_up_days])

table(df[hospital == 1 & female == 0 & children == 0, clow], 
      df[hospital == 1 & female == 0 & children == 0, follow_up_days])

table(df[hospital == 1 & female == 0 & children == 1, clow], 
      df[hospital == 1 & female == 0 & children == 1, follow_up_days])

table(df[hospital == 1 & female == 1 & children == 0, clow], 
      df[hospital == 1 & female == 1 & children == 0, follow_up_days])

table(df[hospital == 1 & female == 1 & children == 1, clow], 
      df[hospital == 1 & female == 1 & children == 1, follow_up_days])

table(df[icu == 1 & female == 0 & children == 1, clow], 
      df[icu == 1 & female == 0 & children == 1, follow_up_days])

table(df[icu == 1 & female == 0 & children == 0, clow], 
      df[icu == 1 & female == 0 & children == 0, follow_up_days])

table(df[icu == 1 & female == 1 & children == 1, clow], 
      df[icu == 1 & female == 1 & children == 1, follow_up_days])

table(df[icu == 1 & female == 1 & children == 0, clow], 
      df[icu == 1 & female == 1 & children == 0, follow_up_days])



df[, sum := fcr + fr + fc + cr + f + r + c]

summary_pre <- df[, lapply(.SD, mean), 
                  by = .(hospital, icu, female, children, follow_up_days), 
                  .SDcols = c('fcr', 'fr', 'fc', 'cr', 'value_fat', 'value_rsp', 
                              'value_cog', 'value_any_main', 'f', 'r', 'c', 'sum')]

fwrite(
  summary_pre,
  file.path(
    outputfolder, "diagnostics", paste0("proportions_pre_post_adjustment_", version_a, ".csv")
  )
)

#df_pre <- df[,c('follow_up_days', 'hospital', 'icu', 'female', 'variable', 'f', 'c', 'r', 'fc', 'fr', 'cr', 'fcr', 'value_any_main', 'sum')]
#df_pre <- melt(df_pre, id.vars = c('follow_up_days', 'hospital', 'icu', 'female', 'variable'))
#setnames(df_pre, 'variable.1', 'outcome')
#setnames(df_pre, 'value', 'proportion')
#summary_pre <- df_pre[,.(mean(proportion),quantile(proportion,0.025, na.rm=TRUE),quantile(proportion,0.975, na.rm=TRUE)), 
#                      by = c('hospital', 'icu', 'female', 'follow_up_days', 'outcome')]
#setnames(summary_pre, c('V1', 'V2', 'V3'), c('mean', 'lower', 'upper'))
#write.csv(summary_pre, paste0(outputfolder, 'diagnostics/proportions_pre_post_adjustment2.csv'))

df[, fcr := fcr * value_any_main/ sum]
df[, fc := fc * value_any_main/ sum]
df[, fr := fr * value_any_main/ sum]
df[, cr := cr * value_any_main/ sum]
df[, f := f * value_any_main/ sum]
df[, c := c * value_any_main/ sum]
df[, r := r * value_any_main/ sum]

df[, sum2 := fcr + fr + fc + cr + f + r + c]

summary <- df[, .(raw_sum = mean(sum), adjusted_sum = mean(sum2)), 
              by = c('hospital', 'icu', 'female', 'children', 'follow_up_days')]

plot(summary$raw_sum, summary$adjusted_sum)
plot(df$sum2, df$sum)

df[, c_mild := c * mild_cog]
df[, c_mod := c * mod_cog]
df[, r_mild := r * mild_rsp]
df[, r_mod := r * mod_rsp]
df[, r_sev := r * sev_rsp]
df[, fc_mild := fc * mild_cog]
df[, fc_mod := fc * mod_cog]
df[, fr_mild := fr * mild_rsp]
df[, fr_mod := fr * mod_rsp]
df[, fr_sev := fr * sev_rsp]
df[, cr_mild_mild := cr * mild_cog * mild_rsp]
df[, cr_mild_mod := cr * mild_cog * mod_rsp]
df[, cr_mild_sev := cr * mild_cog * sev_rsp]
df[, cr_mod_mild := cr * mod_cog * mild_rsp]
df[, cr_mod_mod := cr * mod_cog * mod_rsp]
df[, cr_mod_sev := cr * mod_cog * sev_rsp]
df[, fcr_mild_mild := fcr * mild_cog * mild_rsp]
df[, fcr_mild_mod := fcr * mild_cog * mod_rsp]
df[, fcr_mild_sev := fcr * mild_cog * sev_rsp]
df[, fcr_mod_mild := fcr * mod_cog * mild_rsp]
df[, fcr_mod_mod := fcr * mod_cog * mod_rsp]
df[, fcr_mod_sev := fcr * mod_cog * sev_rsp]

df[, sum3 := f + c_mild + c_mod + r_mild + r_mod + r_sev + fc_mild + fc_mod + 
     fr_mild + fr_mod + fr_sev + cr_mild_mild + cr_mild_mod + cr_mild_sev + 
     cr_mod_mild + cr_mod_mod + cr_mod_sev + fcr_mild_mild + fcr_mild_mod + 
     fcr_mild_sev + fcr_mod_mild + fcr_mod_mod + fcr_mod_sev
]


plot(df$sum2, df$sum3)


final_draws <- df[, c('follow_up_days', 'hospital', 'icu', 'female', 'children', 
                      'variable', 'f', 'c_mild', 'c_mod', 'r_mild', 'r_mod', 'r_sev', 
                      'fc_mild', 'fc_mod', 'fr_mild', 'fr_mod', 'fr_sev',
                      'cr_mild_mild', 'cr_mild_mod', 'cr_mild_sev', 'cr_mod_mild', 
                      'cr_mod_mod', 'cr_mod_sev', 'fcr_mild_mild', 'fcr_mild_mod', 
                      'fcr_mild_sev', 'fcr_mod_mild', 'fcr_mod_mod', 'fcr_mod_sev')]

final_draws <- melt(final_draws, id.vars = c('follow_up_days', 'hospital', 'icu', 
                                             'female', 'children', 'variable'))

setnames(final_draws, 'variable.1', 'outcome')
setnames(final_draws, 'value', 'proportion')

summary <- final_draws[,
                       .(
                         mean = mean(proportion),
                         lower = quantile(proportion, 0.025, na.rm = TRUE),
                         upper = quantile(proportion, 0.975, na.rm = TRUE)
                       ),
                       by = c("hospital", "icu", "female", "children", 
                              "follow_up_days", "outcome")
]

if (save == 1) {
  fwrite(summary, file.path(outputfolder, 'final_proportion_summary.csv'))
}

# Mark draws with version
final_draws[, version := version_a]

summary

final_draws <- final_draws[
  (hospital==0 & icu==0 & follow_up_days %in% c(0, 90-follow_up_days_subtract_c, (365-follow_up_days_subtract_c))) |
    (hospital==1 & icu==0 & follow_up_days %in% c(0, 90-follow_up_days_subtract_h, (365-follow_up_days_subtract_h))) |
    (hospital==0 & icu==1 & follow_up_days %in% c(0, 90-follow_up_days_subtract_i, (365-follow_up_days_subtract_i)))]


head(final_draws)
dim(final_draws)

fwrite(
  final_draws,
  file.path(
    outputfolder,
    paste0("final_proportion_draws_v", version_a, ".csv")
  )
)

if (save == 1) {
  
  fwrite(final_draws,
         file = file.path(outputfolder, "final_proportion_draws.csv")
  )
  
  fwrite(final_draws,
         file = "FILEPATH/final_proportion_draws.csv"
  )
  
}


final_any <- df[,c('follow_up_days', 'hospital', 'icu', 'female', 'children', 
                   'variable', 'value_any_main')]

final_any <- melt(final_any, id.vars = c('follow_up_days', 'hospital', 'icu', 
                                         'female', 'children', 'variable'))

setnames(final_any, 'variable.1', 'outcome')
setnames(final_any, 'value', 'proportion')

summary <- final_any[,
                     .(
                       mean = mean(proportion),
                       lower = quantile(proportion, 0.025, na.rm = TRUE),
                       upper = quantile(proportion, 0.975, na.rm = TRUE)
                     ),
                     by = c("hospital", "icu", "female", "children", 
                            "follow_up_days", "outcome")
]

if (save == 1) {
  fwrite(summary, file.path(outputfolder, 'final_proportion_summary_any.csv'))
}

summary

final_any <- final_any[
  (hospital == 0 & icu == 0 & follow_up_days %in% c(0, 90-follow_up_days_subtract_c, (365 - follow_up_days_subtract_c))) |
    (hospital == 1 & icu == 0 & follow_up_days %in% c(0, 90-follow_up_days_subtract_h, (365 - follow_up_days_subtract_h))) |
    (hospital == 0 & icu == 1 & follow_up_days %in% c(0, 90-follow_up_days_subtract_i, (365 - follow_up_days_subtract_i)))
]

head(final_any)
dim(final_any)

fwrite(final_any, file.path(
  outputfolder,
  paste0(
    "final_proportion_draws_any_v",
    version_a, ".csv"
  )
))

if (save == 1) {
  
  fwrite(final_any,
         file = file.path(outputfolder, "final_proportion_draws_any.csv")
  )
  
  fwrite(final_any,
         file = "FILEPATH/final_proportion_draws_any.csv"
  )
  
}
