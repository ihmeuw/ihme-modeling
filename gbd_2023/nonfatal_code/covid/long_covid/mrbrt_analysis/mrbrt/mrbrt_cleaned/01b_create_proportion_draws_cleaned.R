#--------------------------------------------------------------
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
  DRIVE <- 'FILEPATH'
  DRIVE <- 'FILEPATH'
} else {
  DRIVE <- 'FILEPATH'
  DRIVE <- 'FILEPATH'
}


# load packages
pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)
library(plyr)


folder <- "FILEPATH"
outputfolder <- "FILEPATH"

version_a <- 37
version_b <- 38
version_sev <- 37
# overlap categories rerun

# change to any_main
for(out in c("any", "cog", "fat", "rsp")) {
  model_dir <- paste0(out, "_v", version_a, "/")
  draws1 <- read.csv(paste0(outputfolder, model_dir, "predictions_draws_hospital_icu.csv"))
  draws2 <- read.csv(paste0(outputfolder, model_dir, "predictions_draws_community.csv"))
  draws <- rbind(draws1, draws2)
  draws <- draws[draws$follow_up_days==0,]
  draws$outcome <- out
  try(draws$follow_up_days_comm <- NULL)
  try(draws$follow_up_days_hosp <- NULL)
  try(draws$follow_up_days <- NULL)
  try(draws$male <- NULL)
  try(draws$other_list <- NULL)
  try(draws$memory_problems <- NULL)
  try(draws$fatigue <- NULL)
  try(draws$administrative <- NULL)
  try(draws$cough <- NULL)
  try(draws$shortness_of_breath <- NULL)

  if(out=="any") df <- draws
  else df <- rbind(df, draws)
}
for(out in c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp")) {
  model_dir <- paste0(out, "_v", version_b, "/")
  draws <- read.csv(paste0(outputfolder, model_dir, "predictions_draws.csv"))
  try(draws <- draws[follow_up_days==0])
  try(draws$follow_up_days <- NULL)
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
  draws2 <- draws
  draws2$female <- 1

  draws <- rbind(draws, draws2)
  draws$hospital <- draws$hospital_icu
  draws$icu <- 0
  draws$hospital_icu <- NULL
  df <- rbind(df, draws)
  draws$icu <- draws$hospital
  draws$hospital <- 0
  draws <- draws[draws$icu==1,]
  df <- rbind(df, draws)
}
for(out in c("mild_cog", "mod_cog", "mild_rsp", "mod_rsp", "sev_rsp")) {
  model_dir <- paste0(out, "_v", version_sev, "/")
  draws <- read.csv(paste0(outputfolder, model_dir, "predictions_draws.csv"))
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
  draws2 <- draws
  draws2$female <- 1

  draws <- rbind(draws, draws2)
  draws$hospital <- draws$hospital_icu
  draws$icu <- 0
  draws$hospital_icu <- NULL
  df <- rbind(df, draws)
  draws$icu <- draws$hospital
  draws$hospital <- 0
  draws <- draws[draws$icu==1,]
  df <- rbind(df, draws)
}

df <- data.table(df)
df$X <- NULL
df <- melt(df, id.vars = c("hospital", "icu", "female", "outcome"))
df <- reshape(df, idvar = c("hospital", "icu", "female", "variable"), timevar = "outcome", direction = "wide", sep="_")

df$mild_cog <- df$value_mild_cog / (df$value_mild_cog + df$value_mod_cog)
df$mod_cog <- df$value_mod_cog / (df$value_mild_cog + df$value_mod_cog)
df$mild_rsp <- df$value_mild_rsp / (df$value_mild_rsp + df$value_mod_rsp + df$value_sev_rsp)
df$mod_rsp <- df$value_mod_rsp / (df$value_mild_rsp + df$value_mod_rsp + df$value_sev_rsp)
df$sev_rsp <- df$value_sev_rsp / (df$value_mild_rsp + df$value_mod_rsp + df$value_sev_rsp)


df$value_any[df$value_any>1] <- 1
df$fcr <- df$value_fat_cog_rsp * df$value_any
df$fr <- df$value_fat_rsp * df$value_any - df$fcr
df$fc <- df$value_fat_cog * df$value_any - df$fcr
df$cr <- df$value_cog_rsp * df$value_any - df$fcr
df$f <- df$value_fat - df$fc - df$fr - df$fcr
df$r <- df$value_rsp - df$cr - df$fr - df$fcr
df$c <- df$value_cog - df$fc - df$cr - df$fcr
df$fcr[df$fcr<0] <- 0
df$fc[df$fc<0] <- 0
df$fr[df$fr<0] <- 0
df$cr[df$cr<0] <- 0
df$f[df$f<0] <- 0
df$c[df$c<0] <- 0
df$r[df$r<0] <- 0

df$sum <- df$fcr + df$fr + df$fc + df$cr + df$f + df$r + df$c

df$fcr <- df$fcr * df$value_any / df$sum
df$fc <- df$fc * df$value_any / df$sum
df$fr <- df$fr * df$value_any / df$sum
df$cr <- df$cr * df$value_any / df$sum
df$f <- df$f * df$value_any / df$sum
df$c <- df$c * df$value_any / df$sum
df$r <- df$r * df$value_any / df$sum


df$sum2 <- df$fcr + df$fr + df$fc + df$cr + df$f + df$r + df$c


plot(df$sum2, df$value_any)

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


final_draws <- df[,c("hospital", "icu", "female", "variable", "f", "c_mild", "c_mod", "r_mild", "r_mod", "r_sev", "fc_mild", "fc_mod", "fr_mild", "fr_mod", "fr_sev",
                     "cr_mild_mild", "cr_mild_mod", "cr_mild_sev", "cr_mod_mild", "cr_mod_mod", "cr_mod_sev", "fcr_mild_mild", "fcr_mild_mod", "fcr_mild_sev",
                     "fcr_mod_mild", "fcr_mod_mod", "fcr_mod_sev")]

final_draws <- melt(final_draws, id.vars = c("hospital", "icu", "female", "variable"))
setnames(final_draws, "variable.1", "outcome")
setnames(final_draws, "value", "proportion")
final_draws <- reshape(final_draws, idvar = c("hospital", "icu", "female", "outcome"), timevar = "variable", direction = "wide")
setnames(final_draws, paste0("proportion.draw_", c(0:999)), paste0("draw_", c(0:999)))


head(final_draws)
final_draws[1:10,1:10]
write.csv(final_draws, paste0(outputfolder, "final_proportion_draws_v", version_b, ".csv"))
write.csv(final_draws, paste0(outputfolder, "final_proportion_draws.csv"))



