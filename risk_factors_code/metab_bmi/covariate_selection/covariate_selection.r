library(data.table)
library(lme4)
library(MuMIn)
library(boot)

# read in data
df<-fread("FILEPATH")

# subset to level 3 (nationals)
df<-df[!grepl("_", ihme_loc_id) | ihme_loc_id == "CHN_44533",]

df$age_group_id <- as.factor(df$age_group_id)
df$sex_id <- as.factor(df$sex_id)

ow <- df[!is.na(df$data),]

# read in data
df<-fread("FILEPATH")

# subset to level 3 (nationals)
df<-df[!grepl("_", ihme_loc_id) | ihme_loc_id == "CHN_44533",]

df$age_group_id <- as.factor(df$age_group_id)
df$sex_id <- as.factor(df$sex_id)

ob <- df[!is.na(df$data),]

# test models
ptm <- proc.time()
ow_mod<-dredge(lmer(logit(data) ~ log(ten_yr_lag_energy) + prop_urban + log(ldi_pc) + sdi + abs_latitude + education_yrs_pc + prop_pop_agg + sugar_g_unadj + sugar_g_adj + vehicles_2_plus_4wheels_pc + age_group_id + sex_id + (1|super_region_name) + (1|region_name) + (1|ihme_loc_id), data = ow, na.action = "na.fail"), evaluate = TRUE, trace = TRUE, extra = "adjR^2", fixed = c("age_group_id", "sex_id"), subset = !("sdi" && "log(ldi_pc)") & !("sdi" & "education_yrs_pc") & !("sugar_g_adj" && "sugar_g_unadj"))
ob_mod<-dredge(lmer(logit(data) ~ log(ten_yr_lag_energy) + prop_urban + log(ldi_pc) + sdi + abs_latitude + education_yrs_pc + prop_pop_agg + sugar_g_unadj + sugar_g_adj + vehicles_2_plus_4wheels_pc + age_group_id + sex_id + (1|super_region_name) + (1|region_name) + (1|ihme_loc_id), data = ob, na.action = "na.fail"), evaluate = TRUE, trace = TRUE, extra = "adjR^2", fixed = c("age_group_id", "sex_id"), subset = !("sdi" && "log(ldi_pc)") & !("sdi" & "education_yrs_pc") & !("sugar_g_adj" && "sugar_g_unadj"))
proc.time() - ptm

