# Remove the history and import lib
rm(list=ls(all.names=T))

library(readxl)
library(data.table)
library(dplyr)
library(tidyr)
library(haven)
library(foreign)
library(fuzzyjoin)
library(stringdist)
library(lubridate)

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

version <- 5
code_dir <- "FILEPATH"

source(paste0(code_dir, "db_init.R"))
source(paste0(code_dir, "get_age_map.r"))
source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k ,"FILEPATH/get_location_metadata.R"))
source(paste0(roots$k ,"FILEPATH/get_age_metadata.R"))

oldpath <- "FILEPATH"
outpath <- "FILEPATH"

# Import the old 4-month follow-up data ------------------------------------------------
# df_sav_old <- setDT(read_sav(paste0(oldpath,"Iran COVID Cohort Study_Q1 & Q3_selected variaqble.sav")))
# df_tab_old <- setDT(read_xlsx(paste0(oldpath,"Iran COVID Cohort Study_Q1  Q3_selected variable.xlsx")))
df_old <- setDT(read_csv(paste0(oldpath,"Iran_all_data_marked_v1.csv")))

# Remove irrelevant cols
df_old$age_group_name <- NULL
df_old$...1 <- NULL

symp_cols <- c("fatigue_rule1","fatigue_rule2","post_acute","cognitive","res_mild","res_moderate","res_severe","res_combine",
               "Cog_Res","Cog_Fat","Res_Fat","Cog_Res_Fat","long_term")

orig_cols <- colnames(df_old)[!colnames(df_old) %in% c(symp_cols, "id.2")]

# df_old <- df_old[, c("id.2", symp_cols), with = FALSE]
df_old$id.2 <- as.character(df_old$id.2)

# Modify columns to differentiate follow-up times
setnames(df_old, symp_cols, paste0(symp_cols,"_2021"))
setnames(df_old, "id.2", "id_mp")
df_old$File_2021 <- 1

# Mutate the dataframe to replace "yes" with 1 and "no" with 2 in all columns
df_old <- df_old %>%
  mutate(across(everything(), ~ifelse(. == "yes", 1, ifelse(. == "no", 2, .))),
         Dm3 = ifelse(Dm3 == "female", 1, 2),
         mhb251 = ifelse(mhb251 == "sitting", 1, ifelse(mhb251 == "exercise", 2, mhb251)),
         mhb261 = ifelse(mhb261 == "often", 1, ifelse(mhb261 == "occasionally", 2, ifelse(mhb261 == "at night", 3, mhb261))),
         mha1021 = ifelse(mha1021 == "rest", 1,
                   ifelse(mha1021 == "normal activity", 2,
                   ifelse(mha1021 == "intense activity", 3, mha1021)))) %>%
  mutate(across(-id_mp, as.numeric))

str(df_old)

# Import the new follow-up data ------------------------------------------------
# df_sav_init <- read_sav(paste0(outpath,"file_IHME_send_24mfollow_COVID_form1.2.3.4__followup61224.sav"))
# df_sav <- setDT(read_sav(paste0(outpath,"file_IHME_20240610_COVID_form1.2.3.4_followup61224_Jun 11, 2024.sav")))

# Save a copy in excel
# write.xlsx(df_sav, paste0(outpath,"Iran_Covid_Cohort_new.xlsx"))
df <- setDT(read.xlsx(paste0(outpath,"Iran_Covid_Cohort_new.xlsx")))
df$id_mp <- as.character(df$id_mp)
df$Dm3 <- as.numeric(df$gender.new)
df$Dm5 <- df$Age_m

# Remove corrupted data (no ids)
df <- df[df$file_1.2 != -1,]

# Number of matched IDs between old and new follow-ups
paste("Number of matched IDs between newest and 4m follow-up:",length(df_old$id_mp[df_old$id_mp %in% df$id_mp]))
paste("Number of unique IDs from newest follow-up:", (nrow(df)-sum(is.na(df$id_mp))))
paste("Percentage of patients who responded in the newest follow-up that appear in 4m follow-up:",
      length(df_old$id_mp[df_old$id_mp %in% df$id_mp])/(nrow(df)-sum(is.na(df$id_mp))))

# Save list of col names
cols_orig <- colnames(df)

# Merge old and new follow-ups -------------------------------------------------
df_merged <- full_join(df_old, df, by = "id_mp")
str(df_merged)

# Coalesce the symptom columns based on the orig_cols (keep newest value if there's a difference)
for(symptom in orig_cols) {
  df_merged <- df_merged %>%
    mutate(!!symptom := if_else(!is.na(!!sym(paste0(symptom, ".y"))), !!sym(paste0(symptom, ".y")), !!sym(paste0(symptom, ".x"))))
}

# Select only the necessary columns
removed_cols <- c(paste0(orig_cols, ".x"),paste0(orig_cols, ".y"), 'gender.new', 'Age_m')
df_merged <- df_merged %>%
  select(-all_of(removed_cols))

# Fill in variables on oxygen therapy & dyspnea
df_merged$mhb26 <- ifelse(df_merged$mhb261 == 0, 2, ifelse(df_merged$mhb261 > 0, 1, df_merged$mhb26))
table(df_merged$mhb26, df_merged$mhb261)

df_merged$mha102 <- ifelse(df_merged$mha1021 == 0, 2, ifelse(df_merged$mha1021 > 0, 1, df_merged$mha102))
table(df_merged$mha102, df_merged$mha1021)

# Show new added cols from 4m FU and save data set
colnames(df_merged)[!colnames(df_merged) %in% cols_orig]
# write.xlsx(df_merged, paste0(outpath,"Iran_Covid_Cohort_merged_21_23.xlsx"))

# Copy to a new df for analysis ------------------------------------------------
df_main <- df_merged
setnames(df_main, c('Dm3','Dm5'), c("sex_id","age"), skip_absent = T)
df_main[sex_id==1, sex := "Female"]
df_main[sex_id==2, sex := "Male"]

# Remove Covid reinfection cases
# df_main <- df_main[s136_6m !=1 | is.na(s136_6m),]
# df_main <- df_main[s136_12m !=1 | is.na(s136_12m),]
# df_main <- df_main[s136_24m !=1 | is.na(s136_24m),]

# Merge age groups
df_main[is.na(age), age_group_name := "Unknown"]
df_main[age<= 1, age_group_name := "Early Neonatal to 1"]
df_main[age>1 & age <=4, age_group_name := "1 to 4"]
df_main[age>4 & age<=9, age_group_name := "5 to 9" ]
df_main[age>9 & age<=14, age_group_name := "10 to 14" ]
df_main[age>14 & age<=20, age_group_name := "15 to 19" ]
df_main[age>20 & age<=24, age_group_name := "20 to 24"]
df_main[age>24 & age<=29, age_group_name := "25 to 29" ]
df_main[age>29 & age<=34, age_group_name := "30 to 34" ]
df_main[age>34 & age<=39, age_group_name := "35 to 39" ]
df_main[age>39 & age<=44, age_group_name := "40 to 44"]
df_main[age>44 & age<=49, age_group_name := "45 to 49" ]
df_main[age>49 & age<=54, age_group_name := "50 to 54" ]
df_main[age>54 & age<=59, age_group_name := "55 to 59" ]
df_main[age>59 & age<=64, age_group_name := "60 to 64"]
df_main[age>64 & age<=69, age_group_name := "65 to 69" ]
df_main[age>69 & age<=74, age_group_name := "70 to 74" ]
df_main[age>74 & age<=79, age_group_name := "75 to 79" ]
df_main[age>79 & age<=84, age_group_name := "80 to 84" ]
df_main[age>84 & age<=89, age_group_name := "85 to 89" ]
df_main[age>89 & age<=94, age_group_name := "90 to 94" ]
df_main[age>95, age_group_name := "95 plus" ]

age_map <- get_age_map(type = "all")
age_map <- age_map[, .(age_group_id, age_group_name)]
df_main <- setDT(merge(df_main, age_map, by='age_group_name', all.x=T))

# Make date columns
df_main$date_hospitalized <- ymd(df_main$date_hospitalized)
df_main$date_infection <- ymd(df_main$date_infection)

# Create a new column min_follow_up
df_main$min_follow_up <- pmin(df_main$File_2021, df_main$File_6m, df_main$File_12m, df_main$File_24m, na.rm = T)
df_main$min_follow_up[is.na(df_main$File_2021) & is.na(df_main$File_6m) & is.na(df_main$File_12m) & is.na(df_main$File_24m)] <- NA

table(df_main$min_follow_up)

# Getting the number of patients by age and by sex
sample_n_byAgesex_6m <- df_main %>%
  filter(File_6m==6 | File_2021==1) %>%
  group_by(age_group_id, age_group_name, sex) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "06m")

sample_n_byAgesex_12m <- df_main %>%
  filter(File_12m==12) %>%
  group_by(age_group_id, age_group_name, sex) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "12m")

sample_n_byAgesex_24m <- df_main %>%
  filter(File_24m==24) %>%
  group_by(age_group_id, age_group_name, sex) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "24m")

sample_n_byAgesex <- rbind(sample_n_byAgesex_6m, sample_n_byAgesex_12m, sample_n_byAgesex_24m)

# Getting the number of patients by sex
sample_n_bysex_6m <- df_main %>%
  filter(File_6m==6 | File_2021==1) %>%
  group_by(sex) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "06m")

sample_n_bysex_12m <- df_main %>%
  filter(File_12m==12) %>%
  group_by(sex) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "12m")

sample_n_bysex_24m <- df_main %>%
  filter(File_24m==24) %>%
  group_by(sex) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "24m")

sample_n_bysex <- rbind(sample_n_bysex_6m, sample_n_bysex_12m, sample_n_bysex_24m)

# Getting the number of patients by age groups
sample_n_byage_6m <- df_main %>%
  filter(File_6m==6 | File_2021==1) %>%
  group_by(age_group_id, age_group_name) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "06m")

sample_n_byage_12m <- df_main %>%
  filter(File_12m==12) %>%
  group_by(age_group_id, age_group_name) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "12m")

sample_n_byage_24m <- df_main %>%
  filter(File_24m==24) %>%
  group_by(age_group_id, age_group_name) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "24m")

sample_n_byage <- rbind(sample_n_byage_6m, sample_n_byage_12m, sample_n_byage_24m)

# Getting the number of patients by icu
sample_n_byICU_6m <- df_main %>%
  filter(File_6m==6 | File_2021==1) %>%
  mutate(HI95 = ifelse(!HI95 %in% c(1,2), 'Unknown', HI95)) %>%
  group_by(HI95) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "06m")

sample_n_byICU_12m <- df_main %>%
  filter(File_12m==12) %>%
  mutate(HI95 = ifelse(!HI95 %in% c(1,2), 'Unknown', HI95)) %>%
  group_by(HI95) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "12m")

sample_n_byICU_24m <- df_main %>%
  filter(File_24m==24) %>%
  mutate(HI95 = ifelse(!HI95 %in% c(1,2), 'Unknown', HI95)) %>%
  group_by(HI95) %>%
  dplyr::summarise(N=n()) %>%
  mutate(follow_up = "24m")

sample_n_byICU <- rbind(sample_n_byICU_6m, sample_n_byICU_12m, sample_n_byICU_24m)

# ORIGINAL CLUSTERS ----------------------------------------------

# 1. Fatigue/pain/emotional problem cluster:
# Rule 1: a case must have answered Hp19a.6 (‘reduced ability for daily functions prior to COVID’)= 2 (no) and at least two out of Hp19a.8 (‘feeling sad most of them time prior to COVID’), Hp19a.9 (‘frustration and no hope prior to COVID’), and Hp19a.10 (‘dissatisfaction and not enjoying life prior to COVID’)= 2; I also see questions on first page of Q3 asking history before admission of fatigue which could also apply.
df_main <- mutate(df_main,
                  fatigue_rule1 = ifelse((Hp19a6==2) &
                                           ((Hp19a8==2 & Hp19a9==2) |
                                              (Hp19a8==2 & Hp19a10==2) |
                                              (Hp19a9==2 & Hp19a10==2)), 1, 0))

table(df_main$fatigue_rule1)

# Rule 2: (MHA1.2 (general weakness) = 1 or MHA2.5 (fatigue during normal activity) = 1 or MHA9.2 (muscle weakness) = 1) and ((MHA9.1 (joint pain) = 1 or MHA9.4 (muscle pain) = 1) or (MHA11.1 (depression) = 1 or MHA11.2 (anxiety) = 1))
df_main <- mutate(df_main,
                  fatigue_rule2 = ifelse((mha12==1 | mha25==1 | mha92==1) &
                                           ((mha91==1 | mha94==1) | (mha111==1 | mha112 ==1)), 1, 0))
table(df_main$fatigue_rule2)

# Rule 3 (6m): (s4_6m (fatigue)==1 OR s272_6m (lethargy)==1) AND (s20_6m(muscle pain)==1 OR s40_6m (joint pain)==1 OR s62_6m (depression)==1 OR s93_6m (anxiety)==1)
# Rule 4 (12m)
# Rule 5 (24m)
df_main <- mutate(df_main,
                  fatigue_rule3 = ifelse((s4_6m==1 | s272_6m==1) & (s20_6m==1 | s40_6m==1 | s62_6m==1 | s93_6m==1), 1, 0),
                  fatigue_rule4 = ifelse((s4_12m==1 | s272_12m==1) & (s20_12m==1 | s40_12m==1 | s62_12m==1 | s93_12m==1), 1, 0),
                  fatigue_rule5 = ifelse((s4_24m==1 | s272_24m==1) & (s20_24m==1 | s40_24m==1 | s62_24m==1 | s93_24m==1), 1, 0))

table(df_main$fatigue_rule3)
table(df_main$fatigue_rule4)
table(df_main$fatigue_rule5)

# Combine the fatigue category for 6m follow-up
df_main <- mutate(df_main,
                  post_acute_6m = ifelse((post_acute_2021 == 1 |
                                            (fatigue_rule1 == 1 & (fatigue_rule2 == 1 | fatigue_rule3 == 1) &
                                               File_6m == 6)), 1, 0),
                  # post_acute_12m = ifelse((fatigue_rule1 == 1 & (fatigue_rule2 == 1 | fatigue_rule4 == 1) &
                  #                            File_12m == 12 & (post_acute_6m == 1)), 1, 0),
                  post_acute_12m = ifelse((fatigue_rule4 == 1 & File_12m == 12 & post_acute_6m == 1), 1, 0),
                  post_acute_24m = ifelse((fatigue_rule5 == 1 & File_24m == 24 & post_acute_6m == 1), 1, 0))

table(df_main$post_acute_2021)
table(df_main$post_acute_6m)
table(df_main$post_acute_12m)
table(df_main$post_acute_24m)

# 2. Cognition cluster:
# MHA 11.3 (memory loss) = 1 and Hp19a.4 (reduced concentration and ability for decision making before disease) = 2  and Hp19b.4 (reduced concentration and ability for decision making after disease) = 1
# AND (s8_6m (trouble in concentration) == 1 OR s140_6m (forgetfulness) == 1)
# There is not enough information to grade by severity
df_main <- mutate(df_main,
                  cognitive_6m = ifelse(cognitive_2021 ==1 |
                                          (Hp19a4 == 2 & (mha113 == 1 | s140_6m == 1) &
                                             (s8_6m == 1 | Hp19b4 == 1) & File_6m == 6), 1, 0),
                  # cognitive_12m = ifelse((Hp19a4 == 2 & (mha113 == 1 | s140_12m == 1) & (s8_12m == 1 | Hp19b4 == 1) &
                  #                           File_12m == 12) & (cognitive_4m == 1 | cognitive_6m == 1), 1, 0),
                  cognitive_12m = ifelse((s8_12m == 1 | s140_12m == 1) &
                                            File_12m == 12 & cognitive_6m == 1, 1,0),
                  cognitive_24m = ifelse((s8_24m == 1 | s140_24m == 1) &
                                            File_24m == 24 & cognitive_6m == 1, 1, 0))
table(df_main$cognitive_2021)
table(df_main$cognitive_6m)
table(df_main$cognitive_12m)
table(df_main$cognitive_24m)

# 3. res_combine cluster:

# no severity levels graded beyond 6m (avoid using post-covid variables from 6m follow up for longer follow-ups):
# Mild if MHA10.2.1 = 3 (strenuous activities) and MHA10.3 (oxygen therapy) = 2 (no)
# Moderate if MHA10.2.1 = 2 (normal activities) and MHA10.3 = 2 (no)
# Severe if MHA10.2.1=1 (at rest) OR MHA10.3 = 1 (yes)

df_main <- mutate(df_main,
                  # pre vs post covid
                  res_rule1 = ifelse((mhb25==2 & mhb26==2) |
                                       (mhb25==1 & mhb251==2 & mha1021 %in% c(1,2)) |
                                       (mhb25==2 & mhb26==2 & mhb13==2 & mhb30==2 & (mha104==1 | s3_6m==1 | s257_6m==1)), 1, 0),
                  res_rule2_severe = ifelse(mha1021==1 | mha103==1, 1, 0), # severe
                  res_rule2_moderate = ifelse(mha1021==2 & mha103==2, 1, 0), # moderate
                  res_rule2_mild = ifelse(mha1021==3 & mha103==2, 1, 0), # mild
                  )
table(df_main$res_rule1)

df_main <- mutate(df_main,
                  res_severe_6m = ifelse(res_severe_2021==1 | (res_rule1==1 & res_rule2_severe==1 & File_6m==6), 1, 0),
                  res_moderate_6m = ifelse(res_moderate_2021==1 | (res_rule1==1 & res_rule2_moderate==1 & File_6m==6), 1, 0),
                  res_mild_6m = ifelse(res_mild_2021==1 | (res_rule1==1 & res_rule2_mild==1 & File_6m==6), 1, 0),

                  res_combine_6m = ifelse((res_mild_6m %in% 1 | res_moderate_6m %in% 1 | res_severe_6m %in% 1), 1, 0),

                  res_combine_12m = ifelse((s257_12m==1 | s3_12m==1) & File_12m==12 & res_combine_6m==1, 1, 0),

                  res_combine_24m = ifelse((s257_24m==1 | s3_24m==1) & File_24m==24 & res_combine_6m==1, 1, 0)
                  )

table(df_main$res_mild_6m)
table(df_main$res_moderate_6m)
table(df_main$res_severe_6m)

table(df_main$res_combine_2021)
table(df_main$res_combine_6m)
table(df_main$res_combine_12m)
table(df_main$res_combine_24m)

# Resolve over-lapping levels
df_main$overlap <- rowSums(df_main[,c("res_mild_6m","res_moderate_6m","res_severe_6m")])
df_main[(overlap >1 & res_mild_6m == res_moderate_6m), res_mild_6m := 0]
df_main[(overlap >1 & res_moderate_6m == res_severe_6m), res_moderate_6m := 0]
df_main <- subset(df_main, select= -c(overlap))

table(df_main$res_combine_2021)
table(df_main$res_combine_6m)
table(df_main$res_combine_12m)
table(df_main$res_combine_24m)

#-------------------------------------------------------------------------------
# NEW CLUSTERS -----------------------------------------------------------------

# Fatigue sub cluster
df_main <- mutate(df_main,
                  fatigue_2021 = ifelse((mhb29==2 | Hp19a6==2) &
                                        (mha12==1 | mha25==1 | mha92==1 | mha91==1 | mha94==1) &
                                        File_2021==1, 1, 0),
                  fatigue_6m = ifelse(fatigue_2021==1 |
                                        ((mhb29==2 | Hp19a6==2) &
                                        (mha12==1 | mha25==1 | mha92==1 | mha91==1 | mha94==1 |
                                           s4_6m==1 | s272_6m==1 | s20_6m==1 | s40_6m==1) & File_6m==6), 1, 0),

                  fatigue_12m = ifelse((s4_12m==1 | s272_12m==1 | s20_12m==1 | s40_12m==1) &
                                        fatigue_6m==1 & File_12m==12, 1, 0),

                  fatigue_24m = ifelse((s4_24m==1 | s272_24m==1 | s20_24m==1 | s40_24m==1) &
                                         fatigue_6m==1 & File_24m==24, 1, 0)
                  )
table(df_main$fatigue_2021)
table(df_main$fatigue_6m)
table(df_main$fatigue_12m)
table(df_main$fatigue_24m)

table(df_main$post_acute_2021)
table(df_main$post_acute_6m)
table(df_main$post_acute_12m)
table(df_main$post_acute_24m)

# Headache
df_main <- mutate(df_main,
                  headache_2021 = ifelse(HI24==2 & mha31==1 & File_2021==1, 1, 0),
                  headache_6m = ifelse(headache_2021==1 | (HI24==2 & (mha31==1 | s1_6m==1) & File_6m==6), 1, 0),
                  headache_12m = ifelse(s1_12m==1 & File_12m==12 & headache_6m==1, 1, 0),
                  headache_24m = ifelse(s1_24m==1 & File_24m==24 & headache_6m==1, 1, 0)
                  )
table(df_main$headache_2021)
table(df_main$headache_6m)
table(df_main$headache_12m)
table(df_main$headache_24m)

# Hair loss (alopecia) - No history of hair loss pre Covid
df_main <- mutate(df_main,
                  hairloss_6m = ifelse(s27_6m==1, 1, 0),
                  hairloss_12m = ifelse(s27_12m==1 & hairloss_6m==1, 1, 0),
                  hairloss_24m = ifelse(s27_24m==1 & hairloss_6m==1, 1, 0)
                  )

table(df_main$hairloss_6m)
table(df_main$hairloss_12m)
table(df_main$hairloss_24m)

# Taste/smell loss
df_main <- mutate(df_main,
                  taste_smell_loss_2021 = ifelse((HI26==2 & mha55==1) | (HI30==2 & mha56==1) & File_2021==1, 1, 0),
                  taste_smell_loss_6m = ifelse(taste_smell_loss_2021==1 |
                                                 ((HI26==2 & (mha55==1 | s29_6m==1)) |
                                                    (HI30==2 & (mha56==1 | s86_6m==1)) & File_6m==6), 1, 0),
                  taste_smell_loss_12m = ifelse(s29_12m==1 | s86_12m==1 & File_12m==12 & taste_smell_loss_6m==1, 1, 0),
                  taste_smell_loss_24m = ifelse(s29_24m==1 | s86_24m==1 & File_24m==24 & taste_smell_loss_6m==1, 1, 0)
                  )

table(df_main$taste_smell_loss_2021)
table(df_main$taste_smell_loss_6m)
table(df_main$taste_smell_loss_12m)
table(df_main$taste_smell_loss_24m)

# Sleep problems
df_main <- mutate(df_main,
                  sleep_prob_2021 = ifelse(Hp19a1==2 & (Hp19b1==1 | mha114==1) & File_2021==1, 1, 0),
                  sleep_prob_6m = ifelse(sleep_prob_2021==1 |
                                           (Hp19a1==2 & (Hp19b1==1 | mha114==1 | s76_6m==1 | s133_6m==1) & File_6m==6), 1, 0),
                  sleep_prob_12m = ifelse(s76_12m==1 | s133_12m==1 & File_12m==12 & sleep_prob_6m==1, 1, 0),
                  sleep_prob_24m = ifelse(s76_24m==1 | s133_24m==1 & File_24m==24 & sleep_prob_6m==1, 1, 0)
                  )
table(df_main$sleep_prob_2021)
table(df_main$sleep_prob_6m)
table(df_main$sleep_prob_12m)
table(df_main$sleep_prob_24m)

# Anxiety/depression
df_main <- mutate(df_main,
                  anxiety_depressed_2021 = ifelse(((((Hp19a8==2 & Hp19a9==2) | (Hp19a8==2 & Hp19a10==2) |
                                                   (Hp19a9==2 & Hp19a10==2)) & (mha111==1 | mha112==1)) |
                                                    (mhb21==2 & mha111==1)) & File_2021==1, 1, 0),

                  anxiety_depressed_6m = ifelse(anxiety_depressed_2021==1 |
                                                  (((((Hp19a8==2 & Hp19a9==2) | (Hp19a8==2 & Hp19a10==2) |
                                                   (Hp19a9==2 & Hp19a10==2)) &
                                                     (mha111==1 | mha112==1 | s62_6m==1 | s93_6m==1)) |
                                                     (mhb21==2 & (mha111==1 | s62_6m==1))) & File_6m==6), 1, 0),

                  anxiety_depressed_12m = ifelse((s62_12m==1 | s93_12m==1) & File_12m==12 & anxiety_depressed_6m==1, 1, 0),

                  anxiety_depressed_24m = ifelse((s62_24m==1 | s93_24m==1) & File_24m==24 & anxiety_depressed_6m==1, 1, 0)
                  )

table(df_main$anxiety_depressed_2021)
table(df_main$anxiety_depressed_6m)
table(df_main$anxiety_depressed_12m)
table(df_main$anxiety_depressed_24m)

# Combine the resp, cognitive and fatigue categories
a3 <- df_main

# a3 <- mutate(a3, Cog_Res_4m = ifelse((cognitive_4m %in% 1 & res_combine_4m %in% 1), 1, 0 ))
# a3 <- mutate(a3, Cog_Fat_4m = ifelse((cognitive_4m %in% 1 & post_acute_4m %in% 1), 1, 0 ))
# a3 <- mutate(a3, Res_Fat_4m = ifelse((res_combine_4m %in% 1 & post_acute_4m %in% 1), 1, 0 ))
# a3 <- mutate(a3, Cog_Res_Fat_4m = ifelse((res_combine_4m %in% 1 & post_acute_4m %in% 1 & cognitive_4m %in% 1), 1, 0 ))

a3 <- mutate(a3, Cog_Res_6m = ifelse((cognitive_6m %in% 1 & res_combine_6m %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Fat_6m = ifelse((cognitive_6m %in% 1 & post_acute_6m %in% 1), 1, 0 ))
a3 <- mutate(a3, Res_Fat_6m = ifelse((res_combine_6m %in% 1 & post_acute_6m %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Res_Fat_6m = ifelse((res_combine_6m %in% 1 & post_acute_6m %in% 1 & cognitive_6m %in% 1), 1, 0 ))

a3 <- mutate(a3, Cog_Res_12m = ifelse((cognitive_12m %in% 1 & res_combine_12m %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Fat_12m = ifelse((cognitive_12m %in% 1 & post_acute_12m %in% 1), 1, 0 ))
a3 <- mutate(a3, Res_Fat_12m = ifelse((res_combine_12m %in% 1 & post_acute_12m %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Res_Fat_12m = ifelse((res_combine_12m %in% 1 & post_acute_12m %in% 1 & cognitive_12m %in% 1), 1, 0 ))

a3 <- mutate(a3, Cog_Res_24m = ifelse((cognitive_24m %in% 1 & res_combine_24m %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Fat_24m = ifelse((cognitive_24m %in% 1 & post_acute_24m %in% 1), 1, 0 ))
a3 <- mutate(a3, Res_Fat_24m = ifelse((res_combine_24m %in% 1 & post_acute_24m %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Res_Fat_24m = ifelse((res_combine_24m %in% 1 & post_acute_24m %in% 1 & cognitive_24m %in% 1), 1, 0 ))

# long term clusters (original 3 only)
# a3 <- mutate(a3, long_term_4m = ifelse((cognitive_4m %in% 1 | res_combine_4m %in% 1 | post_acute_4m %in% 1), 1, 0 ))
a3 <- mutate(a3, long_term_6m = ifelse((cognitive_6m %in% 1 | res_combine_6m %in% 1 | post_acute_6m %in% 1), 1, 0 ))
a3 <- mutate(a3, long_term_12m = ifelse((cognitive_12m %in% 1 | res_combine_12m %in% 1 | post_acute_12m %in% 1) &
                                          long_term_6m==1, 1, 0))
a3 <- mutate(a3, long_term_24m = ifelse((cognitive_24m %in% 1 | res_combine_24m %in% 1 | post_acute_24m %in% 1) &
                                          long_term_6m==1, 1, 0 ))

# table(a3$long_term_4m)
table(a3$long_term_6m)
table(a3$long_term_12m)
table(a3$long_term_24m)

# If patients already captured in main clusters, don't count in sub-clusters
a3 <- mutate(a3,
            # fatigue_4m_residual = ifelse(long_term_4m ==1, 0, fatigue_4m),
            fatigue_6m_residual = ifelse(long_term_6m ==1, 0, fatigue_6m),
            fatigue_12m_residual = ifelse(long_term_12m ==1, 0, fatigue_12m),
            fatigue_24m_residual = ifelse(long_term_24m ==1, 0, fatigue_24m),

            # headache_4m_residual = ifelse(long_term_4m ==1, 0, headache_4m),
            headache_6m_residual = ifelse(long_term_6m ==1, 0, headache_6m),
            headache_12m_residual = ifelse(long_term_12m ==1, 0, headache_12m),
            headache_24m_residual = ifelse(long_term_24m ==1, 0, headache_24m),

            hairloss_6m_residual = ifelse(long_term_6m ==1, 0, hairloss_6m),
            hairloss_12m_residual = ifelse(long_term_12m ==1, 0, hairloss_12m),
            hairloss_24m_residual = ifelse(long_term_24m ==1, 0, hairloss_24m),

            # taste_smell_loss_4m_residual = ifelse(long_term_4m ==1, 0, taste_smell_loss_4m),
            taste_smell_loss_6m_residual = ifelse(long_term_6m ==1, 0, taste_smell_loss_6m),
            taste_smell_loss_12m_residual = ifelse(long_term_12m ==1, 0, taste_smell_loss_12m),
            taste_smell_loss_24m_residual = ifelse(long_term_24m ==1, 0, taste_smell_loss_24m),

            # sleep_prob_4m_residual = ifelse(long_term_4m ==1, 0, sleep_prob_4m),
            sleep_prob_6m_residual = ifelse(long_term_6m ==1, 0, sleep_prob_6m),
            sleep_prob_12m_residual = ifelse(long_term_12m ==1, 0, sleep_prob_12m),
            sleep_prob_24m_residual = ifelse(long_term_24m ==1, 0, sleep_prob_24m),

            # anxiety_depressed_4m_residual = ifelse(long_term_4m ==1, 0, anxiety_depressed_4m),
            anxiety_depressed_6m_residual = ifelse(long_term_6m ==1, 0, anxiety_depressed_6m),
            anxiety_depressed_12m_residual = ifelse(long_term_12m ==1, 0, anxiety_depressed_12m),
            anxiety_depressed_24m_residual = ifelse(long_term_24m ==1, 0, anxiety_depressed_24m),

            # long term clusters (new only)
            # long_term_extended_4m = ifelse(fatigue_4m_residual==1 |
            #                                  headache_4m_residual==1 |
            #                                  taste_smell_loss_4m_residual==1 |
            #                                  sleep_prob_4m_residual==1 |
            #                                  anxiety_depressed_4m_residual==1, 1, 0),
            long_term_extended_6m = ifelse(fatigue_6m_residual==1 |
                                             headache_6m_residual==1 |
                                             hairloss_6m_residual==1 |
                                             taste_smell_loss_6m_residual==1 |
                                             sleep_prob_6m_residual==1 |
                                             anxiety_depressed_6m_residual==1, 1, 0),
            long_term_extended_12m = ifelse(fatigue_12m_residual==1 |
                                              headache_12m_residual==1 |
                                              hairloss_12m_residual==1 |
                                              taste_smell_loss_12m_residual==1 |
                                              sleep_prob_12m_residual==1 |
                                              anxiety_depressed_12m_residual==1, 1, 0),
            long_term_extended_24m = ifelse(fatigue_24m_residual==1 |
                                              headache_24m_residual==1 |
                                              hairloss_24m_residual==1 |
                                              taste_smell_loss_24m_residual==1 |
                                              sleep_prob_24m_residual==1 |
                                              anxiety_depressed_24m_residual==1, 1, 0),

            # long term clusters (original + new)
            # long_term_all_4m = ifelse(long_term_4m==1 | long_term_extended_4m==1, 1, 0),
            long_term_all_6m = ifelse(long_term_6m==1 | long_term_extended_6m==1, 1, 0),
            long_term_all_12m = ifelse(long_term_12m==1 | long_term_extended_12m==1, 1, 0),
            long_term_all_24m = ifelse(long_term_24m==1 | long_term_extended_24m==1, 1, 0)
)

a3 <- mutate(a3,
            long_term = ifelse((#long_term_4m %in% 1 |
                                  long_term_6m %in% 1 |
                                  long_term_12m %in% 1 |
                                  long_term_24m %in% 1), 1, 0 ),
            long_term_extended = ifelse((#long_term_extended_4m %in% 1 |
                                           long_term_extended_6m %in% 1 |
                                           long_term_extended_12m %in% 1 |
                                           long_term_extended_24m %in% 1), 1, 0 ),
            long_term_all = ifelse((#long_term_all_4m %in% 1 |
                                      long_term_all_6m %in% 1 |
                                      long_term_all_12m %in% 1 |
                                      long_term_all_24m), 1, 0 ),
)

# Store survey responses
cols_main <- colnames(a3)[!colnames(a3) %in% c('id_mp','age','age_group_name','age_group_id','sex','sex_id','HI95')]

# overlaps in the res_combine category. need to round them one level up
# a3$overlap <- rowSums(a3[,c("res_mild","res_moderate","res_severe")])
#
# # overlap has been fixed
# DT = as.data.table(a3)
# DT[(overlap >1 & res_mild==res_moderate), res_mild := 0]
# DT[(overlap >1 & res_moderate== res_severe), res_moderate := 0]
# DT <- subset(DT, select= -c(overlap))

# Save output with all responses
DT = as.data.table(a3)
write.xlsx(DT, paste0(outpath,"Extract/Iran_tabulate_all_responses_v", version, ".xlsx"))

# only keep the derived variables
# symp_cols2 <- symp_cols[!symp_cols %in% c("id_mp","fatigue_rule1","fatigue_rule2")]
suffix0 <- c("_12m","_24m")
suffix1 <- c("_4m","_6m","_12m","_24m")
suffix2 <- c("_4m","_4m_residual","_6m","_6m_residual","_12m","_12m_residual","_24m","_24m_residual")
desired_cols <- c("id_mp","follow_up","sex","sex_id","age","age_group_name","age_group_id","HI95","ICU_admission (HI95)",
                  'post_acute','cognitive','res_mild','res_moderate','res_severe','res_combine',
                  'Cog_Res','Cog_Fat','Res_Fat','Cog_Res_Fat',
                  'fatigue','fatigue_residual','headache','headache_residual','hairloss','hairloss_residual',
                  'sleep_prob','sleep_prob_residual','taste_smell_loss','taste_smell_loss_residual',
                  'anxiety_depressed','anxiety_depressed_residual',
                  paste0("post_acute",suffix1), paste0("cognitive",suffix1),
                  "res_mild_6m","res_moderate_6m","res_severe_6m", paste0("res_combine",suffix1),
                  paste0("Cog_Res",suffix1), paste0("Cog_Fat",suffix1), paste0("Res_Fat",suffix1), paste0("Cog_Res_Fat",suffix1),
                  paste0("fatigue",suffix2), paste0("headache",suffix2), paste0("hairloss",suffix2),
                  paste0("taste_smell_loss",suffix2),paste0("sleep_prob",suffix2), paste0("anxiety_depressed",suffix2),
                  paste0("long_term",suffix1), paste0("long_term_extended",suffix1), paste0("long_term_all",suffix1),
                  "long_term","long_term_extended","long_term_all", "N",
                  "long_term_proportion","long_term_extended_proportion","long_term_all_proportion")

selected_cols <- desired_cols[desired_cols %in% colnames(DT)]
dt <- DT[, ..selected_cols]
write.xlsx(dt, paste0(outpath,"Extract/Iran_tabulate_all_data_marked_v", version, ".xlsx"))

# Subset data to export
data_inter <- dt %>%
  select(-which(grepl("id_mp|age$|sex_id$|_overlap|long_term$|extended$|_all$", colnames(dt))))

# Tabulate
data_long <- tidyr::gather(data_inter, measure, value, c(post_acute_6m:long_term_all_24m))

# Table by age and sex only ----------------------------------------------------
cocurr_mu <- data_long %>%
  group_by(age_group_id, sex, measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  tidyr::spread(measure, var)
cocurr_mu[is.na(cocurr_mu)] <- 0

# Transforming table
cocurr_mu2 <- cocurr_mu %>%
  pivot_longer(cols = -c(age_group_id, sex), names_to = "variable", values_to = "value") %>%
  mutate(follow_up = str_extract(variable, "\\d{1,2}m"),
         follow_up = case_when(
           # follow_up == "4m" ~ "04m",
           follow_up == "6m" ~ "06m",
           follow_up == "12m" ~ "12m",
           follow_up == "24m" ~ "24m"),
         variable = str_remove(variable, "_\\d{1,2}m")) %>%
  group_by(age_group_id, sex, follow_up, variable) %>%
  summarise(value = sum(value, na.rm = TRUE)) %>%
  pivot_wider(names_from = variable, values_from = value) %>%
  arrange(follow_up)

cocurr_mu2 <- merge(x=cocurr_mu2, y=age_map, by='age_group_id', all.x=TRUE)
sample_n_byAgesex <- sample_n_byAgesex[, !colnames(sample_n_byAgesex) %in% 'age_group_name']
cocurr_mu2 <- merge(x=cocurr_mu2, y=sample_n_byAgesex, by=c('follow_up','age_group_id','sex'), all.x=TRUE)

cocurr_mu2[is.na(cocurr_mu2)] <- 0
cocurr_mu2 <- mutate(cocurr_mu2,
                     long_term_proportion = long_term/N,
                     long_term_extended_proportion = long_term_extended/N,
                     long_term_all_proportion = long_term_all/N)
cocurr_mu2[sapply(cocurr_mu2, is.nan)] <- NA
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_mu2)]
cocurr_mu2 <- cocurr_mu2[, existing_cols]

# Table by sex only ---------------------------------------------------------
cocurr_sex <- data_long %>%
  group_by(sex, measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  tidyr::spread(measure, var)
cocurr_sex[is.na(cocurr_sex)] <- 0

# Transforming table
cocurr_sex2 <- cocurr_sex %>%
  pivot_longer(cols = -c(sex), names_to = "variable", values_to = "value") %>%
  mutate(follow_up = str_extract(variable, "\\d{1,2}m"),
         follow_up = case_when(
           # follow_up == "4m" ~ "04m",
           follow_up == "6m" ~ "06m",
           follow_up == "12m" ~ "12m",
           follow_up == "24m" ~ "24m"),
         variable = str_remove(variable, "_\\d{1,2}m")) %>%
  group_by(sex, follow_up, variable) %>%
  summarise(value = sum(value, na.rm = TRUE)) %>%
  pivot_wider(names_from = variable, values_from = value) %>%
  arrange(follow_up)

cocurr_sex2 <- merge(x=cocurr_sex2, y=sample_n_bysex, by=c('follow_up','sex'), all.x=TRUE)
cocurr_sex2[is.na(cocurr_sex2)] <- 0
cocurr_sex2 <- mutate(cocurr_sex2,
                     long_term_proportion = long_term/N,
                     long_term_extended_proportion = long_term_extended/N,
                     long_term_all_proportion = long_term_all/N)
cocurr_sex2[sapply(cocurr_sex2, is.nan)] <- NA
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_sex2)]
cocurr_sex2 <- cocurr_sex2[, existing_cols]

# Table by age only ------------------------------------------------------------
cocurr_age <- data_long %>%
  group_by(age_group_name, age_group_id, measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)
cocurr_age[is.na(cocurr_age)] <- 0

# Transforming table
cocurr_age2 <- cocurr_age %>%
  pivot_longer(cols = -c(age_group_id, age_group_name), names_to = "variable", values_to = "value") %>%
  mutate(follow_up = str_extract(variable, "\\d{1,2}m"),
         follow_up = case_when(
           # follow_up == "4m" ~ "04m",
           follow_up == "6m" ~ "06m",
           follow_up == "12m" ~ "12m",
           follow_up == "24m" ~ "24m"),
         variable = str_remove(variable, "_\\d{1,2}m")) %>%
  group_by(age_group_id, age_group_name, follow_up, variable) %>%
  summarise(value = sum(value, na.rm = TRUE)) %>%
  pivot_wider(names_from = variable, values_from = value) %>%
  arrange(follow_up)

sample_n_byage <- sample_n_byage[, !colnames(sample_n_byage) %in% 'age_group_name']
cocurr_age2 <- merge(x=cocurr_age2, y=sample_n_byage, by=c('follow_up','age_group_id'), all.x=TRUE)
cocurr_age2[is.na(cocurr_age2)] <- 0
cocurr_age2 <- mutate(cocurr_age2,
                      long_term_proportion = long_term/N,
                      long_term_extended_proportion = long_term_extended/N,
                      long_term_all_proportion = long_term_all/N)
cocurr_age2[sapply(cocurr_age2, is.nan)] <- NA
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_age2)]
cocurr_age2 <- cocurr_age2[, existing_cols]

# Table by ICU admission -------------------------------------------------------
cocurr_icu <- data_long %>%
  group_by(HI95,measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)
cocurr_icu[is.na(cocurr_icu)] <- 0

# Transforming table
cocurr_icu2 <- cocurr_icu %>%
  pivot_longer(cols = -c(HI95), names_to = "variable", values_to = "value") %>%
  mutate(follow_up = str_extract(variable, "\\d{1,2}m"),
         follow_up = case_when(
           # follow_up == "4m" ~ "04m",
           follow_up == "6m" ~ "06m",
           follow_up == "12m" ~ "12m",
           follow_up == "24m" ~ "24m"),
         HI95 = case_when(
           HI95 < 1 ~ "Unknown",
           HI95 ==1 ~ '1',
           HI95 ==2 ~ '2',
         ),
         variable = str_remove(variable, "_\\d{1,2}m")) %>%
  group_by(HI95, follow_up, variable) %>%
  summarise(value = sum(value, na.rm = TRUE)) %>%
  pivot_wider(names_from = variable, values_from = value) %>%
  arrange(follow_up)

cocurr_icu2 <- merge(x=cocurr_icu2, y=sample_n_byICU, by=c('follow_up','HI95'), all.x=TRUE)
cocurr_icu2[is.na(cocurr_icu2)] <- 0
cocurr_icu2 <- mutate(cocurr_icu2,
                      long_term_proportion = long_term/N,
                      long_term_extended_proportion = long_term_extended/N,
                      long_term_all_proportion = long_term_all/N)
cocurr_icu2[sapply(cocurr_icu2, is.nan)] <- NA
existing_cols <- desired_cols[desired_cols %in% colnames(cocurr_icu2)]
cocurr_icu2 <- cocurr_icu2[, existing_cols]

cocurr_icu2 <- mutate(cocurr_icu2,
                      HI95 = case_when(
                        HI95 == '1' ~ 'Yes',
                        HI95 == '2' ~ 'No',
                        HI95 == 'Unknown' ~ 'Unknown'))
setnames(cocurr_icu2, "HI95", "ICU_admission (HI95)", skip_absent = T)
# str(cocurr_icu2)

# Create a new workbook
wb <- createWorkbook()
addWorksheet(wb, "bySex")
writeData(wb, "bySex", cocurr_sex2)
addWorksheet(wb, "byAge")
writeData(wb, "byAge", cocurr_age2)
addWorksheet(wb, "byICU")
writeData(wb, "byICU", cocurr_icu2)
addWorksheet(wb, "byAgeSex")
writeData(wb, "byAgeSex", cocurr_mu2)
saveWorkbook(wb, paste0(outpath,"Extract/Iran_tabulate_clusters_byGroups_v",version, ".xlsx"), overwrite = TRUE)

# Individual symptoms ----------------------------------------------------
df_symp <- setDT(read.xlsx(paste0(outpath,"symptoms_list.xlsx")))
df_symp$Column3 <- trimws(df_symp$Column3)
df_symp$Var_6m <- paste0("s", df_symp$`Disease.code`,"_6m")
df_symp$Var_12m <- paste0("s", df_symp$`Disease.code`,"_12m")
df_symp$Var_24m <- paste0("s", df_symp$`Disease.code`,"_24m")

str(df_symp)

# Initialize the new columns
df_symp <- df_symp %>%
  mutate(
    No_6m = 0, Yes_6m = 0, Yes_6m_residual = 0,
    No_12m = 0, Yes_12m = 0, Yes_12m_residual = 0,
    No_24m = 0, Yes_24m = 0, Yes_24m_residual = 0
  )

# Function to count values in df
count_values <- function(df, var_name, follow_up="6m") {
  var_data <- df[[var_name]]
  yes_count <- sum(var_data == 1, na.rm = TRUE)
  if (follow_up=="6m"){
    long_term_6m <- df$long_term_6m
    yes_residual <- sum(var_data == 1 & long_term_6m != 1, na.rm = TRUE)
  } else if (follow_up=="12m") {
    long_term_12m <- df$long_term_12m
    yes_residual <- sum(var_data == 1 & long_term_12m != 1, na.rm = TRUE)
  } else if (follow_up=="24m") {
    long_term_24m <- df$long_term_24m
    yes_residual <- sum(var_data == 1 & long_term_24m != 1, na.rm = TRUE)
  }

  no_count <- sum(var_data == 0, na.rm = TRUE)
  unknown_count <- sum(!var_data %in% c(0, 1), na.rm = TRUE)
  return(c(no_count, yes_count, yes_residual, unknown_count))
}

# Loop through each row in df_symp
for (i in 1:nrow(df_symp)) {
  # For 6 months
  var_name <- df_symp$Var_6m[i]
  counts <- count_values(a3, var_name, "6m")
  df_symp$No_6m[i] <- counts[1]
  df_symp$Yes_6m[i] <- counts[2]
  df_symp$Yes_6m_residual[i] <- counts[3]
  # df_symp$Unknown_6m[i] <- counts[4]

  # For 12 months
  var_name <- df_symp$Var_12m[i]
  counts <- count_values(a3, var_name, "12m")
  df_symp$No_12m[i] <- counts[1]
  df_symp$Yes_12m[i] <- counts[2]
  df_symp$Yes_12m_residual[i] <- counts[3]
  # df_symp$Unknown_6m[i] <- counts[4]

  # For 24 months
  var_name <- df_symp$Var_24m[i]
  counts <- count_values(a3, var_name, "24m")
  df_symp$No_24m[i] <- counts[1]
  df_symp$Yes_24m[i] <- counts[2]
  df_symp$Yes_24m_residual[i] <- counts[3]
  # df_symp$Unknown_6m[i] <- counts[4]
}

# Remove unnecessary columns
df_symp$Var_6m <- NULL
df_symp$Var_12m <- NULL
df_symp$Var_24m <- NULL
df_symp$Row <- NULL
df_symp$Relevant <- NULL

# Add total residual column
df_symp$Total_residual <- df_symp$Yes_6m_residual + df_symp$Yes_12m_residual + df_symp$Yes_24m_residual

setnames(df_symp, "Column3", 'Disease/symptom Name', skip_absent = T)
# cols_selected <- c('Disease/symptom Name','dis2-sym1-sign1','Disease.code.-.WHO','Disease.code','Relevant',
#                    'No_6m','Yes_6m','Yes_6m_residual','No_12m',	'Yes_12m','Yes_12m_residual',	'No_24m',
#                    'Yes_24m','Yes_24m_residual','Total_residual')
# df_symp <- df_symp[, ..cols_selected]
write.xlsx(df_symp, paste0(outpath,"Extract/Iran_tabulate_individual_symptoms_v", version, ".xlsx"))

# Please ignore this part below ------------------------------------------------------
# Select new columns
# c("post_acute","cognitive","res_mild","res_moderate","res_severe","res_combine","Cog_Res","Cog_Fat","Res_Fat","Cog_Res_Fat","long_term")
# new_cols <- c("ID_send_file","id_mp","sex","age","age_group_name","age_group_id","HI95")
# new_cols2 <- colnames(df_main)[colnames(df_main) %like% "mha|mhb|Hp19|long_term$" & !colnames(df_main) %in% c("mha14","mha15","mha16")]
# new_cols2 <- c("ID_send_file",new_cols2,paste0("HI",c(11:30)))
#
# # Remove columns that contain only NA values or has long characters (not numeric answer)
# data_symp <- DT[, ..new_cols2]
# data_symp <- data_symp %>% select_if(~any(!is.na(.)) & max(nchar(.), na.rm=T)<=2)
# data_symp$ID_send_file <- DT$ID_send_file
#
# combined_counts <- data.frame()
# followup_cols <- setdiff(names(data_symp), "ID_send_file")
#
# # Loop through each column and calculate the count of unique values
# for (col in followup_cols) {
#   # print(col)
#   # Create a table for the unique values
#   counts <- table(data_symp[[col]])
#
#   # Convert the table to a data frame
#   counts_df <- as.data.frame(counts)
#
#   # Rename the columns for merging
#   colnames(counts_df) <- c("Value", "Count")
#
#   # Add a column with the name of the current column being processed
#   counts_df$Variable <- col
#
#   # Bind the current counts data frame to the combined counts data frame
#   combined_counts <- rbind(combined_counts, counts_df)
# }
#
# # Pivot the dataframe to a wider format
# test <- pivot_wider(combined_counts, names_from = Value, values_from = Count, values_fill = list(Count = 0))
# wide_counts <- pivot_wider(combined_counts, names_from = Value, values_from = Count, values_fill = list(Count = 0))
# wide_counts <- wide_counts[, c("Variable",0:6)]
#
# # Add a new column that is the sum of columns whose values aren't 0 or 1 for each row
# wide_counts <- wide_counts %>% mutate(sum_row = rowSums(select(., -c("Variable","1","2")), na.rm = TRUE))
# # wide_counts <- wide_counts %>% mutate(sum_unknown = rowSums(select(., c("2")), na.rm = TRUE))
#
# # Subset to yes/no/unknown variables
# exception_cols <- paste0("mha",c(141:143,151:159))
# wide_counts <- wide_counts[wide_counts$sum_row == 0 | wide_counts$Variable %in% exception_cols,]
# wide_counts$`2` <- wide_counts$`0` + wide_counts$`2`
# setnames(wide_counts, "Variable","Variables")
# wide_counts <- wide_counts[,c("Variables","1","2")]
# colnames(wide_counts) <- c("Variables","Yes","No")
#
# # Merge w/ dictionary
# df_dict <- setDT(read_xlsx(paste0(outpath,"FILEPATH/Iran_cohort_dictionary_old.xlsx")))
# wide_counts2 <- merge(wide_counts, df_dict[, c("Variables","Questions","Responses")], by="Variables", all.x=T)
#
# # Calculate those not captured in original clusters
# bin_vars <- wide_counts2$Variables
# bin_vars_longCovid_residual <- paste0(bin_vars, "_longCovid_residual")
#
# newDT <- DT
# for (var in bin_vars) {
#   # var_longCovid <- paste0(var, "_longCovid")
#   var_longCovid_residual <- paste0(var, "_longCovid_residual")
#   # var_longCovid_residual_ext <- paste0(var, "_longCovid_residual_ext")
#   newDT <- newDT %>%
#     mutate(
#       !!sym(var_longCovid_residual) := ifelse(
#         long_term_12m!=1 & long_term!=1 & .data[[var]]==1, 1, 0)
#     )
# }
#
# # Calculate total patients with long Covid w/out duration criteria, not captured in original clusters
# df_all_residual <- newDT %>% select(all_of(bin_vars_longCovid_residual))
# df_all_residual <- sapply(df_all_residual, as.numeric)
# df_all_residual <- data.frame(colSums(df_all_residual, na.rm = TRUE))
# colnames(df_all_residual) <- "yes_longCovid_residual"
# df_all_residual$Variable_longCovid_residual <- rownames(df_all_residual)
# rownames(df_all_residual) <- c(1:nrow(df_all_residual))
# df_all_residual <- df_all_residual %>%
#   mutate(Variables = stringr::str_replace(Variable_longCovid_residual, "_longCovid_residual", ""))
# df_all_residual$Variable_longCovid_residual <- NULL
#
#
# # Merge with main
# wide_counts2 <- merge(wide_counts2, df_all_residual, by="Variables", all.x=T)
# wide_counts2 <- wide_counts2[, c("Questions","Responses","Variables","No","Yes","yes_longCovid_residual")]
# setnames(wide_counts2, "yes_longCovid_residual" , "Yes_residual")
# wide_counts3 <- wide_counts2[!wide_counts2$Responses %like% "Vaccination" & !wide_counts2$Questions %like% "before|at time of",]
#
# write.xlsx(wide_counts3, paste0(outpath,"Extract/Iran_tabulate_individual_symptoms_v", version, ".xlsx"))
# write.xlsx(newDT, paste0(outpath,"Extract/Iran_tabulate_all_clusters_data_v", version, ".xlsx"))
#
# # Fuzzy match symptoms ----------------------------------------------------
# # wide_counts_new <- wide_counts2[!wide_counts2$Responses %like% "Vaccination|Men|Women",]
# # wide_counts_new <- wide_counts_new %>%
# #   mutate(Symptoms = ifelse(str_detect(Responses, "\\d[:\\-]"),
# #                            str_extract(Responses, "(?<=\\d[:\\-]).*"),Responses)) %>%
# #   mutate(Symptoms = trimws(str_replace_all(Symptoms, "[0-9:\\-]", "")),
# #          Symptoms = str_replace_all(Symptoms, "^\\) ", ""))
# #
# #
# # # Subset to keep only the 'Variables' and 'Symptoms' columns
# # df_subset <- wide_counts_new %>%
# #   select(Variables, Symptoms)
# #
# # # Compute a matrix of string distances using Jaro-Winkler method
# # distance_matrix <- stringdist::stringdistmatrix(df_subset$Symptoms, df_subset$Symptoms, method = "jw")
# #
# # # Convert the distance matrix to a format that can be used to identify similar pairs
# # distance_data <- as.data.frame(as.table(distance_matrix))
# #
# # # Filter out identical matches and very distant matches
# # similar_symptoms <- distance_data %>%
# #   filter(Freq < 0.38, Var1 != Var2) %>%
# #   mutate(Symptom1 = df_subset$Symptoms[Var1],
# #          Var1 = df_subset$Variables[Var1],
# #          Symptom2 = df_subset$Symptoms[Var2],
# #          Var2 = df_subset$Variables[Var2]) %>%
# #   arrange(Var1,Freq)
# #
# # # Remove irrelevant and unmatched symptoms
# # similar_symptoms <- similar_symptoms[(!similar_symptoms$Symptom1 %like% "Vaccination") &
# #                                        ((similar_symptoms$Symptom1 %like% "Diabetes" & similar_symptoms$Freq <= 0.18) |
# #                                           (similar_symptoms$Symptom2 == "Dizziness" & similar_symptoms$Symptom1 %like% "dizziness") |
# #                                           similar_symptoms$Freq <= 0.1),]
# #
# # # Retain unique pairings of symptoms before and after admission/covid
# # sorted_combinations <- unique(as.data.frame(t(apply(similar_symptoms[c("Var1", "Var2")], 1, sort))))
# # names(sorted_combinations) <- c("Var1", "Var2")
# #
# # # Remove pair without after symptoms
# # sorted_combinations <- sorted_combinations[!(sorted_combinations$Var1 %like% "HI|mhb|Hp19a" & sorted_combinations$Var2 %like% "HI|mhb|Hp19a"),]
# #
# # sorted_combinations <- sorted_combinations %>%
# #   mutate(
# #     new_Var1 = if_else(grepl("mha", Var1), Var2, Var1),
# #     new_Var2 = if_else(grepl("mha", Var1), Var1, Var2)
# #   ) %>%
# #   select(-Var1, -Var2) %>%
# #   rename(Var1 = new_Var1, Var2 = new_Var2)
# #
# # var_before <- sorted_combinations$Var1
# # var_after <- sorted_combinations$Var2
# # write.xlsx(sorted_combinations, paste0(outpath,"Iran_matched_symptoms.xlsx"))
#
# # Import data
# var_matched <- setDT(read_xlsx(paste0(outpath,"Iran_matched_symptoms.xlsx")))
# var_matched <- var_matched[!(is.na(Symptoms_before)), c("Symptoms_before","Symptoms_after")]
#
# # Separate the entries in 'Symptoms_before' that are separated by commas into multiple rows
# var_matched2 <- var_matched %>%
#   separate_rows(Symptoms_before, sep = ",\\s*")
#
# var_before <- var_matched2$Symptoms_before
# var_after <- var_matched2$Symptoms_after
#
# # Create another wide count table
# wide_counts_test <- merge(wide_counts, df_dict[, c("Variables","Questions","Responses")], by="Variables")
# wide_counts_test <- wide_counts_test[wide_counts_test$Variables %in% var_after,]
#
# # Calculate those not captured in original clusters
# var_after_longCovid <- paste0(var_after, "_longCovid")
# var_after_longCovid_residual <- paste0(var_after, "_longCovid_residual")
#
# DT2 <- DT
# for (var in var_after) {
#   var_b4 <- var_matched2$Symptoms_before[var_matched2$Symptoms_after == var]
#   var_longCovid <- paste0(var, "_longCovid")
#   var_longCovid_residual <- paste0(var, "_longCovid_residual")
#   # var_longCovid_residual_ext <- paste0(var, "_longCovid_residual_ext")
#
#   if (length(var_b4)==2){
#     var1_b4 <- var_b4[1]
#     var2_b4 <- var_b4[2]
#     DT2 <- DT2 %>%
#       mutate(
#         !!sym(var_longCovid) := ifelse(
#           (.data[[var1_b4]]==2 | .data[[var2_b4]]==2) & .data[[var]]==1, 1, 0))
#   } else {
#     DT2 <- DT2 %>%
#       mutate(
#         !!sym(var_longCovid) := ifelse(
#           .data[[var_b4]]==2 & .data[[var]]==1, 1, 0))
#   }
#
#   DT2 <- DT2 %>%
#     mutate(
#       !!sym(var_longCovid_residual) := ifelse(
#         (long_term_12m==1 | long_term==1), 0, .data[[!!sym(var_longCovid)]])
#     )
# }
#
#
# # Calculate total patients with long Covid
# df_all_lc <- DT2 %>% select(all_of(var_after_longCovid))
# df_all_lc <- data.frame(colSums(df_all_lc, na.rm = TRUE))
# colnames(df_all_lc) <- "yes_longCovid"
# df_all_lc$Variable_longCovid <- rownames(df_all_lc)
# rownames(df_all_lc) <- c(1:nrow(df_all_lc))
# df_all_lc <- df_all_lc %>%
#   mutate(Variables = str_replace(Variable_longCovid, "_longCovid", ""))
# df_all_lc$Variable_longCovid <- NULL
#
# # Calculate total patients with long Covid w/out duration criteria, not captured in original clusters
# df_all_residual <- DT2 %>% select(all_of(var_after_longCovid_residual))
# df_all_residual <- sapply(df_all_residual, as.numeric)
# df_all_residual <- data.frame(colSums(df_all_residual, na.rm = TRUE))
# colnames(df_all_residual) <- "yes_longCovid_residual"
# df_all_residual$Variable_longCovid_residual <- rownames(df_all_residual)
# rownames(df_all_residual) <- c(1:nrow(df_all_residual))
# df_all_residual <- df_all_residual %>%
#   mutate(Variables = stringr::str_replace(Variable_longCovid_residual, "_longCovid_residual", ""))
# df_all_residual$Variable_longCovid_residual <- NULL
#
# # Merge with main
# wide_counts_test <- merge(wide_counts_test, df_all_lc, by="Variables", all.x=T)
# wide_counts_test <- merge(wide_counts_test, df_all_residual, by="Variables", all.x=T)
#
# setnames(var_matched, "Symptoms_after", "Variables")
# wide_counts_test <- merge(wide_counts_test, var_matched, by="Variables", all.x=T)
# setnames(wide_counts_test, "Variables", "Symptoms_after")
# wide_counts_test <- wide_counts_test[, c("Questions","Responses","Symptoms_after", "Symptoms_before",
#                                          "No","Yes","yes_longCovid","yes_longCovid_residual")]
# write.xlsx(wide_counts_test, paste0(outpath,"Extract/Iran_tabulate_longCovid_symptoms_v", version, ".xlsx"))
#
# # Merge byGroups cluster from 12m followup -------------------------------------
# # By sex
# df_sex <- read_csv(paste0(outpath,"FILEPATH/Iran_bySex_v1.csv"))
# df_sex$...1 <- NULL
# setnames(df_sex, "Dm3","sex")
# df_sex$followup_time <- "1st_followup"
# cocurr_sex$followup_time <- "2nd_followup"
# df_sex <- rbind(df_sex, cocurr_sex)
# df_sex <- df_sex[, c("followup_time", setdiff(colnames(df_sex), c("followup_time")))]
#
# # By age
# df_age <- read_csv(paste0(outpath,"FILEPATH/Iran_byAgeSex_v1.csv"))
# df_age$...1 <- NULL
# df_age$Dm3 <- NULL
# df_age <- merge(df_age, age_map, by='age_group_name')
#
# df_age <- df_age %>%
#   group_by(age_group_name,age_group_id) %>%
#   summarise(across(everything(), ~sum(.x, na.rm = TRUE)), .groups = "drop") %>%
#   arrange(age_group_id)
#
# df_age$followup_time <- "1st_followup"
# cocurr_age$followup_time <- "2nd_followup"
# df_age <- rbind(df_age, cocurr_age)
# df_age <- df_age[, c("followup_time", setdiff(colnames(df_age), c("followup_time")))]
#
# # By sex & age
# df_sexage <- read_csv(paste0(outpath,"FILEPATH/Iran_byAgeSex_v1.csv"))
# df_sexage$...1 <- NULL
# setnames(df_sexage, "Dm3","sex")
# df_sexage <- merge(df_sexage, age_map, by='age_group_name')
#
# df_sexage$followup_time <- "1st_followup"
# cocurr_mu$followup_time <- "2nd_followup"
# df_sexage <- rbind(df_sexage, cocurr_mu)
# df_sexage <- df_sexage[, c("followup_time","age_group_name","age_group_id",
#                            setdiff(colnames(df_sexage), c("followup_time","age_group_name","age_group_id")))]
# df_sexage <- df_sexage %>%
#   arrange(followup_time, age_group_id, sex)
#
# # By ICU (no 12-month icu data)
# df_icu <- cocurr_icu
# df_icu$followup_time <- "2nd_followup"
# df_icu <- df_icu[, c("followup_time", setdiff(colnames(df_icu), c("followup_time")))]
#
# # Create a new workbook
# wb1 <- createWorkbook()
# addWorksheet(wb1, "bySex")
# writeData(wb1, "bySex", df_sex)
# addWorksheet(wb1, "byAge")
# writeData(wb1, "byAge", df_age)
# addWorksheet(wb1, "byICU")
# writeData(wb1, "byICU", df_icu)
# addWorksheet(wb1, "byAgeSex")
# writeData(wb1, "byAgeSex", df_sexage)
# saveWorkbook(wb1, paste0(outpath,"Extract/Iran_tabulate_all_clusters_byGroups_v",version, ".xlsx"), overwrite = TRUE)
#
# # All individual symptoms from different followups -----------------------------
# wide_counts_all1 <- wide_counts_test[, !colnames(wide_counts_test) %in% c("Questions","Responses")]
# wide_counts_all2 <- wide_counts3
# setnames(wide_counts_all2, "Variables", "Symptoms_after", skip_absent=T)
#
# wide_counts_all <- merge(wide_counts_all2, wide_counts_all1, all.x=T)
# wide_counts_all <- wide_counts_all[, c("Questions","Responses","Symptoms_after", "Symptoms_before",
#                                          "No","Yes","Yes_residual","yes_longCovid","yes_longCovid_residual")]
# setnames(wide_counts_all, 'Questions', 'Questions (Symptoms_after)')
#
# write.xlsx(wide_counts_all, paste0(outpath,"Extract/Iran_tabulate_all_individual_symptoms_v", version, ".xlsx"))
#
#
# nrow(df_old) - sum(is.na(df_old$id_mp))
# nrow(df) - sum(is.na(df$id_mp))



