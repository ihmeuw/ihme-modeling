## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script:
## Description: Russian Data recoded_using 1000+ data
## Contributors: NAME
## Date 1/13/2022
## Time stamp:
## 8/2/2021 Made updates in the path and also included PCR=1 patients into the analysis
## 1/13/2022 working on the new Russian data
## 4/8/2024 defining new long covid clusters
## --------------------------------------------------------------------- ----

# remove the history and call the GBD functions
rm(list=ls(all.names=T))

library(readxl)
library(data.table)
library(dplyr)
library(openxlsx)
library(forcats)

## Environment Prep ---------------------------------------------------- ----

# rm(list=ls(all.names=T))

version <- 18

# 17 do not count 12 mo ongoing long COVID cases if they did not have the given symptom cluster at their 6 month follow-up
# 16 fix eq5d pd variable names

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k ,"FILEPATH/get_location_metadata.R"))
source(paste0(roots$k ,"FILEPATH/get_age_metadata.R"))

## --------------------------------------------------------------------------

# Summary score using Dutch Tariff method
eq5d_summary <- data.frame(
  levels = c(1:5),
  mb_score = c(0, -0.032, -0.056, -0.166, -0.202),
  sc_score = c(0, -0.039, -0.064, -0.18, -0.165),
  ua_score = c(0, -0.04, -0.09, -0.207, -0.181),
  pd_score = c(0, -0.064, -0.089, -0.353, -0.42),
  ad_score = c(0, -0.073, -0.146, -0.36, -0.425)
)

eq5d_constant <- 0.956

## --------------------------------------------------------------------------
tabulation <- function(path1, path2, wave_number, outpath, followup_time, typeData){

  if(wave_number=='wave_1'){
    mod1 <-
      read_excel(path1)[,c("PIN","Age","Sex","COVID PCR")]
    mod1 <- mod1[mod1$`COVID PCR` %in% 1,]
  } else{
    mod1 <-
      read_excel(path1)[,c("PIN","Age","Sex","Diagnosis (ICD)")]
    mod1 <- mod1[mod1$`Diagnosis (ICD)` %ni% c("NA"),]
  }

  message(paste("Average age: "), mean(mod1$Age))

  ages <- get_age_metadata(age_group_set_id = 19,
                           gbd_round_id = roots$gbd_round)[, !c('most_detailed')][,c('age_group_id','age_group_name')]


  mod12 <- mutate(mod1, age_group_name = ifelse(Age %in% 15:19, "15 to 19",
                                         ifelse(Age %in% 20:24, "20 to 24",
                                         ifelse(Age %in% 25:29, "25 to 29",
                                         ifelse(Age %in% 30:34, "30 to 34",
                                         ifelse(Age %in% 35:39, "35 to 39",
                                         ifelse(Age %in% 40:44, "40 to 44",
                                         ifelse(Age %in% 45:49, "45 to 49",
                                         ifelse(Age %in% 50:54, "50 to 54",
                                         ifelse(Age %in% 55:59, "55 to 59",
                                         ifelse(Age %in% 60:64, "60 to 64",
                                         ifelse(Age %in% 65:69, "65 to 69",
                                         ifelse(Age %in% 70:74, "70 to 74",
                                         ifelse(Age %in% 75:79, "75 to 79",
                                         ifelse(Age %in% 80:84, "80 to 84",
                                         ifelse(Age %in% 85:89, "85 to 89",
                                         ifelse(Age %in% 90:94, "90 to 94", "95 plus")))))))))))))))))

  age_mod12 <- merge(mod12, ages, by='age_group_name')

  # Import followup data
  followup_orig <- read_excel(path2)

  # for some reason, in the raw followup data, flw_eq5d_pd contains the responses at follow-up, and flw_eq5d_pd_2 contains the
  # responses pre-COVID.  Switching them in the code now so the algorithm is logical later
  followup_orig$flw_eq5d_pd_2_orig <- followup_orig$flw_eq5d_pd_2
  followup_orig$flw_eq5d_pd_orig <- followup_orig$flw_eq5d_pd
  followup_orig$flw_eq5d_pd_2 <- followup_orig$flw_eq5d_pd_orig
  followup_orig$flw_eq5d_pd <- followup_orig$flw_eq5d_pd_2_orig

  # same situation occurs for flw_eq5d5l_vas and flw_eq5d5l_vas_2
  followup_orig$flw_eq5d5l_vas_2_orig <- followup_orig$flw_eq5d5l_vas_2
  followup_orig$flw_eq5d5l_vas_orig <- followup_orig$flw_eq5d5l_vas
  followup_orig$flw_eq5d5l_vas_2 <- followup_orig$flw_eq5d5l_vas_orig
  followup_orig$flw_eq5d5l_vas <- followup_orig$flw_eq5d5l_vas_2_orig

  # Delete extra cols
  followup_orig$flw_eq5d_pd_orig <- NULL
  followup_orig$flw_eq5d_pd_2_orig <- NULL
  followup_orig$flw_eq5d5l_vas_orig <- NULL
  followup_orig$flw_eq5d5l_vas_2_orig <- NULL

  followup <- followup_orig[,c("subjid",'per_fat','per_fat_dur','flw_breathless','flw_breathless_dur','flw_recovered',
                               'flw_loss_taste','flw_loss_taste_dur','flw_loss_smell','flw_loss_smell_dur',
                               "flw_eq5d_ad","flw_eq5d_ad_2",'flw_eq5d_ua', 'flw_eq5d_ua_2', 'flw_eq5d_pd_2', 'flw_eq5d_pd',
                               'flw_breathless_now','flw_breathless_pre_c19','flw_limb_weakness','flw_fatigue','for_cov','for_cov_dur',
                               'flw_remember_today','flw_remember_pre_c19','flw_pers_cough','flw_pers_cough_dur',
                               'phq_4_stress','phq_4_worries','mus_pain','mus_pain_dur','flw_joint_pain','flw_joint_pain_dur',
                               'rig_mus','rig_mus_dur','flw_chest_pains','flw_chest_pains_dur','flw_pain_breathing','flw_pain_breathing_dur',
                               'anx_cov','anx_cov_dur','depr_mood','depr_mood_dur',"flw_confusion",'flw_confusion_dur',
                               'flw_eq5d5l_vas','flw_eq5d5l_vas_2','flw_eq5d_mb','flw_eq5d_mb_2','flw_eq5d_sc','flw_eq5d_sc_2')]

  # Store survey responses
  followup_cols <- setdiff(colnames(followup), "subjid")

  # Rename ID column
  names(age_mod12)[names(age_mod12) == "PIN"] <- "subjid"

  followup2 <- followup_orig[, !grepl("_dur|_date|weight|height|vas|house|employ|survey|_up1|_vaccine|_readm|_icu|cough_y|_ethinicity|_bod_|_diag|_fever|thyr|func_|ant_cov|_no$|_other_symp", colnames(followup_orig)) | colnames(followup_orig) == "flw_weight_loss"]

  followup3 <- followup_orig[, grepl("_dur", colnames(followup_orig))]

  # Remove columns that contain only NA values
  followup2 <- followup2 %>% select_if(~any(!is.na(.)) & is.numeric(.))
  followup3 <- followup3 %>% select_if(~any(!is.na(.)) & is.numeric(.))

  comb_counts <- function(followup) {
    # Initialize an empty data frame to store the combined counts
    combined_counts <- data.frame()
    followup_cols <- setdiff(names(followup), "subjid")

    # Loop through each column and calculate the count of unique values
    for (col in followup_cols) {
      # print(col)
      # Create a table for the unique values
      counts <- table(followup[[col]])

      # Convert the table to a data frame
      counts_df <- as.data.frame(counts)

      # Rename the columns for merging
      colnames(counts_df) <- c("Value", "Count")

      # Add a column with the name of the current column being processed
      counts_df$Variable <- col

      # Bind the current counts data frame to the combined counts data frame
      combined_counts <- rbind(combined_counts, counts_df)
    }

    # Pivot the dataframe to a wider format
    wide_counts <- pivot_wider(combined_counts, names_from = Value, values_from = Count, values_fill = list(Count = 0))

    # Reorder some columns
    if (0 %in% colnames(wide_counts)){
      wide_counts <- wide_counts[c("Variable", "0", setdiff(names(wide_counts), c("Variable", "0")))]

      # Add a new column that is the sum of columns 4 to 10 for each row
      wide_counts <- wide_counts %>% mutate(sum_row = rowSums(select(., -c("Variable","0","1","2","3")), na.rm = TRUE))
      wide_counts <- wide_counts %>% mutate(sum_unknown = rowSums(select(., c("2","3")), na.rm = TRUE))

      # Subset to yes/no/unknown variables
      wide_counts$binary <- ifelse(wide_counts$sum_row == 0, 1, 0)
      wide_counts <- wide_counts[wide_counts$binary == 1,]
      wide_counts <- wide_counts[,c("Variable","0","1","sum_unknown")]
      colnames(wide_counts) <- c("Variable","No","Yes","Unknown")
    }

    return(as.data.frame(wide_counts))
  }

  wide_counts2 <- comb_counts(followup2)
  wide_counts3 <- comb_counts(followup3)

  # Verify if duration variable exists for each symptom
  bin_vars <- sub("(_\\d+)?$", "_dur\\1", wide_counts2$Variable)
  bin_vars_dur <- wide_counts3$Variable
  bin_vars_select <- bin_vars_dur[bin_vars_dur %in% bin_vars]
  bin_vars_select <- str_replace(bin_vars_select, "_dur", "")

  # Select only symptoms that have duration
  wide_counts2 <- wide_counts2[wide_counts2$Variable %in% bin_vars_select,]

  # Merge age group w/ followup data
  mod12_fol <- merge(age_mod12, followup_orig)

  # Calculate eq5d summary score pre Covid using Dutch Tariff method
  mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','mb_score')], by.x = 'flw_eq5d_mb', by.y = 'levels', all.x=T)
  mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','sc_score')], by.x = 'flw_eq5d_sc', by.y = 'levels', all.x=T)
  mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','ad_score')], by.x = 'flw_eq5d_ad', by.y = 'levels', all.x=T)
  mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','pd_score')], by.x = 'flw_eq5d_pd', by.y = 'levels', all.x=T)
  mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','ua_score')], by.x = 'flw_eq5d_ua', by.y = 'levels', all.x=T)

  mod12_fol$eq5d_summary <-
    eq5d_constant + mod12_fol$mb_score + mod12_fol$sc_score + mod12_fol$ad_score + mod12_fol$pd_score + mod12_fol$ua_score

  # Remove the existing '_score' columns
  mod12_fol <- select(mod12_fol, -c(mb_score,sc_score,ad_score,pd_score,ua_score))

  # Calculate eq5d summary score post Covid using Dutch Tariff method
  mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','mb_score')], by.x = 'flw_eq5d_mb_2', by.y = 'levels', all.x=T)
  mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','sc_score')], by.x = 'flw_eq5d_sc_2', by.y = 'levels', all.x=T)
  mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','ad_score')], by.x = 'flw_eq5d_ad_2', by.y = 'levels', all.x=T)
  mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','pd_score')], by.x = 'flw_eq5d_pd_2', by.y = 'levels', all.x=T)
  mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','ua_score')], by.x = 'flw_eq5d_ua_2', by.y = 'levels', all.x=T)

  mod12_fol$eq5d_summary_2 <-
    eq5d_constant + mod12_fol$mb_score + mod12_fol$sc_score + mod12_fol$ad_score + mod12_fol$pd_score + mod12_fol$ua_score

  # Remove the existing '_score' columns
  mod12_fol <- select(mod12_fol, -c(mb_score,sc_score,ad_score,pd_score,ua_score))

  # Add a new column that checks if eq5d pair have non-null values
  mod12_fol$eq5d_pair <- apply(mod12_fol, 1, function(x) {
    valid_mb = !is.na(x['flw_eq5d_mb']) & !is.na(x['flw_eq5d_mb_2'])
    valid_sc = !is.na(x['flw_eq5d_sc']) & !is.na(x['flw_eq5d_sc_2'])
    valid_ad = !is.na(x['flw_eq5d_ad']) & !is.na(x['flw_eq5d_ad_2'])
    valid_pd = !is.na(x['flw_eq5d_pd']) & !is.na(x['flw_eq5d_pd_2'])
    valid_ua = !is.na(x['flw_eq5d_ua']) & !is.na(x['flw_eq5d_ua_2'])

    # Sum the valid flags, and check if the sum is greater than or equal to 3
    sum_valid = sum(valid_mb, valid_sc, valid_ad, valid_pd, valid_ua)
    return(sum_valid >= 1) # Return TRUE if 1 or more pairs are valid
  })

  # Getting the number of patients by age and by sex
  sample_n <- mod12_fol %>%
    group_by(age_group_id, age_group_name, Sex) %>%
    dplyr::summarise(N=n())

  # Getting the number of patients by sex
  sample_n_bysex <- mod12_fol %>%
    group_by(Sex) %>%
    dplyr::summarise(N=n())

  # Post-acute consequences of infectious disease
  # •	Post-acute consequences of infectious disease (fatigue, emotional stability, insomnia)
  # lay description: “is always tired and easily upset. The person feels pain all over the body and is depressed”
  post <- mod12_fol[,c("subjid",'flw_limb_weakness','per_fat','flw_fatigue','flw_eq5d_ua',
                       'flw_eq5d_ua_2', 'flw_eq5d_pd_2', 'flw_eq5d_pd',"flw_eq5d_ad","flw_eq5d_ad_2")]

  # "Allow one of the defining items (per_fat, flw_limb_weakness, flw_fatigue, flw_eq5d_ad_2, and flw_eq5d_pd_2) to have missing value"
  # post$na_count <- apply(post, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  # post <- post[post$na_count <=1,]

  post <- mutate(post, post_acute = ifelse((per_fat %in% 1 | flw_limb_weakness %in% 1) &
                                           (flw_fatigue >=3 | is.na(flw_fatigue)) &
                                           (flw_eq5d_ad_2 > 2 | flw_eq5d_pd_2 > 2 ) &
                                           ((flw_eq5d_ad_2 > flw_eq5d_ad) | (flw_eq5d_pd_2 > flw_eq5d_pd) | (flw_eq5d_ua_2 > flw_eq5d_ua)),
                                           1,0))

  message(paste("Average post-acute fatigue: "), mean(post$post_acute, na.rm=T))

  # •	Cognition problems
  cog <- mod12_fol[,c("subjid","flw_confusion",'for_cov','flw_remember_today','flw_remember_pre_c19','flw_eq5d_ua_2')]

  # lay description for mild dementia: “has some trouble remembering recent events,
  # and finds it hard to concentrate and make decisions and plans”

  # "Allow one of the defining items (flw_confusion, for_cov, flw_remember_today, flw_remember_pre_c19, flw_eq5d_ua_2) to have missing value"
  cog$na_count <- apply(cog, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  cog <- cog[cog$na_count <=1,]

  # Mild cognitive problem
  cog <- mutate(cog, cog_mild = ifelse((((for_cov %in% 1 | flw_confusion %in% 1) &
                                         (flw_remember_today %in% 2) &
                                         (flw_eq5d_ua_2 >=3) &
                                         (flw_remember_today > flw_remember_pre_c19)) |
                                        ((for_cov %in% 1 | flw_confusion %in% 1) &
                                         (flw_remember_today %in% 3) &
                                        ((flw_eq5d_ua_2 %in% c(2,3))) &
                                         (flw_remember_today > flw_remember_pre_c19))),
                                       1,0))
  # Moderate cognitive problem
  cog <- mutate(cog, cog_moderate = ifelse((((for_cov %in% 1 | flw_confusion %in% 1) &
                                             (flw_remember_today %in% 3) &
                                             (flw_eq5d_ua_2 >=4) &
                                             (flw_remember_today > flw_remember_pre_c19)) |
                                            ((for_cov %in% 1 | flw_confusion %in% 1) &
                                             (flw_remember_today %in% 4) &
                                             (flw_eq5d_ua_2 >=2) &
                                             (flw_remember_today > flw_remember_pre_c19))),
                                           1,0))

  # Combined cognitive category(if cog_mild =1 | cog_moderate =1, then cognitive =1 )
  cog <- mutate(cog, cognitive = ifelse((cog_mild %in% 1 | cog_moderate %in% 1), 1, 0))
  message(paste("Average cognitive: "), mean(cog$cognitive))

  # •	Respiratory problems
  # lay description for mild COPD: “has cough and shortness of breath after heavy physical activity,
  # but is able to walk long distances and climb stairs”
  # res_mild
  res <- mod12_fol[, c("subjid",'flw_breathless_now', 'flw_pers_cough','flw_eq5d_ua_2','flw_eq5d_ua',
                       'flw_breathless_pre_c19','flw_fatigue','phq_4_stress', 'phq_4_worries')]

  res$na_count <- apply(res, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  res <- res[res$na_count <=1,]

  res <- mutate(res, res_mild = ifelse((((flw_breathless_now %in% 2 | (flw_pers_cough %in% 1 & flw_breathless_now <=3 )) &
                                          flw_eq5d_ua_2 %in% 2) |
                                        ((flw_breathless_now %in% 3) &
                                         (flw_pers_cough %in% 1) &
                                         (flw_eq5d_ua_2 %in% 2)) |
                                         (flw_breathless_now %in% 3 &
                                          flw_pers_cough %in% 0 & flw_eq5d_ua_2 >= 3)) &
                                         (flw_breathless_now > flw_breathless_pre_c19),
                                       1,0))

  # res_moderate
  res <- mutate(res, res_moderate = ifelse(((flw_breathless_now %in% 3 & flw_pers_cough %in% 1 &
                                            (flw_eq5d_ua_2 >= 3 | flw_fatigue %in% c(4,5,6))) |
                                            (flw_breathless_now %in% 4 & flw_eq5d_ua_2 %in% 2 & flw_fatigue <7 &
                                            (phq_4_stress <3 & phq_4_worries <3)) |
                                            (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 %in% 2 | flw_fatigue %in% c(4,5,6)))) &
                                            (flw_breathless_now > flw_breathless_pre_c19),
                                           1,0))

  # res_severe
  # lay description for severe COPD: “has cough, wheezing and shortness of breath all the time.
  # The person has great difficulty walking even short distances or climbing any stairs,
  # feels tired when at rest, and is anxious”
  res <-  mutate(res, res_severe = ifelse(((flw_breathless_now %in% 4 &
                                           (flw_eq5d_ua_2 >= 3 | (flw_fatigue >=7 & !is.na(flw_fatigue)) |
                                            phq_4_stress %in% c(3,4) |
                                            phq_4_worries %in% c(3,4))) |
                                           (flw_breathless_now %in% 5 &
                                           (flw_eq5d_ua_2 >= 3 | (flw_fatigue >=7 & !is.na(flw_fatigue))))) &
                                           (flw_breathless_now > flw_breathless_pre_c19),
                                          1,0))

  # Combined to the respiratory category
  res <- mutate(res, res_combine = ifelse((res_mild %in% 1 | res_moderate %in% 1 | res_severe %in% 1), 1, 0))
  message(paste("Average res: "), mean(res$res_combine))

  # Fatigue
  fatg <- mod12_fol[,c('subjid','per_fat','per_fat_dur','flw_recovered',
                       'eq5d_summary','eq5d_summary_2')]

  if (wave_number == "wave_1" & followup_time == "6_month"){
    fatg <- mutate(fatg, fatigue = ifelse(per_fat %in% 1 & per_fat_dur %in% c(5,7) & flw_recovered <= 3 &
                                            eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1,
                                          1,0))
  } else {
    fatg <- mutate(fatg, fatigue = ifelse(per_fat %in% 1 & per_fat_dur >= 4 & flw_recovered <= 3 &
                                            eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1,
                                          1,0))
  }
  message(paste("Average fatigue: "), mean(fatg$fatigue, na.rm=T))

  # Joint/muscle pain/tightness
  jm_pain <- mod12_fol[,c('subjid','mus_pain','flw_joint_pain','rig_mus',
                          'mus_pain_dur','flw_joint_pain_dur','rig_mus_dur',
                          'eq5d_summary','eq5d_summary_2','flw_recovered')]

  if (wave_number == "wave_1" & followup_time == "6_month"){
    jm_pain <- mutate(jm_pain, joint_mus_pain = ifelse((mus_pain %in% 1 & mus_pain_dur %in% c(5,7)) |
                                                         (flw_joint_pain %in% 1 & flw_joint_pain_dur %in% c(5,7)) |
                                                         (rig_mus %in% 1 & rig_mus_dur %in% c(5,7)) & flw_recovered <= 3 &
                                                         eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1,1,0))
  } else {
    jm_pain <- mutate(jm_pain, joint_mus_pain = ifelse((mus_pain %in% 1 & mus_pain_dur >= 4) |
                                                         (flw_joint_pain %in% 1 & flw_joint_pain_dur >= 4) |
                                                         (rig_mus %in% 1 & rig_mus_dur >= 4) & flw_recovered <= 3 &
                                                         eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1,1,0))
  }
  message(paste("Average joint/muscle pain/tightness: "), mean(jm_pain$joint_mus_pain, na.rm=T))

  # Breathless
  breath <- mod12_fol[,c('subjid','flw_breathless','flw_breathless_dur','flw_breathless_now','flw_breathless_pre_c19',
                         'eq5d_summary','eq5d_summary_2','flw_recovered')]

  if (wave_number == "wave_1" & followup_time == "6_month"){
    breath <- mutate(breath, breathless = ifelse(flw_breathless %in% 1 & flw_breathless_dur %in% c(5,7) & flw_recovered <= 3 &
                                                   ((eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1) |
                                                      flw_breathless_now > flw_breathless_pre_c19),
                                                 1,0))
  } else {
    breath <- mutate(breath, breathless = ifelse(flw_breathless %in% 1 & flw_breathless_dur >= 4 & flw_recovered <= 3 &
                                                   ((eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1) |
                                                       flw_breathless_now > flw_breathless_pre_c19),
                                                 1,0))
  }
  message(paste("Average breathless: "), mean(breath$breathless, na.rm=T))

  # Cough, chest pain, pain breathing
  cough_chest_br <- mod12_fol[,c('subjid','flw_pers_cough','flw_chest_pains','flw_pain_breathing',
                                 'flw_pers_cough_dur','flw_chest_pains_dur','flw_pain_breathing_dur',
                                 'eq5d_summary','eq5d_summary_2','flw_recovered')]

  if (wave_number == "wave_1" & followup_time == "6_month"){
    cough_chest_br <- mutate(cough_chest_br, cough_chest_breath_pain =
                               ifelse((flw_pers_cough %in% 1 & flw_pers_cough_dur %in% c(5,7)) |
                                      (flw_chest_pains %in% 1 & flw_chest_pains_dur %in% c(5,7)) |
                                      (flw_pain_breathing %in% 1 & flw_pain_breathing_dur %in% c(5,7)) & flw_recovered <= 3 &
                                       eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1,
                                      1,0))
  } else {
    cough_chest_br <- mutate(cough_chest_br, cough_chest_breath_pain =
                               ifelse((flw_pers_cough %in% 1 & flw_pers_cough_dur >= 4) |
                                        (flw_chest_pains %in% 1 & flw_chest_pains_dur >= 4) |
                                        (flw_pain_breathing %in% 1 & flw_pain_breathing_dur >= 4) & flw_recovered <= 3 &
                                        eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1,
                                      1,0))
  }
  message(paste("Average cough, chest pain, pain breathing: "), mean(cough_chest_br$cough_chest_breath_pain, na.rm=T))

  # Forgetfulness/lack of concentration/confusion
  forg_conf <- mod12_fol[,c('subjid','for_cov','flw_confusion','flw_remember_today','flw_remember_pre_c19','flw_recovered',
                            'for_cov_dur','flw_confusion_dur','eq5d_summary','eq5d_summary_2')]

  if (wave_number == "wave_1" & followup_time == "6_month"){
    forg_conf <- mutate(forg_conf, forget_confused = ifelse((for_cov %in% 1 & for_cov_dur %in% c(5,7)) |
                                                            (flw_confusion %in% 1 & flw_confusion_dur %in% c(5,7)) & flw_recovered <= 3 &
                                                            ((eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1) |
                                                              flw_remember_today > flw_remember_pre_c19),
                                                            1,0))
  } else {
    forg_conf <- mutate(forg_conf, forget_confused = ifelse((for_cov %in% 1 & for_cov_dur >= 4) |
                                                              (flw_confusion %in% 1 & flw_confusion_dur >= 4) & flw_recovered <= 3 &
                                                              ((eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1) |
                                                                 flw_remember_today > flw_remember_pre_c19),
                                                            1,0))
  }
  message(paste("Average forgetfulness/lack of concentration/confusion: "), mean(forg_conf$forget_confused, na.rm=T))

  # Anxiety/depression
  anx_depres <- mod12_fol[,c('subjid','anx_cov','depr_mood','flw_recovered','anx_cov_dur','depr_mood_dur',
                             'flw_eq5d_ad','flw_eq5d_ad_2','eq5d_summary','eq5d_summary_2')]

  if (wave_number == "wave_1" & followup_time == "6_month"){
    anx_depres <- mutate(anx_depres, anxiety_depressed = ifelse((anx_cov %in% 1 & anx_cov_dur %in% c(5,7)) |
                                                                (depr_mood %in% 1 & depr_mood_dur %in% c(5,7)) & flw_recovered <= 3 &
                                                                  ((eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1) |
                                                                     flw_eq5d_ad_2 > flw_eq5d_ad),
                                                                1,0))
  } else {
    anx_depres <- mutate(anx_depres, anxiety_depressed = ifelse((anx_cov %in% 1 & anx_cov_dur >= 4) |
                                                                  (depr_mood %in% 1 & depr_mood_dur >= 4) & flw_recovered <= 3 &
                                                                  ((eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1) |
                                                                     flw_eq5d_ad_2 > flw_eq5d_ad),
                                                                1,0))
  }
  message(paste("Average anxiety/depression: "), mean(anx_depres$anxiety_depressed, na.rm=T))

  # Loss of taste/smell
  ts_loss <- mod12_fol[,c('subjid','flw_loss_smell','flw_loss_taste','flw_recovered','flw_loss_smell_dur','flw_loss_taste_dur',
                          'eq5d_summary','eq5d_summary_2')]

  if (wave_number == "wave_1" & followup_time == "6_month"){
    ts_loss <- mutate(ts_loss, taste_smell_loss = ifelse((flw_loss_smell %in% 1 & flw_loss_smell_dur %in% c(5,7)) |
                                                         (flw_loss_taste %in% 1 & flw_loss_taste_dur %in% c(5,7)) & flw_recovered <=3 &
                                                           eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1, 1,0))
  } else {
    ts_loss <- mutate(ts_loss, taste_smell_loss = ifelse((flw_loss_smell %in% 1 & flw_loss_smell_dur >= 4) |
                                                         (flw_loss_taste %in% 1 & flw_loss_taste_dur >= 4) & flw_recovered <=3 &
                                                           eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1, 1,0))
  }
  message(paste("Average loss of taste/smell: "), mean(ts_loss$taste_smell_loss, na.rm=T))

  # Merge all clusters
  a1 <- merge(age_mod12, post[,c('subjid','post_acute')])
  a2 <- merge(a1, res[, c('subjid', 'res_mild',"res_moderate","res_severe", "res_combine")])
  a3 <- merge(a2, cog[, c('subjid','cog_mild','cog_moderate','cognitive')])

  a3 <- merge(a3, fatg[, c('subjid','fatigue')], all.x=T)
  a3 <- merge(a3, jm_pain[, c('subjid','joint_mus_pain')], all.x=T)
  a3 <- merge(a3, breath[, c('subjid','breathless')], all.x=T)
  a3 <- merge(a3, cough_chest_br[, c('subjid','cough_chest_breath_pain')], all.x=T)
  a3 <- merge(a3, forg_conf[, c('subjid','forget_confused')], all.x=T)
  a3 <- merge(a3, anx_depres[, c('subjid','anxiety_depressed')], all.x=T)
  a3 <- merge(a3, ts_loss[, c('subjid','taste_smell_loss')], all.x=T)

  # Combine the resp, cognitive and fatigue categories
  a3 <- mutate(a3, Cog_Res = ifelse((cognitive %in% 1 & res_combine %in% 1), 1, 0 ))
  a3 <- mutate(a3, Cog_Fat = ifelse((cognitive %in% 1 & post_acute %in% 1), 1, 0 ))
  a3 <- mutate(a3, Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1), 1, 0 ))
  a3 <- mutate(a3, Cog_Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1
                                         & cognitive %in% 1), 1, 0 ))
  # All long term category
  a3 <- mutate(a3, long_term = ifelse((cognitive %in% 1 | res_combine %in% 1 | post_acute %in% 1), 1, 0 ))
  message(paste("Average long-term: "), mean(a3$long_term))

  # If already in long-term group, exclude from new clusters
  a3 <- mutate(a3, fatigue_residual = ifelse(long_term %in% 1 & fatigue %in% 1, 0, fatigue))
  a3 <- mutate(a3, joint_mus_pain_residual = ifelse(long_term %in% 1 & joint_mus_pain %in% 1, 0, joint_mus_pain))
  a3 <- mutate(a3, breathless_residual = ifelse(long_term %in% 1 & breathless %in% 1, 0, breathless))
  a3 <- mutate(a3, cough_chest_breath_pain_residual = ifelse(long_term %in% 1 & cough_chest_breath_pain %in% 1, 0, cough_chest_breath_pain))
  a3 <- mutate(a3, forget_confused_residual = ifelse(long_term %in% 1 & forget_confused %in% 1, 0, forget_confused))
  a3 <- mutate(a3, taste_smell_loss_residual = ifelse(long_term %in% 1 & taste_smell_loss %in% 1, 0, taste_smell_loss))
  a3 <- mutate(a3, anxiety_depressed_residual = ifelse(long_term %in% 1 & anxiety_depressed %in% 1, 0, anxiety_depressed))

  # Long term with new clusters (exclude long term with old clusters)
  a3 <- mutate(a3, long_term_extended = ifelse(long_term %in% 0 & (fatigue %in% 1 | joint_mus_pain %in% 1 | taste_smell_loss %in% 1 |
                                               breathless %in% 1 | cough_chest_breath_pain %in% 1 |
                                               forget_confused %in% 1 | anxiety_depressed %in% 1), 1, 0))
  message(paste("Average long_term_extended: "), mean(a3$long_term_extended))

  # Long term with both old and new clusters
  a3 <- mutate(a3, long_term_all = ifelse(long_term %in% 1 | long_term_extended %in% 1, 1, 0))
  message(paste("Average long_term_all: "), mean(a3$long_term_all))

  a3$overlap <- rowSums(a3[,c("res_mild","res_moderate","res_severe")])

  DT = as.data.table(a3)
  DT[(overlap >1 & res_mild == res_moderate), res_mild := 0]
  DT[(overlap >1 & res_moderate == res_severe), res_moderate := 0]
  DT$follow_up <- followup_time
  DT[is.na(DT)] <- 0
  DT <- unique(DT)
  DT <- merge(DT, mod12_fol[,c("subjid",'eq5d_summary','eq5d_summary_2')], by = "subjid", all.x=T)

  # Merge survey responses into output
  DT2 <- merge(DT, followup, by = "subjid", all.x=T)

  # List of desired columns in specific order
  desired_columns <- c('subjid','Sex','Age','age_group_name','age_group_id',
                       'post_acute', 'cog_mild', 'cog_moderate', 'cognitive', 'res_mild', 'res_moderate', 'res_severe', 'res_combine','overlap',
                       'Cog_Fat', 'Cog_Res', 'Res_Fat', 'Cog_Res_Fat', 'anxiety_depressed','anxiety_depressed_residual',
                       'breathless','breathless_residual', 'cough_chest_breath_pain','cough_chest_breath_pain_residual',
                       'fatigue','fatigue_residual', 'forget_confused','forget_confused_residual', 'joint_mus_pain','joint_mus_pain_residual',
                       'taste_smell_loss','taste_smell_loss_residual','long_term','long_term_extended','long_term_all',
                       'eq5d_summary','eq5d_summary_2','follow_up', followup_cols)

  # Filter the list of desired columns based on what actually exists in the dataframe
  existing_columns <- desired_columns[desired_columns %in% colnames(DT)]
  existing_columns2 <- desired_columns[desired_columns %in% colnames(DT2)]

  # Subset the dataframe to only include existing columns in the desired order
  DT <- DT[, ..existing_columns]
  DT2 <- DT2[, ..existing_columns2]

  openxlsx::write.xlsx(DT, paste0(outpath ,wave_number, "_",followup_time,"_",typeData,"_all_data_marked_v_", version, ".xlsx"), rowNames=F)
  openxlsx::write.xlsx(DT2, paste0(outpath ,wave_number, "_",followup_time,"_",typeData,"_all_responses_v_", version, ".xlsx"), rowNames=F)

  # Calculate frequencies based on symptoms, duration, recovery
  bin_vars <- wide_counts2$Variable
  bin_vars_dur <- sub("(_\\d+)?$", "_dur\\1", bin_vars)
  bin_vars_longCovid <- paste0(bin_vars, "_longCovid")
  bin_vars_longCovid_noDur <- paste0(bin_vars, "_longCovid_noDur")
  bin_vars_longCovid_residual <- paste0(bin_vars, "_longCovid_residual")
  bin_vars_longCovid_noDur_residual <- paste0(bin_vars, "_longCovid_noDur_residual")

  df_all <- mod12_fol[,c('subjid','flw_recovered','eq5d_summary','eq5d_summary_2',bin_vars,bin_vars_dur)]
  DT <- as.data.frame(DT)
  df_all <- merge(df_all, DT[,c('subjid','long_term', 'long_term_extended','long_term_all','follow_up')], by='subjid')

  for (var in bin_vars) {
    var_dur <- sub("(_\\d+)?$", "_dur\\1", var)
    var_longCovid <- paste0(var, "_longCovid")
    var_longCovid_noDur <- paste0(var, "_longCovid_noDur")
    var_longCovid_residual <- paste0(var, "_longCovid_residual")
    var_longCovid_noDur_residual <- paste0(var, "_longCovid_noDur_residual")

    if (wave_number == "wave_1" & followup_time == "6_month"){
      df_all <- df_all %>%
        mutate(
          !!var_longCovid := ifelse(
            .data[[var]] == 1 & .data[[var_dur]] %in% c(5, 7) & flw_recovered <= 3 &
              eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1, 1, 0),
        )
    } else {
      df_all <- df_all %>%
        mutate(
          !!var_longCovid := ifelse(
            .data[[var]] == 1 & .data[[var_dur]] >= 4 & flw_recovered <= 3 &
              eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1, 1, 0),
        )
    }

    df_all <- df_all %>%
      mutate(
        !!var_longCovid_noDur := ifelse(
          .data[[var]] == 1 & flw_recovered <= 3 &
            eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1, 1, 0),
        !!var_longCovid_residual := ifelse(
          long_term == 1, 0, .data[[var_longCovid]]),
        !!var_longCovid_noDur_residual := ifelse(
          long_term == 1, 0, .data[[var_longCovid_noDur]])
      )
  }

  # Use rowwise() to operate across each row individually
  df_all <- df_all %>%
    rowwise() %>%
    # If any symptom == 1, return 1, or else return 0
    mutate(any_symptoms = min(1,max(c_across(all_of(bin_vars)),0, na.rm=T))) %>%
    mutate(any_longCovid = min(1,max(c_across(matches("_longCovid$")),0, na.rm=T))) %>%
    mutate(any_longCovid_noDur = min(1,max(c_across(matches("_longCovid_noDur$")),0, na.rm=T))) %>%
    mutate(any_longCovid_residual = min(1,max(c_across(matches("_longCovid_residual$")),0, na.rm=T))) %>%
    mutate(any_longCovid_noDur_residual = min(1,max(c_across(matches("_longCovid_noDur_residual$")),0, na.rm=T))) %>%
    ungroup()

  openxlsx::write.xlsx(df_all, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_all_symptoms_v_",version, ".xlsx"),
                       rowNames=F)

  # Export results for long covid residuals
  df_all_resi <- df_all[df_all$any_longCovid == 1, grep("subjid|long_term$|_longCovid$|_longCovid_residual$", colnames(df_all), value = TRUE)]
  df_noDur_resi <- df_all[df_all$any_longCovid_noDur == 1, grep("subjid|long_term$|_noDur$|_noDur_residual$", colnames(df_all), value = TRUE)]
  openxlsx::write.xlsx(df_all_resi, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_longCovid_symptoms_v_",version, ".xlsx"),
                       rowNames=F)
  openxlsx::write.xlsx(df_noDur_resi, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_longCovid_noDur_symptoms_v_",version, ".xlsx"),
                       rowNames=F)

  # Calculate total patients with long Covid
  df_all_lc <- df_all %>% select(bin_vars_longCovid)
  df_all_lc <- data.frame(colSums(df_all_lc, na.rm = TRUE))
  colnames(df_all_lc) <- "yes_longCovid"
  df_all_lc$Variable_longCovid <- rownames(df_all_lc)
  rownames(df_all_lc) <- c(1:nrow(df_all_lc))
  df_all_lc <- df_all_lc %>%
    mutate(Variable = str_replace(Variable_longCovid, "_longCovid", ""))

  # Calculate total patients with long Covid, not captured in original clusters
  df_all_lc_residual <- df_all %>% select(bin_vars_longCovid_residual)
  df_all_lc_residual <- data.frame(colSums(df_all_lc_residual, na.rm = TRUE))
  colnames(df_all_lc_residual) <- "yes_longCovid_residual"
  df_all_lc_residual$Variable_longCovid_residual <- rownames(df_all_lc_residual)
  rownames(df_all_lc_residual) <- c(1:nrow(df_all_lc_residual))
  df_all_lc_residual <- df_all_lc_residual %>%
    mutate(Variable = str_replace(Variable_longCovid_residual, "_longCovid_residual", ""))

  # Calculate total patients with long Covid w/out duration criteria
  df_all_noDur <- df_all %>% select(bin_vars_longCovid_noDur)
  df_all_noDur <- data.frame(colSums(df_all_noDur, na.rm = TRUE))
  colnames(df_all_noDur) <- "yes_longCovid_noDur"
  df_all_noDur$Variable_longCovid_noDur <- rownames(df_all_noDur)
  rownames(df_all_noDur) <- c(1:nrow(df_all_noDur))
  df_all_noDur <- df_all_noDur %>%
    mutate(Variable = str_replace(Variable_longCovid_noDur, "_longCovid_noDur", ""))

  # Calculate total patients with long Covid w/out duration criteria, not captured in original clusters
  df_all_noDur_residual <- df_all %>% select(bin_vars_longCovid_noDur_residual)
  df_all_noDur_residual <- data.frame(colSums(df_all_noDur_residual, na.rm = TRUE))
  colnames(df_all_noDur_residual) <- "yes_longCovid_noDur_residual"
  df_all_noDur_residual$Variable_longCovid_noDur_residual <- rownames(df_all_noDur_residual)
  rownames(df_all_noDur_residual) <- c(1:nrow(df_all_noDur_residual))
  df_all_noDur_residual <- df_all_noDur_residual %>%
    mutate(Variable = str_replace(Variable_longCovid_noDur_residual, "_longCovid_noDur_residual", ""))

  # Calculate "any of the above" row
  any_cols <- c("any_symptoms","any_longCovid","any_longCovid_noDur","any_longCovid_residual","any_longCovid_noDur_residual")
  df_all_any <- df_all %>% select(all_of(any_cols))
  df_all_any <- data.frame(t(colSums(df_all_any, na.rm = TRUE)))
  df_all_any$No <- NA
  df_all_any$Unknown <- NA
  df_all_any$Variable <- "any_of_the_symptoms"
  setnames(df_all_any, "any_symptoms", "Yes")
  setnames(df_all_any, "any_longCovid", "yes_longCovid")
  setnames(df_all_any, "any_longCovid_noDur", "yes_longCovid_noDur")
  setnames(df_all_any, "any_longCovid_residual", "yes_longCovid_residual")
  setnames(df_all_any, "any_longCovid_noDur_residual", "yes_longCovid_noDur_residual")

  # Merge stats
  df_all_lc <- merge(df_all_lc, df_all_lc_residual, by="Variable")
  df_all_noDur <- merge(df_all_noDur, df_all_noDur_residual, by="Variable")
  df_both <- merge(df_all_lc, df_all_noDur, by="Variable")
  df_both <- merge(wide_counts2, df_both, by="Variable")
  df_both$Variable_longCovid <- NULL
  df_both$Variable_longCovid_residual <- NULL
  df_both$Variable_longCovid_noDur <- NULL
  df_both$Variable_longCovid_noDur_residual <- NULL
  df_both <- rbind(df_both, df_all_any)

  openxlsx::write.xlsx(df_both, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_grouped_symptoms_v_",version, ".xlsx"), rowNames=F)

  # Create intermediary table
  inter <- merge(ages, DT)[,!c('subjid','Age','overlap')]
  # print(colnames(inter))

  # Tabulate
  data_long <- gather(inter , measure, value, c(post_acute:long_term_all))

  # Table by sex and age
  cocurr_mu <- data_long %>%
    group_by(age_group_id,age_group_name, Sex, measure) %>%
    filter(value=='1') %>%
    dplyr::summarise(var=n()) %>%
    spread(measure, var)

  cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
  cocurr_mu[is.na(cocurr_mu)] <- 0
  cocurr_mu$follow_up <- followup_time
  cocurr_mu <- cocurr_mu %>% arrange(age_group_id, Sex)

  openxlsx::write.xlsx(cocurr_mu, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_byAgeSex_",version, ".xlsx"), rowNames=F)

  # Table by sex only
  cocurr_sex <- data_long %>%
    group_by(Sex,measure) %>%
    filter(value=='1') %>%
    dplyr::summarise(var=n()) %>%
    spread(measure, var)

  cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
  cocurr_sex[is.na(cocurr_sex)] <- 0
  cocurr_sex$follow_up <- followup_time

  # List of desired columns in specific order
  desired_columns <- c('Sex', 'post_acute', 'cog_mild', 'cog_moderate', 'cognitive', 'res_mild', 'res_moderate', 'res_severe', 'res_combine',
                       'Cog_Fat', 'Cog_Res', 'Res_Fat', 'Cog_Res_Fat', 'anxiety_depressed','anxiety_depressed_residual',
                       'breathless','breathless_residual', 'cough_chest_breath_pain','cough_chest_breath_pain_residual',
                       'fatigue','fatigue_residual', 'forget_confused','forget_confused_residual', 'joint_mus_pain','joint_mus_pain_residual',
                       'taste_smell_loss','taste_smell_loss_residual','long_term','long_term_extended','long_term_all','N','follow_up')

  # Filter the list of desired columns based on what actually exists in the dataframe
  existing_columns <- desired_columns[desired_columns %in% colnames(cocurr_sex)]

  # Subset the dataframe to only include existing columns in the desired order
  cocurr_sex <- cocurr_sex[, existing_columns]
  openxlsx::write.xlsx(cocurr_sex, paste0(outpath ,wave_number,"_",followup_time,"_",typeData, "_bySex_v_", version, ".xlsx"),rowNames=F)
}


# Follow up time, 6-8 month, adult, wave 1 ---------------------------------------------
path1 <-'FILEPATH/Part-1-PCR-Upd-5060 copy.xlsx'

path2 <- 'FILEPATH/6-8 months Follow-up.xlsx'

wave_number <- "wave_1"
followup_time <-"6_month"

outpath<-"FILEPATH"

tabulation(path1, path2, wave_number, outpath, followup_time, typeData ="_" )

# Follow up time, 12_month, adult, wave 1, initial -------------------------------------

path1 <-'FILEPATH/Part-1-PCR-Upd-5060 copy.xlsx'

path2 <-'FILEPATH/Initial_12 months Follow-up.xlsx'

wave_number <- "wave_1"
followup_time <-"12_month"

outpath<-"FILEPATH"

tabulation(path1, path2, wave_number, outpath, followup_time, typeData ="Initial")

# Follow up time, 6 month, adult, wave 2 -----------------------------------------------
path1 <-'FILEPATH/Part1_2 wave U07.1.xlsx'

path2 <- 'FILEPATH/Adults Follow-up 6-8 months_2 wave.xlsx'

wave_number <- "wave_2"
followup_time <-"6_month"

outpath <-"FILEPATH"

tabulation(path1, path2, wave_number, outpath, followup_time, typeData="_")

################################################################################
################################################################################

# ongoing is missing some key variables, like the comparison variables.
# ongoing is not included in the tabulation.
# ongoing 12 months
# wave 1
# the ongoing dataset have many variables with different names

# module 1
# Read in the patient info
mod1 <- read_excel('FILEPATH/Part-1-PCR-Upd-5060 copy.xlsx')[,c("PIN","Age","Sex","COVID PCR")]
# Read in the follow up data
# comor <- read_excel('FILEPATH/Adults Follow-up 6-8 months_2 wave.xlsx')

mod1 <- mod1[mod1$`COVID PCR` %in% 1,]

# Creating age group by GBD
ages <- get_age_metadata(age_group_set_id = 19,
                         gbd_round_id = roots$gbd_round)[, !c('most_detailed')][,c('age_group_id','age_group_name')]


mod12 <- mutate(mod1, age_group_name = ifelse(Age %in% 15:19, "15 to 19",
                                       ifelse(Age %in% 20:24, "20 to 24",
                                       ifelse(Age %in% 25:29, "25 to 29",
                                       ifelse(Age %in% 30:34, "30 to 34",
                                       ifelse(Age %in% 35:39, "35 to 39",
                                       ifelse(Age %in% 40:44, "40 to 44",
                                       ifelse(Age %in% 45:49, "45 to 49",
                                       ifelse(Age %in% 50:54, "50 to 54",
                                       ifelse(Age %in% 55:59, "55 to 59",
                                       ifelse(Age %in% 60:64, "60 to 64",
                                       ifelse(Age %in% 65:69, "65 to 69",
                                       ifelse(Age %in% 70:74, "70 to 74",
                                       ifelse(Age %in% 75:79, "75 to 79",
                                       ifelse(Age %in% 80:84, "80 to 84",
                                       ifelse(Age %in% 85:89, "85 to 89",
                                       ifelse(Age %in% 90:94, "90 to 94", "95_plus")))))))))))))))))

age_mod12 <- merge(mod12, ages, by='age_group_name')

# warning messages due to "NR" missing values
followup_12m_orig <- read_excel('FILEPATH/Ongoing_ 12 months Follow_up.xlsx')

followup_12m <- followup_12m_orig[,c("subjid",'flw_loss_taste_e68fb0','flw_loss_taste_dur_872d2b','flw_loss_smell_dafb95','flw_loss_smell_dur_24a984',"flw_confusion_0006ff",'flw_confusion_dur_cf35cf','flw_breathless_c67847','flw_breathless_dur_a45bb9','flw_breathless_now_6d83c8','flw_limb_weakness_3b26c2','flw_limb_weakness_dur_ace39e','flw_fatigue_e07109','flw2_for_cov','flw2_for_cov_dur','flw_remember_today_3a83ec','flw_pers_cough_bb302b','flw_pers_cough_dur_18465f','flw2_anx_cov','flw2_anx_cov_dur','flw2_depr_mood','flw2_depr_mood_dur','flw_recovered_3f1467','mus_pain_5bc80c','mus_pain_dur_88e06d','flw_joint_pain_0bef41','flw_joint_pain_dur_343be1','flw2_rig_mus','flw2_rig_mus_dur','flw_chest_pains_c6d16d','flw_chest_pains_dur_3c225f','flw_pain_breathing_6e1716','flw_pain_breathing_dur_61e900','flw_eq5d5l_vas_86a73f','flw_eq5d_mb_8fc5d6','flw_eq5d_sc_443d45',"flw_eq5d_ad_e29260",'flw_eq5d_ua_dd0e43','flw_eq5d_pd_2_eafcd1')]

# The column names ending with "_" followed by 6 alphanumeric characters are renamed
colnames(followup_12m_orig) <- gsub("_[0-9a-zA-Z]{6}$", "", colnames(followup_12m_orig))

# Find any duplicated columns and remove them
sorted_col_counts <- sort(table(colnames(followup_12m_orig)), decreasing = TRUE)
dup_names <- names(sorted_col_counts[sorted_col_counts > 1])
followup_12m_orig <- followup_12m_orig[, !colnames(followup_12m_orig) %in% dup_names]

# Remove columns that aren't symptoms we're interested in
followup2 <- followup_12m_orig[, !grepl("_dur|_date|weight|height|vas|house|employ|survey|_up1|_vaccine|_readm|_icu|cough_y|_ethinicity|_bod_|_diag|_fever|thyr|func_|ant_cov|_no$|_other_symp|__[0-9]+$", colnames(followup_12m_orig)) | colnames(followup_12m_orig) == "flw_weight_loss"]
setnames(followup2, "flw_eq5d_ad", "flw_eq5d_ad_2")
setnames(followup2, "flw_eq5d_mb", "flw_eq5d_mb_2")
setnames(followup2, "flw_eq5d_sc", "flw_eq5d_sc_2")
setnames(followup2, "flw_eq5d_ua", "flw_eq5d_ua_2")

# Select duration variables
followup3 <- followup_12m_orig[, grepl("_dur", colnames(followup_12m_orig))]

# Remove columns that contain only NA values
followup2 <- followup2 %>% select_if(~any(!is.na(.)) & is.numeric(.))
followup3 <- followup3 %>% select_if(~any(!is.na(.)) & is.numeric(.))

comb_counts <- function(followup) {
  # Initialize an empty data frame to store the combined counts
  combined_counts <- data.frame()
  followup_cols <- setdiff(names(followup), "subjid")

  # Loop through each column and calculate the count of unique values
  for (col in followup_cols) {

    # Create a table for the unique values
    counts <- table(followup[[col]])

    # Convert the table to a data frame
    counts_df <- as.data.frame(counts)

    # Rename the columns for merging
    colnames(counts_df) <- c("Value", "Count")

    # Add a column with the name of the current column being processed
    counts_df$Variable <- col

    # Bind the current counts data frame to the combined counts data frame
    combined_counts <- rbind(combined_counts, counts_df)
  }

  # Pivot the dataframe to a wider format
  wide_counts <- pivot_wider(combined_counts, names_from = Value, values_from = Count, values_fill = list(Count = 0))

  # Reorder some columns
  if (0 %in% colnames(wide_counts)){
    wide_counts <- wide_counts[c("Variable", "0", setdiff(names(wide_counts), c("Variable", "0")))]

    # Add a new column that is the sum of columns 4 to 10 for each row
    wide_counts <- wide_counts %>% mutate(sum_row = rowSums(select(., -c("Variable","0","1","2","3")), na.rm = TRUE))
    wide_counts <- wide_counts %>% mutate(sum_unknown = rowSums(select(., c("2","3")), na.rm = TRUE))

    # Subset to yes/no/unknown variables
    wide_counts$binary <- ifelse(wide_counts$sum_row == 0, 1, 0)
    wide_counts <- wide_counts[wide_counts$binary == 1,]
    wide_counts <- wide_counts[,c("Variable","0","1","sum_unknown")]
    colnames(wide_counts) <- c("Variable","No","Yes","Unknown")
  }

  return(as.data.frame(wide_counts))
}

wide_counts2 <- comb_counts(followup2)
wide_counts3 <- comb_counts(followup3)

# Verify if duration variable exists for each symptom
bin_vars <- sub("(_\\d+)?$", "_dur\\1", wide_counts2$Variable)
bin_vars_dur <- wide_counts3$Variable
bin_vars_select <- bin_vars_dur[bin_vars_dur %in% bin_vars]
bin_vars_select <- str_replace(bin_vars_select, "_dur", "")

# Select only symptoms that have duration
wide_counts2 <- wide_counts2[wide_counts2$Variable %in% bin_vars_select,]

# using the 6-8 months as the pre, read in the 6-8 months
followup_6m <- read_excel('FILEPATH/6-8 months Follow-up.xlsx')[,c("subjid",'per_fat','per_fat_dur','flw_breathless_pre_c19',"flw_remember_pre_c19",'flw_eq5d_ua',"flw_eq5d_ad","flw_eq5d_pd_2",'flw_eq5d5l_vas_2','flw_eq5d_mb','flw_eq5d_sc')] #'for_cov"

# in the dataset for_cov, flw2_for_cov in the 12 month dataset
# getting per fat, the flow_eq5d_ua and flw_eq5d_ad, remember from the 6-8 months followup data
# per fat is totally missing in the 12 month ongoing dataset use per fat from 6-8 months instead, because per_fat is binary.

# Read in symptom cluster assignments at 6 months
outpath <- "FILEPATH"
clusters_6m <- read_excel(paste0(outpath , "wave_1_6_month___all_data_marked_v_", version, ".xlsx"))
clusters_6m_all <- read_excel(paste0(outpath , "wave_1_6_month___all_symptoms_v_", version, ".xlsx"))

# Select only symptoms that have data available at 6m follow-up
bin_vars_select2 <- colnames(clusters_6m_all)[colnames(clusters_6m_all) %like% "_longCovid$"]
bin_vars_select2 <- str_replace(bin_vars_select2, "_longCovid$", "")
wide_counts2 <- wide_counts2[wide_counts2$Variable %in% bin_vars_select2,]

# for some reason, in the raw followup data, flw_eq5d_pd contains the responses at follow-up, and flw_eq5d_pd_2 contains the
# responses pre-COVID. Switching them in the code now so the algorithm is logical later
setnames(followup_6m, "flw_eq5d_pd_2", "flw_eq5d_pd")
setnames(followup_6m, "flw_eq5d5l_vas_2", "flw_eq5d5l_vas")

# Merge pre & post data
follow_up <- merge(followup_12m, followup_6m)
names(age_mod12)[names(age_mod12) == "PIN"] <- "subjid"
mod12_fol <- merge(age_mod12, follow_up)

# Getting the number of patients by age and by sex
sample_n <-mod12_fol %>%
  group_by(age_group_id,age_group_name,Sex ) %>%
  dplyr::summarise(N=n())

# Getting the number of patients by sex
sample_n_bysex <-mod12_fol %>%
  group_by(Sex) %>%
  dplyr::summarise(N=n())

# Rename some columns
names(mod12_fol)[names(mod12_fol) == "flw_loss_taste_e68fb0"] <- "flw_loss_taste"
names(mod12_fol)[names(mod12_fol) == "flw_loss_taste_dur_872d2b"] <- "flw_loss_taste_dur"
names(mod12_fol)[names(mod12_fol) == "flw_loss_smell_dafb95"] <- "flw_loss_smell"
names(mod12_fol)[names(mod12_fol) == "flw_loss_smell_dur_24a984"] <- "flw_loss_smell_dur"
names(mod12_fol)[names(mod12_fol) == "flw_confusion_0006ff"] <- "flw_confusion"
names(mod12_fol)[names(mod12_fol) == "flw_confusion_dur_cf35cf"] <- "flw_confusion_dur"
names(mod12_fol)[names(mod12_fol) == "flw_breathless_c67847"] <- "flw_breathless"
names(mod12_fol)[names(mod12_fol) == "flw_breathless_dur_a45bb9"] <- "flw_breathless_dur"
names(mod12_fol)[names(mod12_fol) == "flw_breathless_now_6d83c8"] <- "flw_breathless_now"
names(mod12_fol)[names(mod12_fol) == "flw_limb_weakness_3b26c2"] <- "flw_limb_weakness"
names(mod12_fol)[names(mod12_fol) == "flw_limb_weakness_dur_ace39e"] <- "flw_limb_weakness_dur"
names(mod12_fol)[names(mod12_fol) == "flw_fatigue_e07109"] <- "flw_fatigue"
names(mod12_fol)[names(mod12_fol) == "flw2_for_cov"] <- "for_cov"
names(mod12_fol)[names(mod12_fol) == "flw2_for_cov_dur"] <- "for_cov_dur"
names(mod12_fol)[names(mod12_fol) == "flw_pers_cough_bb302b"] <- "flw_pers_cough"
names(mod12_fol)[names(mod12_fol) == "flw_pers_cough_dur_18465f"] <- "flw_pers_cough_dur"
names(mod12_fol)[names(mod12_fol) == "flw_remember_today_3a83ec"] <- "flw_remember_today"
names(mod12_fol)[names(mod12_fol) == "flw2_anx_cov"] <- "anx_cov"
names(mod12_fol)[names(mod12_fol) == "flw2_anx_cov_dur"] <- "anx_cov_dur"
names(mod12_fol)[names(mod12_fol) == "flw2_depr_mood"] <- "depr_mood"
names(mod12_fol)[names(mod12_fol) == "flw2_depr_mood_dur"] <- "depr_mood_dur"
names(mod12_fol)[names(mod12_fol) == "flw_recovered_3f1467"] <- "flw_recovered"
names(mod12_fol)[names(mod12_fol) == "mus_pain_5bc80c"] <- "mus_pain"
names(mod12_fol)[names(mod12_fol) == "mus_pain_dur_88e06d"] <- "mus_pain_dur"
names(mod12_fol)[names(mod12_fol) == "flw_joint_pain_0bef41"] <- "flw_joint_pain"
names(mod12_fol)[names(mod12_fol) == "flw_joint_pain_dur_343be1"] <- "flw_joint_pain_dur"
names(mod12_fol)[names(mod12_fol) == "flw2_rig_mus"] <- "rig_mus"
names(mod12_fol)[names(mod12_fol) == "flw2_rig_mus_dur"] <- "rig_mus_dur"
names(mod12_fol)[names(mod12_fol) == "flw_chest_pains_c6d16d"] <- "flw_chest_pains"
names(mod12_fol)[names(mod12_fol) == "flw_chest_pains_dur_3c225f"] <- "flw_chest_pains_dur"
names(mod12_fol)[names(mod12_fol) == "flw_pain_breathing_6e1716"] <- "flw_pain_breathing"
names(mod12_fol)[names(mod12_fol) == "flw_pain_breathing_dur_61e900"] <- "flw_pain_breathing_dur"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_ua_dd0e43"] <- "flw_eq5d_ua_2"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_ad_e29260"] <- "flw_eq5d_ad_2"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_pd_2_eafcd1"] <- "flw_eq5d_pd_2"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_mb_8fc5d6"] <- "flw_eq5d_mb_2"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_sc_443d45"] <- "flw_eq5d_sc_2"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d5l_vas_86a73f"] <- "flw_eq5d5l_vas_2"

# Calculate eq5d summary score pre Covid using Dutch Tariff method
mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','mb_score')], by.x = 'flw_eq5d_mb', by.y = 'levels', all.x=T)
mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','sc_score')], by.x = 'flw_eq5d_sc', by.y = 'levels', all.x=T)
mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','ad_score')], by.x = 'flw_eq5d_ad', by.y = 'levels', all.x=T)
mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','pd_score')], by.x = 'flw_eq5d_pd', by.y = 'levels', all.x=T)
mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','ua_score')], by.x = 'flw_eq5d_ua', by.y = 'levels', all.x=T)

mod12_fol$eq5d_summary <-
  eq5d_constant + mod12_fol$mb_score + mod12_fol$sc_score + mod12_fol$ad_score + mod12_fol$pd_score + mod12_fol$ua_score

# Remove the existing '_score' columns
mod12_fol <- select(mod12_fol, -c(mb_score,sc_score,ad_score,pd_score,ua_score))

# Calculate eq5d summary score post Covid using Dutch Tariff method
mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','mb_score')], by.x = 'flw_eq5d_mb_2', by.y = 'levels', all.x=T)
mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','sc_score')], by.x = 'flw_eq5d_sc_2', by.y = 'levels', all.x=T)
mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','ad_score')], by.x = 'flw_eq5d_ad_2', by.y = 'levels', all.x=T)
mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','pd_score')], by.x = 'flw_eq5d_pd_2', by.y = 'levels', all.x=T)
mod12_fol <- merge(mod12_fol, eq5d_summary[,c('levels','ua_score')], by.x = 'flw_eq5d_ua_2', by.y = 'levels', all.x=T)

mod12_fol$eq5d_summary_2 <-
  eq5d_constant + mod12_fol$mb_score + mod12_fol$sc_score + mod12_fol$ad_score + mod12_fol$pd_score + mod12_fol$ua_score

# Remove the existing '_score' columns
mod12_fol <- select(mod12_fol, -c(mb_score,sc_score,ad_score,pd_score,ua_score))

# Add a new column that checks if eq5d pair have non-null values
mod12_fol$eq5d_pair <- apply(mod12_fol, 1, function(x) {
  valid_mb = !is.na(x['flw_eq5d_mb']) & !is.na(x['flw_eq5d_mb_2'])
  valid_sc = !is.na(x['flw_eq5d_sc']) & !is.na(x['flw_eq5d_sc_2'])
  valid_ad = !is.na(x['flw_eq5d_ad']) & !is.na(x['flw_eq5d_ad_2'])
  valid_pd = !is.na(x['flw_eq5d_pd']) & !is.na(x['flw_eq5d_pd_2'])
  valid_ua = !is.na(x['flw_eq5d_ua']) & !is.na(x['flw_eq5d_ua_2'])

  # Sum the valid flags, and check if the sum is greater than or equal to 3
  sum_valid = sum(valid_mb, valid_sc, valid_ad, valid_pd, valid_ua)
  return(sum_valid >= 1) # Return TRUE if 1 or more pairs are valid
})


# Post-acute consequences of infectious disease (fatigue, emotional stability, insomnia)
post <- mod12_fol[,c("subjid","flw_limb_weakness",'per_fat','flw_fatigue',
                     'flw_eq5d_ua', 'flw_eq5d_ua_2', 'flw_eq5d_pd_2', 'flw_eq5d_pd',"flw_eq5d_ad","flw_eq5d_ad_2")]

post <- mutate(post, post_acute = ifelse((per_fat %in% 1 | flw_limb_weakness %in% 1) &
                                           (flw_fatigue >=3 | is.na(flw_fatigue)) &
                                           (flw_eq5d_ad_2 > 2 | flw_eq5d_pd_2 > 2 ) &
                                           ((flw_eq5d_ad_2 > flw_eq5d_ad) | (flw_eq5d_pd_2 > flw_eq5d_pd) |(flw_eq5d_ua_2 > flw_eq5d_ua)),
                                         1,0))

summary(post$post_acute)
table(post$post_acute)

# Cognition problems
cog <- mod12_fol[,c("subjid","flw_confusion",'for_cov','flw_remember_today','flw_remember_pre_c19','flw_eq5d_ua_2')]

# lay description for mild dementia: “has some trouble remembering recent events,
# and finds it hard to concentrate and make decisions and plans”

# removing
# "Allow one of the defining items ("flw_confusion",'for_cov','flw_remember_today','flw_remember_pre_c19','flw_limb_weakness','per_fat',
# 'flw_fatigue','flw_eq5d_ua_2', 'flw_eq5d_pd_2') to have missing value"
cog$na_count <- apply(cog, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
cog <- cog[cog$na_count <=1,]

# lay description for mild dementia: “has some trouble remembering recent events,
# and finds it hard to concentrate and make decisions and plans”

# mild cognitive problem
cog <- mutate(cog, cog_mild = ifelse((((for_cov %in% 1 | flw_confusion %in% 1) &
                                       (flw_remember_today %in% 2) &
                                       (flw_eq5d_ua_2 >=3) &
                                       (flw_remember_today > flw_remember_pre_c19)) |
                                      ((for_cov %in% 1 | flw_confusion %in% 1) &
                                       (flw_remember_today %in% 3) &
                                      ((flw_eq5d_ua_2 %in% c(2,3))) &
                                       (flw_remember_today > flw_remember_pre_c19))),
                                     1,0))
table(cog$cog_mild)
summary(cog$cog_mild)

# moderate cognitive problem
# lay description for moderate dementia: “has memory problems and confusion, feels disoriented, at times hears voices that are not real, and needs help with some daily activities”
cog <- mutate(cog, cog_moderate = ifelse((((for_cov %in% 1 | flw_confusion %in% 1) &
                                           (flw_remember_today %in% 3) &
                                           (flw_eq5d_ua_2 >=4) &
                                           (flw_remember_today > flw_remember_pre_c19)) |
                                          ((for_cov %in% 1 | flw_confusion %in% 1) &
                                           (flw_remember_today %in% 4) &
                                           (flw_eq5d_ua_2 >=2) &
                                           (flw_remember_today > flw_remember_pre_c19))),
                                         1,0))
table(cog$cog_moderate)
summary(cog$cog_moderate)

# combined cognitive category(if cog_mild =1 | cog_moderate =1, then cognitive =1 )
cog <- mutate(cog, cognitive = ifelse((cog_mild %in% 1 | cog_moderate %in% 1), 1, 0))
table(cog$cognitive)
summary(cog$cognitive)

# respiratory problems
# lay description for mild COPD: “has cough and shortness of breath after heavy physical activity,
# but is able to walk long distances and climb stairs”
# res_mild
res <- mod12_fol[, c("subjid",'flw_breathless_now', 'flw_pers_cough','flw_eq5d_ua_2','flw_eq5d_ua','flw_breathless_pre_c19','flw_fatigue')]
res$na_count <- apply(res, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
res <- res[res$na_count <=1,]

res <- mutate(res, res_mild = ifelse((((flw_breathless_now %in% 2 | (flw_pers_cough %in% 1 & flw_breathless_now <=3 )) &
                                        flw_eq5d_ua_2 %in% 2) |
                                      ((flw_breathless_now %in% 3) & (flw_pers_cough %in% 1) & (flw_eq5d_ua_2 %in% 2)) |
                                       (flw_breathless_now %in% 3 & flw_pers_cough %in% 0 & flw_eq5d_ua_2 >= 3)) &
                                       (flw_breathless_now > flw_breathless_pre_c19),
                                     1,0))
table(res$res_mild)
summary(res$res_mild)

# res_moderate
res <- mutate(res, res_moderate = ifelse(((flw_breathless_now %in% 3 & flw_pers_cough %in% 1 &
                                          (flw_eq5d_ua_2 >= 3 | flw_fatigue %in% c(4,5,6))) |
                                          (flw_breathless_now %in% 4 & flw_eq5d_ua_2 %in% 2 & flw_fatigue <7) |
                                          (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 %in% 2 | flw_fatigue %in% c(4,5,6) ))) &
                                          (flw_breathless_now > flw_breathless_pre_c19),
                                         1,0))
table(res$res_moderate)
summary(res$res_moderate)

# res_severe
# lay description for severe COPD: “has cough, wheezing and shortness of breath all the time.
# The person has great difficulty walking even short distances or climbing any stairs,
# feels tired when at rest, and is anxious”
res <- mutate(res, res_severe = ifelse(((flw_breathless_now %in% 4 & (flw_eq5d_ua_2 >= 3 | flw_fatigue >=7)) |
                                        (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 >= 3 | flw_fatigue >=7))) &
                                        (flw_breathless_now > flw_breathless_pre_c19), 1, 0))
table(res$res_severe)
summary(res$res_severe)

# combined to the respiratory category
res <- mutate(res, res_combine = ifelse((res_mild %in% 1 | res_moderate %in% 1 | res_severe %in% 1), 1, 0))
table(res$res_combine)

## NEW CLUSTERS

# Fatigue
fatg <- mod12_fol[,c('subjid','per_fat','per_fat_dur','flw_recovered','eq5d_summary','eq5d_summary_2')]

fatg <- mutate(fatg, fatigue = ifelse(per_fat %in% 1 & per_fat_dur %in% c(5,7) & flw_recovered <= 3 &
                                        eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1,
                                      1,0))
table(fatg$fatigue)

# Joint/muscle pain/tightness
jm_pain <- mod12_fol[,c('subjid','mus_pain','flw_joint_pain','rig_mus','flw_recovered',
                        'mus_pain_dur','flw_joint_pain_dur','rig_mus_dur','eq5d_summary','eq5d_summary_2')]

jm_pain <- mutate(jm_pain, joint_mus_pain = ifelse((mus_pain %in% 1 & mus_pain_dur >= 4) |
                                                     (flw_joint_pain %in% 1 & flw_joint_pain_dur >= 4) |
                                                     (rig_mus %in% 1 & rig_mus_dur >= 4) & flw_recovered <= 3 &
                                                     eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1,1,0))
table(jm_pain$joint_mus_pain)

# Breathless
breath <- mod12_fol[,c('subjid','flw_breathless','flw_breathless_dur','flw_breathless_now','flw_breathless_pre_c19',
                       'flw_recovered','eq5d_summary','eq5d_summary_2')]

breath <- mutate(breath, breathless = ifelse(flw_breathless %in% 1 & flw_breathless_dur >= 4 & flw_recovered <= 3 &
                                               ((eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1) |
                                                  flw_breathless_now > flw_breathless_pre_c19),
                                             1,0))
table(breath$breathless)

# Cough, chest pain, pain breathing
cough_chest_br <- mod12_fol[,c('subjid','flw_pers_cough','flw_chest_pains','flw_pain_breathing',
                               'flw_pers_cough_dur','flw_chest_pains_dur','flw_pain_breathing_dur',
                               'flw_recovered','eq5d_summary','eq5d_summary_2')]

cough_chest_br <- mutate(cough_chest_br, cough_chest_breath_pain =
                           ifelse((flw_pers_cough %in% 1 & flw_pers_cough_dur >= 4) |
                                    (flw_chest_pains %in% 1 & flw_chest_pains_dur >= 4) |
                                    (flw_pain_breathing %in% 1 & flw_pain_breathing_dur >= 4) & flw_recovered <= 3 &
                                    eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1,
                                  1,0))
table(cough_chest_br$cough_chest_breath_pain)

# Forgetfulness/lack of concentration/confusion
forg_conf <- mod12_fol[,c('subjid','for_cov','flw_confusion','for_cov_dur','flw_confusion_dur','flw_remember_today','flw_remember_pre_c19',
                          'flw_recovered','eq5d_summary','eq5d_summary_2')]

forg_conf <- mutate(forg_conf, forget_confused = ifelse((for_cov %in% 1 & for_cov_dur >= 4) |
                                                          (flw_confusion %in% 1 & flw_confusion_dur >= 4) & flw_recovered <= 3 &
                                                          ((eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1) |
                                                             flw_remember_today > flw_remember_pre_c19),
                                                        1,0))
table(forg_conf$forget_confused)

# Anxiety/depression
anx_depres <- mod12_fol[,c('subjid','anx_cov','depr_mood','anx_cov_dur','depr_mood_dur','flw_eq5d_ad','flw_eq5d_ad_2',
                           'flw_recovered','eq5d_summary','eq5d_summary_2')]

anx_depres <- mutate(anx_depres, anxiety_depressed = ifelse((anx_cov %in% 1 & anx_cov_dur >= 4) |
                                                              (depr_mood %in% 1 & depr_mood_dur >= 4) & flw_recovered <= 3 &
                                                              ((eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1) |
                                                                 flw_eq5d_ad_2 > flw_eq5d_ad),
                                                            1,0))
table(anx_depres$anxiety_depressed)

# Loss of taste/smell
ts_loss <- mod12_fol[,c('subjid','flw_loss_smell','flw_loss_taste','flw_loss_smell_dur','flw_loss_taste_dur',
                        'flw_recovered','eq5d_summary','eq5d_summary_2')]

ts_loss <- mutate(ts_loss, taste_smell_loss = ifelse((flw_loss_smell %in% 1 & flw_loss_smell_dur >= 4) |
                                                       (flw_loss_taste %in% 1 & flw_loss_taste_dur >= 4) & flw_recovered <=3 &
                                                       eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1, 1,0))
table(ts_loss$taste_smell_loss)

# drop the res severity because not going to use it, the phd stress and anxiety are missing
a1 <- merge(age_mod12, post[,c('subjid','post_acute')])
a2 <- merge(a1, res[, c('subjid',"res_combine")])
a3 <- merge (a2,cog[, c('subjid','cog_mild','cog_moderate','cognitive')])
# a3$overlap <- rowSums(a3[,c("res_mild","res_moderate","res_severe")])

a3 <- merge(a3, fatg[, c('subjid','fatigue')], all.x=T)
a3 <- merge(a3, jm_pain[, c('subjid','joint_mus_pain')], all.x=T)
a3 <- merge(a3, breath[, c('subjid','breathless')], all.x=T)
a3 <- merge(a3, cough_chest_br[, c('subjid','cough_chest_breath_pain')], all.x=T)
a3 <- merge(a3, forg_conf[, c('subjid','forget_confused')], all.x=T)
a3 <- merge(a3, anx_depres[, c('subjid','anxiety_depressed')], all.x=T)
a3 <- merge(a3, ts_loss[, c('subjid','taste_smell_loss')], all.x=T)

# combine the resp, cognitive and fatigue categories
a3 <- mutate(a3, Cog_Res = ifelse((cognitive %in% 1 & res_combine %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Fat = ifelse((cognitive %in% 1 & post_acute %in% 1), 1, 0 ))
a3 <- mutate(a3, Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1
                                       & cognitive %in% 1), 1, 0 ))
# all long term category
a3 <- mutate(a3, long_term = ifelse((cognitive %in% 1 | res_combine %in% 1 | post_acute %in% 1), 1, 0 ))
table(a3$long_term)

# If already in long-term group, exclude from new clusters
a3 <- mutate(a3, fatigue_residual = ifelse(long_term %in% 1 & fatigue %in% 1, 0, fatigue))
a3 <- mutate(a3, joint_mus_pain_residual = ifelse(long_term %in% 1 & joint_mus_pain %in% 1, 0, joint_mus_pain))
a3 <- mutate(a3, breathless_residual = ifelse(long_term %in% 1 & breathless %in% 1, 0, breathless))
a3 <- mutate(a3, cough_chest_breath_pain_residual = ifelse(long_term %in% 1 & cough_chest_breath_pain %in% 1, 0, cough_chest_breath_pain))
a3 <- mutate(a3, forget_confused_residual = ifelse(long_term %in% 1 & forget_confused %in% 1, 0, forget_confused))
a3 <- mutate(a3, taste_smell_loss_residual = ifelse(long_term %in% 1 & taste_smell_loss %in% 1, 0, taste_smell_loss))
a3 <- mutate(a3, anxiety_depressed_residual = ifelse(long_term %in% 1 & anxiety_depressed %in% 1, 0, anxiety_depressed))

# Long term with new clusters (exclude long term with old clusters)
a3 <- mutate(a3, long_term_extended = ifelse(long_term %in% 0 & (fatigue %in% 1 | joint_mus_pain %in% 1 | taste_smell_loss %in% 1 |
                                             breathless %in% 1 | cough_chest_breath_pain %in% 1 |
                                             forget_confused %in% 1 | anxiety_depressed %in% 1), 1, 0))
table(a3$long_term_extended)

# Long term with both old and new clusters
a3 <- mutate(a3, long_term_all = ifelse(long_term %in% 1 | long_term_extended %in% 1, 1, 0))
table(a3$long_term_all)

# Rename duplicated col names between 6m vs 12m
# remove long covid assignment at 12 months if didn't have the symptom cluster at 6 months
a3_cols <- c(colnames(a3),"overlap","res_mild", "res_moderate", "res_severe")
a3_dup_names <- a3_cols[!a3_cols %in% c("subjid","age_group_name","age_group_id","Age","Sex","COVID PCR")]
setnames(clusters_6m, a3_dup_names, paste0(a3_dup_names,"_6mo"))

all_dup_names <- colnames(clusters_6m_all)[!colnames(clusters_6m_all) %in% c("subjid","age_group_name","age_group_id","Age","Sex","COVID PCR")]
setnames(clusters_6m_all, all_dup_names, paste0(all_dup_names,"_6mo"))

# a3_dup_names[!a3_dup_names %in% colnames(clusters_6m)]

# Merge with symptom cluster assignments at 6 months.  this merge also helpfully restricts the data to PCR+ so that we don't have to merge with acute data again
a3 <- data.table(merge(a3, clusters_6m))
table(a3$post_acute, a3$post_acute_6mo)
table(a3$cognitive, a3$cognitive_6mo)
table(a3$res_combine, a3$res_combine_6mo)

# If symptom criteria is not met at 6m, don't count in 12m either
a3[post_acute_6mo==0, post_acute := 0]
a3[cognitive_6mo==0, cognitive := 0]
a3[res_combine_6mo==0, res_combine := 0]

a3[fatigue_6mo==0, fatigue := 0]
a3[joint_mus_pain_6mo==0, joint_mus_pain := 0]
a3[breathless_6mo==0, breathless := 0]
a3[cough_chest_breath_pain_6mo==0, cough_chest_breath_pain := 0]
a3[forget_confused_6mo==0, forget_confused := 0]
a3[taste_smell_loss_6mo==0, taste_smell_loss := 0]
a3[anxiety_depressed_6mo==0, anxiety_depressed := 0]

# Select names that do not match any of the specified patterns
a3_chosen <- names(a3)[!Reduce(`|`, lapply(c("PCR","_6mo","_mild","_moderate","_severe"), function(pattern) {
  grepl(pattern, names(a3))}))]

# Remove redundant cols, NA values and save output
DT <- as.data.table(a3[, ..a3_chosen])
DT$follow_up <- "12_months"
# DT$X <- NULL
DT[is.na(DT)] <- 0
DT <- unique(DT)
DT <- merge(DT, mod12_fol[,c("subjid",'eq5d_summary','eq5d_summary_2')], by = "subjid", all.x=T)
# DT[(overlap >1 & res_mild==res_moderate), res_mild := 0]
# DT[(overlap >1 & res_moderate== res_severe), res_moderate := 0]

# Merge survey responses into output
DT2 <- merge(DT, mod12_fol, by = c("subjid","age_group_name","age_group_id","Age","Sex"), all.x=T)
DT2$eq5d_pair <- NULL

# List of desired columns in specific order
followup_cols <- colnames(mod12_fol)
followup_cols <- followup_cols[!followup_cols %in% c('subjid','Sex','Age','age_group_name','age_group_id','eq5d_pair')]

desired_columns <- c('subjid','Sex','Age','age_group_name','age_group_id',
                     'post_acute', 'cog_mild', 'cog_moderate', 'cognitive', 'res_mild', 'res_moderate', 'res_severe', 'res_combine','overlap',
                     'Cog_Fat', 'Cog_Res', 'Res_Fat', 'Cog_Res_Fat', 'anxiety_depressed','anxiety_depressed_residual',
                     'breathless','breathless_residual', 'cough_chest_breath_pain','cough_chest_breath_pain_residual',
                     'fatigue','fatigue_residual', 'forget_confused','forget_confused_residual', 'joint_mus_pain','joint_mus_pain_residual',
                     'taste_smell_loss','taste_smell_loss_residual','long_term','long_term_extended','long_term_all',
                     'eq5d_summary','eq5d_summary_2','follow_up', followup_cols)

# Filter the list of desired columns based on what actually exists in the dataframe
existing_columns <- desired_columns[desired_columns %in% colnames(DT)]
existing_columns2 <- desired_columns[desired_columns %in% colnames(DT2)]

# Subset the dataframe to only include existing columns in the desired order
DT <- DT[, ..existing_columns]
DT2 <- DT2[, ..existing_columns2]

outpath<-"FILEPATH"
openxlsx::write.xlsx(DT, paste0(outpath,"wv1_12m_ongoing_all_data_marked_v_", version, ".xlsx"),rowNames=F)
openxlsx::write.xlsx(DT2, paste0(outpath,"wv1_12m_ongoing_all_responses_v_", version, ".xlsx"),rowNames=F)

# Calculate frequencies based on symptoms, duration, recovery
bin_vars <- wide_counts2$Variable
bin_vars_dur <- sub("(_\\d+)?$", "_dur\\1", bin_vars)
bin_vars_longCovid <- paste0(bin_vars, "_longCovid")
bin_vars_longCovid_noDur <- paste0(bin_vars, "_longCovid_noDur")
bin_vars_longCovid_residual <- paste0(bin_vars, "_longCovid_residual")
bin_vars_longCovid_noDur_residual <- paste0(bin_vars, "_longCovid_noDur_residual")

df_all <- mod12_fol[,c('subjid','flw_recovered','eq5d_summary','eq5d_summary_2')]
df_all <- merge(df_all, clusters_6m_all, by='subjid', all.x=T)
df_all <- merge(df_all, followup_12m_orig[,c('subjid',bin_vars,bin_vars_dur)], by='subjid')
DT <- as.data.frame(DT)
df_all <- merge(df_all, DT[,c('subjid','long_term', 'long_term_extended','long_term_all','follow_up')], by='subjid')

for (var in bin_vars) {
  var_dur <- sub("(_\\d+)?$", "_dur\\1", var)
  var_longCovid <- paste0(var, "_longCovid")
  var_longCovid_6mo <- paste0(var, "_longCovid_6mo")
  var_longCovid_noDur <- paste0(var, "_longCovid_noDur")
  var_longCovid_noDur_6mo <- paste0(var, "_longCovid_noDur_6mo")
  var_longCovid_residual <- paste0(var, "_longCovid_residual")
  var_longCovid_noDur_residual <- paste0(var, "_longCovid_noDur_residual")

  df_all <- df_all %>%
    mutate(
      # Had to meet long Covid criteria at 6m
      !!var_longCovid := ifelse(
        .data[[var]] == 1 & .data[[var_dur]] >= 4 & flw_recovered <= 3 & .data[[var_longCovid_6mo]] == 1 &
          eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1, 1, 0),
      !!var_longCovid_noDur := ifelse(
        .data[[var]] == 1 & flw_recovered <= 3 & .data[[var_longCovid_noDur_6mo]] == 1 &
          eq5d_summary_2 <= 0.9 & eq5d_summary_2 <= eq5d_summary - 0.1, 1, 0),
      !!var_longCovid_residual := ifelse(
        long_term == 1, 0, .data[[var_longCovid]]),
      !!var_longCovid_noDur_residual := ifelse(
        long_term == 1, 0, .data[[var_longCovid_noDur]])
    )
}

# Use rowwise() to operate across each row individually
df_all <- df_all %>%
  rowwise() %>%
  # If any symptom == 1, return 1, or else return 0
  mutate(any_symptoms = min(1,max(c_across(all_of(bin_vars)),0, na.rm=T))) %>%
  mutate(any_longCovid = min(1,max(c_across(matches("_longCovid$")),0, na.rm=T))) %>%
  mutate(any_longCovid_noDur = min(1,max(c_across(matches("_longCovid_noDur$")),0, na.rm=T))) %>%
  mutate(any_longCovid_residual = min(1,max(c_across(matches("_longCovid_residual$")),0, na.rm=T))) %>%
  mutate(any_longCovid_noDur_residual = min(1,max(c_across(matches("_longCovid_noDur_residual$")),0, na.rm=T))) %>%
  ungroup()

df_all <- df_all[, !colnames(df_all) %like% "_6mo$"]
openxlsx::write.xlsx(df_all, paste0(outpath ,"wv1_12m_ongoing_all_symptoms_v_",version, ".xlsx"), rowNames=F)

# Export results for long covid residuals
df_all_resi <- df_all[df_all$any_longCovid == 1, grep("subjid|long_term$|_longCovid$|_longCovid_residual$", colnames(df_all), value = TRUE)]
df_all_noDur <- df_all[df_all$any_longCovid_noDur == 1, grep("subjid|long_term$|_noDur$|_noDur_residual$", colnames(df_all), value = TRUE)]
openxlsx::write.xlsx(df_all_resi, paste0(outpath ,"wv1_12m_ongoing_longCovid_symptoms_v_",version, ".xlsx"), rowNames=F)
openxlsx::write.xlsx(df_all_resi, paste0(outpath ,"wv1_12m_ongoing_longCovid_noDur_symptoms_v_",version, ".xlsx"), rowNames=F)

# Calculate total patients with long Covid
df_all_lc <- df_all %>% select(bin_vars_longCovid)
df_all_lc <- data.frame(colSums(df_all_lc, na.rm = TRUE))
colnames(df_all_lc) <- "yes_longCovid"
df_all_lc$Variable_longCovid <- rownames(df_all_lc)
rownames(df_all_lc) <- c(1:nrow(df_all_lc))
df_all_lc <- df_all_lc %>%
  mutate(Variable = str_replace(Variable_longCovid, "_longCovid", ""))

# Calculate total patients with long Covid, not captured in original clusters
df_all_lc_residual <- df_all %>% select(bin_vars_longCovid_residual)
df_all_lc_residual <- data.frame(colSums(df_all_lc_residual, na.rm = TRUE))
colnames(df_all_lc_residual) <- "yes_longCovid_residual"
df_all_lc_residual$Variable_longCovid_residual <- rownames(df_all_lc_residual)
rownames(df_all_lc_residual) <- c(1:nrow(df_all_lc_residual))
df_all_lc_residual <- df_all_lc_residual %>%
  mutate(Variable = str_replace(Variable_longCovid_residual, "_longCovid_residual", ""))

# Calculate total patients with long Covid w/out duration criteria
df_all_noDur <- df_all %>% select(bin_vars_longCovid_noDur)
df_all_noDur <- data.frame(colSums(df_all_noDur, na.rm = TRUE))
colnames(df_all_noDur) <- "yes_longCovid_noDur"
df_all_noDur$Variable_longCovid_noDur <- rownames(df_all_noDur)
rownames(df_all_noDur) <- c(1:nrow(df_all_noDur))
df_all_noDur <- df_all_noDur %>%
  mutate(Variable = str_replace(Variable_longCovid_noDur, "_longCovid_noDur", ""))

# Calculate total patients with long Covid w/out duration criteria, not captured in original clusters
df_all_noDur_residual <- df_all %>% select(bin_vars_longCovid_noDur_residual)
df_all_noDur_residual <- data.frame(colSums(df_all_noDur_residual, na.rm = TRUE))
colnames(df_all_noDur_residual) <- "yes_longCovid_noDur_residual"
df_all_noDur_residual$Variable_longCovid_noDur_residual <- rownames(df_all_noDur_residual)
rownames(df_all_noDur_residual) <- c(1:nrow(df_all_noDur_residual))
df_all_noDur_residual <- df_all_noDur_residual %>%
  mutate(Variable = str_replace(Variable_longCovid_noDur_residual, "_longCovid_noDur_residual", ""))

# Calculate "any of the above" row
any_cols <- c("any_symptoms","any_longCovid","any_longCovid_noDur","any_longCovid_residual","any_longCovid_noDur_residual")
df_all_any <- df_all %>% select(all_of(any_cols))
df_all_any <- data.frame(t(colSums(df_all_any, na.rm = TRUE)))
df_all_any$No <- NA
df_all_any$Unknown <- NA
df_all_any$Variable <- "any_of_the_symptoms"
setnames(df_all_any, "any_symptoms", "Yes")
setnames(df_all_any, "any_longCovid", "yes_longCovid")
setnames(df_all_any, "any_longCovid_noDur", "yes_longCovid_noDur")
setnames(df_all_any, "any_longCovid_residual", "yes_longCovid_residual")
setnames(df_all_any, "any_longCovid_noDur_residual", "yes_longCovid_noDur_residual")

# Merge stats
df_all_lc <- merge(df_all_lc, df_all_lc_residual, by="Variable")
df_all_noDur <- merge(df_all_noDur, df_all_noDur_residual, by="Variable")
df_both <- merge(df_all_lc, df_all_noDur, by="Variable")
df_both <- merge(wide_counts2, df_both, by="Variable")
df_both$Variable_longCovid <- NULL
df_both$Variable_longCovid_residual <- NULL
df_both$Variable_longCovid_noDur <- NULL
df_both$Variable_longCovid_noDur_residual <- NULL
df_both <- rbind(df_both, df_all_any)

openxlsx::write.xlsx(df_both, paste0(outpath ,"wv1_12m_ongoing_grouped_symptoms_v_",version, ".xlsx"),rowNames=F)

# Merge with age
inter <- merge(ages, DT)[,!c('subjid','Age')]

# Tabulate
# cols <- c("age_group_id","age_group_name", "sex","icu_hoterm","COVID_PCR")
data_long <- gather(inter , measure, value, c(post_acute:long_term_all))

# table by age and sex
cocurr_mu <- data_long %>%
  group_by(age_group_id,age_group_name,Sex, measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0
cocurr_mu$follow_up <- "12_months"
cocurr_mu <- cocurr_mu %>% arrange(age_group_id, Sex)

openxlsx::write.xlsx(cocurr_mu, paste0(outpath,"wv1_12m_ongoing_byAgeSex_v_", version, ".xlsx"),rowNames=F)

# table by sex only
cocurr_sex <- data_long %>%
  group_by(Sex,measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  spread(measure, var)

cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0
cocurr_sex$follow_up <- "12_months"

# List of desired columns in specific order
desired_columns <- c('Sex', 'post_acute', 'cog_mild', 'cog_moderate', 'cognitive', 'res_mild', 'res_moderate', 'res_severe', 'res_combine',
                     'Cog_Fat', 'Cog_Res', 'Res_Fat', 'Cog_Res_Fat', 'anxiety_depressed','anxiety_depressed_residual',
                     'breathless','breathless_residual', 'cough_chest_breath_pain','cough_chest_breath_pain_residual',
                     'fatigue','fatigue_residual', 'forget_confused','forget_confused_residual', 'joint_mus_pain','joint_mus_pain_residual',
                     'taste_smell_loss','taste_smell_loss_residual','long_term', 'long_term_extended','long_term_all', 'N', 'follow_up')

# Filter the list of desired columns based on what actually exists in the dataframe
existing_columns <- desired_columns[desired_columns %in% colnames(cocurr_sex)]

# Subset the dataframe to only include existing columns in the desired order
cocurr_sex <- cocurr_sex[, existing_columns]

openxlsx::write.xlsx(cocurr_sex, paste0(outpath,"wv1_12m_ongoing_bySex_v_", version, ".xlsx"),rowNames=F)
openxlsx::write.xlsx(cocurr_sex, paste0("FILEPATH", version, ".xlsx"),rowNames=F)

# Read in 6-month data by Sex from wave 1 and 2
outpath<-"FILEPATH"
df_6mo_wave1 <- read_excel(paste0(outpath,"wave_1_6_month___bySex_v_", version, ".xlsx"))
df_6mo_wave2 <- read_excel(paste0(outpath,"wave_2_6_month___bySex_v_", version, ".xlsx"))

df <- data.table(rbind(df_6mo_wave1, df_6mo_wave2))

# Table by sex only
df <- df[, lapply(.SD, sum, na.rm=T), by=c("Sex", "follow_up"),
         .SDcols=setdiff(names(df), c("Sex", "follow_up"))]
df_both <- copy(df)
df_both$Sex <- NULL
df_both <- df_both[, lapply(.SD, sum, na.rm=T), by=c("follow_up"),
         .SDcols=setdiff(names(df_both), c("follow_up"))]
df_both$Sex <- "Both"
df <- rbind(df, df_both)

openxlsx::write.xlsx(df, paste0("FILEPATH", version, ".xlsx"),rowNames=F)

# Merge symptom freq data
df_grouped_symp1 <- read_excel(paste0(outpath,"wave_1_6_month___grouped_symptoms_v_",version,".xlsx"))
#df_grouped_symp2 <- read_excel(paste0(outpath,"wave_1_12_month_Initial_grouped_symptoms_v_",version,".xlsx"))
df_grouped_symp2 <- read_excel(paste0(outpath,"wv1_12m_ongoing_grouped_symptoms_v_",version,".xlsx"))
df_grouped_symp3 <- read_excel(paste0(outpath,"wave_2_6_month___grouped_symptoms_v_",version,".xlsx"))

df_grouped_symp1$follow_up <- "W1_6m"
#df_grouped_symp2$follow_up <- "W1_12m_init"
df_grouped_symp2$follow_up <- "W1_12m"
df_grouped_symp3$follow_up <- "W2_6m"

df_grouped_symp <- rbind(df_grouped_symp1, df_grouped_symp2, df_grouped_symp3)
openxlsx::write.xlsx(df_grouped_symp, paste0(outpath, "all_grouped_symptoms_v_", version, ".xlsx"),rowNames=F)

# Building plots -----------------------------------------------------------
outpath<-"FILEPATH"
df_w1_6m <- read_excel(paste0(outpath,"wave_1_6_month___all_responses_v_", version, ".xlsx"))
df_w1_12m_init <- read_excel(paste0(outpath, "wave_1_12_month_Initial_all_responses_v_", version, ".xlsx"))
df_w1_12m_ong <- read_excel(paste0(outpath, "wv1_12m_ongoing_all_responses_v_", version, ".xlsx"))
df_w2_6m <- read_excel(paste0(outpath, "wave_2_6_month___all_responses_v_", version, ".xlsx"))

df_w1_6m <- na.omit(df_w1_6m[df_w1_6m$fatigue_residual == 1 & df_w1_6m$per_fat != 2,
                             c('fatigue_residual','flw_fatigue','per_fat')])
df_w1_12m_init <- na.omit(df_w1_12m_init[df_w1_12m_init$fatigue_residual == 1 & df_w1_12m_init$per_fat != 2,
                                         c('fatigue_residual','flw_fatigue','per_fat')])
df_w1_12m_ong <- na.omit(df_w1_12m_ong[df_w1_12m_ong$fatigue_residual == 1 & df_w1_12m_ong$per_fat != 2,
                                       c('fatigue_residual','flw_fatigue','per_fat')])
df_w2_6m <- na.omit(df_w2_6m[df_w2_6m$fatigue_residual == 1 & df_w2_6m$per_fat != 2,
                             c('fatigue_residual','flw_fatigue','per_fat')])

# Create histogram of flw_fatigue by per_fat
hist_plot <- function(data, file_pattern, x, y){
  my_plot_grp <- data %>% group_by(per_fat) %>% summarise(N=n(), .groups = 'drop')

  my_plot <-
    ggplot(data, aes(x = x, fill=factor(y))) +
    geom_histogram(bins = 10, binwidth=1, color = "black", show.legend=T) +  # You can change binwidth based on your data
    geom_text(stat = 'count', aes(label = ..count..), vjust = -0.5, color = "black", size=3.5) +  # Add data labels
    facet_wrap(~ factor(per_fat), labeller = as_labeller(function(label) {
      paste("N =", my_plot_grp$N[which(my_plot_grp$per_fat == label)])
    })) +
    labs(x = "\nflw_fatigue", y = "Frequencies\n", fill="per_fat",
         title=paste0("Histogram of flw_fatigue for ",nrow(data)," cases of fatigue_residual")) +  # Change labels as appropriate
    scale_x_continuous(breaks = seq(0, 10, 1)) +  # X-axis ticks from 0 to 10
    theme_minimal() +  # Apply minimal theme
    theme(plot.title = element_text(hjust = 0.5),  # Center the plot title
          panel.background = element_rect(fill = "white", colour = "black"), # Set background to white
          strip.text.x =  element_text(size = 16))  # Remove facet label

  ggsave(paste0(outpath, file_pattern, version,"_hist_fatigue.png"),
         my_plot, width = 10, height = 8, units = "in", bg = "white")
}

hist_plot(df_w1_6m, "wave_1_6_month___all_responses_v_", df_w1_6m$flw_fatigue, df_w1_6m$per_fat)
hist_plot(df_w1_12m_init, "wave_1_12_month_Initial_all_responses_v_", df_w1_12m_init$flw_fatigue, df_w1_12m_init$per_fat)
hist_plot(df_w1_12m_ong, "wv1_12m_ongoing_all_responses_v_", df_w1_12m_ong$flw_fatigue, df_w1_12m_ong$per_fat) # no data to plot
hist_plot(df_w2_6m, "wave_2_6_month___all_responses_v_", df_w2_6m$flw_fatigue, df_w2_6m$per_fat)

# Build bar plots by sex
df_w1_6m_sex <- read_excel(paste0(outpath,"wave_1_6_month___bySex_v_", version, ".xlsx"))
df_w1_12m_init_sex <- read_excel(paste0(outpath, "wave_1_12_month_Initial_bySex_v_", version, ".xlsx"))
df_w1_12m_ong_sex <- read_excel(paste0(outpath, "wv1_12m_ongoing_bySex_v_", version, ".xlsx"))
df_w2_6m_sex <- read_excel(paste0(outpath, "wave_2_6_month___bySex_v_", version, ".xlsx"))

# Reshape the data from wide to long format
bar_plot <- function(data, file_pattern, arrange=F){
  plot_names <- setdiff(colnames(data), c("Sex","follow_up","N"))

  long_df <- data %>%
    pivot_longer(
      cols = -c(Sex, follow_up, N),  # Deselect columns not used for reshaping
      names_to = "Variable",
      values_to = "Value"
    )
  long_df$Variable <- factor(long_df$Variable, levels = plot_names)
  long_df$Sex_label <- paste0(long_df$Sex," (N=",long_df$N,")")

  # Reorder 'Variable' based on total Value
  if (arrange==T){
    means <- long_df %>%
      group_by(Variable) %>%
      summarize(total_value = sum(Value, na.rm = TRUE), .groups = 'drop') %>%
      arrange(total_value)
    long_df$Variable <- factor(long_df$Variable, levels = means$Variable)
  }

  # Create the bar plot
  my_plot <-
    ggplot(long_df, aes(x = Value, y = Variable, fill = factor(Sex_label))) +
    geom_bar(stat = "summary", fun = "mean", position = "dodge") +
    geom_text(aes(label = ..x..), stat = "summary", fun = "mean",
              position = position_dodge(width = 0.9), hjust = -0.5, size = 3, color = "black") +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1, size=14),
          axis.text.y = element_text(size=20)) +
    labs(x = "Count", y = "", fill = "Sex") +
    theme_minimal() +
    scale_fill_brewer(palette = "Set1")

  ggsave(paste0(outpath, file_pattern, version,"_barplot.png"),
         my_plot, width = 10, height = 8, units = "in", bg = "white")
}

bar_plot(df_w1_6m_sex,"wave_1_6_month___bySex_v_", arrange=T)
bar_plot(df_w1_12m_init_sex,"wave_1_12_month_Initial_bySex_v_", arrange=T)
bar_plot(df_w1_12m_ong_sex,"wv1_12m_ongoing_bySex_v_", arrange=T)
bar_plot(df_w2_6m_sex,"wave_2_6_month___bySex_v_", arrange=T)

# Build heat map plots by symptoms
df_w1_6m_symp <- read_excel(paste0(outpath,"wave_1_6_month___grouped_symptoms_v_", version, ".xlsx"))
df_w1_12m_init_symp <- read_excel(paste0(outpath, "wave_1_12_month_Initial_grouped_symptoms_v_", version, ".xlsx"))
df_w1_12m_ong_symp <- read_excel(paste0(outpath, "wv1_12m_ongoing_grouped_symptoms_v_", version, ".xlsx"))
df_w2_6m_symp <- read_excel(paste0(outpath, "wave_2_6_month___grouped_symptoms_v_", version, ".xlsx"))

heat_plot <- function(data, file_pattern){

  # Transform the dataframe into a long format suitable for ggplot
  df_long <- data %>%
    pivot_longer(cols = -c(Variable), names_to = "Condition", values_to = "Count") %>%
    filter(!Condition %in% c("No","Yes","Unknown"), Variable != "any_of_the_symptoms") %>%
    arrange(Variable) %>%
    mutate(Variable = factor(Variable, levels = rev(unique(Variable))))  # For reverse alphabetical order

  # Now, create the 100% stacked bar plot using ggplot2
  my_plot <-
    ggplot(df_long, aes(x = Condition, y = Variable, fill = Count)) +
    geom_tile() +  # Create the tiles
    scale_fill_gradient2(low = "white", mid = "gray", high = "darkred", midpoint=median(df_long$Count)) +  # Gradient color
    geom_text(aes(label = Count), size = 3, color = "black", vjust = 0.5) + # add data counts on the plot
    theme_minimal() +  # Minimal theme
    theme(axis.text.x = element_text(angle = 45, hjust = 1, size=10),  # Rotate x labels for better readability
          axis.text.y = element_text(size=10),
          axis.title.x = element_blank(),  # Remove x axis title
          axis.title.y = element_blank()) +  # Remove y axis title
    labs(fill = "Count")

  ggsave(paste0(outpath, file_pattern, version,"_heatplot.png"),
         my_plot, width = 15, height = 10, units = "in", bg = "white")
}

heat_plot(df_w1_6m_symp,"wave_1_6_month_v_")
heat_plot(df_w1_12m_init_symp,"wave_1_12_month_Initial_v_")
heat_plot(df_w1_12m_ong_symp,"wv1_12m_ongoing_v_")
heat_plot(df_w2_6m_symp,"wave_2_6_month_v_")
