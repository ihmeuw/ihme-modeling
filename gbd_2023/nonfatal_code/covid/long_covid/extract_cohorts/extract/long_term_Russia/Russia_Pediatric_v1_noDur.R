## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script:
## Description: Russian Data recoded_using 1000+ data Pediatric data
## Contributors: NAME
## Date 1/20/2022
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
#remove the history and call the GBD functions
rm(list=ls(all.names=T))

library(dplyr)
library(readxl)
library(openxlsx)
library(data.table)
library(lubridate)
library(forcats)

vers <- 2

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

code_dir <- "FILEPATH"

source(paste0(code_dir, "db_init.R"))
source(paste0(code_dir, "get_age_map.r"))
source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k ,"FILEPATH/get_location_metadata.R"))
source(paste0(roots$k ,"FILEPATH/get_age_metadata.R"))

# ----------------------------------------------------------------------------------
tabulation <- function(path1, path2, wave_number, outpath, followup_time, typeData){

  #the pediatric data has wave 1 and wave 2
  #need to combine the two together
  if(wave_number=="wave_1"){
    mod_wv1 <- read_excel(path1)[,c("PIN","Date of birth","Age")]

    # Define a function to convert age to numeric years
    convert_age_to_years <- function(age) {
      parts <- strsplit(age, " y, | m")[[1]] # Split the age string by ' y, ' and ' m'
      if(length(parts) == 2) {
        years <- as.numeric(parts[1]) # Convert the years part to numeric
        months <- as.numeric(parts[2]) / 12 # Convert the months part to numeric and divide by 12
        return(years + months) # Return the sum of years and the months as a fraction of a year
      } else {
        return(as.numeric(parts[1])) # If there's no month part, return the years as numeric
      }
    }

    # Apply the function to the Age column to create a new numeric age column
    mod_wv1$Age <- sapply(mod_wv1$Age, convert_age_to_years)

    # mod_wv1$DOB <- ymd(mod_wv1$`Date of birth`)
    # mod_wv1$today <- ymd(mod_wv1$`Date of admission`)
    # mod_wv1$Age <- interval(start= mod_wv1$DOB, end=mod_wv1$today)/
    #   lubridate::duration(n=1, unit="years")
    message(paste("Average age: "), mean(mod_wv1$Age, na.rm=T))
    mod_wv1<- mod_wv1[,c("PIN","Age")]
    mod_wv1 <- mod_wv1[!is.na(mod_wv1$PIN),]
    mod_wv1$Age <- round(mod_wv1$Age)
  }
  else{
    mod_wv1 <-
      read_excel(path1)
  }

  age_map <- get_age_map(gbd_year = roots$gbd_year, type = "all")
  age_map <- age_map[, .(age_group_id, age_group_name)]

  mod_wv1 <- mutate(mod_wv1, age_group_name = ifelse(Age < 1, "<1 year",
                                              ifelse(Age %in% 1:4, "1 to 4",
                                              ifelse(Age %in% 5:9, "5 to 9",
                                              ifelse(Age %in% 10:14, "10 to 14",
                                              ifelse(Age %in% 15:19, "15 to 19",
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
                                              ifelse(Age %in% 90:94, "90 to 94",
                                              ifelse(Age >= 95, "95 plus", "Unknown"))))))))))))))))))))))

  mod_wv1 <- merge(mod_wv1, age_map, by='age_group_name')

  followup_orig <- read_excel(path2)

  if (typeData== "Ongoing")
  {
    followup_6m <- read_excel('FILEPATH/Parents-Initial.xlsx')[,c("subjid","vaccination_covid")]
    followup_orig <- merge(followup_orig, followup_6m, by='subjid', all.x=T)

    followup_initial <- followup_orig[,c("subjid", "flw_sex_ong", 'vaccination_covid',"fully_recover_ong", "flw_loss_smell_ong","flw_loss_smell_2_ong", "flw_loss_smell_dur_ong", "flw_loss_smell_dur_2_ong","flw_loss_taste_ong", "flw_loss_taste_2_ong", "flw_loss_taste_dur_ong", "flw_loss_taste_dur_2_ong","flw_fat_ong", "flw_fat_2_ong", "flw_fat_dur_ong", "flw_fat_dur_2_ong", "vas_fatigue_ong","vas_fatigue_change_ong", "flw_muscle_pain_ong", "flw_muscle_pain_2_ong", "flw_muscle_pain_dur_ong","flw_muscle_pain_dur_2_ong", "flw_joint_pain_ong", "flw_joint_pain_2_ong", "flw_joint_pain_dur_ong", "flw_joint_pain_dur_2_ong", "flw_breathless_ong", "flw_breathless_2_ong", "flw_breathless_dur_ong","flw_breathless_dur_2_ong", "flw_pain_breathing_ong", "flw_pain_breathing_2_ong","flw_pain_breathing_dur_ong", "flw_pain_breathing_dur_2_ong", "flw_pers_cough_ong","flw_pers_cough_2_ong", "flw_pers_cough_dur_ong", "flw_pers_cough_dur_2_ong", "flw_chest_pains_ong","flw_chest_pains_2_ong", "flw_chest_pains_dur_ong", "flw_chest_pains_dur_2_ong", "flw_confusion_ong","flw_confusion_2_ong", "flw_confusion_dur_ong", "flw_confusion_dur_2_ong", "flw_eq5d5l_vas_ong","flw_eq5d5l_vas_2_ong",'flw_headache_ong','flw_headache_2_ong')]

    # Replace '_ong' with '' (nothing) in all column names
    colnames(followup_orig) <- gsub("_ong$", "", colnames(followup_orig))
    colnames(followup_initial) <- gsub("_ong$", "", colnames(followup_initial))

  } else {

    followup_initial <- followup_orig[,c("subjid", "flw_sex",'vaccination_covid','fully_recover','flw_loss_smell','flw_loss_smell_2','flw_loss_smell_dur', 'flw_loss_smell_dur_2','flw_loss_taste','flw_loss_taste_2','flw_loss_taste_dur','flw_loss_taste_dur_2','flw_fat',"flw_fat_2",'flw_fat_dur', 'flw_fat_dur_2','vas_fatigue','vas_fatigue_change','flw_muscle_pain','flw_muscle_pain_2','flw_muscle_pain_dur','flw_muscle_pain_dur_2', 'flw_joint_pain','flw_joint_pain_2','flw_joint_pain_dur','flw_joint_pain_dur_2',"flw_breathless","flw_breathless_2",'flw_breathless_dur', 'flw_breathless_dur_2','flw_pain_breathing','flw_pain_breathing_2','flw_pain_breathing_dur','flw_pain_breathing_dur_2','flw_pers_cough', 'flw_pers_cough_2','flw_pers_cough_dur','flw_pers_cough_dur_2','flw_chest_pains','flw_chest_pains_2','flw_chest_pains_dur','flw_chest_pains_dur_2', "flw_confusion","flw_confusion_2",'flw_confusion_dur','flw_confusion_dur_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2','flw_headache','flw_headache_2')]
  }

  names(mod_wv1)[names(mod_wv1) == "PIN"] <- "subjid"

  # Store survey responses
  followup_cols <- setdiff(colnames(followup_initial), c("subjid",'flw_sex'))

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
  # Use sub() to insert '_dur' before any trailing underscore and numbers, or at the end if there are none
  bin_vars <- sub("(_\\d+)?$", "_dur\\1", wide_counts2$Variable)
  bin_vars_dur <- wide_counts3$Variable
  bin_vars_select <- bin_vars_dur[bin_vars_dur %in% bin_vars]
  bin_vars_select <- str_replace(bin_vars_select, "_dur", "")

  # Select only symptoms that have duration
  wide_counts2 <- wide_counts2[wide_counts2$Variable %in% bin_vars_select,]

  # Merging age and response
  mod12_fol <- merge(mod_wv1, followup_orig)

  # Add a new column that checks if at least one pair activ/vas has non-null values
  mod12_fol$eq5d_pair <- apply(mod12_fol, 1, function(x) {
    valid_vas = !is.na(x['flw_eq5d5l_vas']) & !is.na(x['flw_eq5d5l_vas_2']) # Check if both value in the vas pair is not NA
    return(valid_vas==T) # Return TRUE if either pair is valid
  })

  # Getting the number of patients by age and by sex
  sample_n <- mod12_fol %>%
    group_by(age_group_id,age_group_name,flw_sex) %>%
    dplyr::summarise(N=n())

  # Getting the number of patients by sex
  sample_n_bysex <- mod12_fol %>%
    group_by(flw_sex) %>%
    summarise(N=n())

  # Getting the number of patients by vaccination status
  sample_n_byVac <- mod12_fol %>%
    group_by(vaccination_covid) %>%
    summarise(N=n())

  # Post-acute cluster
  post <- mod12_fol[,c("subjid","flw_fat",'vas_fatigue','vas_fatigue_change')]
  post <- mutate(post, post_acute = ifelse((flw_fat %in% 1) & (vas_fatigue %in% c(4,5)) & (vas_fatigue_change %in% c(1,3)), 1,0))

  message(paste("Average fatigue: "), mean(post$post_acute))

  # Cognition problems
  cog <- mod12_fol[,c("subjid","flw_confusion","flw_confusion_2")]

  # lay description for mild dementia: “has some trouble remembering recent events,
  # and finds it hard to concentrate and make decisions and plans removing
  cog$na_count <- apply(cog, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  cog <- cog[cog$na_count <=1,]

  cog <- mutate(cog, cognitive = ifelse((flw_confusion %in% 1 | flw_confusion_2 %in% 1),1,0))
  message(paste("Average cognitive: "), mean(cog$cognitive))

  # Respiratory problems
  # lay description for mild COPD: “has cough and shortness of breath after heavy physical activity,
  # but is able to walk long distances and climb stairs”
  # mild
  res <- mod12_fol[, c("subjid",'flw_breathless', 'flw_breathless_2')]
  res$na_count <- apply(res, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  res <- res[res$na_count <=1,]

  res <- mutate(res, res_combine = ifelse((flw_breathless %in% 1 | flw_breathless_2 %in% 1), 1, 0))
  message(paste("Average res: "), mean(res$res_combine))

  ## Extra clusters

  # Fatigue sub-threshold cluster (removed anx, depres)
  if (wave_number=="wave_1"){
    fatg <- mod12_fol[,c('subjid','flw_fat','flw_fat_2','flw_muscle_pain','flw_muscle_pain_2',
                         'flw_joint_pain','flw_joint_pain_2','flw_diag_anx','flw_diag_depres',
                         'flw_eq5d5l_vas','flw_eq5d5l_vas_2','eq5d_pair','fully_recover')]
    fatg <- mutate(fatg, fatigue_sub = ifelse((flw_fat %in% 1 | flw_fat_2 %in% 1 | flw_muscle_pain %in% 1 | flw_muscle_pain_2 %in% 1 |
                                                 flw_joint_pain %in% 1 | flw_joint_pain_2 %in% 1) &
                                                fully_recover <= 5, 1, 0))
  } else {
    fatg <- mod12_fol[,c('subjid','flw_fat','flw_fat_2','flw_muscle_pain','flw_muscle_pain_2',
                         'flw_joint_pain','flw_joint_pain_2',
                         'flw_eq5d5l_vas','flw_eq5d5l_vas_2','eq5d_pair','fully_recover')]
    fatg <- mutate(fatg, fatigue_sub = ifelse((flw_fat %in% 1 | flw_fat_2 %in% 1 | flw_muscle_pain %in% 1 | flw_muscle_pain_2 %in% 1 |
                                                 flw_joint_pain %in% 1 | flw_joint_pain_2 %in% 1) &
                                                fully_recover <= 5, 1, 0))
  }

  message(paste("Average fatigue_sub: "), mean(fatg$fatigue_sub, na.rm=T))

  # Anxiety depression
  if (wave_number=="wave_1"){
    anxiety_depres <- mod12_fol[,c('subjid','flw_diag_anx','flw_diag_depres','eq5d_pair','fully_recover')]
    anxiety_depres <- mutate(anxiety_depres, anxiety_depressed = ifelse((flw_diag_anx %in% 1 | flw_diag_depres %in% 1) &
                                                fully_recover <= 5, 1, 0))
  } else {
    anxiety_depres <- mod12_fol[,c('subjid','eq5d_pair','fully_recover')]
    anxiety_depres$anxiety_depressed <- 0
  }

  message(paste("Average anxiety_depres: "), mean(anxiety_depres$anxiety_depressed, na.rm=T))

  # Respiratory sub-threshold cluster
  breath <- mod12_fol[,c('subjid','flw_breathless','flw_breathless_2','flw_pers_cough','flw_pers_cough_2',
                         'flw_chest_pains','flw_chest_pains_2','flw_pain_breathing','flw_pain_breathing_2',
                         'flw_cong','flw_cong_2','fully_recover',
                         'flw_eq5d5l_vas','flw_eq5d5l_vas_2','eq5d_pair')]

  breath <- mutate(breath, res_sub = ifelse((flw_breathless %in% 1 | flw_breathless_2 %in% 1 | flw_pers_cough %in% 1 |
                                                  flw_pers_cough_2 %in% 1 | flw_chest_pains %in% 1 | flw_chest_pains_2 %in% 1 |
                                                  flw_pain_breathing %in% 1 | flw_pain_breathing_2 %in% 1 | flw_cong %in% 1 | flw_cong_2 %in% 1) &
                                                 fully_recover <= 5,1,0))
  message(paste("Average res_sub: "), mean(breath$res_sub, na.rm=T))

  # Cognitive sub-threshold cluster
  forg_conf <- mod12_fol[,c('subjid','flw_confusion','flw_confusion_2','flw_confusion_dur','flw_confusion_dur_2',
                            'flw_eq5d5l_vas','flw_eq5d5l_vas_2','eq5d_pair','fully_recover')]

  forg_conf <- mutate(forg_conf, cog_sub = ifelse((flw_confusion %in% 1 | flw_confusion_2 %in% 1) &
                                                            fully_recover <= 5,1,0))
  message(paste("Average cog_sub: "), mean(forg_conf$cog_sub, na.rm=T))

  # Disturbed/loss sense of taste/smell
  ts_loss <- mod12_fol[,c('subjid','flw_loss_smell','flw_loss_smell_2','flw_loss_taste','flw_loss_taste_2','eq5d_pair',
                          'flw_altern_smell','flw_altern_smell_2','flw_altern_taste','flw_altern_taste_2',
                          'flw_eq5d5l_vas','flw_eq5d5l_vas_2','fully_recover')]

  ts_loss <- mutate(ts_loss, taste_smell_loss = ifelse((flw_loss_smell %in% 1 | flw_loss_smell_2 %in% 1 |
                                                          flw_loss_taste %in% 1 | flw_loss_taste_2 %in% 1 |
                                                          flw_altern_smell %in% 1 | flw_altern_smell_2 %in% 1 |
                                                          flw_altern_taste %in% 1 | flw_altern_taste_2 %in% 1) &
                                                         fully_recover <= 5,1,0))
  message(paste("Average ts_loss: "), mean(ts_loss$taste_smell_loss, na.rm=T))

  # Sleep problems
  sleep <- mod12_fol[,c('subjid','flw_sleepless','flw_sleepless_2',
                          'flw_eq5d5l_vas','flw_eq5d5l_vas_2','fully_recover')]

  sleep <- mutate(sleep, sleep_loss = ifelse((flw_sleepless %in% 1 | flw_sleepless_2 %in% 1) &
                                                         fully_recover <= 5,1,0))
  message(paste("Average sleep: "), mean(sleep$sleep_loss, na.rm=T))

  # Dizziness
  dizzy <- mod12_fol[,c('subjid','flw_dizziness','flw_dizziness_2',
                        'flw_eq5d5l_vas','flw_eq5d5l_vas_2','fully_recover')]

  dizzy <- mutate(dizzy, dizziness = ifelse((flw_dizziness %in% 1 | flw_dizziness_2 %in% 1) &
                                               fully_recover <= 5,1,0))
  message(paste("Average dizzy: "), mean(dizzy$dizziness, na.rm=T))

  # Headache
  headache <- mod12_fol[,c('subjid','flw_headache','flw_headache_2',
                        'flw_eq5d5l_vas','flw_eq5d5l_vas_2','fully_recover')]

  headache <- mutate(headache, headache = ifelse((flw_headache %in% 1 | flw_headache_2 %in% 1) &
                                              fully_recover <= 5,1,0))
  message(paste("Average headache: "), mean(headache$headache, na.rm=T))

  # Drop the res severity because not going to use it, the phd stress and anxiety are missing
  a1 <- post[,c('subjid','post_acute')]
  a2 <- merge(a1, res[, c('subjid',"res_combine")])
  a3 <- merge (a2,cog[, c('subjid','cognitive')])

  a3 <- merge(a3, fatg[, c('subjid','fatigue_sub')], all.x=T)
  a3 <- merge(a3, breath[, c('subjid','res_sub')], all.x=T)
  a3 <- merge(a3, forg_conf[, c('subjid','cog_sub')], all.x=T)
  a3 <- merge(a3, ts_loss[, c('subjid','taste_smell_loss')], all.x=T)
  a3 <- merge(a3, sleep[, c('subjid','sleep_loss')], all.x=T)
  a3 <- merge(a3, dizzy[, c('subjid','dizziness')], all.x=T)
  a3 <- merge(a3, headache[, c('subjid','headache')], all.x=T)
  a3 <- merge(a3, anxiety_depres[, c('subjid','anxiety_depressed')], all.x=T)

  # Combine the resp, cognitive and fatigue categories
  a3 <- mutate(a3, Cog_Res = ifelse((cognitive %in% 1 & res_combine %in% 1), 1, 0 ))
  a3 <- mutate(a3, Cog_Fat = ifelse((cognitive %in% 1 & post_acute %in% 1), 1, 0 ))
  a3 <- mutate(a3, Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1), 1, 0 ))
  a3 <- mutate(a3, Cog_Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1
                                         & cognitive %in% 1), 1, 0 ))

  # Import data from 6m to add restriction for longer follow ups
  if (typeData == "Ongoing"){
    clusters_6m <- read_excel(paste0(outpath , "wave_1_6_month_initial_all_data_v_", vers, ".xlsx"))

    a3_cols <- colnames(a3)
    a3_dup_names <- a3_cols[!a3_cols %in% c("subjid","age_group_name","age_group_id","Age","Sex",'ICU',"COVID PCR","follow_up")]
    setnames(clusters_6m, a3_dup_names, paste0(a3_dup_names,"_6mo"), skip_absent = T)
    clusters_6m$follow_up <- NULL

    a3 <- data.table(merge(a3, clusters_6m, by="subjid"))

    # If symptom criteria is not met at 6m, don't count in 12m either
    a3[post_acute_6mo==0, post_acute := 0]
    a3[cognitive_6mo==0, cognitive := 0]
    a3[res_combine_6mo==0, res_combine := 0]

    a3[fatigue_sub_6mo==0, fatigue_sub := 0]
    a3[res_sub_6mo==0, res_sub := 0]
    a3[cog_sub_6mo==0, cog_sub := 0]
    a3[taste_smell_loss_6mo==0, taste_smell_loss := 0]
    a3[sleep_loss_6mo==0, sleep_loss := 0]
    a3[dizziness_6mo==0, dizziness := 0]
    a3[headache_6mo==0, headache := 0]
    a3[anxiety_depressed_6mo==0, anxiety_depressed := 0]
  }

  # Update long term category
  a3 <- mutate(a3, long_term = ifelse((cognitive %in% 1 | res_combine %in% 1 | post_acute %in% 1), 1, 0 ))

  # If already in long-term group, exclude from new clusters
  a3 <- mutate(a3, fatigue_sub_residual = ifelse(long_term %in% 1 & fatigue_sub %in% 1, 0, fatigue_sub))
  a3 <- mutate(a3, res_sub_residual = ifelse(long_term %in% 1 & res_sub %in% 1, 0, res_sub))
  a3 <- mutate(a3, cog_sub_residual = ifelse(long_term %in% 1 & cog_sub %in% 1, 0, cog_sub))
  a3 <- mutate(a3, taste_smell_loss_residual = ifelse(long_term %in% 1 & taste_smell_loss %in% 1, 0, taste_smell_loss))
  a3 <- mutate(a3, sleep_loss_residual = ifelse(long_term %in% 1 & sleep_loss %in% 1, 0, sleep_loss))
  a3 <- mutate(a3, dizziness_residual = ifelse(long_term %in% 1 & dizziness %in% 1, 0, dizziness))
  a3 <- mutate(a3, headache_residual = ifelse(long_term %in% 1 & headache %in% 1, 0, headache))
  a3 <- mutate(a3, anxiety_depressed_residual = ifelse(long_term %in% 1 & anxiety_depressed %in% 1, 0, anxiety_depressed))

  # Long term with new clusters (exclude long term with old clusters)
  a3 <- mutate(a3, long_term_extended = ifelse(long_term %in% 0 & (fatigue_sub_residual %in% 1 | res_sub_residual %in% 1 |
                                                                     cog_sub_residual %in% 1 | headache_residual %in% 1 |
                                                                     sleep_loss_residual %in% 1 | dizziness_residual %in% 1 |
                                                                     taste_smell_loss_residual %in% 1 |
                                                                     anxiety_depressed_residual %in% 1), 1, 0))
  message(paste("Average long-term-extended: "), mean(a3$long_term_extended))

  # Long term with both old and new clusters
  a3 <- mutate(a3, long_term_all = ifelse(long_term %in% 1 | long_term_extended %in% 1, 1, 0))

  if (typeData == "Ongoing"){
    # Select names that do not match any of the specified patterns
    a3_chosen <- names(a3)[!Reduce(`|`, lapply(c("_6mo$","PCR"), function(pattern) {
      grepl(pattern, names(a3))}))]

    # Remove redundant cols, NA values and save output
    a3 <- a3[, ..a3_chosen]
  }

  DT = as.data.table(a3)
  DT$follow_up <- followup_time
  DT[is.na(DT)] <- 0
  DT <- unique(DT)

  # Merge survey responses into output
  DT2 <- merge(DT, followup_initial, by = "subjid", all.x=T)

  # List of desired columns in specific order
  desired_columns <- c('subjid','flw_sex','Age','age_group_name','age_group_id','vaccination_covid',
                       'post_acute', 'cog_mild', 'cog_moderate', 'cognitive',
                       'res_mild', 'res_moderate', 'res_severe', 'res_combine',
                       'Cog_Fat', 'Cog_Res', 'Res_Fat', 'Cog_Res_Fat', 'fatigue_sub','fatigue_sub_residual',
                       'res_sub','res_sub_residual', 'cog_sub','cog_sub_residual','taste_smell_loss','taste_smell_loss_residual',
                       'sleep_loss','sleep_loss_residual', 'dizziness','dizziness_residual','headache','headache_residual',
                       'anxiety_depressed','anxiety_depressed_residual',
                       'long_term','long_term_extended','long_term_all','follow_up', followup_cols)

  # Filter the list of desired columns based on what actually exists in the dataframe
  existing_columns <- desired_columns[desired_columns %in% colnames(DT)]
  existing_columns2 <- desired_columns[desired_columns %in% colnames(DT2)]

  # Subset the dataframe to only include existing columns in the desired order
  DT <- DT[, ..existing_columns]
  DT2 <- DT2[, ..existing_columns2]

  openxlsx::write.xlsx(DT, paste0(outpath, wave_number,"_",followup_time,"_",typeData,"_all_data_v_", vers, ".xlsx"), rowNames=F)
  openxlsx::write.xlsx(DT2, paste0(outpath, wave_number,"_",followup_time,"_",typeData,"_all_responses_v_", vers, ".xlsx"), rowNames=F)

  # Calculate frequencies based on symptoms, duration, recovery
  bin_vars <- wide_counts2$Variable
  bin_vars_dur <- sub("(_\\d+)?$", "_dur\\1", bin_vars)
  bin_vars_longCovid <- paste0(bin_vars, "_longCovid")
  bin_vars_longCovid_noDur <- paste0(bin_vars, "_longCovid_noDur")
  bin_vars_longCovid_residual <- paste0(bin_vars, "_longCovid_residual")
  bin_vars_longCovid_noDur_residual <- paste0(bin_vars, "_longCovid_noDur_residual")
  bin_vars_longCovid_noDur_residual_ext <- paste0(bin_vars, "_longCovid_noDur_residual_ext")

  df_all <- mod12_fol[,c('subjid','flw_sex','age_group_name','age_group_id','vaccination_covid','fully_recover',bin_vars,bin_vars_dur)]
  DT <- as.data.frame(DT)
  df_all <- merge(df_all, DT[,c('subjid','long_term', 'long_term_extended','long_term_all','follow_up')], by='subjid')

  for (var in bin_vars) {
    var_dur <- sub("(_\\d+)?$", "_dur\\1", var)
    var_longCovid <- paste0(var, "_longCovid")
    var_longCovid_noDur <- paste0(var, "_longCovid_noDur")
    var_longCovid_residual <- paste0(var, "_longCovid_residual")
    var_longCovid_noDur_residual <- paste0(var, "_longCovid_noDur_residual")
    var_longCovid_noDur_residual_ext <- paste0(var, "_longCovid_noDur_residual_ext")

    df_all <- df_all %>%
      mutate(
        !!var_longCovid := ifelse(
          .data[[var]] == 1 & .data[[var_dur]] >= 4 & fully_recover <= 5, 1, 0),
        !!var_longCovid_noDur := ifelse(
          .data[[var]] == 1 & fully_recover <= 5, 1, 0),
        !!var_longCovid_residual := ifelse(
          long_term == 1, 0, .data[[var_longCovid]]),
        !!var_longCovid_noDur_residual := ifelse(
          long_term == 1, 0, .data[[var_longCovid_noDur]]),
        !!var_longCovid_noDur_residual_ext := ifelse(
          long_term_all == 1, 0, .data[[var_longCovid_noDur]])
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
    mutate(any_longCovid_noDur_residual_ext = min(1,max(c_across(matches("_longCovid_noDur_residual_ext$")),0, na.rm=T))) %>%
    ungroup()

  # Remove duration-related columns for this extraction
  df_all2 <- df_all[, colnames(df_all)[!colnames(df_all) %like% "_longCovid$|_longCovid_residual$|_dur"]]
  openxlsx::write.xlsx(df_all2, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_all_symptoms_v_",vers, ".xlsx"),
                       rowNames=F)

  # Export results for long covid residuals
  # Additional symptom clusters to include
  additional_cols <- c('subjid','flw_sex','vaccination_covid',
                       'post_acute', 'cognitive','res_combine',
                       'Cog_Fat', 'Cog_Res', 'Res_Fat', 'Cog_Res_Fat', 'fatigue_sub',
                       'res_sub', 'cog_sub', 'taste_smell_loss', 'sleep_loss', 'dizziness','headache','anxiety_depressed')
  additional_cols2 <- additional_cols[!additional_cols %in% c('subjid','flw_sex','vaccination_covid')]
  select_cols <- union(additional_cols, bin_vars)

  df_all_noDur <- merge(df_all, DT[,c('subjid', additional_cols2)])
  df_all_noDur <- df_all_noDur[, c(select_cols,'fully_recover')]

  # Create a new workbook to store long covid clusters
  wb <- createWorkbook()
  addWorksheet(wb, "0. All Data")
  writeData(wb, "0. All Data", df_all_noDur)

  # Loop through each cluster, subset the data to just patients in those clusters and save them in different sheets
  n <- 1
  for (var in additional_cols2){
    df1 <- df_all_noDur[df_all_noDur[[var]] == 1,]
    df1 <- df1[c("subjid","flw_sex",'vaccination_covid',var, setdiff(names(df1),c("subjid","flw_sex",'vaccination_covid',var)))]

    # Add sheets to the workbook with names and add the data frames to those sheets
    addWorksheet(wb, paste0(n,". ",var))
    writeData(wb, paste0(n,". ",var), df1)
    n = n+1
  }

  # Save the workbook to a file
  saveWorkbook(wb, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_longCovid_clusters_v_",vers, ".xlsx"), overwrite = TRUE)

  # Calculate total patients with long Covid
  df_all_lc <- df_all %>% select(all_of(bin_vars_longCovid))
  df_all_lc <- data.frame(colSums(df_all_lc, na.rm = TRUE))
  colnames(df_all_lc) <- "yes_longCovid"
  df_all_lc$Variable_longCovid <- rownames(df_all_lc)
  rownames(df_all_lc) <- c(1:nrow(df_all_lc))
  df_all_lc <- df_all_lc %>%
    mutate(Variable = str_replace(Variable_longCovid, "_longCovid", ""))

  # Calculate total patients with long Covid, not captured in original clusters
  df_all_lc_residual <- df_all %>% select(all_of(bin_vars_longCovid_residual))
  df_all_lc_residual <- data.frame(colSums(df_all_lc_residual, na.rm = TRUE))
  colnames(df_all_lc_residual) <- "yes_longCovid_residual"
  df_all_lc_residual$Variable_longCovid_residual <- rownames(df_all_lc_residual)
  rownames(df_all_lc_residual) <- c(1:nrow(df_all_lc_residual))
  df_all_lc_residual <- df_all_lc_residual %>%
    mutate(Variable = str_replace(Variable_longCovid_residual, "_longCovid_residual", ""))

  # Calculate total patients with long Covid w/out duration criteria
  df_all_noDur <- df_all %>% select(all_of(bin_vars_longCovid_noDur))
  df_all_noDur <- data.frame(colSums(df_all_noDur, na.rm = TRUE))
  colnames(df_all_noDur) <- "yes_longCovid_noDur"
  df_all_noDur$Variable_longCovid_noDur <- rownames(df_all_noDur)
  rownames(df_all_noDur) <- c(1:nrow(df_all_noDur))
  df_all_noDur <- df_all_noDur %>%
    mutate(Variable = str_replace(Variable_longCovid_noDur, "_longCovid_noDur", ""))

  # Calculate total patients with long Covid w/out duration criteria, not captured in original clusters
  df_all_noDur_residual <- df_all %>% select(all_of(bin_vars_longCovid_noDur_residual))
  df_all_noDur_residual <- data.frame(colSums(df_all_noDur_residual, na.rm = TRUE))
  colnames(df_all_noDur_residual) <- "yes_longCovid_noDur_residual"
  df_all_noDur_residual$Variable_longCovid_noDur_residual <- rownames(df_all_noDur_residual)
  rownames(df_all_noDur_residual) <- c(1:nrow(df_all_noDur_residual))
  df_all_noDur_residual <- df_all_noDur_residual %>%
    mutate(Variable = str_replace(Variable_longCovid_noDur_residual, "_longCovid_noDur_residual", ""))

  # Calculate total patients with long Covid w/out duration criteria, not captured in any clusters (both old and new)
  df_all_noDur_residual_ext <- df_all %>% select(all_of(bin_vars_longCovid_noDur_residual_ext))
  df_all_noDur_residual_ext <- data.frame(colSums(df_all_noDur_residual_ext, na.rm = TRUE))
  colnames(df_all_noDur_residual_ext) <- "yes_longCovid_noDur_residual_ext"
  df_all_noDur_residual_ext$Variable_longCovid_noDur_residual_ext <- rownames(df_all_noDur_residual_ext)
  rownames(df_all_noDur_residual_ext) <- c(1:nrow(df_all_noDur_residual_ext))
  df_all_noDur_residual_ext <- df_all_noDur_residual_ext %>%
    mutate(Variable = str_replace(Variable_longCovid_noDur_residual_ext, "_longCovid_noDur_residual_ext", ""))

  # Calculate "any of the above" row
  any_cols <- c("any_symptoms","any_longCovid","any_longCovid_noDur",
                "any_longCovid_residual","any_longCovid_noDur_residual",'any_longCovid_noDur_residual_ext')
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
  setnames(df_all_any, "any_longCovid_noDur_residual_ext", "yes_longCovid_noDur_residual_ext")

  # Merge stats
  df_all_lc <- merge(df_all_lc, df_all_lc_residual, by="Variable")
  df_all_noDur <- merge(df_all_noDur, df_all_noDur_residual, by="Variable")
  df_all_noDur <- merge(df_all_noDur, df_all_noDur_residual_ext, by="Variable")
  df_both <- merge(df_all_lc, df_all_noDur, by="Variable")
  df_both <- merge(wide_counts2, df_both, by="Variable")
  df_both$Variable_longCovid <- NULL
  df_both$Variable_longCovid_residual <- NULL
  df_both$Variable_longCovid_noDur <- NULL
  df_both$Variable_longCovid_noDur_residual <- NULL
  df_both$Variable_longCovid_noDur_residual_ext <- NULL
  df_both <- rbind(df_both, df_all_any)
  df_both$yes_longCovid <- NULL
  df_both$yes_longCovid_residual <- NULL

  openxlsx::write.xlsx(df_both, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_individual_symptoms_v_",vers, ".xlsx"), rowNames=F)

  # Merge sex and response
  inter_1 <- merge(mod12_fol[,c('subjid','flw_sex','age_group_id','age_group_name','vaccination_covid')],DT)
  inter <- inter_1[,!names(inter_1) %in% c("subjid")]

  data_long <- gather(inter , measure, value, c(post_acute:long_term_all) )

  # Table by sex and age
  cocurr_mu <- data_long %>%
    group_by(age_group_id,age_group_name, flw_sex, measure) %>%
    filter(value=='1') %>%
    dplyr::summarise(var=n()) %>%
    spread(measure, var)

  cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
  cocurr_mu[is.na(cocurr_mu)] <- 0
  cocurr_mu$follow_up <- followup_time
  cocurr_mu <- cocurr_mu[cocurr_mu$age_group_name != "95 plus",]
  cocurr_mu <- cocurr_mu %>% arrange(age_group_id, flw_sex)

  # List of desired columns in specific order
  desired_columns <- c('follow_up','age_group_id','age_group_name','flw_sex','post_acute', 'cog_mild', 'cog_moderate', 'cognitive',
                       'res_mild', 'res_moderate', 'res_severe', 'res_combine',
                       'Cog_Fat', 'Cog_Res', 'Res_Fat', 'Cog_Res_Fat', 'fatigue_sub','fatigue_sub_residual',
                       'res_sub','res_sub_residual', 'cog_sub','cog_sub_residual','taste_smell_loss','taste_smell_loss_residual',
                       'sleep_loss','sleep_loss_residual', 'dizziness','dizziness_residual', 'alopecia','alopecia_residual',
                       'headache','headache_residual','anxiety_depressed','anxiety_depressed_residual',
                       'long_term','long_term_extended','long_term_all','N')

  # Filter the list of desired columns based on what actually exists in the dataframe
  existing_columns <- desired_columns[desired_columns %in% colnames(cocurr_mu)]

  # Subset the dataframe to only include existing columns in the desired order
  cocurr_mu <- cocurr_mu[, existing_columns]
  # openxlsx::write.xlsx(cocurr_mu, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_byAgeSex_", vers, ".xlsx"), rowNames=F)

  # Table by sex only
  cocurr_sex <- data_long %>%
    group_by(flw_sex,measure) %>%
    filter(value=='1') %>%
    summarise(var=n()) %>%
    spread(measure, var)

  cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
  cocurr_sex[is.na(cocurr_sex)] <- 0
  cocurr_sex$follow_up <-followup_time

  # List of desired columns in specific order
  desired_columns <- c('follow_up','flw_sex','post_acute', 'cog_mild', 'cog_moderate', 'cognitive',
                       'res_mild', 'res_moderate', 'res_severe', 'res_combine',
                       'Cog_Fat', 'Cog_Res', 'Res_Fat', 'Cog_Res_Fat', 'fatigue_sub','fatigue_sub_residual',
                       'res_sub','res_sub_residual', 'cog_sub','cog_sub_residual','taste_smell_loss','taste_smell_loss_residual',
                       'sleep_loss','sleep_loss_residual', 'dizziness','dizziness_residual', 'alopecia','alopecia_residual',
                       'headache','headache_residual','anxiety_depressed','anxiety_depressed_residual',
                       'long_term','long_term_extended','long_term_all','N')

  # Filter the list of desired columns based on what actually exists in the dataframe
  existing_columns <- desired_columns[desired_columns %in% colnames(cocurr_sex)]

  # Subset the dataframe to only include existing columns in the desired order
  cocurr_sex <- cocurr_sex[, existing_columns]
  # openxlsx::write.xlsx(cocurr_sex, paste0(outpath ,wave_number,"_",followup_time,"_",typeData, "_bySex_v_", vers, ".xlsx"), rowNames=F)

  # Table by vaccination status only
  cocurr_vac <- data_long %>%
    group_by(vaccination_covid,measure) %>%
    filter(value=='1') %>%
    summarise(var=n()) %>%
    spread(measure, var)

  cocurr_vac <- merge(x=cocurr_vac, y=sample_n_byVac, all.y=TRUE)
  cocurr_vac[is.na(cocurr_vac)] <- 0
  cocurr_vac$follow_up <-followup_time

  # List of desired columns in specific order
  desired_columns <- c('follow_up','vaccination_covid','post_acute', 'cog_mild', 'cog_moderate', 'cognitive',
                       'res_mild', 'res_moderate', 'res_severe', 'res_combine',
                       'Cog_Fat', 'Cog_Res', 'Res_Fat', 'Cog_Res_Fat', 'fatigue_sub','fatigue_sub_residual',
                       'res_sub','res_sub_residual', 'cog_sub','cog_sub_residual','taste_smell_loss','taste_smell_loss_residual',
                       'sleep_loss','sleep_loss_residual', 'dizziness','dizziness_residual', 'alopecia','alopecia_residual',
                       'headache','headache_residual','anxiety_depressed','anxiety_depressed_residual',
                       'long_term','long_term_extended','long_term_all','N')

  # Filter the list of desired columns based on what actually exists in the dataframe
  existing_columns <- desired_columns[desired_columns %in% colnames(cocurr_vac)]

  # Subset the dataframe to only include existing columns in the desired order
  cocurr_vac <- cocurr_vac[, existing_columns]

  # Create a new workbook to save different groups (age-sex, sex, vac status)
  wb1 <- createWorkbook()

  addWorksheet(wb1, "bySex")
  writeData(wb1, "bySex", cocurr_sex)
  addWorksheet(wb1, "byVaccination")
  writeData(wb1, "byVaccination", cocurr_vac)
  addWorksheet(wb1, "byAgeSex")
  writeData(wb1, "byAgeSex", cocurr_mu)

  # Save the workbook to a file
  saveWorkbook(wb1, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_symptoms_byGroups_v_",vers, ".xlsx"), overwrite = TRUE)

}

##########################################
###wave 1, pediatric, initial, 6 months
path1<-'FILEPATH/Part1_Children_1 wave.xlsx'
path2<-'FILEPATH/Parents-Initial.xlsx'
outpath<-"FILEPATH"
wave_number <-'wave_1'
typeData<-"initial"
followup_time <-"6_month"
tabulation(path1, path2, wave_number,outpath, followup_time, typeData )

###########################################
###wave 1, pediatric, ongoing, 12 months
path1<-'FILEPATH/Part1_Children_1 wave.xlsx'
path2<-'FILEPATH/Parents-Ongoing.xlsx'
outpath<-"FILEPATH"
wave_number <-'wave_1'
typeData<-"Ongoing"
followup_time <-"12_month"
tabulation(path1, path2, wave_number,outpath, followup_time, typeData )

###########################################
###wave 2, pediatric, 6_months
path1<-'FILEPATH/Part1_2 wave_Children.xlsx'
path2<-'FILEPATH/Children_2 wave_Parents_Database.xlsx'
outpath<-"FILEPATH"
wave_number <-'wave_2'
typeData<-"_"
followup_time <-"6_month"
tabulation(path1, path2, wave_number,outpath, followup_time, typeData )

###########################################
# Merge symptom freq data
vers <- 2
outpath<-"FILEPATH"
df_grouped_symp1 <- read_excel(paste0(outpath,"wave_1_6_month_initial_individual_symptoms_v_",vers,".xlsx"))
df_grouped_symp2 <- read_excel(paste0(outpath,"wave_1_12_month_Ongoing_individual_symptoms_v_",vers,".xlsx"))
df_grouped_symp3 <- read_excel(paste0(outpath,"wave_2_6_month___individual_symptoms_v_",vers,".xlsx"))

df_grouped_symp1$follow_up <- "delta_wave1_6m"
df_grouped_symp2$follow_up <- "delta_wave1_12m"
df_grouped_symp3$follow_up <- "delta_wave2_12m"

df_grouped_symp <- rbind(df_grouped_symp1,df_grouped_symp2,df_grouped_symp3)
df_grouped_symp <- df_grouped_symp[!df_grouped_symp$Variable %like% "any_",]
openxlsx::write.xlsx(df_grouped_symp, paste0(outpath, "all_individual_symptoms_v_", vers, ".xlsx"),rowNames=F)

# Merge symptom by groups data
sheet_names <- c("bySex","byVaccination","byAgeSex")
wb2 <- createWorkbook()

for (sheet in sheet_names){
  df_bygroups1 <- read_excel(paste0(outpath,"wave_1_6_month_initial_symptoms_byGroups_v_",vers,".xlsx"), sheet=sheet)
  df_bygroups2 <- read_excel(paste0(outpath,"wave_1_12_month_Ongoing_symptoms_byGroups_v_",vers,".xlsx"), sheet=sheet)
  df_bygroups3 <- read_excel(paste0(outpath,"wave_2_6_month___symptoms_byGroups_v_",vers,".xlsx"), sheet=sheet)

  df_bygroups1$follow_up <- "delta_wave1_6m"
  df_bygroups2$follow_up <- "delta_wave1_12m"
  df_bygroups3$follow_up <- "delta_wave2_6m"

  # df_bygroups1$long_term_extended <- NULL
  # df_bygroups2$long_term_extended <- NULL
  # df_bygroups3$long_term_extended <- NULL

  missing_cols <- colnames(df_bygroups3)[!colnames(df_bygroups3) %in% colnames(df_bygroups2)]
  for (col in missing_cols){
    df_bygroups2 <- df_bygroups2 %>%
      mutate(!!col := NA)
  }

  if (sheet == "byVaccination"){
    df_bygroups_all <- rbind(df_bygroups1,df_bygroups2,df_bygroups3)
    df_bygroups_all <- df_bygroups_all[df_bygroups_all$vaccination_covid != 0, ]
    df_bygroups_all <- df_bygroups_all %>%
      mutate(vaccination_meaning = ifelse(vaccination_covid==1, "Yes",ifelse(vaccination_covid==2, "No", "Difficult to answer")))
    df_bygroups_all <- df_bygroups_all[c('follow_up',"vaccination_meaning",'vaccination_covid',
                                         setdiff(names(df_bygroups_all),c('follow_up',"vaccination_meaning",'vaccination_covid')))]
  } else {
    df_bygroups_all <- rbind(df_bygroups1,df_bygroups2,df_bygroups3)
  }

  addWorksheet(wb2, sheet)
  writeData(wb2, sheet, df_bygroups_all)
}

# Save the workbook to a file
saveWorkbook(wb2, paste0(outpath,"all_clusters_byGroups_v_",vers, ".xlsx"), overwrite = TRUE)

###########################################
# Build bar plots by sex
# df_w1_6m_sex <- read_excel(paste0(outpath,"wave_1_6_month_initial_bySex_v_", vers, ".xlsx"))
# df_w1_12m_sex <- read_excel(paste0(outpath, "wave_1_12_month_Ongoing_bySex_v_", vers, ".xlsx"))
# df_w2_6m_sex <- read_excel(paste0(outpath, "wave_2_6_month___bySex_v_", vers, ".xlsx"))

# Reshape the data from wide to long format
# bar_plot <- function(data, file_pattern, arrange=F){
#   plot_names <- setdiff(colnames(data), c("flw_sex","follow_up","N"))
#   data <- data[data$flw_sex != 0,]
#
#   long_df <- data %>%
#     pivot_longer(
#       cols = -c(flw_sex, follow_up, N),  # Deselect columns not used for reshaping
#       names_to = "Variable",
#       values_to = "Value"
#     )
#   long_df$Variable <- factor(long_df$Variable, levels = plot_names)
#   long_df$Sex_label <- ifelse(long_df$flw_sex==1, "Male", "Female")
#   long_df$Sex_label <- paste0(long_df$Sex_label," (N=",long_df$N,")")
#
#   # Reorder 'Variable' based on total Value
#   if (arrange==T){
#     means <- long_df %>%
#       group_by(Variable) %>%
#       summarize(total_value = sum(Value, na.rm = TRUE), .groups = 'drop') %>%
#       arrange(total_value)
#     long_df$Variable <- factor(long_df$Variable, levels = means$Variable)
#   }
#
#   # Create the bar plot
#   my_plot <-
#     ggplot(long_df, aes(x = Value, y = Variable, fill = factor(Sex_label))) +
#     geom_bar(stat = "summary", fun = "mean", position = "dodge") +
#     geom_text(aes(label = ..x..), stat = "summary", fun = "mean",
#               position = position_dodge(width = 0.9), hjust = -0.5, size = 3, color = "black") +
#     theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1, size=14),
#           axis.text.y = element_text(size=20)) +
#     labs(x = "Count", y = "", fill = "Sex") +
#     theme_minimal() +
#     scale_fill_brewer(palette = "Set1")
#
#   ggsave(paste0(outpath, file_pattern, vers,"_barplot.png"),
#          my_plot, width = 10, height = 8, units = "in", bg = "white")
# }
#
# bar_plot(df_w1_6m_sex,"wave_1_6_month_initial_bySex_v_", arrange=T)
# bar_plot(df_w1_12m_sex,"wave_1_12_month_Ongoing_bySex_v_", arrange=T)
# bar_plot(df_w2_6m_sex,"wave_2_6_month___bySex_v_", arrange=T)

