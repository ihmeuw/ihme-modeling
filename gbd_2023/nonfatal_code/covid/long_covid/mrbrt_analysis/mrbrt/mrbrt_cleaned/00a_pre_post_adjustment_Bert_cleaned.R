## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Prepare for Mr Bert Run
## Date 3/8/2021
## --------------------------------------------------------------------- ----
## stamp: adding germany data and UW data (3/22/2021)
## stamp: adding the full long term cohort data (4/8/2021)

## Environment Prep ---------------------------------------------------- ----
#rm(list=ls(all.names=T))

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
version <- 5

# source(paste0(.repo_base, 'FILEPATH/utils.R'))
# source(paste0("ROOT" ,"FILEPATH/get_location_metadata.R"))
# source(paste0("ROOT" ,"FILEPATH/get_age_metadata.R"))
library(openxlsx)
library(crosswalk, lib.loc = "FILEPATH")
library(crosswalk002, lib.loc = "FILEPATH")
library(dplyr)
library(gtools)
library(readxl)
setwd("FILEPATH")


#set.seed(123)
# data for the meta-regression
# -- in a real analysis, you'd get this dataset by 
#    creating matched pairs of alternative/reference observations,
#    then using delta_transform() and calculate_diff() to get
#    log(alt)-log(ref) or logit(alt)-logit(ref) as your dependent variable

proportion <- read.csv('proportion_v1.csv')

proportion <- subset(proportion, select=-c(X))
#take out zurich with 6 months followup
proportion <- proportion[!is.na(proportion$measurement) & !proportion$measurement %in% 0 & !proportion$follow_up_time %in% '6-months',]


#getwd()
#4/14/2021 read in new data
#setwd('FILEPATH')
meta <- read_excel("FILEPATH/long_covid_extraction_04.14.2021.xlsx", sheet='extraction')
#3/22/2021 old data
#data <- read_excel("FILEPATH/unadjusted_data_without_pre_post_COVID_v1.xlsx")

#table(data$symptom_cluster)
alldata <- mutate(meta, Group = ifelse((study_id %in% c('CO-FLOW','HAARVI','Helbok et al','pa-COVID','Sweden PronMed ICU')|
                                          study_id %in% 'Zurich cohort' & follow_up_value %in% 6), 'measurement_wo_pre', 'reference'))
#separates reference and those needs adjustment
reference <- alldata[alldata$Group %in% 'reference', ]
data <- alldata[alldata$Group %in% 'measurement_wo_pre',]

data$cases <- as.numeric(data$cases)
data$prev <- data$mean
data$prev_se <- sqrt((data$prev * (data$prev)) / data$sample_size)
#in the 4/8 new datasets, lots of the condition names were changed. So that we lost lots of observations because 
#the name did not match. 

data$condition <- NA
#changing the values 
data$condition[which(data$symptom_cluster == "any symptom cluster")] <- 'long_term'

data$condition[which(data$symptom_cluster == "post-acute fatigue syndrome")] <- 'post_acute'

data$condition[which(data$symptom_cluster == "cognitive")] <- 'cognitive'
data$condition[which(data$symptom_cluster == "mild cognitive among cognitive")] <- 'cog_mild'
data$condition[which(data$symptom_cluster == "moderate cognitive among cognitive")] <- 'cog_moderate'

data$condition[which(data$symptom_cluster == "respiratory")] <- 'res_combine'
data$condition[which(data$symptom_cluster == "mild respiratory among respiratory")] <- 'res_mild'
data$condition[which(data$symptom_cluster == "moderate respiratory among respiratory")] <- 'res_moderate'
data$condition[which(data$symptom_cluster == "severe respiratory among respiratory")] <- 'res_severe'

data$condition[which(data$symptom_cluster == "post-acute fatigue and respiratory")] <- 'Res_Fat'
data$condition[which(data$symptom_cluster == "post-acute fatigue and cognitive")] <- 'Cog_Fat'
data$condition[which(data$symptom_cluster == "respiratory and cognitive")] <- 'Cog_Res'
data$condition[which(data$symptom_cluster == "post-acute fatigue and respiratory and cognitive")] <- 'Cog_Res_Fat'



# input <-data
# variable <-'cognitive'
#table(data$condition)
adjustment_function <-function(input, variable){
  #variable intake variables like cog, cognitive
  #condition is used as the varaible name
  # #calculate the standard deviation around a single data point using 
  # #ADDRESS
  # # the sample distribution of the sample proportion
  # 
  # df_original$sd <- sqrt((df_original$measurement * (df_original$measurement)) / df_original$N)
  # df_original$prev <- df_original$measurement
  # df_original$prev_se <- df_original$sd/ sqrt(df_original$N)
  df_adjusted<- input[(input$condition %in% variable),]
  
  df_original<- proportion[(proportion$condition %in% variable & (proportion$Sex %in% 'Both') &
                              proportion$source %in% c("Iran","Russia","Zurich")),]
  #calculate the standard deviation around a single data point using 
  #ADDRESS
  # the sample distribution of the sample proportion
  
  df_original$prev_se <- sqrt((df_original$measurement * (df_original$measurement)) / df_original$N)
  df_original$prev <- df_original$measurement
  
  
  method_var <- "Group"
  gold_def <- "measurement_pre"
  keep_vars <- c('condition',"Group","prev", "prev_se")
  
  df_matched <- do.call("rbind", lapply(unique(df_original$condition), function(i) {
    dat_i <- filter(df_original, condition == i) %>% mutate(dorm = get(method_var))
    keep_vars <- c("dorm", keep_vars)
    row_ids <- expand.grid(idx1 = 1:nrow(dat_i), idx2 = 1:nrow(dat_i))
    do.call("rbind", lapply(1:nrow(row_ids), function(j) {
      dat_j <- row_ids[j, ] 
      dat_j[, paste0(keep_vars, "_alt")] <- dat_i[dat_j$idx1, keep_vars]
      dat_j[, paste0(keep_vars, "_ref")] <- dat_i[dat_j$idx2, keep_vars]
      filter(dat_j, dorm_alt != gold_def & dorm_alt != dorm_ref)
    })) %>% mutate(id = i) %>% select(-idx1, -idx2)
  }))
  
  
  dat_diff <- as.data.frame(cbind(
    delta_transform(
      mean = df_matched$prev_alt, 
      sd = df_matched$prev_se_alt,
      transformation = "linear_to_logit" ),
    delta_transform(
      mean = df_matched$prev_ref, 
      sd = df_matched$prev_se_ref,
      transformation = "linear_to_logit")
  ))
  names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")
  
  df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
    df = dat_diff, 
    alt_mean = "mean_alt", alt_sd = "mean_se_alt",
    ref_mean = "mean_ref", ref_sd = "mean_se_ref" )
  
  df_matched$id2 <- as.integer(as.factor(df_matched$id)) # ...housekeeping
  
  # format data for meta-regression; pass in data.frame and variable names
  dat1 <- CWData(
    df = df_matched,
    obs = "logit_diff",       # matched differences in logit space
    obs_se = "logit_diff_se", # SE of matched differences in logit space
    alt_dorms = "dorm_alt",   # var for the alternative def/method
    ref_dorms = "dorm_ref",   # var for the reference def/method
    covs = list('condition_ref'),       # list of (potential) covariate columns
    study_id = "id2"          # var for random intercepts; i.e. (1|study_id)
  )
  
  fit1 <- CWModel(
    cwdata = dat1,           # result of CWData() function call
    obs_type = "diff_logit", # must be "diff_logit" or "diff_log"
    cov_models = list( # specify covariate details
      CovModel("intercept")),
    gold_dorm = "measurement_pre"  # level of 'ref_dorms' that's the gold standard
  )
  
  names(df_adjusted)[names(df_adjusted) == "condition"] <- "condition_ref"
  
  #df_adjusted<- df_adjusted[df_adjusted$prev>0 , ]
  df_subset<- df_adjusted[df_adjusted$prev>0 & !is.na(df_adjusted$mean) &df_adjusted$prev<1 &df_adjusted$Group!='reference', ]
  
  dropped_rows <- anti_join(df_adjusted, df_subset)
  
  df_subset[, c("mean_adjusted", "se_adjusted", "diff", "diff_se","data_id")] <- adjust_orig_vals(
    fit_object = fit1, 
    df = df_subset, 
    orig_dorms = "Group", 
    orig_vals_mean = "prev", 
    orig_vals_se = "prev_se" )
  
  dropped_rows[c("se_adjusted","diff","diff_se","data_id")]<-NA
  
  df_adjusted<- rbind(df_subset, dropped_rows)  
  
  return(df_adjusted)
}
# table(data$condition)
# unique(proportion$condition)
cognitive <- adjustment_function(data, variable ='cognitive')
#cog_mild <-adjustment_function(data,variable ='cog_mild')
#too many 1s, it has an assertation that the mean has to be less than 1
Cog_Fat <- adjustment_function(data,variable ='Cog_Fat')
long_term<-adjustment_function(data,variable ='long_term')
post_acute<-adjustment_function(data, variable ='post_acute')
res_combine<-adjustment_function(data,variable ='res_combine')
Res_Fat<-adjustment_function(data,variable ='Res_Fat')
Cog_Res <-adjustment_function(data, variable ='Cog_Res')
#cog_mild
cog_moderate<- adjustment_function(data,variable ='cog_moderate')
Cog_Res_Fat<- adjustment_function(data,variable ='Cog_Res_Fat')
res_mild<- adjustment_function(data,variable ='res_mild')
#too many 1s, it has an assertation that the mean has to be less than 1
#res_moderate<- adjustment_function(data,variable ='res_moderate')
res_severe<- adjustment_function(data,variable ='res_severe')

#need to handle cog_mild specially because the adjustment was not done on it. 
#cog_mild <- data[data$condition %in% c('cog_mild','res_moderate'),]

final_adjusted <- rbind(cognitive, Cog_Fat, long_term, post_acute, res_combine, Res_Fat, Cog_Res, cog_moderate, Cog_Res_Fat, res_severe,res_mild)

# table(final_adjusted$condition_ref)
# table(data$condition)

#cog_mild and res_moderate
special <- data[data$condition %in% c('cog_mild', 'res_moderate'),]
special[c("se_adjusted","diff","diff_se","data_id")]<-NA
reference[c("prev","prev_se","se_adjusted","diff","diff_se","data_id")]<-NA
names(final_adjusted)[names(final_adjusted) == "condition_ref"] <- "condition"
final_adjusted$condition_ref <- NULL
special$condition <- NULL

final_all <-rbind(final_adjusted, special, reference)
#table(final_adjusted$condition_ref)



final_all$adjusted_ratio <- final_all$mean_adjusted/final_adjusted$mean
output_path <- 'FILEPATH'
#output_path <- 'FILEPATH'
write.csv(final_all, paste0(output_path, "adjusted_long_covid_data_", version, ".csv"))




# df_total = data.frame()
# for (i in unique(proportion$condition)){
#   i <- adjustment_function(data, variable =as.character(i) )
#   df <- data.frame(i)
#   df_total <- rbind(df_total,df)
# }
# 
# proportion$conditon
# 
# unique(proportion$conditon)

