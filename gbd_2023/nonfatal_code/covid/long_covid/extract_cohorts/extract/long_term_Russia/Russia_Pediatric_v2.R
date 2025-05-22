## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script:
## Description: Russian Data recoded_using 1000+ data Pediatric data
## Contributors: NAME, NAME
## Date 1/20/2022
## December 2023 extracting pediatric Omicron follow-up for GBD 2022
## --------------------------------------------------------------------- ----

library(readxl)
library(data.table)
## Environment Prep ---------------------------------------------------- ----
#remove the history and call the GBD functions
rm(list=ls(all.names=T))

vers <- 1

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k ,"FILEPATH/get_location_metadata.R"))
source(paste0(roots$k ,"FILEPATH/get_age_metadata.R"))



tabulation <- function(path1, path2, wave_number, outpath, followup_time, typeData){

  #the pediatric data has wave 1 and wave 2
  #need to combine the two together
  #only 1 wave now, deleting related functions
  #added "Age" to concatenate function for mod_wv1
  if(wave_number=="wave_1"){
    mod_wv1 <-
      read_excel(path1)[,c("subjid","date_of_birth")]
    mod_wv1$DOB <- ymd(mod_wv1$`date_of_birth`)
    mod_wv1$today <- ymd(Sys.Date())
    mod_wv1$Age <- interval(start= mod_wv1$DOB, end=mod_wv1$today)/
      duration(n=1, unit="years")
    message(paste("Average age: "), mean(mod_wv1$Age))
    mod_wv1<- mod_wv1[,c("subjid", "Age")]
  }
  else{
    mod_wv1 <-
      read_excel(path1)
  }
  if (typeData== "initial")
  {
    followup_initial <- read_excel(path2)[,c("subjid", "flw_sex","flw_confusion",
                                             "flw_confusion_2",'flw_fat','vas_fatigue','vas_fatigue_change',
                                             "flw_breathless","flw_breathless_2")]
  }


  mod12_fol<-merge(mod_wv1, followup_initial)
  #getting the number of patients by sex
  sample_n_bysex <-mod12_fol %>%
    group_by(flw_sex) %>%
    summarise(N=n())


  post <- mod12_fol[,c("subjid","flw_fat",'vas_fatigue','vas_fatigue_change')]
  post <- mutate(post, post_acute = ifelse((flw_fat %in% 1) & (vas_fatigue %in% c(4,5)) & (vas_fatigue_change %in% c(1,3)), 1,0))

  message(paste("Average fatigue: "), mean(post$post_acute))

  #•	cognition problems
  cog <- mod12_fol[,c("subjid","flw_confusion","flw_confusion_2")]

  # o	lay description for mild dementia: “has some trouble remembering recent events,
  # and finds it hard to concentrate and make decisions and plans”
  # removing anything related to wave 2 or 12 mo f/u
  cog$na_count <- apply(cog, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  cog <- cog[cog$na_count <=1,]


  #cognitive problem
  cog <- mutate(cog, cognitive = ifelse((flw_confusion %in% 1 | flw_confusion_2 %in% 1),1,0))
  message(paste("Average cognitive: "), mean(cog$cognitive))

  # •	respiratory problems
  # o	lay description for mild COPD: “has cough and shortness of breath after heavy physical activity,
  # but is able to walk long distances and climb stairs”
  # mild
  res <- mod12_fol[, c("subjid",'flw_breathless', 'flw_breathless_2')]
  res$na_count <- apply(res, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  res <- res[res$na_count <=1,]

  res <- mutate(res, res_combine = ifelse((flw_breathless %in% 1 | flw_breathless_2 %in% 1), 1, 0))
  message(paste("Average res: "), mean(res$res_combine))

  #drop the res severity because not going to use it, the phd stress and anxiety are missing
  a1 <- post[,c('subjid','post_acute')]
  a2 <- merge(a1, res[, c('subjid',"res_combine")])
  a3 <- merge(a2,cog[, c('subjid','cognitive')])

  #combine the resp, cognitive and fatigue categories
  a3 <- mutate(a3, Cog_Res = ifelse((cognitive %in% 1 & res_combine %in% 1), 1, 0 ))
  a3 <- mutate(a3, Cog_Fat = ifelse((cognitive %in% 1 & post_acute %in% 1), 1, 0 ))
  a3 <- mutate(a3, Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1), 1, 0 ))
  a3 <- mutate(a3, Cog_Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1
                                         & cognitive %in% 1), 1, 0 ))

  #all long term category
  a3 <- mutate(a3, long_term = ifelse((cognitive %in% 1 | res_combine %in% 1 | post_acute %in% 1), 1, 0 ))
  message(paste("Average long-term: "), mean(a3$long_term))

  DT = as.data.table(a3)
  DT$follow_up <- followup_time
  write.csv(DT, paste0(outpath, "_all_data_",wave_number,"_",followup_time,"_",typeData,"_", vers, ".csv"))

  inter_1<- merge(mod12_fol[,c('subjid','flw_sex')],DT)
  inter<- inter_1[,!names(inter_1) %in% c("subjid")]

  data_long <- gather(inter , measure, value, c(post_acute:long_term) )

  #table by sex only
  cocurr_sex <-data_long %>%
    group_by(flw_sex,measure) %>%
    filter(value=='1') %>%
    summarise(var=n()) %>%
    spread(measure, var)

  cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
  cocurr_sex[is.na(cocurr_sex)] <- 0
  cocurr_sex$follow_up <-followup_time
  write.csv(cocurr_sex, paste0(outpath ,wave_number,"_",followup_time,"_",typeData, "_bySex_v_", vers, ".csv"))
}

##########################################
###wave 1, pediatric, initial, 6 months
## changed path2 as original file had Russian characters; created new file removing all non-English characters
path1<-'FILEPATH/Omicron base RC Full 4.0 (Acute).xlsx'
path2<-'FILEPATH/Omicron follow-up 6 m_V2.xlsx'
outpath<-"FILEPATH"
wave_number <-'wave_1'
typeData<-"initial"
followup_time <-"6_month"
tabulation(path1, path2, wave_number,outpath, followup_time, typeData )

###########################################
###deleted wave 1, pediatric, ongoing, 12 months & wave 2, pediatric, 12_months


