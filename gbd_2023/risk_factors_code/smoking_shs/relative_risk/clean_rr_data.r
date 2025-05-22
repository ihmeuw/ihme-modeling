#Purporse: Clean extracted SHS relative risk data for meta-analysis (GBD 2023) 

#------------------Set-up--------------------------------------------------
rm(list=ls())

### runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

home_dir <- "FILEPATH"
setwd(home_dir)

today <- Sys.Date()
'%ni%' <- Negate('%in%')
'%notlike%' <- Negate('%like%')

# ------------------Library------------------------------------------------
library(ggplot2)
library(data.table)
library(openxlsx)
library(pbapply)
library(dplyr)
library(stringr) 
library(knitr)
library(kableExtra)
library(rmarkdown)
library(magick)
library(webshot)
library(ggrepel)
library(tools)
library(R.utils)
library(stringr)

gbd_round <- "gbd2023"
shs_unit <- "binary"

#Extractions folder
in_dir <- paste0("FILEPATH")
out_dir <- paste0(home_dir, "FILEPATH")
version <- paste0(gbd_round,"_",today)

##List files in extracted folder
list <- list.files(in_dir)
list <- list[grep("xlsx", list)]

##Create dataframe
data <- NULL
for (i in list){
  print(i)
  df <- as.data.table(readxl::read_xlsx(paste0(in_dir, i), sheet = "extraction"))
  df <- df[-c(1),] #Remove description row
  df <- df[!is.na(nid)]
  data <- rbind(data, df, fill = T)
}
rm(df)

##Make columns numeric
data$n <- 1:nrow(data)
changeCols <- c("nid", "location_id", "rep_geography", "rep_population", "rep_prevalent_disease", "year_start", "year_end", "year_issue", 
                "age_start", "age_end", "age_issue", "age_mean", "age_sd","age_demographer", "percent_male", "sex_issue", "value_of_duration_fup", 
                "most_adj_model", "least_adj_model", "exp_level_lower", "exp_level_upper", "unexp_level_lower", "unexp_level_upper", "rep_children",
                "lower", "upper", "mean", "standard_error", "uncertainty_issue", "effect_size_derived", "effect_size_from_image", "subgroup_analysis", "value_of_duration_fup",
                "effect_size_multi_location", "pooled_cohort", "dose_response", "cohort_person_years_exp", "cohort_person_years_unexp", "cohort_person_years_total",
                "cohort_number_events_exp", "cohort_number_events_unexp", "cohort_dropout_rate", "cc_community", "cc_cases_exp", "cc_controls_exp",
                "cc_cases_unexp", "cc_controls_unexp","cc_cases_total", "cc_controls_total", "cc_dropout_rate", "included_in_model", "is_outlier")

data[,(changeCols):= lapply(.SD, as.numeric), .SDcols = changeCols] 

confCols <- (names(data)[names(data) %like% 'confounders' & names(data) != "confounders_other"])
data[,(confCols):= lapply(.SD, as.numeric), .SDcols = confCols] 

##Remove columns for which all rows == NA. 
data <- data[,which(unlist(lapply(data, function(x)!all(is.na(x))))),with=F] 

#---------------------------------------------------------------------#
#-------------------------Checking outcomes---------------------------#
#---------------------------------------------------------------------#

unique(data$acause)
data[,cause := acause]
data[cause %like% "stroke", cause := "stroke"]
data[cause %like% "asthma", cause := "asthma"]
data[cause %like% "ihd", cause := "ihd"]
data[cause %like% "copd", cause := "copd"]
data[cause %like% "diabetes", cause := "dm2"]
unique(data$cause)

#---------------------------------------------------------------------#
#--------------------Checking study types-----------------------------#
#---------------------------------------------------------------------#

#Study types to be included: Prospective cohort studies, case-control, case-cohort, case-crossover, and nested case-control 

table(data$design)

##Creating study type var
data <- data[, study_type := ifelse(unit_clean %in% c("binary"), "binary", "cont")]

#---------------------------------------------------------------------#
#--------------------Checking date of publication---------------------#
#---------------------------------------------------------------------#

min(data$year_start)
unique(data[year_start <= 1970,]$field_citation_value)

#---------------------------------------------------------------------#
#--------------------Checking population exposed----------------------#
#---------------------------------------------------------------------#

unique(data$shs_exp_smoking_status)

#For studies with unknown smoking status, assume never-smokers if study with children < 15 only
data <- data[shs_exp_smoking_status == "unknown" & age_end < 15, shs_exp_smoking_status := "adj never smokers"]
data <- data[shs_exp_smoking_status == "any smoking status" & age_end < 15, shs_exp_smoking_status := "adj never smokers"]

#Population exposed: non-smoker vs undefined
unique(data$shs_exp_smoking_status)
data <- data[, exp_group := "nonsmoker"] #non, never, former
data <- data[(shs_exp_smoking_status == "any smoking status" | shs_exp_smoking_status == "unknown"), exp_group := "any"]

def_map <- c("nonsmoker","any")
table(data$exp_group, useNA = "ifany")

#Picking preferred definitions and removing extra rows from the same study 
data[, pref_group:=0]

for (t in unique(data$study_type)){
  for (c in unique(data$cause)){
    for (id in unique(data$nid)) {
      for (map in def_map) {
        if (map %in% unique(data[study_type == t & cause == c & nid==id]$exp_group)) {
          data[study_type == t & cause == c & nid==id & exp_group==map, pref_group:=1]
          print(paste0('Best source exposed group is ', map, ' for ',c, ' nid ', id, ' and Marked with pref_group==1'))
          break()
        } else {
          print(paste0(map, ' not found for ',t, ' cause ',c,' nid ', id, ', moving to next preferred'))
        }
      }
    }  
  }
}

table(data$pref_group)

data<-data[pref_group ==1,]

#---------------------------------------------------------------------#
#--------------------Checking timing of exposure----------------------#
#---------------------------------------------------------------------#

unique(data$shs_exp_temporality)
 
##Timing of exposure: current > ever > former
def_map <- c("current","adulthood","ever")
 
#Picking preferred definitions and removing extra rows from the same study 
 data[, pref_group:=0]
 
 for (t in unique(data$study_type)){
   for (c in unique(data$cause)){
     for (id in unique(data$nid)) {
       for (map in def_map) {
         if (map %in% unique(data[study_type == t & cause == c & nid==id]$shs_exp_temporality)) {
           data[study_type == t & cause == c & nid==id & shs_exp_temporality==map, pref_group:=1]
           print(paste0('Best source temporality is ', map, ' for ',c, ' nid ', id, ' and Marked with pref_group==1'))
           break()
         } else {
           print(paste0(map, ' not found for ',t, ' cause ',c,' nid ', id, ', moving to next preferred'))
         }
       }
     }  
   }
 }
 
 
 table(data$pref_group)

#---------------------------------------------------------------------#
#--------------------Checking location of exposure--------------------#
#---------------------------------------------------------------------#

table(data$shs_exp_location, useNA = "ifany")
unique(data[shs_exp_location == "other"]$shs_exp_location_desc)
unique(data$shs_exp_source)

##Move all parental exposure to home
table(data[shs_exp_source %in% c("maternal", "paternal", "parental", "maternal or paternal", "parental-one", "parental-both", "spouse", "spouse only", "maternal only", "maternal only", "family (anyone in household)", "other family (not spouse)", "maternal and paternal")]$shs_exp_location)
data <- data[shs_exp_source %in% c("maternal", "paternal", "parental", "maternal or paternal", "parental-one", "parental-both", "spouse", "spouse only", "maternal only", "maternal only", "family (anyone in household)", "other family (not spouse)", "maternal and paternal") & shs_exp_location == 'other', shs_exp_location := "home"]

## 1) home or work, home, work; 2) aggregated with home or work; 3) specific locations not within umbrella definition: remove
## For studies where home OR work or multi-OR are reported, as well as home, work and any other separately, we preserve the aggregated row
unique(data$shs_exp_location)

data <- data[shs_exp_location %in% c("home", "home or work", "work", "home and work"), exp_setting := "exact"]
unique(data[shs_exp_location == "other"]$shs_exp_location_desc)
data <- data[shs_exp_location == "other" & shs_exp_location_desc == "cars", exp_setting := "other"]
data <- data[shs_exp_location == "other" & shs_exp_location_desc == "not home", exp_setting := "other"]
data <- data[shs_exp_location == "other" & shs_exp_location_desc %like% "Exposed elsewhere", exp_setting := "other"]
data <- data[is.na(exp_setting), exp_setting := "aggregated"]

def_map <- c("exact","aggregated")
table(data$exp_setting, useNA = "ifany")

#Picking preferred definitions and removing extra rows from the same study 
data[, pref_group:=0]

for (t in unique(data$study_type)){
  for (c in unique(data$cause)){
    for (id in unique(data$nid)) {
      for (map in def_map) {
        if (map %in% unique(data[study_type == t & cause == c & nid==id]$exp_setting)) {
          data[study_type == t & cause == c & nid==id & exp_setting==map, pref_group:=1]
          print(paste0('Best exposure setting is ', map, ' for ',c, ' nid ', id, ' and Marked with pref_group==1'))
          break()
        } else {
          print(paste0(map, ' not found for ',t, ' cause ',c,' nid ', id, ', moving to next preferred'))
        }
      }
    }  
  }
}


##For studies with multiple exposure settings, keep preferred ones:
table(data$pref_group)

data<-data[pref_group == 1]

##Second round of cleaning. For studies with both "home or work" and separate home/work, select aggregated row
data <- data[shs_exp_location %in% c("home or work"), exp_setting2 := "exact"]
data <- data[is.na(exp_setting2), exp_setting2 := "other"]
table(data$exp_setting2, useNA = "ifany")

def_map <- c("exact","other")

#Picking preferred definitions and removing extra rows from the same study 
data[, pref_group:=0]

for (t in unique(data$study_type)){
  for (c in unique(data$cause)){
    for (id in unique(data$nid)) {
      for (map in def_map) {
        if (map %in% unique(data[study_type == t & cause == c & nid==id]$exp_setting2)) {
          data[study_type == t & cause == c & nid==id & exp_setting2==map, pref_group:=1]
          print(paste0('Best exposure setting is ', map, ' for ',c, ' nid ', id, ' and Marked with pref_group==1'))
          break()
        } else {
          print(paste0(map, ' not found for ',t, ' cause ',c,' nid ', id, ', moving to next preferred'))
        }
      }
    }  
  }
}


data <- data[shs_exp_location %in% c("home and work"), pref_group := 1]

##For studies with multiple exposure settings, keep preferred ones:
table(data$pref_group)

data<- data[pref_group ==1]

#---------------------------------------------------------------------#
#--------------------Checking source of exposure----------------------#
#---------------------------------------------------------------------#

table(data$shs_exp_source, useNA = "ifany")

## Any source of exposure works. General sources are preferred than exposed to smoke from specific people (at home, any family > specific family member)
data$exp_source <-NULL
data <- data[, exp_source := shs_exp_source]
data <- data[shs_exp_source %in% c("maternal", "paternal", "maternal and paternal","spouse", "other family (not spouse)", "maternal only", "paternal only", "spouse only", "parental-one", "parental-both"), exp_source := "specific"]
## Preference, from broader to specific source of exposure
table(data$exp_source, useNA = "ifany")
def_map <- c("any","family (anyone in household)", "parental", "specific")

data[, pref_group:=0]

for (t in unique(data$study_type)){
  for (c in unique(data$cause)){
    for (id in unique(data$nid)) {
      for (set in unique(data$shs_exp_location)){
        for (map in def_map) {
          if (map %in% unique(data[study_type == t & cause == c & nid==id & shs_exp_location == set]$exp_source)) {
            data[study_type == t & cause == c & nid==id & shs_exp_location == set & exp_source==map, pref_group:=1]
            print(paste0('Best exposure source is ', map, ' for ',c, ' nid ', id, ' at ', set, ' and Marked with pref_group==1'))
            break()
          } else {
            print(paste0(map, ' not found for ',t, ' cause ',c,' nid ', id, ', moving to next preferred'))
          }
        }
      }  
    }
  }
}


table(data$pref_group)
table(data[pref_group == 1]$exp_source)

data <- data[pref_group ==1]

#---------------------------------------------------------------------#
#--------------------Checking Stroke subcauses------------------------#
#---------------------------------------------------------------------#

##Prefered outcome = Stroke (overall) 
data <- data[acause == "cvd_stroke",stroke_agg := "stroke"]
data <- data[acause == "cvd_stroke_subhem",stroke_agg := "sub"]
data <- data[acause == "cvd_stroke_cerhem",stroke_agg := "sub"]
data <- data[acause == "cvd_stroke_isch",stroke_agg := "sub"]

data[, pref_group:=0]

def_map <- c("stroke", "sub")

for (t in unique(data$study_type)){
  for (c in unique(data[cause=='stroke']$cause)){
    for (id in unique(data$nid)) {
      for (map in def_map) {
        if (map %in% unique(data[study_type == t & cause == c & nid==id]$stroke_agg)) {
          data[study_type == t & cause == c & nid==id & stroke_agg==map, pref_group:=1]
          print(paste0('Best stroke cause is ', map, ' for ',c, ' nid ', id, ' and Marked with pref_group==1'))
          break()
        } else {
          print(paste0(map, ' not found for ',t, ' cause ',c,' nid ', id, ', moving to next preferred'))
        }
      }
    }  
  }
}

##Since doing this for stroke only, mark all other causes to keep (pref_group == 1)
data<-data[cause != 'stroke', pref_group :=1]
table(data$pref_group)

data<-data[pref_group ==1]

#---------------------------------------------------------------------#
#--------------------Checking outcome type----------------------------#
#---------------------------------------------------------------------#

unique(data$outcome_type)

##For now, we apply the same risk for mortality and incidence. Thus, combined row is preferred. 

data <- data[, out_type_agg := ifelse(outcome_type == "Incidence & Mortality", "overall", "specific")]

data[, pref_group:=0]

def_map <- c("overall", "specific")

for (t in unique(data$study_type)){
  for (c in unique(data$cause)){
    for (id in unique(data$nid)) {
      for (map in def_map) {
        if (map %in% unique(data[study_type == t & cause == c & nid==id]$out_type_agg)) {
          data[study_type == t & cause == c & nid==id & out_type_agg==map, pref_group:=1]
          print(paste0('Pref outcome_type is ', map, ' for ',c, ' nid ', id, ' and Marked with pref_group==1'))
          break()
        } else {
          print(paste0(map, ' not found for ',t, ' cause ',c,' nid ', id, ', moving to next preferred'))
        }
      }
    }  
  }
}

#Removing extra rows 
table(data$pref_group)

data<-data[pref_group ==1]

#---------------------------------------------------------------------#
#--------------------Checking reported sex----------------------------#
#---------------------------------------------------------------------#

##Check additional row for sex (both >= other)
data <- data[, pref_sex := sex]
data <- data[pref_sex != "Both", pref_sex:= "Other"]

data[, pref_group:=0]

def_map <- c("Both","Other")

for (t in unique(data$study_type)){
  for (c in unique(data$cause)){
    for (id in unique(data$nid)) {
      for (map in def_map) {
        if (map %in% unique(data[study_type == t & cause == c & nid==id]$pref_sex)) {
          data[study_type == t & cause == c & nid==id & pref_sex==map, pref_group:=1]
          print(paste0('Pref sex is ', map, ' for ',c, ' nid ', id, ' and Marked with pref_group==1'))
          break()
        } else {
          print(paste0(map, ' not found for ',t, ' cause ',c,' nid ', id, ', moving to next preferred'))
        }
      }
    }  
  }
}

#Removing sex=specific rows when both sex is available
table(data$pref_group)

data <- data[pref_group == 1]

#---------------------------------------------------------------------#
#--------------------Checking models adjustment-----------------------#
#---------------------------------------------------------------------#

##Select most adjusted model for each nid, sex, sub_group_analysis,   
data[, pref_group:=0]

def_map <- c(1,0)

for (t in unique(data$study_type)){
  for (c in unique(data$cause)){
    for (id in unique(data$nid)) {
      for (s in unique(data$sex)) {
        for (sub in unique(data$subgroup_analysis)){
          for (map in def_map) {
            if (map %in% unique(data[study_type == t & cause == c & nid==id & sex==s & subgroup_analysis == sub]$most_adj_model)) {
              data[study_type == t & cause == c & nid==id & sex==s & subgroup_analysis == sub & most_adj_model==map, pref_group:=1]
              print(paste0('Model Pref is ', map, ' for ',c, ' nid ', id, ' and Marked with pref_group==1'))
              break()
            } else {
              print(paste0(map, ' not found for ',c,' nid ', id, ', moving to next preferred'))
            }
          }
        }
      }
    }
  }
}

table(data$pref_group)

data<- data[pref_group ==1]

##Prefer not least adjusted models 

data[, pref_group:=0]

def_map <- c(0,1)

for (t in unique(data$study_type)){
  for (c in unique(data$cause)){
    for (id in unique(data$nid)) {
      for (s in unique(data$sex)) {
        for (sub in unique(data$subgroup_analysis)){
          for (map in def_map) {
            if (map %in% unique(data[study_type == t & cause == c & nid==id & sex==s & subgroup_analysis == sub]$least_adj_model)) {
              data[study_type == t & cause == c & nid==id & sex==s & subgroup_analysis == sub & least_adj_model==map, pref_group:=1]
              print(paste0('Model Pref is ', map, ' for ',c, ' nid ', id, ' and Marked with pref_group==1'))
              break()
            } else {
              print(paste0(map, ' not found for ',c,' nid ', id, ', moving to next preferred'))
            }
          }
        }
      }
    }
  }
}

table(data$pref_group)
data<- data[pref_group ==1]

#---------------------------------------------------------------------#
#--------------------Subgroup analysis--------------------------------#
#---------------------------------------------------------------------#

unique(data$subgroup_analysis_free_text)

data[, pref_group:=0]

def_map <- c(1,0)

for (t in unique(data$study_type)){
  for (c in unique(data$cause)){
    for (id in unique(data$nid)) {
      for (s in unique(data$sex)) {
        for (map in def_map) {
          if (map %in% unique(data[study_type == t & cause == c & nid==id & sex==s]$subgroup_analysis)) {
            data[study_type == t & cause == c & nid==id & sex==s & subgroup_analysis==map, pref_group:=1]
            print(paste0('Model subgroup_analysis is ', map, ' for ',c, ' nid ', id, ' and Marked with pref_group==1'))
            break()
          } else {
            print(paste0(map, ' not found for ',c,' nid ', id, ', moving to next preferred'))
          }
        }
      }
    }
  }
}

table(data$pref_group)

unique(data[pref_group == 0]$subgroup_analysis_free_text)

data<- data[pref_group ==1]

#------------------------------------------------------------------------------------------------#
#--------------------Further cleaning - Checking duplicate rows ---------------------------------#
#------------------------------------------------------------------------------------------------#

#Selecting rows from sources with multiple remaining data rows 
dups <- data[nid %in% c(unique(data[duplicated(data$nid)]$nid))]
dups <- dups[, c("nid", "acause", "cause", "location_id", "year_start", "year_end", "age_start", "age_end", "sex", "effect_size_measure", "table_num","mean", "exp_low", "exp_upper", "study_type","shs_exp_location", "shs_exp_source", "shs_exp_temporality", "shs_exp_smoking_status", "unit_clean")]
dups<-dups[,age :=as.character(paste0("'",age_start, "-",age_end))]
dups$age_start<-NULL
dups$age_end<-NULL
dups<-dups[,year :=as.character(paste0("'",year_start, "-",year_end))]
dups$year_start<-NULL
dups$year_end<-NULL
dups<-dups[,expr :=as.character(paste0("'",exp_low, "-",exp_upper))]
dups$exp_low<-NULL
dups$exp_upper<-NULL
setnames(dups, "effect_size_measure", "effect")
setnames(dups, "shs_exp_location", "location")
setnames(dups, "shs_exp_temporality", "temp")
setnames(dups, "shs_exp_source", "source")
setnames(dups, "shs_exp_smoking_status", "smoking")
dups <- dups[,keep := ""]
dups<- dups[study_type == "binary", expr :=""]
dups<- dups[expr == "'NA-NA", expr :=""]

#Exporting files to manually check duplicate rows
for (ca in unique(dups$cause)){
  print(ca)
  df <- dups[cause ==ca,]
  df$cause <- NULL
  df <- df[nid %in% c(unique(df[duplicated(df$nid)]$nid))]
  for (i in unique(df$nid)){
    id <- df[nid == i,]
    uniquelength <- sapply(id,function(x) length(unique(x)))
    id <- subset(id, select=uniquelength>1)
    write.table(id, paste0(home_dir, "FILEPATH"),sep=",", row.names=FALSE)
  }
}

#####-----LRI-----#####

##NID 524585
data <- data[n != 119]

data[nid == 524585 & exp_def %like% "1-10 cigs/day", exp_low := 1]
data[nid == 524585 & exp_def %like% "1-10 cigs/day", exp_upper := 10]
data[nid == 524585 & exp_def %like% "11 cigs/day", exp_low := 11]
data[nid == 524585 & exp_def %like% "11 cigs/day", exp_upper := 11*1.5]

#---------------------------------------------------------------------#
#--------------------Preparing the data--------------------------------#
#---------------------------------------------------------------------#

#Creating author-year column 
data[nid == "510534", field_citation_value := "Kobayashi Y, Yamagishi K, Muraki I, Kokubo Y, Saito I, Yatsuya H, Iso H, Tsugane S, Sawada N, JPHC Study Group. Secondhand smoke and the risk of incident cardiovascular disease among never-smoking women. Prev Med. 2022; 162: 107145."]

pub <- as.data.frame(str_extract_all(data$field_citation_value, "[12][09][0-9][0-9](?=; )", simplify = T))
aut <- as.data.frame(str_extract_all(data$field_citation_value, "([A-Z].*) ", simplify = T))
data$pub <- pub$V1
check<-data[pub ==""]
data[nid == "510530", pub := "2022"]
data[nid == "499143", pub := "2022"]
data <- data[,pub := paste0(word(aut$V1, 1)," ",pub," (",nid,")")]

unique(data$exp_def)
table(data$unit_clean, data$cause)
table(data$design, data$cause)

#Total sample
data$cohort_sample_size_exp <- as.numeric(data$cohort_sample_size_exp)
data$cohort_sample_size_unexp <- as.numeric(data$cohort_sample_size_unexp)
data[, cohort_sample_size_total := cohort_sample_size_exp + cohort_sample_size_unexp]

data[is.na(cc_cases_total),cc_cases_total := cc_cases_exp + cc_cases_unexp]
data[is.na(cc_controls_total),cc_controls_total := cc_controls_exp + cc_controls_unexp]

data[,cc_sample_size_total := cc_cases_total + cc_controls_total]

data[,sample_size := cohort_sample_size_total]
data[is.na(sample_size), sample_size := cc_sample_size_total]

#Total cases 
data<-data[,cases := cc_cases_total]
data<-data[, cohort_number_events_total:= cohort_number_events_exp + cohort_number_events_unexp]
data<-data[is.na(cases), cases := cohort_number_events_total]           
summary(data$cases)
check<-data[is.na(cases)]

#Age
data[is.na(age_start), age_lower := age_mean - 1.96*(age_sd/sqrt(cohort_sample_size_total))]
data[is.na(age_start), age_upper := age_mean + 1.96*(age_sd/sqrt(cohort_sample_size_total))]

#Remove columns for which all rows == NA.
data <- data[,which(unlist(lapply(data, function(x)!all(is.na(x))))),with=F] 

#---------------------------------------------------------------------#
#--------------------Preparing covariates-----------------------------#
#---------------------------------------------------------------------#

##Renaming 
data <- setnames(data, old = c("rep_population"), new = c("bc_subpopulation"))

##cv_design = cohort & case-cohort (0) vs other (1)
unique(data$design)
data <- data[, bc_design := ifelse(design %like% "cohort", 0, 1)]
table(data$bc_design, useNA = "ifany")

##bc_outcome_selfreport = only self-report (1) vs other (0)
table(data$outcome_assess_1)
table(data$outcome_assess_2)
table(data$outcome_assess_3)

data[, bc_outcome_selfreport := ifelse(outcome_assess_1 == "Self-report", 1,0)]
data[(outcome_assess_2 != "Self-report" & !(is.na(outcome_assess_2))), bc_outcome_selfreport := 0]
data[(outcome_assess_3 != "Self-report" & !(is.na(outcome_assess_3))), bc_outcome_selfreport := 0]
table(data$bc_outcome_selfreport, useNA = "ifany")

##bc_exposure_study (mr_brt interim guidance)
table(data$exp_assess_period)
data$exp_assess_num <- as.numeric(data$exp_assess_num)
class(data$exp_assess_num)

data <- data[exp_assess_num > 1, bc_exposure_study := 0]
data <- data[exp_assess_num == 1, bc_exposure_study := 1]
data <- data[exp_assess_period == "only at baseline", bc_exposure_study := 1]
data <- data[bc_design == 1, bc_exposure_study := 1] 
table(data$bc_exposure_study, useNA = "ifany")

##Loss to follow-up
data[,bc_selection_bias := 1]
data[cc_dropout_rate <= 0.20, bc_selection_bias := 0]
data[cohort_dropout_rate <= 0.20, bc_selection_bias := 0]

##Outcome cvs (exact vs component)
data[,bc_outcome_component := ifelse(is.na(outcome_components), 0,1)]
data[acause == "cvd_stroke_subhem",bc_outcome_component := 1]
data[acause == "cvd_stroke_isch",bc_outcome_component := 1]

###-----# Level of adjustment

##First, study/rows among restricted smoking populations, confounders_smoking == 1
data[shs_exp_smoking_status == "never smokers", confounders_smoking := 1]
data[shs_exp_smoking_status == "adj never smokers", confounders_smoking := 1]
data[shs_exp_smoking_status == "former smokers", confounders_smoking := 1]
data[shs_exp_smoking_status == "non-smokers (former and never)", confounders_smoking := 1]
data[sex != "Both", confounders_sex := 1]
data[sex != "Both", confounders_sex := 1]
table(data$confounders_smoking, data$shs_exp_smoking_status)


##Getting list of confounders
confounders <- names(data)[grepl('confounders_', names(data))]

for (j in c(confounders)){
  set(data,which(is.na(data[[j]])),j,0)
}

##Removing age, sex, and smoking as these will be considered separately
confounders_count <- confounders[!grepl('smoking', confounders)]
confounders_count <- confounders_count[!grepl('age', confounders_count)]
confounders_count <- confounders_count[!grepl('sex', confounders_count)]
data<- data[confounders_other !=0, confounders_other:=1]

##If study performed with restricted age group and not controlled for age, than confounders_age == 1 
# check <- data[acause != "resp_asthma" & ((age_end-age_start)<5)]
# check <- check[confounders_age == 0]
# check <- check[shs_exp_unit == "binary",]
data[age_end - age_start < 5 & confounders_age ==0, confounders_age := 1]

##Sum of other covariates
data[,(confounders):= lapply(.SD, as.numeric), .SDcols = confounders] 
data[, total_cvs := rowSums(.SD, na.rm = T), .SDcols = confounders_count]
summary(data$total_cvs)

data[ , bc_adj := as.numeric()]

##cv_adj=3 if no age or sex or smoking adjustment
data[confounders_age==0 | confounders_sex==0 | confounders_smoking==0, bc_adj := 3]

##cv_adj=2 if age+sex+smoking+0-2 adjusted
data[confounders_age==1 & confounders_sex==1 & confounders_smoking==1 & total_cvs >=0 & total_cvs<=2, bc_adj := 2]

##cv_adj=1 if age+sex+smoking+3-4 adjusted
data[confounders_age==1 & confounders_sex==1 & confounders_smoking==1 & total_cvs >=3 & total_cvs<=4, bc_adj := 1]

##cv_adj=0 if age+sex+smoking+5plus adjusted
data[confounders_age==1 & confounders_sex==1 & confounders_smoking==1 & total_cvs >=5, bc_adj := 0]

##check whether there is missing in cv_adj
message("there is ", nrow(data[is.na(bc_adj)]), " missing values in bc_adj")

##add cascading dummies
data[, bc_adj_L0 := 1]
data[, bc_adj_L1 := 1]
data[, bc_adj_L2 := 1]

##if cv_adj==0, change all dummies to be 0
data[bc_adj==0, c("bc_adj_L0", "bc_adj_L1", "bc_adj_L2") := 0]
##if cv_adj==1, change cv_adj_L1 and cv_adj_L2 to be 0
data[bc_adj==1, c("bc_adj_L1", "bc_adj_L2") := 0]
##if cv_adj==2, change cv_adj_L2 to be 0
data[bc_adj==2, c("bc_adj_L2") := 0]

#Unadjusted vs any adjustment 
data[,bc_unadjusted := ifelse(confounders_smoking==0 & confounders_age==0 & confounders_sex==0 & total_cvs == 0, 1,0)]

##remove cv_adj
data[, bc_adj := NULL]
data[, total_cvs := NULL]

#remove other individual confounders
#Keep smoking as BC, 0 = controlled, 1 = not controlled
data[, bc_smoking := ifelse(confounders_smoking == 1,0,1)]

#Keep sex as BC, 0 = controlled, 1 = not controlled 
data[, bc_sex := ifelse(confounders_sex == 1,0,1)]
#data[, (confounders):=NULL]

##Complete any missing value. 
bcs <- names(data)[grepl('bc_', names(data))]
for (j in c(bcs)){
  set(data,which(is.na(data[[j]])),j,1)
}

#---------------------------------------------------------------------#
#--------------------Prepare effect size------------------------------#
#---------------------------------------------------------------------#

##calculate SE from upper and lower 95% CI
data[, mean_se:=(upper-lower)/3.92] 
data[is.na(mean_se), mean_se:=standard_error] 
data <- data[!is.na(mean_se)]

# ##Delta method
log_effect <- as.data.table(delta_transform(mean=data$mean, sd=data$effect_size_se, transformation='linear_to_log'))
setnames(log_effect, c('mean_log', 'sd_log'), c('log_rr', 'log_se'))
data <- cbind(data, log_effect)


#---------------------------------------------------------------------#
#--------------------Preparing to save cleaned data set---------------#
#---------------------------------------------------------------------#

final_data<-data[unit_cat == "categorical"]

final_data <- final_data[nid == 485497 & acause == "resp_asthma", shs_exp_temporality := "ever"]
final_data <- final_data[nid == 487300 & acause == "resp_asthma", shs_exp_temporality := "ever"]

final_data<-final_data[shs_exp_temporality != "former"]

final_data<-final_data[nid != 451168] #NID 451168 and NID 524585 are from the same study reporting the same data.  NID 524585 has one extra year of follow-up.

dt<-copy(final_data)

 dt <- dt[cause == "resp_asthma" & outcome_def %like% "past year", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "last year", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "last 1 year", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "past 12 months", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "last 12 months", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "12-month", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "12-month", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "at 12 months", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "12\nmonths", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "in a given year", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "previous 1 year", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "previous 12 months", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "first year", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "1 year", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "at 6 years", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "at age 2", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "previous 12mo", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "previous year", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "12 months", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "6 and 18 months of age", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "first year of life", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "between 13 and 24 months", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "last 6 months", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "ISAAC", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & outcome_def %like% "6 months", bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 493789, outcome_def :="At the beginning of the NAS-NYS interview, one adult participant was asked two screening questions about each household member to determine who,
 if anyone, had asthma: (1) “Have you (has he/she). ever been told by a doctor or other health professional that you have asthma?” (2) “Do you (does he/she) still have asthma?” Current asthma = yes to both questions"]
 dt <- dt[cause == "resp_asthma" & nid == 494416, outcome_def :="Parents completed questionnaires when children were 12 months. Allergic asthma was defined by self-reported diagnosis of allergic asthma or by self-reported symptoms of cough, wheeze, dyspnoea or chest tightness after contact with cats and/or dogs, dust, pollen."]
 dt <- dt[cause == "resp_asthma" & nid == 494706, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 494889, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 496152, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 496170, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 496331, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 496430, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 496760, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 496530, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 497529, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 497562, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 497674, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 511267, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 511301, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 511301, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 511591, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 485483, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & cause == "resp_asthma" & nid == 485487, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 487237, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 487300, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 487856, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & nid == 494416, bc_last_year :=0]
 dt <- dt[cause == "resp_asthma" & is.na(bc_last_year), bc_last_year :=1]

 asthma <- asthma[outcome_def %like% "physician-diagnosed", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "diagnosis", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "doctor", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "physician", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "doctor-diagnosed", bc_doctor :=0]
 unique(asthma[is.na(bc_last_year) & is.na(bc_doctor)]$outcome_def)
 asthma <- asthma[outcome_def %like% "physician-diagnosed", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "diagnosis", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "doctor", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "physician", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "doctor-diagnosed", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "diagnosed", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "medical history", bc_doctor :=0]
 asthma <- asthma[outcome_def %like% "diagnoses", bc_doctor :=0]
 asthma <- asthma[study_id == 450846, bc_doctor :=0]
 asthma <- asthma[study_id == 493789, bc_doctor :=0]
 asthma <- asthma[study_id == 493789, bc_last_year :=0]
 asthma <- asthma[study_id == 494340, bc_doctor :=0]
 asthma <- asthma[study_id == 494706, bc_doctor :=0]
 asthma <- asthma[study_id == 494889, bc_doctor :=0]
 asthma <- asthma[study_id == 496152, bc_doctor :=0]
 asthma <- asthma[study_id == 496170, bc_doctor :=0]
 asthma <- asthma[study_id == 451001, bc_doctor :=0]
 asthma <- asthma[study_id == 496430, bc_doctor :=0]
 asthma <- asthma[study_id == 496760, bc_doctor :=0]
 asthma <- asthma[study_id == 496530, bc_doctor :=0]
 asthma <- asthma[study_id == 497328, bc_doctor :=0]
 asthma <- asthma[study_id == 497562, bc_doctor :=0]
 asthma <- asthma[study_id == 487856, bc_doctor :=0]
 asthma <- asthma[study_id == 487237, bc_doctor :=0]
 asthma <- asthma[study_id == 485487, bc_doctor :=0]
 asthma <- asthma[study_id == 497676, bc_doctor :=0]
 asthma <- asthma[study_id == 511124, bc_doctor :=0]
 asthma <- asthma[study_id == 497674, bc_doctor :=0]

 dt[nid == 493738 & unit_cat =="cigs" & exp_def %like% "1-2", exp_low := 1]
 dt[nid == 493738 & unit_cat =="cigs" & exp_def %like% "1-2", exp_upper := 2]
 dt[nid == 493738 & unit_cat =="cigs" & exp_def %like% "1-2", unexp_upper := 0]
 dt[nid == 493738 & unit_cat =="cigs" & exp_def %like% "1-2", unexp_low := 0]
 dt[nid == 493738 & unit_cat =="cigs" & exp_def %like% "3 or more", exp_low := 3]
 dt[nid == 493738 & unit_cat =="cigs" & exp_def %like% "3 or more", exp_upper := 4.5]
 dt[nid == 493738 & unit_cat =="cigs" & exp_def %like% "3 or more", unexp_upper := 0]
 dt[nid == 493738 & unit_cat =="cigs" & exp_def %like% "3 or more", unexp_low := 0]

#Non-cohort studies
dt<-dt[cause == "asthma" & age_end >16, rep_children := 0]
dt<-dt[cause == "asthma" & age_end <=16 & design %ni% c("prospective cohort", "retrospective cohort"), rep_children := 1]

#Stroke
dt[, bc_stroke_subhem := ifelse(acause == "cvd_stroke_subhem",1,0)]
dt[, bc_stroke_isch := ifelse(acause == "cvd_stroke_isch",1,0)]

#Save cleaned file
dt[, seq := seq_len(.N)]
write.csv(dt, paste0(out_dir, today,"FILEPATH"), row.names=FALSE)