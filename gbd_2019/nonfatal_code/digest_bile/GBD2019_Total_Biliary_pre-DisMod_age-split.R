#####################################################################################
### This script creates age_sex_split function for data prep
### NOTE: Must have group review specificity set to "sex" and "age" as appropriate
####################################################################################

library(plyr)
library(msm, lib.loc="FILEPATH")
library(metafor, lib.loc="FILEPATH")
library(readxl)
library(data.table)
library(dplyr)
locs <- read.csv("FILEPATH")

pacman::p_load(data.table, ggplot2, magrittr, openxlsx)

cause_path <- "Biliary"  #SPECIFY FILEPATH
cause_name <- "digest_bile" #SPECIFY CAUSE NAME
mrbrt_dir <- paste0("FILEPATH")

##############################################################################################################
df <- fread("FILEPATH")

age_sex_split <- function(df){
  dt <- as.data.table(df)
  n <- nrow(dt)
  dt$id <- seq(1,n,1)
  dt[, seq := as.character(seq)] #Allow missing seq for new obs
  dt[, note_modeler := as.character(note_modeler)]
  
  dt_split <- copy(dt)
  #subset to data needing age-sex splitting
  dt_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to age sex split")]
  dt_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  dt_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  dt_split[is.na(cases), cases := sample_size * mean]
  dt_split <- dt_split[!is.na(cases),]
  dt_split[, split := length(specificity[specificity == "age,sex"]), by = list(nid, group, measure, year_start, year_end)]
  dt_split <- dt_split[specificity %in% c("age", "sex") & split == 0,]
  
  #calculate proportion of cases male and female
  dt_split[, cases_total:= sum(cases), by = list(nid, group, specificity, measure)]
  dt_split[, prop_cases := round(cases / cases_total, digits = 3)]
  
  #calculate proportion of sample male and female
  dt_split[, ss_total:= sum(sample_size), by = list(nid, group, specificity, measure)]
  dt_split[, prop_ss := round(sample_size / ss_total, digits = 3)]
  
  #calculate standard error of % cases & sample_size M and F
  dt_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  dt_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  #estimate ratio & standard error of ratio % cases : % sample
  dt_split[, ratio := round(prop_cases / prop_ss, digits = 3)]
  dt_split[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
  #Save these for later
  dt_ratio <- dt_split[specificity == "sex", list(nid, group, sex, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end)]
  
  #Create age,sex observations 
  age.sex <- copy(dt_split[specificity == "age"])
  age.sex[,specificity := "age,sex"]
  age.sex[,seq := ""]
  age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL]
  male <- copy(age.sex[, sex := "Male"])
  female <- copy(age.sex[, sex := "Female"])
  age.sex <- rbind(male, female)
  
  #Merge sex ratios to age,sex observations
  age.sex <- merge(age.sex, dt_ratio, by = c("nid", "group", "sex", "measure", "year_start", "year_end"), allow.cartesian = T)
  
  #calculate age-sex specific mean, standard_error, cases, sample_size
  age.sex[, standard_error := round(sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2), digits = 4)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := round(cases * prop_cases, digits = 0)]
  age.sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
  age.sex[,note_modeler := paste(note_modeler, "| age,sex split using sex ratio", round(ratio, digits = 2))]
  age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]
  
  ## merge back on any dropped un-split seqs.
  split_ids <- unique(age.sex$id)
  not_split_rows <- dt[(id %in% split_ids)==F,]
  final <- plyr::rbind.fill(age.sex,not_split_rows)
  dt <- as.data.table(final)
  dt[,id:=NULL]
  
  ##final dataset 
  dt <- dt[specificity!="age",]
  dt <- dt[specificity!="sex",]
  dt[specificity=="age,sex",group_review:=1]
  
  ##return the prepped data
  return(df)
  
}

split_dt <- age_sex_split(df)

## SAVE DATA
write.csv(split_dt, paste0(mrbrt_dir,"FILENAME"),  row.names=F)





