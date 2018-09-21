#######################################################################################
### Author: 
### Modified:  12/8/2016
### Date: 11/7/2016
### Project: GBD Nonfatal Estimation
### Purpose: Age-sex Splitting
### Inputs: For this code to work, you must make a copy of your age-sex splitting sheet
### in a .csv format.  In addition, GROUP REVIEW MUST BE SET TO "SEX", "AGE"
### AND "AGE, SEX" WHEN APPROPRIATE
### Outputs: A .xlsx file with parent data with group review marked 0 and new age-split 
### data with a note in note_modeler
### Notes: For now, it will only work if cases is not missing
#######################################################################################

###################
### Setting up ####
###################
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j.root <- "/home/j/" 
} else { 
  j.root <- "J:"
}

#load packages, install if missing
require(data.table)
require(xlsx)

#set information
a_cause <- 
bundle_id <-
request_num <- 

download_dir <- FILEPATH
upload_dir <- FILEPATH
lit_dir <- FILEPATH
#######################################################################################################################################


###########################
### Age-Sex Splitting  ####
###########################

#Load Data
df <- fread(paste0(upload_dir, FILEPATH, ".csv"), stringsAsFactors=FALSE)
df[, seq := as.character(seq)] #Allow missing seq for new obs
df[, note_modeler := as.character(note_modeler)]

df_split <- copy(df)  

#subset to data needing age-sex splitting
df_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to age sex split")]
df_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
df_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
df_split[is.na(cases), cases := sample_size * mean]
df_split <- df_split[!is.na(cases),]
df_split[, split := length(specificity[specificity == "age,sex"]), by = list(nid, group, measure, year_start, year_end)]
df_split <- df_split[specificity %in% c("age", "sex") & split == 0,]


#calculate proportion of cases male and female
df_split[, cases_total:= sum(cases), by = list(nid, group, specificity, measure)]
df_split[, prop_cases := round(cases / cases_total, digits = 3)]

#calculate proportion of sample male and female
df_split[, ss_total:= sum(sample_size), by = list(nid, group, specificity, measure)]
df_split[, prop_ss := round(sample_size / ss_total, digits = 3)]

#calculate standard error of % cases & sample_size M and F
df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]

#estimate ratio & standard error of ratio % cases : % sample
df_split[, ratio := round(prop_cases / prop_ss, digits = 3)]
df_split[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
#Save these for later
df_ratio <- df_split[specificity == "sex", list(nid, group, sex, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end)]

#Create age,sex observations 
age.sex <- copy(df_split[specificity == "age"])
age.sex[,specificity := "age,sex"]
age.sex[,seq := ""]
age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL]
male <- copy(age.sex[, sex := "Male"])
female <- copy(age.sex[, sex := "Female"])
age.sex <- rbind(male, female)   
#Merge sex ratios to age,sex observations
age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "sex", "measure", "year_start", "year_end"))

#calculate age-sex specific mean, standard_error, cases, sample_size
age.sex[, standard_error := round(sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2), digits = 4)]
age.sex[, mean := mean * ratio]
age.sex[, cases := round(cases * prop_cases, digits = 0)]
age.sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
age.sex[,note_modeler := paste(note_modeler, "| age,sex split using sex ratio", round(ratio, digits = 2))]
age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]

##create unique set of nid and group to pull from age.sex    
age.sex.m <- age.sex[,c("nid","group", "measure"), with=F]    
age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure"))

##merge to get the parent rows from age sex split
parent <- merge(age.sex.m, df, by= c("nid", "group", "measure"))
parent[specificity=="age" | specificity=="sex",group_review:=0]
parent[, note_modeler := paste0(note_modeler, "| parent data, has been age-sex split")]

##final dataset 
total <- rbind(parent, age.sex)

##Save data
write.xlsx(total, paste0(upload_dir,FILEPATH, ".xlsx"), row.names=F, showNA=F, sheetName="extraction")