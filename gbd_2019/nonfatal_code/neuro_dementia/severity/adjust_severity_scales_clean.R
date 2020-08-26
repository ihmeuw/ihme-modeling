## ---------------------------------------------------------------------------------------------------- ##
## Format input data for dementia severity mrbrt analysis
##    -Fill in covariates
##    -Merge in IDs
##    -Populate missing vals
##
## Author: USERNAME
## 06/05/2019
## ---------------------------------------------------------------------------------------------------- ##


## SET UP ENVIRONMENT --------------------------------------------------------------------------------- ##

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_drive <- "FILEPATH"
  h_drive <- "FILEPATH"
} else {
  j_drive <- "FILEPATH"
  h_drive <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
date <- gsub("-", "_", Sys.Date())

# ## SET OBJECTS -------------------------------------------------------------------------------------- ##
#

dem_dir <- paste0("FILEPATH")
severity_dir <- paste0("FILEPATH")
fn_dir <- "FILEPATH"


## READ IN DATA  -------------------------------------------------------------------------------------- ##

data <- fread(paste0("FILEPATH"))

data[NID==ID,is_outlier:=1]

## RECONCILE DIFFERENT SEVERITY SCALES ---------------------------------------------------------------- ##

# Look at how many studies for each scale we have
how_many <- unique(data[,.(scale, NID)])
how_many <- how_many[, count:=.N, by = scale]
how_many <- unique(how_many[,.(scale, count)])[order(count, decreasing = T)]

print(how_many)

# Look at how many for each scale after outliering
# Look at how many studies for each scale we have
how_many_noutlier <- unique(data[is.na(is_outlier),.(scale, NID)])
how_many_noutlier <- how_many_noutlier[, count:=.N, by = scale]
how_many_noutlier <- unique(how_many_noutlier[,.(scale, count)])[order(count, decreasing = T)]
setnames(how_many_noutlier, "count", "count_after_outliering")

print(how_many_noutlier)

how_many <- merge(how_many, how_many_noutlier, by = "scale")[order(count, decreasing = T)]

write.csv(how_many, file = paste0("FILEPATH"), row.names = F)


## CDRSB ----------------------------- ##
#create covariate for interpretation 1 of CDRSB
data[scale=="CDR-SB",cv_CDRSB1:=1]
data[cv_CDRSB1!=1,cv_CDRSB1:=0]
#adjust
data[cv_CDRSB1==1 & severity_rating=="0.5-4.0",severity_rating:="0.5"]
data[cv_CDRSB1==1 & severity_rating=="4.5-9.0",severity_rating:="1"]
data[cv_CDRSB1==1 & severity_rating=="9.5-15.5",severity_rating:="2"]
data[cv_CDRSB1==1 & severity_rating=="16.0-18.0",severity_rating:="3"]

#adjust
data[cv_CDRSB1==1 & severity_rating=="^0.5$",severity_rating:="0.5"]
data[cv_CDRSB1==1 & grepl("^0.5$|^1.0-3.0$", severity_rating),severity_rating:="0.5"]
data[cv_CDRSB1==1 & severity_rating=="3.5-7.0",severity_rating:="1"]
data[cv_CDRSB1==1 & severity_rating==">7",severity_rating:="2.0-3.0"]

#set the rest to 0
data[is.na(cv_CDRSB1),cv_CDRSB1:=0]


## DSMIII --------------------------- ##

#create covariate
data[grepl("DSM-III-R", scale),cv_DSMIIIR:=1]
data[cv_DSMIIIR!=1,cv_DSMIIIR:=0]
#adjust
data[cv_DSMIIIR==1 & severity_rating=="mild",severity_rating:="1"]
data[cv_DSMIIIR==1 & tolower(severity_rating)=="moderate",severity_rating:="2"]
data[cv_DSMIIIR==1 & severity_rating=="severe",severity_rating:="3"]

#set the rest to 0
data[is.na(cv_DSMIIIR),cv_DSMIIIR:=0]

## GDS ------------------------------ ##
#create covariate
data[scale=="GDS",cv_GDS:=1]
data[cv_GDS!=1,cv_GDS:=0]
#adjust
data[cv_GDS==1 & grepl(pattern = "2|3", x = severity_rating),severity_rating:="0.5"]
data[cv_GDS==1 & grepl(pattern = "4|5", x = severity_rating),severity_rating:="1"]
data[cv_GDS==1 & grepl(pattern = "^6$", x = severity_rating),severity_rating:="2"]
data[cv_GDS==1 & grepl(pattern = "^7$", x = severity_rating),severity_rating:="3"]
data[cv_GDS==1 & grepl(pattern = "^6.0-7.0$", x = severity_rating),severity_rating:="2.0-3.0"]

#set the rest to 0
data[is.na(cv_GDS),cv_GDS:=0]


## outlier non useable scales ------------------------------------------------------------------------- ##
scales_using <- c("CDR","CDR-SB","DSM-III-R","GDS")

data[!(scale %in% scales_using),is_outlier:=1]

non_useful <- unique(data[scale=="CDR" & grepl("^4.0-5.0$|^1.0-3.0$|terminal", severity_rating),]$NID)
print("The following will be outliered:")
print(data[NID %in% non_useful, c("location_name","year_start","scale","severity_rating","NID")])
data[NID %in% non_useful,is_outlier:=1]

## outlier MCI cases we won't model
data <- data[!(NID %in% 410813 & severity_rating=="0.5"),]


## convert text to numeric severity scale ------------------------------------------------------------- ##

data[!is.na(is_outlier),converted:=1]

data[grepl("0.5-1",severity_rating),severity_rating:="0.5-1.0"]
data[grepl("0.5-1",severity_rating),converted:=1]

data[is.na(converted) & grepl("0.5|very mild", tolower(severity_rating)),severity_rating:="0.5"]
data[severity_rating=="0.5",converted:=1]


data[is.na(converted) & grepl("1|mild", tolower(severity_rating)),severity_rating:="1"]
data[severity_rating=="1",converted:=1]

data[grepl("2.0-3", severity_rating),severity_rating:="2.0-3.0"]
data[severity_rating=="2.0-3.0",converted:=1]

data[is.na(converted) & grepl("2|moderate", tolower(severity_rating)),severity_rating:="2"]
data[severity_rating=="2",converted:=1]

data[is.na(converted) & grepl("3|severe", tolower(severity_rating)),severity_rating:="3"]
data[severity_rating=="3",converted:=1]

data[NID==369739,note_sr:=paste0(note_sr, " the CDR3 was written as CDR >=3, so it's possible this was a non-standard scale and ought to be outliered")]

data[,converted:=NULL]

## CHECK THAT MEANS SUM TO 1 PER SEX/LOC/ETC  --------------------------------------------------------- ##

#subset
data_checks <- copy(data[is.na(is_outlier),])[,.(NID,
                                                 location_id,
                                                 sex_id,
                                                 year_start,
                                                 year_end,
                                                 age_start,
                                                 age_end,
                                                 severity_rating,
                                                 mean,
                                                 cases,
                                                 sample_size,
                                                 specificity)]


#identify blocks that ought to sum to 1
data_checks[,check_id:= .GRP, by = .(NID,
                                     location_id,
                                     sex_id,
                                     year_start,
                                     year_end,
                                     age_start,
                                     age_end,
                                     sample_size,
                                     specificity)]
data_checks[,sums_1:=sum(mean), by = check_id]
data_checks[,sums_1:=round(sums_1,digits=2)]

#subset to problem rows
problems <- data_checks[sums_1!=1,]
if(nrow(problems)>0){
  print("oops; you've got studies for which your severity buckets don't sum to 1. try View(problems).")
}


## AGGREGATE CDR 0.5 and 1  -------------------------------------------------------------------------- ##

very_mild <- unique(data[is.na(is_outlier) & severity_rating=="0.5",]$NID)
mild <- unique(data[is.na(is_outlier) & severity_rating=="1",]$NID)

#make sure studies with very_mild also have mild
check <- very_mild %in% mild
if(sum(check)<length(check)){
  print("consider outliering; there are some studies with very_mild reported, but not mild")
  print(very_mild[!check])
}

#check that all studies have both mean and mean_age
if(nrow(data[is.na(mean)|is.na(mean_age),])>0){
  print("Oops--something went wrong with the following NIDs. Check scripts for calculating mean and mean_age:")
  print(data[is.na(mean)|is.na(mean_age)])
}

to_aggr <- data[NID %in% very_mild & (severity_rating %in% c(0.5, 1)),]

#for each unique .(NID, sex_id, location_id, year_start, year_end) we calculate new mean & mean_age
to_aggr[, group_id:= .GRP, by = .(NID, sex_id, location_id, year_start, year_end, mean_age, sample_size)]

# flag all the studies for which very_mild and mild aren't nicely paired
to_aggr[, count:= .N, by = group_id]

#calculate aggregated mean
to_aggr[,pooled_cases:=sum(cases), by = group_id]
to_aggr[,fraction_mean:=cases/pooled_cases*mean]
to_aggr[,new_mean:=sum(fraction_mean), by = group_id]

#calculate aggr. mean_age
to_aggr[,fraction_mean_age:=cases/pooled_cases*mean_age]
to_aggr[,new_mean_age:=sum(fraction_mean_age), by = group_id]

#isolate aggregated mild vals
mild_data <- to_aggr[severity_rating==1,]
mild_data[,mean:=new_mean]
mild_data[,mean_age:=new_mean_age]
mild_data[,cases:=pooled_cases]


#merge back into dt
keep_cols <- names(mild_data) %in% names(data)
mild_data <- mild_data[,keep_cols, with = F]
all_data <- data[!(NID %in% very_mild & (severity_rating %in% c(0.5, 1))),]

all_data <- rbind(all_data, mild_data)


# ADD STD ERROR  -------------------------------------------------------------------------------------- ##

get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  z <- qnorm(0.975) #this assumes mean 0 std dev 1
  dt[is.na(standard_error), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  return(dt)
}

all_data <- get_se(all_data)



## SEX SPLIT ------------------------------------------------------------------------------------------ ##

#check out which NIDS need to be treated
split_nids <- unique(data[!is.na(specificity),]$NID)
severity_sex_nids <- unique(data[specificity %in% c("split_severity_combined_sex","combined_severity_split_sex"),]$NID)



## AGE SEX SPLITTING
#age_sex_split <- function(raw_dt){

#RANDOM FINAGLING TO GET THIS ALL TO WORK
raw_dt <- all_data
raw_dt[,seq:=1:nrow(raw_dt)]
raw_dt[,crosswalk_parent_seq:=NA]
raw_dt[specificity=="split_sex_combined_age",specificity:="sex"]
raw_dt[specificity=="split_age_combined_sex",specificity:="age"]
raw_dt[,note_modeler:=""]

df_split <- copy(raw_dt)
cvs <- names(df_split)[grepl("^cv", names(df_split))]


##ADDING SOME TESTS FOR READING
df_split <- copy(raw_dt[specificity %in% c("age","sex"),])
df_split[,split:=0]

## CALC CASES MALE/FEMALE
df_split[, cases_total:= sum(cases), by = c("NID", "group", "specificity", "measure", "location_id", "severity_rating", cvs)]
df_split[, prop_cases := cases / cases_total]

## CALC PROP MALE/FEMALE
df_split[, ss_total:= sum(sample_size), by = c("NID", "group", "specificity", "measure", "location_id", "severity_rating", cvs)]
df_split[, prop_ss := sample_size / ss_total]

## CALC SE
df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]

## RATIO
df_split[, ratio := prop_cases / prop_ss]
df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
df_ratio <- df_split[specificity == "sex", c("NID", "group", "sex", "location_id", "measure", "ratio", "se_ratio", "prop_cases", "prop_ss", "year_start", "year_end", "severity_rating", cvs), with = F]

## CREATE NEW OBSERVATIONS
age.sex <- copy(df_split[specificity == "age"])
age.sex[,specificity := "age,sex"]
age.sex[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
age.sex[,seq := ""]
age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL]
male <- copy(age.sex[, sex := "Male"])
female <- copy(age.sex[, sex := "Female"])

age.sex <- rbind(male, female)
age.sex <- merge(age.sex, df_ratio, by = c("NID", "group", "sex", "measure", "location_id", "year_start", "year_end", "severity_rating", cvs), allow.cartesian = T)

## CALC MEANS
age.sex[, standard_error := sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2)]
age.sex[, mean := mean * ratio]
age.sex[, cases := cases * prop_cases]
age.sex[, sample_size := sample_size * prop_ss]
age.sex[,note_modeler := paste(note_modeler, " | age,sex split using sex ratio", round(ratio, digits = 2))]
age.sex[mean > 1, `:=` (group_review = 0, exclude_xwalk = 1, note_modeler = paste0(note_modeler, " | group reviewed out because age-sex split over 1"))]
age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]
age.sex[, c("lower", "upper", "uncertainty_type_value","effective_sample_size") := ""]

## CREATE UNIQUE GROUP TO GET PARENTS
age.sex.m <- age.sex[,c("NID","group", "measure", "location_id", "year_start", "year_end", "severity_rating", cvs), with=F]
age.sex.m <- unique(age.sex.m, by=c("NID","group", "measure", "location_id", "year_start", "year_end", "severity_rating", cvs))

## GET PARENTS
parent <- merge(age.sex.m, raw_dt, by= c("NID","group", "measure", "location_id", "year_start", "year_end", "severity_rating", cvs))
parent[specificity == "age" | specificity == "sex", group_review:=0]
parent[, note_modeler := paste0(note_modeler, " | parent data, has been age-sex split")]

## FINAL DATA
original <- copy(raw_dt)[!seq %in% parent$seq]
total <- rbind(original, age.sex, fill = T) ## DON'T RETURN PARENTS
total[specificity=="age,sex",group_review:=0]

all_data <- total

## CLEAN UP  ------------------------------------------------------------------------------------------ ##

#get rid of outliers
all_data[!is.na(specificity) & is.na(group_review),group_review:=0]
all_data <- all_data[is.na(is_outlier),]


#subset to MRBRT cols
all_data <- all_data[,c("NID",
                        "location_id",
                        "year_start",
                        "year_end",
                        "sex_id",
                        "mean_age",
                        "estimated_mean_age",
                        "age_start",
                        "age_end",
                        "severity_rating",
                        "mean",
                        "standard_error",
                        "cases",
                        "sample_size",
                        grep("cv_",names(all_data),value = T)),
                     with = F]

## SAVE ----------------------------------------------------------------------------------------------- ##

write.csv(all_data, paste0("FILEPATH"), row.names = F)


#mild
all_mild <- all_data[severity_rating %in% c("1","0.5-1.0"),]
write.csv(all_mild, paste0("FILEPATH"), row.names = F)

#moderate
all_mod <- all_data[severity_rating %in% "2",]
write.csv(all_mod, paste0("FILEPATH"), row.names = F)

#severe
all_sev <- all_data[severity_rating %in% "3",]
write.csv(all_sev, paste0("FILEPATH"), row.names = F)
