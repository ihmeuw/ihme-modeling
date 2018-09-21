###########################################################
### Author: USERNAME
### Date: 2/4/16
### Project: Metabolics
### Purpose: Age Sex Split
### Notes: 
###
###     Code to age sex split continuous means to the standard
###     GBD 5-yr age groups/sex groups, done at the global level
###
###     Adapted from  code for BMI (http://www.thelancet.com/journals/lancet/article/PIIS0140-6736(14)60460-8/abstract)
### 
###     Limitations:
###        - SUPER bad global level age split, should explore splitting at more granular levels
###        - USERNAME: I am going to look into regional age-sex pattern creation (12/2/2016)
###########################################################
#######READ###############
##USERNAME: This version of USERNAME age-sex split code is for making a dataset with which to evaluate the age-sex split.
##USERNAME: I've removed crosswalked data and saved the output in the FILEPATH
##USERNAME:11/7/2016: I am going to set up this age-sex split so that it only uses microdata for the age-sex pattern
##USERNAME: 12/2/2016: I am going to set up so that it age sex splits the new GBD 2016 oldest age groups
##USERNAME: 12/3/2016:I'm also doing a handful of other small updates (updating pulling in pops, location set id update, etc)
##USERNAME: 12/13/2015: cleaning up how this script looks and flows, also trying to merge on 'by hand' extracted microdata
##USERNAME: 3/26/2017 adding new save file structure (using get_recent())
## source("FILEPATH")
################### SETUP #########################################
######################################################



rm(list=ls())
os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "J:/"
  h<-"H:/"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
}


date<-gsub("-", "_", USERNAMEs.Date())


library(data.table)
library(dplyr)
library(stringr)
library(ggplot2)
library(MASS)



################### PATHS #########################################
######################################################
me<-"chl"  ##"chl"
old<-F  ##USERNAME: this is for looking at 2015's asp
##USERNAME:note, central functions must be used on cluster
##USERNAME:path to R central functions
central<-paste0(j, "FILEPATH")

##inputs
input_folder<-paste0("FILEPATH")  ##USERNAME: new setup to have get_recent() function pick most recent data version

input_15<-paste0(j, "FILEPATH")  ##USERNAME:this is my file after removing crosswalked prevs



##outputs
#output<-paste0(j,"Project/CVD_Roth/GBD2016/Risks/exposure/data/processing_data/2016/", me, "/to_usfix.csv")
out_path<-paste0(j,"FILEPATH")
if(old==T){
plot_output<-paste0(j, "FILEPATH")
}

if(old==F){
  plot_output<-paste0(j, "FILEPATH")
}







########################## SCRIPTS #################################
#################################################################
  
  
  
source(paste0(j, "FILEPATH"))  ##USERNAME: my function
  
  
source(paste0(central, "get_population.R"))
source(paste0(central, "get_ids.R"))
source(paste0(central, "get_location_metadata.R"))






################### READ IN DATA/LOCS AND CLEAN #########################################
######################################################


##USERNAME: I'm reading in 'micro_mean".  This is the output of the sbp_dc_crosswalk_pres.R script after removing prev data and,
##USERNAME: marking microdata and mean lit (for now) as data_type==0 and prev lit data  as data_type==2
#df<-fread(input)
df<-get_recent(input_folder)

if(old==T){
df<-readRDS(input_15)
df<-as.data.table(df)
df<-df[me_name==me,]
}
## Get location id

  locs <- get_location_metadata(version_id=149)[, .(location_id, ihme_loc_id, super_region_name, region_name)]  
if (!"location_id" %in% names(df)) {
  df <- merge(df, locs, by="ihme_loc_id", all.x=T)
}

df <- df[!is.na(location_id)]
## Drop anything without means  ##USERNAME: for chol, 17 missing means
df <- df[!is.na(mean)]


##USERNAME: drop age_group_id var here, we just need age_start/end
df[, age_group_id:=NULL]

#saveRDS(df, file=paste0("FILEPATH"))

##USERNAME: give data_id, not necessary
#df[, data_id:=.I]



################### GET AND CLEAN POPULATION ESTIMATES #########################################
########################################################
message("Getting populations")
##USERNAME: "-1" means 'all possible values'
pops<-get_population(age_group_id=-1, year_id=-1, sex_id=-1, location_id=-1, location_set_id=22, status="best", gbd_round_id=4)
##USERNAME: link age_group_id with age start/end
age_groups<-get_ids(table="age_group")




##USERNAME:merge on age_group ranges with 
pops<-merge(pops, age_groups, by="age_group_id")




age_ids<-c(6:20, 30,31,32, 235, 1)
pops<-subset(pops, age_group_id %in% age_ids)
pops[age_group_id==1, age_group_name:="0 to 4"]
pops[age_group_id==235, age_group_name:="95 to 99"]
##USERNAME:create 'age_start' and 'age_end'
#as.numeric(unlist(strsplit(unique(pops$age_group_name), "to")))


age_start<-unlist(lapply(strsplit(pops$age_group_name, "to"), "[", 1))
age_end<-unlist(lapply(strsplit(pops$age_group_name, "to"), "[", 2))

pops<-cbind(pops, age_start, age_end)

##USERNAME:fix up the last age group name
#pops<-pops[age_group_id==235, age_start:="95"]
#pops<-pops[age_group_id==235, age_end:="99"]   ##USERNAME:I'm imposing 99 on age end so that I can do the sex split (need numeric)
  

##USERNAME:make numeric
pops<-pops[, age_start:=as.numeric(age_start)]
pops<-pops[, age_end:=as.numeric(age_end)]
  #pops <- pops[, age_start := round(age_start)]
pops[, age_group_id:=NULL]  ##USERNAME: this column causes problems otherwise


print("Done")







################### IF INTERACTIVE #########################################
######################################################
if(T){                         ##USERNAME: I set up this script that it should always be interactive
  location_id <- "location_id" 
  year_id <- "year_id" 
  age_start <- "age_start"
  age_end <- "age_end" 
  sex <- "sex_id"
  estimate <- "mean"
  sample_size <- "sample_size"
  data_type<- "data_type"
}




################### DEFINE AGE-SEX-SPLIT FUNCTION ########################################
######################################################
######################################################
######################################################


##USERNAME:below are the arguments that USERNAME puts into the age_sex_split function.  they are all columns in df
#age_sex_split(df=df[me_name==x], location_id = "location_id", year_id = "year_id", age_start = "age_start",age_end = "age_end", sex = "sex_id",estimate = "mean",sample_size = "sample_size")

#age_sex_split <- function(df, location_id, year_id, age_start, age_end, sex, estimate, sample_size, data_type ) { 

###############
## Setup 
###############

## Generate unique ID for eaUSERNAME merging
df[, split_id := 1:.N]  ##USERNAME: .N means the number of instances, ie, the #of rows.  This is making a column with values= row number

## Make sure age and sex are int
cols <- c(age_start, age_end, sex)  
df[, (cols) := lapply(.SD, as.integer), .SDcols=cols]  ##USERNAME: data.table syntax, read: 'apply function as.integer over the columns in .SDcols

## Save original values
orig <- c(age_start, age_end, sex, estimate, sample_size)
orig.cols <- paste0("orig.", orig)
df[, (orig.cols) := lapply(.SD, function(x) x), .SDcols=orig]  ##USERNAME:creates columns orig.age_start, orig.sex_id, etc...

## Separate metadata from required variables
cols <- c(location_id, year_id, age_start, age_end, sex, estimate, sample_size, data_type) 

if(old==T){
  
  cols <- c(location_id, year_id, age_start, age_end, sex, estimate, sample_size) 
}


meta.cols <- setdiff(names(df), cols)  ##USERNAME: setdiff takes all the cols not in the 'cols' vector
metadata <- df[, meta.cols, with=F] 
data <- df[, c("split_id", cols), with=F]  ##USERNAME: I'm adding 'data_type' so I can only train on microdata







###################SETUP TRAIN AND TEST DATA#########################################
######################################################

## Round age groups to the nearest 5-y boundary
data[, age_start := age_start - age_start %% 5]  ##USERNAME:this arithmetic takes age start (say 17), and subtracts from it modulus 5 of that age (2), yields 5 year age start. 
          


if(old==F){

  data <- data[age_start > 95, (age_start) := 95]  ##USERNAME:updated 12/3/2016 to reflect new age groups (95 and 99)
  data[, age_end := age_end - age_end %%5 + 4]     ##USERNAME: makes last age group 4+ modulus 5 difference
  data <- data[age_end > 99, age_end := 99]       ##USERNAME:updated 12/3/2016 to reflect new age groups (95 and 99)
  
}


                  
if(old==T){                 
data <- data[age_start > 80, (age_start) := 80]  ##USERNAME:updated 12/3/2016 to reflect new age groups (95 and 99)
data[, age_end := age_end - age_end %%5 + 4]     ##USERNAME: makes last age group 4+ modulus 5 difference
data <- data[age_end > 84, age_end := 84]       ##USERNAME:updated 12/3/2016 to reflect new age groups (95 and 99)
}
## Split into training and split set  ##USERNAME:sex_id==1 | 2 is male | female, 3 is both.  

##USERNAME: I'm hashtagging out so I can decide what goes into 'training' (just microdata)
unsplit <- data[data_type!=1 & ((age_end - age_start) == 4   |   age_start>=95) & sex_id %in% c(1,2)]  ##USERNAME: store these rows for merging on later... are neither test nor train
training<-data[data_type==1,]

if(old==T){
  training <- data[(age_end - age_start) == 4 & sex_id %in% c(1,2)]
  #training<-data[data_type==1]
}

split <- data[data_type!=1  &  ((age_end - age_start) != 4 | sex_id == 3)]            ##USERNAME: data_type=1 is microdata








################### CREATE AGE-SEX PATTERN #########################################
######################################################
####################################################


##USERNAME:trying to weight by sample size first
#training[, asp_weight:=sample_size*mean]

#asp<-training[, rel_est:=mean(asp_weight)/sum(sample_size), by=c("sex_id", "age_start")]

# Determine relative age/sex pattern ##USERNAME: 'asp'= 'age sex pattern'





################### MAKE GLOBAL AGE-SEX PATTERN #########################################
######################################################

asp <- aggregate(training[[estimate]],by=lapply(training[,c(age_start, sex), with=F],function(x)x),FUN=mean,na.rm=TRUE)  
##USERNAME:the line above creates a df where for each specific age_start-sex group, there is a single 'relative estimate' mean  ##USERNAME: gives 'mean pattern'
asp<-as.data.table(asp)
asp[sex_id==1, gender:="Male"]
asp[sex_id==2, gender:="Female"]



names(asp)[3] <- "rel_est"


asp <- dcast(asp, formula(paste0(age_start," ~  gender")), value.var="rel_est")  ##USERNAME:casts asp wide by sex (2 columns, 1 for each sex)

asp<-as.data.table(asp)
##USERNAME: smoothing over trend in oldest age groups (take mean of oldest 4 age groups and apply over all 4)
if(F){
  if(me=="chl"){
    asp[age_start>=80, Male:=mean(Male)]
    
    asp[age_start>=80, Female:=mean(Female)]
    
  }
}

setnames(asp, c("Male", "Female"), c("1", "2"))

# Fill NAs with values from adjacent age/sex groups
asp[is.na(asp[[1]]), 1] <- asp[is.na(asp[[1]]),2]    
asp[is.na(asp[[2]]), 2] <- asp[is.na(asp[[2]]),1]    
asp <- melt(asp,id.var=c(age_start), variable.name=sex, value.name="rel_est")  ##USERNAME: this melts long again to 1 col for sex_id
asp$sex_id <- as.integer(as.character(asp$sex_id))  ##USERNAME: need to coerce to character because it's a factor and they get funky w/ coercing






################### GRAPH AGE-SEX PATTERN #########################################
######################################################


asp.t<-copy(asp)
asp.t[sex_id==1, gender:="Male"]
asp.t[sex_id==2, gender:="Female"]





if(me=="sbp"){
pdf(plot_output)
p<-ggplot(data=asp.t[age_start>=25,], aes(x=age_start, y=rel_est))+
  geom_point()+   ##USERNAME: for IHME green: color=I(rgb(0.4, 0.65, 0.25))
  facet_wrap(~gender)+
  ylim(80, 160)+  ##chl: 0-7, sbp: 80-160
  labs(title="Global age-sex Pattern")+
  xlab("Age")+
  ylab("Mean systolic blood pressure")+
  theme_minimal()+
  theme(panel.border=element_rect(color="black", fill=NA, size=0.1))
print(p)

dev.off()
}


if(me=="chl"){
  
  #mspl<-spline(asp.t$age_start, y=asp.t$rel_est)
  #fspl<-spline(asp.t[gender=="Female" ,age_start], y=asp.t[gender=="Male" ,rel_est])
  
  
  pdf(plot_output)
  p<-ggplot(data=asp.t[age_start>=25,], aes(x=age_start, y=rel_est))+
    geom_point()+
    facet_wrap(~gender)+
    ylim(0, 7)+  ##chl: 0-7, sbp: 80-160
    labs(title="Global age-sex pattern")+
    xlab("Age")+
    ylab("Mean total cholesterol")+
    theme_minimal()+
    theme(panel.border=element_rect(color="black", fill=NA, size=0.1))
  print(p)
  
  dev.off()
}









###################SETUP ROWS FOR SPLITTING########################################
######################################################
message("Splitting")

split[, n.age := (age_end + 1 - age_start)/5]  ##USERNAME: calculates the number of 5 year age groups that a row holds and assigns it to the n.age col

split[, n.sex := ifelse(sex_id==3, 2, 1)]      ##USERNAME: if sex_id is "both", assign it a 2, if it is 2 or 1, assign 1.  So tells number of sexes described.
## Expand for age 
split[, age_start_floor := age_start]          
expanded <- rep(split$split_id, split$n.age) %>% data.table("split_id" = .)  ##USERNAME: create a column called split_id that will connect rows that came initially from same row
                                                                  ##for the rep() function, read 'replicate the value in split$split_id the number of times equal to the value in split$n.age'
split <- merge(expanded, split, by="split_id", all=T)   ##USERNAME: merge 'expanded' with 'split' on 'split_id' so that the right amount of new rows can be made from the orignal row in 'split'
split[, age.rep := 1:.N - 1, by=.(split_id)]            ##USERNAME: create 'age.rep' col, describes the iteration number of a split by age from the original row
split[, (age_start):= age_start + age.rep * 5 ]         ##USERNAME: makes new appropriate age_starts for the new rows
split[, (age_end) :=  age_start + 4 ]                  ##USERNAME: makes new appropriate age_ends for the new rows

## Expand for sex
split[, sex_split_id := paste0(split_id, "_", age_start)]              ##USERNAME: creates a col with a unique value for each row, describes the split_id (maps to the original row) and age_start
#split<-split[!is.na(sex_id)]  ##USERNAME: two rows missing sex_id got split, need to go back and find  ##USERNAME: fixed, hopefully won't need this line anymore

expanded <- rep(split$sex_split_id, split$n.sex) %>% data.table("sex_split_id" = .)  ##USERNAME: create 'expanded' again, this time with column 'sex_split_id', with repititions=split$n.sex
split <- merge(expanded, split, by="sex_split_id", all=T)                           ##USERNAME: again, merge 'expanded' onto split, this time creating a row for each unique sex_split_id
split <- split[sex_id==3, (sex) := 1:.N, by=sex_split_id]   ##USERNAME: replaces any sex_id==3 with 1 or 2, depending on if it is the 1st or 2nd new row for the unique sex_split_id
##USERNAME: remember that here, sex="sex_id".  also there is one sex_split_id per original age-country-year row (age was split above)





#####NOTE#####
##USERNAME:replacing 'pop_scaled" with "population" cuz I think that's what the updated thing requires

                                 
###################SPLIT BY AGE AND SEX#########################################
######################################################

## Merge on population and the asp, aggregate pops by split_id
split <- merge(split, pops, by=c("location_id", "year_id", "sex_id", "age_start", "age_end"), all.x=T)  ##USERNAME: merge 'split' and 'pops' dfs
split <- merge(split, asp, by=c("sex_id", "age_start"))                                      ##USERNAME: merge 'split' and 'asp' (age sex pattern) dfs
split[, pop_group := sum(population), by="split_id"]                                         ##USERNAME: this sums all of the total pops by split_id, which should be age-sex-location-country specific


## Calculate R, the single-group age/sex estimate in population space using the age pattern from asp  ##USERNAME:should be unique for each country-year-age-sex
split[, R := rel_est * population]

## Calculate R_group, the grouped age/sex estimate in population space  ##USERNAME: this should split up estimates per country-year
split[, R_group := sum(R), by="split_id"] 

## Split ##USERNAME: check the theory behind the equation here
split[, mean := mean * (pop_group/population) * (R/R_group) ]  ##USERNAME:this multiplies the mean (ie, get(estimate)),
                                                                              ##USERNAME: by the observed ratio of the sex-age population size to the whole population size for that country-year,
                                                                              ##USERNAME: by the theoretical ratio of the sex-age population size to the whole population size for that country-year

## Split the sample size
split[, (sample_size) := sample_size * population/pop_group]       ##USERNAME: I think this step is where uncertainty gets increased as a result of splitting
## Mark as split
split[, cv_split := 1]









#############################################
## Append training, merge back metadata, clean
#############################################
## Append training, mark cv_split
out <- rbind(split, training, unsplit, fill=T)
out <- out[is.na(cv_split), cv_split := 0]  ##USERNAME: mark anything that was from the training df 

## Append on metadata
out <- merge(out, metadata, by="split_id", all.x=T)

## Clean
out <- out[, c(meta.cols, cols, "cv_split"), with=F]
out[, split_id := NULL]
#}    


############END FUNCTION#################
print("Done")






  
#####################################################################
### Run age sex splits
#####################################################################



## Split
#split <- age_sex_split(df=df, 
#                  location_id = "location_id", 
#                  year_id = "year_id", 
#                  age_start = "age_start",
#                  age_end = "age_end", 
#                  sex = "sex_id",
#                  estimate = "mean",
#                  sample_size = "sample_size",
#                 data_type= "data_type")  ##USERNAME: I'm adding this line








#####################################################################
### Clean
#####################################################################


##USERNAME:LOOK AT OUTPUT BEFORE RUNNING LINE BELOW
## Everything looks good, droppping orig. columns
split <- out[, !grep("orig.", names(out), value=T), with=F]


##USERNAME: manually dropping some cols I don't need

#split[, c("age_end", "age_end.y", "n.age", "n.sex", "age_start_floor", "age.rep", "population", "process_version_map_id", "age_group_name", "rel_est", "pop_group", "R", "R_group", "asp_weight", "age_group_id.y", "super_region_name", "region_name"):=NULL]
#setnames(split, "age_end.x", "age_end")
## Generate age_group_id


split[, age_group_id := round((age_start/5)+5)] 
split[age_start>=80, age_group_id:=30]
split[age_start>=85, age_group_id:=31]
split[age_start>=90, age_group_id:=32]
split[age_start>=95, age_group_id:=235]

## Save


##USERNAME I'm saving this age-sex split with updated age groups
write.csv(split, out_path, row.names=F)


print("Saved")

