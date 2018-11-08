rm(list=objects())
library(data.table)
library(dplyr)
library(stringr)
library(ggplot2)

## OS locals
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
} else {
  j <- "/home/j/"
}

dev <- FALSE

if (dev) {
  date <- "2018_01_29"
} else {
  args <- commandArgs(trailingOnly = TRUE)
  date <- args[1]
}

##sy:note, central functions must be used on cluster
##sy:path to R central functions
central<-paste0(j, "FILEPATH")
source(paste0(central, "get_population.R"))
source(paste0(central, "get_ids.R"))
source(paste0(central, "get_location_metadata.R"))

##########################SCRIPTS#################################
#################################################################

reload <- FALSE

if (reload) {
  
  ###################READ IN DATA/LOCS AND CLEAN#########################################
  ######################################################
  
  ###################GET AND CLEAN POPULATION ESTIMATES#########################################
  ######################################################
  pops <- get_population(location_set_id = 22, year_id = c(1980:2016), sex_id = -1, location_id = -1,
                         age_group_id = c(5:20, 30, 31, 32,235), gbd_round_id=5) %>% as.data.table
  pops<-pops[age_group_id>=30, age_group_id:=21]
  pops<-pops[, lapply(.SD, sum, na.rm=TRUE), by=c("age_group_id", "sex_id", "location_id", "year_id", "run_id") ]
  pops[, run_id:=NULL]
  ##sy: link age_group_id with age start/end
  age_groups<-get_ids(table="age_group")
  
  locs<-get_location_metadata(location_set_id=22, gbd_round_id=5)
  
  ##sy:merge on age_group ranges with 
  
  pops<-merge(pops, age_groups, by="age_group_id")
  
  ##sy:limit the pops to the age_group_ids that I model
  age_ids<-c(5:21)
  pops<-subset(pops, age_group_id %in% age_ids)
  
  ##sy:create 'age_start' and 'age_end'
  age_group_name <- as.numeric(unlist(strsplit(unique(as.character(pops$age_group_name)), " ")))
  age_start<-unlist(lapply(strsplit(as.character(pops$age_group_name), " "), "[", 1))
  age_end<-unlist(lapply(strsplit(as.character(pops$age_group_name), " "), "[", 3))
  
  pops<-cbind(pops, age_start, age_end)
  
  ##sy:fix up the last age group name
  pops<-pops[age_group_id==21, age_start:="80"]
  pops<-pops[age_group_id==21, age_end:="125"]   ##sy:I'm imposing 99 on age end so that I can do the sex split (need numeric)
  
  ##sy:make numeric
  pops<-pops[, age_start:=as.numeric(age_start)]
  pops<-pops[, age_end:=as.numeric(age_end)]

  saveRDS(pops, "FILEPATH")
}

pops <- readRDS("FILEPATH")



################### DEFINE AGE-SEX-SPLIT FUNCTION ########################################
######################################################
######################################################
######################################################

age_sex_split <- function(df, location_id, year_id, age_start, age_end, sex, estimate, sample_size) { 
  
  ###############
  ## Setup 
  ###############
  
  ## Generate unique ID for easy merging
  df[, split_id := 1:.N]
  
  ## Make sure age and sex are int
  cols <- c(age_start, age_end, sex)  
  df[, (cols) := lapply(.SD, as.integer), .SDcols=cols]
  
  ## Save original values
  orig <- c(age_start, age_end, sex, estimate, sample_size)
  orig.cols <- paste0("orig.", orig)
  df[, (orig.cols) := lapply(.SD, function(x) x), .SDcols=orig]
  
  ## Separate metadata from required variables
  cols <- c(location_id, year_id, age_start, age_end, sex, estimate, sample_size)
  meta.cols <- setdiff(names(df), cols)
  metadata <- df[, meta.cols, with=F] 
  data <- df[, c("split_id", cols), with=F]

  ###################SETUP TRAIN AND TEST DATA#########################################
  ######################################################
  
  ## Round age groups to the nearest 5-y boundary
  data[, age_start := age_start - age_start %% 5]
  data <- data[age_start > 80, (age_start) := 80]
  data[, age_end := age_end - age_end %%5 + 4]
  data <- data[age_end > 80, age_end := 84]

  training <- data[(age_end - age_start) == 4 & sex_id %in% c(1,2)]
  split <- data[(age_end - age_start) != 4 | sex_id == 3]
  
  ###################CREATE AGE-SEX PATTERN#########################################
  ######################################################
  asp <- aggregate(training[[estimate]],by=lapply(training[,c(age_start, sex), with=F],function(x)x),FUN=mean,na.rm=TRUE)

  names(asp)[3] <- "rel_est"

  asp <- dcast(asp, formula(paste0(age_start," ~ ",sex)), value.var="rel_est")
  asp[is.na(asp[[1]]), 1] <- asp[is.na(asp[[1]]),2]
  asp[is.na(asp[[2]]), 2] <- asp[is.na(asp[[2]]),1]
  asp <- melt(asp,id.var=c(age_start), variable.name=sex, value.name="rel_est")
  asp[[sex]] <- as.integer(asp[[sex]])
  
  
  
  
  ###################GRAPH AGE-SEX PATTERN#########################################
  ######################################################
  
  # asp<-as.data.table(asp)
  # asp[sex_id==1, gender:="Male"]
  # asp[sex_id==2, gender:="Female"]
  #
  # pdf(plot_output)
  # p<-ggplot(data=asp, aes(x=age_start, y=rel_est))+
  #   geom_point() +
  #   facet_wrap(~sex_id)+
  #   labs(title="Age-Sex Pattern")+
  #   xlab("Age")+
  #   ylab("BMI")+
  #   theme_minimal()
  # print(p)
  #
  # dev.off()
  
  ###################SETUP ROWS FOR SPLITTING########################################
  ######################################################
  
  split[, n.age := (age_end + 1 - age_start)/5]
  split[, n.sex := ifelse(sex_id==3, 2, 1)]
  split[, age_start_floor := age_start]          
  expanded <- rep(split$split_id, split$n.age) %>% data.table("split_id" = .)

  split <- merge(expanded, split, by="split_id", all=T)
  split[, age.rep := 1:.N - 1, by=.(split_id)]
  split[, (age_start):= age_start + age.rep * 5 ]
  split[, (age_end) :=  age_start + 4 ]

  ## Expand for sex
  split[, sex_split_id := paste0(split_id, "_", age_start)]
  expanded <- rep(split$sex_split_id, split$n.sex) %>% data.table("sex_split_id" = .)
  split <- merge(expanded, split, by="sex_split_id", all=T)
  split <- split[sex_id==3, (sex) := 1:.N, by=sex_split_id]

  ###################SPLIT BY AGE AND SEX#########################################
  ######################################################
  
  ## Merge on population and the asp, aggregate pops by split_id
  split <- merge(split, pops, by=c("location_id", "year_id", "sex_id", "age_start"), all.x=T)
  split <- merge(split, asp, by=c("sex_id", "age_start"))
  split[, pop_group := sum(population), by="split_id"]

  split[, R := rel_est * population]
  split[, R_group := sum(R), by="split_id"]

  split[, (estimate) := get(estimate) * (pop_group/population) * (R/R_group) ]

  split[, (sample_size) := sample_size * population/pop_group]
  split[, cv_split := 1]
  
  #############################################
  ## Append training, merge back metadata, clean
  #############################################
  
  ## Append training, mark cv_split
  out <- rbind(split, training, fill=T)
  out <- out[is.na(cv_split), cv_split := 0]  ##sy: mark anything that was from the training df 
  
  ## Append on metadata
  out <- merge(out, metadata, by="split_id", all.x=T)
  
  ## Clean
  out <- out[, c(meta.cols, cols, "cv_split"), with=F]
  out[, split_id := NULL]
  
}

############END FUNCTION#################

#####################################################################
#####################################################################
### Overweight Split
#####################################################################
#####################################################################

path_ow <- paste0("FILEPATH")

df <- readRDS(paste0(path_ow, "wide_full_overweight.RDS")) %>%
  mutate(
    overweight_mean = ifelse(overweight_mean == 1, 0.999, overweight_mean),
    overweight_mean = ifelse(overweight_mean == 0, 0.001, overweight_mean),
    # this section could be simplified; converting to logit space for consistency with the Stata script
    logit_variance = overweight_se^2 * (1/(overweight_mean*(1-overweight_mean))),
    variance = logit_variance / (1/(overweight_mean * (1-overweight_mean)))^2,
    year_id = floor((year_start + year_end) / 2)) %>%
  setnames(old = c("overweight_mean", "overweight_ss"), new = c("data", "sample_size")) %>%
  as.data.table(.)

## Get location id
if (!"location_id" %in% names(df)) {
  locs <- readRDS("FILEPATH/geo_hierarchy.RDS") %>% select(location_id, ihme_loc_id)
  df <- merge(df, locs, by="ihme_loc_id", all.x=T)
}

df<-df[!is.na(df$nid) & !is.na(df$data),]
df<-df[df$sample_size>=10,]
df<-df[df$data>0.01,]
df<-df[df$data<0.98 | df$location_id ==29,]
setnames(df, "data", "d")


# Reset sample size to reflect uncertainty
df$sample_size <- (df$d*(1-df$d)) / df$variance
df <- as.data.table(df)

df <- df[!is.na(location_id)]
df<-df[age_start<5, age_start:=1]

#####################################################################
### Run age sex splits
#####################################################################


ptm <- proc.time()
## Split
split <- age_sex_split(df=df, 
                       location_id = "location_id", 
                       year_id = "year_id", 
                       age_start = "age_start",
                       age_end = "age_end", 
                       sex = "sex_id",
                       estimate = "d",
                       sample_size = "sample_size")
proc.time() - ptm


#####################################################################
### Clean
#####################################################################

## Everything looks good, droppping orig. columns
split <- split[, !grep("orig.", names(split), value=T), with=F]

## Generate age_group_id  ##sy:going to have to do this manually now
split <- split[, age_group_id := round(age_start/5) + 5]
split <- split[, c("age_start", "age_end") := NULL]

out <- copy(split)

# Reset variance to reflect split sample size
out$variance <- (out$d*(1-out$d)) / out$sample_size

# Floor variance
out$variance[out$variance<0.0001]<-0.0001

setnames(out, "d", "data")
out<-out[!is.na(out$data)]

# Outlier
merge_vars<-c('location_id', 'year_id', 'age_group_id', 'sex_id')
outliers <- fread(paste0("FILEPATH/outlier_db.csv"))
outliers[, outlier_flag := 1]
## Batch outlier by NID
batch_outliers <- outliers[batch_outlier==1, .(nid, outlier_flag)]
setnames(batch_outliers, "outlier_flag", "batch_flag")
if (nrow(batch_outliers) > 0) {
  out <- merge(out, batch_outliers, by='nid', all.x=T, allow.cartesian=TRUE)
  ## Set outliers
  out <- out[batch_flag == 1, outlier := data]
  out <- out[batch_flag == 1, data := NA]
  out[, batch_flag := NULL]
}

## Specific merges
specific_outliers <- outliers[batch_outlier==0, c(merge_vars, "outlier_flag"), with=F]
setnames(specific_outliers, "outlier_flag", "specific_flag")
if (nrow(specific_outliers) > 0) {
  out <- merge(out, specific_outliers, by=merge_vars, all.x=T)
  ## Set outliers
  out <- out[specific_flag==1, outlier := data]
  out <- out[specific_flag==1, data := NA]
  out[, specific_flag:= NULL]
}

# Clean up the dataset for st-gpr modeling
out<-out[,.(nid, year_id, age_group_id, sex_id, location_id, data, variance, sample_size, cv_urbanicity, cv_diagnostic)]
out<-out[, lapply(.SD, mean, na.rm=TRUE), by=c("location_id", "year_id", "age_group_id", "sex_id", "nid", "cv_urbanicity", "cv_diagnostic"), .SDcols=c("data", "variance", "sample_size") ]

# Reset sample size to reflect final uncertainty
out$sample_size <- (out$data*(1-out$data)) / out$variance
out<-out[out$data>0.01 & out$data<1,]
out$me_name <- "metab_overweight"

write.csv(out, paste0("FILEPATH"), na="", row.names=FALSE)

#####################################################################
#####################################################################
### Obese Split
#####################################################################
#####################################################################

# Read in the data

path_ob <- paste0("FILEPATH")

df <- readRDS(paste0(path_ow, "wide_full_obese.RDS")) %>%
  mutate(
    obese_mean = ifelse(obese_mean == 1, 0.999, obese_mean),
    obese_mean = ifelse(obese_mean == 0, 0.001, obese_mean),
    # this section could be simplified; converting to logit space for consistency with the Stata script
    logit_variance = obese_se^2 * (1/(obese_mean*(1-obese_mean))),
    variance = logit_variance / (1/(obese_mean * (1-obese_mean)))^2,
    year_id = floor((year_start + year_end) / 2)) %>%
  setnames(old = c("obese_mean", "obese_ss"), new = c("data", "sample_size")) %>%
  as.data.table(.)

## Get location id
if (!"location_id" %in% names(df)) {
  locs <- readRDS("FILEPATH/geo_hierarchy.RDS") %>% select(location_id, ihme_loc_id)
  df <- merge(df, locs, by="ihme_loc_id", all.x=T)
}

# NOTE: no restriction to within 0.01 and 0.98 as with overweight
df<-df[!is.na(df$nid) & !is.na(df$data),]
df<-df[df$sample_size>=10,]
setnames(df, "data", "d")

# Reset sample size to reflect uncertainty
df$sample_size <- (df$d*(1-df$d)) / df$variance
df <- as.data.table(df)

# Get location id
if (!"location_id" %in% names(df)) {
  locs <- locs <- get_location_metadata(version_id=149)[, .(location_id, ihme_loc_id)]  
  df <- merge(df, locs, by="ihme_loc_id", all.x=T)
}
df <- df[!is.na(location_id)]
df<-df[age_start<5, age_start:=1]


#####################################################################
### Run age sex splits
#####################################################################


ptm <- proc.time()
## Split
split <- age_sex_split(df=df, 
                       location_id = "location_id", 
                       year_id = "year_id", 
                       age_start = "age_start",
                       age_end = "age_end", 
                       sex = "sex_id",
                       estimate = "d",
                       sample_size = "sample_size")
proc.time() - ptm


#####################################################################
### Clean
#####################################################################

## Everything looks good, droppping orig. columns
split <- split[, !grep("orig.", names(split), value=T), with=F]

## Generate age_group_id  ##sy:going to have to do this manually now
split <- split[, age_group_id := round(age_start/5) + 5]
split <- split[, c("age_start", "age_end") := NULL]

out <- copy(split)

# Reset variance to reflect split sample size
out$variance <- (out$d*(1-out$d)) / out$sample_size

# Floor variance
out$variance[out$variance<0.0001]<-0.0001

setnames(out, "d", "data")
#setnames(out, "smaller_site_unit", "cv_smaller_site_unit")
out<-out[!is.na(out$data)]

# Outlier
merge_vars<-c('location_id', 'year_id', 'age_group_id', 'sex_id')
outliers <- fread(paste0("FILEPATH/outlier_db.csv"))
outliers[, outlier_flag := 1]
## Batch outlier by NID
batch_outliers <- outliers[batch_outlier==1, .(nid, outlier_flag)]
setnames(batch_outliers, "outlier_flag", "batch_flag")
if (nrow(batch_outliers) > 0) {
  out <- merge(out, batch_outliers, by='nid', all.x=T, allow.cartesian=TRUE)
  ## Set outliers
  out <- out[batch_flag == 1, outlier := data]
  out <- out[batch_flag == 1, data := NA]
  out[, batch_flag := NULL]
}
## Specific merges
specific_outliers <- outliers[batch_outlier==0, c(merge_vars, "outlier_flag"), with=F]
setnames(specific_outliers, "outlier_flag", "specific_flag")
if (nrow(specific_outliers) > 0) {
  out <- merge(out, specific_outliers, by=merge_vars, all.x=T)
  ## Set outliers
  out <- out[specific_flag==1, outlier := data]
  out <- out[specific_flag==1, data := NA]
  out[, specific_flag:= NULL]
}
ow <- fread(paste0("FILEPATH"))
ow<-ow[,.(location_id, year_id, age_group_id, sex_id, data)]
setnames(ow, "data", "ow")
out<-merge(out, ow, by=c("location_id", "year_id", "age_group_id", "sex_id"))

# generate proportion
out$data <- out$data / out$ow

# Reset variance to reflect split sample size and new proportion space
out$variance <- (out$data*(1-out$data)) / out$sample_size

# Floor variance
out$variance[out$variance<0.0001]<-0.0001

# Clean up the dataset for st-gpr modeling
out<-out[,.(nid, year_id, age_group_id, sex_id, location_id, data, variance, sample_size, cv_urbanicity, cv_diagnostic)]
out<-out[, lapply(.SD, mean, na.rm=TRUE), by=c("location_id", "year_id", "age_group_id", "sex_id", "nid", "cv_urbanicity", "cv_diagnostic"), .SDcols=c("data", "variance", "sample_size") ]

# Reset sample size to reflect final uncertainty
out$sample_size <- (out$data*(1-out$data)) / out$variance

#out<-out[data>0.0001 & data < 0.95]
out<-out[out$data>0.001 & out$data<0.99,]
out$me_name <- "metab_obese"

write.csv(out, paste0("FILEPATH"), na="", row.names=FALSE)

#####################################################################
#####################################################################
### Mean BMI Split
#####################################################################
#####################################################################

# Read in the data

path_bmi <- paste0("FILEPATH")
df_in <- readRDS(paste0(path_bmi, "wide_full_bmi.RDS"))
table(df_in$bmi_mean < 10); table(df_in$bmi_mean > 70)

df <- df_in %>%
  filter(bmi_mean >= 10 & bmi_mean < 70) %>%
  mutate(
    variance = bmi_se^2,
    year_id = floor((year_start + year_end) / 2)) %>%
  setnames(old = c("bmi_mean", "bmi_ss"), new = c("data", "sample_size")) %>%
  as.data.table(.)

## Get location id
if (!"location_id" %in% names(df)) {
  locs <- readRDS("FILEPATH/geo_hierarchy.RDS") %>% select(location_id, ihme_loc_id)
  df <- merge(df, locs, by="ihme_loc_id", all.x=T)
}

# NOTE: no restriction to within 0.01 and 0.98 as with overweight
df<-df[!is.na(df$nid) & !is.na(df$data),]
df<-df[df$sample_size>=10,]
setnames(df, "data", "d")

# Reset sample size to reflect uncertainty
df <- as.data.table(df)

# Get location id
if (!"location_id" %in% names(df)) {
  locs <- locs <- get_location_metadata(version_id=149)[, .(location_id, ihme_loc_id)]  
  df <- merge(df, locs, by="ihme_loc_id", all.x=T)
}
df <- df[!is.na(location_id)]
df<-df[age_start<5, age_start:=1]


#####################################################################
### Run age sex splits
#####################################################################


ptm <- proc.time()
## Split
split <- age_sex_split(df=df, 
                       location_id = "location_id", 
                       year_id = "year_id", 
                       age_start = "age_start",
                       age_end = "age_end", 
                       sex = "sex_id",
                       estimate = "d",
                       sample_size = "sample_size")
proc.time() - ptm


#####################################################################
### Clean
#####################################################################

## Everything looks good, droppping orig. columns
split <- split[, !grep("orig.", names(split), value=T), with=F]

## Generate age_group_id  ##sy:going to have to do this manually now
split <- split[, age_group_id := round(age_start/5) + 5]
split <- split[, c("age_start", "age_end") := NULL]

out <- copy(split)

# Reset variance to reflect split sample size
out$variance <- (out$d*(1-out$d)) / out$sample_size

# Floor variance
out$variance[out$variance<0.0001]<-0.0001

setnames(out, "d", "data")
out<-out[!is.na(out$data)]

# Outlier
merge_vars<-c('location_id', 'year_id', 'age_group_id', 'sex_id')
outliers <- fread(paste0("FILEPATH/outlier_db.csv"))
outliers[, outlier_flag := 1]
## Batch outlier by NID
batch_outliers <- outliers[batch_outlier==1, .(nid, outlier_flag)]
setnames(batch_outliers, "outlier_flag", "batch_flag")
if (nrow(batch_outliers) > 0) {
  out <- merge(out, batch_outliers, by='nid', all.x=T, allow.cartesian=TRUE)
  ## Set outliers
  out <- out[batch_flag == 1, outlier := data]
  out <- out[batch_flag == 1, data := NA]
  out[, batch_flag := NULL]
}

## Specific merges
specific_outliers <- outliers[batch_outlier==0, c(merge_vars, "outlier_flag"), with=F]
setnames(specific_outliers, "outlier_flag", "specific_flag")
if (nrow(specific_outliers) > 0) {
  out <- merge(out, specific_outliers, by=merge_vars, all.x=T)
  ## Set outliers
  out <- out[specific_flag==1, outlier := data]
  out <- out[specific_flag==1, data := NA]
  out[, specific_flag:= NULL]
}

# Clean up the dataset for st-gpr modeling
out<-out[,.(nid, year_id, age_group_id, sex_id, location_id, data, variance, sample_size, cv_urbanicity, cv_diagnostic)]
out<-out[, lapply(.SD, mean, na.rm=TRUE), by=c("location_id", "year_id", "age_group_id", "sex_id", "nid", "cv_urbanicity", "cv_diagnostic"), .SDcols=c("data", "variance", "sample_size") ]

# Reset sample size to reflect final uncertainty
out$sample_size <- (out$data*(1-out$data)) / out$variance
out<-out[out$data>0.01 & out$data<1,]
out$me_name <- "metab_bmi"

write.csv(out, paste0("FILEPATH"), na="", row.names=FALSE)

