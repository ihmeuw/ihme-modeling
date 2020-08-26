rm(list=objects())
library(data.table)
library(plyr)
library(dplyr)
library(stringr)
library(ggplot2)
library(base)

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- FILEPATH
share_dir <- FILEPATH # only accessible from the cluster
code_dir <- if (os == "Linux") FILEPATH else if (os == "Windows") ""

##sy:note, central functions must be used on cluster
##sy:path to R central functions
source("FILEPATH/primer.R")
central <- "FILEPATH"
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Parent directory where intermediate data will be stored",
                    default = FILEPATH, type = "character")
parser$add_argument("--decomp_step", help = "Decomp step we're uploading for",
                    default = "step2", type = "character")
parser$add_argument("--ow_bundle_version_id", help = "Prevalence overweight bundle version id being processed",
                    default = 5498, type = "integer")
parser$add_argument("--ob_bundle_version_id", help = "Prevalence obese bundle version id being processed",
                    default = 5501, type = "integer")

args <- parser$parse_args()
list2env(args, environment()); rm(args)


##########################SCRIPTS#################################
#################################################################

reload <- F

out_dir <- sprintf(FILEPATH)

if (reload) {
  
  ###################READ IN DATA/LOCS AND CLEAN#########################################
  ######################################################
  
  ###################GET AND CLEAN POPULATION ESTIMATES#########################################
  ######################################################
  pops <- get_population(location_set_id = 22, year_id = c(1980:2019), sex_id = -1, location_id = -1,
                         age_group_id = c(5:20, 30, 31, 32,235), decomp_step = decomp_step, gbd_round_id=6) %>% as.data.table
  pops<-pops[age_group_id>=30, age_group_id:=21]
  pops<-pops[, lapply(.SD, sum, na.rm=TRUE), by=c("age_group_id", "sex_id", "location_id", "year_id", "run_id") ]
  pops[, run_id:=NULL]
  ##sy: link age_group_id with age start/end
  age_groups<-get_ids(table="age_group")
  
  locs<-get_location_metadata(location_set_id=22, gbd_round_id=6)
  
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
  pops<-pops[age_group_id==21, age_end:="125"]
  
  ##sy:make numeric
  pops<-pops[, age_start:=as.numeric(age_start)]
  pops<-pops[, age_end:=as.numeric(age_end)]

  saveRDS(pops, sprintf("%s/step_6_agesex_split_pops.RDS", out_dir))
}

locs <- get_locations()
pops <- readRDS(sprintf("%s/step_6_agesex_split_pops.RDS", out_dir))
ages <- get_age_map(type = 'all')[age_group_id %in% c(5:21), .(age_group_id, age_start = age_group_years_start, age_end = age_group_years_end -1)]

################### DEFINE AGE-SEX-SPLIT FUNCTION ########################################
######################################################
######################################################
######################################################

age_sex_split <- function(df, location_id, year_id, age_start, age_end, sex, estimate, sample_size, out_dir) { 
  
  ###############
  ## Setup 
  ###############
  
  # Save seqs coming in to make sure nothing dropped
  seq_in <- unique(df$seq)
  
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
  data[, age_start := ifelse(age_start >= 5, round_any(age_start, 5, floor), 1)]
  data <- data[age_start > 80, (age_start) := 80]
  data[, age_end := round_any(age_end, 5, floor) + 4]
  data <- data[age_end > 80, age_end := 84]

  training <- data[sex_id %in% c(1,2)][(age_end - age_start == 4)|(age_start == 1 & age_end - age_start == 3)]
  split <- data[!split_id %in% training$split_id]
  
  if((nrow(training) + nrow(split)) != length(seq_in)) stop("Number of rows in training and split sets inconsistent with number of rows from input df")
  
  ###################CREATE AGE-SEX PATTERN#########################################
  ######################################################
  if (decomp_step == 2) {
    asp <- aggregate(training[[estimate]],by=lapply(training[,c(age_start, sex), with=F],function(x)x),FUN=mean,na.rm=TRUE)
  
    names(asp)[3] <- "rel_est"
  
    asp <- dcast(asp, formula(paste0(age_start," ~ ",sex)), value.var="rel_est")
    asp[is.na(asp[[1]]), 1] <- asp[is.na(asp[[1]]),2]
    asp[is.na(asp[[2]]), 2] <- asp[is.na(asp[[2]]),1]
    asp <- melt(asp,id.var=c(age_start), variable.name=sex, value.name="rel_est")
    asp[[sex]] <- as.integer(asp[[sex]])
    write.csv(sprintf("%s/step2_age_sex_pattern.csv", out_dir))
  } else {
    asp <- read.csv(sprintf("%s/step2_age_sex_pattern.csv", out_dir))
  }
  
  ###################SET UP ROWS FOR SPLITTING####################################
  ######################################################
  
  split[, n.age := (age_end + 1 - age_start)/5]
  split[, n.sex := ifelse(sex_id==3, 2, 1)]
  split[, age_start_floor := age_start]          
  expanded <- rep(split$split_id, split$n.age) %>% data.table("split_id" = .)

  split <- merge(expanded, split, by="split_id", all=T)
  split[, age.rep := 1:.N - 1, by=.(split_id)]
  split[, (age_start):= ifelse(age_start == 1, age_start + age.rep * 5 -1, age_start + age.rep * 5)][age_start == 0, age_start := 1]
  split[, (age_end) :=  ifelse(age_start == 1, age_start + 4 - 1, age_start + 4)]

  ## Expand for sex
  split[, sex_split_id := paste0(split_id, "_", age_start)]
  expanded <- rep(split$sex_split_id, split$n.sex) %>% data.table("sex_split_id" = .)
  split <- merge(expanded, split, by="sex_split_id", all=T)
  split <- split[sex_id==3, (sex) := 1:.N, by=sex_split_id]

  ###################SPLIT BY AGE AND SEX#########################################
  ######################################################
  
  ## Merge on population and the asp, aggregate pops by split_id
  split <- merge(split, pops, by=c("location_id", "year_id", "sex_id", "age_start"), all.x=T)
  if(any(is.na(split$population))) stop("Missing populations after merge. Something likely wrong with groups. Double check")
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
  
  ## Check all data accounted for
  stopifnot(all(seq_in %in% unique(out$seq)))
  
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

# Read in the data and calculate the variance:

path_ow <- FILEPATH
orig_names <- fread(sprintf("%s/bundle_version_%d.csv", path_ow, ow_bundle_version_id)) %>% names(.)

df <- fread(sprintf("%s/bundle_version_%d.csv", path_ow, ow_bundle_version_id)) %>%
  mutate(
    overweight_mean = ifelse(overweight_mean == 1, 0.999, overweight_mean),
    overweight_mean = ifelse(overweight_mean == 0, 0.001, overweight_mean),
    logit_variance = overweight_se^2 * (1/(overweight_mean*(1-overweight_mean))),
    variance = logit_variance / (1/(overweight_mean * (1-overweight_mean)))^2,
    year_id = floor((year_start + year_end) / 2)) %>%
  setnames(old = c("overweight_mean", "overweight_ss"), new = c("data", "sample_size")) %>%
  as.data.table(.)

## Get location id
if (!"location_id" %in% names(df)) {
  locs <- get_locations(level = "all") %>% .[, .(location_id, ihme_loc_id)]
  df <- merge(df, locs, by="ihme_loc_id", all.x=T)
}

df<-df[!is.na(df$nid) & !is.na(df$data),]
df<-df[df$sample_size>=10,]
df<-df[df$data>0.01,]
df<-df[df$data<0.99 | df$location_id ==29,]
setnames(df, "data", "d")




# For sources where single-year age groups have been extracted, multiple points being mapped to age group id, need to aggregate where possible. 
# Also need to aggregate sources where age range provided may be granular above the level of the terminal age group in the age split function. This should ideally be identified and done within the function, but keeping it simple for now
df[, short_age_grp := age_start > 1 & (age_end - age_start) < 4][, above_terminal_age := age_start >= 80]

age_agg_df <- df[short_age_grp == T|above_terminal_age == T]
age_agg_df[, age_agg_grp := round_any(age_start, 5, floor)] # this may not work as intended for data where the range of age_start and age_end in a given row is less than five years but split across GBD age groups

age_agg_df <- age_agg_df[, .(d = weighted.mean(d, sample_size), sample_size = sum(sample_size), overweight_se = sqrt(sum(overweight_se^2)),  age_start = min(age_start), age_end = max(age_end), seq = max(seq), cv_age_aggregated = 1), 
           by = .(nid, ihme_loc_id, location_id, sex_id, year_start, year_end, year_id, cv_urbanicity, cv_diagnostic, bundle_id, bundle_version_id, me_name)]
age_agg_df[, c('logit_variance') := .(overweight_se^2 * (1/(d*(1-d))))][, c('variance') := .(logit_variance / (1/(d * (1-d)))^2)]

df <- rbind(df[short_age_grp == F & above_terminal_age == F, !c('short_age_grp', 'above_terminal_age')], age_agg_df, use.names = T, fill = T)
df[is.na(cv_age_aggregated), cv_age_aggregated := 0]


# Reset sample size to reflect uncertainty
df$sample_size <- (df$d*(1-df$d)) / df$variance

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
                       sample_size = "sample_size",
                       out_dir = out_dir)
proc.time() - ptm


#####################################################################
### Clean
#####################################################################

## Everything looks good, dropping orig. columns
split <- split[, !grep("orig.", names(split), value=T), with=F]

## Generate age_group_id
split <- merge(split, ages[, !'age_end'], by = 'age_start')
split[, c("age_start", "age_end") := NULL]

out <- copy(split)

# Reset variance to reflect split sample size
out$variance <- (out$d*(1-out$d)) / out$sample_size

# Floor variance
out$variance[out$variance<0.0001]<-0.0001

setnames(out, "d", "data")
out<-out[!is.na(out$data)]

# Outlier
merge_vars<-c('location_id', 'year_id', 'age_group_id', 'sex_id')
outliers <- fread(FILEPATH)
outliers[, outlier_flag := 1]
## Batch outlier by NID
batch_outliers <- outliers[batch_outlier==1, .(nid, outlier_flag)]
setnames(batch_outliers, "outlier_flag", "batch_flag")
if (nrow(batch_outliers) > 0) {
  out <- merge(out, batch_outliers, by='nid', all.x=T, allow.cartesian=TRUE)
  ## Set outliers
  out <- out[batch_flag == 1, outlier := data]
  out <- out[batch_flag == 1, is_outlier := 1]
  out[, batch_flag := NULL]
}

## Specific merges
specific_outliers <- outliers[batch_outlier==0, c(merge_vars, "outlier_flag"), with=F]
setnames(specific_outliers, "outlier_flag", "specific_flag")
if (nrow(specific_outliers) > 0) {
  out <- merge(out, specific_outliers, by=merge_vars, all.x=T)
  ## Set outliers
  out <- out[specific_flag==1, outlier := data]
  out <- out[specific_flag==1, is_outlier := 1]
  out[, specific_flag:= NULL]
}

out[is.na(is_outlier), is_outlier := 0]

# Keeping only columns we need
out <- out[, .(bundle_id, bundle_version_id, me_name, seq, nid, location_id, year_start, year_end, year_id, age_group_id, sex_id, data, variance, sample_size, cv_urbanicity, cv_diagnostic, cv_age_aggregated, cv_split, is_outlier)]

# Removing any duplicates
out <- unique(out)

# Reset sample size to reflect final uncertainty
out$sample_size <- (out$data*(1-out$data)) / out$variance
out[data < .01|data>1, is_outlier := 1]


write.csv(out, sprintf('FILEPATH/bundle_version_%d.csv', ow_bundle_version_id), row.names=FALSE)

#####################################################################
#####################################################################
### Obese Split
#####################################################################
#####################################################################

# Read in the data

path_ob <- FILEPATH
df <- fread(sprintf("%s/bundle_version_%d.csv", path_ob, ob_bundle_version_id)) %>%
  mutate(
    obese_mean = ifelse(obese_mean == 1, 0.999, obese_mean),
    obese_mean = ifelse(obese_mean == 0, 0.001, obese_mean),
    logit_variance = obese_se^2 * (1/(obese_mean*(1-obese_mean))),
    variance = logit_variance / (1/(obese_mean * (1-obese_mean)))^2,
    year_id = floor((year_start + year_end) / 2)) %>%
  setnames(old = c("obese_mean", "obese_ss"), new = c("data", "sample_size")) %>%
  as.data.table(.)

## Get location id
if (!"location_id" %in% names(df)) {
  locs <- get_locations(level = "all") %>% .[, .(location_id, ihme_loc_id)]
  df <- merge(df, locs, by="ihme_loc_id", all.x=T)
}

# NOTE: no restriction to within 0.01 and 0.98 as with overweight
df<-df[!is.na(df$nid) & !is.na(df$data),]
df<-df[df$sample_size>=10,]
setnames(df, "data", "d")

# For sources where single-year age groups have been extracted, multiple points being mapped to age group id, need to aggregate where possible.
# Also need to aggregate sources where age range provided may be granular above the level of the terminal age group in the age split function. This should ideally be identified and done within the function, but keeping it simple for now.
df[, short_age_grp := age_start > 1 & (age_end - age_start) < 4][, above_terminal_age := age_start >= 80]

age_agg_df <- df[short_age_grp == T|above_terminal_age == T]
age_agg_df[, age_agg_grp := round_any(age_start, 5, floor)] # this may not work as intended for data where the range of age_start and age end in a given row is less than five years but split across GBD age groups

age_agg_df <- age_agg_df[, .(d = weighted.mean(d, sample_size), sample_size = sum(sample_size), obese_se = sqrt(sum(obese_se^2)),  age_start = min(age_start), age_end = max(age_end), seq = max(seq), cv_age_aggregated = 1), 
                         by = .(nid, ihme_loc_id, location_id, sex_id, year_start, year_end, year_id, cv_urbanicity, cv_diagnostic, bundle_id, bundle_version_id, me_name)]
age_agg_df[, c('logit_variance') := .(obese_se^2 * (1/(d*(1-d))))][, c('variance') := .(logit_variance / (1/(d * (1-d)))^2)]

df <- rbind(df[short_age_grp == F & above_terminal_age == F, !c('short_age_grp', 'above_terminal_age')], age_agg_df, use.names = T, fill = T)
df[is.na(cv_age_aggregated), cv_age_aggregated := 0]

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
                       sample_size = "sample_size",
                       out_dir = out_dir)
proc.time() - ptm


#####################################################################
### Clean
#####################################################################

## Everything looks good, dropping orig. columns
split <- split[, !grep("orig.", names(split), value=T), with=F]

## Generate age_group_id
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
outliers <- fread(paste0(j, "FILEPATH/outlier_db.csv"))
outliers[, outlier_flag := 1]
## Batch outlier by NID
batch_outliers <- outliers[batch_outlier==1, .(nid, outlier_flag)]
setnames(batch_outliers, "outlier_flag", "batch_flag")
if (nrow(batch_outliers) > 0) {
  out <- merge(out, batch_outliers, by='nid', all.x=T, allow.cartesian=TRUE)
  ## Set outliers
  out <- out[batch_flag == 1, outlier := data]
  out <- out[batch_flag == 1, is_outlier := 1]
  out[, batch_flag := NULL]
}
## Specific merges
specific_outliers <- outliers[batch_outlier==0, c(merge_vars, "outlier_flag"), with=F]
setnames(specific_outliers, "outlier_flag", "specific_flag")
if (nrow(specific_outliers) > 0) {
  out <- merge(out, specific_outliers, by=merge_vars, all.x=T)
  ## Set outliers
  out <- out[specific_flag==1, outlier := data]
  out <- out[specific_flag==1, is_outlier := 1]
  out[, specific_flag:= NULL]
}

out[is.na(is_outlier), is_outlier := 0]

ow <- fread(sprintf('FILEPATH/bundle_version_%d.csv', ow_bundle_version_id))
ow<-ow[,.(nid, location_id, year_id, age_group_id, sex_id, cv_urbanicity, cv_diagnostic, cv_age_aggregated, cv_split, data)]
setnames(ow, "data", "ow")
out<-merge(out, ow, by=c("location_id", "year_id", "age_group_id", "sex_id", "nid", 'cv_urbanicity', 'cv_diagnostic', 'cv_age_aggregated', 'cv_split'), all.x = T)

# For data that don't have exact match within study, see if exact loc year age sex match available
out_rematch <- merge(out[is.na(ow), !'ow'], ow[, !c('nid', grep("cv", names(ow), value = T)), with = F], by = c('location_id', 'age_group_id', 'sex_id', 'year_id'), all.x = T)
out_rematch[, cv_rematched := 1]

# Append rematched and no match ob data to originally matched data
out <- rbind(out[!is.na(ow)], out_rematch, use.names = T, fill = T)
out[is.na(cv_rematched), cv_rematched := 0]

# Drop data that has no match
out <- out[!is.na(ow)]

# Drop data where prevalence of obesity greater than prevalence of overweight
out <- out[ow >= data]

# generate proportion
out$data <- out$data / out$ow

# For cases where multiple matches in rematched dataset, take ss-weighted average across them for now, so no duplicates
dup_seqs <- out[cv_split == 0 & is_outlier == 0, .N, by = .(seq)][N != 1]$seq
collapsed_seqs <- out[seq %in% dup_seqs, .(data = mean(data)), by = eval(names(out)[!names(out) %in% c("data", "ow")])]
out <- rbind(out[!seq %in% dup_seqs], collapsed_seqs, use.names = T, fill = T)

# Reset variance to reflect split sample size and new proportion space
out$variance <- (out$data*(1-out$data)) / out$sample_size

# Floor variance
out$variance[out$variance<0.0001]<-0.0001

# Clean up the dataset for st-gpr modeling
# Keeping only columns we need
out <- out[, .(bundle_id, bundle_version_id, me_name, seq, nid, location_id, year_start, year_end, year_id, age_group_id, sex_id, data, variance, sample_size, cv_urbanicity, cv_diagnostic, cv_age_aggregated, cv_split, cv_rematched, is_outlier)]

# Get rid of duplicates (not sure where they're coming from)
out <- unique(out)

# Reset sample size to reflect final uncertainty
out$sample_size <- (out$data*(1-out$data)) / out$variance

out[data<0.001| data>0.99, is_outlier := 1]
out[, me_name := 'prop_ow_ob']

write.csv(out, sprintf('FILEPATH/bundle_version_%d.csv', ob_bundle_version_id), row.names=FALSE)

