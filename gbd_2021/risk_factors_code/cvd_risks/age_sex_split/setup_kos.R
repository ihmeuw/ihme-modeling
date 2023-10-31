####################
##Author: USERNAME
##Date: DATE
##Purpose: Set-up holdout data to input the age-sex splitter
##Notes:
########################

setup_split_data<-function(me, mod_name,
                           data_folder, output_folder,
                           validate=F, kos=NA, decomp_step){
  os <- .Platform$OS.type
  if (os=="windows") {
    j<- "FILEPATH"
    h<-"FILEPATH"
  } else {
    j<- "FILEPATH"
    h<-"FILEPATH"
  }
  
  
  date<-gsub("-", "_", Sys.Date())
  
  
  library(ggplot2)
  library(data.table)
  require(dplyr)
  library(RMySQL)
  library(readxl)
  library(dbplyr)
  
  ################### SCRIPTS #########################################
  ######################################################
  
  central<-"FILEPATH"
  
  source("FILEPATH/get_bundle_version.R")
  source(paste0(j, "FILEPATH/get_recent.R"))  ##USERNAME: my function for getting most recent data
  source(paste0(central, "get_location_metadata.R"))
  source(paste0(central, "get_population.R"))
  source(paste0(j, "FILEPATH/job_hold.R"))  ##USERNAME: my function for getting most recent data
  source("/FILEPATH/make_ko.R")  ##USERNAME: my function for getting most recent data
  source("/FILEPATH/get_user_input.R")
  
  ################### PATHS AND ARGS #########################################
  ######################################################
  ##args
  if(F){
    me<-"sbp"
    validate<-T
    kos<-5
    mod_name<-"crossval"
    data_folder<-paste0(j, "FILEPATH/", me, "_to_split/")
    output_folder<-paste0("/FILEPATH/", me, "/ko_input_data/")  ##USERNAME: this should change if not saving KOs
  }
  
  
  ##paths
  
  # offline_path<-("FILEPATH/sbp_to_split.rds")
  # central<-"/FILEPATH/"
  
  dir.create(output_folder, recursive=T)
  
  ################### GET DATA #########################################
  ######################################################
  
  if (me == 'sbp') {
    bundle_version_id <- get_id_from_user(me, 'bundle_version_id')
    fetch_type <- get_fetch_from_user()
    df <- get_bundle_version(bundle_version_id, fetch_type)
    # Use original age start and end pairs instead of age start and age end pairs calculated by 
    # bundle upload using age group ID 
    df[,'age_start'] <- df[,'age_start_orig']
    df[,'age_end'] <- df[,'age_end_orig']
  } else if (me == 'ldl') {
    # Get crosswalked data 
    df <-get_recent(data_folder, pattern=paste0("decomp", decomp_step))
    #df <- fread("/FILEPATH/to_split_decompiterative_2020_03_29.csv")
  }
  
  message(" Getting locs...")
  locs<-get_location_metadata(location_set_id=22, gbd_round_id=7)[ ,.(ihme_loc_id, super_region_id, region_id)]
  print("  Done getting locs")
  #if("location_id" %in% names(df)){df[, location_id:=NULL]}
  df<-merge(df, locs, by=c("ihme_loc_id"))
  # Add sex_id col
  if (!('sex_id' %in% colnames(df))) {
    df <- transform(df, sex_id=ifelse(sex=='Male', 1, ifelse(sex=="Female", 2, ifelse(sex=="Both", 3, NA)))) #fixing so sex=="Both" = sex_id==3
  }
  
  if ('val' %in% colnames(df)) {
    df <- setnames(df, 'val', 'mean')
  }
  
  if(T){
    location_id <- "location_id"
    year_id <- "year_id"
    estimate <- "mean"
    sample_size <- "sample_size"
    data_type<- "data_type"
  }
  
  ##USERNAME: drop duplicates
  dupes<-df[duplicated(df[,c('location_id', 'sex_id', 'year_id', 'nid', 'age_start', 'age_end', 'mean')]),]
  df<-unique(df)
  
  #df <- as.data.table(df)
  
  ## Generate unique ID for easy merging
  #df[, split_id := 1:.N]  ##USERNAME: .N means the number of instances, ie, the #of rows.  This is making a column with values= row number
  
  ## Make sure age and sex are int
  cols <- c("age_start", "age_end", "sex_id", "nid", "sample_size")
  df[, (cols) := lapply(.SD, as.integer), .SDcols=cols]  ##USERNAME: data.table syntax, read: 'apply function as.integer over the columns in .SDcols
  
  ## Save original values
  orig <- c("age_start", "age_end", "sex_id", "mean", "sample_size")
  orig.cols <- paste0("orig.", orig)
  df[, (orig.cols) := lapply(.SD, function(x) x), .SDcols=orig]  ##USERNAME:creates columns orig.age_start, orig.sex_id, etc...
  
  ## Separate metadata from required variables
  cols <- c("location_id", "year_id", "age_start", "age_end", "sex_id", "mean", "sample_size", "data_type", "nid", "standard_error")
  
  meta.cols <- setdiff(names(df), cols)
  metadata <- df[, meta.cols, with=F]
  # data <- df[, c("split_id", cols), with=F]  #
  data<-copy(df)
  
  ################### IMPUTE NA SE #########################################
  ################################################################
  ##USERNAME: impute standard error where it is missing (give .98 percentile)
  
  if(sum(nrow(data[is.na(standard_error)])+nrow(data[standard_error==0]))>0){
    message(paste0("Some input data points to be split are missing standard error, giving these values the 98th percentile standard error "))
    
    miss_ses<-sum(nrow(data[is.na(standard_error)])+nrow(data[standard_error==0]))
    miss_ss<-sum(nrow(data[is.na(sample_size)])+nrow(data[sample_size==0]))
    miss_sds<-sum(nrow(data[is.na(standard_deviation)])+nrow(data[standard_deviation==0]))
    # high_se<-quantile(data$standard_error, probs=.98, na.rm=T)
    # data[is.na(standard_error) | standard_error==0, standard_error:=high_se]
    
    ##USERNAME: if sample size is missing, give 5th percentile SS
    five_ss<-quantile(data$sample_size, probs=0.05, na.rm=T)
    data[is.na(sample_size), sample_size:=five_ss]
    
    ##USERNAME: calculate SE for rows missing SE but not SD
    data[is.na(standard_error) & !is.na(standard_deviation), standard_error:=standard_deviation/sqrt(sample_size)]
    
    ##USERNAME: bring in SD model and predict
    #load(sd_mod_path, verbose=T)
    sd_mod<-get_recent(sd_mod_path, pattern=paste0("decomp", decomp_step))
    
    ##USERNAME: need to give an age group id and a sex id for prediction, use males and oldest age group to max se
    data[, age_group_id:=235]
    data[, sex_id_real:=sex_id]
    data[, sex_id:=1]
    data[, log_mean := log(mean)]
    
    ##USERNAME: predict SD and calculate SE
    sds<-exp(predict.lm(sd_mod, newdata=data[is.na(standard_error) & is.na(standard_deviation)]))
    ses<-sds/sqrt(data[is.na(standard_error) & is.na(standard_deviation), sample_size])
    data[is.na(standard_error) & is.na(standard_deviation), standard_error:=ses]
    
    neg_sds<-nrow(data[standard_error<0])
    if(neg_sds>0){stop("There are ", neg_sds, " rows of data with negative standard deviations, please check these!")}
    
    message(paste0("  ", miss_ss, " had imputed sample size"))
    message(paste0("  ", miss_ses, " had predicted standard errors"))
    message(paste0("  Predicted standard errors had mean: ", mean(ses)))
    #message(paste0("  98th percentile standard error used was ", high_se))
    
    ##USERNAME:limit extreme standard_errors
    maxvar<-quantile(data$standard_error, probs=.999)
    minvar<-quantile(data$standard_error, probs=.01)
    data[standard_error>maxvar, standard_error:=maxvar]
    data[standard_error<minvar, standard_error:=minvar]
    
    ##USERNAME: switch back sex_id
    data[, log_mean := NULL]
    data[, sex_id:=sex_id_real]
    data[, sex_id_real:=NULL]
    data[, age_group_id:=NULL]
  }
  
  
  
  ################### SETUP TRAIN AND TEST DATA #########################################
  ######################################################
  message(" Prepping dataset...")
  ## Round age groups to the nearest 5-y boundary
  data[, age_start := age_start - age_start %% 5]  ##USERNAME:this arithmetic takes age start (say 17), and subtracts from it modulus 5 of that age (2), yields 5 year age start.
  
  ##USERNAME: update on this-when adjusting below, there are a handful of cases where age_start>age_end
  data[age_start > 95, age_start := 95]  ##USERNAME:updated DATE to reflect new age groups (95 and 99)
  data[, age_end := age_end - age_end %%5 + 4]     ##USERNAME: makes last age group 4+ modulus 5 difference
  data[age_end > 99, age_end := 99]       ##USERNAME:updated DATE to reflect new age groups (95 and 99)
  
  if(validate==F){  ##USERNAME: then save the dataset in this format
    data[data_type==1, to_train:=1]
    
    ##USERNAME: drop implausible vals
    if(me=="sbp"){
      data[to_train==1 & age_start<5 & mean>125, to_train:=0]
    }
    
    
    data[data_type!=1  &  ((age_end - age_start) != 4 | sex_id == 3), to_split:=1]
    setnames(data, "mean", "data")
    #saveRDS(data, paste0(output_folder, mod_name, "_data.rds"))
    #saveRDS(metadata, paste0(output_folder, mod_name, "_metadata.rds")) ##USERNAME: save this later in the setup_split_data script
    
    return(list(data = data))
    message("Done saving data")
  }else{
    
    ##USERNAME: create aggregated columns
    unsplit <- data[data_type!=1 & ((age_end - age_start) == 4   |   age_start>=95) & sex_id %in% c(1,2)]  ##USERNAME: store these rows for merging on later... are neither test nor train
    training<-data[data_type==1,]
    
    
    
    
    ################### CREATE HOLDOUTS #########################################
    ######################################################
    
    message("Creating holdouts..")
    
    ##USERNAME: treat each src_loc_yr as a unique NID
    training[, src_loc_yr:=paste0(nid, "_", location_id, "_", year_id)]
    setnames(training, c("nid", "src_loc_yr"), c("og_nid", "nid"))
    
    setnames(training, "mean", "data")
    training[, age_group_id := round((age_start/5)+5)]
    training[age_start>=80, age_group_id:=30]
    training[age_start>=85, age_group_id:=31]
    training[age_start>=90, age_group_id:=32]
    training[age_start>=95, age_group_id:=235]
    
    ko_items<-prep_ko(training, by_sex=0)
    training<-get_kos(ko_items[[1]], ko_items[[2]], ko_items[[3]], ko_items[[4]],
                      drop_nids=T, prop_to_hold=0.2, seed=20)
    
    
    
    ################### CONSTRUCT FAKE DATASETS #########################################
    ######################################################
    
    for(ko in 1:kos){
      message("Saving ko ", ko)
      
      message(" Replicating age-sex ranges to be split")
      ##USERNAME: replicate age-sex ranges that need to be split
      data[data_type!=1  &  ((age_end - age_start) != 4 | sex_id == 3), to_split:=1]
      split_patterns<-data[ ,.(count=sum(to_split)), by=.(age_start, age_end, sex_id)]
      split_patterns[, prop:=count/sum(count, na.rm=T)]
      
      
      
      
      training.t<-training[get(paste0("ko", ko))==0,]
      
      ##USERNAME: construct fake data points to be split from ko data
      split.t<-training[get(paste0("ko", ko))==1,]
      split_patterns[, new_count:=ceiling(prop*nrow(split.t))]
      
      ##USERNAME: mark split.t rows to be split based on split_patterns
      split_list<-list()
      for(n in 1:nrow(split_patterns)){
        as_range<-split_patterns[n]
        if(!is.na(as_range$new_count)){
          
          ##USERNAME: identify which sources fit the criteria
          if(as_range$sex_id==3){
            nids<-split.t[age_start==as_range$age_start, unique(nid)]
            nids<-split.t[nid %in% nid & age_end==as_range$age_end, unique(nid)]  ##USERNAME: find sources that have data up to the age_end
          }else{
            nids<-split.t[age_start==as_range$age_start & sex_id==as_range$sex_id, unique(nid)]
            nids<-split.t[nid %in% nid & age_end==as_range$age_end & sex_id==as_range$sex_id, unique(nid)]
          }
          
          ##USERNAME: take a random subset of those sources to fit the 'new count'
          if(as_range$new_count>length(nids)){
            nids<-sample(nids, length(nids))
          }else{
            nids<-sample(nids, as_range$new_count)
            
          }
          temp<-split.t[nid %in% nids & age_start>=as_range$age_start & age_end<=as_range$age_end, ]
          new_data<-temp[, .(data=weighted.mean(data, sample_size)), by=nid]  ##USERNAME: nid is still src_loc_yr right now
          new_data$standard_error<-temp[, weighted.mean(standard_error, sample_size), by=nid]$V1  ##USERNAME: nid is still src_loc_yr right now
          new_data[, age_start:=as_range$age_start]
          new_data[, age_end:=as_range$age_end]
          new_data[, sex_id:=as_range$sex_id]
          split_list[[n]]<-new_data
        }
      }
      split<-rbindlist(split_list)
      
      if(F){##USERNAME: this was the old way of creating fake data points (split the entire source)
        split<-split.t[, .(data=weighted.mean(data, sample_size)), by=nid]  ##USERNAME: nid is still src_loc_yr right now
        ##USERNAME: construct fake standard errors for data points
        split$standard_error<-split.t[, weighted.mean(standard_error, sample_size), by=nid]$V1  ##USERNAME: nid is still src_loc_yr right now
        
        ##USERNAME: get the metadata for the constructed data points to split
        split_age_start<-split.t[, .(age_start=min(age_start)), by=nid]
        split_age_end<-split.t[, .(age_end=max(age_end)), by=nid]
        split_sex<-split.t[, .(sex_id=unique(sex_id)), by=nid]
        split_sex[, n_sexes:=length(unique(sex_id)), by=nid]
        setnames(split_sex, "sex_id", "og_sex_id")
        split_sex[, sex_id:=ifelse(n_sexes==2, 3, og_sex_id)]
        split_sex<-unique(split_sex[, .(nid, sex_id)])
        
        ##USERNAME: merge metadata pieces
        split_meta<-merge(split_age_start, split_age_end, by="nid")
        split_meta<-merge(split_meta, split_sex, by="nid")
        split<-merge(split, split_meta, by="nid")
      }
      ##USERNAME: get other metadata (that doesn't need to be constructed like age and sex)
      meta<-unique(training[, .(location_id, year_id, nid, og_nid)])
      split<-merge(split, meta, by="nid", all.y=F)
      split[, to_split:=1]
      
      ##combine-- the splitting function will parse them apart
      ko_data<-rbind(split, training.t, fill=T)
      
      drop_cols<-c(paste0("ko", 1:kos), "age_group_id")
      ko_data[, (drop_cols):=NULL]
      ko_data[is.na(to_split), to_split:=0]
      ko_data[to_split==0, to_train:=1]
      
      ##USERNAME: put nid back
      ko_data[, nid:=og_nid]
      ko_data[, og_nid:=NULL]
      
      #saveRDS(ko_data, file=paste0(output_folder, mod_name, "_", ko, "_data.rds"))
      
    }
    
    
    training[, nid:=og_nid]
    training[, og_nid:=NULL]
    #saveRDS(training, paste0(output_folder, mod_name, "_orig_data.rds"))
    
    
    message("Done saving KOs")
    return(list(ko_data = ko_data, training =  training))
  }
}