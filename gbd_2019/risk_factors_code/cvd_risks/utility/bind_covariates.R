#################################################################################################
## Purpose: pull covariates from covariate database and merge on to df

### Inputs ###
## df: a dataframe that has age, sex, year, and location_ids
## cov_list: character list of covariates in the covariate database, in the form of 'covariate_name_short'

### Outputs ###
## returns a df with original data and covariates merged on by age, sex, year and location_ids
################################################################################################

### Load paths and libraries ###
j <- "FILEPATH"

library(data.table)
library(dplyr)

### Load shared functions
central <- "FILEPATH"

source(paste0(central, "get_covariate_estimates.R"))
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_population.R"))
source(paste0(central, "get_ids.R"))

### Get covariates and bind them ###
bind_covariates <- function(df, cov_list, custom_cov_list=NULL, decomp_step)
  {
  ### Data checks ###
  cols <- c("sex_id", "year_id") ## these columns are necessary
  invisible(lapply(cols, function(x)
    {
      if(!x %in% names(df)){stop(paste0("Your data is missing a column named ", x, "!"))}
    }))
  
  ### Making sure location information is present ###
  if(!"location_id"  %in% names(df) & "ihme_loc_id"  %in% names(df))
    {
      message("You're missing location_id, merging on using location_set_id 22")
      locs <- get_location_metadata(location_set_id=22)[, .(ihme_loc_id, location_id)]
      df <- merge(df, locs, by="ihme_loc_id")
    }
  
  if(!"location_id"  %in% names(df) &  !"ihme_loc_id" %in% names(df))
    {
      message("You're missing location_id AND ihme_loc_id! You need at least one!")
    }
  
  if(any(df[["age_group_id"]]==27))
    {
      stop("You have age_group_id 27 in your data. Currently not aggregating covariates for age-standardized data")
    }
  
  ### Custom covariates ###
  cust_covs <- unlist(lapply(custom_cov_list, "[", 1))
  cust_covs_paths < -unlist(lapply(custom_cov_list, "[", 2))
  cov_list <- c(cov_list, cust_covs)
  
  ### Pull covariate_id values ###  
  covariates <- get_ids(table="covariate")
  cids <- covariates[covariate_name %in% cov_list, covariate_id]
  
  ### Read in covariates ###
  covs <- list()
  multi_age_covs <- list()
  multi_sex_covs <- list()
  multi_agesex_covs <- list()
  cov_name_short <- list()  
  
  for(i in 1:length(cov_list))
    {
      ### Get covariates from a custom location ###
      if(cov_list[i] %in% cust_covs)
        { 
          path <- cust_covs_paths[match(cov_list[i], cust_covs)]
          message(paste("Getting", cov_list[i], "from", path))
      
      ### Make sure custom covariate is in right format and that the necessary columns are present ###
      if(grepl(".csv", path))
        {
          nec_cols <- c("covariate_name_short", "location_id", "age_group_id", "sex_id", "year_id", "mean_value")
          x <- fread(path)
          
          lapply(nec_cols, FUN=function(y)
            {
              if(!y %in% names(x))
                {
                  stop(paste("Missing a necessary column!:", y))
                }
            })
      } else {
        message("Specified path is not a .csv!")
      }
    } else {
      ### Get covariates from the database ###
      cid <- covariates[covariate_name==cov_list[i], covariate_id]
      message(paste("Getting", cov_list[i], "from covariate database for", decomp_step, 'using ID', cid))
      x <- get_covariate_estimates(covariate_id=cid, location_id=unique(df$location_id),
              year_id=unique(df$year_id), decomp_step=decomp_step)
    }
    
    x <- x[, .(covariate_name_short, location_id, year_id, age_group_id, sex_id, mean_value)]
    ages <- unique(x$age_group_id)
    years <- unique(x$year_id)
    sexes <- unique(x$sex_id)
    
    cov_name_short[[i]] <- unique(x$covariate_name_short)
    
    ### Split up covariates by granularity of estimation for merging ###
    
    ### Covariate available by age and sex
    if(length(ages)>1 & length(sexes)>1)
      {
        message(paste("  Multiple age groups and sexes estimated for", cov_list[i]))
        x <- x[, .(covariate_name_short, location_id, sex_id, age_group_id, year_id, mean_value)]
        multi_agesex_covs[[i]] <- x

    ### Covariate available by age but not sex      
    if(length(ages)>1 & length(sexes)==1)
      {
        message(paste("  Multiple age groups estimated for; data are not sex-specific", cov_list[i]))
        x <- x[, .(covariate_name_short, location_id, age_group_id, year_id, mean_value)]
        multi_age_covs[[i]] <- x

    ### Covariate available by sex but not by age 
    if(length(ages)==1 & length(sexes)>1)
      {
        message(paste("  Multiple sexes estimated for", cov_list[i]))
        x <- x[, .(covariate_name_short, location_id, sex_id, year_id, mean_value)]
        multi_sex_covs[[i]]<-x   

    ### Covariate available by neither sex nor age
      } else {
        x <- x[, .(covariate_name_short, location_id, year_id, mean_value)]  ## if only 1 age group and only 1 sex group estimated
        miss_locs<-unique(df$location_id)[!unique(df$location_id) %in% unique(x$location_id)]
        if(length(miss_locs>0))
          {
            message("  You have the following location_ids in your data that are missing from the ", cov_list[i], " covariate: ", paste0(miss_locs, collapse=", "))
          }
      covs[[i]] <- x
      }
    }
  }

### Setup age_group_id if missing or aggregated ###
drp_ages <- F ##sy: default to keep ages, only drop if created below
  if(length(multi_age_covs)>0 | length(multi_agesex_covs)>0){
    if(!"age_group_id" %in% names(df) | all(df[["age_group_id"]]==22)){
      if("age_group_id" %in% names(df) & all(df[["age_group_id"]]==22)){
        drp_ages <- T ## this lets the function know to drop age_start and age_end at the end of the script
        df[age_group_id==22, age_start:=0]
        df[age_group_id==22, age_end:=99]
      }
      if("age_start" %in% names(df) & "age_end" %in% names(df)){
        message("Missing age_group_id, but have age_start and age_end. Setting up for covariate estimate aggregation.")
        df <- as.data.table(df)
        df[, split_id:=seq_len(nrow(df))]
        df.exp <- copy(df)
        df.exp <- df.exp[, .(split_id, age_start, age_end, sex_id, location_id, year_id)]
        df.exp[, age_start:=age_start-age_start %% 5]
        df.exp[, age_end:=age_end-age_end %% 5+4]
        
        ### Expand df for each necessary row
          # calculates the number of 5 year age groups that a row holds and assigns it to the n.age col
          # calculates the number of 5 year age groups that a row holds and assigns it to the n.age col
        df.exp[, n.age := (age_end + 1 - age_start)/5]
        # create a column called split_id that will connect rows that came initially from same row
        expanded <- rep(df.exp$split_id, df.exp$n.age) %>% data.table("split_id" = .) 
        df.exp <- merge(expanded, df.exp, by="split_id", all=T)
        # create 'age.rep' col, describes the iteration number of a split by age from the original row
        df.exp[, age.rep := 1:.N - 1, by=.(split_id)]
        df.exp[, age_start := age_start + age.rep * 5 ]
        df.exp[, age_end :=  age_start + 4 ]
        df.exp[, c("n.age", "age.rep") := NULL]
        
        ## Expand for sex
        ## creates a col with a unique value for each row, describes the split_id (maps to the original row) and age_start
        df.exp[, sex_split_id := paste0(split_id, "_", age_start)]
        ## if sex_id is "both", assign it a 2, if it is 2 or 1, assign 1.  So tells number of sexes described.
        df.exp[, n.sex := ifelse(sex_id==3, 2, 1)]
        
        ## create 'expanded' again, this time with column 'sex_split_id', with repititions=split$n.sex
        expanded <- rep(df.exp$sex_split_id, df.exp$n.sex) %>% data.table("sex_split_id" = .)
        ## again, merge 'expanded' onto split, this time creating a row for each unique sex_split_id
        df.exp <- merge(expanded, df.exp, by="sex_split_id", all=T)
        df.exp$sex_id <- as.integer(df.exp$sex_id) 
        ## replaces any sex_id==3 with 1 or 2, depending on if it is the 1st or 2nd new row for the unique sex_split_id
        df.exp <- df.exp[sex_id==3, sex_id := 1:.N, by=sex_split_id]
        ## remember that here, sex="sex_id".  also there is one sex_split_id per original age-country-year row (age was split above)
        df.exp[, c("n.sex", "sex_split_id"):=NULL]
        
        message("  Getting age_group_ids..")
        ## get age_group_ids and reformat
        invisible(age_ids<-get_ids(table="age_group"))
        suppressWarnings(age_ids[, age_start:=as.numeric(unlist(lapply(strsplit(age_ids$age_group_name, "to"), "[", 1)))])
        suppressWarnings(age_ids[, age_end:=as.numeric(unlist(lapply(strsplit(age_ids$age_group_name, "to"), "[", 2)))])
        ## merge
        df.exp <- merge(df.exp, age_ids[!is.na(age_start) & age_end-age_start==4, .(age_group_id, age_start, age_end)], by=c("age_start", "age_end"), all.y=F, all.x=T)
        
        ## change age group for pops
        df.exp[age_group_id==236, age_group_id:=1]
        df.exp[age_group_id %in% c(44, 33, 45, 46) , age_group_id:=235]
        df.exp[age_start>100 , age_group_id:=235]
        
    } else {
        stop("Missing age_group_id and age_start/age_end!")
      }
    }
  }
  
  
  ################### AGGREGATE COVS FOR AGGREGATED DATA  #########################################
    
  if(("age_start" %in% names(df) & "age_end" %in% names(df)) || 3 %in% unique(df$sex_id)){
    if(length(multi_age_covs)!=0 | length(multi_agesex_covs)>0 | length(multi_sex_covs)!=0){
      #stop("You specified an age or sex -specific coveriate, but your data is not age and/or sex specific!--Will aggregate in the future but breaking for now")
      
      ################### CHECK COVARIATE AGGREGATION #########################################
      check_agg <- function(temp_cov, check_vars, agg_covs, u, covar){
        miss_splits <- df.exp[split_id %in% df$split_id[!df$split_id %in% temp_cov$split_id],]
        miss_splits <- temp_cov[is.na(mean_value)]
        
        if(nrow(miss_splits)>0){
          message(" Missing aggregation results for ", nrow(miss_splits), " rows for ", covar)
          
          invisible(lapply(c(check_vars), function(x){  ##sy: merge vars gets inherited from agg()
            miss_pop <- unique(miss_splits[[x]])[!unique(miss_splits[[x]]) %in% unique(pops[[x]])]
            miss_cov <- unique(miss_splits[[x]])[!unique(miss_splits[[x]]) %in% unique(agg_covs[[u]][[x]])]
            
            if(length(miss_pop)>0){
              message("  Missing the following ", x, "(s) from the population estimates:")
              print(miss_pop)
            }
            if(length(miss_cov)>0){
              message("  Missing the following ", x, "(s) from the ", covar, " estimates:")
              print(miss_cov)
            }
          }))
          message(" Missing aggregated rows will be returned with NAs")
        }
      }
      
      
      ################### AGGREGATE COVARIATE #########################################
      
      agg <- function(agg_covs, merge_vars, check_vars){
        new_est <- list()
        #agg_covs<-agg_covs[!sapply(agg_covs, is.null)]
        for(u in 1:length(agg_covs)){
          temp_cov <- agg_covs[[u]]
          
          covar <- unique(temp_cov[!is.na(covariate_name_short), covariate_name_short])
          
          temp_cov <- merge(temp_cov, pops, by=c("location_id", "year_id", "sex_id", "age_group_id"), all.x=T)
          temp_cov <- merge(temp_cov, df.exp, by=merge_vars, all.y=T)
          check_agg(temp_cov, check_vars, agg_covs, u, covar)
          ##sy: aggregate, weighted by population size
          new_est[[u]] <- temp_cov[, .(mean_value=weighted.mean(mean_value, population, na.rm=T)), by=.(split_id)]
          new_est[[u]][, covariate_name_short:=covar]
        }
        return(new_est)
      }
      
      
      ## do some quick checks to make sure there are no NAs before trying get_populations
      invisible(lapply(c("location_id", "age_group_id", "year_id", "sex_id"), function(z){
        if(z!="age_group_id"){
          if(any(is.na(df[[z]]))){stop(paste0("You have NA values for ", z, "; this will cause issues."))}
        }else{
          if(any(is.na(df.exp[[z]]))){stop(paste0("You have NA values for ", z, "; this will cause issues."))}
        }
      }))
      
      message("    Getting populations for aggregation...")
      pops <- get_population(location_id=unique(df$location_id), 
                           age_group_id=c(unique(df.exp$age_group_id), 22), ##sy: need all age group ids, and for multi_sex_covs 
                           year_id=unique(df$year_id), 
                           sex_id=c(unique(df$sex_id), 3),
                           decomp_step=decomp_step)
      #pops[, process_version_map_id:=NULL]
      message("    Done getting populations")
      
      
      agg_list <- list()
      ## if there is sex-aggregated data but a sex specific covariate
      if(3 %in% unique(df$sex_id) & length(multi_sex_covs)!=0){
        message("  Aggregating sex-specific covariates to merge on to data")
        multi_sex_covs<-multi_sex_covs[!sapply(multi_sex_covs, is.null)]
        ##sy: create age group id for merging w/ pops
        invisible(lapply(1:length(multi_sex_covs), function(i){
          multi_sex_covs[[i]][, age_group_id:=22]
        }))
        
        msc <- agg(multi_sex_covs, c("location_id", "year_id", "sex_id"), check_vars=c("location_id", "year_id"))
        multi_sex_covs <- list() ##sy: empty this list
        agg_list <- append(agg_list, msc)
      }
      ## if there is age-aggregated data but age-specific covariates
      if(("age_start" %in% names(df) & "age_end" %in% names(df)) && length(multi_age_covs)>0){
        message("  Aggregating age-specific covariates to merge on to data")
        multi_age_covs <- multi_age_covs[!sapply(multi_age_covs, is.null)]
        
        ## create sex_id for merging w/ pops
        invisible(lapply(1:length(multi_age_covs), function(i){
          multi_age_covs[[i]][, sex_id:=3]
        }))
        
        mac <- agg(multi_age_covs, c("location_id", "year_id", "age_group_id"), check_vars=c("location_id", "year_id"))
        multi_age_covs <-list() ## empty this list
        agg_list <- append(agg_list, mac)
      }
      
      if(("age_start" %in% names(df) & "age_end" %in% names(df)) || 3 %in% unique(df$sex_id) && length(multi_agesex_covs)>0){
        message("  Aggregating age-sex specific covariates to merge on to data")
        multi_agesex_covs <- multi_agesex_covs[!sapply(multi_agesex_covs, is.null)]
        
        masc <- agg(multi_agesex_covs, c("location_id", "year_id", "age_group_id", "sex_id"),check_vars=c("location_id", "year_id", "age_group_id"))
        multi_agesex_covs <- list() ## empty this list
        agg_list <- append(agg_list, masc)
      }
      
      
      agg_list <- rbindlist(agg_list)
      
      agg_list <- dcast(agg_list, ...~covariate_name_short, value.var="mean_value")
      df <- merge(df, agg_list, by="split_id")
      
    }
    ## merge on the aggregated covariates
    
  }
  
  ################### RESHAPE COVARIATE LISTS  #########################################
  if(length(covs)>0){
    covs <- rbindlist(covs)
    covs_cast <- dcast(covs, location_id+year_id~covariate_name_short, value.var="mean_value")
  }
  
  if(length(multi_age_covs)>0){
    multi_age_covs <- rbindlist(multi_age_covs)
    agecovs_cast <- dcast(multi_age_covs, location_id+year_id+age_group_id~covariate_name_short, value.var="mean_value")
  }
  
  if(length(multi_sex_covs)>0){
    multi_sex_covs <- rbindlist(multi_sex_covs)
    sexcovs_cast <- dcast(multi_sex_covs, location_id+year_id+sex_id~covariate_name_short, value.var="mean_value")
  }
  
  if(length(multi_agesex_covs)>0){
    multi_agesex_covs <- rbindlist(multi_agesex_covs)
    agesexcovs_cast <- dcast(multi_agesex_covs, location_id+year_id+sex_id+age_group_id~covariate_name_short, value.var="mean_value")
  }
  
  ################### MERGE COVARIATES ON TO DATA  #########################################
  ######################################################################
  rws <- nrow(df)
  cov_name_short <- unlist(cov_name_short)
  binder <- copy(df)
  
  message("Binding")
  
  if("covs_cast"  %in% ls()){
    binder <- merge(binder, covs_cast, by=c("location_id", "year_id"))
  }
  
  
  if("agecovs_cast" %in% ls()){
    binder <- merge(binder, agecovs_cast, by=c("location_id", "year_id", "age_group_id"))
  }
  
  
  if("sexcovs_cast" %in% ls()){
    binder <- merge(binder, sexcovs_cast, by=c("location_id", "year_id", "sex_id"))
  }
  
  if("agesexcovs_cast" %in% ls()){
    binder <- merge(binder, agesexcovs_cast, by=c("location_id", "year_id", "age_group_id", "sex_id"))
  }
  
  if(nrow(binder)!=rws){message("Row number change after merging covariates..this probably means you have age-sex-location-years in your data that aren't in the covariate")}
  
  if(any(is.na(binder[, c(cov_name_short), with=F]))){message("Some covariates have missing values after binding..this probably means you have age-sex-location-years in your data that aren't in the covariate ")}
  
  print("Done binding")
  
  ## if age start and age end were created but the original data was all ages, drop those cols
  if(drp_ages==T){
    binder[, c("age_start, age_end"):=NULL]
  }
  
  
  return(list(binder, cov_name_short))
  
  
}