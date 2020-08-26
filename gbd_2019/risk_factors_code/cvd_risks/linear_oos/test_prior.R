################################
## Purpose: calculate out of sample validity after a stgpr run with holdouts
########################################################################################################################################################

################### FUNCTION FOR TAGGING MODELS THAT VIOLATE SIGNIFICANCE #########################################
restrict_violations<-function(rmses, covs, prior_sign=NULL, p_value=.05){
  ################### REMOVE BASED ON SIGN VIOLATION #########################################
  rmses[, sign_violation:=0]
  if(!is.null(prior_sign)){
    message("Removing models that violate prior signs")
    signs<-data.table(sig=prior_sign, cov=covs)
    
    for(i in 1:nrow(signs)){
      cov<-signs[i, cov]
      sign<-signs[i, sig]
      ## get models where cov doesn't violate sign
      if(sign==-1){
        message(cov, " must be negative")
        message("  Dropping ", nrow(rmses[get(paste0(cov, "_fixd"))>0,])," model(s) where ", cov, " is greater than 0")
        
        rmses[get(paste0(cov, "_fixd"))>0, sign_violation:=1]
      }
      if(sign==1){
        message(cov, " must be positive")
        message("  Dropping ", nrow(rmses[get(paste0(cov, "_fixd"))<0,])," model(s) where ", cov, " is less than 0")
        
        rmses[get(paste0(cov, "_fixd"))<0, sign_violation:=1]
      }
    }
  }
  
  
  ################### REMOVE BASED ON SIGNIF VIOLATION #########################################
  z<-qnorm(p_value/2, mean=0, sd=1, lower.tail=F)
  rmses[, sig_violation:=0]
  invisible(
    lapply(covs, function(x){
      lowers<-rmses[, get(paste0(x, "_fixd"))-z*get(paste0(x, "_fixd_se"))]
      uppers<-rmses[, get(paste0(x, "_fixd"))+z*get(paste0(x, "_fixd_se"))]
      temp<-data.table(lower=lowers, upper=uppers)
      temp[, insig:=ifelse(data.table::between(0, lower, upper), 1, 0)]
      
      message(nrow(temp[insig==1, ]), " models have p>", p_value, " for ", x)
      rmses[temp$insig==1, sig_violation:=1]
      rmses[temp$insig==0, sig_violation:=0]
    })
  )
  rmses[sig_violation==1 | sign_violation==1, drop:=1]
  rmses[is.na(drop), drop:=0]
  return(rmses)
}

## right now, only set up for lmer, not lm
test_prior<-function(crosswalk_version_id, decomp_step, cov_list, data_transform, username,
                     count_mods=T, rank_method="aic", modtype="lmer", offset=0.0001,
                     custom_covs=NULL, fixed_covs=NULL, random_effects=NULL, ban_pairs=NULL,
                     by_sex=T,
                     polynoms=NULL, prior_sign=NULL, p_value=0.05,
                     intrxn=NULL,  
                     ko_prop=0.25, kos=5, remove_subnats=T, no_new_ages=T, only_data_locs=F, drop_nids=T, seed=SEED, location_set_id=22,
                     proj="PROJECT", slots_per_job=2, forms_per_job){
  
  
  ##########sy: shouldn't need to change anything below #####################
  j <- "FILEPATH"
  h <- "FILEPATH"
  date<-gsub("-", "_", Sys.Date())
  
  library(plyr)
  library(dplyr)
  library(data.table)
  library(DBI)
  library(RMySQL)
  library(lme4)
  require(ini)
  library(MuMIn, lib='FILEPATH')
  library(ggplot2)
  
  ################### QUICK CHECKS #########################################
  ## check prior_signs
  if(!is.null(prior_sign)){
    if(length(prior_sign)!=length(cov_list)+length(custom_covs)){stop("Length of prior_sign is not equal to length of cov_list!!")}
  }
  
  ## check model types
  if(modtype=="lmer" & is.null(random_effects)){stop("You specified modtype as lmer, but did not specify any random effects!")}
  if(modtype=="lm" & !is.null(random_effects)){stop("You specified modtype as lm, and also specified random effects!")}
  
  ################### PATHS #########################################
  ## path to central functions
  central <- "FILEPATH"
  stgpr_central<-"FILEPATH"
  
  
  ## path to temporarily save model outputs--this folder gets created and deleted during the function
  output_folder <- paste0("FILEPATH", crosswalk_version_id, "_", date, "/")
  
  
  if(file.exists(output_folder)){
    message(paste("Temporary directory", output_folder, "already exists, deleting contents"))
    unlink(output_folder, recursive = T)
    message("Done deleting")
  }
  
  dir.create(output_folder)
     
  ###################  SCRIPTS  #########################################
  source('FILEPATH/setup.r')
  source('FILEPATH/bind_covariates.R')
  source("FILEPATH/make_ko.R")
  source("FILEPATH/job_hold.R"))
  source("FILEPATH/get_location_metadata.R")
  source("FILEPATH/get_crosswalk_version.R") 
  
  ###################  GET FULL DATA #########################################
  data <- get_crosswalk_version(crosswalk_version_id)
  data <- as.data.table(data)
  message("Done")
  
  ## drop outliers
  data <- subset(data, is_outlier==0)
  
  message('Outlier data dropped')
  
  ## recode to sex_id (this is for back compatability)
  data[, sex_id:=ifelse(sex=="Male", 1, ifelse(sex=="Female", 2, 3))]
  
  ## subset data to necessary cols
  data <- data[, .(nid, location_id, year_id, age_group_id, sex_id, val, variance)]
  setnames(data, "val", "data")
  
  if("cv_custom_prior" %in% names(data)){
    message("Dropping cv_custom_prior column")
    data[, cv_custom_prior:=NULL]
  }
  
  ## check data compatability w/ model
  if(nrow(data[is.na(data)])>0){
    message("You have ", nrow(data[is.na(data)]), " NAs in your data, these rows will be dropped")
    data<-data[!is.na(data)]
  }
  if(length(unique(data$age_group_id))<=1  &  any(grepl("age_group_id", c(fixed_covs, random_effects)))) {stop("You specified age_group_id as a predictor, but you have less than 2 levels of this variable")}
  
  
  ###################  GET LOCS AND COVARIATES #########################################
  locs <- get_location_metadata(location_set_id=location_set_id)[, .(location_id, super_region_name, region_name, location_name, level)]
  locs <- as.data.table(locs)
  
  data_and_names <- bind_covariates(data, cov_list=cov_list, custom_cov_list=custom_covs, decomp_step=decomp_step)
  data <- data_and_names[[1]] ## this is the data w/ bound covariate estimates
  cov_list <- data_and_names[[2]] ## these are the covariate_name_short values
  
  for(cov in cov_list){
    if(nrow(data[is.na(get(cov)),])>0){
      message("There are ",  nrow(data[is.na(get(cov)),])," missing estimates for ", cov, ", these rows will be dropped!")
      Sys.sleep(5)
      data <- data[!is.na(get(cov))]
    }
  }
  
  for(cov in cov_list){
    if(!cov %in% names(data)){ stop(paste(cov, "missing from data... make sure that the covariate has estimates!"))}
  }
    
  ## this creates a column in the dataset, and also saves the names of those colums in the 'polynoms' vector
  if(!is.null(polynoms)){
    polys <- strsplit(polynoms, "\\^")
    
    polynoms.t<-list()
    for(i in 1:length(polys)){
      basecov<-polys[[i]][1]
      if(!basecov %in% names(data)){ stop("ATTEMPTING TO CREATE POLYNOMIAL, MISSING: ", polys[[i]][1])}
      
      data[, paste0(basecov, polys[[i]][2]):=get(basecov)^as.numeric(polys[[i]][2])]
      polynoms.t[[i]]<-paste0(basecov, polys[[i]][2])
    }
    polynoms <- unlist(polynoms.t)
  }
  
  data <- merge(data, locs, by="location_id")
  
  
  ## this data offsetting function is from stgpr
  data <- offset.data(data, data_transform, offset)
  
  ###################  GET KOs AND SAVE TO TEMP FOLDER #########################################
    
  ## set up by sexes or not
  if(by_sex==T){
    sex_list<-c("M", "F")
      } else {
    sex_list<-c("both_sexes")
  }
  
  
  for(sexchar in sex_list){
    message("Prepping ", sexchar, " data for KO creation")
           
    if(sexchar=="M"){  sex<-1}
    if(sexchar=="F"){  sex<-2}
    if(sexchar=="both_sexes"){  sex<-c(1,2,3)}
    data.s <- data[sex_id %in% sex, ]
    
    ## set up data set if it's 'all ages'
    if(length(unique(data.s$age_group_id))==1){
      if(unique(data.s$age_group_id)==22  | unique(data.s$age_group_id)==27){
        data.s[, age_group_id:=22]  ##sy: set to 22
        by_age<-0
      }else{
        stop("Ask about incorporating custom age groups")
      }
    }else{
      by_age<-1
    }
    if(F){
      remove_subnats<-T
      no_new_ages<-T
      only_data_locs<-F
      drop_nids<-T
      seed<-10
    }
    
    ko_items <- prep_ko(data.s, remove_subnats=remove_subnats, location_set_id=location_set_id, by_age=by_age, by_sex=ifelse(by_sex, 1, 0))
    message("  Done")
    
    message("Generating KOs")
    ## generate knockouts.  The arguments are set up to take the output of the prep_ko function directly
    test <- get_kos(ko_items[[1]], ko_items[[2]], ko_items[[3]], ko_items[[4]], prop_to_hold=ko_prop, kos=kos, seed=seed, no_new_ages=no_new_ages, only_data_locs=only_data_locs, drop_nids = drop_nids)
    message("  Done")
    
    ## writing the formmated dataset to avoid doing it for each child process
    saveRDS(test, file=paste0(output_folder, sexchar, "_full_data.rds"))
    write.csv(test, file=paste0("FILEPATH", crosswalk_version_id, '_data_', sexchar, '_', date, '.csv'))
    message("Saved ", sexchar, " prepped data to temp folder")
  }
  
  ###################  GET ALL POSSIBLE MODELS #########################################
    
  ## check to make sure banned pairs are valid names
  invisible(lapply(unlist(ban_pairs), function(x){
    if(!x %in% names(data)){message("You specified ", x, " as a banned pair, but it is not a valid covariate name short in your data!")}
  }))
  
  
  
  ## set up formula
  if(!is.null(random_effects)){
    form<-paste0( data_transform,"(data)~", paste0(cov_list, collapse="+"),
                  "+", paste0(polynoms, collapse="+"), "+", paste0(fixed_covs, collapse="+"), "+", paste0(random_effects, collapse="+"))
  }else{
    form<-paste0( data_transform,"(data)~", paste0(cov_list, collapse="+"),
                  "+", paste0(polynoms, collapse="+"), "+", paste0(fixed_covs, collapse="+"))
  }
  
    
  ## set up banned set logic
  sub<-paste(unlist(lapply(ban_pairs, function(x){
    thing<-c(rep(NA, times=length(x)-1))
    for(i in 1:length(x)-1){
      if(i<length(x)){
        bans<-c(rep(NA, times=length(x)-i))
        for(n in i+1:length(x)-i){
          bans[n-i]<-paste0("'", x[n], "'")
        }
        bans.t<-paste(bans, collapse=" | ")
        if(i<length(x)-1){
          thing[i]<-paste0("!('", x[i], "'  &&  (",  bans.t, "))")
        }else{
          thing[i]<-paste0("!('", x[i], "'  &&  ",  bans.t, ")")
          
        }
        
      }
      
    }
    return(paste(thing, collapse=" & "))
    
    
  })), collapse="  &  ")
   
  message(paste("Getting formulas..."))
  message(paste("  General formula:", form))
  message(paste("  Banned sets:", sub))
    
  ## add polynomials to cov_list:
  ## save original cov list for prior_signs later first
  og_cov_list<-cov_list
  cov_list<-c(cov_list, polynoms)
    
  ## Function to remove ban pairs from a list of covariates 
  remove.ban <- function(cov_list, ban) {
    out <- lapply(cov_list, function(covs) {
      if (length(covs) == 1 | (!all(sort(intersect(covs, ban)) == sort(ban))) | length(intersect(covs, ban))==0) return(covs)
    })
    return(out[!sapply(out, is.null)])
  }
  ## Create all combinations of covariates
  mod_list <- lapply(1:length(cov_list), function(x) combn(cov_list, x, simplify=FALSE)) %>% unlist(., recursive=F)
  
  if(!is.null(ban_pairs)){
    ## Create combinations of ban sets
    keepers<-list()
    for(i in 1:length(ban_pairs)){
      x<-ban_pairs[[i]]
      ban_list <- combn(x, 2, simplify=FALSE)
      ## Create lists for each ban set
      banned_lists <- lapply(ban_list, function(ban) remove.ban(mod_list, ban))
      ## Intersect them all
      keepers[[i]]<-Reduce(intersect, banned_lists)
    }
    
    temp_forms<-unlist(lapply(Reduce(intersect, keepers), function(x){ paste0(x, collapse="+")}))
  }else{
    temp_forms<-unlist(lapply(mod_list, function(x){paste0(x, collapse="+")}))
  }
    
  ## paste all pieces together
  if(!is.null(random_effects)){
    if(!is.null(fixed_covs)){
      forms.n<-paste(
        paste0(data_transform, "(data)"),
        paste(temp_forms, paste0(fixed_covs, collapse="+"), paste0(random_effects, collapse="+"), sep="+"),
        sep="~")
      
      ## add on the null mod
      null_mod<-paste(paste0(data_transform,"(data)"),
                      paste(paste0(fixed_covs, collapse="+"), paste0(random_effects, collapse="+"), sep="+"),
                      sep="~")
      
    }else{
      forms.n<-paste(
        paste0(data_transform, "(data)"),
        paste(temp_forms, paste0(random_effects, collapse="+"), sep="+"),
        sep="~")
      
      ## add on the null mod
      null_mod<-paste(paste0(data_transform,"(data)"),
                      paste(paste0(random_effects, collapse="+"), sep="+"),
                      sep="~")
    }
  }else{
    forms.n<-paste(
      paste0(data_transform, "(data)"),
      paste(temp_forms, paste0(fixed_covs, collapse="+"), sep="+"),
      sep="~")
    
    ## add on the null mod
    null_mod<-paste(paste0(data_transform,"(data)"),
                    paste(paste0(fixed_covs, collapse="+"), sep="+"),
                    sep="~")
    
    if(!is.null(fixed_covs)){
      forms.n<-paste(
        paste0(data_transform, "(data)"),
        paste(temp_forms, paste0(fixed_covs, collapse="+"), sep="+"),
        sep="~")
      
      ## add on the null mod
      null_mod<-paste(paste0(data_transform,"(data)"),
                      paste(paste0(fixed_covs, collapse="+"), sep="+"),
                      sep="~")
      
    }else{
      forms.n<-paste(
        paste0(data_transform, "(data)"),
        paste(temp_forms, sep="+"),
        sep="~")
      
      ## add on the null mod
      null_mod<-paste(paste0(data_transform,"(data)"),
                      1,
                      sep="~")
    }
    
    
    
  }
  
  forms<-c(null_mod, forms.n)
  message("Done getting formulas")
  
  if(count_mods==T){
    message("Done--Returning ", length(forms), " formulas")
    if(by_sex==T){
      message(paste("You set by_sex==T, so real number of formulas to be evaluated is", length(forms)*2))
    }
    return(forms)
    #stop("Done")
  }
  
  
  if(by_sex==T){
    message(paste(length(forms)*2, "total formulas to evaluate"))
  }else{
    message(paste(length(forms), "total formulas to evaluate"))
  }
  
  saveRDS(forms, file=paste0(output_folder, "forms.rds"))
  
  #write.csv(as.data.table(x=forms), file=paste0(kos_folder, "prior_oos_results_", date, "/forms.csv"), row.names=F)
  message(paste0("Model combinations saved to ", paste0(output_folder, "forms.rds")))
  
  
  
  
  ###################  LAUNCH JOBS #########################################
  file_list<-list()
  for(sexchar in sex_list){
    
    ## setup number of jobs to submit
    n <- length(forms)
    n_divisions <- forms_per_job
    #create start and end_ids
    seq <- data.table(start =seq(1, n, by = n_divisions))
    seq[, end:=shift(start, type = "lead") - 1]
    seq[nrow(seq), end:=n]
    seq[, id:=seq(.N)]
    nsubs <- max(seq$id)
    message("Launching ", nsubs, " splitting jobs for sex: ", sexchar)
    
    
    for(i in 1:max(seq$id)){
      if(F){
        i<-16
        sexchar<-"both_sexes"
        data_transform<-"logit"
      }
      
      ## get start and end forms
      date <- gsub("-", "_", Sys.Date())
      start <- seq[id ==i, start]
      end <- seq[id == i, end]
      
      proj <- PROJECT
      threads <- THREADS
      mem <- MEMORY
      queue <- QUEUE

      command <- paste0("qsub -l fthread=", threads, 
                  " -l m_mem_free=", mem, 
                  " -l archive",
                  " -q ", queue, 
                  " -P ", proj, 
                  " -N ", paste0("oos_", sexchar, "_", i),
                  " -o FILEPATH/", username, "/output -e FILEPATH", username, "/errors", 
                  " FILEPATH/shell.sh",
                  start, " ", end, " ",
                  sexchar, " ",  crosswalk_version_id, " ", date, " ", data_transform, " ", modtype, " ", kos)
      
      system(command)
      
      
    }
  }
  ## job hold
  message("Waiting on jobs...")
  job_hold("oos_")
  message("Finished model testing")
  
  
  ################### READ IN RESULTS, RANK, OUTPUT #########################################
  rmse_files<-list.files(path=output_folder, pattern=".csv", full.names=T)
  
  file_length<-ifelse(by_sex, 2*length(forms), length(forms))
  if(length(rmse_files)!=file_length){
    print(paste0(file_length-length(rmse_files), " model results are missing! Check error logs; jobs may have broken"))
    Sys.sleep(5)
  }
  
  ## read in the results
  message("Reading in results, and ranking models")
  stack<-list()
  if(length(rmse_files>0)){
    for(i in 1:length(rmse_files)){
      stack[[i]] <- fread(rmse_files[i])
    }
  }else{
    stop("All jobs broken")
  }
  rmses<-rbindlist(stack, fill=T)
  
  
  ################### RANK MODELS #########################################
  ## rank models by selected method
  if(rank_method=="oos.rmse"){
    ## sort by oos rmse
    rmses<-setorder(rmses, out_rmse)
  }
  if(rank_method=="aic"){
    rmses<-setorder(rmses, aic)
  }
     
  rmses<-restrict_violations(rmses=rmses, prior_sign=prior_sign, covs=cov_list, p_value=p_value)
  
  
  ################### SAVE #########################################
  message("Done")
  return(list(rmses, data))
}

