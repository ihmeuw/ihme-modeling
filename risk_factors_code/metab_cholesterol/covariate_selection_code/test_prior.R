################################
##author: USERNAME
##date: 3/28/2017
##purpose: calculate out of sample validity after a stgpr run with holdouts
##notes:   -created with the stgpr file-save format in 2016, may need to change
##         -must be run on cluster (need access to share where stgpr outputs saved)
##         
##      03/28/2017: running out of sample rmse for all possible models
#
########################################################################################################################################################
#            ##### test_prior()  Function ######
#       
#
#       #### Purpose: test out of sample RMSE of all possible linear prior models for STGPR
#                      MUST BE RUN ON CLUSTER, CURRENTLY NEED TO ACCESS /share/
#
#       #### Notes: -One job is submitted per model to proj_custom_models.  Use count_mods=T (the dUSERt) to find how many models you need!
#                   -updated on 6/2/2017 to use data_id instead of run_id, and will create holdouts using the get_kos() function
#                   
#
#
################ Arguments ##############################################################################################################################
#
#     ######### REQUIRED #########
#
#           -data_id: A GBD2016 STGPR data_id that corresponds to data uploaded to the STGPR database
#               ex:   data_id=3906
#
#           -cov_list: A character string of covariates in the covariate_name_short form.  Must include any custom covariates (see custom_covs argument)
#               ex:   cov_list=c("sodium_age_spec","alcohol_lpc",  "vegetables_g_adj", "sdi", "prev_overweight", "haqi", "nuts_seeds_g_adj")
#
#           -data_transform: Character string, either "log" or "logit" to be inherited by STGPR's transform_data() function
#               ex:   data_transform="log
#
#           -username: Charcter, your ihme username.  For saving outputs and errors to sge
#               ex: username="USERNAME"
#
#     ######### OPTIONAL #########
#           -count_mods:  Logical.  If TRUE, will only count the number of models that would be evaluated given other arguments
#                           DUSERt is TRUE; the idea is you should know how many models you are going to test before testing them
#                           One job is submitted per model..so the number of models is important!  Keep an eye on cluster availability!
#                           Based on number of covariates in cov_list, polynoms, and ban_pairs
#                           DOES NOT DUPLICATE MODELS IF by_sex==T --So if by_sex==T, double the number of formulas!
#               ex: count_mods=F
#
#           -rank_method: Character.  Set the method by which to rank model results.
#                           Currently only options are "aic" for Aikake information criterion and "oos.rmse" for Out of sample root-mean-squared-error
#                           DUSERt is "aic"- this is so that models won't break if no holdouts are run.
#                           Both methods will be returned in the output dataframe regardless of the method picked
#               ex: rank_method="aic"
#
#           -modtype: Character.  Define regression function to use.
#                           Must be either "lmer" or "lm"
#                           DUSERt is "lmer"
#                           If random effects specified, modtype must be set to "lmer", the dUSERt
#                           If no random effects specified, modtype must be set to "lm"
#               ex: modtype="lm"
#
#           -offset: Numeric. Value to add to logit transformed models to avoid taking logit(0)
#                           Defulat is 0.0001
#               ex: offset=0.001
#
#           -custom_covs: A list object of character vectors containing custom covariate_name_short and filepath where custom covariate is saved.
#                           Currently only reading in covariates stored in .csv format
#                           Each character vector must be of length 2.  The first character is the covariate_name_short as specified in the file and in cov_list, the second is the filepath
#                           Any custom covariates must be specified here as well as in cov_list.
#                           Paths must be accessible by the cluster
#               ex:   custom_covs=list(c("sodium_age_spec", paste0(j, "FILEPATH")), c("asdf_covariate", "DRIVE:/asdf/path.csv"))
#
#           -fixed_covs: A character vector of covariates to be run in every model.
#               ex:   fixed_covs=c("as.factor(age_group_id)", "sex_id")
#
#           -random_effects: A character vector of random effects to include in the model.  
#                           Must specify in lmer() random effect syntax, in order to specify intercept/slope and/or conditional covariates
#               ex:   random_effects=c("(1|super_region_name)", "(1|region_name)")
#
#           -ban_pairs: A list of character vectors containing covariates never to include together.  Can ban more than two covariates together
#               ex:   ban_pairs=list(c("alcohol_binge_prev", "alcohol_lpc"), c("mean_bmi", "prev_overweight", "prev_obesity"))
#
#           -by_sex: Logical.  If TRUE, will subset data by sex and run oos testing separately.
#                           DUSERt is TRUE.
#               ex:   by_sex=T
#
#           -polynoms: A character vector of covariates to be included as non-linear terms
#                           Each covariate must also be included in cov_list
#                           Currently not forcing that the linear and non-linear terms of a covariate always be included together!
#               ex:   polynoms<-c("sdi^2", "haqi^.5")
#                           
#           -prior_sign: Numeric vector of length==length(cov_list).  Each value is the imposed direction of the beta for the corresponding covariate in cov_list
#                           A value of 1 imposes a positive beta value, a value of -1 imposes a negative beta value, and a value of 0 imposes no direction
#                           Models containing beta values violating the prior signs will be subset out of the output..It may be useful to include them to look at!
#               ex: prior_sign=c(-1, 1, 0, 1)
#
#           -intrxn: A character vector of interaction terms to be included in the model.
#                           NOT THOROUGHLY TESTED!!!- NOT YET IMPLEMENTED
#                           Must be in R USERNAMEntax format, eg "cov1:cov2"
#                           Currently not forcing "cov1" and "cov2" to appear as fixed effects if "cov1:cov2" is specified
#               ex:   intrxn=("prev_overweight:factor(age_group_id)", "alcohol_lpc:sdi")
#
#          -ko_prop: The proportion of data to hold out for each KO. DUSERt is 0.25
#                           Knockouts are created by replicating the age-year missingness pattern of one location in another
#
#           -kos: Number of knockouts to perform on the data set.   DUSERt is 5
#
#           -remove_subnats: Logical. If T, then all subnational locations will be removed from the dataset and the square before returning
#                           Defualt is T
#                           This argument exists for STGPR users.  Linear models are being run without subnational data, so out of sample testing can be done w/o subnational data
#
#           -no_new_ages: Logical. If T, then age groups for which all data points were held out will be put back into the training dataset.
#                           DUSERt is T.  This is to avoid models breaking if age group is specified as a fixed effect
#
#           -only_data_locs: Logical. If T, then only locations with any data will be used to create age-year missingness patterns to recreate in the holdouts
#                           DUSERt is F.  CODEm does not do this, which is why the dUSERt is F
#                           This avoids necessarily dropping out 100% of the data in a location
#
#           -seed: Numeric, seed to set for knockouts.   
#
#           -location_version_id: Numeric, version id to be passed on to prep_ko() and get_location_metadata()
#                           Should be the same version id used in your STGPR model
#
#           -proj: Charcter, the name of a valid IHME cluster project.  Defualt is "proj_crossval"
#               ex: proj="proj_custom_models"
#
#           -slots_per_job: Integer, number of slots per job to request.  A job gets submitted per linear model and these are generally small
#                           DUSERt is 2.  Run a linear model on your data if you think this may not be appropriate
#               ex: slots_per_job=1
#
################# Output ###############################################################################################################################
#      
#     If count_mods==F 
#       -A data.table with each row corresponding to a model, and the following columns:
#           out_rmse: the average out of sample rmse for that model
#           in_rmse: the in sample rmse for that model
#           aic: the in-sample Aikake information criterion value for that model
#           Intercept_fixd: the global intercept for that model
#           Intercept_fixd_se: the standard error of the global intercept for that model
#           'cov'_fixed: a column for each covariate, giving the fixed effect coefficient
#           'cov'_fixed_se: a column for each covariate, giving the standard error of that covariate's fixed effect coefficient
#
#     If count_mods==T (the dUSERt)
#       -A character vector of all models that would be evaluated given the other arguments passed into the function
#
#
########################################################################################################################################################





  
  ##USERNAME: right now, only set up for lmer, not lm
  test_prior<-function(data_id, cov_list, data_transform, username,
                       count_mods=T, rank_method="aic", modtype="lmer", offset=0.0001,
                       custom_covs=NULL, fixed_covs=NULL, random_effects=NULL, ban_pairs=NULL, 
                       by_sex=T, 
                       polynoms=NULL, prior_sign=NULL,
                       intrxn=NULL,  ##USERNAME:intrxn isn't implemented yet
                       ko_prop=0.25, kos=5, remove_subnats=T, no_new_ages=T, only_data_locs=F, seed=32594, location_version_id=149,
                       proj="proj_crossval", slots_per_job=2){
  
    
    if(F){  ##USERNAME: interactively test 
      data_id<-4555  ##chl
      data_id<-4363  ##fruits
      data_id<-3865 ##smoking
      data_id<-4376 ##vaccines
      path<-paste0("FILEPATH", data_id, ".h5")
      cov_list<-c("wasting_prop_whz_under_2sd", "stunting_prop_haz_under_2sd", "sdi")
      data<-h5read(path, "data")
      data<-as.data.table(data)
      
    }
    
    
    
    ##########USERNAME: shouldn't need to change anything below #####################
    os <- .Platform$OS.type
    if (os=="windows") {
      #stop("Must be run on cluster!")
      j<- "J:/"
    } else {
      lib_path <- "FILEPATH"
      j<-"FILEPATH"
    }
    date<-gsub("-", "_", USERNAMEs.Date())
    

    library(data.table, lib.loc=lib_path)
    library(plyr, lib.loc=lib_path)
    library(DBI, lib.loc=lib_path)
    library(dplyr, lib.loc=lib_path)
    library(RMySQL, lib.loc=lib_path)
    library(lme4, lib.loc=lib_path)
    library(MuMIn, lib.loc=lib_path)
    library(boot, lib.loc=lib_path)
    library(ggplot2, lib.loc=lib_path)
    library(rhdf5, lib.loc=lib_path)
    library(arm, lib.loc=lib_path)
    
    
    ################### QUICK CHECKS #########################################
    #####################################################
    
    ##USERNAME: check prior_signs
    if(!is.null(prior_sign)){
      if(length(prior_sign)!=length(cov_list)){stop("Length of prior_sign is not equal to length of cov_list!!")}
    }
    
    ##USERNAME: check model types
    if(modtype=="lmer" & is.null(random_effects)){stop("You specified modtype as lmer, but did not specify any random effects!")}
    if(modtype=="lm" & !is.null(random_effects)){stop("You specified modtype as lm, and also specified random effects!")}
    
    
    
  ################### PATHS #########################################
  #####################################################
  
    
  # ##USERNAME:paths
  # 
  #   
  ##USERNAME:path to data
  path<-paste0("FILEPATH", data_id, ".h5")
  ##USERNAME:path to central functions
  central<-paste0(j, "FILEPATH")
  
  
  
  
  ##USERNAME: path to temporarily save model outputs--this folder gets created and deleted during the function
  output_folder<-paste0(j, "FILEPATH", data_id, "_", date, "/")
  
  
  
  if(file.exists(output_folder)){
    message(paste("Directory", output_folder, "already exists, deleting contents"))
    unlink(output_folder, recursive = T)
    message("Done deleting")
  }
  
  dir.create(output_folder)

  
  
  


  ###################  SCRIPTS  #########################################
  #####################################################


  setwd(paste0(j, 'FILEPATH'))

  source('register_data.r')
  source('setup.r')
  source('model.r')
  source('rake.r')
  source('utility.r')
  #source('graph.r')
  #source('clean.r')


  source(paste0(j, "FILEPATH/bind_covariates.R"))
  source(paste0(j, "FILEPATH/make_ko.R"))
  source(paste0(central, "get_location_metadata.R"))



  ###################  GET FULL DATA #########################################
  #####################################################
  
  ##USERNAME: this USERNAMEstem is new--pull in a data_id from STGPR database, create own knockouts
  
  data<-h5read(path, "data")
  data<-as.data.table(data)
  
  ##USERNAME: check data compatability w/ model
  if(nrow(data[is.na(data)])>0){message("You have some NAs in your data, these will be dropped")}
  if(length(unique(data$age_group_id))<=1  &  any(grepl("age_group_id", c(fixed_covs, random_effects)))) {stop("You specified age_group_id as a predictor, but you have less than 2 levels of this variable")}
  
  
  ###################  GET LOCS AND COVARIATES #########################################
  #####################################################
  
  locs<-get_location_metadata(version_id=location_version_id)[, .(location_id, super_region_name, region_name, location_name, level)]
  locs<-as.data.table(locs)


  ##USERNAME: bind on covariate data
  #data<-bind_covariates(df=data, cov_list)

  #data<-merge(data, locs, by="location_id")

  ##USERNAME: get covariates from covariate database
  data<-bind_covariates(df=data, cov_list, custom_cov_list=custom_covs)
  
  for(cov in cov_list){
    if(nrow(data[is.na(get(cov)),])>0){
      message("There are ",  nrow(data[is.na(get(cov)),])," missing estimates for ", cov, ", these rows will be dropped!")
      USERNAMEs.sleep(5)
      data<-data[!is.na(get(cov))]
    }
    
    
  }
  
  
  for(cov in cov_list){
    if(!cov %in% names(data)){ stop(paste(cov, "missing from data... make sure that the covariate has estimates!"))}
  }
  
  
  ##USERNAME: this creates a column in the dataset, and also saves the names of those colums in the 'polynoms' vector
  if(!is.null(polynoms)){
    polys<-strsplit(polynoms, "\\^")
    
    polynoms.t<-list()
    for(i in 1:length(polys)){
      basecov<-polys[[i]][1]
      if(!basecov %in% names(data)){ stop("ATTEMPTING TO CREATE POLYNOMIAL, MISSING: ", polys[[i]][1])}
      
      data[, paste0(basecov, polys[[i]][2]):=get(basecov)^as.numeric(polys[[i]][2])]
      polynoms.t[[i]]<-paste0(basecov, polys[[i]][2])
    }
    polynoms<-unlist(polynoms.t)
  }
  
  data<-merge(data, locs, by="location_id")
  
  
  ##USERNAME: this data offsetting function is from stgpr
  data<-offset.data(data, data_transform, offset)
  
  
  
  ###################  GET KOs AND SAVE TO TEMP FOLDER #########################################
  #######################################################
  
  ##USERNAME: set up by sexes or not
  if(by_sex==T){
    sex_list<-c("M", "F")
  }else{
    sex_list<-c("both_sexes")
  }
  
  
  for(sexchar in sex_list){
    message("Prepping ", sexchar, " data for KO creation")
    ##USERNAME: prep data for knockout creation
    
    
    if(sexchar=="M"){  sex<-1}
    if(sexchar=="F"){  sex<-2}
    if(sexchar=="both_sexes"){  sex<-c(1,2,3)}
    data.s<-data[sex_id %in% sex, ]
    
    ##USERNAME: set up data set if it's 'all ages'
    if(length(unique(data.s$age_group_id))==1){
      if(unique(data.s$age_group_id)==22  | unique(data.s$age_group_id)==27){
        data.s[, age_group_id:=22]  ##USERNAME: set to 22
        by_age<-0
      }else{
        stop("Ask about incorporating custom age group")
      }
    }else{
      by_age<-1
    }
    
    ko_items<-prep_ko(data.s, remove_subnats=remove_subnats, location_version_id=location_version_id, by_age=by_age, by_sex=ifelse(by_sex, 1, 0))
    message("  Done")
    
    message("Generating KOs")
    ##USERNAME: generate knockouts.  The arguments are set up to take the output of the prep_ko function directly
    test<-get_kos(ko_items[[1]], ko_items[[2]], ko_items[[3]], ko_items[[4]], prop_to_hold=ko_prop, kos=kos, seed=seed, no_new_ages=no_new_ages, only_data_locs=only_data_locs)
    message("  Done")
    
    ##USERNAME: writing the formmated dataset to avoid doing it for each child process
    saveRDS(test, file=paste0(output_folder, sexchar, "_full_data.rds"))
    message("Saved ", sexchar, " prepped data to temp folder")
  }
  
  
  


  ###################  GET ALL POSSIBLE MODELS #########################################
  #####################################################
  
  
  ##USERNAME: set up formula
  if(!is.null(random_effects)){
    form<-paste0( data_transform,"(data)~", paste0(cov_list, collapse="+"),
                  "+", paste0(polynoms, collapse="+"), "+", paste0(fixed_covs, collapse="+"), "+", paste0(random_effects, collapse="+"))
  }else{
    form<-paste0( data_transform,"(data)~", paste0(cov_list, collapse="+"),
                  "+", paste0(polynoms, collapse="+"), "+", paste0(fixed_covs, collapse="+"))
  }



  ##USERNAME: set up banned set logic--The result of this doesn't get applied, it's only for use w/ dredge() function
  sub<-paste(unlist(lapply(ban_pairs, function(x){
    thing<-c(rep(NA, times=length(x)-1))
    #i<-0
    #while(i < length(x)){ i<-i+1; if(i==10) break;
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



  message(paste("Getting models"))
  message(paste("  General formula:", form))
  message(paste("  Banned sets:", sub))
  
  
  ##USERNAME: add polynomials to cov_list:
  ##USERNAME: save original cov list for prior_signs later first
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

  ##USERNAME: paste all pieces together
  if(!is.null(random_effects)){
    if(!is.null(fixed_covs)){
      forms.n<-paste(
        paste0(data_transform, "(data)"),
        paste(temp_forms, paste0(fixed_covs, collapse="+"), paste0(random_effects, collapse="+"), sep="+"),
        sep="~")
      
      ##USERNAME: add on the null mod
      null_mod<-paste(paste0(data_transform,"(data)"),
                      paste(paste0(fixed_covs, collapse="+"), paste0(random_effects, collapse="+"), sep="+"),
                      sep="~")
      
    }else{
      forms.n<-paste(
        paste0(data_transform, "(data)"),
        paste(temp_forms, paste0(random_effects, collapse="+"), sep="+"),
        sep="~")
      
      ##USERNAME: add on the null mod
      null_mod<-paste(paste0(data_transform,"(data)"),
                      paste(paste0(random_effects, collapse="+"), sep="+"),
                      sep="~")
    }
  }else{
    forms.n<-paste(
      paste0(data_transform, "(data)"),
      paste(temp_forms, paste0(fixed_covs, collapse="+"), sep="+"),
      sep="~")
    
    ##USERNAME: add on the null mod
    null_mod<-paste(paste0(data_transform,"(data)"),
                    paste(paste0(fixed_covs, collapse="+"), sep="+"),
                    sep="~")
    
    if(!is.null(fixed_covs)){
      forms.n<-paste(
        paste0(data_transform, "(data)"),
        paste(temp_forms, paste0(fixed_covs, collapse="+"), sep="+"),
        sep="~")
      
      ##USERNAME: add on the null mod
      null_mod<-paste(paste0(data_transform,"(data)"),
                      paste(paste0(fixed_covs, collapse="+"), sep="+"),
                      sep="~")
      
    }else{
      forms.n<-paste(
        paste0(data_transform, "(data)"),
        paste(temp_forms, sep="+"),
        sep="~")
      
      ##USERNAME: add on the null mod
      null_mod<-paste(paste0(data_transform,"(data)"),
                      1,
                      sep="~")
    }
    
    
    
  }



  forms<-c(null_mod, forms.n)
  
  if(count_mods==T){
      message("Done--Returning ", length(forms), " formulas")
      if(by_sex==T){
        message(paste("You set by_sex==T, so real number of formulas to be evaluated is", length(forms)*2))
      }
    return(forms)
    stop("Done")
    }
  

  if(by_sex==T){
    message(paste(length(forms)*2, "total formulas to evaluate"))
  }else{
    message(paste(length(forms), "total formulas to evaluate"))
  }
  saveRDS(forms, file=paste0(output_folder, "forms.rds"))

  message(paste0("Model combinations saved to ", paste0(output_folder, "forms.rds")))
  
  


  ###################  LAUNCH JOBS #########################################
  #####################################################
  ###################################################






  ##USERNAME: male and female formula lists need to be same length (legnth(mforms)==length(fforms) is TRUE)

  file_list<-list()
  for(sex in sex_list){
    for(i in 1:length(forms)){
    if(F){
      i<-16
      sex<-"both_sexes"
      data_id = 5982
      data_transform<-"logit"
      date<-gsub("-", "_", Sys.Date())
    }


      command <- paste0("qsub -pe multi_slot ", slots_per_job, " -P ", proj, " -N ", paste0("oos_", sex, "_", i), 
                        " -o FILEPATH", username, "/output -e FILEPATH", username, "/errors", " FILEPATH/shell.sh ",
                        sex, " ",  data_id, " ", date, " ", i, " ", data_transform, " ", modtype, " ", kos)
      system(command)

     
    }
  }




  message("Waiting on jobs...")
  job_hold("oos_")
  message("Finished model testing")

  
  ################### READ IN RESULTS, RANK, OUTPUT #########################################
  #####################################################
  
  
  rmse_files<-list.files(path=output_folder, pattern=".csv", full.names=T)
  
  if(length(rmse_files)!=length(forms) & by_sex==F){
    message("Some model results are missing! Jobs may have broken")
  }
  if(length(rmse_files)!=2*length(forms) & by_sex==T){
    message("Some model results are missing! Check error logs; jobs may have broken")
  }
  
  ##USERNAME: read in the results
  message("Reading in results, and ranking models")
  stack<-list()
  for(i in 1:length(rmse_files)){
    stack[[i]]<-fread(rmse_files[i])
  }
  rmses<-rbindlist(stack, fill=T)
  
  
  ################### RANK MODELS #########################################
  #####################################################
  
  ##USERNAME: rank models by selected method
  if(rank_method=="oos.rmse"){
    ##USERNAME: sort by oos rmse
    rmses<-setorder(rmses, out_rmse)
  }
  if(rank_method=="aic"){
    rmses<-setorder(rmses, aic)
  }
  
  
  
  
  ################### SUBSET MODELS THAT VIOLATE PRIOR SIGNS #########################################
  ################################################################
  
  ##USERNAME: remove any models that violate prior signs
  if(!is.null(prior_sign)){
    message("Removing models that violate prior signs")
    signs<-data.table(sig=prior_sign, cov=og_cov_list)
    
    for(i in 1:nrow(signs)){
      cov<-signs[i, cov]
      sign<-signs[i, sig]
      ##USERNAME: get models where cov doesn't violate sign
      if(sign==-1){
        message(cov, " must be negative")
        message("  Dropping ", nrow(rmses[get(paste0(cov, "_fixd"))>0,])," model(s) where ", cov, " is greater than 0")
        
        rmses<-rmses[get(paste0(cov, "_fixd"))<=0  |  is.na(get(paste0(cov, "_fixd"))),]
      }
      if(sign==1){
        message(cov, " must be positive")
        message("  Dropping ", nrow(rmses[get(paste0(cov, "_fixd"))<0,])," model(s) where ", cov, " is less than 0")
        
        rmses<-rmses[get(paste0(cov, "_fixd"))>=0  |  is.na(get(paste0(cov, "_fixd"))),]
      }
      
      
    }
    
    
  }
  
  
  
  
  ################### SAVE #########################################
  ################################################################
  
  message("Cleaning out temp folder...")
  unlink(output_folder, recursive=T)
  
  message("Done")
  return(rmses)
}


