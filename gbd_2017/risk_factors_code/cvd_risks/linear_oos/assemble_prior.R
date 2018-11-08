################################
##author:USERNAME
##date: 11/10/2017
##purpose: -Average the results of the test_prior() function and create ensemble linear predictions
##notes:   -
##
##      03/28/2017: running out of sample rmse for all possible models
##
##      11/2/2017: submitted jobs will run from the cvd singularity. Require that cov_list covariates be full covariate names (change to central function)
##                    Added forms_per_job argument
########################################################################################################################################################
#            ##### test_prior()  Function ######
#
#
#       #### Purpose: Model average a set of linear priors by some weight
#
#       #### Notes: -
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
#           -cov_list: A character string of covariates in the covariate_name form.  Must include any custom covariates (see custom_covs argument)
#                         This used to take covariate_name_short strings!!!! The change is due to changes in the central comp machinery. Must now be a full covariate name!
#               ex:   cov_list<-c("Mean BMI", "Alcohol (liters per capita)", "omega 3 unadjusted(g)")
#
#           -data_transform: Character string, either "log" or "logit" to be inherited by STGPR's transform_data() function
#               ex:   data_transform="log
#
#           -username: Charcter, your ihme username.  For saving outputs and errors to sge
#               ex: username="syadgir"
#
#     ######### OPTIONAL #########
#
#
#
################# Output ###############################################################################################################################
#
#
#
########################################################################################################################################################





assemble_prior<-function(data, rmses, cov_list, data_transform,
                         custom_cov_list=NULL, polynoms=NULL, n_mods=10,
                         plot_mods=F, age_trend=F, plot_mods_path=NULL, username=NULL, proj, ##USERNAME: this line is all just for plotting
                         weight_col="out_rmse", by_sex=T, location_set_id=22){






  ###################  SETUP  #########################################
  #####################################################


  date<-gsub("-", "_", Sys.Date())


  require(data.table)
  require(plyr)
  require(DBI)
  require(dplyr)
  require(RMySQL)
  require(lme4)
  require(MuMIn)
  require(boot)
  require(ggplot2)
  require(rhdf5)
  #require(arm)
  require(matrixStats)


  ###################  PATHS AND ARGS  #########################################
  #####################################################

  stgpr_central<-"FILEPATH"

  ###################  SCRIPTS  #########################################
  #####################################################

  ##USERNAME: need to set wd fo rstgpr functions to get read in.. this is very annoying but settind original wd back, should work out
  wd_orig<-getwd()
  setwd(stgpr_central)
  source('register_data.r')
  source('setup.r')
  source('model.r')
  source('rake.r')
  source('utility.r')
  #source('graph.r')
  #source('clean.r')
  setwd(wd_orig)



  central<-paste0(j, "FILEPATH")

  source(paste0(central, "get_location_metadata.R"))
  source(paste0("FILEPATH/utility/job_hold.R"))
  source(paste0("FILEPATH/utility/append_pdfs.R"))


  ################### DEFINE PRED.LM FUNCTION #########################################
  ######################################################

  ##USERNAME: this is from patty, should be same as used in STGPR.  Allows lm or lmer
  pred.lm <- function(df, model, predict_re=0) {
    ## RE form
    re.form <- ifelse(predict_re==1, NULL, NA)
    ## Predict
    if (class(model) == "lmerMod") {
      prior <- predict(model, newdata=df, allow.new.levels=T, re.form=re.form)
    } else {
      prior <- predict(model, newdata=df)
    }
    return(prior)
  }



  ###################  QUICK CHECKS  #########################################
  #####################################################
  nec_cols<-c("data", "location_id", "year_id", "age_group_id", "sex_id")
  invisible(lapply(nec_cols, function(x){
    if(!x %in% names(data)){
      stop(paste0("Missing necessary column:", x))
    }
  }))
  nec_cols<-c(weight_col)
  invisible(lapply(nec_cols, function(x){
    if(!x %in% names(rmses)){
      stop(paste0("Missing necessary column:", x))
    }
  }))

  if(plot_mods==T & length(plot_mods_path)!=1){
    stop("plot_mods==T but you have not specified a valid plot_mods_path!")
  }


  if(n_mods>nrow(rmses)){
      message("You specified to average over ", n_mods, " models, but only supplied ", nrow(rmses), ". Only the supplied models will be averaged.")
      n_mods<-nrow(rmses)
    }

  ##USERNAME: drop data NAs (if square already, just make a new one)
  data<-data[!is.na(data)]
  locs<-get_location_metadata(location_set_id=location_set_id)

  ##USERNAME: get subnat locs for dropping from predictions (get dropped from data later)
  subnat_locs<-locs[level>=4, location_id]
  ##USERNAME: put back in england, wales, ireland, and china w/o macao
  subnat_locs<-setdiff(subnat_locs, c(44533, 4749, 361, 354, 434, 84, 433, 4636))


  l_set_v_id<-unique(locs$location_set_version_id)

  ###################  GET COVARIATES #########################################
  #####################################################

  ##USERNAME: set up data set if it's 'all ages'
  if(length(unique(data$age_group_id))==1){
    if(unique(data$age_group_id)==22  | unique(data.s$age_group_id)==27){
      data[, age_group_id:=22]  ##USERNAME: set to 22
      by_age<-0
    }else{
      stop("Ask Simon about incorporating custom age group")
    }
  }else{
    by_age<-1
  }

  sqr<-make_square(l_set_v_id, 1980, 2017, covariates=NA, by_age=by_age, by_sex=by_sex)

  ##USERNAME: get covariates
  sqr_and_names<-bind_covariates(sqr, cov_list=cov_list, custom_cov_list=custom_covs)
  sqr<-sqr_and_names[[1]] ##USERNAME: this is the data w/ bound covariate estimates
  cov_list<-sqr_and_names[[2]] ##USERNAME: these are the covariate_name_shorts


  if(by_sex==T){
    sex_list<-c("M", "F")
  }else{
    sex_list<-c("both_sexes")
  }


  ################### GET ANY POLYNOMIALS FOR SQR #########################################
  #####################################################


  ##USERNAME: this creates a column in the dataset, and also saves the names of those colums in the 'polynoms' vector
  if(!is.null(polynoms)){
    polys<-strsplit(polynoms, "\\^")

    polynoms.t<-list()
    for(i in 1:length(polys)){
      basecov<-polys[[i]][1]
      if(!basecov %in% names(sqr)){ stop("ATTEMPTING TO CREATE POLYNOMIAL, MISSING: ", polys[[i]][1])}

      sqr[, paste0(basecov, polys[[i]][2]):=get(basecov)^as.numeric(polys[[i]][2])]
      polynoms.t[[i]]<-paste0(basecov, polys[[i]][2])
    }
    polynoms<-unlist(polynoms.t)
  }
  ##USERNAME: add polynoms to cov_list
  og_cov_list<-cov_list
  cov_list<-c(cov_list, polynoms)

  ##USERNAME: get locs
  sqr<-merge(sqr, locs[, .(location_name, region_name, super_region_name, location_id)], by="location_id")



  ################### LOOP BY SEX #########################################
  #####################################################
  output<-list()
  for(sexchar in sex_list){
    ##USERNAME: run models
    message("Model averaging for ", sexchar)
    s_id<-ifelse(sexchar=="both_sexes", 3, ifelse(sexchar=="M", 1, 2))

    sqr.s<-sqr[sex_id==s_id,]
    data.s<-data[sex_id==s_id,]
    data.s<-data.s[!location_id %in% subnat_locs]

    rmses.s<-rmses[sex==sexchar]



    if(n_mods>nrow(rmses.s)){
      message("You specified to average over ", n_mods, " models for sex ", sexchar, ", but only supplied ", nrow(rmses.s), ". Only the supplied models will be averaged.")
      n_mods.s<-nrow(rmses.s)
    }else{
      n_mods.s<-n_mods
    }



    new_ages<-setdiff(unique(sqr.s$age_group_id), unique(data.s$age_group_id))
    if(length(new_ages)>0){
      message(" These age_group_ids are not in your data and will not be included in any models that have factor(age_group_id): ",
              paste0(new_ages, collapse = ", "))
      sqr.s<-sqr.s[!age_group_id %in% new_ages,]
    }


    ################### CREATE PREDICTIONS #########################################
    #####################################################
    for(n in 1:n_mods.s){
      message("  Predicting for model ", n)

      form<-rmses.s[n, covs]
      form<-paste0(paste0(data_transform,"(data)~", form))
      modtype<-ifelse(grepl("\\(1 \\|", form), "lmer", "lm")

      if(modtype=="lmer"){
        mod<-lmer(as.formula(form), data=data.s)
      }
      if(modtype=="lm"){
        mod<-lm(as.formula(form), data=data.s)
      }

      ##USERNAME: predict on the square
      sqr.s[, paste0("pred", n):=transform_data(pred.lm(sqr.s, mod), data_transform, reverse=T)]
      sqr.s[, paste0("wt", n):=rmses.s[n, get(weight_col)]]

      rm(mod)
    }
    n<-n_mods.s

    ################### AVERAGE PREDICTIONS #########################################
    #####################################################
    message(" Averaging..")
    wts<-1/rmses.s[1:n, get(weight_col)]
    invisible(lapply(1:n, function(x){
      #numerator<-sqr.s[, get(paste0("pred", x))]
      sqr.s[, paste0("numerator",x):=get(paste0("pred", x))*wts[x]]
    }))


    sqr.s[, ave_result:=rowSums(.SD)/sum(wts), .SDcols=grep("numerator", names(sqr.s), value=T)]


    ################### SAVE #########################################
    #####################################################

    sqr.s[, c(grep("numerator|pred|wt", names(sqr.s), value=T), cov_list):=NULL]

    output[[length(output)+1]]<-sqr.s
    message("Done averaging for ", sexchar)
  }

  output<-rbindlist(output)




}
