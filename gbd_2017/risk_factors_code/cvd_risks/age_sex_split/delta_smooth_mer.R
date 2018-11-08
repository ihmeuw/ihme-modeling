####################
##Author: USERNAME
##Date:6/11/2018
##Purpose: General purpose mixed-effects model in TMB with non-linear age effects
# gdbsource(file = "tmb_hosp_cw.R", T)
########################

##USERNAME: documentation coming.. Majority of defined sub-functions come from this R script: "FILEPATH/utility/model_helper_functions.R"


################### PATHS AND ARGS #########################################
################################################################


delta_smooth_mer<-function(response, data,
                           fixefs = NULL, ranefs = NULL,
                           delta_vars = NULL, mesh_points = NULL, 
                           tau = rep(1, times=length(delta_vars)), est_tau = F,
                           ses = NULL, weights = NULL, kos = NULL,
                           silent_adfun = F, 
                           #pred_data = NULL, n_draws = 1000, return_draws = F,
                           X=NULL, Z=NULL
                           #X_pred=NULL, Z_pred=NULL
                           ){
  
  
  require(ggplot2)
  require(data.table)
  require(lme4) ##USERNAME: this is for X and Z matrix creation only
  
  require(TMB)
  require(TMBhelper)
  require(mvtnorm)
  #library(tmbstan)
  
  
  if(F){
    data<-copy(ran_sims)
    response<-"sim_data"
    fixefs<-c( "V1")
    ranefs<-c("level_1")
    delta_vars<-paste0("dim_", 1:2)
    
    
    ses<-NULL
    weights<-NULL
    kos<-NULL
    pred_data<-copy(ran_sims)
    #form <- "sim_data ~ V1 + V2 + (1 | level_1) + (1 | level_2)"
    tau <- rep(exp(1), times=length(delta_vars))
    silent_adfun <- F
    
    
    

  }
  
  ################### SCRIPTS #########################################
  ################################################################
  
  tmb_path<-"FILEPATH"
  orig_wd<-getwd() ##USERNAME: get current wd and reset after compiling tmb model to avoid issues
  setwd(tmb_path)
  TMB::compile("tmb_hosp_cw.cpp")
  dyn.load(dynlib("tmb_hosp_cw"))
  #gdbsource(file = "tmb_hosp_cw.cpp", interactive=T)
  
  
  source("FILEPATH/utility/model_helper_functions.R") ##USERNAME: this contains functions for creating random effects model matricies and formulas
  
  
  ################### GET DATA INPUTS #########################################
  ################################################################
  
  data<-copy(data)
  
  ##USERNAME: quick checks
  for(var in c(fixefs, ranefs, delta_vars)){
    if(!var %in% names(data)){stop("Missing ", var, " from data")}
  }
  
  form<-as.formula(make_formula(response = response, fixefs = fixefs, ranefs = ranefs))
  
  
  ##USERNAME: number of levels of random effects
  #n_l<-length(findbars(form))
  n_l<-length(ranefs)
  n_s<-sum(get_ranef_lvl_counts(data, form)) ##USERNAME: total number of random effects
  n_dims<-length(delta_vars) ##USERNAME: number of dimensions to delta smooth
  
  
  ################### CONSTRUCT MODEL MATRICIES #########################################
  ################################################################
  
  ##USERNAME: setup random effects model matrix
  if(missing(Z)){
    if(n_l>0){
      ##USERNAME: function defined above
      Z<-make_ranef_matrix(df = data, form = form)
      
      ##USERNAME: get number of random effects at each level
      z_names<-tstrsplit(colnames(Z), ";value:")[[1]]
      l_s<-as.numeric(factor(z_names, levels=ranefs))
      
    }else{
      Z<-matrix(0, nrow=nrow(data), ncol=0)
      l_s<-1
    }
  }


  
  ##USERNAME: setup fixed effects model matrix
  if(missing(X)){
    if(length(fixefs)>0){
      ##USERNAME: formula w/o random effects
      fix_form<-paste0(paste0(response, "~0+"), paste(fixefs, collapse="+"))
      setcolorder(data, fixefs)
      X<-as.matrix(model.matrix(formula(fix_form), data=data))
      
    }else{
      X<-matrix(0, nrow=nrow(data), ncol=0)
    }
  }
  #model.frame(form, data=ran_sims)
  
  ################### SETUP DELTA VARS AND MAP PARAMS #########################################
  ################################################################
  
  ##sy; if mesh points aren't given, make each uniquely observed value a mesh point
  if(!missing(delta_vars)){
    if(missing(mesh_points)){
      mesh_points<-lapply(1:length(delta_vars), function(x){
        sort(unique(data[[delta_vars[x]]]))
      })
      names(mesh_points)<-delta_vars
    }
    if(any(!names(mesh_points) %in% delta_vars)){stop("You provided a delta_var that is missing from your mesh_points")}
    if(any(!delta_vars %in% names(mesh_points))){stop("You provided a mesh_point var that is missing from your delta_vars")}
    if(any(!names(mesh_points)!=names(delta_vars))){stop("Please make sure the order of entries in mesh_points matches order of delta_vars")}
    
    
    ##USERNAME: create delta matrix and get number of values for each dimension
    A<-make_delta_matrix(mesh_points, data)
    n_xvals<-unlist(lapply(mesh_points, length))
  }else{
    
    A<-matrix(0, nrow=nrow(data), ncol=0)
    n_xvals<-0
    
    set_pars[["omega_a"]]<-factor(rep(NA, times=1))
    set_pars[["log_tau_param"]]<-factor(rep(NA, times=n_dims))
  }

  ################### SETUP REMAINING MAP PARAMS #########################################
  ################################################################
  set_pars<-list()
  if(est_tau==F){
    
    if(length(tau)!=length(delta_vars)){stop("Length of tau unequal to length of delta_vars")}
    set_pars[["log_tau_param"]]<-factor(rep(NA, times=n_dims))
  }else{
    tau<-rep(exp(1), times=length(delta_vars)) ##USERNAME: set tau data to 1 so tau estimation is multiplied by 1
  }
  
  ##USERNAME: if no random effects
  if(n_l==0){
    set_pars[["epsilon_s"]]<-factor(rep(NA, times=0))
    set_pars[["log_sigma_l"]]<-factor(rep(NA, times=0))
  }
  
  ##USERNAME: format weights
  if(missing(weights)){
    weights<-rep(1, times = nrow(data)) ##USERNAME: give every data point the same weight
  }
  if(missing(ses)){
    ses<-rep(.01, times = nrow(data))
  }
  if(missing(kos)){
    kos<-rep(0, times = nrow(data))
  }
  
  ################### SETUP OPTION VECTOR #########################################
  ################################################################
  
  option_vec<- 0 #c(ifelse(link=="logit", 1, 0))
  
  ################### STORE TMB DATA #########################################
  ################################################################
  
  ##USERNAME: create data list to give to tmb
  mod_data<-list(
    option_vec = option_vec,
    ##USERNAME: object lengths
    n_dims = length(delta_vars), n_xvals = n_xvals,
    n_k = ncol(X), n_l = n_l, n_s = ncol(Z), l_s = l_s,
    
    ##USERNAME: data
    y_i = data[[response]], se_i = ses, weight_i = weights, ko_i = kos,
    X_ik = X, Z_is = Z, A_ia = A,
    log_tau_data = log(tau)
    
    ##USERNAME: prediction data
    #n_preds = 0, X_ik_pred = X_pred, Z_is_pred = Z_pred, age_i_pred = age_i_pred
  )
  
  ##USERNAME: set initial parameter states
  init_pars<-list(
    alpha = 0, beta_k = rep(0, times = ncol(X)),
    #init_omega = 0, 
    omega_a = rep(0, times=ncol(A)),
    log_sigma_e = 1, log_tau_param = rep(1, times = n_dims), epsilon_s = rep(0, times = ncol(Z)), log_sigma_l = rep(1, times = mod_data$n_l)
  )
  
  randoms<-c("omega_a", "epsilon_s")
  #randoms<-c("epsilon_s")
  
  #set_pars<-list()
  #set_pars[["alpha"]]<-factor(rep(NA, times=1)) ##USERNAME: turn this off for simulation 
  
  ################### DATA CHECKS #########################################
  ################################################################

  ################### RUN MODEL #########################################
  ################################################################
  
  obj<-MakeADFun(data = mod_data, parameters = init_pars, random = randoms, DLL = "tmb_hosp_cw", silent = silent_adfun, map=set_pars)
  
  mod1<-TMBhelper::Optimize(obj=obj)
  
  report<-sdreport(obj, getJointPrecision=T)
  
  setwd(orig_wd)
  
  
  ################### GET RESULTS #########################################
  ################################################################
  
  ##USERNAME: get ADreported results
  mus<-report$value
  ses<-report$sd
  mus<-data.table(par=names(mus), mu=mus, se=ses)
  #age_map[, mu:=mu+fit[par=="beta_k", est]]
  
  
  ests<-obj$env$last.par.best
  cov_mat<-as.matrix(solve(report$jointPrecision))  ##USERNAME: this will break if Hessian not pos/def
  
  ##USERNAME: rename random effects
  #names(ests)[names(ests)=="epsilon_s"]<-colnames(Z)
  
  par_ests<-data.table(par=names(ests), est=ests, se=sqrt(diag(cov_mat)))
  
  ##USERNAME: rename some params
  mus[par=="beta_k", par:=fixefs]
  mus[par=="epsilon_s", par:=colnames(Z)]
  

  return(list(
    model_results = list(mod = mod1, vals = mus),
    tmb_outputs = list(obj = obj, report = report), ##USERNAME: these two objects are for predictions with the predict_delta_mer() function
    
    data_objects = list(
      values = list(response = response, fixefs = fixefs, ranefs = ranefs, mesh_points = mesh_points),
      matrices = list(X = X, Z = Z, A = A)
      )
    ))
}



################### FUNCTION TO PREDICT A DELTA MODEL #########################################
################################################################


predict_delta_mer<-function(fit, new_data, transform=NULL, ...){
  
  ##optional args
  if(!exists("n_draws")){
    n_draws<-1000
  }
  
  # if(!exists("return_draws")){
  #   return_draws<-F
  # }
  # if(!exists("upper_lower")){
  #   upper_lower<-F
  # }
  
  ################### SCRIPTS #########################################
  ################################################################
  
  source("FILEPATHutility/model_helper_functions.R") ##USERNAME: this contains functions for creating random effects model matricies and formulas
  
  ################### SIMULATE DRAWS #########################################
  ################################################################
  
  if(F){
    fit<-copy(tmb_mod4)
  }
  
  ##USERNAME: these objects come from a delta smooth model, fit iwth the delta_smooth_mer() function
  obj<-fit$tmb_outputs[[1]]
  report<-fit$tmb_outputs[[2]]
  
  par_draws<-simulate_tmb_draws(obj, report) ##USERNAME: option to specify number of draws unused here
  #par_draws<-simulate_tmb_draws(obj, report) ##USERNAME: option to specify number of draws
  
  ##USERNAME: initialize draw list here. Gets filled out below depending on which effects need to be predicted
  draw_list<-list(
    alpha = par_draws[, names(par_draws)=="alpha", with=F]
  )
  
  ################### SETUP DATA #########################################
  ################################################################
  
  ##USERNAME: get data from fit
  values<-fit$data_objects$values
  matrices<-fit$data_objects$matrices
  
  form<-formula(make_formula(response = values$response, fixefs = values$fixefs, ranefs = values$ranefs))
  
  X_pred<-make_fixef_matrix(new_data, fixefs=values$fixefs)
  Z_pred<-make_ranef_matrix(new_data, form=form, training_matrix = matrices$Z)
  A_pred<-make_delta_matrix(xvals = values$mesh_points, df=new_data)
  
  data_list<-list()
  
  
  ################### PREDICT #########################################
  ################################################################
  
  ##USERNAME: setup prediction math, fill out data_list and draw_list
  pred_math<-"alpha"
  ##USERNAME: if there are fixed effects
  if(length(values$fixefs)>0){
    pred_math<-paste0(pred_math, " + X %*% beta")
    data_list$X<-X_pred
    draw_list$beta<-par_draws[, names(par_draws)=="beta_k", with=F]
  }
  
  ##USERNAME: if there are random effects
  if(length(values$ranefs)>0){
    pred_math<-paste0(pred_math, " + Z %*% epsilon")
    data_list$Z<-Z_pred
    draw_list$epsilon<-par_draws[, names(par_draws)=="epsilon_s", with=F]
  }
  
  ##USERNAME:if there are delta effects
  if(length(values$mesh_points)>0){
    pred_math<-paste0(pred_math, " + A %*% omega")
    data_list$A<-A_pred
    draw_list$omega<-par_draws[, names(par_draws)=="omega_a", with=F]
  }
  ##USERNAME: add transformation if given
  if(!missing(transform)){
    pred_math<-paste0(transform, "(", pred_math, ")")
  }
  
  ##USERNAME: prediction function from model_helper_functions.R
  preds<-predict_draws(data_list = data_list, draw_list = draw_list, prediction_math = pred_math, ...)
                       #return_draws = return_draws, upper_lower = upper_lower) ##USERNAME: options to return draws or to give upper/lower
  
  return(preds)
}
