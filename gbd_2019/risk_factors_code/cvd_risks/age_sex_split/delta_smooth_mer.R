####################
## Purpose: General purpose mixed-effects model in TMB with non-linear age effects
## gdbsource(file = "tmb_hosp_cw.R", T)
########################

require(ggplot2)
require(data.table)
require(lme4) ## this is for X and Z matrix creation only

require(TMB)
require(mvtnorm)

################### custom optimize wrapper script for tmb #########################################
## function to avoid tmbhelper bug
custom_optimize<-function (obj, fn = obj$fn, gr = obj$gr, startpar = obj$par, 
                           lower = rep(-Inf, length(startpar)), upper = rep(Inf, length(startpar)), 
                           getsd = TRUE, control = list(eval.max = 10000, iter.max = 10000, 
                                                        trace = 0), bias.correct = FALSE, 
                           bias.correct.control = list(sd = FALSE, split = NULL, nsplit = NULL, vars_to_correct = NULL), 
                           savedir = NULL, loopnum = 3, newtonsteps = 0, n = Inf, ...) 
{
  BS.control = list(sd = FALSE, split = NULL, nsplit = NULL, 
                    vars_to_correct = NULL)
  for (i in 1:length(bias.correct.control)) {
    if (tolower(names(bias.correct.control)[i]) %in% tolower(names(BS.control))) {
      BS.control[[match(tolower(names(bias.correct.control)[i]), 
                        tolower(names(BS.control)))]] = bias.correct.control[[i]]
    }
  }
  start_time = Sys.time()
  parameter_estimates = nlminb(start = startpar, objective = fn, 
                               gradient = gr, control = control, lower = lower, upper = upper)
  for (i in seq(2, loopnum, length = max(0, loopnum - 1))) {
    Temp = parameter_estimates[c("iterations", "evaluations")]
    parameter_estimates = nlminb(start = parameter_estimates$par, 
                                 objective = fn, gradient = gr, control = control, 
                                 lower = lower, upper = upper)
    parameter_estimates[["iterations"]] = parameter_estimates[["iterations"]] + 
      Temp[["iterations"]]
    parameter_estimates[["evaluations"]] = parameter_estimates[["evaluations"]] + 
      Temp[["evaluations"]]
  }
  for (i in seq_len(newtonsteps)) {
    g <- as.numeric(gr(parameter_estimates$par))
    h <- optimHess(parameter_estimates$par, fn = fn, gr = gr)
    parameter_estimates$par <- parameter_estimates$par - 
      solve(h, g)
    parameter_estimates$objective <- fn(parameter_estimates$par)
  }
  parameter_estimates = parameter_estimates[c("par", "objective", 
                                              "iterations", "evaluations")]
  parameter_estimates[["run_time"]] = Sys.time() - start_time
  parameter_estimates[["max_gradient"]] = max(abs(gr(parameter_estimates$par)))
  parameter_estimates[["Convergence_check"]] = ifelse(parameter_estimates[["max_gradient"]] < 
                                                        1e-04, "There is no evidence that the model is not converged", 
                                                      "The model is likely not converged")
  parameter_estimates[["number_of_coefficients"]] = c(Total = length(unlist(obj$env$parameters)), 
                                                      Fixed = length(startpar), Random = length(unlist(obj$env$parameters)) - 
                                                        length(startpar))
  parameter_estimates[["AIC"]] = TMBhelper::TMBAIC(opt = parameter_estimates)
  if (n != Inf) {
    parameter_estimates[["AICc"]] = TMBhelper::TMBAIC(opt = parameter_estimates, 
                                                      n = n)
    parameter_estimates[["BIC"]] = TMBhelper::TMBAIC(opt = parameter_estimates, 
                                                     p = log(n))
  }
  parameter_estimates[["diagnostics"]] = data.frame(Param = names(startpar), 
                                                    starting_value = startpar, Lower = lower, MLE = parameter_estimates$par, 
                                                    Upper = upper, final_gradient = as.vector(gr(parameter_estimates$par)))
  if (getsd == TRUE) {
    h <- optimHess(parameter_estimates$par, fn = fn, gr = gr)
    if (is.character(try(chol(h), silent = TRUE))) {
      warning("Hessian is not positive definite, so standard errors are not available")
      if (!is.null(savedir)) {
        capture.output(parameter_estimates, file = file.path(savedir, 
                                                             "parameter_estimates.txt"))
      }
      return(list(opt = parameter_estimates, h = h))
    }
    if (bias.correct == FALSE | is.null(BS.control[["vars_to_correct"]])) {
      if (!is.null(BS.control[["nsplit"]])) {
        if (BS.control[["nsplit"]] == 1) 
          BS.control[["nsplit"]] = NULL
      }
      parameter_estimates[["SD"]] = sdreport(obj = obj, 
                                             par.fixed = parameter_estimates$par, hessian.fixed = h, 
                                             bias.correct = bias.correct, bias.correct.control = BS.control[c("sd", 
                                                                                                              "split", "nsplit")], ...)
    }
    else {
      if ("ADreportIndex" %in% names(obj$env)) {
        Which = as.vector(unlist(obj$env$ADreportIndex()[BS.control[["vars_to_correct"]]]))
      }
      else {
        parameter_estimates[["SD"]] = sdreport(obj = obj, 
                                               par.fixed = parameter_estimates$par, hessian.fixed = h, 
                                               bias.correct = FALSE, ...)
        Which = which(rownames(summary(parameter_estimates[["SD"]], 
                                       "report")) %in% BS.control[["vars_to_correct"]])
      }
      if (!is.null(BS.control[["nsplit"]]) && BS.control[["nsplit"]] > 
          1) {
        Which = split(Which, cut(seq_along(Which), BS.control[["nsplit"]]))
      }
      Which = Which[sapply(Which, FUN = length) > 0]
      if (length(Which) == 0) 
        Which = NULL
      message(paste0("Bias correcting ", length(Which), 
                     " derived quantities"))
      parameter_estimates[["SD"]] = sdreport(obj = obj, 
                                             par.fixed = parameter_estimates$par, hessian.fixed = h, 
                                             bias.correct = TRUE, bias.correct.control = list(sd = BS.control[["sd"]], 
                                                                                              split = Which, nsplit = NULL), ...)
    }
    parameter_estimates[["Convergence_check"]] = ifelse(parameter_estimates$SD$pdHess == 
                                                          TRUE, parameter_estimates[["Convergence_check"]], 
                                                        "The model is definitely not converged")
  }
  if (!is.null(savedir)) {
    save(parameter_estimates, file = file.path(savedir, "parameter_estimates.RData"))
    capture.output(parameter_estimates, file = file.path(savedir, 
                                                         "parameter_estimates.txt"))
  }
  if (parameter_estimates[["Convergence_check"]] != "There is no evidence that the model is not converged") {
    message("#########################")
    message(parameter_estimates[["Convergence_check"]])
    message("#########################")
  }
  return(parameter_estimates)
}




################### delta model #########################################
## compile the model any time this script gets sourced
TMB::compile("FILEAPTH/tmb_hosp_cw.cpp")
dyn.load(dynlib("FILEPATH/tmb_hosp_cw"))

delta_smooth_mer<-function(response, data,
                           fixefs = NULL, ranefs = NULL,
                           delta_vars = NULL, mesh_points = NULL, 
                           tau = rep(1, times=length(delta_vars)), est_tau = T,
                           ses = NULL, weights = NULL, kos = NULL,
                           sigma_l_prior_means = NULL, sigma_l_prior_sds = NULL, 
                           silent_adfun = F, 
                           X=NULL, Z=NULL,
                           family="gaussian", sample_size=NULL
                           ){
  
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
  source("FILEPATH/model_helper_functions.R") ## this contains functions for creating random effects model matricies and formulas
  
  ################### DATA CHECKS #########################################
  if(!family %in% c("gaussian", "binomial")){stop("Specified family not supported!")}
  if(length(data[[response]])!=length(sample_size) & !is.null(sample_size)){stop("Response vector different length than sample size vector!")}
    
  ################### GET DATA INPUTS #########################################
  data <- copy(data)
  
  ## quick checks
  for(var in c(fixefs, ranefs, delta_vars)){
    if(!var %in% names(data)){stop("Missing ", var, " from data")}
  }
  
  form <- as.formula(make_formula(response = response, fixefs = fixefs, ranefs = ranefs))
  
  ## number of levels of random effects
  n_l <- length(ranefs)
  n_s <- sum(get_ranef_lvl_counts(data, form)) ## total number of random effects
  n_dims <- length(delta_vars) ## number of dimensions to delta smooth
  
  
  ################### CONSTRUCT MODEL MATRICIES #########################################
  ## setup random effects model matrix
  if(missing(Z)){
    if(n_l>0){
      ## function defined above
      Z <- make_ranef_matrix(df = data, form = form)
      
      ## get number of random effects at each level
      z_names <- tstrsplit(colnames(Z), ";value:")[[1]]
      l_s <- as.numeric(factor(z_names, levels=ranefs))
      
    } else {
      Z <- matrix(0, nrow=nrow(data), ncol=0)
      l_s <- 1
    }
  }

  ## setup fixed effects model matrix
  if(missing(X)){
    if(length(fixefs)>0){
      ## formula w/o random effects
      fix_form <- paste0(paste0(response, "~0+"), paste(fixefs, collapse="+"))
      setcolorder(data, fixefs)
      X <- as.matrix(model.matrix(formula(fix_form), data=data))
      
    }else{
      X <- matrix(0, nrow=nrow(data), ncol=0)
    }
  }
    
  ################### SETUP DELTA VARS AND MAP PARAMS #########################################
  ## if mesh points aren't given, make each uniquely observed value a mesh point
  if(!missing(delta_vars) & !is.null(delta_vars)){
    if(missing(mesh_points)){
      mesh_points <- lapply(1:length(delta_vars), function(x){
        sort(unique(data[[delta_vars[x]]]))
      })
      names(mesh_points) <- delta_vars
    }
    
    ## checks on mesh points
    if(any(!names(mesh_points) %in% delta_vars)){stop("You provided a mesh_point var that is missing from your delta_vars")}
    if(any(!delta_vars %in% names(mesh_points))){stop("You provided a delta_var that is missing from your mesh_points")}
    if(any(!names(mesh_points)!=names(delta_vars))){stop("Please make sure the order of entries in mesh_points matches order of delta_vars")}
    
    ## create delta matrix and get number of values for each dimension
    A <- make_delta_matrix(mesh_points, data)
    n_xvals <- unlist(lapply(mesh_points, length))
    
  }

  ################### SETUP MAP PARAMS #########################################
  set_pars <- list()
  option_vec <- list()
  ## if tau was supplied by the user
  if(est_tau==F){
    if(length(tau)!=length(delta_vars)){stop("Length of tau unequal to length of delta_vars")}
    set_pars[["log_tau_param"]] <- factor(rep(NA, times=n_dims))
  }
  if(is.null(tau) | missing(tau)){
    tau <- rep(exp(1), times=length(delta_vars)) ##  set tau data to 1 so tau estimation is multiplied by 1
  }
  
  ## if no random effects
  if(n_l==0){
    set_pars[["epsilon_s"]]<-factor(rep(NA, times=0))
    set_pars[["log_sigma_l"]]<-factor(rep(NA, times=0))
  }
  
  ## if no variables to delta smooth
  if(missing(delta_vars) | is.null(delta_vars)){
    A <- matrix(0, nrow=nrow(data), ncol=0)
    n_xvals <- 0
    
    set_pars[["omega_a"]] <- factor(rep(NA, times=0))
    set_pars[["log_tau_param"]] <- factor(rep(NA, times=n_dims))
  }
  
  ## check if any of the delta variables have two or fewer x variables
  if(!missing(delta_vars) & !is.null(delta_vars)){
    
    ## get number of values in each dimension
    dvar_lengths<-unlist(lapply(delta_vars, function(d){
      length(unique(mesh_points[[d]]))
    }))
    
    ## stop if there are too few mesh points
    if(any(dvar_lengths<2)){stop("You provided one or fewer mesh points for at least one dimension")}
    
    ## set tau to a map param if there are only 2 mesh points for a given dim. This is to keep hessian positive definite
    if(any(dvar_lengths)==2){
      
      ## create a number for each tau param, this is how map in TMB knows to estimate different values for each param
      tau_map <- seq_len(length.out=length(dvar_lengths))
      
      ## where dvar_lengths are equal to 2, set the tau_map to NA. This is how TMB knows to not try to estimate that parameter (ie it stays at starting value)
      tau_map[dvar_lengths==2] <- NA
      set_pars[["log_tau_param"]] <- tau_map
    }
  }
  
  if(!missing(sigma_l_prior_means) & !is.null(sigma_l_prior_means)){
    if(length(sigma_l_prior_means)!=n_l){stop("You only provided priors for some levels of random effects!")}
    if(length(sigma_l_prior_sds)!=n_l){stop("You are missing SDs on your prior!")}
    
    option_vec[[1]] <- 1
    
  }else{
    option_vec[[1]] <- 0
    sigma_l_prior_means <- 0
    sigma_l_prior_sds <- 0
  }
  
  ## format weights
  if(missing(weights) | is.null(weights)){
    weights<-rep(1, times = nrow(data)) ## give every data point the same weight
  }
  ## format standard errors
  if(missing(ses) | is.null(ses)){
    ses<-rep(.01, times = nrow(data))
    option_vec[[3]] <- 0
  } else {
    option_vec[[3]] <- 1
  }
  ## format knockout binary
  if(missing(kos) | is.null(kos)){
    kos<-rep(0, times = nrow(data))
  }
  
  ## set family
  if(family=="gaussian"){
    option_vec[[2]] <- 1
    sampsize_i<-rep(1, times=nrow(data))
  }
  
  if(family=="binomial"){
    set_pars[["log_sigma_e"]]<-factor(rep(NA, times=1))
    option_vec[[2]] <- 2
    sampsize_i <- sample_size
  }
  
  ################### STORE TMB DATA #########################################
  ## create data list to give to tmb
  mod_data <- list(
    option_vec = option_vec,
    ## object lengths
    n_dims = length(delta_vars), n_xvals = n_xvals,
    n_k = ncol(X), n_l = n_l, n_s = ncol(Z), l_s = l_s,
    
    ## data
    y_i = data[[response]], sampsize_i = sampsize_i, 
    se_i = ses, weight_i = weights, ko_i = kos,
    
    ## matricies
    X_ik = X, Z_is = Z, A_ia = A,
    
    ## priors
    log_tau_data = log(tau),
    sigma_l_prior_mean = sigma_l_prior_means, sigma_l_prior_sd = sigma_l_prior_sds
    
    ## prediction data
    #n_preds = 0, X_ik_pred = X_pred, Z_is_pred = Z_pred, age_i_pred = age_i_pred
  )
  
  ## set initial parameter states
  init_pars <- list(
    alpha = 0, beta_k = rep(0, times = ncol(X)),
    #init_omega = 0, 
    omega_a = rep(0, times=ncol(A)),
    log_sigma_e = 1, log_tau_param = rep(1, times = n_dims), epsilon_s = rep(0, times = ncol(Z)), log_sigma_l = rep(1, times = mod_data$n_l)
  )
  
  randoms <- c("omega_a", "epsilon_s")
  
  ################### RUN MODEL #########################################
  obj <- MakeADFun(data = mod_data, parameters = init_pars, random = randoms, DLL = "tmb_hosp_cw", silent = silent_adfun, map=set_pars)
  mod1 <- custom_optimize(obj=obj)
  
  report <- sdreport(obj, getJointPrecision=T)

  ## get ADreported results
  means <- report$value
  ses <- report$sd
  means <- data.table(par=names(means), mean=means, se=ses)

  ests <- obj$env$last.par.best

  ## try to invert the precision matrix for covariance matrix. If this fails, Hessian was not positive/definite
  par_ests <- tryCatch(
    ## try matrix inversion
    expr = function(){
      cov_mat <- as.matrix(solve(report$jointPrecision))  ## this will break if Hessian not pos/def
      par_ests <- data.table(par=names(ests), est=ests, se=sqrt(diag(cov_mat)))

      return(par_ests)
    },
    error = function(err){
      message("Precision matrix inversion failed with the following error: ")
      message("  ", err)
      message("Returning model results w/o standard errors for debugging")

      par_ests <- "Precision matrix inversion failed, no standard errors availbile!"
      return(par_ests)
    }
  )
  
  ## rename some params
  means[par=="beta_k", par:=fixefs]
  means[par=="epsilon_s", par:=colnames(Z)]
  
  ################### SAVE #########################################
  return(list(
    model_results = list(mod = mod1, vals = means),  ## these two objects are model summaries
    tmb_outputs = list(obj = obj, report = report), ## these two objects are for predictions with the predict_delta_mer() function, are typical TMB outputs
    
    ## these are objects that are input or are intermediate data objects
    data_objects = list(
      values = list(response = response, fixefs = fixefs, ranefs = ranefs, mesh_points = mesh_points), ## model equation components
      matrices = list(X = X, Z = Z, A = A) ## all design matricies
      )
    ))
}

################### FUNCTION TO PREDICT A DELTA MODEL #########################################
predict_delta_mer <- function(fit, new_data, transform=NULL, n_draws=1000, ...){
  
  ##optional args
  # if(!exists("n_draws")){
  #   n_draws<-1000
  # }
  
  # if(!exists("return_draws")){
  #   return_draws<-F
  # }
  # if(!exists("upper_lower")){
  #   upper_lower<-F
  # }
  
################### SCRIPTS #########################################
source("FILEPATH/model_helper_functions.R") ## this contains functions for creating random effects model matricies and formulas
  
################### SIMULATE DRAWS #########################################
  if(F){
    fit <- copy(tmb_mod4)
  }
  
  ## these objects come from a delta smooth model, fit iwth the delta_smooth_mer() function
  obj <- fit$tmb_outputs[[1]]
  report <- fit$tmb_outputs[[2]]
  
  ## get parameter draws, option to specify number of draws unused here
  par_draws <- simulate_tmb_draws(obj, n_draws=n_draws)

  ## initialize draw list here. Gets filled out below depending on which effects need to be predicted
  draw_list <- list(
    alpha = par_draws[, names(par_draws)=="alpha", with=F]
  )
  
################### SETUP DATA #########################################
  ## get data from fit
  values <- fit$data_objects$values
  matrices <- fit$data_objects$matrices
  
  form <- formula(make_formula(response = values$response, fixefs = values$fixefs, ranefs = values$ranefs))
  
  X_pred <- make_fixef_matrix(new_data, fixefs=values$fixefs)
  Z_pred <- make_ranef_matrix(new_data, form=form, training_matrix = matrices$Z)
  
  if(ncol(fit$data_objects$matrices$A)>0){
    A_pred <- make_delta_matrix(xvals = values$mesh_points, df=new_data)
  }
  
  data_list <- list()
  
################### PREDICT #########################################
  ## setup prediction math, fill out data_list and draw_list
  pred_math <- "alpha"
  ## if there are fixed effects
  if(length(values$fixefs)>0){
    pred_math <- paste0(pred_math, " + X %*% beta")
    data_list$X <- X_pred
    draw_list$beta <- par_draws[, names(par_draws)=="beta_k", with=F]
  }
  
  ## if there are random effects
  if(length(values$ranefs)>0){
    pred_math <- paste0(pred_math, " + Z %*% epsilon")
    data_list$Z <- Z_pred
    draw_list$epsilon <- par_draws[, names(par_draws)=="epsilon_s", with=F]
  }
  
  ##if there are delta effects
  if(ncol(fit$data_objects$matrices$A)>0){
    pred_math <- paste0(pred_math, " + A %*% omega")
    data_list$A <- A_pred
    draw_list$omega <- par_draws[, names(par_draws)=="omega_a", with=F]
  }
  ## add transformation if given
  if(!missing(transform)){
    pred_math <- paste0(transform, "(", pred_math, ")")
  }
  
  ## prediction function from model_helper_functions.R
  preds <- predict_draws(data_list = data_list, draw_list = draw_list, prediction_math = pred_math, ...)
                       #return_draws = return_draws, upper_lower = upper_lower) ## options to return draws or to give upper/lower
  
  return(preds)
}
