####################
## Purpose: This script holds useful modelling formulas
########################

require(data.table)
require(lme4)

source("FILEPATH/data_tests.R")

################### INV LOGIT #########################################

inv_logit<-function(x){
  1/(1+exp(-x))
}

################### MAKE R-SYNTAX FORMULA #########################################

## make formula with R ssyntax
make_formula<-function(response, fixefs=NULL, ranefs=NULL, add_terms=NULL){
  ## if no fixed effects
  if(length(fixefs)==0){
    form <- paste0(paste0(response, "~"), paste(paste0("(1|", ranefs, ")"), collapse="+"))
    
    ## if no fixed effects and no random effects
    if(length(ranefs)==0){
      form <- paste0(paste0(response, "~1"))
    }
  } else {
    ## if no random effects
    if(length(ranefs)==0){
      form <- paste0(paste0(response, "~"), paste(fixefs, collapse="+"), collapse="+")
      
    } else {
      ## if no fixed effects and no random effects
      form <- paste0(paste0(response, "~"), 
                   paste(c(paste(fixefs, collapse="+"), paste(paste0("(1|", ranefs, ")"), collapse="+")), collapse="+"))
    }
  }
  
  ## add additional terms
  if(length(add_terms)>0){
    add_terms <- paste0(add_terms, collapse="+")
    
    form <- paste0(c(form, add_terms), collapse="+")
  }
  
  ## save formula as a string, will need to convert w/ as.formula()
  string_form <- gsub(" ", "", form)
  #form<-as.formula(form)
  message("Model formula:")
  message(" ", string_form)
  return(as.formula(string_form))
}

################### EXPAND DATA #########################################
## Arguments
#   - starts_ends: a named list of charcter vectors of length 2. 
#       The name of the list is the new variable to be created. 
#       The first element is the variable of the starting value. 
#       The second element is the name of the variable of the end value
#     ex: list(age=c("age_start", "age_end"), exposure=c("exposure_start", "exposure_end"))
#
#   - var_values: a named list of numeric vectors of the values to split out at for a given variable
#
#   - value_weights: a named list of numeric vectors, the same length as the corresponding vector in var_values. Weights applied to each 

## Expand data 'integration' if a data point covers multiple discrete values
expand_data <- function(df, starts_ends, var_values, value_weights=NULL){
  
  df <- copy(df)
  
  ################### CHECKS #########################################
    ## make sure certain cols don't already exist
      new_cols<-c("temp_seq", paste0(names(starts_ends), "_temp_seq"), paste0("n_", names(starts_ends)),  "split_wt")
      lapply(new_cols, df=df, check_exists, not_exist=T) ##sy: check_exists function comes from data_tests.R script
  
    ## make sure certain cols do exist
      lapply(unlist(starts_ends), df=df, check_exists)
  
  
  ################### SETUP #########################################
    ## create row id number
      if("temp_seq" %in% names(df)){stop("Already a column called 'temp_seq' in data frame!")}
      df[, temp_seq:=1:.N]
  
    ## create original weight column where each row is 1
      df[, split_wt:=1]
  
  ################### LOOP THROUGH DIMENSION #########################################
    ## get number of values each data point covers in each dimension
      for(d in 1:length(starts_ends)){
    
    ## store some input info
        varname <- names(starts_ends)[d]
        vals <- var_values[[d]]
        cols <- starts_ends[[d]]
    
    ## create dim specific ind (for merging)
    df[, paste0(varname, "_temp_seq"):=1:.N]
    
  ################### ROUND VALUES TO NEAREST VALUE #########################################
    ## create temporary roundedstart/end cols
      get_nearest <- function(x, vals){
      diffs <- abs(x - vals)
      out <- unique(vals[diffs==min(diffs)])[1]
      return(out)
    }

    ## get temporary starting value
      df[, paste0("temp_", cols[1]):=sapply(get(cols[1]), get_nearest, vals=vals)]
      df[, paste0("temp_", cols[2]):=sapply(get(cols[2]), get_nearest, vals=vals)]
    
    ## make sure there are no end vals greater than starting vals
      if(any(df[[paste0("temp_", cols[2])]]<df[[paste0("temp_", cols[1])]])){stop("Ending value smaller than starting value for ", names(starts_ends)[d])}
    
  ################### FIND NUMBER OF EXPANSIONS FOR EACH ROW #########################################
    ## get number of values to expand out between the start and end for each row
      for(i in 1:nrow(df)){
      
      ## this line sums up the T/F for each val between the lower and upper for the row being looped through
        n_bet <- sum(sapply(vals, FUN=data.table::between, lower=df[[paste0("temp_", cols[1])]][i], upper=df[[paste0("temp_", cols[2])]][i]))
        df[i, paste0("n_", varname):=n_bet]
    }
    
  ################### EXPAND ROWS #########################################
    ## expand the number of rows
      expanded <- as.data.table(rep(df[[paste0(varname, "_temp_seq")]], times=df[[paste0("n_", varname)]]))
    
    ## get the new variable values
      new_vals <- unlist(lapply(1:length(unique(df[[paste0(varname,"_temp_seq")]])), function(x){
        loop_seq <- unique(df[[paste0(varname,"_temp_seq")]])[x]
      
        ## get minimum val for this value
        start_val <- df[get(paste0(varname, "_temp_seq"))==loop_seq, get(paste0("temp_", cols[1]))]
      
        ## get the values
        temp_vals <- var_values[[d]][var_values[[d]]>=start_val]
        temp_vals <- temp_vals[1:(df[get(paste0(varname, "_temp_seq"))==loop_seq, get(paste0("n_", varname))])]
        return(temp_vals)
    }))
    
    expanded <- cbind(expanded, new_vals)
    setnames(expanded, c("V1", "new_vals"), c(paste0(varname, "_temp_seq"), varname))
       
    df <- merge(expanded, df, by=paste0(varname, "_temp_seq"))
    
  ################### SCALE DOWN DATA WEIGHTS #########################################
  ## reduce weights
    if(missing(value_weights)){
      df[, split_wt:=split_wt/get(paste0("n_", varname))] ##sy: if each value given equal weight, just divide by number of splits performed
    } else {
      
  ## merge weights onto df
      wt_dt <- data.table(val=var_values[[d]], wt=value_weights[[d]])
      
      df <- merge(df, wt_dt, by.x=paste0())
      
    }
  }
  
  ################### CLEAN OUT TEMP COLS #########################################
  return(df)
}

################### MAKE FIXED EFFECTS MATRIX #########################################
make_fixef_matrix <- function(df, fixefs=NULL, add_intercept=F){
  df <- copy(df)
  df <- as.data.table(df)
  df[, temp_response:=1]
  
  if(length(fixefs)>0){
    fix_form <- paste0("temp_response~1+", paste(fixefs, collapse="+"))
    if(all(fixefs %in% names(df))){
      setcolorder(df, fixefs)
    }
    X <- as.matrix(model.matrix(formula(fix_form), data=df))
    
    ##sy: drop first column, the intercept created by model.matrix. This gets recreated in toggle below
    X <- as.matrix(X[, -1])
    
  } else {
    #X<-as.matrix(rep(0, times=nrow(df)), ncol=1)
    X <- array(0, dim = c(nrow=nrow(df), 0))
  }
  
  if(add_intercept==T){
    X <- cbind(rep(1, times=nrow(X)), X)
  }
  return(X)
}

################### MAKE RANDOM EFFECTS MATRIX #########################################
## create random effects matrix; training matrix to give random effects in case you're trying to make a prediction matrix 
make_ranef_matrix <- function(df, form, training_matrix=NULL){
  require(lme4) ## this gets the mkReTrms function
  df <- copy(df)
  
  ## find random effects and response
  ranefs <- unlist(as.character(lapply(findbars(form), "[[", 3)))
  response <- as.character(form)[2] ## may not work in all cases
  cols <- ranefs
  if(response!=""){cols <- c(response, cols)}
  
  #df<-df[, c(cols), with=F]
  if("x" %in% names(df)){
    stop("Column named 'x' in your df, please rename!")
  }
  
  if(length(ranefs)>0){
    ## format values of ranef columns to keep track of them easily
    invisible(
      ranef_order <- lapply(ranefs, function(x){
        df[, (x):=paste0(x,";value:", get(x))]
      })
    )
    
    if(is.null(training_matrix)){
      ## create matrix using lme4s functions
      re_terms <- mkReTrms(findbars(as.formula(form)), model.frame(subbars(form), data=df))
      Z <- as.matrix(t(re_terms[["Zt"]])) ## Z is returned transposed
      flist <- unique(unlist(re_terms[["flist"]])) ## flist gets the correct order of random effects
      Z <- Z[, flist] ## this re-orders Z to have correct order of random effects
    } else {
      
      df <- df[, ranefs, with=F]
      
      message("Constructing prediction matrix..")
      ## lapply to evaluate where the training matrix values equal prediction matrix values
      Z <- sapply(1:ncol(training_matrix), function(x){
        ranef <- tstrsplit(colnames(training_matrix)[x], ";value:")[[1]]
        as.numeric(df[[ranef]]==colnames(training_matrix)[x])
      })
      
      ## assert that Z is a matrix
      if(!is.matrix(Z)){Z <- matrix(Z, ncol=ncol(training_matrix))}
      colnames(Z) <- colnames(training_matrix)
      message("Done")
    }
  } else {
    Z <- array(0, dim = c(nrow=nrow(df), 0)) ## return empty array if no random effects
  }

  return(Z)
}

################### GET NUMBER OF RANDOM EFFECTS IN EACH LEVEL #########################################
get_ranef_lvl_counts <- function(df, form){
  
  ## find random effects
  ranefs <- unlist(as.character(lapply(findbars(form), "[[", 3)))
  
  if(length(ranefs)>0){
    n_s <- unlist(lapply(ranefs, function(x){
      length(unique(df[[x]]))
    }))
  } else {
    n_s <- array(0, dim=0)
  }

  
  return(n_s)
}

################### CREATE MATRiX FOR DELTA SMOOTHING #########################################
## xvals must be a named list. Each element needs to be a numeric vector of the values to estimate differences between for a given dimension
## df must be a data.table of data with column names corresponding to the names of the elements in the xvals list

make_delta_matrix <- function(xvals, df){
  ## xvals needs to be a named list.
  
  ## checks
  if(is.null(names(xvals))){stop("xvals must be a named list")}
  if(!all(names(xvals) %in% names(df))){stop("Names of xvals not in data")}
  
  ## loop over each dimension
  full_matrix <- lapply(1:length(xvals), function(d){
    
    ## loop over each value
    sub_matrix <- sapply(2:length(xvals[[d]]), function(x){
      
      xval <- xvals[[d]][x]
      prev_xval <- xvals[[d]][x-1]
      
      ## stop if any values are less than the initial values
      if(x==2){
        if(any(df[[names(xvals)[d]]] < prev_xval)){stop(message("You have values in ", names(xvals)[d], " that are less than the minimum mesh point!"))}
      }
      
      ## get binaries if row values are greater than or equal to the previous xval.
      temp <- as.numeric(df[[names(xvals)[d]]] > prev_xval)
      
      ## get change from previous xval or change from the value in the data and the xval if the value in the data is between previous xval
      change <- ifelse(data.table::between(df[[names(xvals)[d]]], prev_xval, xval, incbounds=F), df[[names(xvals)[d]]] - prev_xval, xval - prev_xval)
      
      temp <- temp * change
      
      return(temp)
    })
    ## give colnames based on index for clarity
    colnames(sub_matrix) <- paste0(names(xvals)[d], "_", 2:length(xvals[[d]]))
    return(sub_matrix)
  })
  Reduce(cbind, full_matrix)
}

################### INTERACT DELTA VARS #########################################
## delta vars need to be continuous but it's useful to interact a continuous var with a categorical (or other continuos) one
## this functions creates an A matrix with interacted vars
interact_delta_vars <- function(df, interact_pairs, drop_reference=T){
  df <- copy(df)
  invisible(
    intrxn_cols <- unlist(lapply(1:length(interact_pairs), function(x){
      
      ## detect classes of pairs
      pair <- interact_pairs[[x]]
      pair_classes <- lapply(df[, c(pair), with=F], class)
      message(" Printing detected classes for ", paste(pair, collapse=":"))
      print(pair_classes)
      
      char_col <- pair[pair_classes %in% c("factor", "character")]
      
      ## create interaction cols for character
      if(length(char_col)>1){stop(paste0(length(char_col), " vars detected as character, need one numeric col"))}
      if(length(char_col)>0){
        
        ## make binary ranef matrix of categorical
        Z_full <- make_ranef_matrix(df, form=as.formula(paste0("~(1|", char_col, ")")))
        ## drop first column (reference col)
        if(drop_reference==T){
          Z_full <- Z_full[, -1]
        }
        ## multiply each column by the continuous var
        Z_full <- Z_full*df[[setdiff(pair, char_col)]]
        
        colnames(Z_full) <- paste0(setdiff(pair, char_col), ":", colnames(Z_full))
        intrxn_col <- colnames(Z_full)
        Z_full <- as.data.table(Z_full)
        df[, (intrxn_col):=Z_full]
                
      } else {
        ## create interaction col if both cols are numeric/integer
        intrxn_col <- paste0(pair, collapse=":")
        df[, (intrxn_col):=get(pair[1]) * get(pair[2])]
      }
      return(intrxn_col)
    }))
  )
  return(list(intrxn_df=df, intrxn_cols=intrxn_cols))
}

################### CREATE BASE MATRIX #########################################
## make a baseic matrix with sparsity besides tri-diagonal
make_tridiag_matrix <- function(n_sites, rho=NULL){
  m0 <- matrix(0, ncol=n_sites, nrow=n_sites)
  m1 <- copy(m0)
  ## if a correlation is given, create precision matrix for a exponential decay
  if(!is.null(rho)){
    m0[col(m0)==row(m0)] <- 1+rho^2 ## get diagnoals
    m1[col(m1)==row(m1)-1 | col(m1)==row(m1)+1]<- -rho ## get off-diagonals
    m0<-as(m0, "dgTMatrix")
    
  } else {
    m0[col(m0)==row(m0)]<-1 ## get diagnoals
    m1[col(m1)==row(m1)-1 | col(m1)==row(m1)+1]<- 1 ## get off-diagonals
    m0<-as(m0, "dgTMatrix")
    m1<-as(m1, "dgTMatrix")
  }
  return(list(m0, m1))
}

################### SIMULATE MV NORMAL W/ CHOLESKY #########################################
rmvnorm_prec <- function(mu = NULL, prec, n.sims) {
  if(is.null(mu)) {
    mu <- rep(0, dim(prec)[1])
  }
  z <- matrix(MASS::mvrnorm(length(mu) * n.sims, 0, 1 ), ncol=n.sims)
  L <- Matrix::Cholesky(prec  , super = TRUE)
  z <- Matrix::solve(L, z, system = "Lt") ## z = Lt^-1 %*% z
  z <- Matrix::solve(L, z, system = "Pt") ## z = Pt    %*% z
  z <- as.matrix(z)
  outmat <- t(mu + z)
  colnames(outmat) <-  colnames(prec)
  return(outmat)
}

################### GET TMB PARAMETER DRAWS #########################################
simulate_tmb_draws <- function(model_object, n_draws=1000){
  require(mvtnorm)
  require(Matrix)
  require(TMB)
  
  ## run report to get joint precision
  report <- TMB::sdreport(model_object, getJointPrecision=T)
  
  ## estimates stored from best iteration
  ests <- model_object$env$last.par.best
  
  ##s if only 1 fixed effect parameter
  if(length(ests)==1){
    
    se <- as.matrix(sqrt(report$cov.fixed))
    par_draws <- rnorm(n=n_draws, mean=ests, sd=se)
    
    par_draws <- data.table(par_draws)
    names(par_draws) <- names(ests)
  } else {
    if(!is.null(model_object$env$random)){
      #cov_mat<-as.matrix(Matrix::solve(report$jointPrecision))  ## this will break if Hessian not pos/def
      par_draws <- rmvnorm_prec(mu=ests, prec=report$jointPrecision, n.sim=n_draws)
      
    } else {
      ## if only fixed effects
      cov_mat <- report$cov.fixed
      par_draws <- mvtnorm::rmvnorm(n=n_draws, mean=ests, sigma=cov_mat)
    }
  }
  
  return(as.data.table(par_draws))
}

################### CALCULATE PREDICTIONS FROM DRAWS #########################################
predict_draws <- function(prediction_math, draw_list, data_list=NULL,
                        return_draws=T, upper_lower=F, 
                        mid_fun = "mean", quants = c(0.025, 0.975)){
  
  ################### SUB FUNCTIONS #########################################
    calc_draw <- function(x, draw_list, temp_env, prediction_math){
    sub_draws <- lapply(draw_list,  "[", x, ) #function(n){ ## this syntax gets an entire row
    #   n[x]
    # })
    ##s add draw vals to temporary environemnt
    lapply(par_names, function(n){
      temp_env[[n]]<-as.numeric(sub_draws[[n]])
    })
    ## run the prediction formula
    pred <- eval(parse(text=prediction_math), envir=temp_env)
    
    rm(list = names(draw_list), envir = temp_env)
    
    return(data.table(draw=paste0("draw", x-1), pred_row=1:length(pred), pred=as.numeric(pred)))
  }
  
  ################### SETUP ENVIRONMENT #########################################
  ## get number of draws
  n_draws <- unique(unlist(lapply(draw_list, nrow)))
  
  ## check to make sure number of draws are correct
  if(length(n_draws)>1){stop("Varying number of draws supplied to elements of draw_list")}
  
  ## get names of objects
  par_names <- names(draw_list)
  data_names <- names(data_list)
  
  ## create a temporary environment to store values. Putting data here since data won't have draws (if data has draws, put it in draw_list)
  temp_env <- new.env()
  invisible(
    lapply(data_names, function(x){
      temp_env[[x]] <- data_list[[x]]
    })
  )
  
  ## convert draw list to matrix
  draw_list <- lapply(draw_list, as.matrix)
  
  ################### CALCULATE DRAWS #########################################
  message("Calculating draws...")
  
  linpred_draws <- lapply(1:n_draws, calc_draw, 
                        draw_list=draw_list, temp_env=temp_env, prediction_math=prediction_math) ## sub-args
  #message("Done with pred lapply")
  
  ################### FORMAT AND RETURN #########################################
  ## format draw output
  linpred_draws <- rbindlist(linpred_draws)
  setkey(linpred_draws, pred_row)
  
  if(return_draws==T){
    ## make wide on draws if returning draws
    preds <- dcast(linpred_draws, formula = pred_row ~ draw, value.var = "pred")
  } else {
    mid_txt <- paste0(mid_fun,"(pred)")
    if(upper_lower==T){
      preds <- linpred_draws[,  .(pred=eval(parse(text = mid_txt)), lower=quantile(pred, probs=quants[1]), upper=quantile(pred, probs=quants[2])), by="pred_row"]
      
    } else {
      ## calculate mean and SD of draws if not returing draws and want SE
      preds <- linpred_draws[,  .(pred=eval(parse(text = mid_txt)), se=sd(pred)), by="pred_row"]
    }
  }
  preds[, pred_row:=NULL]
  message("Done")
  return(preds)
}

#calc_draw(x=8, draw_list=draw_list, temp_env=temp_env, prediction_math=prediction_math)

################### SCALE VARIABLES #########################################
## get scaling factors first
get_scalers <- function(fixef, dt){
  ## first check for missing vars
  check_missing(fixef, dt, warn=T)
  
  m <- mean(dt[[fixef]], na.rm=T)
  s <- sd(dt[[fixef]], na.rm=T)
  out <- c(mean=m, sd=s)
  return(out)
}

scale_fixefs <- function(dt, fixefs, scalers=NULL){
  dt <- copy(dt)
  dt <- as.data.table(dt)
  
  scale_factors <- lapply(fixefs, get_scalers, dt)
  names(scale_factors) <- fixefs
  
  # scale_var <- function(fixef, scale_mean, scale_sd, dt){
  #   dt[, (paste0("scaled_", x)):=(get(x) - scale_mean)/scale_sd]
  # }
  #scale_var <- function()
  # 
  # ## scale
  # lapply(fixefs, scale_var, scale_mean=scale_factors[[]])
  # 
  dt[, (paste0("scaled_", fixefs)):=lapply(fixefs, function(x){scale(get(x))})]
  
  return(list(
    scale_factors=scale_factors,
    dt=dt
  ))
}

################### APPLY SCALERS #########################################
apply_scalers <- function(dt, scalers){
  dt <- copy(dt)
  for(s in 1:length(scalers)){
    var <- names(scalers[s])
    temp_scale <- scalers[[s]]
    if(var %in% names(dt)){
      message("Rescaling ", var)
      
      dt[, (paste0("scaled_", var)):=(get(var)-temp_scale[1])/temp_scale[2]]
      
    }
  }
  return(dt)
}


################### COMPILE TMB MODEL #########################################
compile_tmb <- function(x){
  require(TMB)
  require(data.table)
  
  folders <- unlist(tstrsplit(x, "/"))
  
  ## get parent folder that holds model, and model name
  parent_folder <- paste0(paste0(folders[-length(folders)], collapse="/"), "/")
  model_file <- folders[length(folders)]
  model_name <- unlist(tstrsplit(model_file, ".cpp"))[1]
  
  ## get current wd and reset after compiling tmb model to avoid issues
  orig_wd <- getwd() 
  setwd(parent_folder)
  TMB::compile(model_file)
  dyn.load(dynlib(model_name))
  
  ## re-set WD 
  setwd(orig_wd)
}
