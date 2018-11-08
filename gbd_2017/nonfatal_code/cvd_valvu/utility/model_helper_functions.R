####################
##Purpose: This script holds useful modelling formulas
# source("FILEPATH/utility/model_helper_functions.R")
# NOTE: thorough documentation not currently here, sorry. these functions are relatively straightforward
########################


require(data.table)
require(lme4)



################### MAKE R-SYNTAX FORMULA #########################################
################################################################

## make formula with R ssyntax
make_formula<-function(response, fixefs=NULL, ranefs=NULL){
  ## if no fixed effects
  if(length(fixefs)==0){
    form<-paste0(paste0(response, "~"), paste(paste0("(1|", ranefs, ")"), collapse="+"))
    
    ## if no fixed effects and no random effects
    if(length(ranefs)==0){
      form<-paste0(paste0(response, "~1"))
    }
  }else{
    ## if no random effects
    if(length(ranefs)==0){
      form<-paste0(paste0(response, "~"), paste(fixefs, collapse="+"), collapse="+")
      
    }else{
      ## if no fixed effects and no random effects
      form<-paste0(paste0(response, "~"), 
                   paste(c(paste(fixefs, collapse="+"), paste(paste0("(1|", ranefs, ")"), collapse="+")), collapse="+"))
    }
  }
  
  ## save formula as a string, will need to convert w/ as.formula()
  string_form<-gsub(" ", "", form)
  #form<-as.formula(form)
  message("Model formula:")
  message(" ", string_form)
  return(as.formula(string_form))
}


################### MAKE FIXED EFFECTS MATRIX #########################################
################################################################

make_fixef_matrix<-function(df, fixefs=NULL){
  df<-copy(df)
  df[, temp_response:=1]
  
  if(length(fixefs)>0){
    fix_form<-paste0("temp_response~0+", paste(fixefs, collapse="+"))
    setcolorder(df, fixefs)
    X<-as.matrix(model.matrix(formula(fix_form), data=df))
  }else{
    #X<-as.matrix(rep(0, times=nrow(df)), ncol=1)
    X<-array(0, dim = c(nrow=nrow(df), 0))
  }
  return(X)
}

################### MAKE RANDOM EFFECTS MATRIX #########################################
################################################################


## create random effects matrix
  ## training matrix to give random effects in case you're trying to make a prediction matrix 
make_ranef_matrix<-function(df, form, training_matrix=NULL){
  require(lme4) ## this gets the mkReTrms function
  df<-copy(df)
  
  ## find random effects
  ranefs<-unlist(as.character(lapply(findbars(form), "[[", 3)))

  if(length(ranefs)>0){
    ## format values of ranef columns to keep track of them easily
    invisible(
      lapply(ranefs, function(x){
        df[, (x):=paste0(x,";value:", get(x))]
      })
    )
    
    if(is.null(training_matrix)){
      Z<-as.matrix(t(mkReTrms(findbars(as.formula(form)), model.frame(subbars(form), data=df))[["Zt"]])) ## model matrix for random effects
    }else{
      
      df<-df[, ranefs, with=F]
      
      message("Constructing prediction matrix..")
      ## lapply to evaluate where the training matrix 
      Z<-sapply(1:ncol(training_matrix), function(x){
        ranef<-tstrsplit(colnames(training_matrix)[x], ";value:")[[1]]
        as.numeric(df[[ranef]]==colnames(training_matrix)[x])
      })
      colnames(Z)<-colnames(training_matrix)
      message("Done")
    }
  }else{
    Z<-array(0, dim = c(nrow=nrow(df), 0))
  }

  return(Z)
}





################### GET NUMBER OF RANDOM EFFECTS IN EACH LEVEL #########################################
#######################################################################

get_ranef_lvl_counts<-function(df, form){
  
  ## find random effects
  ranefs<-unlist(as.character(lapply(findbars(form), "[[", 3)))
  
  if(length(ranefs)>0){
    n_s<-unlist(lapply(ranefs, function(x){
      length(unique(df[[x]]))
    }))
  }else{
    n_s<-array(0, dim=0)
  }

  
  return(n_s)
}

################### CREATE MATRiX FOR DELTA SMOOTHING #########################################
#######################################################################

## xvals must be a named list. Each element needs to be a numeric vector of the values to estimate differences between for a given dimension
## df must be a data.table of data with column names corresponding to the names of the elements in the xvals list

make_delta_matrix<-function(xvals, df){
  ## xvals needs to be a named list.
  
  ## checks
  if(is.null(names(xvals))){stop("xvals must be a named list")}
  if(!all(names(xvals) %in% names(df))){stop("Names of xvals not in data")}
  
  ## loop over each dimension
  full_matrix<-lapply(1:length(xvals), function(d){
    ## loop over each value
    sub_matrix<-sapply(2:length(xvals[[d]]), function(x){
      
      xval<-xvals[[d]][x]
      prev_xval<-xvals[[d]][x-1]
      
      ## stop if any values are less than the initial values
      if(x==2){
        if(any(df[[names(xvals)[d]]] < prev_xval)){stop(message("You have values in ", names(xvals)[d], " that are less than the minimum mesh point!"))}
      }
      
      ## get binaries if row values are greater than or equal to the xval
      temp<-as.numeric(df[[names(xvals)[d]]] >= xval)
      ## get change from previous xval or change from the value in the data and the xval if the value in the data is between previous xval
      change<-ifelse(data.table::between(df[[names(xvals)[d]]], prev_xval, xval, incbounds=F), xval - df[[names(xvals)[d]]], xval - prev_xval)
      
      temp<-temp * change
      
      return(temp)
    })
    ## give colnames based on index for clarity
    colnames(sub_matrix)<-paste0(names(xvals)[d], "_", 2:length(xvals[[d]]))
    return(sub_matrix)
  })
  Reduce(cbind, full_matrix)
}



################### CREATE BASE MATRIX #########################################
#######################################################################

## make a baseic matrix with sparsity besides tri-diagonal
make_tridiag_matrix<-function(n_sites, rho=NULL){
  m0<-matrix(0, ncol=n_sites, nrow=n_sites)
  m1<-copy(m0)
  ## if a correlation is given, create precision matrix for a exponential decay
  if(!is.null(rho)){
    m0[col(m0)==row(m0)]<-1+rho^2 ## get diagnoals
    m1[col(m1)==row(m1)-1 | col(m1)==row(m1)+1]<- -rho ## get off-diagonals
    m0<-as(m0, "dgTMatrix")
    
  }else{
    m0[col(m0)==row(m0)]<-1 ## get diagnoals
    m1[col(m1)==row(m1)-1 | col(m1)==row(m1)+1]<- 1 ## get off-diagonals
    m0<-as(m0, "dgTMatrix")
    m1<-as(m1, "dgTMatrix")
  }
  return(list(m0, m1))
}

################### GET TMB PARAMETER DRAWS #########################################
################################################################

simulate_tmb_draws<-function(model_object, model_report, n_draws=1000){
  require(mvtnorm)
  
  ests<-model_object$env$last.par.best
  cov_mat<-as.matrix(solve(model_report$jointPrecision))  ## this will break if Hessian not pos/def
  
  par_draws<-rmvnorm(n=n_draws, mean=ests, sigma=cov_mat)
  return(as.data.table(par_draws))
}


################### CALCULATE PREDICTIONS FROM DRAWS #########################################
################################################################

predict_draws<-function(prediction_math, draw_list, data_list,
                        return_draws=T, upper_lower=F){
  
  ################### SUB FUNCTIONS #########################################
  ################################################################
  
  calc_draw<-function(x, draw_list, temp_env, prediction_math){
    sub_draws<-lapply(draw_list, function(n){
      n[x]
    })
    ## add draw vals to temporary environemnt
    lapply(par_names, function(n){
      temp_env[[n]]<-as.numeric(sub_draws[[n]])
    })
    ## run the prediction formula
    pred<-eval(parse(text=prediction_math), envir=temp_env)
    
    rm(list = names(draw_list), envir = temp_env)
    
    return(data.table(draw=paste0("draw", x-1), pred_row=1:length(pred), pred=as.numeric(pred)))
  }
  
  ################### SETUP ENVIRONMENT #########################################
  ################################################################
  ## get number of draws
  n_draws<-unique(unlist(lapply(draw_list, nrow)))
  
  ##check to make sure number of draws are correct
  if(length(n_draws)>1){stop("Varying number of draws supplied to elements of draw_list")}
  
  ## get names of objects
  par_names<-names(draw_list)
  data_names<-names(data_list)
  
  ## create a temporary environment to store values. Putting data here since data won't have draws (if data has draws, put it in draw_list)
  temp_env<-new.env()
  lapply(data_names, function(x){
    temp_env[[x]]<-data_list[[x]]
  })
  
  ################### CALCULATE DRAWS #########################################
  ################################################################
  
  message("Calculating draws...")
  
  linpred_draws<-lapply(1:n_draws, calc_draw, 
                        draw_list=draw_list, temp_env=temp_env, prediction_math=prediction_math) ## sub-args

  ################### FORMAT AND RETURN #########################################
  ################################################################
  
  ## format draw output
  linpred_draws<-rbindlist(linpred_draws)
  setkey(linpred_draws, pred_row)
  
  if(return_draws==T){
    ## make wide on draws if returning draws
    preds<-dcast(linpred_draws, formula = pred_row ~ draw, value.var = "pred")
  }else{
    if(upper_lower==T){
      preds<-linpred_draws[,  .(pred=mean(pred), lower=quantile(pred, probs=0.025), upper=quantile(pred, probs=0.975)), by="pred_row"]
      
    }else{
      ##calculate mean and SD of draws if not returing draws and want SE
      preds<-linpred_draws[,  .(pred=mean(pred), se=sd(pred)), by="pred_row"]
    }
  }
  preds[, pred_row:=NULL]
  message("Done")
  return(preds)
}

#calc_draw(x=8, draw_list=draw_list, temp_env=temp_env, prediction_math=prediction_math)

