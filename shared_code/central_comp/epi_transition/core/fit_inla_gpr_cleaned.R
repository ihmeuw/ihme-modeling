## ##################
## setup R session ##
## ##################

rm(list=ls())
if (Sys.info()[1] == "Linux"){
  j <- "FILEPATH"
  h <- "FILEPATH"
} else if (Sys.info()[1] == "Darwin"){
  j <- "FILEPATH"
  h <- "FILEPATH"
}

currentDir <- function() {
                                        # Identify program directory
  if (!interactive()) {
    cmdArgs <- commandArgs(trailingOnly = FALSE)
    match <- grep("--file=", cmdArgs)
    if (length(match) > 0) fil <- normalizePath(gsub("--file=", "", cmdArgs[match]))
    else fil <- normalizePath(sys.frames()[[1]]$ofile)
    dir <- dirname(fil)
  } else dir <- "FILEPATH" 
  return(dir)
}


source(paste0(currentDir(), "/primer.R"))


## ############################################
## PARSE ARGUMENTS ############################
## ############################################

parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Site where data will be stored",
                    dUSERt = "FILEPATH", type = "character")
parser$add_argument("--etmtid", help = "Model type ID",
                    dUSERt = 6, type = "integer")
parser$add_argument("--etmvid", help = "Model version ID",
                    dUSERt = 8, type = "integer")
parser$add_argument("--mvid", help = "Model version ID (of input)",
                    dUSERt = 197, type = "integer")
parser$add_argument("--gbdrid", help = "GBD round ID",
                    dUSERt = 4, type = "integer")
parser$add_argument("--lsid", help = "Location set ID",
                    dUSERt = 35, type = "integer")
parser$add_argument("--yids", help = "Year IDs",
                    dUSERt = c(seq(1990, 2010, 5), 2016), nargs = "+", type = "integer")
parser$add_argument("--agid", help = "Age group ID",
                    dUSERt = 10, type = "integer")
parser$add_argument("--sid", help = "Sex ID",
                    dUSERt = 2, type = "integer")
parser$add_argument("--gbdid", help = "GBD ID (cause/risk/etc)",
                    dUSERt = 296, type = "integer")
parser$add_argument("--usintercept", help = "Whether or not to include intercept for US States",
                    dUSERt = 0, type = "integer")
parser$add_argument("--usnat", help = "Whether or not to use US national rather than states",
                    dUSERt = 1, type = "integer")
parser$add_argument("--meth", help = "Type of prior used by INLA to fit GPR",
                    dUSERt = 11, type = "integer")
parser$add_argument("--nbasis", help = "Number of Internal mesh points to create, will fit nbasis + 2 then drop terminal", 
                    dUSERt = 2, type = "integer")
parser$add_argument("--meshrange", help = "Start and stop of mesh points",
                    dUSERt = c(.3, .7), nargs = "+", type = "double")
parser$add_argument("--prioronly", help = "Use only prior rather than add in GPR",
                    dUSERt = 0, type = "integer")
args <- parser$parse_args()


list2env(args, environment()); rm(args)

usintercept <- as.logical(usintercept)
usnat <- as.logical(usnat)
prioronly <- as.logical(prioronly)


## ############################################
## write a function for the gpr fit in INLA  #############################################
## ############################################

require(INLA, lib.loc = "FILEPATH")
require(splines)
require(RColorBrewer)
require(plyr)

## FUNCTION THAT CALCULATES PIECEWISE FIT BASED ON GIVEN PARAMETERS
calc.pw <- function(x, k1, k2, a, b, c, d, e, f, methid) {

    if (methid == 5) {    
        
        if(x <= k1) {
            
            res <- a + b*x
            
        } else if (x > k1 & x <=k2) {
            
            
            res <- c + d*x
            
        } else if (x > k2) {
            
            
            res <- e + f*x
        }
    } else {
        
        ## USING A DYNAMIC SINGLE KNOT PIECEWISE
        
        if (x <= k1) {
            
            res <- a + b*x
            
        } else {
            
            res <- c + d*x
            
        }
        
    }
    return(res)
    
}



calc.pw.ssr <- function(params, x, y, methid) {
    
    k1 <- rep(params['k1'], length(x))
    k2 <- rep(params['k2'], length(x))
    as <- rep(params['a'], length(x))
    bs <- rep(params['b'], length(x))
    ds <- rep(params['d'], length(x))
    fs <- rep(params['f'], length(x))
    
    cs <- as + (bs-ds)*k1
    es <- cs + (ds-fs)*k2
    
    result <- mapply(calc.pw, x = x, k1 = k1, k2 = k2, a = as, b = bs, c = cs, d = ds, e = es, f = fs, methid = methid)
    
    ssr <- sum((y - result)^2)
    
    return(ssr)
    
}


nlminb.pw <- function(x, y, methid) {
    
    if (methid == 5) {
    
        k1min <- .3
        k1max <- .5
        
    } else {
        
        k1min <- .25
        k1max <- .75
    }
    param.pw <- nlminb(start = c(k1 = .4, k2 = .6, a = 0,b = 0,d = 0,f = 0),
                       objective = calc.pw.ssr,
                       x = x, y = y, methid = methid,
                       lower = c(k1min, .501, -Inf, -Inf, -Inf, -Inf),
                       upper = c(k1max, .7, Inf, Inf, Inf, Inf),
                       control = list(rel.tol = 1e-5))
    
    return(param.pw$par)
}

fit.lm <- function(df,
                   method,
                   ...) {
    
    message("Fitting First-stage Linear model")
    
    x  <- df$sdi
    y  <- df$val
    var <- df$var

    
    
    if(method %in% c("piecewise", "okpw")) {
        
        ## FIND OPTIMAL PIECEWISE PARAMETERS
        start <- Sys.time()
        pwparams <- nlminb.pw(x,y, meth)
        print(Sys.time()-start)
        
        ## CREATE DESIGN MATRIX
        d <- data.table(x = x,
                        k1 = rep(pwparams['k1'], length(x)),
                        k2 = rep(pwparams['k2'], length(x)),
                        a = rep(pwparams['a'], length(x)),
                        b = rep(pwparams['b'], length(x)),
                        d = rep(pwparams['d'], length(x)),
                        f = rep(pwparams['f'], length(x)),
                        var = var,
                        y = y)
        
        d[, c:= a + (b-d)*k1][, e := c + (d-f)*k2]
        
        preds <- mapply(calc.pw, x = d$x, k1 = d$k1, k2 = d$k2,
                        a = d$a, b = d$b, c = d$c, d = d$d, e = d$e, f = d$f,
                        methid = meth)
        
        d[,resid := y-preds]
        
        ## CREATE PRED DESIGN MATRIX
        predd <- data.table(expand.grid(x = round(seq(0,1,.005), 3),
                                    k1 = pwparams['k1'],
                                    k2 = pwparams['k2'],
                                    a = pwparams['a'],
                                    b = pwparams['b'],
                                    d = pwparams['d'],
                                    f = pwparams['f']))
        
        predd[, c:= a + (b-d)*k1][, e := c + (d-f)*k2]
        
        pwpred <- mapply(calc.pw, x = predd$x, k1 = predd$k1, k2 = predd$k2,
                       a = predd$a, b = predd$b, c = predd$c, d = predd$d, e = predd$e, f = predd$f,
                       methid = meth)
        
        predd[, pred := pwpred]
        
        
    } else {
        
        formula <- switch(method,
                          linear = "y ~ -1 + int + x",
                          quad   = "y ~ -1 + int + x + x2",
                          cubic = "y ~ -1 + int + x + x2 + x3",
                          okspline = "y ~ -1 + int + bs(x, knots = .75, degree = 1, Boundary.knots = c(0,1))",
                          ok2spline = "y ~ -1 + int + bs(x, knots = .3, degree = 1, Boundary.knots = c(0,1))",
                          ok4spline = "y ~ - 1 + int + bs(x, knots = .2, degree = 1, Boundary.knots = c(0,1))",
                          ok3spline = "y ~ -1 + int + bs(x, knots = .5, degree = 1, Boundary.knots = c(0,1))",
                          ok5spline = "y ~ -1 + int + bs(x, knots = .25, degree = 1, Boundary.knots = c(0,1))",
                          ok6spline = "y ~ -1 + int + bs(x, knots = .35, degree = 1, Boundary.knots = c(0,1))"
                          )
        
        print(formula)
        
        d <-  data.table(int = rep(1, length(x)),  ## make a design matrix with intercept and x(sdi)
                         x   = x,
                         x2  = x ^ 2,
                         x3 = x ^ 3,
                         y,
                         var)
        
        
        fit <- lm(as.formula(formula), data = d)
        
        d[, resid := resid(fit)]
        
        ## Predict over range of SDI so can add back the residuals
        sdirange <- seq(0,1,.005)
        predd <- data.table(int = rep(1, length(sdirange)),
                            x = round(sdirange, 3),
                            x2 = sdirange^2,
                            x3 = sdirange^3)
        
        predd[, pred := predict(fit, newdata = predd)]
        
        
    }
    

    
    return(list(resids = d[, .(sdi = x, val = resid, var)], preds = predd[, .(sdi = x, lmpred = pred)]))
    
    
    
}

fit.gpr <- function(df, ## needs to have a column named val (response)
                    ## and column named  var (var across response)
                    ## and column named sdi (explanatory)
                    n.basis = 5, 
                    x.out = seq(0, 1, by = 0.005), ## values to predict on
                    pcprior = FALSE, ## can set to true on geos nodes
                    pc.prior.vals = c(0.05, 8), ## range (how correlated heights of bases are over SDI) and amplitude (variability in mean function) parameters that go into the basis functions
                    pvn = NA, ## Mean of variance (amplitude)
                    prn = NA, ## Mean of range (scale)
                    plot.internal = FALSE, ## make plot within function call?
                    verbose.INLA = TRUE,
                    spatial.param.prec = 1e2,
                    method = "direct",## Options are
                    us.int = TRUE, ## include intercept for US counties?
                    ...){

  message("Setting up for GPR fit in INLA")
  ## Sets up the input with the method specifying what it fed into GPR. This will
  ## then be used on the back end after the GPR step to get final fitted values
  ## In particular: estimate @ x.out = yhat.out + gpmod$mean
    
  x  <- df$sdi
  y  <- df$val
  w  <- 1 / df$var


  ## set smoothness
  alpha = degree = 2

  ## set some dUSERt priors
  sigma0=sd(y)
  rho0 = 0.25*(max(x) - min(x))

  ## make a mesh and set spde priors (using the data...)
 
  if (etmtid == 1) {
      
      meshseq <- seq(0, 1, length.out = n.basis + 2)
      meshseq <- meshseq[seq(2, length(meshseq)-1, 1)]
      
  }
  
  if (etmtid %in% c(3,4,5,6)) meshseq <- seq(meshrange[1], meshrange[2], length.out = n.basis)
  
  mesh <- inla.mesh.1d(meshseq, degree = degree, interval = c(0,1))
  
  nu <-  alpha - 1/2
  kappa0 <- sqrt(8 * nu)/rho0
  tau0 <- 1 / (4 * kappa0^3 * sigma0^2)^0.5

  if(!pcprior){

    spde <- inla.spde2.matern(mesh, alpha=alpha, constr = FALSE,
                              prior.variance.nominal = pvn,
                              prior.range.nominal = prn, 
                              theta.prior.prec = spatial.param.prec)

  }else{ 
    spde <-  inla.spde2.pcmatern(mesh,alpha=alpha,prior.range=c(pc.prior.vals[1],0.1),prior.sigma=c(pc.prior.vals[2],0.1))
  }

  ## setup INLA projector matrices, indices, data, ...
  A <-  inla.spde.make.A(mesh, loc=x)
  Ap <- inla.spde.make.A(mesh, loc=x.out)
  index <-  inla.spde.make.index("sinc", n.spde = spde$n.spde)
  design.matrix <- data.frame(int = rep(1, length(x)),  ## make a design matrix with intercept and x(sdi)
                              x   = x,
                              x2  = x ^ 2,
                              x3 = x ^ 3)
  print(design.matrix)
  
  pred.design.matrix <- data.frame(int = rep(1, length(x.out)), ## make a design matrix with intercept and x=(sdi)
                                   x   = x.out,
                                   x2  = x.out ^ 2,
                                   x3 = x.out ^ 3)
  print(pred.design.matrix)
  
  st.est <- inla.stack(data=list(y=y), A=list(A, 1),  effects=list(index, design.matrix),  tag="est")
  st.pred <- inla.stack(data=list(y=NA), A=list(Ap, 1),  effects=list(index, pred.design.matrix),  tag="pred")
  sestpred <- inla.stack(st.est,st.pred)

  ## pick the formula
  formula <- switch(method,
                    direct = "y ~ -1 + f(sinc, model=spde)",
                    linear = "y ~ -1 + int + x + f(sinc, model=spde)",
                    quad   = "y ~ -1 + int + x + x2 + f(sinc, model=spde)",
                cubic = "y ~ -1 + int + x + x2 + x3 + f(sinc, model=spde)")
  
  ## if we want to include us intercept, add it on
  if(us.int){
    print("Using US Dummy")
    formula <- paste(formula, "us", sep = " + ")
  }
  
  formula <- as.formula(formula)
  
  print(formula)

  data <-  inla.stack.data(sestpred)

  ## setup INLA to run with weights
  inla.setOption("enable.inla.argument.weights", TRUE)

  INLA:::inla.dynload.workaround()

  ## fit inla
  message("Fitting GPR in INLA")
  
  if (etmtid %in% c(3,4,5, 6)) {
      
      d <- cbind(design.matrix, y)
      
      if(meth %in% c(2,6,7,8, 9,11, 12)) {
          
          
          mod <- lm(y~-1 + int + x, data = d)
          print(coef(mod))
          
          result <- inla(formula,
                          data=data,
                          family="normal",
                          control.predictor= list(A=inla.stack.A(sestpred), compute=TRUE),
                          control.compute = list(config = TRUE),
                          weights = c(w, rep(1, length(x.out))),
                          verbose = verbose.INLA,#,
                          control.fixed = list(prec.intercept = 1e5,
                                               mean = list(int = coef(mod)["int"], x = coef(mod)["x"]),
                                               prec = 10))
          
          
      } else if (meth == 3) {
          
          mod <- lm(y~ -1 + int + x + x2, data = d)
          print(coef(mod))
          
          result <-  inla(formula,
                          data=data,
                          family="normal",
                          control.predictor= list(A=inla.stack.A(sestpred), compute=TRUE),
                          control.compute = list(config = TRUE),
                          weights = c(w, rep(1, length(x.out))),
                          verbose = verbose.INLA,#,
                          control.fixed = list(prec.intercept = 1e5,
                                               mean = list(int = coef(mod)["int"], x = coef(mod)["x"], x2 = coef(mod)["x2"]),
                                               prec = 10))
      } else if (meth == 4) {
          
          mod <- lm(y~ -1 + int + x + x2 + x3, data = d)
          print(coef(mod))
          
          result <-  inla(formula,
                          data=data,
                          family="normal",
                          control.predictor= list(A=inla.stack.A(sestpred), compute=TRUE),
                          control.compute = list(config = TRUE),
                          weights = c(w, rep(1, length(x.out))),
                          verbose = verbose.INLA,#,
                          control.fixed = list(prec.intercept = 1e5,
                                               mean = list(int = coef(mod)["int"], x = coef(mod)["x"], x2 = coef(mod)["x2"], x3 = coef(mod["x3"])),
                                               prec = 10))
          
      } else if (meth %in% c(5, 10)) {
          
          result <-  inla(formula,
                          data=data,
                          family="normal",
                          control.predictor= list(A=inla.stack.A(sestpred), compute=TRUE),
                          control.compute = list(config = TRUE),
                          weights = c(w, rep(1, length(x.out))),
                          verbose = verbose.INLA,
                          control.fixed = list(prec = 10))
          
      }
      
      
      
  } else {
      
      result <-  inla(formula,
                      data=data,
                      family="normal",
                      control.predictor= list(A=inla.stack.A(sestpred), compute=TRUE),
                      control.compute = list(config = TRUE),
                      weights = c(w, rep(1, length(x.out))),
                      verbose = verbose.INLA,#,
                      control.fixed = list(prec = 10))
      
  }
  


  ## make predictions of mean fit
  message("Making posterior predictive fit")
  ii <- inla.stack.index(sestpred, tag='pred')$data
  gpmod = list(x.out=x.out,
               mean=result$summary.fitted.values$mean[ii],
               lcb=result$summary.fitted.values$"0.025quant"[ii],
               ucb=result$summary.fitted.values$"0.975quant"[ii],
               inlaobj=result,
               mesh = mesh)
  
  print(meshseq)

  ## plot
  if(plot.internal){
    message("Plotting")
    pdf("FILEPATH", width = 10, height = 10)
    plot(val ~ sdi, df, xlab="SDI",ylab="Logit MR")
    lines(gpmod$x.out, gpmod$mean, col = "red")
    lines(gpmod$x.out, gpmod$lcb, lty=2, col = "red")
    lines(gpmod$x.out, gpmod$ucb, lty=2, col = "red")
    dev.off()
  }

  return(gpmod)
}


## ######################################################################
## Get and prep data ####################################################
## ######################################################################

load.prepped.data <- function(data_dir, etmtid, etmvid, mvid, lsid, yids, agid, sid, gbdid, gbdrid, usnat, sdidf, locsdf) { 
    
    ## Whether or not including US national in lieu of states
    
    if (usnat == T) {
        
        locsdf <- locsdf[(level == 3 & !ihme_loc_id %like% "IND|CHN|BRA")| (level == 4 & ihme_loc_id %like% "IND")|
                             (most_detailed == 1 & ihme_loc_id %like% "CHN|BRA")]
        print("Using US National")
    } else {
        
        locsdf <- locsdf[(level == 3 & !ihme_loc_id %like% "IND|CHN|USA|BRA")| (level == 4 & ihme_loc_id %like% "IND")|
                             (most_detailed == 1 & ihme_loc_id %like% "CHN|USA|BRA")]
        print("Using US States")
    }
    
    if (etmtid == 1) {
        
        dfs <- rbindlist(et.getDraws(data_dir,
                                     etmtid, etmvid, mvid, lsid,
                                     lids = locsdf$location_id, yids, agids = agid, sids = sid,
                                     gbdids = gbdid,
                                     gbdrid,
                                     popsdf))
        
        ## Transformation
        
        dfs <- dfs[metric_id == 3,  val := log(val + 1e-8)]
        ## For rates
        
        dfs <- dfs[metric_id == 2 & val == 0, val := 1e-4][metric_id == 2 & val >= (1 - 1e-4), val := 1 - 1e-4][metric_id == 2, val := log(val / (1 - val))]
        ## For proportions
        
        ## Take mean and var of transformed draws for appropriate inputs to prior
        df <- dfs[, .(val = mean(val),
                      var = var(val)), by = list(location_id, year_id, age_group_id, sex_id, measure_id, metric_id)]
        
        ## if variance is zero, inflate it a tiny bit
        df$var[which(df$var == 0)] <- 1e-10
        
        
    } else if (etmtid %in% c(3, 4, 5, 6)) {
        
        df <- {}
        
        try(df <- et.getOutputs(data_dir, 
                           etmtid, etmvid, mvid, lsid,
                           lids = locsdf$location_id, yids, agids = agid, sids = sid,
                           gbdids = gbdid, 
                           gbdrid))
        
        if(is.null(df)) saveRDS(df, file = "FILEPATH")
        
        ## SHOULD CHECK IF ALL ZERO OR ALL NA
        if(!is.null(df)) {
            
            if(all(df$val == 0, na.rm = T)|all(is.na(df$val))) df <- NULL
            
        }
        
        if (!is.null(df)) {
            

            df[is.na(val), c("val", "upper", "lower") := 0]
            
            df <- df[val >= 0]
            
            ## COMPUTE VARIANCE FROM UI BOUNDS - IF NONE (POP STRUCTURE) SET ARBITRARY
            df[, var := (((upper-lower)/2)/1.96)^2]
            if(etmtid %in% c(3,4)) df[, var := 1]
            
            df[metric_id == 3, var := var*((1/(val + 1e-8))^2)][metric_id == 3, val := log(val + 1e-8)]
            
            
            
            ### If variance is still zero after tranformation set arbitrary
            df[var == 0, var := 1]
            
            df <- df[metric_id == 2 & val == 0, val := 1e-4][metric_id == 2 & val >= (1 - 1e-4), val := 1 - 1e-4][metric_id == 2, val := log(val / (1 - val))]
            
            
            df[, c("upper", "lower") := NULL]
            
        }
        
    }

    if (!is.null(df)) {
        
        stopifnot(all(!is.na(df$val)))
        
        ## Attach SDI covariate
        df <- merge(df,
                    sdidf[, c("location_id", "year_id", "sdi"), with = FALSE],
                    by = c("location_id", "year_id"),
                    all.x = TRUE)
        

        ## identify US states
        uslocs <- locsdf[ihme_loc_id %like% "USA", location_id]
        
        df[, us := (location_id %in% uslocs)]
        
        
    }

    
    return(df)
}

## ################################################################################
## get draws ######################################################################
## ################################################################################

## here's a function to take draws from GPR fit in INLA
get.draws <- function(inla.fit,
                      x.out = seq(0, 1, by = 0.005),
                      n.draws = 10000,
                      mesh,
                      etmt_id = NA,
                      etmv_id = NA,
                      ag_id = NA,
                      se_id = NA,
                      meas = NA,
                      metric_id = NA,
                      cause = NA,
                      format = "wide" ## could also be long
                      ){

  ## take draws from the inla fit
  draws <- inla.posterior.sample(n.draws, inla.fit)

  ## get parameter names
  par.names <- rownames(draws[[1]]$latent)
  gpr.ind <- grep('^sinc*', par.names) ## get entries in INLA fit correspoding to GPR
  lin.ind <- c(grep('^int', par.names),
               grep('^x', par.names)) ## get entries in INLA fir corresponding to mean


  ## make the cov matrix (if needed)
  if(length(lin.ind) == 0){
    cov.mat <- NULL
  }
  if(length(lin.ind) == 2){
    ## make the covariates:
    cov.mat <- data.frame(int = rep(1, length(x.out)),
                          x = x.out)
  }
  if(length(lin.ind) == 3){
    ## make the covariates:
    cov.mat <- data.frame(int = rep(1, length(x.out)),
                          x   = x.out,
                          x2  = x.out ^ 2)
  }
  if(length(lin.ind) == 4){
    ## make the covariates:
    cov.mat <- data.frame(int = rep(1, length(x.out)),
                          x = x.out,
                          x2 = x.out ^ 2,
                          x3 = x.out ^ 3)
  }


  ## get samples as amtrices
  pred.gpr <- sapply(draws, function (x)
    x$latent[gpr.ind])

  ## make the projector matrix
  A.pred <- inla.spde.make.A(mesh = mesh,
                             loc = x.out)

  ## make a matrix of GPR draws
  gpr.draws <- as.matrix(A.pred %*% pred.gpr)

  ## make draw of 'prior'
  if(!is.null(cov.mat)){
    pred.lin <- sapply(draws, function (x)
      x$latent[lin.ind])
    lin.draws <- as.matrix(cov.mat) %*% pred.lin
  }

  ## make full draws
  if(!is.null(cov.mat)){
    draws <- lin.draws + gpr.draws
  }else{
    draws <- gpr.draws
  }
  colnames(draws) <- paste0("draw", 0:(n.draws - 1))
  draws <- cbind(x.out, draws)
  colnames(draws)[1] <- "sdi"

  ## convert from wide to long
  if(format == "long"){
    pred <- as.vector(t(draws[, -1])) ## first n.draws should all be with lowest sdi, next n.draws with next lower sdi, ...
    n <- length(pred)
    draws <- data.table(etmodel_type_id = rep(etmt_id, n),
                        etmodel_version_id = rep(etmv_id, n),
                        sdi = sort(rep(draws[, 1], n.draws)),
                        age_group_id = rep(ag_id, n),
                        sex_id = rep(se_id, n),
                        measure_id = rep(meas, n),
                        metric_id = rep(metric_id, n),
                        cause_id = gbdid,
                        draw = sort(rep(1:100, 100)),
                        sim = rep(1:100, 100),
                        gprpred = pred)
    
    draws[, sdi := round(sdi,3)]
  }
  return(draws)
}


## ################################################################################
## parallelized full run ##########################################################
## ################################################################################

Methods <- c("direct",
             "linear",
             "quad",
             "cubic",
             "piecewise",
             "okspline",
             "ok2spline",
             "ok4spline",
             "ok3spline",
             "okpw",
             "ok5spline",
             "ok6spline")
failed.runs <- character()
num.failed  <- 0

print(Methods[meth])

locsdf <- readRDS("FILEPATH")

## Load SDI
sdidf <- readRDS("FILEPATH")
sdidf <- sdidf[location_id %in% locsdf$location_id]

    ## get the data
    tmp.df <- try(load.prepped.data(data_dir, 
                                etmtid, 
                                etmvid, 
                                mvid, 
                                lsid, 
                                yids, 
                                agid, 
                                sid, 
                                gbdid, 
                                gbdrid, 
                                usnat, sdidf,
                                locsdf))
    
## STORE metadata
    if (etmtid == 5) measid <- 1
    if (etmtid == 6) measid <- 3

nullnrow <- function(df) if(is.null(df)) return(0) else return(nrow(df))   
        
if (nullnrow(tmp.df) == 0) {
    
    zerodf <- as.data.table(expand.grid(etmodel_type_id = etmtid,
                          age_group_id = agid,
                          sex_id = sid,
                          measure_id = measid,
                          metric_id = 3,
                          cause_id = gbdid,
                          sdi = round(seq(0,1,.005), 3),
                          lmpred = log(1e-8),
                          pred = log(1e-8)))
    
    saveRDS(zerodf, file = "FILEPATH")
    
    zerodrawsdf <- as.data.table(expand.grid(etmodel_type_id = etmtid,
                            etmodel_version_id = etmvid,
                            sdi = round(seq(0,1,.005), 3),
                            age_group_id = agid,
                            sex_id = sid,
                            measure_id = measid,
                            metric_id = 3,
                            cause_id = gbdid,
                            draw = 1:100,
                            sim = 1:100,
                            lmpred = 0,
                            pred = 0))
    
    saveRDS(zerodrawsdf,"FILEPATH")
    
    
    
    unlink(x = "FILEPATH")
    
    
    
    
} else {
    
    ## Store more metadata
    measid <- unique(tmp.df$measure_id)
    metid <- unique(tmp.df$metric_id)
    
    ## FIT LINEAR MODEL
    lm.df <- fit.lm(df = tmp.df,
                    method = Methods[meth],
                    meshrange = meshrange)
    
    tmp.fit <- {}
    
if (!prioronly) {    
    ## Save n basis and pcprior arguments based on model type
    if (etmtid  == 1) pcpvals <- c(0.05, 5)
    if (etmtid == 2) pcpvals <- c(0.05, .1)
    if (etmtid %in% c(3, 4, 5,6)) {
        
        pvn <- 1
        prn <- .2
        prc <- 1e10
    }

    ## pass it to INLA to fit
    message(sprintf("ON AGE.ID: %i SEX.ID: %i ON METHOD: %s", agid, sid, Methods[meth]))

    
    try(tmp.fit <- fit.gpr(df = lm.df$resids,
                           method = "direct", 
                           verbose.INLA = TRUE,
                           pcprior = FALSE, 
                           prior.variance.nominal = pvn,
                           prior.range.nominal = prn,
                           spatial.param.prec = prc,
                           n.basis = nbasis,
                           us.int = usintercept))
    
    if(is.null(tmp.fit)) {
        
        prc <- 1
        
        try(tmp.fit <- fit.gpr(df = lm.df$resids,
                               method = "direct", 
                               verbose.INLA = TRUE,
                               pcprior = FALSE,
                               prior.variance.nominal = pvn,
                               prior.range.nominal = prn,
                               spatial.param.prec = prc,
                               n.basis = nbasis,
                               us.int = usintercept))
        
        
    }
    
    if(is.null(tmp.fit)) {
        
        prc <- .001
        
        try(tmp.fit <- fit.gpr(df = lm.df$resids,
                               method = "direct",
                               verbose.INLA = TRUE,
                               pcprior = FALSE, 
                               prior.variance.nominal = pvn,
                               prior.range.nominal = prn,
                               spatial.param.prec = prc,
                               n.basis = nbasis,
                               us.int = usintercept))
        
        
    }
    }
    if(is.null(tmp.fit)|prioronly){
        
        saveRDS(tmp.fit, file = "FILEPATH")
        zerofitdf <- as.data.table(expand.grid(etmodel_type_id = etmtid,
                                            age_group_id = agid,
                                            sex_id = sid,
                                            measure_id = measid,
                                            metric_id = 3,
                                            cause_id = gbdid,
                                            sdi = round(seq(0,1,.005), 3),
                                            gprpred = 0))
        
        zerofitdf <- merge(zerofitdf, lm.df$preds, by = "sdi")
        zerofitdf[, pred := gprpred + lmpred][, gprpred := NULL]
        
        
        saveRDS(zerofitdf, file = "FILEPATH")
        
        zerodrawsfitdf <- as.data.table(expand.grid(etmodel_type_id = etmtid,
                                                 etmodel_version_id = etmvid,
                                                 sdi = round(seq(0,1,.005), 3),
                                                 age_group_id = agid,
                                                 sex_id = sid,
                                                 measure_id = measid,
                                                 metric_id = 3,
                                                 cause_id = gbdid,
                                                 draw = 1:100,
                                                 sim = 1:100,
                                                 gprpred = 0))
        
        zerodrawsfitdf <- merge(zerodrawsfitdf, lm.df$preds, by = "sdi")
        zerodrawsfitdf[, pred := gprpred + lmpred][, gprpred := NULL]
        
        ## Back Transform Draws
        zerodrawsfitdf <- zerodrawsfitdf[metric_id == 3, pred := exp(pred) - 1e-8][metric_id == 3, lmpred := exp(lmpred) - 1e-8]
        zerodrawsfitdf <- zerodrawsfitdf[metric_id == 2, pred := exp(pred) / (1 + exp(pred))]
        
        floor <- zerodrawsfitdf[pred > 0, .1*min(pred)] # Set floor as 10 percent of lowest non-zero value
        zerodrawsfitdf <- zerodrawsfitdf[pred < 0, pred := floor]
        
        # Make sure SDI is rounded to 3-digits and return
        zerodrawsfitdf <- zerodrawsfitdf[, sdi := round(sdi, 3)]
        

        
        if(prioronly) message("Keeping only first-stage fit, per manual specification")
        else message("GPR did not converge--saving first-stage fit")

        
    } 
    stopifnot(!is.null(tmp.fit)|!prioronly)
    
    
    ## if it's still not worked, mark it as bad
    if(is.null(tmp.fit)){
        num.failed <- num.failed + 1
        failed.runs[num.failed] <- sprintf("fit_%i_%i_%s", agid, sid, Methods[meth])
    }else{
        
        ## save the mean
        
        draws <- get.draws(tmp.fit[[5]], n.draws = 100, mesh = tmp.fit[[6]])
        tmp.fit[[2]] <- rowMeans(draws[, -1]) ## take the mean across the rows, leave out first sdi col
        
        tmp.res <- data.table(et_model_type = rep(etmtid, length(tmp.fit[[1]])),
                              age_group_id = rep(agid, length(tmp.fit[[1]])),
                              sex_id = rep(sid, length(tmp.fit[[1]])),
                              measure = rep(measid, length(tmp.fit[[1]])),
                              metric_id = rep(metid, length(tmp.fit[[1]])),
                              cause_id = rep(gbdid, length(tmp.fit[[1]])),
                              sdi = round(tmp.fit[[1]], 3),
                              gprpred = tmp.fit[[2]])
        
        ## ADD PREDICTED RESIDUALS TO LM FIT
        tmp.res <- merge(tmp.res, lm.df$preds, by = "sdi")
        tmp.res[, pred := gprpred + lmpred][, gprpred := NULL]
        
        
        if(interactive()){
            message("Plotting")
            
            pdf("FILEPATH", width = 10, height = 10)
            plot(val ~ sdi, tmp.df, xlab="SDI",ylab="Log MR", ylim = c(min(tmp.df$val)-.5, max(tmp.df$val + .5)))
            lines(tmp.res$sdi, tmp.res$lmpred, col = "blue")
            lines(tmp.res$sdi, tmp.res$pred, lty=2, col = "red")
            dev.off()
        }
        
        
        

        gprmeta <- data.table(nbases = nbasis, precision = summary(tmp.fit$inlaobj)$hyperpar$mean[1], theta1 = summary(tmp.fit$inlaobj)$hyperpar$mean[2],
                              theta2 = summary(tmp.fit$inlaobj)$hyperpar$mean[3], meshrange = meshrange)
        
        saveRDS(gprmeta, file = "FILEPATH")
        
        saveRDS(tmp.res, file = "FILEPATH")
        

    }
    
    
    
    
    ## write a check to see if any failed
    
    num.failed <- 0
    failed.list <- list(NULL)
    
    
    tmp.mean.df <- {}
    try(tmp.mean.df <- readRDS("FILEPATH"))
    
    if(is.null(tmp.mean.df)){
        num.failed <- num.failed + 1
        failed.list[[num.failed]] <- sprintf("fit_%i_%i_%s", agid, sid, Methods[meth])
    }
    
    
    num.failed ## to see if they all worked
    failed.list ## names of failed ones
    


  